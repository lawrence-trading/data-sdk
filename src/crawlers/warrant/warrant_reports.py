"""dlt source and resources for the three TWSE MOPS warrant reports.

| Report | MOPS page | dlt resource | Incremental cursor |
|---|---|---|---|
| 權證基本資料彙總表 | ``t90sb01`` | ``warrant_basic_info`` | ``exercise_end_date`` |
| 履約價格及行使比例調整公告彙總表 | ``t95sb02`` | ``warrant_strike_ratio_adjustment`` | ``adjustment_effective_date`` |
| 同上，生效日尚未到 | ``t95sb02`` | ``warrant_pending_adjustment`` | 無（整份替換） |
| 履約價格／履約點數重設公告彙總表 | ``t95sb03`` | ``warrant_strike_ratio_reset`` | ``reset_effective_date`` |
| 同上，生效日尚未到 | ``t95sb03`` | ``warrant_pending_reset`` | 無（整份替換） |

Column names follow the original ``common/warrant_basic_crawler`` vocabulary
(``type``, ``latest_strike``, ``alloc_qty_per_1k``, …) so the basic-info table
stays a drop-in replacement for the existing ``warrant_basic_info.parquet``
that ``common/warrant_meta`` reads.

Every resource is ``write_disposition='append'``: the two event reports are
immutable once published, and only the *immutable, issuance-time* fields of
the basic-info report are kept (whether a warrant's strike/ratio have since
changed is answered by the event reports, not by re-reading the 'latest'
snapshot columns). Nothing is ever updated in place, so no merge/upsert — and
therefore no SQL-capable destination — is needed.

All three reports live on the same MOPS site behind the same site-wide rate
limiter, so they share one cookie-primed session. Retries use dlt's
``requests.Client``, whose ``retry_condition`` can inspect the response
*body* — necessary because MOPS signals throttling with a ``200 OK`` page
containing '過於頻繁'/'請稍後' rather than an error status.
"""
from __future__ import annotations

import datetime
import io
import math
import time

import dlt
import pandas as pd
import requests
from bs4 import BeautifulSoup
from dlt.sources.helpers.requests import Client

from . import taiwan_today

MOPS_BASE_URL = 'https://mopsov.twse.com.tw/mops/web/'
MOPS_USER_AGENT = (
    'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 '
    '(KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36'
)
DEFAULT_REQUEST_DELAY_SECONDS = 0.5
MAX_RETRY_ATTEMPTS = 4
HISTORY_START_YEAR = 2003          # MOPS t90sb01 delisted data goes back to ~ROC 92
EARLIEST_CURSOR_DATE = datetime.date(2003, 1, 1)
MARKET_CODES = ((1, 'twse'), (2, 'otc'))
RESULT_PAGE_SIZE = 1000            # MOPS returns 1000 rows/page; a short page is the last one
PENDING_ADJUSTMENT_WINDOW_DAYS = 45  # how far ahead to read not-yet-effective t95sb02 rows
# Each event report is published twice, once per market; both must be read or
# every OTC warrant's strike history is missing.
ADJUSTMENT_ENDPOINTS = ('ajax_t95sb02', 'ajax_o_t95sb02')
RESET_ENDPOINTS = ('ajax_t95sb03', 'ajax_o_t95sb03')
MAX_RESULT_PAGES = 1000            # pagination safety cap (a busy year is ~55 pages)

# MOPS renders blanks as these tokens; pandas decodes &nbsp; to a literal '\xa0'.
BLANK_CELL_TOKENS = {'', '--', '—', '－', 'nan', 'none', '\xa0'}


# ── HTTP ─────────────────────────────────────────────────────────────────────

def is_throttle_notice(response: requests.Response | None, exception: BaseException | None = None) -> bool:
    return response is not None and ('過於頻繁' in response.text or '請稍後' in response.text)


def create_cookie_primed_session() -> requests.Session:
    """A retrying session with the site-wide ``jcsession`` cookie primed."""
    session = Client(
        request_max_attempts=MAX_RETRY_ATTEMPTS,
        retry_condition=is_throttle_notice,
        session_attrs={'headers': {'User-Agent': MOPS_USER_AGENT}},
    ).session
    session.get(MOPS_BASE_URL + 't90sb01', timeout=30)
    return session


def post_to_mops(
    session: requests.Session,
    ajax_endpoint: str,
    form_payload: dict,
    request_delay_seconds: float
) -> requests.Response:
    """POST to a MOPS ``ajax_*`` endpoint, sleeping ``request_delay_seconds``
    first (proactive politeness, distinct from the session's reactive retry)."""
    time.sleep(request_delay_seconds)
    response = session.post(MOPS_BASE_URL + ajax_endpoint, data=form_payload, timeout=60)
    response.encoding = 'utf-8'
    if is_throttle_notice(response):
        # The retry predicate only *retries* on a throttle notice; once attempts
        # are exhausted tenacity returns the last (still-throttled) response
        # instead of raising. Treating that as 'no data' would let the
        # incremental cursor advance past a window that was never fetched.
        raise RuntimeError(f'MOPS is still throttling {ajax_endpoint} after {MAX_RETRY_ATTEMPTS} attempts')
    return response


# ── parsing ──────────────────────────────────────────────────────────────────

def parse_republic_date(raw_value: object) -> pd.Timestamp:
    """ROC ``'YYY/MM/DD'`` (``'113/04/09'``) -> Gregorian Timestamp (``2024-04-09``);
    blank/malformed -> ``NaT``. The only place 民國年 is converted away."""
    parts = str(raw_value).strip().split('/')
    if len(parts) != 3:
        return pd.NaT
    try:
        republic_year, month, day = (int(part) for part in parts)
        return pd.Timestamp(year=republic_year + 1911, month=month, day=day)
    except (ValueError, TypeError):
        return pd.NaT


def format_republic_date(date_value: datetime.date) -> str:
    """Gregorian date -> MOPS ROC ``YYYMMDD`` query token (``2025-05-19`` -> ``'1140519'``)."""
    return f'{date_value.year - 1911}{date_value.month:02d}{date_value.day:02d}'


def read_html_table_as_strings(table_html: str) -> pd.DataFrame | None:
    """Parse one ``<table>`` with every column forced to ``str``.

    Left to itself ``read_html`` infers column types, and an all-digit column
    (a warrant code like ``'030076'``) silently becomes an int, losing its
    leading zero. The column count isn't known up front — and an oversized
    ``converters`` map raises ``IndexError`` — so parse once to learn it, then
    re-parse forcing every column to ``str``.
    """
    probe = pd.read_html(io.StringIO(table_html), flavor='lxml')
    if not probe or probe[0].empty:
        return None
    column_count = probe[0].shape[1]
    return pd.read_html(
        io.StringIO(table_html), converters={i: str for i in range(column_count)}, flavor='lxml'
    )[0]


def dataframe_to_records(
    frame: pd.DataFrame,
    date_columns: list[str],
    numeric_columns: list[str]
) -> list[dict]:
    """Renamed MOPS frame -> plain-Python records: dates to ``datetime.date``,
    numerics to ``float``, everything else to stripped ``str``; blanks to ``None``.

    ``date_columns``/``numeric_columns`` may name columns absent from ``frame``
    (the uncapped 表二 section has no cap/floor columns); those are skipped.
    """
    def clean_text(raw_value: object) -> str | None:
        text = str(raw_value).replace('\xa0', ' ').strip()
        return None if text.lower() in BLANK_CELL_TOKENS else text

    def clean_number(raw_value: object) -> float:
        text = clean_text(raw_value)
        if text is None:
            return math.nan
        try:
            return float(text.replace(',', ''))
        except ValueError:
            return math.nan

    cleaned = frame.copy()
    for column in cleaned.columns:
        if column in date_columns:
            cleaned[column] = cleaned[column].map(parse_republic_date)
        elif column in numeric_columns:
            cleaned[column] = cleaned[column].map(clean_number)
        else:
            cleaned[column] = cleaned[column].map(clean_text)

    present_date_columns = [column for column in date_columns if column in cleaned.columns]
    present_numeric_columns = [column for column in numeric_columns if column in cleaned.columns]
    records = cleaned.to_dict('records')
    for record in records:
        for column in present_date_columns:
            record[column] = None if pd.isna(record[column]) else pd.Timestamp(record[column]).date()
        for column in present_numeric_columns:
            record[column] = None if math.isnan(record[column]) else record[column]
    return records


def column_type_hints(
    ordered_columns: list[str],
    numeric_columns: list[str],
    date_columns: list[str],
    bool_columns: list[str] = (),
) -> dict:
    """dlt column hints pinning the type of every column, in output order.

    Without these, dlt infers each column's type from the data it sees. It
    buffers rows in chunks (``buffer_max_items``, 5000 by default), and for a
    chunk in which some column is NULL in *every* row it cannot infer a type,
    so it **omits that column from that chunk's parquet file** and adds it back
    once a non-NULL value shows up. The table directory then holds files with
    two different schemas, which a plain ``pd.read_parquet(dir)`` or
    ``pl.scan_parquet(glob)`` refuses to read.

    That is not hypothetical here: cap/floor are populated only by the rare
    上下限型 warrants (tens of rows out of ~85k), so the leading chunks are
    all-NULL. Pinning the types keeps every file on one schema.

    Hinted columns are emitted **before** unhinted ones, in the order they are
    declared here — so ``ordered_columns`` must list *every* column, in the
    intended output order, or the parquet column order silently changes.
    """
    def data_type(column: str) -> str:
        if column in numeric_columns:
            return 'double'
        if column in date_columns:
            return 'date'
        if column in bool_columns:
            return 'bool'
        return 'text'

    return {column: {'data_type': data_type(column), 'nullable': True} for column in ordered_columns}


# ── t90sb01: warrant basic info ──────────────────────────────────────────────

# The t90sb01 table is 20 columns in this fixed order; mapped by **position**
# because the live headers carry footnote noise ('(詳備註一)') and stray spaces.
BASIC_INFO_COLUMNS = [
    'warrant_id',             # 權證代號 — primary key (str; keep leading chars/zeros)
    'warrant_name',           # 權證簡稱
    'issuer',                 # 發行機構名稱
    'type',                   # 權證類型 (認購=call / 認售=put)
    'lp_quote_method',        # 流動量提供者報價方式
    'list_date',              # 上市日期
    'exercise_start_date',    # 履約開始日
    'last_trade_date',        # 最後交易日 (time-to-expiry)
    'exercise_end_date',      # 履約截止日 (到期日)
    'settlement_note',        # 結算方式說明
    'issued_qty_k_units',     # 權證發行數量(仟單位)
    'target_stock_id',        # 標的代號
    'target_name',            # 標的名稱
    'alloc_qty_per_1k',       # 最新標的履約配發數量(每仟單位權證) — /1000 = shares per unit
    'original_strike',        # 原始履約價格(元)/履約點數
    'original_cap',           # 原始上限價格/上限點數
    'original_floor',         # 原始下限價格/下限點數
    'latest_strike',          # 最新履約價格(元)/履約點數 — the strike K
    'latest_cap',             # 最新上限價格/上限點數
    'latest_floor',           # 最新下限價格/下限點數
]
BASIC_INFO_DATE_COLUMNS = [
    'list_date',
    'exercise_start_date',
    'last_trade_date',
    'exercise_end_date',
]
BASIC_INFO_NUMERIC_COLUMNS = [
    'issued_qty_k_units',
    'alloc_qty_per_1k',
    'original_strike',
    'original_cap',
    'original_floor',
    'latest_strike',
    'latest_cap',
    'latest_floor',
]


def crawl_basic_info_market(
    session: requests.Session,
    market_code: int,
    query_fields: dict,
    request_delay_seconds: float
) -> pd.DataFrame | None:
    """Page through one market's t90sb01 result for one query.

    MOPS's '下一頁' button is unreliable (a full page can lack it), so paging
    stops on a short page, an empty page, or an all-already-seen page instead.
    """
    pages: list[pd.DataFrame] = []
    seen_warrant_ids: set[str] = set()
    for page_number in range(1, MAX_RESULT_PAGES + 1):
        if page_number == 1:
            payload = {'step': '1', 'firstin': '1', 'off': '1', 'r': str(market_code), **query_fields}
        else:
            payload = {
                'step': '1', 'TYPEK': '', 'r': str(market_code), 'rc': '',
                'start_date': '', 'end_date': '', 'firstin': '1', 'isShowForm': '1',
                'pagesize': '', 'pageno': str(page_number), **query_fields,
            }
        html = post_to_mops(session, 'ajax_t90sb01', payload, request_delay_seconds).text

        # Isolate the data table: the one containing 權證代號 with the most rows
        # (the page also carries small helper tables).
        soup = BeautifulSoup(html, 'lxml')
        data_table, most_rows = None, 0
        for table in soup.find_all('table'):
            if '權證代號' in table.get_text() and len(table.find_all('tr')) > most_rows:
                data_table, most_rows = table, len(table.find_all('tr'))
        if data_table is None:
            break
        frame = read_html_table_as_strings(str(data_table))
        if frame is None or frame.shape[1] < len(BASIC_INFO_COLUMNS):
            break

        frame = frame.iloc[:, : len(BASIC_INFO_COLUMNS)]
        frame.columns = BASIC_INFO_COLUMNS
        # Drop stray non-data rows (e.g. a repeated header) lacking a real id.
        # Codes are 6 characters (+ recycling suffix) from 2005 on; before that
        # they were 4 digits (``0680a``), so the floor is 4, not 6.
        frame = frame[frame['warrant_id'].str.match(r'^\w{4,}$', na=False)].reset_index(drop=True)
        if frame.empty:
            break
        page_warrant_ids = set(frame['warrant_id'])
        if page_warrant_ids <= seen_warrant_ids:
            break
        pages.append(frame)
        seen_warrant_ids |= page_warrant_ids
        if len(frame) < RESULT_PAGE_SIZE:
            break
    return pd.concat(pages, ignore_index=True) if pages else None


@dlt.resource(
    name='warrant_basic_info',
    write_disposition='append',
    columns=column_type_hints(
        BASIC_INFO_COLUMNS + ['market'], BASIC_INFO_NUMERIC_COLUMNS, BASIC_INFO_DATE_COLUMNS
    ),
)
def warrant_basic_info_resource(
    session: requests.Session,
    history_start_year: int = HISTORY_START_YEAR,
    history_end_year: int | None = None,
    request_delay_seconds: float = DEFAULT_REQUEST_DELAY_SECONDS,
    exercise_end_date=dlt.sources.incremental('exercise_end_date', initial_value=EARLIEST_CURSOR_DATE),
):
    """Expired/delisted warrants only: the rc=0 view returns 到期日 <= today,
    swept one 到期日 year-window at a time to keep each result set pageable.

    The cursor is ``exercise_end_date`` because that is what the sweep is keyed
    on: an expired warrant's row is immutable, so once a 到期日 window has
    been read it never needs reading again, and the sweep resumes from the
    cursor's year rather than ``history_start_year``. (An earlier cursor on
    ``list_date`` made a backfill of older windows impossible -- every old
    row was filtered out as "already seen".) Live warrants are not yielded
    here; they come from :func:`warrant_active_snapshot_resource`, and
    ``build_basic_info`` adopts the ones this table has not recorded yet.

    ``history_end_year`` caps the sweep so a multi-decade backfill can be run
    in chunks: MOPS drops the connection after ~30 minutes of paging, and dlt
    discards the whole package when extraction fails, so one run per few
    years is what actually lands. The cursor carries over between runs.
    """
    today = taiwan_today()
    frames: list[pd.DataFrame] = []

    first_year = max(history_start_year, exercise_end_date.last_value.year)
    last_year = min(history_end_year or today.year, today.year)
    for year in range(first_year, last_year + 1):
        last_month = 12 if year < today.year else today.month
        window = {
            'rc': '0',
            'start_date': f'{year - 1911}01',
            'end_date': f'{year - 1911}{last_month:02d}',
        }
        for market_code, market_name in MARKET_CODES:
            frame = crawl_basic_info_market(session, market_code, window, request_delay_seconds)
            if frame is not None:
                frames.append(frame.assign(market=market_name))

    if not frames:
        return
    combined = pd.concat(frames, ignore_index=True)
    # Warrant codes are recycled after expiry, so the same warrant_id can denote
    # different instruments over time — dedupe on the composite key, never id alone.
    combined = combined.drop_duplicates(subset=['warrant_id', 'exercise_end_date'], keep='first')
    yield dataframe_to_records(combined, BASIC_INFO_DATE_COLUMNS, BASIC_INFO_NUMERIC_COLUMNS)


@dlt.resource(
    name='warrant_active_snapshot',
    write_disposition='replace',
    columns=column_type_hints(
        BASIC_INFO_COLUMNS + ['market', 'crawl_date'],
        BASIC_INFO_NUMERIC_COLUMNS,
        BASIC_INFO_DATE_COLUMNS + ['crawl_date'],
    ),
)
def warrant_active_snapshot_resource(
    session: requests.Session,
    request_delay_seconds: float = DEFAULT_REQUEST_DELAY_SECONDS,
):
    """t90sb01 current view, whole table replaced every run.

    :func:`warrant_basic_info_resource` records expired warrants only, and
    never re-reads one. This resource has no cursor and replaces its whole
    table, so every run re-reads the current terms of every live warrant: it
    is the only place a live warrant's ``exercise_end_date``,
    ``last_trade_date`` and ``latest_*`` are kept fresh after an early
    termination, extension or adjustment, and the only source of a listing
    until it expires and the delisted sweep picks it up.
    """
    today = taiwan_today()
    frames: list[pd.DataFrame] = []
    for market_code, market_name in MARKET_CODES:
        frame = crawl_basic_info_market(session, market_code, {}, request_delay_seconds)
        if frame is not None:
            frames.append(frame.assign(market=market_name))
    if not frames:
        return
    combined = pd.concat(frames, ignore_index=True)
    combined = combined.drop_duplicates(subset=['warrant_id', 'exercise_end_date'], keep='first')

    # crawl_date is set after dataframe_to_records: it is already a real date,
    # and that helper would run it through the ROC-string parser and blank it.
    records = dataframe_to_records(combined, BASIC_INFO_DATE_COLUMNS, BASIC_INFO_NUMERIC_COLUMNS)
    for record in records:
        record['crawl_date'] = today
    yield records


# ── t95sb02 / t95sb03: strike & ratio events ─────────────────────────────────

# NOTE: ``latest_ratio`` here (最新行使比例, e.g. 1.9700 shares/unit) is *not*
# the same unit as basic info's ``alloc_qty_per_1k`` (per 1000 units, e.g.
# 100 → 0.1 shares/unit), so the two deliberately do not share a name.
ADJUSTMENT_CAPPED_COLUMNS = [
    'warrant_id',                 # 權證代號
    'warrant_name',               # 權證名稱
    'exercise_method',            # 履約方式 (歐式/美式)
    'type',                       # 權證類型 (認購/認售)
    'target_stock_id',            # 標的證券代號
    'target_name',                # 標的證券名稱
    'latest_strike',              # 最新履約價格/履約指數
    'latest_ratio',               # 最新行使比例 (shares per warrant unit)
    'latest_cap',                 # 最新上限價格(元)
    'latest_floor',               # 最新下限價格(元)
    'adjustment_effective_date',  # 調整生效日期
    'exercise_end_date',          # 權證到期日
]
ADJUSTMENT_UNCAPPED_COLUMNS = [
    'warrant_id',                 # 權證代號
    'warrant_name',               # 權證名稱
    'exercise_method',            # 履約方式 (歐式/美式)
    'type',                       # 權證類型 (認購/認售)
    'target_stock_id',            # 標的證券代號
    'target_name',                # 標的證券名稱
    'latest_strike',              # 最新履約價格/履約指數
    'latest_ratio',               # 最新行使比例 (shares per warrant unit)
    'adjustment_effective_date',  # 調整生效日期
    'exercise_end_date',          # 權證到期日
]
ADJUSTMENT_NUMERIC_COLUMNS = [
    'latest_strike',
    'latest_ratio',
    'latest_cap',
    'latest_floor',
]

RESET_CAPPED_COLUMNS = [
    'warrant_id',            # 權證代號
    'warrant_name',          # 權證名稱
    'exercise_method',       # 履約方式 (歐式/美式)
    'type',                  # 權證類型 (認購/認售)
    'target_stock_id',       # 標的代號
    'target_name',           # 標的名稱
    'original_strike',       # 原始履約價格(元)/履約點數
    'original_cap',          # 原始上限價格(元)/上限點數
    'original_floor',        # 原始下限價格(元)/下限點數
    'reset_strike',          # 重設後履約價格(元)/履約點數
    'reset_cap',             # 重設後上限價格(元)/上限點數
    'reset_floor',           # 重設後下限價格(元)/下限點數
    'latest_ratio',          # 最新行使比例 (shares per warrant unit)
    'reset_effective_date',  # 重設生效日期
    'exercise_end_date',     # 權證到期日
]
RESET_UNCAPPED_COLUMNS = [
    'warrant_id',            # 權證代號
    'warrant_name',          # 權證名稱
    'exercise_method',       # 履約方式 (歐式/美式)
    'type',                  # 權證類型 (認購/認售)
    'target_stock_id',       # 標的代號
    'target_name',           # 標的名稱
    'original_strike',       # 原始履約價格(元)/履約點數
    'reset_strike',          # 重設後履約價格(元)/履約點數
    'latest_ratio',          # 最新行使比例 (shares per warrant unit)
    'reset_effective_date',  # 重設生效日期
    'exercise_end_date',     # 權證到期日
]
RESET_NUMERIC_COLUMNS = [
    'original_strike',
    'original_cap',
    'original_floor',
    'reset_strike',
    'reset_cap',
    'reset_floor',
    'latest_ratio',
]


def crawl_event_report(
    session: requests.Session,
    ajax_endpoint: str,
    capped_columns: list[str],
    uncapped_columns: list[str],
    effective_date_column: str,
    numeric_columns: list[str],
    start_date: datetime.date,
    end_date: datetime.date,
    request_delay_seconds: float,
) -> list[dict]:
    """One 生效日期 window of t95sb02/t95sb03, both sections, as records.

    Both reports split their result into 表一 (上下限型, carries cap/floor
    columns) and 表二 (非上下限型, no cap/floor); either may be absent as a
    '尚無資料' message. Neither report paginates — one query returns the whole
    window — so there is no page loop here.
    """
    payload = {
        'step': '1',
        'firstin': '1',
        'TYPEK': '',
        'colorchg': '',
        'flag': '',
        'co_id': '',
        'warrant_id': '',
        'effective_date_1': format_republic_date(start_date),
        'effective_date_2': format_republic_date(end_date),
    }
    html = post_to_mops(session, ajax_endpoint, payload, request_delay_seconds).text
    soup = BeautifulSoup(html, 'lxml')
    date_columns = [effective_date_column, 'exercise_end_date']

    records: list[dict] = []
    for section_label, columns, has_cap_floor in (
        ('表一', capped_columns, True),
        ('表二', uncapped_columns, False),
    ):
        label_tag = next(
            (tag for tag in soup.find_all('b') if tag.get_text(strip=True).startswith(section_label)), None
        )
        if label_tag is None:
            continue
        # Walk forward to whichever comes first: this section's data table, or
        # its '尚無資料' (no data) marker.
        data_table = None
        for element in label_tag.find_all_next():
            if element.name == 'table' and 'hasBorder' in (element.get('class') or []):
                data_table = element
                break
            if '尚無資料' in element.get_text():
                break
        if data_table is None:
            continue

        frame = read_html_table_as_strings(str(data_table))
        if frame is None:
            continue
        frame = frame.iloc[:, : len(columns)]
        frame.columns = columns
        # MOPS's own response has been observed to repeat a row verbatim;
        # cross-run incremental filtering can't catch a duplicate that arrives
        # inside a single fetch, so drop it here.
        frame = frame.drop_duplicates().reset_index(drop=True)
        for record in dataframe_to_records(frame, date_columns, numeric_columns):
            record['has_cap_floor'] = has_cap_floor
            records.append(record)
    return records


@dlt.resource(
    name='warrant_strike_ratio_adjustment',
    write_disposition='append',
    columns=column_type_hints(
        ADJUSTMENT_CAPPED_COLUMNS + ['has_cap_floor'],
        ADJUSTMENT_NUMERIC_COLUMNS,
        ['adjustment_effective_date', 'exercise_end_date'],
        ['has_cap_floor'],
    ),
)
def strike_ratio_adjustment_resource(
    session: requests.Session,
    request_delay_seconds: float = DEFAULT_REQUEST_DELAY_SECONDS,
    adjustment_effective_date=dlt.sources.incremental(
        'adjustment_effective_date', initial_value=EARLIEST_CURSOR_DATE
    ),
):
    """t95sb02 — one row per strike/ratio adjustment (driven by the
    underlying's ex-dividend/ex-rights/capital changes)."""
    today = taiwan_today()
    if adjustment_effective_date.last_value > today:
        return
    # One yield for both markets: a second yield is filtered against the cursor
    # the first one already advanced, which silently drops the OTC history.
    yield [
        record
        for ajax_endpoint in ADJUSTMENT_ENDPOINTS
        for record in crawl_event_report(
            session,
            ajax_endpoint,
            ADJUSTMENT_CAPPED_COLUMNS,
            ADJUSTMENT_UNCAPPED_COLUMNS,
            'adjustment_effective_date',
            ADJUSTMENT_NUMERIC_COLUMNS,
            adjustment_effective_date.last_value,
            today,
            request_delay_seconds,
        )
    ]


@dlt.resource(
    name='warrant_pending_adjustment',
    write_disposition='replace',
    columns=column_type_hints(
        ADJUSTMENT_CAPPED_COLUMNS + ['has_cap_floor'],
        ADJUSTMENT_NUMERIC_COLUMNS,
        ['adjustment_effective_date', 'exercise_end_date'],
        ['has_cap_floor'],
    ),
)
def pending_adjustment_resource(
    session: requests.Session,
    request_delay_seconds: float = DEFAULT_REQUEST_DELAY_SECONDS,
):
    """t95sb02 rows whose 生效日 is still in the future, replaced whole each run.

    An ex-dividend adjustment is published days ahead of the date it takes
    effect, and ``t90sb01``'s snapshot applies it a day early -- so without
    these rows the history learns of the change only from a snapshot diff
    dated at the crawl, a day before it is true. These rows carry their own
    生效日, so the history gets the real date.

    Kept out of :func:`strike_ratio_adjustment_resource` because its cursor
    would advance past today and then block every later run. This table is
    replaced whole instead: a row here is provisional until its date arrives,
    after which the incremental resource records it as history.
    """
    today = taiwan_today()
    for ajax_endpoint in ADJUSTMENT_ENDPOINTS:
        yield crawl_event_report(
            session,
            ajax_endpoint,
            ADJUSTMENT_CAPPED_COLUMNS,
            ADJUSTMENT_UNCAPPED_COLUMNS,
            'adjustment_effective_date',
            ADJUSTMENT_NUMERIC_COLUMNS,
            today + datetime.timedelta(days=1),
            today + datetime.timedelta(days=PENDING_ADJUSTMENT_WINDOW_DAYS),
            request_delay_seconds,
        )


@dlt.resource(
    name='warrant_pending_reset',
    write_disposition='replace',
    columns=column_type_hints(
        RESET_CAPPED_COLUMNS + ['has_cap_floor'],
        RESET_NUMERIC_COLUMNS,
        ['reset_effective_date', 'exercise_end_date'],
        ['has_cap_floor'],
    ),
)
def pending_reset_resource(
    session: requests.Session,
    request_delay_seconds: float = DEFAULT_REQUEST_DELAY_SECONDS,
):
    """t95sb03 rows whose 生效日 is still in the future, replaced whole each run.

    A reset-type warrant fixes its provisional strike on its listing day, and
    the reset is published the day before -- so on the eve of a listing MOPS
    and the exchange already show the reset strike while ``t90sb01``'s
    ``original_strike`` is still the provisional one. Same treatment as
    :func:`pending_adjustment_resource`, and the same reason for keeping it out
    of the cursored resource.
    """
    today = taiwan_today()
    for ajax_endpoint in RESET_ENDPOINTS:
        yield crawl_event_report(
            session,
            ajax_endpoint,
            RESET_CAPPED_COLUMNS,
            RESET_UNCAPPED_COLUMNS,
            'reset_effective_date',
            RESET_NUMERIC_COLUMNS,
            today + datetime.timedelta(days=1),
            today + datetime.timedelta(days=PENDING_ADJUSTMENT_WINDOW_DAYS),
            request_delay_seconds,
        )


@dlt.resource(
    name='warrant_strike_ratio_reset',
    write_disposition='append',
    columns=column_type_hints(
        RESET_CAPPED_COLUMNS + ['has_cap_floor'],
        RESET_NUMERIC_COLUMNS,
        ['reset_effective_date', 'exercise_end_date'],
        ['has_cap_floor'],
    ),
)
def strike_ratio_reset_resource(
    session: requests.Session,
    request_delay_seconds: float = DEFAULT_REQUEST_DELAY_SECONDS,
    reset_effective_date=dlt.sources.incremental(
        'reset_effective_date', initial_value=EARLIEST_CURSOR_DATE
    ),
):
    """t95sb03 — reset-type warrants' provisional strike being finalized, once,
    on their own listing day."""
    today = taiwan_today()
    if reset_effective_date.last_value > today:
        return
    # One yield for both markets; see strike_ratio_adjustment_resource.
    yield [
        record
        for ajax_endpoint in RESET_ENDPOINTS
        for record in crawl_event_report(
            session,
            ajax_endpoint,
            RESET_CAPPED_COLUMNS,
            RESET_UNCAPPED_COLUMNS,
            'reset_effective_date',
            RESET_NUMERIC_COLUMNS,
            reset_effective_date.last_value,
            today,
            request_delay_seconds,
        )
    ]


# ── t95sb01: issuer announcements ────────────────────────────────────────────

# 公告種類 codes on the t95sb01 form. Only the ones that move a warrant's term
# dates are crawled: an early termination pulls 到期日 forward, an extension
# pushes it back. Strike/ratio announcements (4, 6) duplicate t95sb02/t95sb03.
ANNOUNCEMENT_TYPE_CODES = {
    '11': 'early_termination',
    '9': 'extension_applied',
    '10': 'extension_effective',
}
ANNOUNCEMENT_COLUMNS = [
    'issuer_name',          # 公司名稱
    'announcement_kind',    # 公告種類
    'announcement_date',    # 輸入日期 — the day the market learned, NOT the effective date
    'warrant_id',           # 權證代號
    'warrant_name',         # 權證名稱
    'document_name',        # 公告稿 — '<issuer>-<new expiry>-<announced>-<time>-<seq>.doc'
]
# MOPS keeps only a rolling ~8 months of t95sb01,
# so this is a going-forward capture: history before the window is unrecoverable.
ANNOUNCEMENT_EARLIEST_DATE = datetime.date(2026, 1, 1)
ANNOUNCEMENT_ENDPOINTS = (('ajax_t95sb01', 'twse'), ('ajax_o_t95sb01', 'otc'))


def parse_announced_expiry_date(document_name: str) -> pd.Timestamp:
    """New 到期日 out of a 公告稿 filename, or NaT.

    ``7790-20260810-20260805-141118-9.doc`` -> 2026-08-10. The second field
    encodes the new expiry in Gregorian format, not ROC.
    """
    parts = str(document_name).split('-')
    if len(parts) < 3:
        return pd.NaT
    return pd.to_datetime(parts[1], format='%Y%m%d', errors='coerce')


def crawl_announcements_window(
    session: requests.Session,
    ajax_endpoint: str,
    type_code: str,
    start_date: datetime.date,
    end_date: datetime.date,
    request_delay_seconds: float,
) -> list[dict]:
    """One 公告種類, one 輸入日期 window, every issuer. Does not paginate."""
    payload = {
        'step': '1',
        'TYPEK': '',
        'firstin': '1',
        'co_id': '',
        'warrant_id': '',
        'type': type_code,
        'rd': '2',
        'in_month': '',
        'date_1': format_republic_date(start_date),
        'date_2': format_republic_date(end_date),
    }
    html = post_to_mops(session, ajax_endpoint, payload, request_delay_seconds).text
    soup = BeautifulSoup(html, 'lxml')

    data_table = None
    for table in soup.find_all('table'):
        rows = table.find_all('tr')
        if len(rows) > 1 and '權證代號' in rows[0].get_text():
            data_table = table
            break
    if data_table is None:
        return []

    frame = read_html_table_as_strings(str(data_table))
    if frame is None or frame.shape[1] < len(ANNOUNCEMENT_COLUMNS):
        return []
    frame = frame.iloc[:, : len(ANNOUNCEMENT_COLUMNS)]
    frame.columns = ANNOUNCEMENT_COLUMNS
    frame = frame[frame['warrant_id'].str.match(r'^\w{6,}$', na=False)]
    if frame.empty:
        return []

    frame = frame.drop_duplicates().reset_index(drop=True)
    frame['announcement_type'] = ANNOUNCEMENT_TYPE_CODES[type_code]

    # The expiry embedded in the document name is already Gregorian, so it is
    # attached after dataframe_to_records rather than through its ROC-date path.
    announced_expiry_dates = frame['document_name'].map(parse_announced_expiry_date)
    records = dataframe_to_records(frame, ['announcement_date'], [])
    for record, announced_expiry_date in zip(records, announced_expiry_dates):
        record['announced_expiry_date'] = (
            None if pd.isna(announced_expiry_date) else announced_expiry_date.date()
        )
    return records


@dlt.resource(
    name='warrant_announcement',
    write_disposition='append',
    columns=column_type_hints(
        ANNOUNCEMENT_COLUMNS + ['announced_expiry_date', 'announcement_type', 'market'],
        [],
        ['announcement_date', 'announced_expiry_date'],
    ),
)
def warrant_announcement_resource(
    session: requests.Session,
    request_delay_seconds: float = DEFAULT_REQUEST_DELAY_SECONDS,
    announcement_date=dlt.sources.incremental(
        'announcement_date', initial_value=ANNOUNCEMENT_EARLIEST_DATE
    ),
):
    """t95sb01 / o_t95sb01 — the only point-in-time source for expiry changes.

    A warrant terminated early stops trading days after the announcement, so
    BSM needs the announcement date, not the new expiry, as the moment the
    market repriced.
    """
    today = taiwan_today()
    window_start = announcement_date.last_value
    if window_start > today:
        return
    for ajax_endpoint, market_name in ANNOUNCEMENT_ENDPOINTS:
        for type_code in ANNOUNCEMENT_TYPE_CODES:
            records = crawl_announcements_window(
                session,
                ajax_endpoint,
                type_code,
                window_start,
                today,
                request_delay_seconds,
            )
            for record in records:
                record['market'] = market_name
            yield records


# ── source ───────────────────────────────────────────────────────────────────

@dlt.source(name='mops_warrant_reports')
def warrant_reports_source(
    history_start_year: int = HISTORY_START_YEAR,
    history_end_year: int | None = None,
    request_delay_seconds: float = DEFAULT_REQUEST_DELAY_SECONDS,
):
    session = create_cookie_primed_session()
    return [
        warrant_basic_info_resource(session, history_start_year, history_end_year, request_delay_seconds),
        warrant_active_snapshot_resource(session, request_delay_seconds),
        strike_ratio_adjustment_resource(session, request_delay_seconds),
        pending_adjustment_resource(session, request_delay_seconds),
        strike_ratio_reset_resource(session, request_delay_seconds),
        pending_reset_resource(session, request_delay_seconds),
        warrant_announcement_resource(session, request_delay_seconds),
    ]
