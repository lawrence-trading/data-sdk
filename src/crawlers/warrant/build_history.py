"""Build ``<cache-dir>/warrant_history.parquet`` -- point-in-time warrant terms.

One row per warrant per *state change*. Each row is a full snapshot of the
warrant's terms as known on ``effective_date``, so a lookup is one as-of join:

    history.join_asof(quotes, on='date', by='warrant_id', strategy='backward')
        .filter(pl.col('date') <= pl.col('exercise_end_date'))

There is deliberately no ``valid_to``: it would be exactly the next row's
``effective_date``, and dropping it turns an insert-plus-update into a plain
append. The trailing filter is what stops an as-of join from returning terms for
a date after the warrant expired.

**Expiry is a state column, not a static one.** Black-Scholes needs the maturity
the market believed at the time; a warrant terminated early was priced against
its original maturity right up to the announcement. So ``exercise_end_date`` and
``last_trade_date`` sit alongside strike and ratio, and an expiry change is its
own event row.

Sources, in precedence order on a ``(warrant_id, warrant_name, effective_date)`` collision:

| rank | source | covers | why |
|---|---|---|---|
| 1 | the TEJ Pro adjustment export | seed window | Historical backbone; MOPS's t95sb02 keeps only a rolling ~18 months. |
| 2 | ``mops_raw/warrant_announcement`` | captured announcement window | Expiry changes with their *announcement* date -- the only point-in-time expiry source. |
| 3 | ``mops_raw/warrant_strike_ratio_adjustment`` / ``_reset`` | after seed end | Keeps strike history running once the frozen TEJ seed stops. |
| 4 | synthesised issuance from the dimension table | warrants absent from TEJ | New listings, and the 2019 cohort predating the TEJ window. |
| 5 | ``mops_raw/warrant_active_snapshot`` diff | anything the above missed | Today's terms for a warrant whose change fell in a period no event source covers, dated at the crawl. |

**Rows can predate ``list_date``.** The issuance row sits on the issue date,
2-4 days before listing, and a reset-type warrant's reset lands on or just
before the listing day.
Both are kept as dated: the reset is the state that traded from day one, and
the issuance row before it carries the pre-reset strike. Whether a date is
tradable is the reader's concern -- ``list_date`` is on every row, and
``WarrantInfoWrapper.get_warrant_history(as_of=...)`` already filters on it.

Usage:
    python -m data_sdk.crawlers.warrant.build_history --cache-dir cache
"""
from __future__ import annotations

import argparse
from pathlib import Path

import numpy as np
import pandas as pd

from . import (
    DEFAULT_CACHE_PATH,
    TEJ_ADJUSTMENT_GLOB,
    TEJ_BASIC_INFO_GLOB,
    cache_directory as cache_dir,
    find_tej_seed,
    taiwan_today,
)
from .build_basic_info import WARRANT_KEY, add_warrant_key, load_raw_table

# Terms carried on every row. Missing values are forward-filled within a
# warrant, so a strike-only event keeps the expiry it inherited and vice versa.
#
# The underlying is a state too: a share conversion or merger moves every
# live warrant on it to the new code; earlier quote joins need the old code.
# Each TEJ row carries the underlying as of that event. MOPS's t95sb02/t95sb03
# rows after the seed carry the underlying as MOPS listed it when crawled (a conversion posts a row for
# every live warrant on its effective day), and whatever the chain then
# still disagrees with the dimension table on is closed by a
# ``target_change`` row (see :func:`events_from_target_closure`).
# ``target_name`` is not forward-filled but derived from the code on every
# row, because TEJ and MOPS spell names differently (國巨* / 國巨).
STATE_COLUMNS = [
    'strike',
    'ratio',
    'cap',
    'floor',
    'exercise_end_date',
    'last_trade_date',
    'target_stock_id',
]
STATIC_COLUMNS = [
    'issuer',
    'type',
    'market',
    'list_date',
    'exercise_start_date',
    'original_strike',
    'is_american',
]
EVENT_COLUMNS = WARRANT_KEY + ['effective_date', 'sequence'] + STATE_COLUMNS + [
    'target_name',
    'event_type',
    'source',
    'source_rank',
]


# Same-moment precedence: on a shared (effective_date, sequence) the lower
# rank wins (see chain_events).
SOURCE_RANK = {
    'tej': 1,
    'mops_announcement': 2,
    'mops_strike': 3,
    'dim_synthesised': 4,
    'mops_snapshot': 5,
    'dim_closure': 5,
}


def empty_events() -> pd.DataFrame:
    return pd.DataFrame(columns=EVENT_COLUMNS)


def tej_underlying_codes(tej: pd.DataFrame, dim_warrant: pd.DataFrame) -> pd.Series:
    """TEJ's ``標的代號`` in MOPS's spelling.

    A stock or ETF is ``2330.TW`` / ``6488.TWO``: the exchange suffix goes
    and the code is MOPS's. An index is a TEJ token (``Y9999`` 加權指數,
    ``M2300`` 電子類, ``HSI.HK``) where MOPS has its own (``IX0001``,
    ``IX0027``, ``HSI``), so each token is mapped to the MOPS code that most
    of the warrants carrying it have in the dimension table. Only tokens
    are mapped that way: a stock code is kept as TEJ states it, because the
    cases where it differs from the dimension table are exactly the
    conversions this column exists to record.
    """
    codes = tej['標的代號'].str.replace(r'\.TWO?$', '', regex=True)
    is_token = ~codes.str.fullmatch(r'\d{4,6}', na=False)
    if not is_token.any():
        return codes
    dim_codes = dim_warrant[WARRANT_KEY + ['target_stock_id']].rename(columns={'target_stock_id': 'dim_code'})
    tokens = (
        tej.loc[is_token, WARRANT_KEY].assign(token=codes[is_token])
        .merge(dim_codes, on=WARRANT_KEY, how='left')
        .dropna(subset=['dim_code'])
    )
    token_map = tokens.groupby('token')['dim_code'].agg(lambda names: names.mode().iloc[0])
    print(f'TEJ index tokens mapped to MOPS codes: {token_map.to_dict()}')
    return codes.where(~is_token, codes.map(token_map).fillna(codes))


def events_from_tej(tej: pd.DataFrame, dim_warrant: pd.DataFrame) -> pd.DataFrame:
    """TEJ's per-state rows -> events (rank 1).

    Dates are kept as TEJ records them: the ``上市櫃`` row on the *issue*
    date (2-4 days before listing) and a reset on the day it took effect,
    which can precede listing. Both are real states
    -- the issue-time strike, then the reset one that traded from day one --
    and whether a date is tradable is the reader's concern (``list_date`` is
    on every row). TEJ labels every strike move ``重設`` regardless of whether
    it was a genuine reset or an ex-rights adjustment, so the label is mapped
    to a neutral ``change`` rather than pretending to know which.
    """
    tej = add_warrant_key(tej, '權證代號', '權證簡稱')
    tej['target_stock_id'] = tej_underlying_codes(tej, dim_warrant)

    events = pd.DataFrame({
        'warrant_id': tej['warrant_id'],
        'warrant_name': tej['warrant_name'],
        'effective_date': tej['年月日'],
        'sequence': pd.to_numeric(tej['序號'], errors='coerce').fillna(1).astype(int),
        'strike': pd.to_numeric(tej['履約價(元)'], errors='coerce'),
        'ratio': pd.to_numeric(tej['行使比例'], errors='coerce'),
        'target_stock_id': tej['target_stock_id'],
        'target_name': tej['標的簡稱'],
        'event_type': tej['事件說明'].map({'上市櫃': 'issuance'}).fillna('change'),
    })
    events['source'] = 'tej'
    is_issuance = events['event_type'] == 'issuance'
    print(f'TEJ events: {len(events):,} ({int(is_issuance.sum()):,} issuance)')
    return events


# (event_type, published table, pending table, effective date, strike, cap, floor)
MOPS_STRIKE_SOURCES = [
    (
        'adjustment',
        'warrant_strike_ratio_adjustment',
        'warrant_pending_adjustment',
        'adjustment_effective_date',
        'latest_strike',
        'latest_cap',
        'latest_floor',
    ),
    (
        'reset',
        'warrant_strike_ratio_reset',
        'warrant_pending_reset',
        'reset_effective_date',
        'reset_strike',
        'reset_cap',
        'reset_floor',
    ),
]


def events_from_mops_strike(
    cache_directory: Path,
    seed_end_date: pd.Timestamp,
    tej_keys: set,
) -> pd.DataFrame:
    """t95sb02 / t95sb03 rows (rank 3): from the frozen TEJ seed's last day
    on, plus everything for warrants TEJ never covers (bull/bear, the
    extendable ones, MOPS-only listings), whose only strike history is here.
    """
    frames = []
    for (
        event_type,
        published,
        pending,
        date_column,
        strike_column,
        cap_column,
        floor_column,
    ) in MOPS_STRIKE_SOURCES:
        # Announced but not yet in force rows carry their own 生效日, which is
        # what keeps the change off the days before it (see the resource's docstring).
        tables = [
            table
            for table in (
                load_raw_table(cache_directory, published),
                load_raw_table(cache_directory, pending),
            )
            if table is not None
        ]
        if not tables:
            continue
        rows = pd.concat(tables, ignore_index=True)
        if not len(rows):
            continue
        rows = add_warrant_key(rows, 'warrant_id', 'warrant_name')
        frames.append(pd.DataFrame({
            'warrant_id': rows['warrant_id'],
            'warrant_name': rows['warrant_name'],
            'effective_date': rows[date_column],
            'sequence': 1,
            'strike': rows[strike_column],
            'ratio': rows['latest_ratio'],
            'cap': rows.get(cap_column),
            'floor': rows.get(floor_column),
            'target_stock_id': rows['target_stock_id'],
            'target_name': rows['target_name'],
            'event_type': event_type,
        }))
    if not frames:
        return empty_events()

    events = pd.concat(frames, ignore_index=True)
    # Inclusive: the seed's last day may be partial (an export pulled
    # mid-day), and a same-day collision is settled by source_rank anyway.
    after_seed = events['effective_date'] >= seed_end_date
    tej_covered = pd.Series(
        [key in tej_keys for key in zip(events['warrant_id'], events['warrant_name'])],
        index=events.index,
    )
    events = events[after_seed | ~tej_covered]
    events['source'] = 'mops_strike'
    print(f'MOPS strike events: {int(after_seed.sum()):,} from {seed_end_date.date()},'
          f' {int((~after_seed & ~tej_covered).sum()):,} earlier for warrants outside TEJ')
    return events


def events_from_announcements(cache_directory: Path) -> pd.DataFrame:
    """Expiry changes dated by their announcement, not their effect (rank 2)."""
    announcements = load_raw_table(cache_directory, 'warrant_announcement')
    if announcements is None:
        print('WARNING: no warrant_announcement table -- expiry changes will be undated')
        return empty_events()

    announcements = add_warrant_key(announcements, 'warrant_id', 'warrant_name')
    announcements = announcements[announcements['announced_expiry_date'].notna()]
    # Only early-termination filenames encode the new expiry. Extension
    # documents use the same field for a different purpose and are excluded.
    announcements = announcements[announcements['announcement_type'] == 'early_termination']

    # The announcement names the new expiry but not the new last trading day.
    # The settlement gap between them is fixed per warrant, so the last trading
    # day is carried on the same row by shifting the prevailing gap; without it
    # the row would forward-fill the pre-termination last trading day, which now
    # falls after expiry.
    events = pd.DataFrame({
        'warrant_id': announcements['warrant_id'],
        'warrant_name': announcements['warrant_name'],
        'effective_date': announcements['announcement_date'],
        # Sequence 2, after any strike event on the same day: this row carries
        # only the expiry, and on a shared (date, sequence) the dedup would
        # keep it over the adjustment and forward-fill a stale strike.
        'sequence': 2,
        'exercise_end_date': announcements['announced_expiry_date'],
        'event_type': 'expiry_change',
    })
    events['source'] = 'mops_announcement'
    events = events.drop_duplicates(subset=WARRANT_KEY + ['effective_date'], keep='last')
    print(f'announcement expiry events: {len(events):,}')
    return events


def scheduled_expiry_dates(cache_directory: Path, dim_warrant: pd.DataFrame) -> pd.DataFrame:
    """Per warrant, the maturity written into it at issuance.

    A warrant terminated early was priced against its *original* maturity until
    the announcement, so the issuance row must not carry the post-termination
    date that the dimension table (correctly) holds as current. TEJ records the
    scheduled last trading day, which is the only surviving record of the
    original schedule; the expiry sits a fixed 2-4 days after it, so the gap is
    taken from the warrant's own actual pair.

    Falls back to the dimension table's current dates where TEJ has no row --
    correct for warrants that were never rescheduled.
    """
    fallback = dim_warrant[WARRANT_KEY + ['exercise_end_date', 'last_trade_date']].rename(
        columns={
            'exercise_end_date': 'scheduled_exercise_end_date',
            'last_trade_date': 'scheduled_last_trade_date',
        }
    )
    fallback['current_exercise_end_date'] = fallback['scheduled_exercise_end_date']
    tej_basic_path = find_tej_seed(TEJ_BASIC_INFO_GLOB)
    if tej_basic_path is None:
        return fallback

    tej = pd.read_parquet(
        tej_basic_path, columns=['權證代號', '權證名', '預定最後交易日', '最後交易日', '到期日']
    )
    for column in ['預定最後交易日', '最後交易日', '到期日']:
        tej[column] = pd.to_datetime(tej[column])
    tej = add_warrant_key(tej, '權證代號', '權證名')
    tej = tej.drop_duplicates(subset=WARRANT_KEY, keep='first')

    # 預定最後交易日 can differ from the actual last trading day due to
    # closures or schedule arithmetic. Treat only shifts beyond a week as
    # reschedules; otherwise use actual dates as the schedule.
    # Limitation: detecting a termination within a week requires announcements.
    settlement_gap = tej['到期日'] - tej['最後交易日']
    is_rescheduled = (tej['最後交易日'] - tej['預定最後交易日']).dt.days < -7
    scheduled_last_trade_date = tej['預定最後交易日'].where(is_rescheduled, tej['最後交易日'])
    scheduled = pd.DataFrame({
        'warrant_id': tej['warrant_id'],
        'warrant_name': tej['warrant_name'],
        'tej_scheduled_last_trade_date': scheduled_last_trade_date,
        'tej_scheduled_exercise_end_date': scheduled_last_trade_date + settlement_gap,
    })

    out = fallback.merge(scheduled, on=WARRANT_KEY, how='left')
    has_schedule = out['tej_scheduled_last_trade_date'].notna()
    out.loc[has_schedule, 'scheduled_last_trade_date'] = out.loc[
        has_schedule, 'tej_scheduled_last_trade_date'
    ]
    out.loc[has_schedule, 'scheduled_exercise_end_date'] = out.loc[
        has_schedule, 'tej_scheduled_exercise_end_date'
    ]
    rescheduled_count = int(
        (out['scheduled_exercise_end_date'] != out['current_exercise_end_date']).sum()
    )
    print(f'warrants whose scheduled expiry differs from current: {rescheduled_count:,}')
    return out.drop(
        columns=[
            'tej_scheduled_last_trade_date',
            'tej_scheduled_exercise_end_date',
            'current_exercise_end_date',
        ]
    )


def first_priced_event(events: pd.DataFrame) -> pd.DataFrame:
    """Per warrant, the earliest event carrying both a strike and a ratio."""
    priced = events.dropna(subset=['strike', 'ratio', 'effective_date'])
    priced = priced.sort_values(WARRANT_KEY + ['effective_date', 'sequence'])
    return (
        priced.groupby(WARRANT_KEY, as_index=False)
        .head(1)[WARRANT_KEY + ['strike', 'ratio', 'effective_date']]
        .rename(columns={
            'strike': 'later_strike',
            'ratio': 'later_ratio',
            'effective_date': 'later_date',
        })
    )


def events_from_dim(
    dim_warrant: pd.DataFrame,
    covered_keys: set[str],
    other_events: pd.DataFrame,
) -> pd.DataFrame:
    """Issuance rows for warrants no other source covers (rank 4).

    New listings crawled after the TEJ seed was frozen, plus the 2019 cohort
    that predates the seed window. ``original_strike`` is the issuance strike
    and ``alloc_qty_per_1k / 1000`` the issuance ratio.

    ``t90sb01`` publishes only the *latest* allocation, so on a warrant that
    has been adjusted since it listed that ratio is not the one it issued
    with. An ex-rights adjustment conserves ``strike x ratio``, so where a
    later event is known the issuance ratio is backed out from it instead
    (``strike' x ratio' / original_strike``); ``validate`` measures how well
    the invariant holds. A warrant with no later event keeps the published
    allocation, which for it is both the issuance and the current ratio.
    """
    is_covered = [
        key in covered_keys
        for key in zip(dim_warrant['warrant_id'], dim_warrant['warrant_name'])
    ]
    uncovered = dim_warrant[~pd.Series(is_covered, index=dim_warrant.index)]
    events = pd.DataFrame({
        'warrant_id': uncovered['warrant_id'],
        'warrant_name': uncovered['warrant_name'],
        'effective_date': uncovered['list_date'],
        'sequence': 0,
        'strike': uncovered['original_strike'],
        'ratio': uncovered['alloc_qty_per_1k'] / 1000.0,
        'cap': uncovered['original_cap'],
        'floor': uncovered['original_floor'],
        # The dimension table's underlying is post-conversion; a warrant
        # outside TEJ has no issuance-time record of it anywhere.
        'target_stock_id': uncovered['target_stock_id'],
        'target_name': uncovered['target_name'],
        'event_type': 'issuance',
    })
    events['source'] = 'dim_synthesised'

    priced = first_priced_event(other_events)
    events = events.merge(priced, on=WARRANT_KEY, how='left')
    is_backed_out = (
        events['later_strike'].notna()
        & events['later_ratio'].notna()
        & (events['strike'] > 0)
        & (events['later_date'] > events['effective_date'])
    )
    events.loc[is_backed_out, 'ratio'] = (
        events.loc[is_backed_out, 'later_strike']
        * events.loc[is_backed_out, 'later_ratio']
        / events.loc[is_backed_out, 'strike']
    ).round(3)  # t90sb01 publishes the allocation as an integer per 1000 units
    events = events.drop(columns=['later_strike', 'later_ratio', 'later_date'])
    print(f'synthesised issuance for uncovered warrants: {len(events):,}'
          f' ({int(is_backed_out.sum()):,} ratios backed out of a later event)')
    return events



# TODO: MOPS's t90sb01 applies an ex-dividend adjustment a day before it takes
# effect, so a diff dated at the crawl is a day early. Plan:
# a daily TWSE/TPEx OpenAPI snapshot resource as the diff's reference (the
# exchange applies on the ex-date), plus t95sb02's future-effective rows for
# the true dates; MOPS's snapshot becomes verify's reference instead.
def events_from_snapshot_diff(
    snapshot: pd.DataFrame | None,
    crawl_date: pd.Timestamp,
    dim_warrant: pd.DataFrame,
    events: pd.DataFrame,
    scheduled_expiry: pd.DataFrame,
) -> pd.DataFrame:
    """Close the gap between the last known event and what MOPS reports today.

    A warrant whose strike moved in a period no event source covers would
    otherwise carry a stale strike right up to its current row. The snapshot knows today's terms
    but not when they changed, so the event is dated at the crawl: the change is
    recorded no later than it actually happened, and never invented earlier.
    """
    if snapshot is None:
        return empty_events()

    snapshot = add_warrant_key(snapshot, 'warrant_id', 'warrant_name')
    current_terms = snapshot[
        WARRANT_KEY + ['latest_strike', 'alloc_qty_per_1k', 'exercise_end_date', 'last_trade_date']
    ]
    current_terms = current_terms.drop_duplicates(subset=WARRANT_KEY, keep='last')
    # A warrant that has not listed yet is not trading on anything, and MOPS's
    # snapshot carries a provisional strike for it; there is nothing to diff.
    not_yet_listed = dim_warrant.loc[dim_warrant['list_date'] > crawl_date, WARRANT_KEY]
    current_terms = current_terms.merge(not_yet_listed, on=WARRANT_KEY, how='left', indicator=True)
    current_terms = current_terms[current_terms['_merge'] == 'left_only'].drop(columns=['_merge'])

    # The last known state per warrant, with expiry forward-filled the way
    # chain_events will do it: a strike-only event carries no expiry.
    ordered = events.sort_values(WARRANT_KEY + ['effective_date', 'sequence'])
    ordered['exercise_end_date'] = pd.to_datetime(ordered['exercise_end_date'], errors='coerce')
    ordered['exercise_end_date'] = ordered.groupby(WARRANT_KEY)['exercise_end_date'].ffill()
    last_events = (
        ordered.groupby(WARRANT_KEY, as_index=False)
        .last()[WARRANT_KEY + ['strike', 'ratio', 'exercise_end_date']]
        .rename(columns={'exercise_end_date': 'known_exercise_end_date'})
    )
    compared = current_terms.merge(last_events, on=WARRANT_KEY, how='inner')
    compared['snapshot_ratio'] = compared['alloc_qty_per_1k'] / 1000.0
    compared['exercise_end_date'] = pd.to_datetime(compared['exercise_end_date'])
    # Strike events carry no expiry, so for most warrants the last known
    # expiry is the one the chain will seed the first row with: the schedule.
    compared = compared.merge(
        scheduled_expiry[WARRANT_KEY + ['scheduled_exercise_end_date']], on=WARRANT_KEY, how='left'
    )
    compared['known_exercise_end_date'] = compared['known_exercise_end_date'].fillna(
        compared['scheduled_exercise_end_date']
    )

    strike_moved = (compared['latest_strike'] - compared['strike']).abs() > 0.01
    ratio_moved = (compared['snapshot_ratio'] - compared['ratio']).abs() > 1e-4
    # An expiry the snapshot reports that no announcement explained: MOPS
    # posts a termination on an underlying's delisting under 終止上市/上櫃
    # rather than 提前終止, which the announcement resource does not read.
    expiry_moved = (
        compared['known_exercise_end_date'].notna()
        & (compared['exercise_end_date'] != compared['known_exercise_end_date'])
    )
    unexplained = compared[strike_moved | ratio_moved | expiry_moved]
    if unexplained.empty:
        return empty_events()

    # Dated at the crawl, or at expiry if the warrant has already expired:
    # the snapshot still lists it for a few days after, and an ex-dividend
    # between its last trading day and expiry (t95sb02 rarely records those)
    # is exactly what its settlement terms then reflect.
    effective_date = pd.to_datetime(unexplained['exercise_end_date']).clip(upper=crawl_date)
    # Restate only what moved. A warrant whose expiry changed still has the
    # strike it had yesterday, and MOPS applies an ex-dividend adjustment to
    # the snapshot a day early -- carrying that strike onto an expiry-only diff
    # would date the adjustment a day before it is true.
    terms_moved = (strike_moved | ratio_moved)[unexplained.index]
    expiry_only = ~terms_moved
    diff_events = pd.DataFrame({
        'warrant_id': unexplained['warrant_id'],
        'warrant_name': unexplained['warrant_name'],
        'effective_date': effective_date.fillna(crawl_date),
        'sequence': 9,
        'strike': unexplained['latest_strike'].where(terms_moved),
        'ratio': unexplained['snapshot_ratio'].where(terms_moved),
        'exercise_end_date': unexplained['exercise_end_date'].where(expiry_moved[unexplained.index]),
        'last_trade_date': pd.to_datetime(unexplained['last_trade_date']).where(expiry_moved[unexplained.index]),
        'event_type': 'snapshot_diff',
    })
    diff_events['source'] = 'mops_snapshot'
    expiry_only_count = int(expiry_only.sum())
    print(f'snapshot diffs no event explained: {len(diff_events):,}'
          f' ({expiry_only_count:,} expiry only;'
          f' {int((effective_date < crawl_date).sum()):,} on an already-expired warrant, dated at its expiry)')
    return diff_events


def events_from_target_closure(
    dim_warrant: pd.DataFrame,
    events: pd.DataFrame,
    crawl_date: pd.Timestamp,
) -> pd.DataFrame:
    """Close the underlying the chain ends on against the dimension table.

    The dimension table holds the underlying of MOPS's latest record of the
    warrant: the snapshot's for a live one, the delisted sweep's for an
    expired one, both post-conversion. Where the last event still names
    another code, the move fell where no event source dates it -- a
    warrant outside TEJ whose conversion row MOPS never posted, or a
    conversion between the seed's end and the first crawl -- and this
    records it no later than it happened: at the crawl for a live warrant,
    at expiry for an expired one, exactly as
    :func:`events_from_snapshot_diff` dates a strike.
    """
    ordered = events.dropna(subset=['effective_date']).sort_values(WARRANT_KEY + ['effective_date', 'sequence'])
    last_known = (
        ordered.groupby(WARRANT_KEY, sort=False)['target_stock_id'].last()
        .rename('known_target_stock_id')
        .reset_index()
    )
    compared = dim_warrant[WARRANT_KEY + ['target_stock_id', 'target_name', 'exercise_end_date']].merge(
        last_known, on=WARRANT_KEY, how='inner'
    )
    moved = compared[
        compared['known_target_stock_id'].notna()
        & (compared['known_target_stock_id'] != compared['target_stock_id'])
    ]
    if moved.empty:
        return empty_events()
    closure = pd.DataFrame({
        'warrant_id': moved['warrant_id'],
        'warrant_name': moved['warrant_name'],
        'effective_date': pd.to_datetime(moved['exercise_end_date']).clip(upper=crawl_date).fillna(crawl_date),
        'sequence': 9,
        'target_stock_id': moved['target_stock_id'],
        'target_name': moved['target_name'],
        'event_type': 'target_change',
    })
    closure['source'] = 'dim_closure'
    moves = moved.groupby(['known_target_stock_id', 'target_stock_id', 'target_name']).size()
    print(f'underlying closed against the dimension table: {len(closure):,} warrants')
    for (old_code, new_code, new_name), count in moves.items():
        print(f'  {old_code} -> {new_code} {new_name}: {count:,}')
    return closure


def underlying_names(dim_warrant: pd.DataFrame, events: pd.DataFrame) -> pd.Series:
    """Code -> name, MOPS's spelling first, TEJ's where MOPS never named the code."""
    mops_names = pd.concat([
        dim_warrant[['target_stock_id', 'target_name']],
        events.loc[events['source'] != 'tej', ['target_stock_id', 'target_name']],
    ]).dropna()
    tej_names = events.loc[events['source'] == 'tej', ['target_stock_id', 'target_name']].dropna()
    names = pd.concat([mops_names, tej_names]).drop_duplicates(subset=['target_stock_id'], keep='first')
    return names.set_index('target_stock_id')['target_name']


def trading_days_before(dates: pd.Series, days: int) -> pd.Series:
    """Each date moved back by ``days`` Taiwan trading days.

    The calendar is FinMind's ``TaiwanStockTradingDate``, which the exchange
    publishes for the whole year ahead -- plain business days miss the
    holidays. Dates the calendar does not reach (next year's before it is
    published) and any failure to fetch it fall back to business days.
    """
    business_days = dates - days * pd.offsets.BDay()
    if dates.empty:
        return business_days
    try:
        from data_sdk.wrappers.finmind_broker_wrapper import FinMindWrapper
        trading_dates = FinMindWrapper().get_trading_dates(
            (dates.min() - pd.Timedelta(days=30)).strftime('%Y-%m-%d'),
            dates.max().strftime('%Y-%m-%d'),
        )
        calendar = np.sort(pd.to_datetime(trading_dates['date']).to_numpy())
    except Exception as error:
        print(f'WARNING: no trading calendar ({error}); last trading days use business days')
        return business_days
    if len(calendar) == 0:
        print('WARNING: empty trading calendar; last trading days use business days')
        return business_days

    positions = np.searchsorted(calendar, dates.to_numpy(), side='left')
    is_covered = (dates.to_numpy() <= calendar[-1]) & (positions >= days)
    shifted = calendar[np.clip(positions - days, 0, None)]
    covered_dates = pd.Series(shifted, index=dates.index)
    is_covered_series = pd.Series(is_covered, index=dates.index)
    uncovered_count = int((~is_covered_series).sum())
    if uncovered_count:
        print(f'{uncovered_count:,} dates beyond the trading calendar use business days')
    return covered_dates.where(is_covered_series, business_days)


def chain_events(
    events: pd.DataFrame,
    dim_warrant: pd.DataFrame,
    scheduled_expiry: pd.DataFrame,
) -> pd.DataFrame:
    """Order, de-duplicate and forward-fill events into point-in-time rows.

    An undated event is kept only when it is a synthesised issuance -- the
    warrant's list_date is unknown (see build_basic_info), so its single row
    is undated too. It still gives the warrant its current terms; an as-of
    lookup never matches it, which is the right answer for "listed when?".
    """
    is_dated = events['effective_date'].notna()
    is_undated_issuance = ~is_dated & (events['source'] == 'dim_synthesised')
    events = events[is_dated | is_undated_issuance].dropna(subset=WARRANT_KEY)
    known_warrants = set(zip(dim_warrant['warrant_id'], dim_warrant['warrant_name']))
    is_known = [key in known_warrants for key in zip(events['warrant_id'], events['warrant_name'])]
    events = events[pd.Series(is_known, index=events.index)]

    # Events before list_date are kept: the issuance on the issue date and any
    # reset/adjustment applied between issue and first trade are real states.
    # Only the count is reported, as a tripwire for join noise on a recycled code.
    listing_dates = dim_warrant[WARRANT_KEY + ['list_date']]
    with_listing = events.merge(listing_dates, on=WARRANT_KEY, how='left')
    before_listing = with_listing['effective_date'] < with_listing['list_date']
    print(f'events dated before list_date (kept): {int(before_listing.sum()):,}')

    # Two rows can share a moment for two different reasons, resolved in order:
    #   1. A reset-type warrant's strike is reset on its own listing day, so the
    #      issuance row and the reset land on the same date. The reset is the
    #      state that actually traded, so a non-issuance row outranks issuance.
    #   2. The same event reaches us from two sources near the seed boundary;
    #      the lower SOURCE_RANK wins.
    events['is_issuance'] = events['event_type'] == 'issuance'
    # na_position: an issuance whose list_date is unknown (see
    # build_basic_info) is undated and must still open the chain.
    events = events.sort_values(
        WARRANT_KEY + ['effective_date', 'sequence', 'is_issuance', 'source_rank'],
        na_position='first',
    )
    events = events.drop_duplicates(
        subset=WARRANT_KEY + ['effective_date', 'sequence'], keep='first'
    )
    events = events.drop(columns=['is_issuance'])

    # Seed each warrant's first row with the maturity it was issued with, so
    # that forward-filling carries the originally scheduled expiry until an
    # announcement moves it -- rather than back-dating today's expiry to
    # issuance, which would misprice every early-terminated warrant.
    events = events.merge(scheduled_expiry, on=WARRANT_KEY, how='left')
    is_first_row = ~events.duplicated(subset=WARRANT_KEY, keep='first')
    events.loc[is_first_row, 'exercise_end_date'] = events.loc[
        is_first_row, 'exercise_end_date'
    ].fillna(events.loc[is_first_row, 'scheduled_exercise_end_date'])
    events.loc[is_first_row, 'last_trade_date'] = events.loc[
        is_first_row, 'last_trade_date'
    ].fillna(events.loc[is_first_row, 'scheduled_last_trade_date'])
    events = events.drop(columns=['scheduled_exercise_end_date', 'scheduled_last_trade_date'])

    # Each row is a full state snapshot: an event that only moved the expiry
    # inherits the prevailing strike, and vice versa.
    grouped = events.groupby(WARRANT_KEY, sort=False)
    for column in STATE_COLUMNS:
        events[column] = grouped[column].ffill()

    # An expiry_change carries only the new expiry, so its last_trade_date was
    # forward-filled from before the change and can now sit after expiry. MOPS
    # publishes the new last trading day with the termination (the dimension
    # table holds it), so that is taken where the dimension's expiry is the
    # same one. Otherwise use two trading days before the new expiry.
    # Where both exist, compare them as a check on the calendar rule.
    last_trade_date_is_stale = events['last_trade_date'] > events['exercise_end_date']
    stale = events.loc[last_trade_date_is_stale, WARRANT_KEY + ['exercise_end_date']]
    published = stale.merge(
        dim_warrant[WARRANT_KEY + ['exercise_end_date', 'last_trade_date']].rename(
            columns={
                'exercise_end_date': 'published_exercise_end_date',
                'last_trade_date': 'published_last_trade_date',
            }
        ),
        on=WARRANT_KEY,
        how='left',
    ).set_axis(stale.index)
    expiry_is_published = published['exercise_end_date'] == published['published_exercise_end_date']
    published_last_trade_date = published['published_last_trade_date'].where(expiry_is_published)
    derived_last_trade_date = trading_days_before(stale['exercise_end_date'], days=2)
    both_known = published_last_trade_date.notna()
    disagree = both_known & (published_last_trade_date != derived_last_trade_date)
    print(f'last trading day after an expiry change: {int(both_known.sum()):,} from MOPS,'
          f' {int((~both_known).sum()):,} derived; {int(disagree.sum()):,} where MOPS and'
          ' the calendar rule disagree')
    events.loc[last_trade_date_is_stale, 'last_trade_date'] = published_last_trade_date.fillna(
        derived_last_trade_date
    )

    # A handful of TEJ rows are dated a day or two after the warrant expired.
    # An as-of lookup filters on expiry anyway, so such a row can never be
    # returned; dropping it keeps the table self-consistent. A warrant's first
    # row is always kept, so no warrant loses its history entirely.
    is_first_event = ~events.duplicated(subset=WARRANT_KEY, keep='first')
    is_after_expiry = events['effective_date'] > events['exercise_end_date']
    droppable = is_after_expiry & ~is_first_event
    if droppable.any():
        print(f'dropped {int(droppable.sum()):,} events dated after exercise_end_date')
    events = events[~droppable].copy()

    events['target_name'] = events['target_stock_id'].map(underlying_names(dim_warrant, events))
    history = events.merge(
        dim_warrant[WARRANT_KEY + STATIC_COLUMNS], on=WARRANT_KEY, how='left'
    )
    # "Current" means in force today, not the last row: an adjustment announced
    # for a future date sits in the table with that date, and until it arrives
    # the terms that trade are the ones before it.
    today = pd.Timestamp(taiwan_today())
    in_force = history['effective_date'].isna() | (history['effective_date'] <= today)
    current_rows = history[in_force].groupby(WARRANT_KEY, sort=False).tail(1).index
    # A warrant that lists tomorrow has no row in force yet, and every warrant
    # must have exactly one: give it its issuance row. That is also what the
    # exchange publishes for an unlisted warrant -- a reset or an adjustment
    # dated on the listing day has not happened yet either.
    not_yet_listed = ~history[WARRANT_KEY].agg(tuple, axis=1).isin(
        set(map(tuple, history.loc[current_rows, WARRANT_KEY].values))
    )
    current_rows = current_rows.union(
        history[not_yet_listed].groupby(WARRANT_KEY, sort=False).head(1).index
    )
    history['is_current'] = history.index.isin(current_rows)
    pending = int((~in_force).sum())
    if pending:
        print(f'rows effective after today: {pending:,}'
              f' ({int(not_yet_listed.groupby([history["warrant_id"], history["warrant_name"]]).any().sum()):,}'
              ' warrants not yet listed)')
    return history


def build_history(cache_directory: Path, dim_warrant: pd.DataFrame) -> pd.DataFrame:
    # Bull/bear certificates and extendable warrants are not a trading target
    # and their terms (knock-out barrier and financing cost; a maturity that
    # moves by extension) do not fit this table's model. They stay in the
    # dimension table, flagged, and are left out of the history entirely.
    is_excluded = (
        dim_warrant['is_bull_bear'].fillna(False).astype(bool)
        | dim_warrant['is_extendable'].fillna(False).astype(bool)
    )
    print(f'excluding {int(is_excluded.sum()):,} bull/bear and extendable warrants from the history')
    dim_warrant = dim_warrant[~is_excluded]

    tej_adjustment_path = find_tej_seed(TEJ_ADJUSTMENT_GLOB)
    frames = []
    seed_end_date = pd.Timestamp.min

    if tej_adjustment_path is not None:
        print(f'TEJ seed: {tej_adjustment_path}')
        tej = pd.read_parquet(
            tej_adjustment_path,
            columns=['權證代號', '權證簡稱', '年月日', '序號', '事件說明', '履約價(元)', '行使比例', '標的代號', '標的簡稱'],
        )
        tej['年月日'] = pd.to_datetime(tej['年月日'])
        # Coverage ends at the raw TEJ date, not a transformed event date:
        # those can run past raw coverage and suppress MOPS events.
        seed_end_date = tej['年月日'].max()
        frames.append(events_from_tej(tej, dim_warrant))
        print(f'TEJ seed ends {seed_end_date.date()}')
    else:
        print(f'WARNING: no {TEJ_ADJUSTMENT_GLOB} in the TEJ seed dir'
              ' -- history will be MOPS-only (~5% coverage)')

    tej_keys = set(zip(frames[0]['warrant_id'], frames[0]['warrant_name'])) if frames else set()
    frames.append(events_from_announcements(cache_directory))
    frames.append(events_from_mops_strike(cache_directory, seed_end_date, tej_keys))

    events = pd.concat([frame for frame in frames if len(frame)], ignore_index=True)
    for column in STATE_COLUMNS:
        if column not in events.columns:
            events[column] = pd.NA

    # Only an issuance event covers a warrant. A change or a reset describes a
    # state the warrant moved to, not the one it was issued with, so without a
    # synthesised issuance, warrants starting with a change or reset would
    # have no terms before that event.
    is_issuance = events['event_type'] == 'issuance'
    covered_keys = set(zip(events.loc[is_issuance, 'warrant_id'], events.loc[is_issuance, 'warrant_name']))
    events = pd.concat([events, events_from_dim(dim_warrant, covered_keys, events)], ignore_index=True)
    scheduled_expiry = scheduled_expiry_dates(cache_directory, dim_warrant)
    snapshot = load_raw_table(cache_directory, 'warrant_active_snapshot')
    crawl_date = (
        pd.to_datetime(snapshot['crawl_date']).max() if snapshot is not None else pd.Timestamp(taiwan_today())
    )
    events = pd.concat(
        [events, events_from_snapshot_diff(snapshot, crawl_date, dim_warrant, events, scheduled_expiry)],
        ignore_index=True,
    )
    events = pd.concat(
        [events, events_from_target_closure(dim_warrant, events, crawl_date)], ignore_index=True
    )

    for column in ['exercise_end_date', 'last_trade_date']:
        events[column] = pd.to_datetime(events[column], errors='coerce')
    unranked = set(events['source']) - SOURCE_RANK.keys()
    if unranked:
        raise ValueError(f'sources without a SOURCE_RANK: {sorted(unranked)}')
    # Next to `source`, where the history has always carried it.
    events = events.drop(columns='source_rank', errors='ignore')
    events.insert(
        events.columns.get_loc('source') + 1, 'source_rank', events['source'].map(SOURCE_RANK).astype(int)
    )
    return chain_events(events, dim_warrant, scheduled_expiry)


def main() -> None:
    parser = argparse.ArgumentParser(description='Build point-in-time warrant term history.')
    parser.add_argument('--cache-dir', default=None,
                        help=f'default: $DATA_SDK_WARRANT_CACHE_PATH or {DEFAULT_CACHE_PATH}')
    parser.add_argument('--out', default=None)
    arguments = parser.parse_args()

    cache_directory = cache_dir(arguments.cache_dir)
    out_path = (
        Path(arguments.out) if arguments.out else cache_directory / 'warrant_history.parquet'
    )

    dim_warrant = pd.read_parquet(cache_directory / 'warrant_basic_info.parquet')
    history = build_history(cache_directory, dim_warrant)
    history.to_parquet(out_path, index=False)
    print(f'wrote {len(history):,} rows for {len(history[WARRANT_KEY].drop_duplicates()):,} warrants -> {out_path}')


if __name__ == '__main__':
    main()
