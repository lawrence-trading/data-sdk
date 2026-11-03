"""Build ``<cache-dir>/warrant_basic_info.parquet`` -- the warrant dimension table.

One row per warrant, holding the attributes that do not change over its life
plus its *current* terms. Point-in-time strike/ratio/expiry live in
``warrant_history.parquet`` instead (see :mod:`build_history`).

Three sources, in precedence order per field:

1. ``mops_raw/warrant_basic_info`` -- the base population and every issuance-time
   field (``original_strike``, ``issuer``, ``target_stock_id``, ...).
2. ``mops_raw/warrant_active_snapshot`` -- supplies every live warrant (the
   basic-info resource records a warrant only once it has expired) and
   overwrites the *mutable* term fields (``exercise_end_date``,
   ``last_trade_date``, ``latest_*``) and the underlying (``target_stock_id``,
   ``target_name``, which a share conversion or merger moves -- 中光電 5371 ->
   中光電投控 3718 on 2026-09-03) of anything it covers. The snapshot has no
   cursor and is replaced whole every run, so it always reflects MOPS today.
3. the TEJ Pro export (``$DATA_SDK_TEJ_WARRANTS_PATH``) -- repairs ``list_date`` /
   ``exercise_start_date``, which MOPS itself serves corrupted (~51,500 rows,
   nearly all OTC warrants expired 2011-2019, with both set to the literal
   2023-12-26; confirmed by live-replaying the MOPS query).
4. a frozen FinMind warrant summary (``$DATA_SDK_FINMIND_WARRANTS_PATH``) --
   the same repair for the ~48,700 corrupted rows that predate TEJ's window.
   What neither covers is left null rather than wrong.

The key is ``(warrant_id, warrant_name)`` with ``warrant_id`` stripped of its
recycling suffix. It is what joins to TEJ too: ``exercise_end_date`` shifts on
early termination and ``list_date`` is the very field being repaired, so the
name is the only other stable identifier both vendors share.

Usage:
    python -m data_sdk.crawlers.warrant.build_basic_info --cache-dir cache
"""
from __future__ import annotations

import argparse
import glob
from pathlib import Path

import pandas as pd

from . import (
    DEFAULT_CACHE_PATH,
    FINMIND_SUMMARY_GLOB,
    TEJ_BASIC_INFO_GLOB,
    cache_directory as cache_dir,
    find_finmind_seed,
    find_tej_seed,
)

#: Primary key. ``warrant_id`` is the listing code (6 characters; 4 digits
#: before 2005) with any recycling suffix stripped; it is NOT unique on its own (MOPS reuses a code
#: once the previous warrant expires -- ``030001`` has been three different
#: warrants), so the name is part of the key.
WARRANT_KEY = ['warrant_id', 'warrant_name']

#: The literal date MOPS serves in place of the real list_date /
#: exercise_start_date on its corrupted rows.
CORRUPTED_MOPS_DATE = pd.Timestamp('2023-12-26')

DATE_COLUMNS = [
    'list_date',
    'exercise_start_date',
    'last_trade_date',
    'exercise_end_date',
]
# Fields the snapshot is allowed to overwrite: everything that can change after
# issuance. original_* and the identity columns are deliberately not here.
# The underlying is mutable too: a share conversion or merger moves every
# live warrant on it to the new code (中光電 5371 -> 中光電投控 3718, 精星 8183
# -> 精成科 6191, 三商壽 2867 -> 玉山金 2884, all in 2026), and MOPS updates
# only the live ones -- an expired warrant keeps the underlying it expired on,
# which is what the basic-info sweep records. So after the overlay every row
# carries the underlying of MOPS's latest record of it; the point-in-time
# version is the history's target_stock_id state column (build_history).
MUTABLE_TERM_COLUMNS = [
    'exercise_end_date',
    'last_trade_date',
    'latest_strike',
    'latest_cap',
    'latest_floor',
    'alloc_qty_per_1k',
    'target_stock_id',
    'target_name',
]


def load_raw_table(cache_directory: Path, table_name: str) -> pd.DataFrame | None:
    files = sorted(
        glob.glob(str(cache_directory / 'mops_raw' / 'mops_raw' / table_name / '*.parquet'))
    )
    if not files:
        return None
    frame = pd.concat([pd.read_parquet(path) for path in files], ignore_index=True)
    frame = frame.drop(columns=[column for column in frame.columns if column.startswith('_dlt_')])
    # Crawls before 2026-09 stored a blank cell as the text 'nan'.
    frame = frame.replace({'nan': None})
    for column in frame.columns:
        if column.endswith('_date'):
            frame[column] = pd.to_datetime(frame[column])
    return frame


def add_warrant_key(frame: pd.DataFrame, id_column: str, name_column: str) -> pd.DataFrame:
    """Normalise the identity columns to ``(warrant_id, warrant_name)``.

    A letter after the traded code is a recycling suffix, not part of it, and
    each vendor assigns its own (MOPS ``703055b`` vs TEJ ``703055Y``), so it
    is stripped. Traded codes are 6 characters from 2005 on and 4 digits
    before (``0680a`` -> ``0680``); a 6-character code ending in a letter
    (``03001T``) is complete and untouched.
    """
    out = frame.copy()
    out['warrant_id'] = out[id_column].str.replace(r'^(\d{4}|\w{6})[A-Za-z]$', r'\1', regex=True)
    if name_column != 'warrant_name':
        out['warrant_name'] = out[name_column]
    return out


def union_new_listings(basic: pd.DataFrame, snapshot: pd.DataFrame) -> pd.DataFrame:
    """Add warrants the snapshot sees but the incremental table has not.

    The basic-info crawl sweeps every 到期日 year-window and is therefore slow
    enough to run on a longer cycle than the snapshot, so between its runs the
    newest listings exist only in the snapshot. The snapshot is a complete
    current view with the same columns, so those rows can be adopted as-is.
    """
    known_warrants = set(zip(basic['warrant_id'], basic['warrant_name']))
    is_known = [
        key in known_warrants for key in zip(snapshot['warrant_id'], snapshot['warrant_name'])
    ]
    new_listings = snapshot[~pd.Series(is_known, index=snapshot.index)]
    if new_listings.empty:
        return basic
    print(f'adopted {len(new_listings):,} warrants seen only in the snapshot')
    adopted = new_listings.drop(columns=['crawl_date'])
    return pd.concat([basic, adopted[basic.columns]], ignore_index=True)


def overlay_snapshot(basic: pd.DataFrame, snapshot: pd.DataFrame) -> pd.DataFrame:
    """Refresh the mutable term fields from the current-view snapshot."""
    snapshot_terms = snapshot[WARRANT_KEY + MUTABLE_TERM_COLUMNS + ['crawl_date']]
    snapshot_terms = snapshot_terms.drop_duplicates(subset=WARRANT_KEY, keep='last')
    out = basic.merge(snapshot_terms, on=WARRANT_KEY, how='left', suffixes=('', '_snapshot'))

    is_covered = out['crawl_date'].notna()
    target_before = out[['target_stock_id', 'target_name']].copy()
    changed_count = 0
    for column in MUTABLE_TERM_COLUMNS:
        snapshot_column = f'{column}_snapshot'
        differs = is_covered & (out[column] != out[snapshot_column]) & out[snapshot_column].notna()
        changed_count += int(differs.sum())
        out.loc[differs, column] = out.loc[differs, snapshot_column]
    out['term_source'] = 'mops_raw'
    out.loc[is_covered, 'term_source'] = 'mops_snapshot'

    print(f'snapshot covers {int(is_covered.sum()):,} warrants, refreshed {changed_count:,} field values')
    # Reported on its own: an underlying that moved is a reorganization.
    target_moved = target_before['target_stock_id'].ne(out['target_stock_id'])
    if target_moved.any():
        moves = (
            target_before[target_moved]
            .join(out.loc[target_moved, ['target_stock_id', 'target_name']], rsuffix='_now')
            .value_counts()
        )
        for (old_id, old_name, new_id, new_name), count in moves.items():
            print(f'  underlying moved: {old_id} {old_name} -> {new_id} {new_name} on {count:,} warrants')
    return out.drop(columns=[f'{column}_snapshot' for column in MUTABLE_TERM_COLUMNS] + ['crawl_date'])


def impute_dates_from_tej(basic: pd.DataFrame, tej_basic: pd.DataFrame) -> pd.DataFrame:
    tej_dates = tej_basic.rename(
        columns={'上市日': 'tej_list_date', '履約開始日': 'tej_exercise_start_date'}
    )
    tej_dates['tej_list_date'] = pd.to_datetime(tej_dates['tej_list_date'])
    tej_dates['tej_exercise_start_date'] = pd.to_datetime(tej_dates['tej_exercise_start_date'])
    tej_dates = add_warrant_key(tej_dates, '權證代號', '權證名')
    tej_dates = tej_dates.drop_duplicates(subset=WARRANT_KEY, keep='first')

    out = basic.merge(
        tej_dates[WARRANT_KEY + ['tej_list_date', 'tej_exercise_start_date']],
        on=WARRANT_KEY,
        how='left',
    )
    list_date_is_wrong = out['tej_list_date'].notna() & (out['list_date'] != out['tej_list_date'])
    start_date_is_wrong = (
        out['tej_exercise_start_date'].notna()
        & (out['exercise_start_date'] != out['tej_exercise_start_date'])
    )

    out['list_date_source'] = 'mops'
    out.loc[list_date_is_wrong, 'list_date_source'] = 'tej_imputed'
    out.loc[list_date_is_wrong, 'list_date'] = out.loc[list_date_is_wrong, 'tej_list_date']

    out['exercise_start_date_source'] = 'mops'
    out.loc[start_date_is_wrong, 'exercise_start_date_source'] = 'tej_imputed'
    out.loc[start_date_is_wrong, 'exercise_start_date'] = out.loc[
        start_date_is_wrong, 'tej_exercise_start_date'
    ]

    print(f'imputed list_date from TEJ: {int(list_date_is_wrong.sum()):,} rows')
    print(f'imputed exercise_start_date from TEJ: {int(start_date_is_wrong.sum()):,} rows')
    return out.drop(columns=['tej_list_date', 'tej_exercise_start_date'])


def impute_dates_from_finmind(basic: pd.DataFrame, finmind: pd.DataFrame) -> pd.DataFrame:
    """Second repair pass for ``list_date``, for warrants TEJ does not cover.

    The corrupted MOPS rows are almost all OTC warrants that expired before
    2020 (48,739 of the 51,537 are OTC), and TEJ's export starts in 2019.
    FinMind's warrant summary lists them from 2011 with a listing date. Rows
    are matched on the code plus the last trading day -- FinMind's
    ``end_date`` is the last trading day on TWSE rows but the expiry on OTC
    rows, so both are tried. On the 19,687 warrants all three sources share,
    FinMind agrees with TEJ to the day 97.7% of the time and within 3 days
    99.9%, so it ranks below TEJ and is only applied where the MOPS date is
    still visibly wrong (after the last trading day).

    Whatever stays unrepaired is nulled afterwards (:func:`null_unrepaired_dates`);
    downstream as-of logic treats null as "not yet listed".
    """
    finmind = finmind[['stock_id', 'date', 'end_date']].rename(
        columns={'stock_id': 'warrant_id', 'date': 'finmind_list_date', 'end_date': 'finmind_end_date'}
    )
    finmind['finmind_list_date'] = pd.to_datetime(finmind['finmind_list_date'])
    finmind['finmind_end_date'] = pd.to_datetime(finmind['finmind_end_date'])
    finmind = finmind.drop_duplicates(subset=['warrant_id', 'finmind_end_date'], keep='first')

    out = basic.reset_index(drop=True)
    by_last_trade = out[['warrant_id', 'last_trade_date']].merge(
        finmind, left_on=['warrant_id', 'last_trade_date'], right_on=['warrant_id', 'finmind_end_date'], how='left'
    )['finmind_list_date']
    by_expiry = out[['warrant_id', 'exercise_end_date']].merge(
        finmind, left_on=['warrant_id', 'exercise_end_date'], right_on=['warrant_id', 'finmind_end_date'], how='left'
    )['finmind_list_date']
    finmind_list_date = by_last_trade.fillna(by_expiry)

    is_corrupted = out['list_date'] > out['last_trade_date']
    repairable = is_corrupted & finmind_list_date.notna()
    out.loc[repairable, 'list_date'] = finmind_list_date[repairable]
    out.loc[repairable, 'list_date_source'] = 'finmind_imputed'
    # MOPS corrupts exercise_start_date alongside list_date, and only on
    # American warrants (a European one's start is its expiry, which is
    # intact), so the repaired listing date is its start as well.
    start_corrupted = repairable & (out['exercise_start_date'] == CORRUPTED_MOPS_DATE)
    out.loc[start_corrupted, 'exercise_start_date'] = finmind_list_date[start_corrupted]
    out.loc[start_corrupted, 'exercise_start_date_source'] = 'finmind_imputed'

    print(f'imputed list_date from FinMind: {int(repairable.sum()):,} rows')
    print(f'imputed exercise_start_date from FinMind: {int(start_corrupted.sum()):,} rows')
    return out


def null_unrepaired_dates(basic: pd.DataFrame) -> pd.DataFrame:
    """A listing date after the last trading day is not a date; null it."""
    out = basic.copy()
    unrepaired = out['list_date'] > out['last_trade_date']
    out.loc[unrepaired, ['list_date', 'list_date_source']] = [pd.NaT, 'unknown']
    unrepaired_start = unrepaired & (out['exercise_start_date'] == CORRUPTED_MOPS_DATE)
    out.loc[unrepaired_start, ['exercise_start_date', 'exercise_start_date_source']] = [pd.NaT, 'unknown']
    print(f'list_date still unrepaired (nulled): {int(unrepaired.sum()):,} rows')
    return out


def build_dim_warrant(cache_directory: Path) -> pd.DataFrame:
    basic = load_raw_table(cache_directory, 'warrant_basic_info')
    if basic is None:
        raise FileNotFoundError(f'no warrant_basic_info under {cache_directory}/mops_raw')
    # Append-only table: a re-swept 到期日 window (e.g. the cursor's own year,
    # or a backfill) lands the same expired rows a second time.
    basic = add_warrant_key(basic, 'warrant_id', 'warrant_name')
    # A warrant whose expiry moved between sweeps (early termination) appears
    # under two exercise_end_dates; the later sweep has the final one.
    basic = basic.drop_duplicates(subset=WARRANT_KEY, keep='last')
    print(f'MOPS basic_info: {len(basic):,} rows')

    snapshot = load_raw_table(cache_directory, 'warrant_active_snapshot')
    if snapshot is None:
        print('WARNING: no warrant_active_snapshot table -- mutable term fields may be stale')
        basic['term_source'] = 'mops_raw'
    else:
        snapshot = add_warrant_key(snapshot, 'warrant_id', 'warrant_name')
        basic = union_new_listings(basic, snapshot)
        basic = overlay_snapshot(basic, snapshot)

    tej_basic_path = find_tej_seed(TEJ_BASIC_INFO_GLOB)
    if tej_basic_path is not None:
        print(f'TEJ seed: {tej_basic_path}')
        basic = impute_dates_from_tej(basic, pd.read_parquet(tej_basic_path))
    else:
        print(f'WARNING: no {TEJ_BASIC_INFO_GLOB} in the TEJ seed dir -- skipping list_date repair')
        basic['list_date_source'] = 'mops'
        basic['exercise_start_date_source'] = 'mops'

    finmind_path = find_finmind_seed()
    if finmind_path is not None:
        print(f'FinMind seed: {finmind_path}')
        basic = impute_dates_from_finmind(basic, pd.read_parquet(finmind_path))
    else:
        print(f'WARNING: no {FINMIND_SUMMARY_GLOB} in the FinMind seed dir -- pre-2019 list_date repair skipped')
    basic = null_unrepaired_dates(basic)

    # Bull/bear certificates (牛證/熊證) behave differently and TEJ never covers
    # them, so downstream studies exclude them -- flag rather than drop.
    basic['is_bull_bear'] = basic['warrant_name'].str.contains('牛|熊', regex=True, na=False)
    # Extendable warrants (展延型): 104 warrants, 2014-2019, named ``<series>展NN``
    # (元展06, 富展01, 永豐金元大61展01). 大展證券's 2005-08 plain warrants share
    # the suffix (大展01) and are told apart by issuer. Also excluded from
    # the history: their maturity moves by extension, which is not modelled.
    ends_in_extension_series = basic['warrant_name'].str.contains(r'展\d{2}$', regex=True, na=False)
    basic['is_extendable'] = ends_in_extension_series & (basic['issuer'] != '大展')

    # Exercise style, which no source states for the whole population: t90sb01
    # has no such column, the exchange OpenAPI has none either, and TEJ's
    # 權證類型 / t95sb02's exercise_method only cover part of the universe. It is
    # implied by the dates instead -- an American warrant can be exercised from
    # its first trading day, a European one only at maturity. Checked against
    # TEJ's 權證類型 on all 418,527 warrants both sources share: no exceptions.
    # Nullable: unknown where either date could not be repaired.
    both_dates_known = basic['exercise_start_date'].notna() & basic['list_date'].notna()
    basic['is_american'] = (basic['exercise_start_date'] == basic['list_date']).where(both_dates_known).astype('boolean')
    return basic


def main() -> None:
    parser = argparse.ArgumentParser(description='Build the warrant dimension table.')
    parser.add_argument('--cache-dir', default=None,
                        help=f'default: $DATA_SDK_WARRANT_CACHE_PATH or {DEFAULT_CACHE_PATH}')
    parser.add_argument('--out', default=None)
    arguments = parser.parse_args()

    cache_directory = cache_dir(arguments.cache_dir)
    out_path = (
        Path(arguments.out) if arguments.out else cache_directory / 'warrant_basic_info.parquet'
    )

    dim_warrant = build_dim_warrant(cache_directory)
    dim_warrant.to_parquet(out_path, index=False)
    print(f'wrote {len(dim_warrant):,} rows -> {out_path}')


if __name__ == '__main__':
    main()
