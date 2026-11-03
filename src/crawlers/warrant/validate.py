"""Validate the raw MOPS tables produced by ``python -m mops_crawler``.

Reads only what the pipeline already wrote — no network — and prints one line
per check. Checks are ordered from cheap structural ones to the two that
actually prove the data is usable for reconstructing historical strike/ratio:

* **closure** — ``t90sb01``'s *latest* strike/ratio must equal the most recent
  event in ``t95sb02``/``t95sb03``. The two sides come from separately crawled
  MOPS reports, so agreeing is strong evidence both were parsed correctly.
* **K x ratio invariant** — an ex-rights adjustment moves strike and ratio in
  opposite directions (``K' = K·S'/S``, ``ratio' = ratio·S/S'``), so their
  product is conserved across a warrant's adjustments. This is the assumption
  the planned SCD2 back-out of the *initial* ratio rests on; if it fails, that
  plan needs rethinking.

Usage:
    python -m mops_crawler.validate --cache-dir cache
"""
from __future__ import annotations

import argparse
import glob
from pathlib import Path

import pandas as pd

from . import DEFAULT_CACHE_PATH, cache_directory as cache_dir
from .build_basic_info import WARRANT_KEY, add_warrant_key

RAW_WARRANT_KEY = ['warrant_id', 'exercise_end_date']


def load_table(cache_directory: Path, table_name: str) -> pd.DataFrame:
    files = sorted(glob.glob(str(cache_directory / 'mops_raw' / 'mops_raw' / table_name / '*.parquet')))
    if not files:
        raise FileNotFoundError(f'no parquet files for {table_name} under {cache_directory}')
    # diagonal concat: tolerate schema drift in tables written before column
    # type hints were added (see column_type_hints in warrant_reports.py).
    frame = pd.concat([pd.read_parquet(f) for f in files], ignore_index=True)
    frame = frame.replace({'nan': None})
    for column in frame.columns:
        if column.endswith('_date'):
            frame[column] = pd.to_datetime(frame[column])
    return frame


def report(label: str, passed: bool, detail: str = '') -> None:
    print(f'  [{"PASS" if passed else "FAIL"}] {label}' + (f' — {detail}' if detail else ''))


def check_structure(basic_info, adjustment, reset) -> None:
    print('\n== structure ==')
    for name, frame, key in (
        ('basic_info', basic_info, RAW_WARRANT_KEY),
        ('adjustment', adjustment, RAW_WARRANT_KEY + ['adjustment_effective_date']),
        ('reset', reset, RAW_WARRANT_KEY + ['reset_effective_date']),
    ):
        duplicates = int(frame.duplicated(subset=key).sum())
        report(f'{name}: no duplicate natural key {tuple(key)}', duplicates == 0, f'{duplicates:,} duplicates')

    # A few dozen pre-2005 rows have no type at all on MOPS; NaN is tolerated.
    bad_type = set(basic_info['type'].dropna().unique()) - {'認購', '認售'}
    report('basic_info: type in {認購, 認售}', not bad_type, str(bad_type))
    bad_id = int((~basic_info['warrant_id'].str.match(r'^\w{4,}$')).sum())
    report('basic_info: warrant_id well formed', bad_id == 0, f'{bad_id:,} malformed')
    for column in ('latest_strike', 'alloc_qty_per_1k'):
        non_positive = int((basic_info[column] <= 0).sum())
        report(f'basic_info: {column} > 0', non_positive == 0, f'{non_positive:,} non-positive')


def check_date_ordering(basic_info) -> None:
    print('\n== date ordering (basic_info) ==')
    ordered = basic_info.dropna(subset=['list_date', 'last_trade_date', 'exercise_end_date'])
    violations = int((ordered['list_date'] > ordered['last_trade_date']).sum())
    report('list_date <= last_trade_date', violations == 0, f'{violations:,} violations')
    violations = int((ordered['last_trade_date'] > ordered['exercise_end_date']).sum())
    report('last_trade_date <= exercise_end_date', violations == 0, f'{violations:,} violations')


def check_referential_integrity(basic_info, adjustment, reset) -> None:
    """Every event must belong to a warrant in basic_info — matched on
    (warrant_id, exercise_end_date), so this also cross-checks that the expiry
    date agrees between two separately crawled reports."""
    print('\n== referential integrity ==')
    known = set(map(tuple, basic_info[RAW_WARRANT_KEY].dropna().values))
    for name, events in (('adjustment', adjustment), ('reset', reset)):
        event_keys = set(map(tuple, events[RAW_WARRANT_KEY].dropna().values))
        orphans = event_keys - known
        report(f'{name}: every event maps to a basic_info warrant',
               not orphans, f'{len(orphans):,} orphan warrants (of {len(event_keys):,})')


def check_reset_timing(basic_info, reset) -> None:
    """Reset-type warrants finalize their provisional strike on their own
    listing day, so reset_effective_date should equal list_date."""
    print('\n== reset timing ==')
    merged = reset.merge(basic_info[RAW_WARRANT_KEY + ['list_date']], on=RAW_WARRANT_KEY, how='inner')
    if merged.empty:
        report('reset_effective_date == list_date', False, 'no rows matched')
        return
    same_day = int((merged['reset_effective_date'] == merged['list_date']).sum())
    report('reset_effective_date == list_date', same_day == len(merged),
           f'{same_day:,}/{len(merged):,} match')


def check_strike_ratio_invariant(adjustment) -> None:
    """K x ratio should be conserved across a warrant's adjustments."""
    print('\n== K x ratio invariant (adjustment events) ==')
    events = adjustment.dropna(subset=['latest_strike', 'latest_ratio']).copy()
    events['product'] = events['latest_strike'] * events['latest_ratio']
    grouped = events.groupby(RAW_WARRANT_KEY)['product']
    multi = grouped.count() > 1
    if not multi.any():
        report('warrants with >1 adjustment exist', False, 'none found')
        return
    spread = ((grouped.max() - grouped.min()) / grouped.mean())[multi]
    print(f'  {multi.sum():,} warrants have >1 adjustment event')
    for tolerance in (0.001, 0.01, 0.05):
        within = int((spread <= tolerance).sum())
        print(f'    within {tolerance:>5.1%} relative spread: {within:,}/{len(spread):,}'
              f' ({within / len(spread):.1%})')
    report('median relative spread < 1%', float(spread.median()) < 0.01,
           f'median={spread.median():.4%}, p90={spread.quantile(0.9):.4%}')


def check_closure_with_basic_info(basic_info, adjustment) -> None:
    """basic_info's *latest* strike/ratio must equal the most recent adjustment."""
    print('\n== closure: basic_info latest == most recent adjustment ==')
    latest_event = (adjustment.sort_values('adjustment_effective_date')
                    .groupby(RAW_WARRANT_KEY).tail(1)
                    .set_index(RAW_WARRANT_KEY)[['latest_strike', 'latest_ratio']])
    reference = basic_info.set_index(RAW_WARRANT_KEY)[['latest_strike', 'alloc_qty_per_1k']]
    joined = latest_event.join(reference, how='inner', rsuffix='_basic')
    if joined.empty:
        report('strike closure', False, 'no overlapping warrants')
        return

    strike_match = (joined['latest_strike'] - joined['latest_strike_basic']).abs() <= 0.005
    report('latest_strike matches most recent adjustment',
           strike_match.all(), f'{int(strike_match.sum()):,}/{len(joined):,} match')

    # Report the empirical ratio unit relationship rather than assuming one:
    # alloc_qty_per_1k is "shares per 1000 units", latest_ratio is per unit.
    scale = (joined['alloc_qty_per_1k'] / joined['latest_ratio']).replace([float('inf')], pd.NA).dropna()
    if len(scale):
        print(f'  alloc_qty_per_1k / latest_ratio → median {scale.median():.4f}'
              f' (p10 {scale.quantile(0.1):.4f}, p90 {scale.quantile(0.9):.4f})')
        consistent = int(((scale / scale.median() - 1).abs() <= 0.001).sum())
        report('ratio unit scale is consistent across warrants',
               consistent == len(scale), f'{consistent:,}/{len(scale):,} consistent')


def check_curated_layer(cache_directory: Path) -> None:
    """Checks on the built dimension + history tables, if they exist.

    The closure checks are the ones that matter: the last row of a warrant's
    history must agree with the terms the dimension table currently reports,
    otherwise the event chain lost or gained a change somewhere.
    """
    dimension_path = cache_directory / 'warrant_basic_info.parquet'
    history_path = cache_directory / 'warrant_history.parquet'
    if not (dimension_path.exists() and history_path.exists()):
        print('\n== curated layer ==\n  [SKIP] build_basic_info / build_history have not run')
        return

    print('\n== curated layer ==')
    # The history leaves bull/bear and extendable warrants out by design (build_history).
    dimension = pd.read_parquet(dimension_path)
    is_excluded = (
        dimension['is_bull_bear'].fillna(False).astype(bool)
        | dimension['is_extendable'].fillna(False).astype(bool)
    )
    dimension = dimension[~is_excluded]
    history = pd.read_parquet(history_path)
    warrant_count = len(history[WARRANT_KEY].drop_duplicates())
    print(f'  dim_warrant {len(dimension):,} rows | warrant_history {len(history):,} rows'
          f' for {warrant_count:,} warrants')

    report('every warrant has history',
           warrant_count == len(dimension),
           f'{warrant_count:,} of {len(dimension):,}')

    current_row_counts = history.groupby(WARRANT_KEY)['is_current'].sum()
    report('exactly one is_current per warrant',
           bool((current_row_counts == 1).all()),
           f'{int((current_row_counts != 1).sum()):,} warrants violate')

    dated = history.dropna(subset=['effective_date'])
    listed_before_expiry = dated['effective_date'] <= dated['exercise_end_date']
    report('effective_date <= exercise_end_date',
           bool(listed_before_expiry.all()),
           f'{int((~listed_before_expiry).sum()):,} violations'
           f' ({len(history) - len(dated):,} rows undated: list_date unknown)')

    with_last_trade = history.dropna(subset=['last_trade_date'])
    tradable_before_expiry = with_last_trade['last_trade_date'] <= with_last_trade['exercise_end_date']
    report('last_trade_date <= exercise_end_date',
           bool(tradable_before_expiry.all()),
           f'{int((~tradable_before_expiry).sum()):,} violations'
           f' ({len(history) - len(with_last_trade):,} rows without one: 2003-04 MOPS blanks)')

    # The last row per warrant, which may be an adjustment not yet in force:
    # the dimension's latest_strike comes from MOPS's snapshot, and MOPS applies
    # an ex-dividend adjustment a day early, so the two agree on the final row
    # rather than on is_current.
    history = history.sort_values(WARRANT_KEY + ['effective_date', 'sequence'], na_position='first')
    current = history.groupby(WARRANT_KEY, sort=False).tail(1).merge(
        dimension[WARRANT_KEY + ['latest_strike', 'alloc_qty_per_1k', 'exercise_end_date']],
        on=WARRANT_KEY,
        suffixes=('', '_dim'),
    )
    # A warrant with no strike history at all (pre-2020 expiry, outside TEJ)
    # carries only its issuance strike, so it cannot close against the final
    # one; the check is over warrants that have some history.
    has_history = current['source'] != 'dim_synthesised'
    strike_matches = (current['strike'] - current['latest_strike']).abs() <= 0.01
    report('current strike == dim latest_strike (>=98%, warrants with history)',
           float(strike_matches[has_history].mean()) >= 0.98,
           f'{strike_matches[has_history].mean():.2%} of {int(has_history.sum()):,};'
           f' {int((~has_history).sum()):,} synthesised-only at {strike_matches[~has_history].mean():.2%}')

    ratio_matches = (current['ratio'] - current['alloc_qty_per_1k'] / 1000).abs() <= 1e-4
    report('current ratio == dim alloc_qty_per_1k/1000 (>=99%)',
           float(ratio_matches.mean()) >= 0.99,
           f'{ratio_matches.mean():.2%}')

    expiry_matches = current['exercise_end_date'] == current['exercise_end_date_dim']
    report('current expiry == dim exercise_end_date (>=98%)',
           float(expiry_matches.mean()) >= 0.98,
           f'{expiry_matches.mean():.2%}')

    # The underlying is a state column closed against the dimension table by
    # build_history, so the final row must name the same code; a miss means a
    # conversion the chain recorded but the dimension table does not have.
    current_underlying = history.groupby(WARRANT_KEY, sort=False).tail(1).merge(
        dimension[WARRANT_KEY + ['target_stock_id']], on=WARRANT_KEY, suffixes=('', '_dim')
    )
    underlying_closes = current_underlying['target_stock_id'].fillna('') == current_underlying['target_stock_id_dim'].fillna('')
    report('current underlying == dim target_stock_id',
           bool(underlying_closes.all()),
           f'{int((~underlying_closes).sum()):,} warrants differ')
    underlying_count = history.groupby(WARRANT_KEY)['target_stock_id'].nunique()
    print(f'  warrants whose underlying changed within their history: {int((underlying_count > 1).sum()):,}'
          f' ({int((history["event_type"] == "target_change").sum()):,} dated only by closure)')

    corrupted_list_dates = dimension['list_date'] > dimension['last_trade_date']
    report('dim list_date <= last_trade_date (MOPS 2023-12-26 bug repaired)',
           not bool(corrupted_list_dates.any()),
           f'{int(corrupted_list_dates.sum()):,} violations;'
           f' {int(dimension["list_date"].isna().sum()):,} unknown (null)')

    # The underlying moves on a share conversion or merger, and MOPS updates it
    # on every live warrant; the dimension table must follow (build_basic_info
    # refreshes it from the snapshot), or every warrant on the new code is
    # invisible to a join on the underlying's quotes.
    try:
        snapshot = add_warrant_key(load_table(cache_directory, 'warrant_active_snapshot'), 'warrant_id', 'warrant_name')
    except FileNotFoundError:
        print('  [SKIP] dim underlying == snapshot: no warrant_active_snapshot')
        return
    snapshot = snapshot.drop_duplicates(subset=WARRANT_KEY, keep='last')
    live = dimension.merge(
        snapshot[WARRANT_KEY + ['target_stock_id', 'target_name']],
        on=WARRANT_KEY,
        suffixes=('', '_snapshot'),
    )
    underlying_matches = (
        (live['target_stock_id'].fillna('') == live['target_stock_id_snapshot'].fillna(''))
        & (live['target_name'].fillna('') == live['target_name_snapshot'].fillna(''))
    )
    report('dim underlying == snapshot underlying (live warrants)',
           bool(underlying_matches.all()),
           f'{int((~underlying_matches).sum()):,} of {len(live):,} differ')


def main() -> None:
    parser = argparse.ArgumentParser(description='Validate raw MOPS warrant tables.')
    parser.add_argument('--cache-dir', default=None,
                        help=f'default: $DATA_SDK_WARRANT_CACHE_PATH or {DEFAULT_CACHE_PATH}')
    arguments = parser.parse_args()
    cache_directory = cache_dir(arguments.cache_dir)

    # Same dedup as build_basic_info: re-swept 到期日 windows append the same
    # expired rows again, which is expected, not a defect.
    basic_info = load_table(cache_directory, 'warrant_basic_info').drop_duplicates(
        subset=RAW_WARRANT_KEY, keep='last'
    )
    adjustment = load_table(cache_directory, 'warrant_strike_ratio_adjustment')
    reset = load_table(cache_directory, 'warrant_strike_ratio_reset')
    print(f'basic_info {basic_info.shape[0]:,} rows | adjustment {adjustment.shape[0]:,} rows'
          f' | reset {reset.shape[0]:,} rows')

    check_structure(basic_info, adjustment, reset)
    check_date_ordering(basic_info)
    check_referential_integrity(basic_info, adjustment, reset)
    check_reset_timing(basic_info, reset)
    check_strike_ratio_invariant(adjustment)
    check_closure_with_basic_info(basic_info, adjustment)
    check_curated_layer(cache_directory)


if __name__ == '__main__':
    main()
