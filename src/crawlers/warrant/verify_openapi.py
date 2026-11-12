"""Cross-check the built tables against the TWSE / TPEx OpenAPI.

An independent read of the same facts. MOPS is scraped from HTML and TEJ arrives
as a frozen export, so both can drift or be misparsed without anything in
:mod:`validate` noticing — those checks only prove the tables are internally
consistent. The exchanges publish the current terms of every *live* warrant as
JSON, which is a different pipeline end to end, so agreement there is real
evidence the numbers are right.

Only live warrants are covered: the OpenAPI carries no expired ones, so this
verifies the leading edge, not history.

    python -m data_sdk.crawlers.warrant.verify_openapi --cache-dir cache

Exits non-zero if any match rate falls below its threshold.
"""
from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

import pandas as pd

from . import DEFAULT_CACHE_PATH, cache_directory as cache_dir
import requests

OPENAPI_ENDPOINTS = (
    ('twse', 'https://openapi.twse.com.tw/v1/opendata/t187ap37_L'),
    ('otc', 'https://www.tpex.org.tw/openapi/v1/mopsfin_t187ap37_O'),
)
REQUEST_TIMEOUT_SECONDS = 120

# Exchange field -> ours. 最新標的履約配發數量 is per 1000 units, like MOPS's
# alloc_qty_per_1k, so it is divided down to a per-unit ratio.
STRIKE_FIELD = '最新履約價格(元)/履約指數'
ALLOCATION_FIELD = '最新標的履約配發數量(每仟單位權證)'
EXPIRY_FIELD = '履約截止日'
LAST_TRADE_FIELD = '最後交易日'

STRIKE_TOLERANCE = 0.01
RATIO_TOLERANCE = 1e-4


def parse_republic_yyyymmdd(raw_value: object) -> pd.Timestamp:
    """``'1150730'`` -> 2026-07-30. Unlike MOPS's HTML these carry no slashes."""
    text = str(raw_value).strip()
    if len(text) != 7 or not text.isdigit():
        return pd.NaT
    return pd.Timestamp(
        year=int(text[:3]) + 1911, month=int(text[3:5]), day=int(text[5:7])
    )


def fetch_json_with_resume(url: str, max_requests: int = 30) -> list:
    """GET a large JSON body from a server that drops the connection mid-body.

    TPEx's OpenAPI can close a response before the declared Content-Length.
    It honours Range, so the body is collected in pieces: each retry asks for
    ``bytes=<received>-`` until the declared length is in hand, then the JSON
    is parsed as the real completeness check.
    """
    body = b''
    declared_length = None
    for _ in range(max_requests):
        headers = {'Range': f'bytes={len(body)}-'} if body else {}
        response = requests.get(url, headers=headers, timeout=REQUEST_TIMEOUT_SECONDS, stream=True)
        response.raise_for_status()
        if declared_length is None:
            declared_length = int(response.headers.get('Content-Length', 0)) or None
        try:
            for chunk in response.iter_content(65536):
                body += chunk
        except requests.exceptions.ChunkedEncodingError:
            continue  # partial body kept; the next request resumes from its end
        if declared_length is None or len(body) >= declared_length:
            try:
                return json.loads(body)
            except json.JSONDecodeError:
                body = b''  # a corrupt assembly: start over
                declared_length = None
    raise RuntimeError(f'{url}: incomplete after {max_requests} requests ({len(body):,} bytes)')


def fetch_openapi_terms() -> pd.DataFrame:
    frames = []
    for market_name, url in OPENAPI_ENDPOINTS:
        records = fetch_json_with_resume(url)
        frame = pd.DataFrame(records)
        frame['market'] = market_name
        print(f'  {market_name}: {len(frame):,} live warrants from the exchange')
        frames.append(frame)

    published = pd.concat(frames, ignore_index=True)
    # The history leaves bull/bear and extendable warrants out (see
    # build_history); the exchange lists them, so drop them here or they
    # count as missing. Same name rules as build_basic_info.
    names = published['權證簡稱'].astype(str)
    is_excluded = names.str.contains('牛|熊', regex=True) | names.str.contains(r'展\d{2}$', regex=True)
    print(f'  excluding {int(is_excluded.sum()):,} bull/bear and extendable warrants')
    published = published[~is_excluded]
    return pd.DataFrame({
        'warrant_id': published['權證代號'].astype(str).str[:6],
        'warrant_name': published['權證簡稱'].astype(str).str.strip(),
        'published_strike': pd.to_numeric(published[STRIKE_FIELD], errors='coerce'),
        'published_ratio': pd.to_numeric(published[ALLOCATION_FIELD], errors='coerce') / 1000.0,
        'published_exercise_end_date': published[EXPIRY_FIELD].map(parse_republic_yyyymmdd),
        'published_last_trade_date': published[LAST_TRADE_FIELD].map(parse_republic_yyyymmdd),
    }).drop_duplicates(subset=['warrant_id', 'warrant_name'], keep='last')


def report(label: str, match_rate: float, matched: int, total: int) -> bool:
    passed = matched == total
    print(f'  [{"PASS" if passed else "FAIL"}] {label} — {match_rate:.2%} ({matched:,}/{total:,})')
    return passed


def verify(cache_directory: Path) -> bool:
    history = pd.read_parquet(cache_directory / 'warrant_history.parquet')
    current = history[history['is_current']]

    print('fetching exchange OpenAPI...')
    published = fetch_openapi_terms()

    compared = current.merge(published, on=['warrant_id', 'warrant_name'], how='inner')
    print(f'\n== verify against TWSE/TPEx OpenAPI ==')
    print(f'  matched {len(compared):,} of {len(published):,} live warrants'
          f' ({len(published) - len(compared):,} not in our tables)')

    strike_matches = (compared['strike'] - compared['published_strike']).abs() <= STRIKE_TOLERANCE
    ratio_matches = (compared['ratio'] - compared['published_ratio']).abs() <= RATIO_TOLERANCE
    expiry_matches = compared['exercise_end_date'] == compared['published_exercise_end_date']
    last_trade_matches = compared['last_trade_date'] == compared['published_last_trade_date']

    results = [
        report('strike', float(strike_matches.mean()), int(strike_matches.sum()), len(compared)),
        report('ratio', float(ratio_matches.mean()), int(ratio_matches.sum()), len(compared)),
        report('exercise_end_date', float(expiry_matches.mean()), int(expiry_matches.sum()), len(compared)),
        report('last_trade_date', float(last_trade_matches.mean()), int(last_trade_matches.sum()), len(compared)),
    ]

    mismatched = compared[~strike_matches | ~ratio_matches]
    if len(mismatched):
        print(f'\n  first mismatches ({len(mismatched):,} rows differ on strike or ratio):')
        columns = [
            'warrant_id',
            'warrant_name',
            'strike',
            'published_strike',
            'ratio',
            'published_ratio',
            'effective_date',
        ]
        print(mismatched[columns].head(10).to_string(index=False))

    return all(results)


def main() -> None:
    parser = argparse.ArgumentParser(description='Verify built terms against the exchange OpenAPI.')
    parser.add_argument('--cache-dir', default=None,
                        help=f'default: $DATA_SDK_WARRANT_CACHE_PATH or {DEFAULT_CACHE_PATH}')
    arguments = parser.parse_args()

    if not verify(cache_dir(arguments.cache_dir)):
        sys.exit(1)


if __name__ == '__main__':
    main()
