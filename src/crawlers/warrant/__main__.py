"""CLI entry point: ``python -m mops_crawler --cache-dir <path>``.

Writes raw parquet tables under ``<cache-dir>/mops_raw/`` and keeps the dlt
pipeline's incremental-cursor state under ``<cache-dir>/mops_pipeline_state/``.
The destination is plain ``filesystem`` (parquet files) — every resource is
append-only (see :mod:`mops_crawler.warrant_reports` for why), so no
merge/upsert support, and therefore no SQL-capable destination, is needed.
"""
from __future__ import annotations

import argparse
import errno
import time
from pathlib import Path

import dlt
from dlt.common.storages.file_storage import FileStorage

from . import DEFAULT_CACHE_PATH, cache_directory as cache_dir
from .warrant_reports import HISTORY_START_YEAR, warrant_reports_source

RESOURCE_NAMES = [
    'warrant_basic_info',
    'warrant_active_snapshot',
    'warrant_strike_ratio_adjustment',
    'warrant_pending_adjustment',
    'warrant_strike_ratio_reset',
    'warrant_pending_reset',
    'warrant_announcement',
]


# TODO: workaround. Remove once `fsc` is off the NFS mount or dlt retries upstream.
def patch_dlt_rmtree_for_nfs(
    retry_delays_seconds: tuple[float, ...] = (0.5, 1, 2, 4, 8, 16, 32),
) -> None:
    """Retry dlt's package-folder delete while NFS silly-renamed files linger.

    dlt commits a load package and then ``shutil.rmtree``s the extracted copy
    with no retry. On the NFS mount holding the cache dir (``fsc`` on) a file
    closed moments earlier is silly-renamed to ``.nfs*`` at unlink time and
    the rmtree fails -- ``ENOTEMPTY`` on the directory, or ``EBUSY`` when it
    reaches the ``.nfs*`` entry itself. These entries clear asynchronously;
    the default backoff allows ~64 seconds for cleanup under load.

    ``FileStorage.delete_folder`` is the one exit every ``delete_package``
    call goes through, so patching it covers all of them. Remove once dlt
    retries upstream.
    """
    original_delete_folder = FileStorage.delete_folder
    transient_errnos = {errno.ENOTEMPTY, errno.EBUSY}

    def delete_folder_with_retry(self, relative_path, recursively=False, delete_ro=False):
        for attempt, delay_seconds in enumerate([*retry_delays_seconds, None]):
            try:
                return original_delete_folder(self, relative_path, recursively, delete_ro)
            except OSError as error:
                is_last_attempt = delay_seconds is None
                if error.errno not in transient_errnos or is_last_attempt:
                    raise
                print(f'delete of {relative_path} hit {error}; retry {attempt + 1} in {delay_seconds}s')
                time.sleep(delay_seconds)

    FileStorage.delete_folder = delete_folder_with_retry


def main() -> None:
    parser = argparse.ArgumentParser(description='Crawl TWSE MOPS warrant reports into raw parquet tables.')
    parser.add_argument('--cache-dir', default=None,
                        help=f'writes mops_raw/ and mops_pipeline_state/ under this'
                             f' (default: $DATA_SDK_WARRANT_CACHE_PATH or {DEFAULT_CACHE_PATH})')
    parser.add_argument('--history-start-year', type=int, default=HISTORY_START_YEAR, help='earliest 到期日 year to sweep for delisted warrants')
    parser.add_argument('--history-end-year', type=int, default=None, help='last 到期日 year to sweep (default: this year); use to backfill in chunks')
    parser.add_argument('--resources', nargs='+', choices=RESOURCE_NAMES, default=None, help='restrict the run to these resources (default: all three)')
    arguments = parser.parse_args()

    cache_directory = cache_dir(arguments.cache_dir)
    patch_dlt_rmtree_for_nfs()
    pipeline = dlt.pipeline(
        pipeline_name='mops_warrant_reports',
        destination=dlt.destinations.filesystem(
            bucket_url=(cache_directory / 'mops_raw').resolve().as_uri()
        ),
        dataset_name='mops_raw',
        pipelines_dir=str(cache_directory / 'mops_pipeline_state'),
    )
    source = warrant_reports_source(arguments.history_start_year, arguments.history_end_year)
    if arguments.resources is not None:
        source = source.with_resources(*arguments.resources)
    print(pipeline.run(source, loader_file_format='parquet'))


if __name__ == '__main__':
    main()
