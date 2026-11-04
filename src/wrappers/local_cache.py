"""Default location of a vendor's on-disk cache: the user's cache directory.

``$XDG_CACHE_HOME/data_sdk/<name>``, ``~/.cache/data_sdk/<name>`` when
``XDG_CACHE_HOME`` is unset or empty. The same rule MarketCalendar,
DispositionList and FinMindCalendar apply to their own caches.
"""

import os


def local_cache_dir(*parts: str) -> str:
    base = os.environ.get("XDG_CACHE_HOME") or os.path.join(os.path.expanduser("~"), ".cache")
    return os.path.join(base, "data_sdk", *parts)
