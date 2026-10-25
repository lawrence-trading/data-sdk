import os
import sys
from pathlib import Path


def env_path(var: str) -> Path:
    """Directory named by the environment variable ``var``.

    Every DATA_SDK_*_PATH is resolved here, so all of them behave the same way:
    unset or empty falls back to the current directory and prints a warning to
    stderr; nothing has a built-in NFS default.
    """
    value = os.environ.get(var)
    if not value:
        print(f"Warning: {var} not set. Using current directory.", file=sys.stderr)
        return Path(".")
    return Path(value)
