"""Where each vendor cache lives when no path variable is set, and that the
variable still wins. The defaults are local (``$XDG_CACHE_HOME`` or
``~/.cache`` under ``data_sdk``); none of them is on NFS."""

import os

import pytest

from data_sdk import CbInfoWrapper, FinLabWrapper, FinMindWrapper, TaifexWrapper
from data_sdk.wrappers.local_cache import local_cache_dir

# (accessor, variable, default tail under <cache home>/data_sdk)
CASES = [
    (TaifexWrapper.cache_dir, "DATA_SDK_TAIFEX_CACHE_PATH", ("taifex_cache",)),
    (FinLabWrapper.cache_dir, "DATA_SDK_FINLAB_CACHE_PATH", ("finlab_cache",)),
    (CbInfoWrapper.cache_dir, "DATA_SDK_CB_CACHE_PATH", ("finmind_cache", "cb")),
    (FinMindWrapper.weight_dir, "DATA_SDK_FINMIND_WEIGHT_PATH", ("finmind_cache", "twii_weight")),
]
IDS = [c[1] for c in CASES]


@pytest.mark.parametrize("accessor,variable,tail", CASES, ids=IDS)
def test_default_is_under_xdg_cache_home(monkeypatch, tmp_path, accessor, variable, tail):
    monkeypatch.delenv(variable, raising=False)
    monkeypatch.setenv("XDG_CACHE_HOME", str(tmp_path))
    assert accessor() == os.path.join(str(tmp_path), "data_sdk", *tail)


@pytest.mark.parametrize("accessor,variable,tail", CASES, ids=IDS)
def test_default_falls_back_to_home_dot_cache(monkeypatch, tmp_path, accessor, variable, tail):
    monkeypatch.delenv(variable, raising=False)
    monkeypatch.delenv("XDG_CACHE_HOME", raising=False)
    monkeypatch.setenv("HOME", str(tmp_path))
    assert accessor() == os.path.join(str(tmp_path), ".cache", "data_sdk", *tail)


@pytest.mark.parametrize("accessor,variable,tail", CASES, ids=IDS)
def test_variable_overrides_the_default(monkeypatch, tmp_path, accessor, variable, tail):
    monkeypatch.setenv("XDG_CACHE_HOME", str(tmp_path / "xdg"))
    monkeypatch.setenv(variable, "/somewhere/else")
    assert accessor() == "/somewhere/else"


@pytest.mark.parametrize("accessor,variable,tail", CASES, ids=IDS)
def test_default_never_names_nfs(monkeypatch, accessor, variable, tail):
    monkeypatch.delenv(variable, raising=False)
    monkeypatch.delenv("XDG_CACHE_HOME", raising=False)
    assert not accessor().startswith("/mnt/nfs")


def test_local_cache_dir_joins_parts(monkeypatch, tmp_path):
    monkeypatch.setenv("XDG_CACHE_HOME", str(tmp_path))
    assert local_cache_dir("a", "b") == os.path.join(str(tmp_path), "data_sdk", "a", "b")
