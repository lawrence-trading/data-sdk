"""Offline tests for the DATA_SDK_*_PATH convention (no network, no NFS)."""
from pathlib import Path

import pytest

from data_sdk.wrappers import fop_wrapper, order_book_wrapper
from data_sdk.wrappers.finmind_broker_wrapper import FinMindWrapper
from data_sdk.wrappers.shioaji_wrapper import ShioajiWrapper
from data_sdk.wrappers.tej_wrapper import TEJWrapper

ACCESSORS = [
    ("DATA_SDK_ORDER_BOOK_PARQUET_PATH", order_book_wrapper._order_book_base_path),
    ("DATA_SDK_FOP_PARQUET_PATH", fop_wrapper._fop_base_path),
    ("DATA_SDK_TEJ_CACHE_PATH", TEJWrapper._cache_dir),
    ("DATA_SDK_FINMIND_BROKER_PATH", FinMindWrapper.broker_dir),
    ("DATA_SDK_FINMIND_BROKER_WARRANT_PATH", FinMindWrapper.warrant_broker_dir),
    ("DATA_SDK_SHIOAJI_TICKS_PATH", ShioajiWrapper.ticks_dir),
    ("DATA_SDK_SHIOAJI_FUTURES_TICKS_PATH", ShioajiWrapper.futures_ticks_dir),
]


@pytest.mark.parametrize("var,accessor", ACCESSORS, ids=[v for v, _ in ACCESSORS])
def test_unset_variable_means_current_directory_with_a_warning(var, accessor, monkeypatch, capsys, tmp_path):
    monkeypatch.chdir(tmp_path)  # TEJWrapper._cache_dir calls makedirs on the result
    monkeypatch.delenv(var, raising=False)
    assert Path(accessor()) == Path(".")
    assert capsys.readouterr().err.strip() == f"Warning: {var} not set. Using current directory."


@pytest.mark.parametrize("var,accessor", ACCESSORS, ids=[v for v, _ in ACCESSORS])
def test_set_variable_is_used_as_is_without_a_warning(var, accessor, monkeypatch, capsys, tmp_path):
    target = tmp_path / "data"
    target.mkdir()
    monkeypatch.setenv(var, str(target))
    assert Path(accessor()) == target
    assert capsys.readouterr().err == ""


def test_empty_variable_counts_as_unset(monkeypatch, capsys):
    monkeypatch.setenv("DATA_SDK_FOP_PARQUET_PATH", "")
    assert fop_wrapper._fop_base_path() == Path(".")
    assert "DATA_SDK_FOP_PARQUET_PATH not set" in capsys.readouterr().err


def test_tej_cache_dir_is_created(monkeypatch, tmp_path):
    monkeypatch.setenv("DATA_SDK_TEJ_CACHE_PATH", str(tmp_path / "tej" / "cache"))
    assert Path(TEJWrapper._cache_dir()).is_dir()

