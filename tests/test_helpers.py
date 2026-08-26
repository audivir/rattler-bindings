"""Tests for the small, pure-logic helper functions."""

from __future__ import annotations

import io
from pathlib import Path
from typing import Any

import pytest

from rattler_bindings import dump_jsonl, get_msg, get_package_name, remove_empty_folder

FIXTURES = Path(__file__).parent / "fixtures"


def test_dump_jsonl_writes_one_compact_json_object_per_line() -> None:
    data: list[dict[str, Any]] = [
        {"a": 1, "b": "x"},
        {"nested": {"c": [1, 2, 3]}},
    ]
    fd = io.StringIO()

    dump_jsonl(data, fd)

    # Each dict becomes its own compact-json line, in input order, newline-terminated
    # (including the last line), so the file is directly re-readable line by line.
    assert fd.getvalue() == '{"a": 1, "b": "x"}\n{"nested": {"c": [1, 2, 3]}}\n'


def test_dump_jsonl_with_empty_sequence_writes_nothing() -> None:
    fd = io.StringIO()

    dump_jsonl([], fd)

    assert fd.getvalue() == ""


def test_get_package_name_reads_nested_package_name_from_recipe_yaml() -> None:
    name = get_package_name(FIXTURES)

    assert name == "my-package"


def test_remove_empty_folder_removes_only_when_empty(tmp_path: Path) -> None:
    empty = tmp_path / "empty"
    empty.mkdir()
    nonempty = tmp_path / "nonempty"
    nonempty.mkdir()
    (nonempty / "file.txt").write_text("x")

    remove_empty_folder(empty)
    remove_empty_folder(nonempty)

    assert not empty.exists()
    assert nonempty.is_dir()
    assert (nonempty / "file.txt").exists()


def test_remove_empty_folder_is_a_noop_when_folder_does_not_exist(tmp_path: Path) -> None:
    missing = tmp_path / "does-not-exist"

    remove_empty_folder(missing)  # must not raise

    assert not missing.exists()


@pytest.mark.parametrize(
    ("log_line", "expected"),
    [
        ({"fields": {"message": "hello"}}, "hello"),
        ({"fields": {"message": "Build variant: python=3.11"}}, "Build variant: python=3.11"),
    ],
)
def test_get_msg_extracts_fields_message(log_line: dict[str, Any], expected: str) -> None:
    assert get_msg(log_line) == expected
