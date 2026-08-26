"""Tests for `optimized_rattler_build`.

`rattler_build` itself is mocked throughout (it is exercised in
`test_rattler_build.py`), so these tests focus on: the `test`/`skip_existing`
value mapping, the opinionated defaults it bakes into the `rattler_build` call,
the success/failure branching, the `run_conda_index` branch, and the `check`
branch. One end-to-end-ish test exercises the real `clean_output` against a
real `tmp_path` layout to confirm the positional argument forwarding is correct.
"""

from __future__ import annotations

import importlib.util
import sys
from pathlib import Path
from typing import Any, Literal
from unittest.mock import MagicMock

import pytest

import rattler_bindings
from rattler_bindings import BLD_PREFIX, SRC_PREFIX, get_local_ts, optimized_rattler_build

FIXTURES = Path(__file__).parent / "fixtures"
PACKAGE = "my-package"  # matches tests/fixtures/recipe.yaml
VARIANT = "python_3.11_h1234abc_0"
TIMESTAMP_ISO = "2024-07-15T12:00:00.000000Z"
TIMESTAMP = get_local_ts(TIMESTAMP_ISO)


def build_logs(*, src_file: Path) -> list[dict[str, Any]]:
    return [
        {"fields": {"message": "found 1 variants"}},
        {"timestamp": TIMESTAMP_ISO, "fields": {"message": f"{BLD_PREFIX}{VARIANT}"}},
        {"fields": {"message": f"{SRC_PREFIX}{src_file} to work directory"}},
        {"fields": {"message": "Build and test complete"}},
    ]


@pytest.fixture
def mock_rattler_build(monkeypatch: pytest.MonkeyPatch) -> MagicMock:
    mock = MagicMock(return_value=([], 0))
    monkeypatch.setattr(rattler_bindings, "rattler_build", mock)
    return mock


@pytest.fixture
def mock_clean_output(monkeypatch: pytest.MonkeyPatch) -> MagicMock:
    mock = MagicMock(return_value=Path("/fake/package.conda"))
    monkeypatch.setattr(rattler_bindings, "clean_output", mock)
    return mock


@pytest.mark.parametrize(
    ("test", "expected_test"),
    [(True, "native-and-emulated"), (False, "skip"), ("native", "native")],
)
@pytest.mark.parametrize(
    ("skip_existing", "expected_skip"),
    [(True, "local"), (False, "none"), ("all", "all")],
)
@pytest.mark.usefixtures("mock_clean_output")
def test_test_and_skip_existing_are_mapped_to_rattler_build_literals(
    mock_rattler_build: MagicMock,
    test: bool | Literal["native"],
    expected_test: str,
    skip_existing: bool | Literal["all"],
    expected_skip: str,
) -> None:
    optimized_rattler_build(
        "recipe",
        "out",
        run_conda_index=False,
        test=test,
        skip_existing=skip_existing,
    )

    kwargs = mock_rattler_build.call_args.kwargs
    assert kwargs["test"] == expected_test
    assert kwargs["skip_existing"] == expected_skip


@pytest.mark.usefixtures("mock_clean_output")
def test_bakes_in_opinionated_defaults_not_exposed_as_parameters(
    mock_rattler_build: MagicMock,
) -> None:
    optimized_rattler_build("recipe", "out", run_conda_index=False)

    kwargs = mock_rattler_build.call_args.kwargs
    assert kwargs["ignore_recipe_variants"] is True
    assert kwargs["log_style"] == "json"
    assert kwargs["wrap_log_lines"] == "false"
    assert kwargs["color"] == "never"


def test_failed_build_with_check_true_raises_and_skips_cleanup(
    mock_rattler_build: MagicMock, mock_clean_output: MagicMock
) -> None:
    mock_rattler_build.return_value = ([{"fields": {"message": "boom"}}], 1)

    with pytest.raises(RuntimeError, match=r"rattler-build failed. logs written to:"):
        optimized_rattler_build("recipe", "out", check=True)

    mock_clean_output.assert_not_called()


def test_failed_build_with_check_false_returns_none_package_without_raising(
    mock_rattler_build: MagicMock, mock_clean_output: MagicMock
) -> None:
    logs = [{"fields": {"message": "boom"}}]
    mock_rattler_build.return_value = (logs, 1)

    pkg, returned_logs, code = optimized_rattler_build("recipe", "out", check=False)

    assert pkg is None
    assert returned_logs == logs
    assert code == 1
    mock_clean_output.assert_not_called()


@pytest.mark.usefixtures("mock_rattler_build", "mock_clean_output")
def test_run_conda_index_false_skips_indexing_entirely(monkeypatch: pytest.MonkeyPatch) -> None:
    find_spec = MagicMock()
    monkeypatch.setattr(importlib.util, "find_spec", find_spec)

    optimized_rattler_build("recipe", "out", run_conda_index=False)

    find_spec.assert_not_called()


@pytest.mark.usefixtures("mock_rattler_build", "mock_clean_output")
def test_run_conda_index_true_but_conda_index_unavailable_does_not_import_it(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(importlib.util, "find_spec", lambda unused_name: None)
    # Block the real import outright: if the "unavailable" guard were broken and
    # the code tried `import conda_index.api` anyway, this makes that raise
    # ImportError instead of silently succeeding (conda_index happens to be
    # installed in this dev environment).
    monkeypatch.setitem(sys.modules, "conda_index", None)
    monkeypatch.setitem(sys.modules, "conda_index.api", None)

    pkg, _, code = optimized_rattler_build("recipe", "out", run_conda_index=True)

    assert code == 0
    assert pkg == Path("/fake/package.conda")


@pytest.mark.usefixtures("mock_rattler_build", "mock_clean_output")
def test_run_conda_index_true_and_available_calls_update_index(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    conda_index_api = pytest.importorskip("conda_index.api")
    monkeypatch.setattr(
        importlib.util,
        "find_spec",
        lambda name: object() if name == "conda_index" else None,
    )
    mock_update_index = MagicMock()
    monkeypatch.setattr(conda_index_api, "update_index", mock_update_index)

    optimized_rattler_build("recipe", "out", run_conda_index=True)

    mock_update_index.assert_called_once_with("out")


def test_successful_build_forwards_arguments_to_real_clean_output(
    mock_rattler_build: MagicMock, tmp_path: Path
) -> None:
    """Exercise the real (unmocked) `clean_output`.

    Confirms the positional argument order `optimized_rattler_build` uses when
    calling it.
    """
    output_dir = (tmp_path / "output").resolve()
    output_dir.mkdir()
    src_cache_dir = output_dir / "src_cache"
    src_cache_dir.mkdir()
    src_file = src_cache_dir / "my-package-1.2.3.tar.gz"
    src_file.write_bytes(b"tarball")
    bld_dir = output_dir / "bld" / f"rattler-build_{PACKAGE}_{TIMESTAMP}"
    bld_dir.mkdir(parents=True)
    pkg_file = output_dir / f"{VARIANT}.conda"
    pkg_file.write_bytes(b"package")
    mock_rattler_build.return_value = (build_logs(src_file=src_file), 0)

    pkg, _, code = optimized_rattler_build(
        FIXTURES, output_dir, run_conda_index=False, skip_existing=False
    )

    assert code == 0
    assert pkg == pkg_file
    assert not src_file.exists()
    assert not bld_dir.exists()
