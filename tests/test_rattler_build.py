"""Tests for `rattler_build`.

`subprocess.Popen` is replaced with a small recording stand-in so no real
`rattler-build` binary ever runs; only its presence on disk (as an empty file
under `$CONDA_PREFIX/bin/`) is required to get past the existence check.
"""

from __future__ import annotations

import json
import shlex
import warnings
from pathlib import Path
from typing import TYPE_CHECKING, Any

import pytest

from rattler_bindings import rattler_build

if TYPE_CHECKING:
    from collections.abc import Callable

FIXTURES = Path(__file__).parent / "fixtures"


@pytest.fixture
def conda_prefix(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> Path:
    prefix = (tmp_path / "conda").resolve()
    (prefix / "bin").mkdir(parents=True)
    (prefix / "bin" / "rattler-build").write_text("")
    monkeypatch.setenv("CONDA_PREFIX", str(prefix))
    return prefix


@pytest.fixture
def patched_popen(
    monkeypatch: pytest.MonkeyPatch,
) -> Callable[..., list[list[Any]]]:
    """Patch `subprocess.Popen` and return an accessor for the recorded argv lists."""
    calls: list[list[Any]] = []

    def configure(stdout: str = "", stderr: str = "", returncode: int = 0) -> list[list[Any]]:
        class FakePopen:
            def __init__(self, args: list[Any], **unused_kwargs: Any) -> None:
                calls.append(list(args))
                self.returncode = returncode

            def communicate(self) -> tuple[str, str]:
                return stdout, stderr

        monkeypatch.setattr("rattler_bindings.subprocess.Popen", FakePopen)
        return calls

    configure()  # sensible default so tests that don't care can skip calling this
    return configure


def test_missing_conda_prefix_raises(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.delenv("CONDA_PREFIX", raising=False)

    with pytest.raises(FileNotFoundError, match=r"No conda prefix found."):
        rattler_build()


def test_missing_rattler_build_binary_raises(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setenv("CONDA_PREFIX", str(tmp_path))  # no bin/rattler-build under it

    with pytest.raises(FileNotFoundError, match=r"rattler-build not found."):
        rattler_build()


@pytest.mark.usefixtures("conda_prefix")
def test_non_empty_stdout_raises(patched_popen: Callable[..., Any]) -> None:
    patched_popen(stdout="unexpected chatter on stdout")

    with pytest.raises(RuntimeError, match="stdout should be empty"):
        rattler_build()


@pytest.mark.usefixtures("conda_prefix")
def test_stderr_jsonl_is_parsed_and_returncode_is_forwarded(
    patched_popen: Callable[..., Any],
) -> None:
    fixture_text = (FIXTURES / "build_logs.jsonl").read_text()
    expected_logs = [json.loads(line) for line in fixture_text.splitlines()]
    patched_popen(stderr=fixture_text, returncode=42)

    logs, code = rattler_build()

    assert logs == expected_logs
    assert code == 42


@pytest.mark.usefixtures("conda_prefix")
def test_no_test_true_emits_deprecation_warning_and_appends_flag(
    patched_popen: Callable[..., list[list[Any]]],
) -> None:
    calls = patched_popen()

    with pytest.warns(DeprecationWarning, match="no_test is deprecated"):
        rattler_build(no_test=True)

    assert "--no-test" in calls[-1]


@pytest.mark.usefixtures("conda_prefix")
def test_no_test_false_emits_no_warning(
    patched_popen: Callable[..., list[list[Any]]],
) -> None:
    calls = patched_popen()

    with warnings.catch_warnings():
        warnings.simplefilter("error")
        rattler_build(no_test=False)

    assert "--no-test" not in calls[-1]


def test_full_command_line_construction(
    conda_prefix: Path, patched_popen: Callable[..., list[list[Any]]]
) -> None:
    """All flag categories (positional, repeatable, boolean, quoted-optional, special) at once."""
    calls = patched_popen()
    recipe = conda_prefix
    output_dir = conda_prefix / "out"

    rattler_build(
        recipe=recipe,
        recipe_dir=Path("recipes"),
        up_to="my pkg",
        channels=("conda-forge", "bioconda"),
        variant_config=Path("variants.yaml"),
        verbose=2,
        quiet=True,
        ignore_recipe_variants=True,
        render_only=True,
        with_solve=True,
        wrap_log_lines="true",
        keep_build=True,
        no_build_id=True,
        compression_threads=4,
        experimental=True,
        extra_meta={"author": "Jane Doe"},
        package_format="tar-bz2",
        package_compression=5,
        no_include_recipe=True,
        color_build_log=True,
        output_dir=output_dir,
        skip_existing="all",
        noarch_build_platform="noarch",
    )

    exec_path = conda_prefix / "bin" / "rattler-build"
    expected = [
        exec_path,
        "build",
        "--recipe",
        recipe.resolve(),
        "--build-platform",
        "linux-64",
        "--host-platform",
        "linux-64",
        "--log-style",
        "fancy",
        "--color",
        "auto",
        "--test",
        "native-and-emulated",
        "--output-dir",
        output_dir.resolve(),
        "--skip-existing",
        "all",
        "--channel",
        "conda-forge",
        "--channel",
        "bioconda",
        "--verbose",
        "--verbose",
        "--quiet",
        "--ignore-recipe-variants",
        "--render-only",
        "--with-solve",
        # note: unlike every other boolean flag here, this one is sent with an
        # underscore rather than a hyphen - an existing inconsistency in the source.
        "--keep_build",
        "--no-build-id",
        "--experimental",
        "--no-include-recipe",
        "--color-build-log",
        "--recipe-dir",
        shlex.quote("recipes"),
        "--up-to",
        shlex.quote("my pkg"),
        "--variant-config",
        shlex.quote(str(Path("variants.yaml"))),
        "--wrap-log-lines",
        shlex.quote("true"),
        "--compression-threads",
        shlex.quote("4"),
        "--noarch-build-platform",
        shlex.quote("noarch"),
        "--extra-meta",
        shlex.quote("author=Jane Doe"),
        "--package-format",
        "tar-bz2:5",
    ]
    assert calls[-1] == expected


def test_default_command_line_omits_optional_and_boolean_flags(
    conda_prefix: Path, patched_popen: Callable[..., list[list[Any]]]
) -> None:
    calls = patched_popen()

    rattler_build()

    exec_path = conda_prefix / "bin" / "rattler-build"
    args = calls[-1]
    assert args[0] == exec_path
    assert "--package-format" in args
    assert "conda" in args  # no compression suffix
    for absent_flag in (
        "--quiet",
        "--render-only",
        "--with-solve",
        "--keep_build",
        "--no-build-id",
        "--experimental",
        "--no-include-recipe",
        "--color-build-log",
        "--no-test",
        "--recipe-dir",
        "--up-to",
        "--target-platform",
        "--variant-config",
        "--extra-meta",
    ):
        assert absent_flag not in args
