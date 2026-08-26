"""Tests for `clean_output`.

Log fixtures are built by hand rather than replayed from a static file because
each scenario needs the on-disk build/source-cache layout to line up exactly
with what the log lines claim (same timestamp, same variant, same source path).
"""

from __future__ import annotations

from pathlib import Path
from typing import Any, Literal

import pytest

from rattler_bindings import BLD_PREFIX, SRC_PREFIX, clean_output, get_local_ts

FIXTURES = Path(__file__).parent / "fixtures"
PACKAGE = "my-package"  # matches tests/fixtures/recipe.yaml
VARIANT = "python_3.11_h1234abc_0"
TIMESTAMP_ISO = "2024-07-15T12:00:00.000000Z"
TIMESTAMP = get_local_ts(TIMESTAMP_ISO)


def build_logs(
    *, skipped: bool = False, src_file: Path, final_message: str = "Build and test complete"
) -> list[dict[str, Any]]:
    """Build a minimal, realistic rattler-build JSONL log sequence."""
    last_message = f"Skipping build for {VARIANT}" if skipped else final_message
    return [
        {"fields": {"message": "found 1 variants"}},
        {"timestamp": TIMESTAMP_ISO, "fields": {"message": f"{BLD_PREFIX}{VARIANT}"}},
        {"fields": {"message": f"{SRC_PREFIX}{src_file} to work directory"}},
        {"fields": {"message": last_message}},
    ]


def bld_dir_of(output_dir: Path) -> Path:
    return output_dir / "bld" / f"rattler-build_{PACKAGE}_{TIMESTAMP}"


@pytest.fixture
def output_dir(tmp_path: Path) -> Path:
    out = (tmp_path / "output").resolve()
    out.mkdir()
    return out


@pytest.mark.parametrize("package_format", ["conda", "tar-bz2"])
@pytest.mark.parametrize("skip_existing", [False, True, "all"])
def test_full_build_removes_caches_and_locates_package(
    output_dir: Path,
    package_format: Literal["conda", "tar-bz2"],
    skip_existing: bool | Literal["all"],
) -> None:
    ext = ".conda" if package_format == "conda" else ".tar.bz2"
    src_cache_dir = output_dir / "src_cache"
    src_cache_dir.mkdir()
    src_file = src_cache_dir / "my-package-1.2.3.tar.gz"
    src_file.write_bytes(b"tarball")
    bld_dir = bld_dir_of(output_dir)
    bld_dir.mkdir(parents=True)
    (bld_dir / "work").mkdir()
    pkg_file = output_dir / f"{VARIANT}{ext}"
    pkg_file.write_bytes(b"package")
    # skip_existing being truthy still leads to a real (not skipped) build here,
    # since the final log message does not match "Skipping build for ...".
    logs = build_logs(skipped=False, src_file=src_file)

    result = clean_output(FIXTURES, output_dir, package_format, skip_existing, logs)

    assert result == pkg_file
    assert not src_file.exists()
    assert not src_cache_dir.exists()  # emptied and removed
    assert not bld_dir.exists()
    assert not bld_dir.parent.exists()  # "bld" folder emptied and removed


@pytest.mark.parametrize("skip_existing", [True, "all"])
def test_skipped_build_leaves_no_build_or_source_dir_but_still_finds_package(
    output_dir: Path, skip_existing: bool | Literal["all"]
) -> None:
    # A skipped build never ran rattler-build's build step, so neither the bld dir
    # nor a fresh source-cache download exist; the package from a previous run does.
    pkg_file = output_dir / f"{VARIANT}.conda"
    pkg_file.write_bytes(b"package")
    src_file = output_dir / "src_cache" / "my-package-1.2.3.tar.gz"  # deliberately absent
    logs = build_logs(skipped=True, src_file=src_file)

    result = clean_output(FIXTURES, output_dir, "conda", skip_existing, logs)

    assert result == pkg_file


def test_clean_src_cache_false_keeps_source_file(output_dir: Path) -> None:
    src_cache_dir = output_dir / "src_cache"
    src_cache_dir.mkdir()
    src_file = src_cache_dir / "my-package-1.2.3.tar.gz"
    src_file.write_bytes(b"tarball")
    bld_dir_of(output_dir).mkdir(parents=True)
    pkg_file = output_dir / f"{VARIANT}.conda"
    pkg_file.write_bytes(b"package")
    logs = build_logs(skipped=False, src_file=src_file)

    clean_output(FIXTURES, output_dir, "conda", False, logs, clean_src_cache=False)

    assert src_file.exists()
    assert src_cache_dir.exists()


def test_clean_bld_cache_false_with_skipped_build_touches_nothing(output_dir: Path) -> None:
    # No bld dir was ever created (the build was skipped), and clean_bld_cache=False
    # means the "keep it" logging branch is also never taken - nothing to clean up.
    pkg_file = output_dir / f"{VARIANT}.conda"
    pkg_file.write_bytes(b"package")
    src_file = output_dir / "src_cache" / "my-package-1.2.3.tar.gz"
    logs = build_logs(skipped=True, src_file=src_file)

    result = clean_output(FIXTURES, output_dir, "conda", True, logs, clean_bld_cache=False)

    assert result == pkg_file
    assert not bld_dir_of(output_dir).exists()


def test_clean_bld_cache_false_keeps_build_dir(output_dir: Path) -> None:
    src_cache_dir = output_dir / "src_cache"
    src_cache_dir.mkdir()
    src_file = src_cache_dir / "my-package-1.2.3.tar.gz"
    src_file.write_bytes(b"tarball")
    bld_dir = bld_dir_of(output_dir)
    bld_dir.mkdir(parents=True)
    pkg_file = output_dir / f"{VARIANT}.conda"
    pkg_file.write_bytes(b"package")
    logs = build_logs(skipped=False, src_file=src_file)

    clean_output(FIXTURES, output_dir, "conda", False, logs, clean_bld_cache=False)

    assert bld_dir.exists()


@pytest.mark.parametrize(
    ("skip_existing", "skipped", "create_bld_dir"),
    [
        pytest.param(False, False, False, id="not-skipped-but-bld-dir-missing"),
        pytest.param(True, True, True, id="skipped-but-bld-dir-present"),
    ],
)
def test_bld_dir_state_inconsistent_with_logs_raises(
    output_dir: Path, skip_existing: bool, skipped: bool, create_bld_dir: bool
) -> None:
    src_file = output_dir / "src_cache" / "my-package-1.2.3.tar.gz"
    if create_bld_dir:
        bld_dir_of(output_dir).mkdir(parents=True)
    logs = build_logs(skipped=skipped, src_file=src_file)

    with pytest.raises(ValueError, match="bld dir exists but it shouldnt or vice versa"):
        clean_output(FIXTURES, output_dir, "conda", skip_existing, logs)


def test_missing_source_file_raises_runtime_error(output_dir: Path) -> None:
    bld_dir_of(output_dir).mkdir(parents=True)
    src_file = output_dir / "src_cache" / "never-downloaded.tar.gz"  # not created
    logs = build_logs(skipped=False, src_file=src_file)

    with pytest.raises(RuntimeError, match="should not happen"):
        clean_output(FIXTURES, output_dir, "conda", False, logs)


def test_unknown_package_format_raises(output_dir: Path) -> None:
    bld_dir_of(output_dir).mkdir(parents=True)
    src_cache_dir = output_dir / "src_cache"
    src_cache_dir.mkdir()
    src_file = src_cache_dir / "my-package-1.2.3.tar.gz"
    src_file.write_bytes(b"tarball")
    logs = build_logs(skipped=False, src_file=src_file)

    with pytest.raises(ValueError, match="Unknown package-format: zip"):
        clean_output(FIXTURES, output_dir, "zip", False, logs)  # type: ignore[arg-type]


@pytest.mark.parametrize(
    "package_files",
    [
        pytest.param([], id="no-match"),
        pytest.param(["a/pkg.conda", "b/pkg.conda"], id="ambiguous-match"),
    ],
)
def test_non_unique_package_match_raises(output_dir: Path, package_files: list[str]) -> None:
    bld_dir_of(output_dir).mkdir(parents=True)
    src_cache_dir = output_dir / "src_cache"
    src_cache_dir.mkdir()
    src_file = src_cache_dir / "my-package-1.2.3.tar.gz"
    src_file.write_bytes(b"tarball")
    # place zero or two files literally named "<variant>.conda" in distinct subdirectories
    for i, _ in enumerate(package_files):
        d = output_dir / f"subdir{i}"
        d.mkdir()
        (d / f"{VARIANT}.conda").write_bytes(b"package")
    logs = build_logs(skipped=False, src_file=src_file)

    with pytest.raises(RuntimeError, match="none or multiple output packages found"):
        clean_output(FIXTURES, output_dir, "conda", False, logs)
