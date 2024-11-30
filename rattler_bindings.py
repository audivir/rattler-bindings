"""Python bindings for rattler-build."""

from __future__ import annotations

import importlib.util
import json
import logging
import os
import shlex
import shutil
import subprocess
import tempfile
import warnings
from datetime import datetime
from os import PathLike
from pathlib import Path
from subprocess import PIPE
from typing import IO, TYPE_CHECKING, Any, Literal, TypeAlias

from ruamel.yaml import YAML

if TYPE_CHECKING:
    from collections.abc import Sequence

StrPath: TypeAlias = str | PathLike[str]
"""str or PathLike"""

PLATFORM: TypeAlias = Literal[
    "linux-64", "linux-aarch64", "win-64", "osx-64", "osx-arm64", "noarch"
]
"""Possible value:
- linux-64
- linux-aarch64
- win-64
- osx-64
- osx-arm64
- noarch
"""

SRC_PREFIX = "Copying source from url: "
BLD_PREFIX = "Build variant: "

logger = logging.getLogger(__name__)


def _dump_jsonl(data: Sequence[dict[str, Any]], fd: IO[str]) -> None:
    """Dump the sequence of dicts linewise to `fd`."""
    for line in data:
        fd.write(json.dumps(line) + "\n")


def _get_package_name(recipe: StrPath) -> str:
    """Return the package name from the `recipe.yaml` file in `recipe`."""
    with (Path(recipe) / "recipe.yaml").open() as f:
        yaml = YAML(typ="rt")
        recipe_content = yaml.load(f)

    return recipe_content["package"]["name"]  # type: ignore[no-any-return]


def _remove_empty_folder(folder: StrPath) -> None:
    """Remove `folder` if it exists and is empty."""
    folder = Path(folder)
    if folder.is_dir() and not any(folder.iterdir()):
        folder.rmdir()
        logger.debug("Removed empty folder: %s", folder)


def _get_msg(log_line: dict) -> str:
    """Return the $.fields.message part from the log entry."""
    return log_line["fields"]["message"]  # type:ignore[no-any-return]


def _get_local_ts(iso_dt: str | datetime) -> int:
    """Append the local tz to UTC or naive `iso_dt` and returns its timestamp."""
    if isinstance(iso_dt, str):
        iso_dt = datetime.strptime(iso_dt, "%Y-%m-%dT%H:%M:%S.%fZ")  # noqa: DTZ007
    # Get the current local time with the system's local timezone
    local_time = datetime.now()  # noqa: DTZ005
    # Get the local timezone offset
    local_tz = local_time.astimezone().tzinfo
    updated_dt = iso_dt.astimezone(local_tz)
    updated_tz = updated_dt.tzinfo
    if updated_tz is None:
        raise ValueError("local timezone transfer failed")
    return int(updated_tz.fromutc(updated_dt).timestamp())


def _clean_output(  # noqa: C901, PLR0912
    recipe: StrPath,
    output_dir: StrPath,
    package_format: Literal["tar-bz2", "conda"],
    skip_existing: bool | Literal["all"],
    logs: Sequence[dict[str, Any]],
    clean_bld_cache: bool = True,
    clean_src_cache: bool = True,
) -> Path:
    """Remove build and source directory if provided."""
    recipe = Path(recipe).resolve()
    output_dir = Path(output_dir).resolve()
    # line 0 -> found N variants
    # line 1 -> build variant: ...
    variant_line = logs[1]

    package = _get_package_name(recipe)
    timestamp = _get_local_ts(variant_line["timestamp"])
    variant = _get_msg(variant_line).removeprefix(BLD_PREFIX)

    bld_dir = output_dir / "bld" / f"rattler-build_{package}_{timestamp}"
    not_skipped = True
    if skip_existing:
        msg = _get_msg(logs[-1])
        if msg == f"Skipping build for {variant}":
            not_skipped = False
    logger.debug("Build process was%sskipped", " not " if not_skipped else " ")

    bld_dir_exists = bld_dir.is_dir()

    if bld_dir_exists != not_skipped:
        raise ValueError("bld dir exists but it shouldnt or vice versa")

    if not_skipped:
        src_msg = next(
            msg for line in logs[2:] if (msg := _get_msg(line)).startswith(SRC_PREFIX)
        )
        src_file_str, *_ = shlex.split(src_msg.removeprefix(SRC_PREFIX))
        src_file = Path(src_file_str)

        if not src_file.is_file():
            raise RuntimeError("should not happen")

        if clean_src_cache:
            src_file.unlink()
            logger.debug("Removed source cache: %s", src_file)
        else:
            logger.debug("Source cache kept at: %s", src_file)

    if clean_src_cache:
        _remove_empty_folder(output_dir / "src_cache")

    if clean_bld_cache:
        if bld_dir_exists:
            shutil.rmtree(bld_dir)
            logger.debug("Removed build cache: %s", bld_dir)
        _remove_empty_folder(output_dir / "bld")
    elif bld_dir_exists:
        logger.debug("Build cache kept at: %s", bld_dir)

    if package_format == "tar-bz2":
        ext = ".tar.bz2"
    elif package_format == "conda":
        ext = ".conda"
    else:
        raise ValueError(f"Unknown package-format: {package_format}")

    matches = list(output_dir.rglob(f"{variant}{ext}"))
    if len(matches) != 1:
        raise RuntimeError(f"none or multiple output packages found: {matches}")
    match = matches[0]
    logger.debug("Output package located at: %s", match)
    return match


def rattler_build(  # noqa: C901, PLR0913
    recipe: StrPath = Path(),
    recipe_dir: StrPath | None = None,
    up_to: str | None = None,
    build_platform: PLATFORM = "linux-64",
    target_platform: PLATFORM | None = None,
    host_platform: PLATFORM = "linux-64",
    channels: Sequence[str] = ("conda-forge",),  # to accept multiple channels
    variant_config: Path | None = None,
    verbose: Literal[0, 1, 2, 3] = 0,  # to accept multiple verbosities
    quiet: bool = False,
    ignore_recipe_variants: bool = False,
    log_style: Literal["fancy", "json", "plain"] = "fancy",
    render_only: bool = False,
    with_solve: bool = False,
    wrap_log_lines: Literal["true", "false"] | None = None,
    color: Literal["always", "never", "auto"] = "auto",
    keep_build: bool = False,
    no_build_id: bool = False,
    compression_threads: int | None = None,
    experimental: bool = False,
    extra_meta: dict[str, str] | None = None,  # dict instead of multiple key=val pairs
    package_format: Literal["tar-bz2", "conda"] = "conda",
    package_compression: int | None = None,  # to allow better typing
    no_include_recipe: bool = False,
    no_test: bool = False,  # TODO: deprecated warning
    test: Literal["skip", "native", "native-and-emulated"] = "native-and-emulated",
    color_build_log: bool = False,
    output_dir: StrPath = Path("./output"),
    skip_existing: Literal["none", "local", "all"] = "none",
    noarch_build_platform: PLATFORM | None = None,
) -> tuple[list[dict[str, Any]], int]:
    """Build a package from a recipe.

    Helpstrings from rattler-build version 0.31.1.
    Changes marked with (MOD!) and (NEW!).

    Args:
        recipe: The recipe file or directory containing `recipe.yaml`.
            Defaults to the current directory.
            [default: .]
        recipe_dir: The directory that contains recipes.
        up_to: Build recipes up to the specified package.
        build_platform: The build platform to use for the build
            (e.g. for building with emulation, or rendering).
            [default: linux-64]
        target_platform: The target platform for the build
        host_platform: The host platform for the build.
            If set, it will be used to determine also the target_platform
            (as long as it is not noarch).
            [default: linux-64]
        channels: Add channels to search for dependencies in. (MOD!)
            [default: (conda-forge, )]
        variant_config: Variant configuration files for the build.
        verbose: Increase logging verbosity
        ignore_recipe_variants:
            Do not read the `variants.yaml` file next to a recipe.
        quiet: Decrease logging verbosity.
        log_style: Logging style.
            Possible values:
            - fancy: Use fancy logging output
            - json:  Use JSON logging output
            - plain: Use plain logging output
        render_only: Render the recipe files without executing the build.
        with_solve: Render the recipe files with solving dependencies.
        wrap_log_lines: Wrap log lines at the terminal width.
            This is automatically disabled on CI
            (by detecting the `CI` environment variable).
            [env: RATTLER_BUILD_WRAP_LOG_LINES=]
            [possible values: true, false]
        color: Enable or disable colored output from rattler-build.
            Also honors the `CLICOLOR` and `CLICOLOR_FORCE` environment variable.
            [env: RATTLER_BUILD_COLOR=]
            [default: auto]
            Possible values:
            - always: Always use colors
            - never:  Never use colors
            - auto:   Use colors when the output is a terminal
        keep_build: Keep intermediate build artifacts after the build
        no_build_id: Don't use build id(timestamp) when creating build directory name
        compression_threads: The number of threads to use for compression
            (only relevant when also using `--package-format conda`).
        experimental: Enable experimental features.
            [env: RATTLER_BUILD_EXPERIMENTAL=]
        extra_meta: Extra metadata to include in about.json.
        package_format: The package format to use for the build. (MOD!)
            Can be one of `tar-bz2` or `conda`.
            [default: conda]
        package_compression: You can also add a compression level
            to the package format e.g. `tar-bz2:<number>` (from 1 to 9)
            or `conda:<number>` (from -7 to 22). (NEW!)
        no_include_recipe: Don't store the recipe in the final package.
        no_test: Do not run tests after building
            (deprecated, use `--test=skip` instead).
        test: The strategy to use for running tests.
            [default: native-and-emulated]
            Possible values:
            - skip:                Skip the tests
            - native:              Run the tests only if the build platform is the same
                                   as the host platform. Otherwise, skip the tests.
                                   If the target platform is noarch,
                                   the tests are always executed
            - native-and-emulated: Always run the tests
        color_build_log: Don't force colors in the output of the build script
        output_dir: Output directory for build artifacts.
            [env: CONDA_BLD_PATH=]
            [default: ./output]
        skip_existing: Whether to skip packages that already exist
            in any channel If set to `none`, do not skip any packages,
            default when not specified.
            If set to `local`, only skip packages that already exist locally,
            default when using `--skip-existing.
            If set to `all`, skip packages that already exist in any channel.
            [default: none]
            Possible values:
            - none:  Do not skip any packages
            - local: Skip packages that already exist locally
            - all:   Skip packages that already exist in any channel
        noarch_build_platform: Define a "noarch platform"
            for which the noarch packages will be built for.
            The noarch builds will be skipped on the other platforms.

    Returns:
        The loaded json logs and the return code of rattler-build.

    Raises:
        FileNotFoundError: If no conda prefix is set or rattler-build is not found.
    """
    conda_prefix = os.getenv("CONDA_PREFIX")
    if not conda_prefix:
        raise FileNotFoundError("No conda prefix found.")
    rattler_build_exec = Path(conda_prefix).resolve() / "bin" / "rattler-build"

    if not rattler_build_exec.is_file():
        raise FileNotFoundError("rattler-build not found.")

    # fmt: off
    args: list[str | Path] = [
        rattler_build_exec, "build",
        "--recipe", Path(recipe).resolve(),
        "--build-platform", build_platform,
        "--host-platform", host_platform,
        "--log-style", log_style,
        "--color", color,
        "--test", test,
        "--output-dir", Path(output_dir).resolve(),
        "--skip-existing", skip_existing,
    ]
    # fmt: on

    # add channels
    for channel in channels:
        args.extend(("--channel", channel))
    # add verbosity
    args.extend("--verbose" for _ in range(verbose))

    # package

    # boolean values:
    # fmt: off
    boolean_values = [
        (quiet, "--quiet"),
        (ignore_recipe_variants, "--ignore-recipe-variants"),
        (render_only, "--render-only"),
        (with_solve, "--with-solve"),
        (keep_build, "--keep_build"),
        (no_build_id, "--no-build-id"),
        (experimental, "--experimental"),
        (no_include_recipe, "--no-include-recipe"),
        (color_build_log, "--color-build-log"),
    ]
    # fmt: on
    for arg, flag in boolean_values:
        if arg:
            args.append(flag)

    # deprecated:
    if no_test:
        warnings.warn(
            "no_test is deprecated. user test=skip instead.",
            DeprecationWarning,
            stacklevel=1,
        )
        args.append("--no-test")

    # optional values:
    # fmt: off
    optional_values: list[tuple[Any, str]] = [
        (recipe_dir, "--recipe-dir"),
        (up_to, "--up-to"),
        (target_platform, "--target-platform"),
        (variant_config, "--variant-config"),
        (wrap_log_lines, "--wrap-log-lines"),
        (compression_threads, "--compression-threads"),
        (noarch_build_platform, "--noarch-build-platform"),
    ]
    # fmt: on
    for arg, flag in optional_values:
        if arg is not None:
            args.extend((flag, shlex.quote(str(arg))))

    # special:
    if extra_meta:
        for key, value in extra_meta.items():
            args.extend(("--extra-meta", shlex.quote(f"{key}={value}")))

    # package format:
    fmt = (
        f"{package_format}:{package_compression}"
        if package_compression is not None
        else package_format
    )
    args.extend(("--package-format", fmt))

    logger.debug("rattler-build: %s", " ".join(str(x) for x in args))

    # custom strings (e.g. extra-meta are quoted)
    # else is either boolean of Paths/ints/Literals (but not enforced!)
    proc = subprocess.Popen(args, stdout=PIPE, stderr=PIPE, text=True)  # noqa: S603
    stdout, stderr = proc.communicate()

    if stdout != "":
        raise RuntimeError("stdout should be empty")

    logs = [json.loads(line) for line in stderr.splitlines()]

    return logs, proc.returncode


def optimized_rattler_build(  # noqa: PLR0913
    recipe: StrPath,
    output_dir: StrPath,
    clean_bld_cache: bool = True,
    clean_src_cache: bool = True,
    run_conda_index: bool = True,
    check: bool = True,
    build_platform: PLATFORM = "linux-64",
    target_platform: PLATFORM | None = None,
    host_platform: PLATFORM = "linux-64",
    channels: Sequence[str] = ("conda-forge",),
    keep_build: bool = False,
    compression_threads: int = 0,
    experimental: bool = False,
    extra_meta: dict[str, str] | None = None,
    package_format: Literal["tar-bz2", "conda"] = "conda",
    package_compression: int | None = None,
    no_include_recipe: bool = True,
    test: bool | Literal["native"] = True,
    skip_existing: bool | Literal["all"] = True,
    noarch_build_platform: PLATFORM | None = None,
) -> tuple[Path | None, list[dict[str, Any]], int]:
    """Build a package from a recipe with optimized arguments.

    Look at `rattler_build` for full documentation.

    Removed arguments:
        recipe_dir, up_to, variant_config, quiet, verbose,
        ignore_recipe_variants (but set to True), log_style (but set to json),
        wrap_log_lines (but set to false), color (but set to never), no_build_id
        render_only, with_solve, no_test (deprecated anyway), color_build_log

    Args:
        recipe: removed default.
        output_dir: removed default.
        clean_bld_cache: added. delete bld subfolder after successful finish.
            [default: True]
        clean_src_cache: added. delete src_cache subfolder after successful finish.
            [default: True]
        run_conda_index: added. update the `output_dir` index with conda_index
            after successfzul finish.
            [default: True]
        check: added. raise RuntimeError when rattler-build failed. [default: True]
        build_platform: forwarded.
        target_platform: forwarded.
        host_platform: forwarded.
        channels: forwarded.
        keep_build: forwarded.
        compression_threads: default set to 0.
        experimental: forwarded.
        extra_meta: forwarded.
        package_format: forwarded.
        package_compression: forwarded.
        no_include_recipe: default set to True.
        test: True -> native-and-emulated, native -> native, False -> skip.
        skip_existing: True -> local, all -> all, False -> none.
        noarch_build_platform: forwarded.

    Returns:
        The path to the build package or none if build failed,
        the loaded json logs and the return code of rattler-build.

    Raises:
        RuntimeError: If `check` is True and `rattler-build` exits non-zero.
    """
    test_map: dict[
        bool | Literal["native"], Literal["skip", "native", "native-and-emulated"]
    ] = {False: "skip", "native": "native", True: "native-and-emulated"}
    skip_existing_map: dict[bool | Literal["all"], Literal["none", "local", "all"]] = {
        False: "none",
        True: "local",
        "all": "all",
    }
    logs, code = rattler_build(
        recipe=recipe,
        build_platform=build_platform,
        target_platform=target_platform,
        host_platform=host_platform,
        channels=channels,
        ignore_recipe_variants=True,
        log_style="json",
        wrap_log_lines="false",
        color="never",
        keep_build=keep_build,
        compression_threads=compression_threads,
        experimental=experimental,
        extra_meta=extra_meta,
        package_format=package_format,
        package_compression=package_compression,
        no_include_recipe=no_include_recipe,
        test=test_map[test],
        output_dir=output_dir,
        skip_existing=skip_existing_map[skip_existing],
        noarch_build_platform=noarch_build_platform,
    )

    pkg: Path | None = None
    if code == 0:
        pkg = _clean_output(
            recipe,
            output_dir,
            package_format,
            skip_existing,
            logs,
            clean_bld_cache,
            clean_src_cache,
        )

        if run_conda_index:
            has_conda = importlib.util.find_spec("conda_index")
            if has_conda:
                import conda_index.api

                conda_index.api.update_index(output_dir)
                logger.debug("conda_index run on %s", output_dir)
            else:
                logger.warning("conda_index not available. indexing skipped.")
    else:
        logger.error(
            "rattler-build failed. directories not removed. check returned logs."
        )
        if check:
            with tempfile.NamedTemporaryFile("w") as f:
                _dump_jsonl(logs, f)
            raise RuntimeError(f"rattler-build failed. logs written to: {f.name}")
    return pkg, logs, code


__all__ = ["PLATFORM", "optimized_rattler_build", "rattler_build"]
