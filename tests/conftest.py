"""Shared fixtures for the rattler_bindings test suite."""

from __future__ import annotations

import time
from typing import TYPE_CHECKING

import pytest

if TYPE_CHECKING:
    from collections.abc import Callable, Iterator


@pytest.fixture
def set_tz(monkeypatch: pytest.MonkeyPatch) -> Iterator[Callable[[str], None]]:
    """Yield a setter that switches the process local timezone for the test.

    Restores the previous timezone once the test finishes, since `time.tzset`
    changes process-wide C library state that outlives `monkeypatch`'s own
    environment variable restoration.
    """
    if not hasattr(time, "tzset"):  # pragma: no cover
        # Defensive: time.tzset only exists on Unix (not Windows); this suite
        # runs on macOS/Linux, so there's no realistic way to exercise this.
        pytest.skip("time.tzset is not available on this platform")

    def _set(tz: str) -> None:
        monkeypatch.setenv("TZ", tz)
        time.tzset()

    yield _set
    time.tzset()
