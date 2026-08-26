"""Tests for `get_local_ts`.

`get_local_ts` round-trips its input through `astimezone(local_tz)` followed by
`local_tz.fromutc(...)`. For a naive `datetime` (or the equivalent `...Z`-suffixed
string, which `strptime` parses into a naive `datetime` since the trailing `Z` is
consumed as a literal character, not a `%Z` timezone marker) those two operations
cancel out, so the function returns the *correct* absolute UTC timestamp regardless
of the process's local timezone. For an already tz-aware UTC `datetime`, they do
*not* cancel: the result is shifted by the current local UTC offset. This is the
behavior under test, not merely branch coverage.
"""

from __future__ import annotations

from datetime import datetime, timezone
from typing import TYPE_CHECKING

import pytest

if TYPE_CHECKING:
    from collections.abc import Callable

from rattler_bindings import get_local_ts

# A summer instant: any timezone with a July 15 DST period picks it up, so the
# offset used below is exactly what `Etc/GMT-2`'s (fixed, DST-less) offset predicts.
ISO_STR = "2024-07-15T12:00:00.000000Z"
CORRECT_EPOCH = int(datetime(2024, 7, 15, 12, 0, 0, tzinfo=timezone.utc).timestamp())


@pytest.mark.parametrize("tz", ["UTC", "Etc/GMT-2", "Etc/GMT+5"])
@pytest.mark.parametrize(
    "iso_dt",
    [ISO_STR, datetime(2024, 7, 15, 12, 0, 0)],  # noqa: DTZ001 - naive on purpose
    ids=["str", "naive-datetime"],
)
def test_naive_or_string_input_returns_correct_utc_epoch_regardless_of_local_tz(
    set_tz: Callable[[str], None], tz: str, iso_dt: str | datetime
) -> None:
    set_tz(tz)

    assert get_local_ts(iso_dt) == CORRECT_EPOCH


def test_aware_utc_datetime_input_is_shifted_by_the_current_local_offset(
    set_tz: Callable[[str], None],
) -> None:
    # Etc/GMT-2 is a fixed +02:00 offset (no DST), so the offset "right now" (the
    # moment the function calls `datetime.now()`) equals the offset for any other
    # date too, making the expected shift deterministic.
    set_tz("Etc/GMT-2")
    aware = datetime(2024, 7, 15, 12, 0, 0, tzinfo=timezone.utc)

    result = get_local_ts(aware)

    assert result == CORRECT_EPOCH + 2 * 3600
