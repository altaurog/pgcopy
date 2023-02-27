import pytest

from pgcopy import util


@pytest.mark.parametrize(
    "arr,info",
    [
        ([], (1, 0)),
        ([1, 2], (1, 2)),
        ([[1, 2], [3, 4], [5, 6]], (2, 3, 2)),
    ],
)
def test_array_info(arr, info):
    assert util.array_info(arr) == info


def test_bad_array():
    with pytest.raises(ValueError, match="subarray dimensions must match"):
        util.array_info([1, [2]])


@pytest.mark.parametrize(
    "arr,flat",
    [
        ([], []),
        ([1, 2], [1, 2]),
        ([[1, 2], [3, 4]], [1, 2, 3, 4]),
        ([[[1], [2]]], [1, 2]),
    ],
)
def test_flatten(arr, flat):
    assert list(util.array_iter(arr)) == flat
