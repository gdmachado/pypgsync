import pytest

from pypgsync.utils import attrs_to_uri, intervals


def test_attrs_to_uri_with_valid_input():
    """Test behavior of attrs_to_uri method when receiving valid input."""
    assert attrs_to_uri('user', 'passwd', 'host', 'port',
                        'db') == 'postgresql://user:passwd@host:port/db'


def test_attrs_to_uri_with_invalid_input():
    """Test behavior of attrs_to_uri method when receiving invalid input."""
    with pytest.raises(ValueError):
        attrs_to_uri('', 'passwd', 'host', 'port', 'db')


def test_intervals():
    """Test behavior of intervals method when receiving valid input."""
    assert list(intervals(1, 1, 5)) == [(1, 1)]
    assert list(intervals(1, 10, 5)) == [(1, 5), (6, 10)]

def test_intervals():
    """Test behavior of intervals method when receiving valid input."""
    with pytest.raises(ValueError):
        list(intervals(10, 1, 5))
