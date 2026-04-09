"""Unit tests for utility functions."""

from utils import clean_html


def test_clean_html():
    """Test HTML stripping logic."""
    assert clean_html("<p>Hello <b>World</b>!</p>") == "Hello World!"
    assert clean_html("") == ""
    assert clean_html(None) == ""
    assert clean_html("<div>  Spacing   test  </div>") == "Spacing test"
