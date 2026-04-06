"""Utility functions for data cleaning and transformation."""

from typing import Optional

from bs4 import BeautifulSoup


def clean_html(html_content: Optional[str]) -> str:
    """
    Strip HTML tags from raw string input, preserving line breaks.
    Also removes extra spaces at the beginning and end.
    """
    if not html_content:
        return ""

    # Use \n to keep paragraph structure, and strip() to clean edges
    return (
        BeautifulSoup(html_content, "html.parser")
        .get_text(separator="\n", strip=True)
        .strip()
    )
