"""Utility functions for data cleaning and transformation."""

from typing import Optional

from bs4 import BeautifulSoup


def clean_html(html_content: Optional[str]) -> str:
    """
    Strip HTML tags from raw string input, preserving line breaks.
    Uses lxml for high-performance parsing and cleans hidden unicode spaces.
    """
    if not html_content:
        return ""

    # Parse with 'lxml' for C-level speed instead of python's 'html.parser'
    cleaned_text = BeautifulSoup(html_content, "lxml").get_text(
        separator="\n", strip=True
    )

    # Normalize annoying non-breaking spaces (\xa0) common in E-commerce data
    return cleaned_text.replace("\xa0", " ").strip()
