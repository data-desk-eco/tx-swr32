"""Shared utilities for RRC web scraping."""

import re


def get_viewstate(text: str) -> str:
    """Extract JSF ViewState token from HTML/AJAX response."""
    m = re.search(r'name="javax\.faces\.ViewState"[^/]*value="([^"]*)"', text)
    if m:
        return m.group(1)
    m = re.search(r'javax\.faces\.ViewState:0">(.*?)]]', text)
    return m.group(1).replace("<![CDATA[", "") if m else ""
