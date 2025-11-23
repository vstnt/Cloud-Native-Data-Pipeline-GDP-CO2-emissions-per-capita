"""
Crawler modules
---------------

Responsáveis por extrair dados de fontes web (ex.: Wikipedia)
e gravar a camada RAW correspondente.
"""

from .wikipedia_co2_crawler import (  # noqa: F401
    RAW_BASE_PREFIX,
    crawl_wikipedia_co2_raw,
    fetch_wikipedia_co2_html,
    find_co2_table_html,
    parse_co2_table_rows,
)

__all__ = [
    "RAW_BASE_PREFIX",
    "crawl_wikipedia_co2_raw",
    "fetch_wikipedia_co2_html",
    "find_co2_table_html",
    "parse_co2_table_rows",
]
