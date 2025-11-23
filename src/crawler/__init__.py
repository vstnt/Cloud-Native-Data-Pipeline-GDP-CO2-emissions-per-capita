"""
Crawler modules
---------------

Responsáveis por extrair dados de fontes web (ex.: Wikipedia)
e gravar a camada RAW correspondente.
"""

from .wikipedia_co2_crawler import (  # noqa: F401
    RAW_OUTPUT_DIR as WIKIPEDIA_RAW_OUTPUT_DIR,
    crawl_wikipedia_co2_raw,
    fetch_wikipedia_co2_html,
    find_co2_table_html,
    parse_co2_table_rows,
)

__all__ = [
    "WIKIPEDIA_RAW_OUTPUT_DIR",
    "crawl_wikipedia_co2_raw",
    "fetch_wikipedia_co2_html",
    "find_co2_table_html",
    "parse_co2_table_rows",
]

