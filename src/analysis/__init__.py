"""
Analysis layer
--------------

Functions responsible for generating analytical outputs from the curated dataset:

- gdp_vs_co2_scatter.png
- correlation_summary.csv
"""

from .econ_environment_analytics import (  # noqa: F401
    ANALYSIS_OUTPUT_DIR,
    CORRELATION_CSV_NAME,
    SCATTER_PNG_NAME,
    build_correlation_summary,
    build_gdp_vs_co2_scatter,
)

__all__ = [
    "ANALYSIS_OUTPUT_DIR",
    "SCATTER_PNG_NAME",
    "CORRELATION_CSV_NAME",
    "build_gdp_vs_co2_scatter",
    "build_correlation_summary",
]

