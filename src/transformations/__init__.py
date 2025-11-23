"""
Transformations layer
----------------------

Módulos responsáveis por converter dados da camada RAW em estruturas
tipadas e normalizadas para a camada PROCESSED/curated.
"""

from .world_bank_gdp_processed import (  # noqa: F401
    PROCESSED_OUTPUT_DIR as WORLD_BANK_PROCESSED_OUTPUT_DIR,
    build_world_bank_gdp_dataframe,
    process_world_bank_gdp_raw_file,
    save_world_bank_gdp_parquet_partitions,
)
from .wikipedia_co2_processed import (  # noqa: F401
    PROCESSED_OUTPUT_DIR as WIKIPEDIA_CO2_PROCESSED_OUTPUT_DIR,
    build_wikipedia_co2_dataframe,
    normalize_country_name,
    process_wikipedia_co2_raw_file,
    save_wikipedia_co2_parquet_partitions,
)

__all__ = [
    "WORLD_BANK_PROCESSED_OUTPUT_DIR",
    "WIKIPEDIA_CO2_PROCESSED_OUTPUT_DIR",
    "build_world_bank_gdp_dataframe",
    "build_wikipedia_co2_dataframe",
    "process_world_bank_gdp_raw_file",
    "process_wikipedia_co2_raw_file",
    "save_world_bank_gdp_parquet_partitions",
    "save_wikipedia_co2_parquet_partitions",
    "normalize_country_name",
]
