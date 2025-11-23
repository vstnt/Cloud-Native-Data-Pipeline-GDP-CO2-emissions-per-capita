from __future__ import annotations

from pathlib import Path

from .models import (
    Column,
    DataSource,
    DataSourceType,
    Dataset,
    MetadataStore,
)


def build_default_metadata_store() -> MetadataStore:
    store = MetadataStore()

    world_bank_base_url = "https://api.worldbank.org/v2"

    gdp_source = DataSource(
        name="World Bank API - GDP per capita (current US$)",
        type=DataSourceType.API,
        uri=f"{world_bank_base_url}/country/all/indicator/NY.GDP.PCAP.CD",
        description=(
            "World Bank GDP per capita (current US$) indicator "
            "(NY.GDP.PCAP.CD)."
        ),
    )

    gdp_columns = [
        Column(
            name="country_code",
            dtype="string",
            description="ISO 3-letter country code.",
            nullable=False,
        ),
        Column(
            name="country_name",
            dtype="string",
            description="Country or region name.",
            nullable=False,
        ),
        Column(
            name="year",
            dtype="int",
            description="Reference year of the observation.",
            nullable=False,
        ),
        Column(
            name="gdp_per_capita_usd",
            dtype="float",
            description="GDP per capita in current US dollars.",
            nullable=True,
        ),
    ]

    gdp_dataset = Dataset(
        id="gdp_per_capita",
        name="GDP per capita (current US$)",
        description=(
            "World Bank GDP per capita (current US$) dataset, built from "
            "indicator NY.GDP.PCAP.CD."
        ),
        source=gdp_source,
        columns=gdp_columns,
        tags=["gdp", "per_capita", "world_bank", "economic"],
        extra_metadata={
            "source_organization": "World Bank",
            "indicator_code": "NY.GDP.PCAP.CD",
            "update_frequency": "annual",
            "raw_format": "json",
            "value_field_name": "gdp_per_capita_usd",
        },
    )

    co2_source = DataSource(
        name="Our World In Data - CO2 dataset",
        type=DataSourceType.FILE,
        uri="https://raw.githubusercontent.com/owid/co2-data/master/owid-co2-data.csv",
        description=(
            "Our World In Data CO2 dataset (owid-co2-data), which includes "
            "CO2 emissions per capita."
        ),
    )

    co2_columns = [
        Column(
            name="country_code",
            dtype="string",
            description="ISO 3-letter country code.",
            nullable=False,
        ),
        Column(
            name="country_name",
            dtype="string",
            description="Country or region name.",
            nullable=False,
        ),
        Column(
            name="year",
            dtype="int",
            description="Reference year of the observation.",
            nullable=False,
        ),
        Column(
            name="co2_per_capita",
            dtype="float",
            description="CO2 emissions in metric tons per capita.",
            nullable=True,
        ),
    ]

    co2_dataset = Dataset(
        id="co2_emissions_per_capita",
        name="CO2 emissions (metric tons per capita)",
        description=(
            "Our World In Data CO2 emissions (metric tons per capita) dataset, "
            "derived from the owid-co2-data repository."
        ),
        source=co2_source,
        columns=co2_columns,
        tags=["co2", "per_capita", "our_world_in_data", "emissions", "climate"],
        extra_metadata={
            "source_organization": "Our World In Data",
            "original_dataset": "owid-co2-data",
            "update_frequency": "annual",
            "raw_format": "csv",
            "value_field_name": "co2_per_capita",
            "country_code_field": "iso_code",
            "country_name_field": "country",
            "year_field": "year",
        },
    )

    store.register_dataset(gdp_dataset)
    store.register_dataset(co2_dataset)

    return store


def generate_metadata_file(output_path: Path | None = None) -> Path:
    if output_path is None:
        output_path = Path("config") / "datasets_metadata.json"

    store = build_default_metadata_store()
    store.save_to_file(output_path)
    return output_path


def main() -> None:
    generate_metadata_file()


if __name__ == "__main__":
    main()
