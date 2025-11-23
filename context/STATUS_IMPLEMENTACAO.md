# Status da Implementação do Projeto

_Última atualização: 23/11/2025_

## Estrutura de pastas

- Criadas pastas principais:
  - `src/` (código-fonte da pipeline)
  - `config/` (configurações e metadata)
  - `data/raw`, `data/intermediate`, `data/processed`
  - `infra/terraform`, `infra/k8s`
  - `docs/`
  - `tests/unit`, `tests/integration`
- Estrutura de módulos em `src/pipeline/`:
  - `ingestion/`
  - `processing/`
  - `storage/`
  - `orchestration/`
  - `metadata/`

## Módulo de Metadata

Arquivos principais:
- `src/pipeline/metadata/models.py`
  - `DataSourceType`, `DataSource`, `Column`, `Dataset`
  - `MetadataStore` (registro em memória com salvar/carregar em JSON)
- `src/pipeline/metadata/bootstrap.py`
  - `build_default_metadata_store()`: registra dois datasets:
    - `gdp_per_capita` (World Bank API – indicador `NY.GDP.PCAP.CD`)
    - `co2_emissions_per_capita` (Our World In Data – `owid-co2-data.csv`)
  - `generate_metadata_file()`: gera `config/datasets_metadata.json`

Arquivo gerado:
- `config/datasets_metadata.json` com metadata de GDP e CO₂.

## Módulo de Ingestão

Arquivo principal:
- `src/pipeline/ingestion/world_bank.py`
  - `fetch_dataset_from_world_bank(dataset)`: busca dados do World Bank (JSON paginado).
  - `fetch_dataset_from_owid_co2(dataset)`: baixa e lê CSV de CO₂ da Our World In Data.
  - `save_rows_to_csv(...)`: salva dados em CSV em `data/raw/`.
  - `ingest_dataset(dataset_id, ...)`: escolhe a fonte (World Bank ou OWID) com base no metadata.
  - `ingest_default_datasets(...)` e `main()`: ingestão padrão de GDP e CO₂.

Arquivos de dados gerados em `data/raw/`:
- `gdp_per_capita.csv`
- `co2_emissions_per_capita.csv`

## Próximo passo sugerido

- Implementar módulo de **processing** para:
  - Carregar `gdp_per_capita.csv` e `co2_emissions_per_capita.csv`.
  - Unificar por país/ano.
  - Preparar dataset analítico para consumo por orquestração/análises.

