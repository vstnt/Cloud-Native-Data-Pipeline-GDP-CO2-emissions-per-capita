# Resumo da implementação atual

Documento de apoio para manter o contexto do que já foi implementado até o momento, em complemento ao checklist de `context/STATUS_IMPLEMENTACAO.md` e aos PDFs do case/plano.

## 1. Estrutura geral do projeto

- Pastas principais criadas:
  - `src/metadata` – abstração local de metadados/checkpoints (mock de DynamoDB via JSON).
  - `src/adapters` – abstrações de I/O:
    - `StorageAdapter` (Local/S3).
    - `MetadataAdapter` (local/DynamoDB).
  - `src/ingestion_api` – ingestão da World Bank API (camada RAW).
  - `src/crawler` – crawler da Wikipedia para CO₂ per capita (camada RAW).
  - `src/transformations` – transformações para camadas PROCESSED e CURATED (World Bank, Wikipedia e mapping de países).
  - `src/analysis` – geração de artefatos analíticos (Analytical Output).
  - `src/local_pipeline.py` – orquestração local end-to-end da pipeline.
  - `raw/` – arquivos de entrada em formato RAW (JSONL).
  - `processed/` – saídas processadas em formato Parquet, com particionamento por ano ou tipo de dado.
  - `curated/` – camada curated/analytical (join GDP x CO₂ por país-ano).
  - `analysis/` – artefatos analíticos (gráficos, CSVs de correlação).
  - `docs/` – diagramas/apresentação/documentação final.
  - `cloud/` – placeholders para IAM, variáveis de ambiente e instruções de infraestrutura.

Essa estrutura está alinhada ao desenho proposto no plano (camadas RAW → PROCESSED → CURATED/ANALYTICAL, pensando em migração posterior para S3/DynamoDB/Lambda).

## 2. Módulo de METADATA (local, mock de DynamoDB)

- Localização: `src/metadata/`.
- Principais funções expostas via `metadata.__init__`:
  - `start_run(run_scope)`
  - `end_run(ingestion_run_id, status=..., rows_processed=..., last_checkpoint=..., error_message=...)`
  - `save_checkpoint(source, value)`
  - `load_checkpoint(source, default=None)`
  - Helpers: `get_last_run`, `list_runs`, `get_all_checkpoints`, `reset_local_store`.
- Implementa um store local em `local_metadata.json` (mock de tabela DynamoDB), com:
  - Histórico de execuções (`runs`, incluindo status, timestamps, linhas processadas).
  - Checkpoints genéricos (`checkpoints`, por exemplo último ano carregado).
- Scopes padronizados definidos:
  - `WORLD_BANK_API_SCOPE = "world_bank_api"`
  - `WIKIPEDIA_CO2_SCOPE = "wikipedia_co2"`
  - `CURATED_JOIN_SCOPE = "curated_join"`
- Esse módulo é utilizado pela ingestão da World Bank, pelo crawler da Wikipedia e pela camada CURATED.

### 2.1 Adapters de metadata (MetadataAdapter)

- Localização: `src/adapters/metadata.py`.
- Interface:
  - `class MetadataAdapter` – define:
    - `start_run(run_scope) -> str`
    - `end_run(ingestion_run_id, status="SUCCESS", rows_processed=None, last_checkpoint=None, error_message=None) -> dict`
    - `save_checkpoint(source, value)`
    - `load_checkpoint(source, default=None)`
    - `list_runs(run_scope=None) -> list[dict]`
- Implementações:
  - `LocalMetadataAdapter` – encapsula o módulo `metadata` atual (`local_metadata.json`).
  - `DynamoMetadataAdapter` – pronta para uso com uma tabela DynamoDB real, padrão:
    - `pk = "RUN#<ingestion_run_id>"` / `pk = "CHECKPOINT#<source>"`.
    - `sk = "META"`.
- Hoje a execução local usa `LocalMetadataAdapter`; a `DynamoMetadataAdapter` foi desenhada para a Parte 2 (AWS).

### 2.2 Adapters de storage (StorageAdapter)

- Localização: `src/adapters/storage.py`.
- Interface:
  - `class StorageAdapter` – define:
    - `write_raw(key: str, content: bytes) -> str`
    - `read_raw(key: str) -> bytes`
    - `write_parquet(df, key: str) -> str`
    - `read_parquet(key: str) -> DataFrame`
    - `list_keys(prefix: str) -> list[str]`
- Implementações:
  - `LocalStorageAdapter(root_dir=".")`:
    - Interpreta `key` como caminho relativo sob `root_dir` (ex.: `raw/world_bank_gdp/...`).
  - `S3StorageAdapter(bucket, base_prefix=None)`:
    - Usa `boto3.client("s3")` para ler/gravar sob `s3://bucket/[base_prefix]/<key>`.
- A execução local usa `LocalStorageAdapter`; a `S3StorageAdapter` será usada na Parte 2 (AWS) para mapear os mesmos layouts lógicos para S3.

## 3. Ingestão RAW da World Bank API (GDP per capita)

- Localização: `src/ingestion_api/world_bank_ingestion.py`.
- Responsabilidades implementadas:
  - Chamada à API do World Bank para o indicador `NY.GDP.PCAP.CD` (GDP per capita, current US$).
  - Paginação completa da API (`fetch_all_indicator_records`).
  - Cálculo de `record_hash` (SHA1 do payload normalizado).
  - Lógica incremental por ano, usando checkpoint:
    - Chave de checkpoint: `last_year_loaded_world_bank` (`WORLD_BANK_CHECKPOINT_KEY`).
    - Checkpoint armazenado via `MetadataAdapter.save_checkpoint` / `load_checkpoint`.
  - Enriquecimento dos registros RAW com:
    - `ingestion_run_id`, `igestion_ts`, `data_source`, `raw_payload`, `record_hash`, `raw_file_path`.
  - Persistência dos dados RAW em JSONL usando `StorageAdapter`:
    - Prefixo lógico: `RAW_BASE_PREFIX = "raw/world_bank_gdp"`.
    - Chave: `raw/world_bank_gdp/world_bank_gdp_raw_<timestamp>.jsonl`.
    - Localmente, essas chaves mapeiam 1:1 para caminhos em disco.
  - Registro completo da execução em `local_metadata.json` via `MetadataAdapter.start_run` / `end_run`.
- Assinatura atual:
  - `ingest_world_bank_gdp_raw(storage: StorageAdapter, metadata: MetadataAdapter, min_year=None, max_year=None) -> str`
  - Retorna a chave lógica do arquivo RAW (que, no ambiente local, coincide com o path em `raw/`).
- Há múltiplos arquivos RAW em `raw/world_bank_gdp/`, indicando que a ingestão foi testada/rodada localmente, inclusive para anos 2000–2023.

## 4. Processamento World Bank → camada PROCESSED

- Localização: `src/transformations/world_bank_gdp_processed.py`.
- Funcionalidades principais:
  - Leitura de arquivos RAW JSONL gerados por `ingest_world_bank_gdp_raw`.
  - Conversão para schema tabular PROCESSED:
    - `country_code`, `country_name`, `year`, `gdp_per_capita_usd`,
      `indicator_id`, `indicator_name`, `ingestion_run_id`, `ingestion_ts`, `data_source`.
  - Tipagem explícita (inteiro, float, timestamp, string) alinhada ao plano.
- Persistência em Parquet particionado por ano:
  - Prefixo lógico PROCESSED: `PROCESSED_BASE_PREFIX = "processed/world_bank_gdp"`.
  - Layout: `processed/world_bank_gdp/year=<ano>/processed_worldbank_gdp_per_capita.parquet`.
- Funções de alto nível:
  - `build_world_bank_gdp_dataframe(raw_file_path)`
  - `save_world_bank_gdp_parquet_partitions(df, output_dir=..., storage=None)`
    - Com `storage is None`: grava em disco local (`output_dir`).
    - Com `storage` (ex.: S3): usa `storage.write_parquet` e retorna as chaves lógicas.
  - `process_world_bank_gdp_raw_file(raw_file_path, output_dir=..., storage=None)`
- Existem partições geradas para múltiplos anos em `processed/world_bank_gdp/`, evidenciando execuções locais de teste.

## 5. Crawler RAW da Wikipedia (CO₂ per capita)

- Localização: `src/crawler/wikipedia_co2_crawler.py`.
- Responsabilidades implementadas:
  - Download da página oficial de emissões per capita da Wikipedia:
    - `https://en.wikipedia.org/wiki/List_of_countries_by_carbon_dioxide_emissions_per_capita`
  - Identificação da tabela principal de emissões per capita.
  - Limpeza de células (remover footnotes, normalizar espaços).
  - Conversão do HTML da tabela em uma lista de linhas “sujas” (`raw_table_json`).
  - Construção de um registro RAW alinhado ao schema definido no plano:
    - `ingestion_run_id`, `ingestion_ts`, `data_source = "wikipedia_co2"`,
      `page_url`, `table_html`, `raw_table_json`, `record_hash`, `raw_file_path`.
  - Persistência do RAW em JSONL via `StorageAdapter`:
    - Prefixo lógico: `RAW_BASE_PREFIX = "raw/wikipedia_co2"`.
    - Arquivo: `wikipedia_co2_raw_<timestamp>.jsonl`.
  - Uso de `MetadataAdapter` para registrar runs (`start_run` / `end_run` com `rows_processed`).
- Assinatura atual:
  - `crawl_wikipedia_co2_raw(storage: StorageAdapter, metadata: MetadataAdapter, url=..., timeout=30, run_scope=...) -> str`
  - Retorna a chave lógica do arquivo RAW (no ambiente local, coincide com o path em `raw/`).
- Há arquivos RAW em `raw/wikipedia_co2/`, resultado de execuções locais do crawler.

## 6. Processamento Wikipedia CO₂ → camada PROCESSED

- Localização: `src/transformations/wikipedia_co2_processed.py`.
- Principais pontos implementados:
  - Leitura do JSONL RAW gerado pelo crawler.
  - Normalização de nomes de países (`normalize_country_name`) para facilitar joins:
    - lower, remoção de acentos, remoção de pontuação, colapso de espaços.
  - Interpretação da estrutura da tabela (originalmente larga) e conversão para formato longo:
    - Geração de uma linha por `(country, year)` (anos de interesse: 2000 e 2023).
  - Parsing robusto de valores numéricos de emissões:
    - Tratamento de traços, células vazias, strings especiais (NA/N/A), separadores de milhar etc.
  - Construção de registros PROCESSED com schema:
    - `country_name`, `country_name_normalized`, `country_code` (opcional),
      `year`, `co2_tons_per_capita`, `notes`, `ingestion_run_id`, `ingestion_ts`, `data_source`.
  - Tipagem explícita das colunas no `DataFrame`.
  - Integração opcional com um `country_mapping` (quando fornecido) para preencher `country_code` e padronizar `country_name`.
- Persistência em Parquet particionado por ano:
  - Prefixo lógico PROCESSED: `PROCESSED_BASE_PREFIX = "processed/wikipedia_co2"`.
  - Layout: `processed/wikipedia_co2/year=<ano>/processed_wikipedia_co2_per_capita.parquet`.
- Funções de alto nível:
  - `build_wikipedia_co2_dataframe(raw_file_path, country_mapping=None)`
  - `save_wikipedia_co2_parquet_partitions(df, output_dir=..., storage=None)`
    - Local: escreve Parquet em `processed/wikipedia_co2/...`.
    - Com `storage`: permite gravar diretamente em S3 via `StorageAdapter`.
  - `process_wikipedia_co2_raw_file(raw_file_path, output_dir=..., country_mapping=None, storage=None)`
- Atualmente existem partições geradas em:
  - `processed/wikipedia_co2/year=2000/`
  - `processed/wikipedia_co2/year=2023/`

## 7. Mapping de países (country_mapping)

- Localização: `src/transformations/country_mapping.py`.
- Arquivo de overrides manuais: `src/transformations/country_mapping_overrides.csv`.
- Objetivo: gerar um mapping canônico de países para ser usado na camada CURATED e nos joins entre GDP e CO₂:
  - Base: PROCESSED da World Bank (`processed/world_bank_gdp`).
  - Saída: Parquet único em `processed/country_mapping/country_mapping.parquet`.
- Pipeline implementado:
  1. Leitura de todos os Parquet em `processed/world_bank_gdp/`.
  2. Extração de `country_code` e `country_name`.
  3. Cálculo de `country_name_normalized` via `normalize_country_name` (compartilhado com o módulo da Wikipedia).
  4. Remoção de duplicados por `country_name_normalized`.
  5. Atribuição de `source_precedence = "world_bank"` para esses registros.
  6. Leitura opcional de overrides em CSV:
     - Colunas esperadas: `country_name_normalized`, `country_code`, `country_name`.
     - Overrides têm prioridade sobre a base e recebem `source_precedence = "override"`.
  7. Persistência do resultado em Parquet:
     - Diretório: `processed/country_mapping/`
     - Arquivo: `country_mapping.parquet`.
- Funções utilitárias disponíveis:
  - `build_country_mapping_from_world_bank_parquet(processed_dir=...)`
  - `build_country_mapping(processed_dir=..., overrides_path=...)`
  - `save_country_mapping_parquet(mapping_df, output_dir=...)`
  - `build_and_save_country_mapping_from_world_bank(...)`
  - `load_country_mapping(path=None)` (usa o caminho padrão se não informado).
- Há um arquivo `processed/country_mapping/country_mapping.parquet`, indicando que o pipeline foi rodado depois da ingestão 2000–2023.

## 8. Camada CURATED (Economic & Environmental by country-year)

- Localização: `src/transformations/curated_econ_environment_country_year.py`.
- Objetivo: materializar o dataset `curated_econ_environment_country_year`, combinando os indicadores econômicos (GDP per capita) e ambientais (CO₂ per capita) por `(country_code, year)`.
- Fontes de entrada (camada PROCESSED):
  - World Bank GDP per capita:
    - Local: `processed/world_bank_gdp/year=*/processed_worldbank_gdp_per_capita.parquet`.
    - Cloud: via keys sob `WORLD_BANK_PROCESSED_BASE_PREFIX`.
  - Wikipedia CO₂ per capita:
    - Local: `processed/wikipedia_co2/year=*/processed_wikipedia_co2_per_capita.parquet`.
    - Cloud: via keys sob `WIKIPEDIA_CO2_PROCESSED_BASE_PREFIX`.
- Regras principais implementadas:
  - Join por `(country_code, year)` entre os dois datasets.
  - Cálculo de `co2_per_1000usd_gdp`:
    - `co2_tons_per_capita * 1000 / gdp_per_capita_usd` quando ambos válidos e `gdp_per_capita_usd > 0`.
  - Criação de campos de auditoria/origem:
    - `gdp_source_system = "world_bank_api"`
    - `co2_source_system = "wikipedia_co2"`
    - `first_ingestion_run_id`, `last_update_run_id`, `last_update_ts`.
- Layout de saída (camada CURATED, pensado para mapear 1:1 para S3):
  - Diretório/prefixo raiz: `CURATED_OUTPUT_DIR = "curated/env_econ_country_year"` / `CURATED_BASE_PREFIX = "curated/env_econ_country_year"`.
  - Estrutura:
    - `curated/env_econ_country_year/year=<ano>/snapshot_date=<YYYYMMDD>/curated_econ_environment_country_year.parquet`
- Funções de alto nível:
  - `build_curated_econ_environment_country_year_dataframe(world_bank_df, wikipedia_df, curated_run_id, snapshot_ts)` – join/derivações.
  - `build_curated_econ_environment_country_year_from_processed(..., storage=None)` – lê PROCESSED (local ou via `StorageAdapter`) e monta o DataFrame curated em memória.
  - `save_curated_econ_environment_country_year_parquet_partitions(df, output_dir=..., snapshot_date=..., storage=None)` – persiste o DataFrame curated:
    - Local: em `curated/env_econ_country_year/...`.
    - Com `storage`: em S3 (ou outro backend) via `StorageAdapter`.
  - `build_and_save_curated_econ_environment_country_year(..., storage=None)` – pipeline completo da camada CURATED:
    - Abre um run no `metadata` com escopo `CURATED_JOIN_SCOPE`.
    - Constrói o DataFrame a partir do PROCESSED.
    - Salva os Parquet.
    - Registra `rows_processed` + checkpoint (`snapshot_date=YYYYMMDD`) em `local_metadata.json`.
- Depois de reprocessar as camadas PROCESSED (World Bank 2000–2023 e Wikipedia com mapping aplicado), existem arquivos curated populados, por exemplo:
  - `curated/env_econ_country_year/year=2000/snapshot_date=20251123/...`
  - `curated/env_econ_country_year/year=2023/snapshot_date=20251123/...`

## 9. Analytical Output (artefatos analíticos)

- Localização: `src/analysis/econ_environment_analytics.py` e `src/analysis/__init__.py`.
- Objetivo: gerar os artefatos analíticos descritos na seção “Analytical Output” do plano a partir do dataset curated:
  - Artefato 1: `gdp_vs_co2_scatter.png`
  - Artefato 2: `correlation_summary.csv`
- Layout de saída (local, mas pensado para mapear facilmente a um prefixo S3 se necessário):
  - Diretório raiz de análise: `analysis/`
  - Arquivos:
    - `analysis/gdp_vs_co2_scatter.png`
    - `analysis/correlation_summary.csv`
- Dependência adicional: `matplotlib` (adicionada em `requirements.txt`) para geração do scatter.
- Leitura da camada CURATED:
  - Local: via caminhos em `curated/env_econ_country_year/...`.
  - Cloud: via `StorageAdapter` (quando fornecido), usando prefixo `CURATED_BASE_PREFIX`.

### 9.1 Scatterplot GDP vs CO₂ (`gdp_vs_co2_scatter.png`)

- Função principal: `build_gdp_vs_co2_scatter(curated_root=..., output_dir=..., year=2023, storage=None)` (também acessível via CLI `python -m analysis.econ_environment_analytics`).
- Fonte de dados: camada CURATED, filtrando apenas o ano 2023.
- Configuração do gráfico:
  - Eixo X: `gdp_per_capita_usd`.
  - Eixo Y: `co2_tons_per_capita`.
  - Cor dos pontos: `co2_per_1000usd_gdp` (colormap `viridis`).
  - Título: `"GDP vs CO2 per capita - 2023"`.
  - Grade leve para facilitar leitura.
- Saída:
  - Arquivo PNG salvo em `analysis/gdp_vs_co2_scatter.png`.

### 9.2 Correlation Summary (`correlation_summary.csv`)

- Função principal: `build_correlation_summary(curated_root=..., output_dir=..., years=(2000, 2023), storage=None)`.
- Fonte de dados: camada CURATED para os anos 2000 e 2023.
- Para cada ano, a função:
  - Filtra linhas com `gdp_per_capita_usd` e `co2_tons_per_capita` válidos.
  - Calcula `pearson_correlation_gdp_co2` usando correlação de Pearson entre GDP per capita e CO₂ per capita.
  - Calcula:
    - `top5_countries_highest_co2_per_1000usd_gdp`: países com maior `co2_per_1000usd_gdp` (top 5), como string com nomes separados por `;`.
    - `top5_countries_lowest_co2_per_1000usd_gdp`: países com menor `co2_per_1000usd_gdp` (top 5), também separados por `;`.
- Output:
  - CSV salvo em `analysis/correlation_summary.csv` com colunas:
    - `year`
    - `pearson_correlation_gdp_co2`
    - `top5_countries_highest_co2_per_1000usd_gdp`
    - `top5_countries_lowest_co2_per_1000usd_gdp`

## 10. Orquestração LOCAL (entrypoint local)

- Localização: `src/local_pipeline.py`.
- Objetivo: encadear toda a pipeline local (Passo 9 do checklist):
  1. Ingestão World Bank API (RAW).
  2. Processamento World Bank → PROCESSED.
  3. Geração do `country_mapping`.
  4. Crawler da Wikipedia (RAW).
  5. Processamento Wikipedia → PROCESSED (com mapping).
  6. Camada CURATED (join GDP x CO₂).
  7. Analytical Output (scatter + correlation summary).
- Uso de adapters:
  - Instancia `LocalStorageAdapter()` e `LocalMetadataAdapter()` uma única vez.
  - Passa esses adapters para:
    - `ingest_world_bank_gdp_raw(storage, metadata, ...)`.
    - `crawl_wikipedia_co2_raw(storage, metadata, ...)`.
  - As transformações/curated/analysis são chamadas com o modo local padrão (`storage=None`), mas já estão preparadas para receber um `StorageAdapter` no futuro (modo cloud).
- CLI:
  - `PYTHONPATH=src python -m local_pipeline`
  - Argumentos opcionais:
    - `--min-year`, `--max-year` para limitar o intervalo de ingestão da World Bank.
- Retorno:
  - Um dicionário com as principais chaves de artefatos gerados (`world_bank_raw`, `world_bank_processed`, `country_mapping`, `wikipedia_raw`, `wikipedia_processed`, `curated`, `analysis`).

## 11. Itens ainda não implementados (alto nível)

Com base em `context/STATUS_IMPLEMENTACAO.md` e no código atual:

- Parte 1 (execução local):
  - Itens (1)–(9) estão **concluídos**, incluindo:
    - Módulo de metadata local.
    - Ingestão RAW (World Bank).
    - Processamento PROCESSED (World Bank/Wikipedia).
    - Mapping de países.
    - Camada CURATED.
    - Analytical Output.
    - Orquestração local (`src/local_pipeline.py`).
- Parte 2 (AWS) – ainda não iniciados:
  - S3 real (estrutura de buckets/prefixos).
  - Tabela DynamoDB real.
  - Função AWS Lambda, IAM Role, regra EventBridge (agendamento diário).
  - Testes de execução na nuvem (end-to-end).
  - Preenchimento da documentação final em `docs/` (documento e apresentação).

Os adapters (`StorageAdapter`, `MetadataAdapter`, `S3StorageAdapter`, `DynamoMetadataAdapter`) já foram criados exatamente para facilitar a implementação desses itens da Parte 2 sem reescrever a lógica principal da pipeline.

## 12. Como usar este resumo

- `STATUS_IMPLEMENTACAO.md` responde a **quais grandes passos já foram concluídos**.
- Este `RESUMO_IMPLEMENTACAO.md` responde a **como cada passo foi implementado e onde está no código/arquivos**.
- À medida que avançarmos na Parte 2 (AWS), podemos:
  - Marcar os itens correspondentes como concluídos em `STATUS_IMPLEMENTACAO.md`.
  - Adicionar ou ajustar subseções aqui, descrevendo brevemente:
    - Módulos criados/alterados.
    - Paths relevantes (S3, DynamoDB, Lambda).
    - Decisões importantes de design (por exemplo, nomes de buckets, estrutura de prefixos, chaves de partição da tabela DynamoDB).


## 13. Progresso recente na Parte 2 (cloud/AWS)

Desde a versao inicial deste resumo, a parte 2 recebeu preparacao de codigo (entrypoint cloud, .env e adaptacao das transformacoes), mas os recursos AWS ainda nao foram criados.

- Novo entrypoint cloud: src/cloud_pipeline.py (run_cloud_pipeline e lambda_handler), orquestrando a pipeline com S3StorageAdapter + DynamoMetadataAdapter e lendo variaveis PIPELINE_S3_BUCKET, PIPELINE_S3_BASE_PREFIX, PIPELINE_METADATA_TABLE.
- Loader de .env: src/env_loader.py (load_dotenv_if_present), usado por cloud_pipeline, ingestion_api/world_bank_ingestion.py (WORLD_BANK_INDICATOR) e crawler/wikipedia_co2_crawler.py (WIKIPEDIA_URL).
- Transformacoes prontas para ler RAW via StorageAdapter: world_bank_gdp_processed e wikipedia_co2_processed aceitam um StorageAdapter para ler JSONL (ex.: de S3).
- Country mapping preparado para S3: country_mapping.py usa COUNTRY_MAPPING_BASE_PREFIX e pode montar o mapping a partir de Parquets lidos via StorageAdapter.

Estado atual: o codigo esta pronto para ser empacotado em uma Lambda (handler cloud_pipeline.lambda_handler); falta apenas criar bucket S3, tabela DynamoDB, IAM Role e EventBridge, e configurar as mesmas variaveis de ambiente usadas no .env.
