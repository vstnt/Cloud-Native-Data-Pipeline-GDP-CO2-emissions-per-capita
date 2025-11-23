# Resumo da implementação atual

Documento de apoio para manter o contexto do que já foi implementado até o momento, em complemento ao checklist de `context/STATUS_IMPLEMENTACAO.md` e aos PDFs do case/plano.

## 1. Estrutura geral do projeto

- Pastas principais criadas:
  - `src/metadata` – abstração local de metadados/checkpoints (mock de DynamoDB).
  - `src/ingestion_api` – ingestão da World Bank API (camada RAW).
  - `src/crawler` – crawler da Wikipedia para CO₂ per capita (camada RAW).
  - `src/transformations` – transformações para camada PROCESSED (World Bank, Wikipedia e mapping de países).
  - `raw/` – arquivos de entrada em formato RAW (JSONL).
  - `processed/` – saídas processadas em formato Parquet, com particionamento por ano ou tipo de dado.
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
- Esse módulo já é utilizado pela ingestão da World Bank e pelo crawler da Wikipedia.

## 3. Ingestão RAW da World Bank API (GDP per capita)

- Localização: `src/ingestion_api/world_bank_ingestion.py`.
- Responsabilidades implementadas:
  - Chamada à API do World Bank para o indicador `NY.GDP.PCAP.CD`.
  - Paginação completa da API (`fetch_all_indicator_records`).
  - Cálculo de `record_hash` (SHA1 do payload normalizado).
  - Lógica incremental por ano, usando checkpoint:
    - Chave de checkpoint: `last_year_loaded_world_bank` (`WORLD_BANK_CHECKPOINT_KEY`).
    - Checkpoint armazenado no módulo `metadata`.
  - Enriquecimento dos registros RAW com:
    - `ingestion_run_id`, `ingestion_ts`, `data_source`, `raw_payload`, `record_hash`, `raw_file_path`.
  - Persistência dos dados RAW em JSONL:
    - Diretório: `raw/world_bank_gdp/`
    - Nome de arquivo: `world_bank_gdp_raw_<timestamp>.jsonl`.
  - Registro completo da execução em `local_metadata.json` (start_run/end_run com status, linhas, checkpoint final).
- Já existem arquivos RAW em `raw/world_bank_gdp/`, indicando que a ingestão foi testada/rodada localmente.

## 4. Processamento World Bank → camada PROCESSED

- Localização: `src/transformations/world_bank_gdp_processed.py`.
- Funcionalidades principais:
  - Leitura de arquivos RAW JSONL gerados por `ingest_world_bank_gdp_raw`.
  - Conversão para schema tabular PROCESSED:
    - `country_code`, `country_name`, `year`, `gdp_per_capita_usd`,
      `indicator_id`, `indicator_name`, `ingestion_run_id`, `ingestion_ts`, `data_source`.
  - Tipagem explícita (inteiro, float, timestamp, string) alinhada ao plano.
  - Salvamento em Parquet particionado por ano:
    - Diretório raiz: `processed/world_bank_gdp/`
    - Layout: `processed/world_bank_gdp/year=<ano>/processed_worldbank_gdp_per_capita.parquet`.
- Funções de alto nível já disponíveis:
  - `build_world_bank_gdp_dataframe(raw_file_path)`
  - `save_world_bank_gdp_parquet_partitions(df, output_dir=...)`
  - `process_world_bank_gdp_raw_file(raw_file_path, output_dir=...)`
- Existem partições geradas em `processed/world_bank_gdp/year=1960/`, evidenciando execução local de teste.

## 5. Crawler RAW da Wikipedia (CO₂ per capita)

- Localização: `src/crawler/wikipedia_co2_crawler.py`.
- Responsabilidades implementadas:
  - Download da página oficial de emissões de CO₂ per capita da Wikipedia (`WIKIPEDIA_CO2_URL`).
  - Seleção da tabela correta (classe `wikitable` + heurísticas em caption/conteúdo).
  - Limpeza de células (remover footnotes, normalizar espaços).
  - Conversão do HTML da tabela em uma lista de linhas “sujas” (`raw_table_json`).
  - Construção de um registro RAW alinhado ao schema definido no plano:
    - `ingestion_run_id`, `ingestion_ts`, `data_source = "wikipedia_co2"`,
      `page_url`, `table_html`, `raw_table_json`, `record_hash`, `raw_file_path`.
  - Persistência da camada RAW em JSONL:
    - Diretório: `raw/wikipedia_co2/`
    - Arquivo: `wikipedia_co2_raw_<timestamp>.jsonl`.
  - Uso do módulo `metadata` para registrar runs (`start_run` / `end_run` com `rows_processed`).
- Já existe pelo menos um arquivo RAW em `raw/wikipedia_co2/`, resultado de execução local do crawler.

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
    - Diretório raiz: `processed/wikipedia_co2/`
    - Layout: `processed/wikipedia_co2/year=<ano>/processed_wikipedia_co2_per_capita.parquet`.
- Funções de alto nível:
  - `build_wikipedia_co2_dataframe(raw_file_path, country_mapping=None)`
  - `save_wikipedia_co2_parquet_partitions(df, output_dir=...)`
  - `process_wikipedia_co2_raw_file(raw_file_path, output_dir=..., country_mapping=None)`
- Já existem partições geradas em:
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
- Já existe um arquivo `processed/country_mapping/country_mapping.parquet`, indicando que o pipeline foi rodado pelo menos uma vez.

## 8. Itens ainda não implementados (alto nível)

Com base em `context/STATUS_IMPLEMENTACAO.md` e no código atual:

- Ainda não implementados / em aberto na Parte 1 (local):
  - (7) Camada CURATED (join GDP x CO₂ com mapping de países e regras de negócio do plano).
  - (8) Analytical Output (módulos em `src/analysis/` ainda vazios).
  - (9) Orquestração local (entrypoint que encadeia ingestão → processed → curated → análise).
- Ainda não iniciados na Parte 2 (AWS):
  - S3 real (estrutura de buckets/prefixos).
  - Tabela DynamoDB real.
  - Lambda, IAM Role, EventBridge (agendamento) e testes de execução na nuvem.
  - Preenchimento da documentação final em `docs/final_documentation.md` e apresentação em `docs/presentation.pdf`.

## 9. Como usar este resumo

- `STATUS_IMPLEMENTACAO.md` responde “quais grandes passos já foram concluídos?”.
- Este `RESUMO_IMPLEMENTACAO.md` responde “como cada passo foi implementado e onde está no código/arquivos?”.
- Ao avançarmos (por exemplo, implementando a camada CURATED e o Analytical Output), podemos:
  - Marcar o item como concluído em `STATUS_IMPLEMENTACAO.md`.
  - Adicionar uma subseção aqui descrevendo brevemente o que foi feito (módulos, paths, decisões importantes).

