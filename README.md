# Cloud-Native-Data-Pipeline-GDP-CO2-emissions-per-capita
Cloud-Native Data Pipeline Using Python and Public Data Sources

## Execução local (Parte 1)

- Instalar dependências: `pip install -r requirements.txt`
- Rodar pipeline local end‑to‑end:
  - PowerShell: `$env:PYTHONPATH='src'; python -m local_pipeline`
  - Bash: `PYTHONPATH=src python -m local_pipeline`

Isso gera arquivos em `raw/`, `processed/`, `curated/` e `analysis/`.

## Preparação para execução em cloud (Parte 2)

A pipeline cloud reutiliza a mesma lógica, mas com S3 + DynamoDB via `src/cloud_pipeline.py`.

Variáveis de ambiente esperadas (também configuradas em `.env` local):

- `PIPELINE_S3_BUCKET` – nome do bucket S3 onde ficarão RAW/PROCESSED/CURATED.
- `PIPELINE_S3_BASE_PREFIX` – prefixo lógico opcional dentro do bucket (ex.: `gdp-co2-pipeline`).
- `PIPELINE_METADATA_TABLE` – nome da tabela DynamoDB usada pelo `DynamoMetadataAdapter`.

Exemplo de handler para AWS Lambda:

- Handler: `cloud_pipeline.lambda_handler`

Para detalhes passo‑a‑passo da parte cloud, ver `cloud/instructions.md`.
