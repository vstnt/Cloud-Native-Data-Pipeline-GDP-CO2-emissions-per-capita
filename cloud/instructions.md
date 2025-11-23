# Cloud instructions (Parte 2)

Este arquivo documenta como configurar as variáveis de ambiente e a execução da pipeline em modo cloud (S3 + DynamoDB).

## 1. `.env` local para desenvolvimento

O arquivo `.env` na raiz do projeto contém placeholders que você deve ajustar antes de usar:

- `PIPELINE_S3_BUCKET` – bucket S3 onde os dados da pipeline serão gravados.
- `PIPELINE_S3_BASE_PREFIX` – prefixo lógico opcional dentro do bucket (ex.: `gdp-co2-pipeline`).
- `PIPELINE_METADATA_TABLE` – nome da tabela DynamoDB de metadados (runs + checkpoints).

Exemplo (já criado em `.env`):

```bash
PIPELINE_S3_BUCKET=your-gdp-co2-bucket
PIPELINE_S3_BASE_PREFIX=gdp-co2-pipeline
PIPELINE_METADATA_TABLE=gdp_co2_metadata
```

Use seu mecanismo preferido para carregar esse `.env` localmente (plugin do IDE, `python-dotenv`, etc.) ou exporte as variáveis manualmente.

## 2. Configuração esperada na AWS

Ao criar a função AWS Lambda que executará a pipeline cloud:

- Defina as mesmas variáveis de ambiente (`PIPELINE_S3_BUCKET`, `PIPELINE_S3_BASE_PREFIX`, `PIPELINE_METADATA_TABLE`) na configuração da função.
- Use como handler: `cloud_pipeline.lambda_handler`.
- Garanta que a role da Lambda tenha permissões de:
  - `s3:GetObject`, `s3:PutObject`, `s3:ListBucket` no bucket/prefixo configurado.
  - `dynamodb:PutItem`, `dynamodb:GetItem`, `dynamodb:UpdateItem`, `dynamodb:Scan` na tabela de metadata.

## 3. Execução da pipeline cloud (visão geral)

- A função `run_cloud_pipeline` em `src/cloud_pipeline.py` orquestra:
  1. Ingesta RAW da World Bank (S3).
  2. Processamento PROCESSED da World Bank (S3).
  3. Geração do `country_mapping` (S3).
  4. Crawler RAW da Wikipedia (S3).
  5. Processamento PROCESSED da Wikipedia (S3, com mapping).
  6. Camada CURATED (S3).
  7. Analytical Output local, lendo CURATED via S3.

O evento da Lambda pode opcionalmente conter `min_year` e `max_year` para limitar o intervalo de anos da ingestão da World Bank.
