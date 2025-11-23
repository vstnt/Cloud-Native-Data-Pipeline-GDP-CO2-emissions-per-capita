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

## 3. Deploy do Lambda (container image)

Pré-requisitos (no seu WSL):

- Docker instalado e rodando
- AWS CLI v2 configurado (`aws configure`) com conta/região corretas

Passos:

1) Ajuste as variáveis no arquivo `.env` na raiz do projeto (já existem exemplos):

   - `PIPELINE_S3_BUCKET`
   - `PIPELINE_S3_BASE_PREFIX`
   - `PIPELINE_METADATA_TABLE`

2) Construa a imagem, publique no ECR e faça o deploy do stack CloudFormation:

   - Bash (WSL):

     `cloud/lambda/build_and_deploy.sh`

   Esse script irá:
   - Criar/usar um repositório ECR (`gdp-co2-pipeline`).
   - Buildar a imagem do Lambda (base `python:3.11` da AWS Lambda) com todo o código e dependências.
   - Publicar a imagem no ECR.
   - Criar/atualizar o stack CloudFormation com:
     - IAM Role mínima para S3 (bucket/prefixo) e DynamoDB (tabela).
     - Função Lambda com `PackageType: Image`.
     - Regra de agendamento EventBridge (diária às 02:00 UTC por padrão).

Parâmetros do deploy (opcionais via env):

- `STACK_NAME` (default `gdp-co2-pipeline`)
- `STACK_SUFFIX` (default `gdp-co2`)
- `AWS_REGION` (default `us-east-1`)
- `SCHEDULE_EXPRESSION` (ex.: `rate(1 day)`)
- `MEMORY_SIZE` (default `2048`)
- `TIMEOUT` (default `900`)

Saída: o comando mostra os outputs do stack, incluindo nome da função.

## 4. Teste rápido (invocar manualmente)

Após o deploy, você pode invocar manualmente:

- Descobrir o nome da função (caso não tenha notado nos outputs):
  `aws cloudformation describe-stacks --stack-name gdp-co2-pipeline --query 'Stacks[0].Outputs[?OutputKey==\`LambdaFunctionName\`].OutputValue' --output text`

- Invocar sem payload (usa anos padrão):
  `aws lambda invoke --function-name <nome-da-funcao> --payload '{}' out.json`

- Invocar com intervalo de anos restrito:
  `aws lambda invoke --function-name <nome-da-funcao> --payload '{"min_year": 2000, "max_year": 2023}' out.json`

O arquivo `out.json` conterá o `statusCode` e um resumo dos artefatos.

## 5. Execução da pipeline cloud (visão geral)

- A função `run_cloud_pipeline` em `src/cloud_pipeline.py` orquestra:
  1. Ingesta RAW da World Bank (S3).
  2. Processamento PROCESSED da World Bank (S3).
  3. Geração do `country_mapping` (S3).
  4. Crawler RAW da Wikipedia (S3).
  5. Processamento PROCESSED da Wikipedia (S3, com mapping).
  6. Camada CURATED (S3).
  7. Analytical Output local, lendo CURATED via S3.

O evento da Lambda pode opcionalmente conter `min_year` e `max_year` para limitar o intervalo de anos da ingestão da World Bank.

Observações:
- Os artefatos analíticos são gravados no filesystem efêmero do Lambda (pasta `analysis/`). Isso é intencional no escopo do case; se quiser persistir esses artefatos em S3, posso adaptar para salvá‑los via `StorageAdapter` também.
