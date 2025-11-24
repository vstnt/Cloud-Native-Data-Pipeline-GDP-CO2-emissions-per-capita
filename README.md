# Cloud-Native-Data-Pipeline-GDP-CO2-emissions-per-capita
Cloud-Native Data Pipeline Using Python and Public Data Sources

## Architecture Overview

- Public data sources:
  - World Bank API (GDP per capita — indicator `NY.GDP.PCAP.CD`).
  - Wikipedia (CO₂ per capita — main table scraping).
- Serverless orchestration:
  - Amazon EventBridge schedules the run (daily, configurable).
  - AWS Lambda (container image) executes the end‑to‑end pipeline.
- Data layers in Amazon S3 (data lake):
  - RAW: `raw/world_bank_gdp/…` and `raw/wikipedia_co2/…` (JSONL with audit fields and `record_hash`).
  - PROCESSED: `processed/world_bank_gdp/year=<year>/…` and `processed/wikipedia_co2/year=<year>/…` (Parquet, typed schema).
  - CURATED: `curated/env_econ_country_year/year=<year>/snapshot_date=<YYYYMMDD>/curated_econ_environment_country_year.parquet`.
  - ANALYTICS: `analytics/<YYYYMMDD>/gdp_vs_co2_scatter.png` and `analytics/<YYYYMMDD>/correlation_summary.csv`.
- Metadata and incremental loads:
  - DynamoDB records `runs` (status, timestamps, rows processed) and `checkpoints` (e.g., `last_year_loaded_world_bank`).
- Portability via adapters:
  - `StorageAdapter` (Local/S3) and `MetadataAdapter` (Local JSON/DynamoDB) standardize I/O and metadata handling.
- Infrastructure as code and deployment:
  - Image pushed to ECR; stack created via CloudFormation (`cloud/lambda/template.yaml`).
  - IAM least‑privilege to the S3 bucket/prefix and DynamoDB table; logging in CloudWatch.

Run sequence (Lambda):

1) World Bank ingestion (RAW → S3)
2) World Bank processing (PROCESSED → S3)
3) Country mapping build (PROCESSED → S3)
4) Wikipedia crawler (RAW → S3)
5) Wikipedia processing (PROCESSED → S3)
6) CURATED join (CURATED → S3)
7) Analytical outputs (PNG/CSV → S3 `analytics/`)

Note: the scatter plot may be skipped automatically when curated data is
not available or contains no valid rows for the selected year (see
`src/cloud_pipeline.py`).

Main modules: `src/cloud_pipeline.py`, `src/adapters/*`, `src/ingestion_api/*`, `src/crawler/*`, `src/transformations/*`, `src/analysis/*`.

## Chosen Cloud Services & Justification

- AWS Lambda (Container Image)
  - Serverless compute, pay‑per‑use, ideal for scheduled, short‑lived workloads.
  - Supports native deps (numpy/pyarrow/matplotlib) packaged in the image (`cloud/lambda/Dockerfile`).
- Amazon S3
  - Durable, low‑cost data lake to organize RAW/PROCESSED/CURATED/ANALYTICS under a `base_prefix`.
  - Partitioning by `year=` and `snapshot_date=` enables reprocessing and auditability.
- Amazon DynamoDB
  - Lightweight metadata store: `RUN#<id>` and `CHECKPOINT#<source>` items enable incremental loads and run history.
  - Low cost, low latency; key‑value model fits this use case.
- Amazon EventBridge (schedule)
  - Triggers the Lambda daily (or custom `rate/cron`) with zero servers to manage.
- Amazon ECR
  - Lambda image registry; integrates nicely with CloudFormation deployments.
- AWS CloudFormation
  - Infrastructure as code with parameters (bucket, prefix, table, memory/timeout, schedule) in `cloud/lambda/template.yaml`.
- Amazon CloudWatch Logs
  - Standard Lambda logging via `AWSLambdaBasicExecutionRole` for production troubleshooting.

## Local Run (Part 1)

- Install dependencies: `pip install -r requirements.txt`
- Run the full pipeline locally:
  - PowerShell: `$env:PYTHONPATH='src'; python -m local_pipeline`
  - Bash: `PYTHONPATH=src python -m local_pipeline`

This produces files under `raw/`, `processed/`, `curated/`, and `analysis/`.

## Cloud Execution (Part 2)

The cloud pipeline reuses the same logic, but with S3 + DynamoDB via `src/cloud_pipeline.py`.

Expected environment variables (also configured in local `.env`):

- `PIPELINE_S3_BUCKET` – S3 bucket name for RAW/PROCESSED/CURATED.
- `PIPELINE_S3_BASE_PREFIX` – optional logical base prefix inside the bucket (e.g., `gdp-co2-pipeline`).
- `PIPELINE_METADATA_TABLE` – DynamoDB table name used by `DynamoMetadataAdapter`.

DynamoDB table schema required for metadata:

- Partition key: `pk` (String)
- Sort key: `sk` (String)

Items follow these patterns:
- Runs: `pk = RUN#<uuid>`, `sk = META`
- Checkpoints: `pk = CHECKPOINT#<source>`, `sk = META`

Example handler for AWS Lambda:

- Handler: `cloud_pipeline.lambda_handler`

For step‑by‑step cloud details, see `cloud/instructions.md`.

## Incremental Ingestion Strategy

- Checkpointing for World Bank API:
  - Uses a DynamoDB (cloud) or local JSON (dev) metadata store via `MetadataAdapter`.
  - Checkpoint key: `last_year_loaded_world_bank` (stores the last ingested year).
  - On each run, ingestion filters records to `year > checkpoint` and updates the checkpoint to the max ingested year.
  - Optional bounds `--min-year` and `--max-year` further restrict the window; if `min-year` exceeds the checkpoint, baseline becomes `min_year - 1`.
  - Even when no new rows are found, a RAW file is written (may be empty) for traceability; the run is recorded with status.
- Wikipedia crawler:
  - Snapshot with revision guard: the crawler queries the MediaWiki API for the latest `revid`. If unchanged, it skips re‑crawling and processing; if changed, it downloads and parses the full page and writes a new RAW snapshot.
  - RAW records now include `pageid`, `revid`, and `rev_timestamp`; the checkpoint key is `last_revid_wikipedia_co2`.
- Curated layer and Analytics:
  - CURATED uses `snapshot_date=<YYYYMMDD>` in the path to allow time‑travel and reproducibility.
  - The curated run stores `last_checkpoint = snapshot_date=<YYYYMMDD>` in metadata.

## Data Schema Decisions

- RAW (World Bank GDP per capita): one JSONL line per API record, enriched with audit fields.
  - Keys: original API payload plus `ingestion_run_id`, `ingestion_ts`, `data_source="world_bank_api"`, `raw_payload` (normalized JSON), `record_hash`, `raw_file_path`.
- RAW (Wikipedia CO₂ per capita): one JSONL record per crawl.
  - Keys: `ingestion_run_id`, `ingestion_ts`, `data_source="wikipedia_co2"`, `page_url`, `pageid`, `revid`, `rev_timestamp`, `table_html`, `raw_table_json={headers, rows}`, `record_hash`, `raw_file_path`.
- PROCESSED (World Bank): typed, long‑format by country‑year.
  - Columns: `country_code`, `country_name`, `year`, `gdp_per_capita_usd`, `indicator_id`, `indicator_name`, `ingestion_run_id`, `ingestion_ts`, `data_source`.
  - Storage: Parquet partitioned by `year` under `processed/world_bank_gdp/year=<year>/`.
- PROCESSED (Wikipedia): typed, long‑format (two years: 2000 and 2023).
  - Columns: `country_name`, `country_name_normalized`, `country_code` (optional via mapping), `year`, `co2_tons_per_capita`, `notes`, `ingestion_run_id`, `ingestion_ts`, `data_source`.
  - Storage: Parquet partitioned by `year` under `processed/wikipedia_co2/year=<year>/`.
- Country Mapping: canonical join keys derived from World Bank processed + optional overrides.
  - Columns: at least `country_name_normalized`, `country_code`, `country_name`.
  - Storage: single Parquet at `processed/country_mapping/country_mapping.parquet`.
- CURATED (econ_environment_country_year): join by `(country_code, year)` with derived metric.
  - Columns: `country_code`, `country_name`, `year`, `gdp_per_capita_usd`, `co2_tons_per_capita`, `co2_per_1000usd_gdp`, `gdp_source_system`, `co2_source_system`, `first_ingestion_run_id`, `last_update_run_id`, `last_update_ts`.
  - Storage: Parquet partitioned by `year` and `snapshot_date` under `curated/env_econ_country_year/`.
- Analytics outputs:
  - `gdp_vs_co2_scatter.png` (2023 only) and `correlation_summary.csv` (years 2000 and 2023) saved locally in `analysis/` or to S3 under `analytics/<YYYYMMDD>/` when running in cloud.

## Instructions to Run the Entire Project

- Prerequisites
  - Python 3.11, Docker (for cloud build), AWS CLI v2 configured, access to an S3 bucket and a DynamoDB table.
  - Install Python deps: `pip install -r requirements.txt`.

- Local end‑to‑end run
  - PowerShell: `$env:PYTHONPATH='src'; python -m local_pipeline`
  - Bash (Linux/macOS/WSL/Git Bash): `PYTHONPATH=src python -m local_pipeline`
  - CMD (Prompt de Comando): `set PYTHONPATH=src && python -m local_pipeline`
  - Optional bounds: append `--min-year 2000 --max-year 2023`.
  - Outputs: `raw/`, `processed/`, `curated/`, `analysis/`; metadata at `local_metadata.json`.

  - Cloud deployment and run
  - Configure `.env` with `PIPELINE_S3_BUCKET`, `PIPELINE_S3_BASE_PREFIX`, `PIPELINE_METADATA_TABLE`, `AWS_REGION`.
  - In Bash/WSL source .env (optional)
      `set -a; source .env; set +a`
  - Build and deploy: `cloud/lambda/build_and_deploy.sh`.
  - Function name
    - If you used the defaults, the Lambda function name is `gdp-co2-pipeline-gdp-co2-lambda`.
    - Otherwise, fetch it from the stack outputs:
      `aws cloudformation describe-stacks --stack-name ${STACK_NAME:-gdp-co2-pipeline} --query 'Stacks[0].Outputs[?OutputKey==\`LambdaFunctionName\`].OutputValue' --output text --region ${AWS_REGION}`
  - Invoke manually (examples)
    - Default (no payload) — AWS CLI v2:
      `aws lambda invoke --function-name gdp-co2-pipeline-gdp-co2-lambda --cli-binary-format raw-in-base64-out --payload '{}' out.json --region ${AWS_REGION}`
    - With bounds — AWS CLI v2:
      `aws lambda invoke --function-name gdp-co2-pipeline-gdp-co2-lambda --cli-binary-format raw-in-base64-out --payload '{"min_year":2000,"max_year":2023}' out.json --region ${AWS_REGION}`
    - Using sample event file:
      `aws lambda invoke --function-name gdp-co2-pipeline-gdp-co2-lambda --payload fileb://cloud/lambda/sample_event.json out.json --region ${AWS_REGION}`
    - Note: AWS CLI v2 requires `--cli-binary-format raw-in-base64-out` when passing inline JSON via `--payload`. When using `fileb://...`, the flag is not required.
  - Outputs in S3: `raw/`, `processed/`, `curated/`, `analytics/<YYYYMMDD>/` under the configured base prefix.
  - Scheduled runs via EventBridge (default daily 02:00 UTC).

  - Cleanup / Teardown (optional)
    - Delete the CloudFormation stack (Bash/WSL):
      `aws cloudformation delete-stack --stack-name ${STACK_NAME:-gdp-co2-pipeline} --region ${AWS_REGION}`
    - Wait for completion (optional):
      `aws cloudformation wait stack-delete-complete --stack-name ${STACK_NAME:-gdp-co2-pipeline} --region ${AWS_REGION}`
    - If you enabled resource creation (`CreateS3Bucket=true` / `CreateDynamoTable=true`), bucket/table are retained. Remove them manually using one of the options below.

    - Option A — Source .env (Bash/WSL) and run:
      `set -a; source .env; set +a`
      `aws s3 rm s3://$PIPELINE_S3_BUCKET --recursive --region $AWS_REGION`
      `aws s3api delete-bucket --bucket "$PIPELINE_S3_BUCKET" --region "$AWS_REGION"`
      `aws dynamodb delete-table --table-name "$PIPELINE_METADATA_TABLE" --region "$AWS_REGION"`

    - Option B — Read names from CloudFormation outputs (Bash/WSL):
      `STACK_NAME=${STACK_NAME:-gdp-co2-pipeline}; AWS_REGION=${AWS_REGION:-us-east-2}`
      `BUCKET=$(aws cloudformation describe-stacks --stack-name "$STACK_NAME" --region "$AWS_REGION" --query "Stacks[0].Outputs[?OutputKey=='EffectiveBucketName'].OutputValue" --output text)`
      `TABLE=$(aws cloudformation describe-stacks --stack-name "$STACK_NAME" --region "$AWS_REGION" --query "Stacks[0].Outputs[?OutputKey=='EffectiveMetadataTableName'].OutputValue" --output text)`
      `aws s3 rm s3://"$BUCKET" --recursive --region "$AWS_REGION"`
      `aws s3api delete-bucket --bucket "$BUCKET" --region "$AWS_REGION"`
      `aws dynamodb delete-table --table-name "$TABLE" --region "$AWS_REGION"`

    - Option C — PowerShell:
      `$env:PIPELINE_S3_BUCKET='env-econ-pipeline-data'; $env:AWS_REGION='us-east-2'`
      `aws s3 rm "s3://$env:PIPELINE_S3_BUCKET" --recursive --region $env:AWS_REGION`
      `aws s3api delete-bucket --bucket $env:PIPELINE_S3_BUCKET --region $env:AWS_REGION`
      `aws dynamodb delete-table --table-name env_econ_pipeline_metadata --region $env:AWS_REGION`

    - Clean up residual IAM role (if needed; IAM is global, no region flag):
      `ROLE=${ROLE:-gdp-co2-pipeline-gdp-co2-lambda-role}`
      `aws iam get-role --role-name "$ROLE" --no-cli-pager`
      `for p in $(aws iam list-role-policies --role-name "$ROLE" --query 'PolicyNames[]' --output text); do aws iam delete-role-policy --role-name "$ROLE" --policy-name "$p"; done`
      `for a in $(aws iam list-attached-role-policies --role-name "$ROLE" --query 'AttachedPolicies[].PolicyArn' --output text); do aws iam detach-role-policy --role-name "$ROLE" --policy-arn "$a"; done`
      `aws iam delete-role --role-name "$ROLE"`

    - Tip (versioned bucket): you can use a single command to force-remove contents and the bucket:
      `aws s3 rb s3://$PIPELINE_S3_BUCKET --force --region $AWS_REGION`

## Assumptions & Limitations

- External data stability
  - Wikipedia table structure may change; heuristics choose the main CO₂ per capita table or fallback to the first `wikitable`.
  - World Bank indicator defaults to `NY.GDP.PCAP.CD` but can be overridden via `WORLD_BANK_INDICATOR`.
- Infrastructure scope
  - The CloudFormation template can use existing S3/DynamoDB resources or optionally create them (controlled by parameters `CreateS3Bucket` and `CreateDynamoTable`). When creation is enabled, resources are retained on stack deletion.
  - Lambda max timeout (900s) bounds runtime; memory defaults to 2048 MB and may need tuning for larger runs.
- Incremental behavior
  - Incremental ingestion applies to World Bank by year using a single checkpoint.
  - Wikipedia uses a revision guard (snapshot semantics with skip on unchanged `revid`).
    - Why: the page does not expose stable row keys and edits can change historical values; MediaWiki diffs are text‑based and don’t map reliably to table cells. We avoid row‑level incremental diffs and, when a new revision exists, reprocess the entire table.
    - Consequence: simple, idempotent, auditable; avoids unnecessary runs when nothing changed.
- Data quality and mapping
  - Country mapping uses normalization rules and optional overrides (`src/transformations/country_mapping_overrides.csv`); some names may require curation.
  - Joining on `(country_code, year)` drops rows without matching CO₂ or GDP.
- Observability and ops
  - Logging via CloudWatch; no metrics dashboards/alerts included.
  - Lightweight HTTP retries for external sources (World Bank, Wikipedia) with exponential backoff + jitter; SDK retries apply for S3/DynamoDB.
