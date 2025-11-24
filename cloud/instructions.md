# Cloud instructions (Part 2)

This file documents environment configuration and how to run the pipeline in the cloud (S3 + DynamoDB).

## 1. Local `.env` for development

The `.env` at the project root contains placeholders you should adjust before use:

- `PIPELINE_S3_BUCKET` – S3 bucket where the pipeline data will be stored.
- `PIPELINE_S3_BASE_PREFIX` – optional logical base prefix in the bucket (e.g., `gdp-co2-pipeline`).
- `PIPELINE_METADATA_TABLE` – DynamoDB table name for metadata (runs + checkpoints).

Example (already included in `.env`):

```bash
PIPELINE_S3_BUCKET=your-gdp-co2-bucket
PIPELINE_S3_BASE_PREFIX=gdp-co2-pipeline
PIPELINE_METADATA_TABLE=gdp_co2_metadata
```

Use your preferred mechanism to load this `.env` locally (IDE plugin, `python-dotenv`, etc.) or export the variables manually.

## 2. Expected AWS configuration

When creating the AWS Lambda function that runs the cloud pipeline:

- Set the same environment variables (`PIPELINE_S3_BUCKET`, `PIPELINE_S3_BASE_PREFIX`, `PIPELINE_METADATA_TABLE`) in the function configuration.
- Use handler: `cloud_pipeline.lambda_handler`.
- Ensure the Lambda execution role has permissions:
  - `s3:GetObject`, `s3:PutObject`, `s3:ListBucket` on the configured bucket/prefix.
  - `dynamodb:PutItem`, `dynamodb:GetItem`, `dynamodb:UpdateItem`, `dynamodb:Scan` on the metadata table.

## 3. Lambda deployment (container image)

Prerequisites (on WSL or Linux):

- Docker installed and running
- AWS CLI v2 configured (`aws configure`) with the correct account/region

Steps:

1) Set the variables in the project root `.env` (examples included):

   - `PIPELINE_S3_BUCKET`
   - `PIPELINE_S3_BASE_PREFIX`
   - `PIPELINE_METADATA_TABLE`

2) Build the image, push to ECR, and deploy the CloudFormation stack:

   - Bash:

     `cloud/lambda/build_and_deploy.sh`

   This script will:
   - Create/use an ECR repository (`gdp-co2-pipeline`).
   - Build the Lambda image (AWS Lambda Python 3.11 base) with code and dependencies.
   - Push the image to ECR.
   - Create/update the CloudFormation stack with:
     - Minimal IAM Role for S3 (bucket/prefix) and DynamoDB (table).
     - Lambda function with `PackageType: Image`.
     - EventBridge schedule rule (daily at 02:00 UTC by default).

Deployment parameters (optional via env):

- `STACK_NAME` (default `gdp-co2-pipeline`)
- `STACK_SUFFIX` (default `gdp-co2`)
- `AWS_REGION` (default `us-east-1`)
- `SCHEDULE_EXPRESSION` (e.g., `rate(1 day)`)
- `MEMORY_SIZE` (default `2048`)
- `TIMEOUT` (default `900`)

Output: the command prints the stack outputs, including the function name.

## 4. Quick test (manual invoke)

After deployment, you can invoke manually:

- Get the function name (if you missed it in outputs):
  `aws cloudformation describe-stacks --stack-name gdp-co2-pipeline --query 'Stacks[0].Outputs[?OutputKey==\`LambdaFunctionName\`].OutputValue' --output text`

- Invoke without payload (uses default years):
  `aws lambda invoke --function-name <function-name> --payload '{}' out.json`

- Invoke with a restricted year range:
  `aws lambda invoke --function-name <function-name> --payload '{"min_year": 2000, "max_year": 2023}' out.json`

The `out.json` file will contain the `statusCode` and an artefact summary.

## 5. Cloud pipeline execution (overview)

- The `run_cloud_pipeline` in `src/cloud_pipeline.py` orchestrates:
  1. World Bank RAW ingestion (S3).
  2. World Bank PROCESSED transformation (S3).
  3. `country_mapping` build (S3).
  4. Wikipedia RAW crawler (S3).
  5. Wikipedia PROCESSED transformation (S3, with mapping).
  6. CURATED layer (S3).
  7. Analytical outputs, reading CURATED via S3.

The Lambda event may optionally contain `min_year` and `max_year` to limit the World Bank ingestion range.

Notes:
- From this version onward, analytical artefacts (PNG and CSV) are also saved in S3 via the `StorageAdapter` under `analytics/<YYYYMMDD>/...`. In local execution, they are still written to the `analysis/` folder.
