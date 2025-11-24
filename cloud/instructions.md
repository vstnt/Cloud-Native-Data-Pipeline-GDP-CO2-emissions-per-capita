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

Notes:
- If you deploy using `cloud/lambda/build_and_deploy.sh` (CloudFormation), the handler and environment variables are set by the stack. No manual config needed in the console.
- The template can either use existing S3/DynamoDB resources or create them when enabled via parameters. DynamoDB table schema (when created) is `pk` (HASH, String) and `sk` (RANGE, String).

### Inline IAM policy example
Use this as a reference inline policy for the Lambda execution role. Replace placeholders (`YOUR_BUCKET_NAME`, `YOUR_BASE_PREFIX`, `YOUR_REGION`, `YOUR_ACCOUNT_ID`, `YOUR_TABLE_NAME`). If you do not use a base prefix, change the S3 object ARN to `arn:aws:s3:::YOUR_BUCKET_NAME/*` and drop the `s3:prefix` condition.

```json
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Sid": "AllowCloudWatchLogs",
      "Effect": "Allow",
      "Action": [
        "logs:CreateLogGroup",
        "logs:CreateLogStream",
        "logs:PutLogEvents"
      ],
      "Resource": "*"
    },
    {
      "Sid": "AllowS3DataLakeAccess",
      "Effect": "Allow",
      "Action": [
        "s3:GetObject",
        "s3:PutObject"
      ],
      "Resource": [
        "arn:aws:s3:::YOUR_BUCKET_NAME/YOUR_BASE_PREFIX/*"
      ]
    },
    {
      "Sid": "AllowS3ListOnBucket",
      "Effect": "Allow",
      "Action": [
        "s3:ListBucket"
      ],
      "Resource": [
        "arn:aws:s3:::YOUR_BUCKET_NAME"
      ],
      "Condition": {
        "StringLike": {
          "s3:prefix": [
            "YOUR_BASE_PREFIX/*"
          ]
        }
      }
    },
    {
      "Sid": "AllowDynamoDBMetadataTable",
      "Effect": "Allow",
      "Action": [
        "dynamodb:PutItem",
        "dynamodb:GetItem",
        "dynamodb:UpdateItem",
        "dynamodb:Scan"
      ],
      "Resource": "arn:aws:dynamodb:YOUR_REGION:YOUR_ACCOUNT_ID:table/YOUR_TABLE_NAME"
    }
  ]
}
```

### Storage paths (S3)
- RAW
  - `raw/world_bank_gdp/world_bank_gdp_raw_<timestamp>.jsonl`
  - `raw/wikipedia_co2/wikipedia_co2_raw_<timestamp>.jsonl`
- PROCESSED
  - `processed/world_bank_gdp/year=<year>/processed_worldbank_gdp_per_capita.parquet`
  - `processed/wikipedia_co2/year=<year>/processed_wikipedia_co2_per_capita.parquet`
  - `processed/country_mapping/country_mapping.parquet`
- CURATED
  - `curated/env_econ_country_year/year=<year>/snapshot_date=<YYYYMMDD>/curated_econ_environment_country_year.parquet`
- ANALYTICS (when running in cloud)
  - `analytics/<YYYYMMDD>/gdp_vs_co2_scatter.png`
  - `analytics/<YYYYMMDD>/correlation_summary.csv`

## 3. Lambda deployment (container image)

Prerequisites (on WSL or Linux):

- Docker installed and running
- AWS CLI v2 configured (`aws configure`) with the correct account/region
- Either pre-created S3 bucket (`PIPELINE_S3_BUCKET`) and DynamoDB table (`PIPELINE_METADATA_TABLE`), or enable creation via the parameters below.

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
      - Optional creation of S3 bucket and/or DynamoDB table when enabled; both are retained on stack deletion.

Deployment parameters (optional via env):

- `STACK_NAME` (default `gdp-co2-pipeline`)
- `STACK_SUFFIX` (default `gdp-co2`)
- `AWS_REGION` (default `us-east-1`)
- `SCHEDULE_EXPRESSION` (e.g., `rate(1 day)`)
- `MEMORY_SIZE` (default `2048`)
- `TIMEOUT` (default `900`)
- `CREATE_S3_BUCKET` (`false`/`true`) – when `true`, CloudFormation creates the S3 bucket named `PIPELINE_S3_BUCKET` and retains it on stack deletion.
- `CREATE_DYNAMO_TABLE` (`false`/`true`) – when `true`, CloudFormation creates the DynamoDB table named `PIPELINE_METADATA_TABLE` (keys `pk`/`sk`) and retains it on stack deletion.

Behavior when creation is enabled:
- S3: The bucket name will be exactly `PIPELINE_S3_BUCKET`. Ensure it is globally unique and not owned by another AWS account.
- DynamoDB: The table name will be exactly `PIPELINE_METADATA_TABLE` with keys `pk` (String) and `sk` (String), billing mode `PAY_PER_REQUEST`.

Output: the command prints the stack outputs, including the function name.

Compute details:
- Runtime: AWS Lambda container image (Python 3.11 base).
- Defaults: `MEMORY_SIZE=2048` MB, `TIMEOUT=900` s; adjust via env if needed.

## 4. Quick test (manual invoke)

After deployment, you can invoke manually:

- Get the function name (if you missed it in outputs):
  `aws cloudformation describe-stacks --stack-name gdp-co2-pipeline --query 'Stacks[0].Outputs[?OutputKey==\`LambdaFunctionName\`].OutputValue' --output text`

- Invoke without payload (uses default years):
  `aws lambda invoke --function-name <function-name> --payload '{}' out.json`

- Invoke with a restricted year range:
  `aws lambda invoke --function-name <function-name> --payload '{"min_year": 2000, "max_year": 2023}' out.json`

- Invoke with sample file payload:
  `aws lambda invoke --function-name <function-name> --payload fileb://cloud/lambda/sample_event.json out.json`

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

## 6. Optional environment variables

These are optional and allow customizing external sources:

- `WORLD_BANK_INDICATOR` – World Bank indicator to ingest (default `NY.GDP.PCAP.CD`).
- `WIKIPEDIA_URL` – Source URL for the CO₂ per capita table (default as in code).

## 7. Cleanup / Teardown

- Delete the CloudFormation stack:
  `aws cloudformation delete-stack --stack-name ${STACK_NAME:-gdp-co2-pipeline} --region ${AWS_REGION}`

- Optionally wait for completion:
  `aws cloudformation wait stack-delete-complete --stack-name ${STACK_NAME:-gdp-co2-pipeline} --region ${AWS_REGION}`

- Retained resources (when created via parameters):
  - S3 bucket and/or DynamoDB table are created with `DeletionPolicy: Retain`. They are not deleted by the stack.
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
  - Tip (versioned bucket): you can use a single command to force-remove contents and the bucket:
    `aws s3 rb s3://$PIPELINE_S3_BUCKET --force --region $AWS_REGION`
