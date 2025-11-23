#!/usr/bin/env bash
set -euo pipefail

# Build and deploy the GDP x CO2 Lambda (container image) via CloudFormation.

# Requirements:
# - bash, sed, grep, awk, jq (optional)
# - docker
# - awscli v2 (configured with credentials and default region)

STACK_NAME=${STACK_NAME:-gdp-co2-pipeline}
STACK_SUFFIX=${STACK_SUFFIX:-gdp-co2}
ECR_REPO=${ECR_REPO:-gdp-co2-pipeline}
IMAGE_TAG=${IMAGE_TAG:-$(date +%Y%m%dT%H%M%SZ)}
REGION=${AWS_REGION:-${AWS_DEFAULT_REGION:-us-east-1}}

SCRIPT_DIR=$(cd "$(dirname "$0")" && pwd)
REPO_ROOT=$(cd "${SCRIPT_DIR}/../.." && pwd)

# Read defaults from .env if present
if [ -f "${REPO_ROOT}/.env" ]; then
  # shellcheck disable=SC2046
  export $(grep -v '^#' "${REPO_ROOT}/.env" | xargs -d '\n' -n1)
fi

PIPELINE_S3_BUCKET=${PIPELINE_S3_BUCKET:?Set PIPELINE_S3_BUCKET in env or .env}
PIPELINE_S3_BASE_PREFIX=${PIPELINE_S3_BASE_PREFIX:-gdp-co2-pipeline}
PIPELINE_METADATA_TABLE=${PIPELINE_METADATA_TABLE:?Set PIPELINE_METADATA_TABLE in env or .env}
SCHEDULE_EXPRESSION=${SCHEDULE_EXPRESSION:-"cron(0 2 * * ? *)"}
MEMORY_SIZE=${MEMORY_SIZE:-2048}
TIMEOUT=${TIMEOUT:-900}

echo "Using AWS region: ${REGION}"

ACCOUNT_ID=$(aws sts get-caller-identity --query Account --output text)
if [ -z "${ACCOUNT_ID}" ]; then
  echo "Failed to resolve AWS account ID. Is AWS CLI configured?" >&2
  exit 1
fi

ECR_URI="${ACCOUNT_ID}.dkr.ecr.${REGION}.amazonaws.com"
IMAGE_NAME="${ECR_REPO}:${IMAGE_TAG}"
FULL_IMAGE_URI="${ECR_URI}/${IMAGE_NAME}"

echo "Ensuring ECR repository: ${ECR_REPO}"
aws ecr describe-repositories --repository-names "${ECR_REPO}" --region "${REGION}" >/dev/null 2>&1 || \
  aws ecr create-repository --repository-name "${ECR_REPO}" --region "${REGION}" >/dev/null

echo "Logging in to ECR..."
aws ecr get-login-password --region "${REGION}" | docker login --username AWS --password-stdin "${ECR_URI}"

echo "Building Docker image: ${IMAGE_NAME}"
docker build -t "${IMAGE_NAME}" -f "${SCRIPT_DIR}/Dockerfile" "${REPO_ROOT}"

echo "Tagging image as: ${FULL_IMAGE_URI}"
docker tag "${IMAGE_NAME}" "${FULL_IMAGE_URI}"

echo "Pushing image to ECR..."
docker push "${FULL_IMAGE_URI}"

echo "Deploying CloudFormation stack: ${STACK_NAME}"
aws cloudformation deploy \
  --stack-name "${STACK_NAME}" \
  --template-file "${SCRIPT_DIR}/template.yaml" \
  --capabilities CAPABILITY_NAMED_IAM \
  --parameter-overrides \
      StackNameSuffix="${STACK_SUFFIX}" \
      ImageUri="${FULL_IMAGE_URI}" \
      PipelineBucketName="${PIPELINE_S3_BUCKET}" \
      PipelineBasePrefix="${PIPELINE_S3_BASE_PREFIX}" \
      MetadataTableName="${PIPELINE_METADATA_TABLE}" \
      ScheduleExpression="${SCHEDULE_EXPRESSION}" \
      MemorySize="${MEMORY_SIZE}" \
      Timeout="${TIMEOUT}" \
  --region "${REGION}"

echo "Deployment completed. Stack outputs:"
aws cloudformation describe-stacks --stack-name "${STACK_NAME}" --region "${REGION}" \
  --query 'Stacks[0].Outputs' --output table
