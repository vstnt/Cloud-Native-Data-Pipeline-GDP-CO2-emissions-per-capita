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
CREATE_S3_BUCKET=${CREATE_S3_BUCKET:-false}
CREATE_DYNAMO_TABLE=${CREATE_DYNAMO_TABLE:-false}

echo "Using AWS region: ${REGION}"

# Resolve AWS CLI (WSL or Windows fallback)
AWS="aws"
if ! command -v "${AWS}" >/dev/null 2>&1; then
  WIN_AWS="/mnt/c/Program Files/Amazon/AWSCLIV2/aws.exe"
  if [ -x "${WIN_AWS}" ]; then
    AWS="${WIN_AWS}"
    echo "Using Windows AWS CLI at: ${AWS}"
  else
    echo "AWS CLI not found. Install in WSL (preferred) or Windows (MSI)." >&2
    echo "- WSL install: https://docs.aws.amazon.com/cli/latest/userguide/getting-started-install.html" >&2
    echo "- Windows MSI: https://aws.amazon.com/cli/" >&2
    exit 1
  fi
fi

# Ensure Docker is available
if ! command -v docker >/dev/null 2>&1; then
  echo "Docker CLI not found. Install Docker Desktop and enable WSL integration." >&2
  exit 1
fi

ACCOUNT_ID=$("${AWS}" sts get-caller-identity --query Account --output text)
if [ -z "${ACCOUNT_ID}" ]; then
  echo "Failed to resolve AWS account ID. Is AWS CLI configured?" >&2
  exit 1
fi

ECR_URI="${ACCOUNT_ID}.dkr.ecr.${REGION}.amazonaws.com"
IMAGE_NAME="${ECR_REPO}:${IMAGE_TAG}"
FULL_IMAGE_URI="${ECR_URI}/${IMAGE_NAME}"

echo "Ensuring ECR repository: ${ECR_REPO}"
"${AWS}" ecr describe-repositories --repository-names "${ECR_REPO}" --region "${REGION}" >/dev/null 2>&1 || \
  "${AWS}" ecr create-repository --repository-name "${ECR_REPO}" --region "${REGION}" >/dev/null

echo "Logging in to ECR..."
"${AWS}" ecr get-login-password --region "${REGION}" | docker login --username AWS --password-stdin "${ECR_URI}"

echo "Building Docker image for Lambda (prefer buildx, docker media types)"
if docker buildx version >/dev/null 2>&1; then
  set +e
  docker buildx build \
    --platform linux/amd64 \
    -t "${FULL_IMAGE_URI}" \
    --output type=registry,oci-mediatypes=false \
    --provenance=false \
    --sbom=false \
    -f "${SCRIPT_DIR}/Dockerfile" "${REPO_ROOT}"
  BUILD_STATUS=$?
  set -e
  if [ ${BUILD_STATUS} -ne 0 ]; then
    echo "buildx failed or not compatible; falling back to classic builder (schema2, gzip)"
    DOCKER_BUILDKIT=0 docker build -t "${IMAGE_NAME}" -f "${SCRIPT_DIR}/Dockerfile" "${REPO_ROOT}"
    docker tag "${IMAGE_NAME}" "${FULL_IMAGE_URI}"
    docker push "${FULL_IMAGE_URI}"
  fi
else
  echo "buildx not available; using classic builder (schema2, gzip)"
  DOCKER_BUILDKIT=0 docker build -t "${IMAGE_NAME}" -f "${SCRIPT_DIR}/Dockerfile" "${REPO_ROOT}"
  docker tag "${IMAGE_NAME}" "${FULL_IMAGE_URI}"
  docker push "${FULL_IMAGE_URI}"
fi

# Convert template path to Windows path if using aws.exe from Windows
TEMPLATE_FILE="${SCRIPT_DIR}/template.yaml"
TEMPLATE_ARG="${TEMPLATE_FILE}"
case "${AWS}" in
  *AWSCLIV2/aws.exe)
    if command -v wslpath >/dev/null 2>&1; then
      TEMPLATE_ARG=$(wslpath -w "${TEMPLATE_FILE}")
    fi
    ;;
esac

echo "Deploying CloudFormation stack: ${STACK_NAME}"
"${AWS}" cloudformation deploy \
  --stack-name "${STACK_NAME}" \
  --template-file "${TEMPLATE_ARG}" \
  --capabilities CAPABILITY_NAMED_IAM \
  --parameter-overrides \
      StackNameSuffix="${STACK_SUFFIX}" \
      ImageUri="${FULL_IMAGE_URI}" \
      PipelineBucketName="${PIPELINE_S3_BUCKET}" \
      PipelineBasePrefix="${PIPELINE_S3_BASE_PREFIX}" \
      MetadataTableName="${PIPELINE_METADATA_TABLE}" \
      CreateS3Bucket="${CREATE_S3_BUCKET}" \
      CreateDynamoTable="${CREATE_DYNAMO_TABLE}" \
      ScheduleExpression="${SCHEDULE_EXPRESSION}" \
      MemorySize="${MEMORY_SIZE}" \
      Timeout="${TIMEOUT}" \
  --region "${REGION}"

echo "Deployment completed. Stack outputs:"
"${AWS}" cloudformation describe-stacks --stack-name "${STACK_NAME}" --region "${REGION}" \
  --query 'Stacks[0].Outputs' --output table
