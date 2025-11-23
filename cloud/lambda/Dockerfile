# Lambda container image for the GDP x CO2 pipeline
# Uses the AWS Lambda Python 3.11 base image and bundles project dependencies.

FROM public.ecr.aws/lambda/python:3.11

# Copy and install Python dependencies into the Lambda task root
COPY requirements.txt ./
RUN python -m pip install --no-cache-dir -r requirements.txt -t ${LAMBDA_TASK_ROOT}

# Copy application source code to the Lambda task root
COPY src/ ${LAMBDA_TASK_ROOT}/

# Headless matplotlib in Lambda
ENV MPLBACKEND=Agg
ENV MPLCONFIGDIR=/tmp/matplotlib

# Default handler (can be overridden in the Lambda config)
CMD ["cloud_pipeline.lambda_handler"]

