# Container image for MLM training from an experiment directory.
#
# Build context should be an experiment directory:
#   experiments/<exp>/
#     code/
#       marketlm/
#       requirements.txt
#     scripts/
#     entrypoints/
#
# Usage:
#   docker build -f infra/experiment.Dockerfile -t <tag> experiments/<exp>/
#
FROM pytorch/pytorch:2.4.0-cuda12.1-cudnn9-runtime

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PIP_NO_CACHE_DIR=1 \
    PYTHONPATH=/app

WORKDIR /app

# Copy requirements from experiment's code directory
COPY code/requirements.txt /app/requirements.txt
RUN pip install --upgrade pip \
    && pip install --extra-index-url https://download.pytorch.org/whl/cu121 -r requirements.txt \
    && pip install wandb

# Install Google Cloud SDK (gsutil)
RUN apt-get update && apt-get install -y curl gnupg lsb-release \
    && echo "deb [signed-by=/usr/share/keyrings/cloud.google.gpg] http://packages.cloud.google.com/apt cloud-sdk main" | tee -a /etc/apt/sources.list.d/google-cloud-sdk.list \
    && curl https://packages.cloud.google.com/apt/doc/apt-key.gpg | apt-key --keyring /usr/share/keyrings/cloud.google.gpg add - \
    && apt-get update && apt-get install -y google-cloud-cli \
    && apt-get clean && rm -rf /var/lib/apt/lists/*

# Copy experiment code
COPY code/marketlm /app/marketlm

# Copy experiment scripts and entrypoints
COPY scripts /app/scripts
COPY entrypoints /app/entrypoints

# Make entrypoints executable
RUN chmod -R +x /app/entrypoints/ 2>/dev/null || true

# Default entrypoint
ENTRYPOINT ["/app/entrypoints/run.sh"]
