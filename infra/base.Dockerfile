# Container image for EUR/USD dVAE training on NVIDIA GPUs
FROM pytorch/pytorch:2.4.0-cuda12.1-cudnn9-runtime

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PIP_NO_CACHE_DIR=1 \
    PYTHONPATH=/app

WORKDIR /app

COPY requirements.txt /app/requirements.txt
RUN pip install --upgrade pip \
    && pip install --extra-index-url https://download.pytorch.org/whl/cu121 -r requirements.txt \
    && pip install wandb

# Install Google Cloud SDK (gsutil)
RUN apt-get update && apt-get install -y curl gnupg lsb-release \
    && echo "deb [signed-by=/usr/share/keyrings/cloud.google.gpg] http://packages.cloud.google.com/apt cloud-sdk main" | tee -a /etc/apt/sources.list.d/google-cloud-sdk.list \
    && curl https://packages.cloud.google.com/apt/doc/apt-key.gpg | apt-key --keyring /usr/share/keyrings/cloud.google.gpg add - \
    && apt-get update && apt-get install -y google-cloud-cli \
    && apt-get clean && rm -rf /var/lib/apt/lists/*

COPY . /app

# Make entrypoints executable (generic runner + any legacy that might exist)
RUN chmod -R +x entrypoints/ legacy/entrypoints/ experiments/*/entrypoints/ 2>/dev/null || true

# Default entrypoint is the generic runner
ENTRYPOINT ["/app/entrypoints/run.sh"]
