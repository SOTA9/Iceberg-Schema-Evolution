# Base Airflow image (Python 3.11)
FROM apache/airflow:2.9.3-python3.11

# Switch to root to install system dependencies
USER root

# Install Docker client (needed for DockerOperator)
# FIX: each package on same line or proper continuation — your original had broken backslashes
RUN apt-get update && apt-get install -y \
    docker.io \
    curl \
    git \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

# Ensure docker binary is on PATH
ENV PATH="/usr/bin:${PATH}"

# Switch back to airflow user (NEVER run pip as root for airflow image)
USER airflow

# Install Airflow Docker provider
RUN pip install --no-cache-dir \
    apache-airflow-providers-docker==3.9.0

# Set working directory
WORKDIR /opt/airflow
