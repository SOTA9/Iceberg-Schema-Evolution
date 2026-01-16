# Base Airflow image (Python 3.11)
FROM apache/airflow:2.9.3-python3.11

# Switch to root to install system dependencies
USER root

# Install Docker client (needed for DockerOperator)
RUN apt-get update && apt-get install -y \
    docker.io \
    curl \
    git \
 && apt-get clean \
 && rm -rf /var/lib/apt/lists/*

# Set PATH for docker
ENV PATH="/usr/bin:${PATH}"

# Switch back to airflow user
USER airflow

# Install Airflow Docker provider (needed for DockerOperator)
RUN pip install --no-cache-dir apache-airflow-providers-docker

# Optional: install extra Python packages if needed by your DAGs
# RUN pip install --no-cache-dir pandas requests pyarrow

# Set working directory
WORKDIR /opt/airflow

# Copy Airflow plugins if any (optional)
# COPY plugins/ /opt/airflow/plugins/

# Default command: start webserver or scheduler is defined in docker-compose.yml
