# ── Spark 4.0 + Iceberg image ─────────────────────────────────────────────
# FIX: apache/spark:4.0.0-scala2.13-java21-ubuntu is the correct published tag.
#      4.0.1 does not exist yet on Docker Hub — use 4.0.0.
FROM apache/spark:4.0.0-scala2.13-java21-ubuntu

USER root

# Install Python, pip, and venv
# --break-system-packages is only supported on pip ≥ 23 (PEP 668).
# The apache/spark Ubuntu base ships an older pip, so we use a venv instead.
RUN apt-get update && apt-get install -y \
        python3 python3-pip python3-venv curl unzip \
    && rm -rf /var/lib/apt/lists/*

# Create a virtualenv so pip installs are fully isolated and version-agnostic
RUN python3 -m venv /opt/venv
ENV PATH="/opt/venv/bin:$PATH"

# Upgrade pip inside the venv (always works, no flag needed)
RUN pip install --upgrade pip

# ── Environment ───────────────────────────────────────────────────────────
ENV PYSPARK_PYTHON=/opt/venv/bin/python3 \
    PYTHONPATH=/opt/spark/src \
    IVY_HOME=/opt/spark/.ivy2 \
    SPARK_LOCAL_DIRS=/tmp/spark \
    SPARK_SUBMIT_OPTS="-Dspark.jars.ivy=/opt/spark/.ivy2" \
    SPARK_HOME=/opt/spark

RUN mkdir -p /opt/spark/.ivy2 /tmp/spark && \
    chmod -R 777 /opt/spark/.ivy2 /tmp/spark

# ── Python dependencies ───────────────────────────────────────────────────
COPY requirements.txt /opt/spark/requirements.txt
RUN pip install --no-cache-dir -r /opt/spark/requirements.txt

# ── Pre-download Iceberg JARs at build time ───────────────────────────────
# This avoids downloading them on every spark-submit at runtime.
# The dummy submit caches JARs into IVY_HOME.
COPY src /opt/spark/src
WORKDIR /opt/spark/src

RUN echo "print('jar warmup')" > /tmp/dummy_script.py && \
    $SPARK_HOME/bin/spark-submit \
      --master local[*] \
      --packages \
        org.apache.iceberg:iceberg-spark-runtime-4.0_2.13:1.10.1,\
org.apache.iceberg:iceberg-gcp:1.10.1,\
com.google.api-client:google-api-client:2.2.0,\
com.google.http-client:google-http-client-gson:1.41.0,\
com.google.oauth-client:google-oauth-client:1.34.1 \
      --conf spark.jars.ivy=/opt/spark/.ivy2 \
      --conf spark.ui.enabled=false \
      /tmp/dummy_script.py || true && \
    rm /tmp/dummy_script.py

# ── Non-root user ─────────────────────────────────────────────────────────
RUN useradd -ms /bin/bash sparkuser && \
    chown -R sparkuser:sparkuser /opt/spark/.ivy2 /tmp/spark /opt/spark/src

USER sparkuser

# Default: run bronze (overridden by DockerOperator command)
CMD ["python3", "bronze_ingest.py"]








