FROM apache/spark:4.0.1-scala2.13-java21-ubuntu

USER root

RUN apt-get update && apt-get install -y \
        python3 python3-pip curl unzip \
    && rm -rf /var/lib/apt/lists/*

RUN python3 -m pip install --upgrade pip

ENV PYSPARK_PYTHON=python3 \
    PYTHONPATH=/opt/spark/src \
    IVY_HOME=/opt/spark/.ivy2 \
    SPARK_LOCAL_DIRS=/tmp/spark \
    SPARK_SUBMIT_OPTS="-Dspark.jars.ivy=/opt/spark/.ivy2" \
    SPARK_HOME=/opt/spark

RUN mkdir -p /opt/spark/.ivy2 /tmp/spark && \
    chmod -R 777 /opt/spark/.ivy2 /tmp/spark

COPY requirements.txt /opt/spark/requirements.txt
RUN pip3 install --no-cache-dir -r /opt/spark/requirements.txt

COPY src /opt/spark/src
WORKDIR /opt/spark/src

RUN echo "print('dummy')" > dummy_script.py
RUN $SPARK_HOME/bin/spark-submit \
      --packages \
org.apache.iceberg:iceberg-spark-runtime-4.0_2.13:1.10.1, \
org.apache.iceberg:iceberg-gcp:1.10.1, \
com.google.api-client:google-api-client:2.2.0, \
com.google.http-client:google-http-client-gson:1.41.0, \
com.google.oauth-client:google-oauth-client:1.34.1 \
      --master local[*] dummy_script.py || true
RUN rm dummy_script.py

RUN useradd -ms /bin/bash sparkuser && \
    chown -R sparkuser:sparkuser /opt/spark/.ivy2 /tmp/spark /opt/spark/src

USER sparkuser

CMD ["python3", "bronze_ingest.py"]









