FROM apache/airflow:2.7.1-python3.11

ENV AIRFLOW_HOME=/opt/airflow

# Install Python dependencies
COPY requirements.txt /tmp/requirements.txt
RUN pip install --upgrade pip \
    && pip install --no-cache-dir -r /tmp/requirements.txt

WORKDIR /opt/airflow
EXPOSE 8080

# Keep official entrypoint
CMD ["webserver"]





