FROM astronomerinc/ap-airflow:2.7.0-python3.11

# Copy DAGs, source, configs
COPY dags/ /usr/local/airflow/dags/
COPY src/ /usr/local/airflow/src/
COPY configs/ /usr/local/airflow/configs/
COPY .env /usr/local/airflow/.env

# Install Python dependencies
COPY requirements.txt /usr/local/airflow/
RUN pip install --no-cache-dir -r /usr/local/airflow/requirements.txt

# Set GCP credentials
ENV GOOGLE_APPLICATION_CREDENTIALS=/usr/local/airflow/configs/gcp_credentials.json


