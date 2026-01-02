FROM apache/airflow:2.7.1-python3.11

# Install Chrome as root (for Selenium or web scraping DAGs)
USER root
RUN apt-get update && apt-get install -y wget unzip curl gnupg \
    && wget -q -O - https://dl.google.com/linux/linux_signing_key.pub | apt-key add - \
    && sh -c 'echo "deb [arch=amd64] http://dl.google.com/linux/chrome/deb/ stable main" >> /etc/apt/sources.list.d/google.list' \
    && apt-get update && apt-get install -y google-chrome-stable \
    && rm -rf /var/lib/apt/lists/*

# Switch to airflow user
USER airflow
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt






