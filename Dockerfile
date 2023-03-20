FROM apache/airflow:2.2.5
COPY requirements_freeze.txt .
RUN pip install --no-cache-dir -r requirements_freeze.txt
RUN mkdir creds/
COPY creds/ ./creds/