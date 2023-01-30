FROM apache/airflow:2.2.5
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt
RUN mkdir creds/
COPY creds/ ./creds/