FROM apache/airflow:2.9.3

COPY requirements.txt .

COPY jars /opt/spark/jars/custom

RUN pip install --no-cache-dir -r requirements.txt