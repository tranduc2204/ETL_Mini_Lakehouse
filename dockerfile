FROM apache/airflow:2.9.3

# PySpark cần JVM nhưng image Airflow không có Java -> cài JDK.
# Phải chuyển sang root để apt-get, xong trả lại user airflow.
USER root
RUN apt-get update \
    && apt-get install -y --no-install-recommends default-jdk \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*
# default-jdk trên Debian cài vào /usr/lib/jvm/default-java
ENV JAVA_HOME=/usr/lib/jvm/default-java
USER airflow

COPY requirements.txt .

COPY jars /opt/spark/jars/custom

RUN pip install --no-cache-dir -r requirements.txt
