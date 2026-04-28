FROM apache/airflow:3.2.1

USER root
RUN apt-get update \
  && apt-get install -y --no-install-recommends \
     freetds-dev \
     freetds-bin \
     unixodbc-dev \
     gcc \
     g++ \
  && apt-get autoremove -yqq --purge \
  && apt-get clean \
  && rm -rf /var/lib/apt/lists/*

USER airflow
RUN pip install --no-cache-dir \
    "apache-airflow==3.2.1" \
    "apache-airflow-providers-microsoft-mssql==4.5.2" \
    "apache-airflow-providers-postgres" \
    "pymssql>=2.3.5"