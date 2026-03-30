FROM apache/airflow:slim-2.8.1-python3.11

USER root

# Установка системных зависимостей для сборки библиотек
RUN apt-get update \
    && apt-get install -y --no-install-recommends \
           build-essential \
           libpq-dev \
    && apt-get autoremove -yqq --purge \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

USER airflow

# Устанавливаем необходимые Python-библиотеки с фиксированными совместимыми версиями
RUN pip install --no-cache-dir \
    pandas==2.1.4 \
    scikit-learn \
    joblib \
    requests \
    azure-storage-blob==12.8.1 \
    psycopg2-binary==2.9.9 \
    sqlalchemy==1.4.52 \
    streamlit==1.31.0 \
    plotly==5.18.0 \
    "connexion[swagger-ui]" \
    apache-airflow-providers-postgres==2.0.0

USER root

# Создаём директории и назначаем владельца
RUN mkdir -p /opt/airflow/data /opt/airflow/logs /opt/streamlit \
    && chown -R airflow: /opt/airflow/data /opt/airflow/logs /opt/streamlit

USER airflow
