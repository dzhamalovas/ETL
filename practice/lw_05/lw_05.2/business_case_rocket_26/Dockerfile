FROM apache/airflow:slim-2.8.1-python3.11

USER root

# Создаём директории и назначаем владельца (ID 50000 - стандартный пользователь airflow)
RUN mkdir -p /opt/airflow/data /opt/airflow/logs /opt/airflow/app \
    && chown -R 50000:0 /opt/airflow/data /opt/airflow/logs /opt/airflow/app

USER airflow

# Устанавливаем необходимые Python-библиотеки для ETL, ML и Streamlit
RUN pip install --no-cache-dir \
    pandas scikit-learn joblib requests streamlit \
    torch torchvision transformers Pillow plotly psycopg2-binary \
    jupyter