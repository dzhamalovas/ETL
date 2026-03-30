import urllib.request
import airflow.utils.dates
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator

TARGET_PAGES = {"Google", "Amazon", "Apple", "Microsoft", "Facebook"}

dag = DAG(
    dag_id="stocksense_wiki_etl_v2",
    start_date=airflow.utils.dates.days_ago(1),
    schedule_interval="@hourly",
    template_searchpath="/tmp",
    max_active_runs=1,
    catchup=False
)

def _get_data(year, month, day, hour, output_path):
    url = (
        "https://dumps.wikimedia.org/other/pageviews/"
        f"{year}/{year}-{month:0>2}/pageviews-{year}{month:0>2}{day:0>2}-{hour:0>2}0000.gz"
    )
    urllib.request.urlretrieve(url, output_path)

get_data = PythonOperator(
    task_id="get_data",
    python_callable=_get_data,
    op_kwargs={
        "year": "{{ data_interval_start.subtract(hours=3).year }}",
        "month": "{{ data_interval_start.subtract(hours=3).month }}",
        "day": "{{ data_interval_start.subtract(hours=3).day }}",
        "hour": "{{ data_interval_start.subtract(hours=3).hour }}",
        "output_path": "/tmp/wikipageviews.gz",
    },
    dag=dag,
)

extract_gz = BashOperator(
    task_id="extract_gz",
    bash_command="gunzip --force /tmp/wikipageviews.gz",
    dag=dag
)

def _fetch_pageviews(pagenames, execution_date):
    result = dict.fromkeys(pagenames, 0)
    with open("/tmp/wikipageviews", "r") as f:
        for line in f:
            domain_code, page_title, view_counts, _ = line.split(" ")
            if domain_code == "en" and page_title in pagenames:
                result[page_title] = view_counts

    with open("/tmp/postgres_query.sql", "w") as f:
        for pagename, pageviewcount in result.items():
            f.write(
                "INSERT INTO pageview_counts VALUES ("
                f"'{pagename}', {pageviewcount}, '{execution_date}'"
                ");\n"
            )

fetch_pageviews = PythonOperator(
    task_id="fetch_pageviews",
    python_callable=_fetch_pageviews,
    op_kwargs={
        "pagenames": TARGET_PAGES,
        "execution_date": "{{ data_interval_start }}"
    },
    dag=dag,
)

write_to_postgres = PostgresOperator(
    task_id="write_to_postgres",
    postgres_conn_id="my_postgres",
    sql="postgres_query.sql",
    dag=dag,
)

def _export_to_csv():
    import psycopg2
    import csv

    conn = psycopg2.connect(
        host="wiki_results",
        port=5432,
        dbname="airflow",
        user="airflow",
        password="airflow"
    )
    cur = conn.cursor()
    cur.execute("SELECT pagename, pageviewcount, datetime FROM pageview_counts ORDER BY datetime DESC;")
    rows = cur.fetchall()
    colnames = [desc[0] for desc in cur.description]
    cur.close()
    conn.close()

    with open("/opt/airflow/data/pageview_data.csv", "w", newline="") as f:
        writer = csv.writer(f)
        writer.writerow(colnames)
        writer.writerows(rows)

export_to_csv = PythonOperator(
    task_id="export_to_csv",
    python_callable=_export_to_csv,
    dag=dag,
)

get_data >> extract_gz >> fetch_pageviews >> write_to_postgres >> export_to_csv
