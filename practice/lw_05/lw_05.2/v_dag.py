import json
import pathlib
import requests
import requests.exceptions as requests_exceptions
import pandas as pd
import airflow.utils.dates

from airflow import DAG
from airflow.operators.python import PythonOperator

# =========================================================
# КОНФИГУРАЦИЯ (Storage Layer)
# =========================================================

# Общая папка данных (проброшена через Docker volume → ./data)
DATA_DIR = "/opt/airflow/data"

# Папка для изображений
IMAGES_DIR = f"{DATA_DIR}/images"

# JSON файл с данными запусков
JSON_FILE = f"{DATA_DIR}/launches.json"

# Ограничение на количество изображений (ускоряет выполнение)
MAX_IMAGES = 10

# API источник (Source Layer)
API_URL = f"https://ll.thespacedevs.com/2.3.0/launches/upcoming/?format=json&mode=list&limit={MAX_IMAGES}"


# =========================================================
# ЗАДАНИЕ 2: УВЕДОМЛЕНИЯ ПРИ СБОЯХ
# =========================================================

def notify_failure(context):
    """
    Callback-функция Airflow.
    Вызывается при ошибке любой задачи.
    """
    print(f"❌ Ошибка в задаче: {context['task_instance'].task_id}")


# =========================================================
# ПОДГОТОВКА ДАННЫХ
# =========================================================

def clean_images():
    """
    Очистка старых изображений перед новым запуском.
    (Важно: не удаляем весь data/, чтобы не потерять JSON и CSV)
    """
    pathlib.Path(IMAGES_DIR).mkdir(parents=True, exist_ok=True)

    for file in pathlib.Path(IMAGES_DIR).glob("*"):
        file.unlink()

    print("🧹 Папка изображений очищена")


# =========================================================
# ЗАДАНИЕ 1: АВТОМАТИЧЕСКАЯ ОБРАБОТКА ДАННЫХ (ETL)
# =========================================================

# -------- EXTRACT --------
def download_launches():
    """
    Загрузка данных о запусках из внешнего API.
    """
    response = requests.get(API_URL, timeout=30)

    if response.status_code != 200:
        raise Exception("Ошибка загрузки данных API")

    data = response.json()

    with open(JSON_FILE, "w", encoding="utf-8") as f:
        json.dump(data, f)

    print("✅ JSON успешно загружен")


# -------- TRANSFORM --------
def parse_launches():
    """
    Преобразование JSON → структурированный CSV.
    """
    with open(JSON_FILE, encoding="utf-8") as f:
        data = json.load(f)

    launches = data.get("results", [])

    parsed = []

    for launch in launches:
        parsed.append({
            "name": launch.get("name"),
            "date": launch.get("net"),
            "status": launch.get("status", {}).get("name"),
            "image": launch.get("image", {}).get("image_url")
            if isinstance(launch.get("image"), dict)
            else launch.get("image")
        })

    df = pd.DataFrame(parsed)

    if df.empty:
        raise ValueError("❌ Нет данных для обработки")

    df.to_csv(f"{DATA_DIR}/parsed_launches.csv", index=False)

    print("✅ CSV с обработанными данными создан")


# -------- LOAD --------
def download_images():
    """
    Скачивание изображений ракет.
    """
    df = pd.read_csv(f"{DATA_DIR}/parsed_launches.csv")

    pathlib.Path(IMAGES_DIR).mkdir(parents=True, exist_ok=True)

    for i, row in df.iterrows():
        url = row["image"]

        if pd.isna(url):
            continue

        try:
            response = requests.get(url, timeout=20)
            response.raise_for_status()

            filename = f"{IMAGES_DIR}/{i}.jpg"

            with open(filename, "wb") as f:
                f.write(response.content)

            print(f"📷 Скачано: {filename}")

        except Exception as e:
            print(f"⚠️ Ошибка загрузки изображения: {e}")


# =========================================================
# ЗАДАНИЕ 3: АНАЛИЗ ЧАСТОТЫ ЗАПУСКОВ
# =========================================================

def analyze_launch_frequency():
    """
    Анализ количества запусков по месяцам.
    """
    df = pd.read_csv(f"{DATA_DIR}/parsed_launches.csv")

    df["date"] = pd.to_datetime(df["date"])
    df["month"] = df["date"].dt.to_period("M")

    result = df.groupby("month").size().reset_index(name="launch_count")

    result.to_csv(f"{DATA_DIR}/launch_frequency.csv", index=False)

    print("📊 Частота запусков рассчитана")
    print(result)


# =========================================================
# ОПРЕДЕЛЕНИЕ DAG
# =========================================================

with DAG(
    dag_id="listing_sabina_rocket",  # Уникальное имя DAG
    start_date=airflow.utils.dates.days_ago(1),
    schedule_interval=None,  # запуск вручную
    catchup=False,
    default_args={
        "retries": 2  # повтор при ошибках
    }
) as dag:

    # Подготовка
    t1 = PythonOperator(
        task_id="clean_images",
        python_callable=clean_images,
        on_failure_callback=notify_failure
    )

    # Extract
    t2 = PythonOperator(
        task_id="download_json",
        python_callable=download_launches,
        on_failure_callback=notify_failure
    )

    # Transform
    t3 = PythonOperator(
        task_id="parse_json",
        python_callable=parse_launches,
        on_failure_callback=notify_failure
    )

    # Load
    t4 = PythonOperator(
        task_id="download_images",
        python_callable=download_images,
        on_failure_callback=notify_failure
    )

    # Analytics
    t5 = PythonOperator(
        task_id="analyze_launch_frequency",
        python_callable=analyze_launch_frequency,
        on_failure_callback=notify_failure
    )

    # =========================================================
    # ПАЙПЛАЙН (логика DAG)
    # =========================================================

    t1 >> t2 >> t3 >> t4 >> t5