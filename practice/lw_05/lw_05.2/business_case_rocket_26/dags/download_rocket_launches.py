import json
import pathlib
import airflow.utils.dates
import requests
import requests.exceptions as requests_exceptions
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator

# --- Конфигурационные переменные ---

# Основная папка для данных, которая проброшена на хост в ./data
DATA_DIR = "/opt/airflow/data"
# Вложенная папка для изображений (будет создаваться заново после очистки)
IMAGES_DIR = f"{DATA_DIR}/images"
# Временный файл для JSON внутри контейнера
TMP_JSON_FILE = f"{DATA_DIR}/launches.json"
# Ограничиваем количество картинок на одну прогонку DAG (ускоряет и дает прогресс).
MAX_IMAGES = 10
# URL API (Swagger для v2.3.0: список upcoming запусков)
# Важно: в v2.3.0 endpoint называется `/launches/upcoming/`, а не `/launch/upcoming/`.
# Для ускорения и уменьшения размера ответа используем `mode=list`
# (в таком режиме JSON значительно компактнее, но `image.image_url` сохраняется).
API_URL = f"https://ll.thespacedevs.com/2.3.0/launches/upcoming/?format=json&mode=list&limit={MAX_IMAGES}"

# --- Определение DAG ---
dag = DAG(
    dag_id="download_rocket_launch",
    description="Cleans the entire data directory, then downloads rocket pictures.",
    start_date=airflow.utils.dates.days_ago(14),
    schedule_interval="@daily",
    catchup=False
)

# --- Определение Задач ---

# 1. ЗАДАЧА ОЧИСТКИ. Удаляем всё содержимое папки /opt/airflow/data
clean_data_directory = BashOperator(
    task_id="clean_data_directory",
    # Надежная команда: сначала убеждаемся, что папка существует, потом удаляем всё ВНУТРИ неё
    bash_command=f"mkdir -p {DATA_DIR} && rm -rf {DATA_DIR}/*",
    dag=dag,
)

# 2. ЗАДАЧА СКАЧИВАНИЯ JSON: Скачиваем свежий список запусков
download_launches = BashOperator(
    task_id="download_launches",
    # `-f` чтобы не скачивать HTML/404 в JSON-файл и не падать потом на json.load.
    bash_command=(
        f"curl -fSL --connect-timeout 15 --max-time 120 --progress-bar "
        f"-H 'Accept: application/json' -o {TMP_JSON_FILE} '{API_URL}'"
    ),
    dag=dag,
)

# 3. ЗАДАЧА СКАЧИВАНИЯ КАРТИНОК. Обрабатываем JSON и загружаем фото
def _get_pictures():
    # Эта команда создаст заново папку /images внутри /data
    pathlib.Path(IMAGES_DIR).mkdir(parents=True, exist_ok=True)
    with open(TMP_JSON_FILE, encoding="utf-8") as f:
        try:
            launches = json.load(f)
        except json.JSONDecodeError as e:
            # Чтобы в логах было видно, что прилетело вместо JSON (например, HTML error page).
            f.seek(0)
            preview = f.read(500)
            raise RuntimeError(
                f"Launch API returned non-JSON payload. json error: {e}. Payload preview: {preview!r}"
            ) from e

        # В API v2.3.0 поле `image` - это объект, внутри него лежит `image_url`.
        image_urls = []
        for launch in launches.get("results", []):
            image = launch.get("image")
            if isinstance(image, dict):
                image_url = image.get("image_url")
                if image_url:
                    image_urls.append(image_url)
            elif isinstance(image, str) and image:
                # На случай неожиданных форматов данных.
                image_urls.append(image)

        # Убираем дубликаты, чтобы не скачивать одно и то же.
        image_urls = list(dict.fromkeys(image_urls))[:MAX_IMAGES]
        for image_index, image_url in enumerate(image_urls, start=1):
            print(f"[{image_index}/{len(image_urls)}] Downloading: {image_url}")
            try:
                response = requests.get(image_url, timeout=30)
                response.raise_for_status()
                image_filename = image_url.split("/")[-1]
                target_file = f"{IMAGES_DIR}/{image_filename}"
                with open(target_file, "wb") as f:
                    f.write(response.content)
                print(f"Downloaded {image_url} to {target_file}")
            except requests_exceptions.MissingSchema:
                print(f"{image_url} appears to be an invalid URL.")
            except requests_exceptions.RequestException as e:
                print(f"Could not download {image_url}: {e}")

get_pictures = PythonOperator(
    task_id="get_pictures",
    python_callable=_get_pictures,
    dag=dag
)

# 4. ЗАДАЧА УВЕДОМЛЕНИЯ. Сообщаем о результате
notify = BashOperator(
    task_id="notify",
    bash_command=f'echo "There are now $(ls {IMAGES_DIR}/ | wc -l) images in {IMAGES_DIR}."',
    dag=dag,
)

# --- Порядок выполнения ---
# Сначала чистим, потом скачиваем JSON, потом скачиваем картинки, потом уведомляем
clean_data_directory >> download_launches >> get_pictures >> notify