import airflow
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago
import requests
import pandas as pd
from sklearn.linear_model import LinearRegression
import joblib


default_args = {
    'owner': 'airflow',
    'start_date': days_ago(1),
}

dag = DAG(
    dag_id="variant_6",
    default_args=default_args,
    description="ETL pipeline variant 6: Rome weather, mean fill, bar chart",
    schedule_interval="@daily",
)

# 1. Получение прогноза погоды (РИМ, 5 дней)
def fetch_weather_forecast():
    import os
    api_key = "f0a57694e61a477d839182816261303"

    url = f"https://api.weatherapi.com/v1/forecast.json?key={api_key}&q=Rome&days=5"

    response = requests.get(url)
    data = response.json()

    forecast_data = [
        (day['date'], day['day']['avgtemp_c'])
        for day in data['forecast']['forecastday']
    ]

    df = pd.DataFrame(forecast_data, columns=['date', 'temperature'])

    data_dir = '/opt/airflow/data'
    os.makedirs(data_dir, exist_ok=True)

    df.to_csv(os.path.join(data_dir, 'weather_forecast.csv'), index=False)

    print("Weather forecast for Rome saved.")

# 2. Очистка данных погоды (ЗАМЕНА ПРОПУСКОВ СРЕДНИМ)
def clean_weather_data():
    import os
    data_dir = '/opt/airflow/data'
    df = pd.read_csv(os.path.join(data_dir, 'weather_forecast.csv'))

    df['temperature'] = df['temperature'].fillna(df['temperature'].mean())

    df.to_csv(os.path.join(data_dir, 'clean_weather.csv'), index=False)

    print("Cleaned weather data saved.")

# 3. Получение данных продаж
def fetch_sales_data():
    import os

    sales_data = {
        'date': ['2025-03-21','2025-03-22','2025-03-23','2025-03-24','2025-03-25'],
        'sales': [10,15,20,5,3]
    }

    df = pd.DataFrame(sales_data)

    data_dir = '/opt/airflow/data'
    os.makedirs(data_dir, exist_ok=True)

    df.to_csv(os.path.join(data_dir, 'sales_data.csv'), index=False)

    print("Sales data saved.")

# 4. Очистка продаж (СРЕДНЕЕ)
def clean_sales_data():
    import os
    data_dir = '/opt/airflow/data'
    df = pd.read_csv(os.path.join(data_dir, 'sales_data.csv'))

    df['sales'] = df['sales'].fillna(df['sales'].mean())

    df.to_csv(os.path.join(data_dir, 'clean_sales.csv'), index=False)

    print("Cleaned sales data saved.")

# 5. Объединение данных
def join_datasets():
    import os
    data_dir = '/opt/airflow/data'

    weather_df = pd.read_csv(os.path.join(data_dir, 'clean_weather.csv'))
    sales_df = pd.read_csv(os.path.join(data_dir, 'clean_sales.csv'))

    joined_df = pd.merge(weather_df, sales_df, how='cross')

    joined_df = joined_df[['temperature', 'sales']]

    joined_df.to_csv(os.path.join(data_dir, 'joined_data.csv'), index=False)

    print("Joined dataset saved.")

# 6. Обучение модели
def train_ml_model():
    import os
    data_dir = '/opt/airflow/data'
    df = pd.read_csv(os.path.join(data_dir, 'joined_data.csv'))

    X = df[['temperature']]
    y = df['sales']

    model = LinearRegression()
    model.fit(X, y)

    joblib.dump(model, os.path.join(data_dir, 'ml_model.pkl'))

    print("Model trained.")

# 7. BAR CHART
def plot_bar_chart():
    import os
    import seaborn as sns
    import matplotlib.pyplot as plt

    data_dir = '/opt/airflow/data'

    df = pd.read_csv(os.path.join(data_dir, 'clean_weather.csv'))

    sns.set_theme(style="whitegrid")

    plt.figure(figsize=(10,6))

    ax = sns.barplot(
        x='date',
        y='temperature',
        data=df,
        palette="coolwarm"
    )

    plt.title("5-Day Temperature Forecast for Rome", fontsize=16)
    plt.xlabel("Date", fontsize=12)
    plt.ylabel("Temperature (°C)", fontsize=12)

    plt.xticks(rotation=45)

    # подписи значений на столбцах
    for p in ax.patches:
        ax.annotate(
            f'{p.get_height():.1f}',
            (p.get_x() + p.get_width()/2, p.get_height()),
            ha='center',
            va='bottom',
            fontsize=10
        )

    plt.tight_layout()

    plt.savefig(os.path.join(data_dir, "rome_temperature_bar_chart.png"))

    print("Bar chart saved.")

# операторы
fetch_weather_task = PythonOperator(
    task_id="fetch_weather_forecast",
    python_callable=fetch_weather_forecast,
    dag=dag,
)

clean_weather_task = PythonOperator(
    task_id="clean_weather_data",
    python_callable=clean_weather_data,
    dag=dag,
)

fetch_sales_task = PythonOperator(
    task_id="fetch_sales_data",
    python_callable=fetch_sales_data,
    dag=dag,
)

clean_sales_task = PythonOperator(
    task_id="clean_sales_data",
    python_callable=clean_sales_data,
    dag=dag,
)

join_task = PythonOperator(
    task_id="join_datasets",
    python_callable=join_datasets,
    dag=dag,
)

train_model_task = PythonOperator(
    task_id="train_ml_model",
    python_callable=train_ml_model,
    dag=dag,
)

bar_chart_task = PythonOperator(
    task_id="plot_bar_chart",
    python_callable=plot_bar_chart,
    dag=dag,
)

# зависимости
fetch_weather_task >> clean_weather_task
fetch_sales_task >> clean_sales_task

[clean_weather_task, clean_sales_task] >> join_task

join_task >> train_model_task >> bar_chart_task