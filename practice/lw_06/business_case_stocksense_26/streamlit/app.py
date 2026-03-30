import streamlit as st
import pandas as pd
import plotly.express as px
import os

# Настройка страницы
st.set_page_config(page_title="StockSense BI Dashboard", layout="wide")

st.title("Бизнес-кейс «StockSense». Аналитика")

# Путь к CSV (общая папка data, примонтированная в Docker)
CSV_PATH = "/opt/airflow/data/pageview_data.csv"

# Загрузка данных из CSV
def load_data():
    if not os.path.exists(CSV_PATH):
        return pd.DataFrame()
    try:
        df = pd.read_csv(CSV_PATH)
        df['datetime'] = pd.to_datetime(df['datetime'])
        return df
    except Exception as e:
        st.error(f"Ошибка чтения CSV: {e}")
        return pd.DataFrame()

df = load_data()

if not df.empty:
    st.success(f"Данные загружены (строк: {len(df)})")

    # Убираем дубликаты если есть
    df = df.drop_duplicates()

    # --- Столбчатая диаграмма ---
    st.subheader("Столбчатая диаграмма. Просмотры по компаниям")
    bar_df = df.groupby('pagename')['pageviewcount'].sum().reset_index()
    fig_bar = px.bar(
        bar_df,
        x='pagename',
        y='pageviewcount',
        color='pagename',
        text='pageviewcount',
        labels={'pagename': 'Компания', 'pageviewcount': 'Просмотры'},
        title='Суммарные просмотры Wikipedia по компаниям'
    )
    fig_bar.update_traces(textposition='outside')
    st.plotly_chart(fig_bar, use_container_width=True)

    # --- Круговая диаграмма ---
    st.subheader("Круговая диаграмма. Доля просмотров")
    fig_pie = px.pie(
        bar_df,
        values='pageviewcount',
        names='pagename',
        title='Доля просмотров каждой компании',
        hole=0.3
    )
    fig_pie.update_traces(textinfo='label+percent+value')
    st.plotly_chart(fig_pie, use_container_width=True)

    # --- Таблица ---
    st.subheader("Детальные данные из DWH")
    st.dataframe(df, use_container_width=True)
    # --- Круговая диаграмма (Sony vs Остальные) ---
    st.subheader("Круговая диаграмма. Доля Sony против остальных")

    sony_views = bar_df[bar_df['pagename'] == 'Sony']['pageviewcount'].sum()
    total_views = bar_df['pageviewcount'].sum()
    other_views = total_views - sony_views

    sony_df = pd.DataFrame({
        'category': ['Sony', 'Остальные'],
        'views': [sony_views, other_views]
    })

    fig_pie_sony = px.pie(
        sony_df,
        values='views',
        names='category',
        title='Доля Sony от общего числа просмотров',
        color='category',
        color_discrete_map={
            'Sony': '#1f77b4',
            'Остальные': '#ff7f0e'
        }
    )

    fig_pie_sony.update_traces(textinfo='label+percent+value')

    st.plotly_chart(fig_pie_sony, use_container_width=True)

else:
    st.warning("Файл данных не найден. Запустите DAG в Airflow и дождитесь завершения всех задач.")
    if st.button("Обновить страницу"):
        st.rerun()
