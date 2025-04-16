import streamlit as st
import pandas as pd
import random

st.set_page_config(page_title="Случайные фильмы", page_icon=None, layout="wide")

# Заголовок приложения
st.title("🎬 Рекомендательная система фильмов")
st.subheader("Случайная подборка из 10 фильмов")


# Функция для загрузки данных
@st.cache_data
def load_data():
    try:
        data = pd.read_csv("data/basic_data.csv")
        return data
    except FileNotFoundError:
        st.error("Файл 'basic_data.csv' не найден. Пожалуйста, проверьте путь к файлу.")
        return None


# Загрузка данных
movies_df = load_data()

if movies_df is not None:
    required_columns = {"movie_title", "description", "image_url"}
    if not required_columns.issubset(movies_df.columns):
        st.error(
            "Файл должен содержать колонки 'movie_title', 'description' и 'image_url'."
        )
    else:
        if len(movies_df) < 10:
            st.warning("В файле меньше 10 фильмов. Показаны все доступные:")
            selected_movies = movies_df.copy()
        else:
            selected_movies = movies_df.sample(10)

        # Настройка плейсхолдера
        placeholder_url = (
            "https://critics.io/img/movies/poster-placeholder.png?text=Нет+изображения"
        )

        # Построение сетки: 5 рядов, по 4 колонки (image-expander x2)
        st.success("Вот ваша случайная подборка:")

        rows = [
            selected_movies.iloc[i : i + 2] for i in range(0, 10, 2)
        ]  # 5 рядов по 2 фильма

        for row in rows:
            cols = st.columns(4)

            for idx, (i, movie) in enumerate(row.iterrows()):
                col_img = cols[idx * 2]
                col_exp = cols[idx * 2 + 1]

                # Безопасная обработка URL
                image_url = movie["image_url"]
                if (
                    pd.notna(image_url)
                    and isinstance(image_url, str)
                    and image_url.strip() != ""
                ):
                    col_img.image(image_url, use_container_width=True)
                else:
                    col_img.image(placeholder_url, use_container_width=True)

                with col_exp.expander(f"**🎥 {movie['movie_title']}**"):
                    st.write(movie["description"])
                    st.write(f"**ID фильма:** {movie.name}")

# Боковая панель
with st.sidebar:
    st.header("Настройки")
    if st.button("Обновить подборку") and movies_df is not None:
        st.experimental_rerun()
