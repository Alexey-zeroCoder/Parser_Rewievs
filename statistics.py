# Statistics.py
import sqlite3

# region Константы
DB_FILE = "reviews.db"  # Путь к файлу базы данных
# DB_FILE = "/Users/aleksejiutmanov/PycharmProjects/Rewiews_collecter/reviews.db"  # Абсолютный путь

# ANSI escape-коды для цветного вывода
RED = "\033[91m"
GREEN = "\033[92m"
BLUE = "\033[94m"
RESET = "\033[0m"
# endregion

# region Вспомогательные функции
def execute_query(query, params=None, db_file=DB_FILE):
    """
    Выполняет SQL-запрос к базе данных и возвращает результат.

    Args:
        query: Строка SQL-запроса.
        params:  Кортеж параметров для запроса (или None, если параметров нет).
                 Используется для защиты от SQL-инъекций.
        db_file: Путь к файлу базы данных

    Returns:
        Результат запроса (список кортежей) или None в случае ошибки.
    """
    try:
        with sqlite3.connect(db_file) as conn:  # Подключаемся к БД.
            cursor = conn.cursor()  # Создаем курсор.
            if params:
                cursor.execute(query, params)  # Выполняем запрос с параметрами.
            else:
                cursor.execute(query)  # Выполняем запрос без параметров.
            return cursor.fetchall()  # Возвращаем результат запроса.
    except sqlite3.Error as e:
        print(f"{RED}Ошибка при выполнении SQL-запроса: {e}{RESET}")
        return None  # Возвращаем None в случае ошибки.


# endregion

# region Функции статистики

def get_total_reviews_count(db_file=DB_FILE):
    """Возвращает общее количество отзывов в базе данных."""
    query = "SELECT COUNT(*) FROM reviews"  # SQL-запрос для подсчета всех записей.
    result = execute_query(query, db_file=db_file)  # Выполняем запрос.
    if result:
        return result[0][0]  # Возвращаем первый элемент первого кортежа (общее количество).
    return 0  # Если произошла ошибка возвращаем 0


def get_reviews_count_by_length(min_length, max_length=None, db_file=DB_FILE):
    """
    Возвращает количество отзывов с длиной текста в заданном диапазоне.

    Args:
        min_length: Минимальная длина текста.
        max_length: Максимальная длина текста (None - без верхнего ограничения).

    Returns:
        Количество отзывов, удовлетворяющих условию.
    """
    if max_length is None:
        query = "SELECT COUNT(*) FROM reviews WHERE length >= ?"  # Запрос без верхнего ограничения.
        params = (min_length,)
    else:
        query = "SELECT COUNT(*) FROM reviews WHERE length >= ? AND length <= ?"  # Запрос с верхним ограничением.
        params = (min_length, max_length)
    result = execute_query(query, params, db_file=db_file)
    if result:
        return result[0][0]
    return 0


def get_reviews_count_with_mat(db_file=DB_FILE):
    """Возвращает количество отзывов, содержащих ненормативную лексику."""
    query = "SELECT COUNT(*) FROM reviews WHERE has_mat = 1"  # has_mat - это BOOLEAN (0 или 1).
    result = execute_query(query, db_file=db_file)
    if result:
        return result[0][0]
    return 0


def get_reviews_count_by_category(category, db_file=DB_FILE):
    """Возвращает количество отзывов в заданной категории."""
    query = "SELECT COUNT(*) FROM reviews WHERE category = ?"  # Используем параметризованный запрос.
    params = (category,)  # Параметры передаются в виде кортежа.
    result = execute_query(query, params, db_file=db_file)
    if result:
        return result[0][0]
    return 0


def get_reviews_count_by_object(object_url, db_file=DB_FILE):
    """Возвращает количество отзывов для заданного объекта (по URL)."""
    query = "SELECT COUNT(*) FROM reviews WHERE object_url = ?"
    params = (object_url,)
    result = execute_query(query, params, db_file=db_file)
    if result:
        return result[0][0]
    return 0


def get_reviews_count_by_date(start_date, end_date, db_file=DB_FILE):
    """
    Возвращает количество отзывов, собранных в заданном диапазоне дат.

    Args:
        start_date: Начальная дата (строка в формате 'YYYY-MM-DD').
        end_date: Конечная дата (строка в формате 'YYYY-MM-DD').

    Returns:
        Количество отзывов.
    """
    query = "SELECT COUNT(*) FROM reviews WHERE date_scraped BETWEEN ? AND ?"
    params = (start_date, end_date)  # Передаем даты как строки
    result = execute_query(query, params, db_file=db_file)
    if result:
        return result[0][0]
    return 0
# endregion

# region Примеры использования (если файл запущен напрямую)

total_reviews = get_total_reviews_count()
print(f"{BLUE}Общее количество отзывов: {total_reviews}{RESET}")

reviews_100_1000 = get_reviews_count_by_length(500, 2000)
print(f"{BLUE}Количество отзывов длиной от 100 до 1000 символов: {reviews_100_1000}{RESET}")

reviews_100_plus = get_reviews_count_by_length(100)
print(f"{BLUE}Количество отзывов длиной от 100 символов и более: {reviews_100_plus}{RESET}")

reviews_with_mat = get_reviews_count_with_mat()
print(f"{BLUE}Количество отзывов с ненормативной лексикой: {reviews_with_mat}{RESET}")

some_category = "byitovaya-tehnika"  # Пример категории.
reviews_in_category = get_reviews_count_by_category(some_category)
print(f"{BLUE}Количество отзывов в категории '{some_category}': {reviews_in_category}{RESET}")

some_object_url = "https://vseotzyvy.ru/item/12345/"  # Пример URL объекта.
reviews_for_object = get_reviews_count_by_object(some_object_url)
print(f"{BLUE}Количество отзывов для объекта '{some_object_url}': {reviews_for_object}{RESET}")

# Пример с датами
start_date_str = '2023-01-01'  # Начальная дата.
end_date_str = '2026-01-01'  # Конечная дата.
reviews_in_date_range = get_reviews_count_by_date(start_date_str, end_date_str)
print(
    f"{BLUE}Количество отзывов, собранных между {start_date_str} и {end_date_str}: {reviews_in_date_range}{RESET}")
# endregion