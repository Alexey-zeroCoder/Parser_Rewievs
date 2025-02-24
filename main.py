import asyncio
import aiohttp
from bs4 import BeautifulSoup
import os
import re
import sqlite3
import pandas as pd
from datetime import datetime

#region Константы и настройки
MAX_CONCURRENCY = 5  # Максимальное количество одновременных запросов.
BASE_URL = "https://vseotzyvy.ru"  # Базовый URL сайта.
REVIEWS_FILE = 'reviews.txt'  # Файл для сохранения отзывов (текстовый).
MAT_WORDS_FILE = 'mat_words.txt'  # Файл со списком ненормативных слов.
PROGRESS_FILE = 'progress.txt'  # Файл для сохранения прогресса парсинга.
DB_FILE = os.path.join(os.path.dirname(__file__), 'reviews.db')  # *Абсолютный* путь к файлу БД SQLite.

# ANSI escape-коды для цветного вывода.
RED = "\033[91m"
GREEN = "\033[92m"
BLUE = "\033[94m"
RESET = "\033[0m"
#endregion

#region Вспомогательные функции
def clean_text(text):
    """Очищает текст отзыва от пустых строк."""
    lines = text.split('\n')
    cleaned_lines = [line for line in lines if line.strip()]  # Убираем пустые строки.
    return '\n'.join(cleaned_lines)

def contains_mat(text, mat_words):
    """Проверяет наличие ненормативной лексики в тексте."""
    text_lower = re.sub(r'[^\w\s]', '', text.lower())  # Приводим к нижнему регистру, убираем пунктуацию.
    return next((word for word in text_lower.split() if word in mat_words), None)  # Ищем первое совпадение.

async def fetch(session, url):
    """Асинхронно скачивает страницу по URL."""
    async with session.get(url) as response:
        response.raise_for_status()  # Проверяем HTTP-статус (если не 200 OK, будет исключение).
        return await response.text()  # Возвращаем текст страницы.
#endregion

#region Функции для работы с категориями и объектами
async def get_categories(session):
    """Получает список URL категорий с главной страницы сайта."""
    html = await fetch(session, BASE_URL)  # Скачиваем главную страницу.
    soup = BeautifulSoup(html, 'html.parser')  # Парсим HTML.
    categories = []
    for link in soup.find_all('a', href=True):  # Находим все ссылки.
        if link['href'].startswith('/category/'):  # Если ссылка ведет на категорию.
            categories.append(BASE_URL + link['href'])  # Добавляем полный URL категории в список.
    return categories

async def fetch_objects_page(session, category_url, page_number, seen_urls):
    """Получает URL объектов (товаров/услуг) с одной страницы категории."""
    url = f"{category_url}?page={page_number}"  # Формируем URL страницы категории.
    try:
        html = await fetch(session, url)  # Скачиваем страницу.
    except aiohttp.ClientError as e:
        print(f"{RED}Ошибка загрузки: {url}: {e}{RESET}")  # Выводим сообщение об ошибке.
        return []  # Возвращаем пустой список, если не удалось скачать страницу.

    soup = BeautifulSoup(html, 'html.parser')  # Парсим HTML.
    new_urls = []
    for link in soup.find_all('a', href=True, title=True):  # Находим все ссылки с атрибутом title.
        obj_url = BASE_URL + link['href']  # Формируем полный URL объекта.
        if obj_url not in seen_urls:  # Если URL объекта еще не встречался.
            seen_urls.add(obj_url)  # Добавляем URL в множество просмотренных.
            new_urls.append(obj_url)  # Добавляем URL в список.
    return new_urls

async def get_target_object_url(session, category_url, target_index):
    """Получает URL целевого объекта по его индексу в категории."""
    seen_urls = set()  # Множество для хранения уже просмотренных URL объектов.
    page_number = (target_index // 15) + 1  # Вычисляем номер страницы (15 объектов на странице).
    obj_position = target_index % 15  # Вычисляем позицию объекта на странице.
    objects_fetched = (page_number - 1) * 15  # Количество объектов, просмотренных на предыдущих страницах.

    while True:
        object_urls = await fetch_objects_page(session, category_url, page_number, seen_urls)  # Получаем URL объектов с текущей страницы.
        if not object_urls:  # Если объектов на странице нет (или ошибка загрузки).
            break  # Выходим из цикла.
        if objects_fetched + len(object_urls) > target_index:  # Если целевой объект на текущей странице.
            return object_urls[obj_position], page_number, obj_position  # Возвращаем URL, номер страницы и позицию.
        objects_fetched += len(object_urls)  # Увеличиваем счетчик просмотренных объектов.
        page_number += 1  # Переходим к следующей странице.
    return None, page_number  # Возвращаем None, если объект не найден.
#endregion

#region Функции для работы с отзывами
async def get_reviews_urls(session, object_url, page):
    """Получает URL отзывов с одной страницы объекта."""
    url = f"{object_url}?page={page}"  # Формируем URL страницы отзывов.
    try:
        html = await fetch(session, url)  # Скачиваем страницу.
    except aiohttp.ClientError as e:
        print(f"{RED}Ошибка загрузки: {url}: {e}{RESET}")  # Выводим сообщение об ошибке.
        return []  # Возвращаем пустой список.

    soup = BeautifulSoup(html, 'html.parser')  # Парсим HTML.
    return [BASE_URL + link['href'] for link in soup.find_all('a', href=True, class_='r_space')]  # Возвращаем список URL отзывов.

async def process_review(session, review_url, mat_words, lock, processed_reviews, file_queue, category, object_url):
    """Обрабатывает один отзыв: скачивает, проверяет, добавляет в очереди."""
    try:
        review_html = await fetch(session, review_url)  # Скачиваем страницу отзыва.
    except aiohttp.ClientError as e:
        print(f"{RED}Ошибка загрузки: {review_url}: {e}{RESET}")  # Выводим сообщение об ошибке.
        return None  # Возвращаем None, если не удалось скачать страницу.

    review_soup = BeautifulSoup(review_html, 'html.parser')  # Парсим HTML.
    review_text_element = review_soup.find('span', class_='description line-height-comfort')  # Ищем элемент с текстом отзыва.

    if review_text_element:  # Если элемент с текстом отзыва найден.
        cleaned_text = clean_text(review_text_element.get_text(strip=True))  # Извлекаем и очищаем текст отзыва.
        async with lock:  # Блокируем доступ к `processed_reviews` (чтобы избежать состояния гонки).
            if cleaned_text in processed_reviews:  # Если отзыв уже был обработан.
                message = f"{RED}Ошибка: Отзыв уже есть. Ссылка: {review_url}{RESET}"
                print(message)
                return None  # Возвращаем None.
            mat_word = contains_mat(cleaned_text, mat_words)  # Проверяем наличие ненормативной лексики.
            if mat_word:  # Если найден мат.
                message = f"{RED}Ошибка: Найден мат: {mat_word}. Ссылка: {review_url}{RESET}"
                print(message)
                has_mat = True  # Устанавливаем флаг наличия мата.
            else:  # Если мат не найден.
                message = f"{GREEN}Отзыв добавлен: {len(cleaned_text)} симв., ссылка: {review_url}{RESET}"
                print(message)
                has_mat = False  # Устанавливаем флаг отсутствия мата.
            processed_reviews.add(cleaned_text)  # Добавляем текст отзыва в множество обработанных.
            await file_queue.put((cleaned_text, review_url))  # Добавляем текст и URL в очередь для записи в файл.

            # Возвращаем словарь с данными отзыва.
            return {
                'length': len(cleaned_text),
                'category': category,
                'object_url': object_url,
                'review_url': review_url,
                'text': cleaned_text,
                'has_mat': has_mat,
                'date_scraped': datetime.now()  # Добавляем текущую дату и время.
            }
    else:  # Если элемент с текстом отзыва не найден.
        message = f"{RED}Ошибка: Отзыв не найден. Ссылка: {review_url}{RESET}"
        print(message)
        return None  # Возвращаем None.
#endregion

#region Функции для сохранения и загрузки данных
async def save_to_file(file_queue, file_lock):
    """Асинхронно записывает отзывы в текстовый файл."""
    while True:
        cleaned_text, _ = await file_queue.get()  # Получаем текст отзыва и URL из очереди (URL не используется).
        async with file_lock:  # Блокируем доступ к файлу (чтобы избежать одновременной записи из разных задач).
            try:
                with open(REVIEWS_FILE, 'a', encoding='utf-8') as f:  # Открываем файл для добавления текста.
                    f.write(f"Source Text: \nRephrased Text: {cleaned_text}\nLength: {len(cleaned_text)}\n\n")  # Записываем отзыв.
            except Exception as e:
                print(f"{RED}Ошибка записи в файл: {e}{RESET}")  # Выводим сообщение об ошибке.
        file_queue.task_done()  # Сообщаем очереди, что задача выполнена.

def reload_from_disk(file_path):
    """Загружает множество уже обработанных отзывов из текстового файла."""
    if os.path.exists(file_path):  # Если файл существует.
        reviews = set()  # Создаем пустое множество для хранения отзывов.
        try:
            with open(file_path, 'r', encoding='utf-8') as f:  # Открываем файл для чтения.
                for line in f:  # Читаем файл построчно.
                    if line.startswith("Rephrased Text:"):  # Если строка начинается с "Rephrased Text:".
                        reviews.add(line.split(":", 1)[1].strip())  # Извлекаем текст отзыва и добавляем в множество.
        except Exception as e:
            print(f"{RED}Ошибка чтения {file_path}: {e}.{RESET}")  # Выводим сообщение об ошибке.
            return {"reviews": set()}  # Возвращаем пустое множество в случае ошибки.
        return {"reviews": reviews}  # Возвращаем множество отзывов.
    return {"reviews": set()}  # Возвращаем пустое множество, если файл не существует.

def create_db():
    """Создает базу данных SQLite и таблицу reviews, если они не существуют."""
    with sqlite3.connect(DB_FILE) as conn:  # Подключаемся к базе данных (файл будет создан, если не существует).
        cursor = conn.cursor()  # Создаем курсор для выполнения SQL-запросов.
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS reviews (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                length INTEGER,
                category TEXT,
                object_url TEXT,
                review_url TEXT,
                text TEXT,
                has_mat BOOLEAN,
                date_scraped DATETIME
            )
        ''')  # Создаем таблицу reviews, если она не существует.
        conn.commit()  # Сохраняем изменения в базе данных.

def save_reviews_to_db(reviews_data):
    """Сохраняет список отзывов в базу данных SQLite (пакетная вставка)."""
    if not reviews_data:  # Если список отзывов пуст.
        return  # Ничего не делаем.
    try:
        with sqlite3.connect(DB_FILE) as conn:  # Подключаемся к базе данных.
            cursor = conn.cursor()  # Создаем курсор.
            cursor.executemany('''
                INSERT INTO reviews (length, category, object_url, review_url, text, has_mat, date_scraped)
                VALUES (:length, :category, :object_url, :review_url, :text, :has_mat, :date_scraped)
            ''', reviews_data)  # Выполняем *пакетную* вставку данных.  Используем именованные плейсхолдеры.
            conn.commit()  # Сохраняем изменения.
            print(f"{GREEN}Сохранено {len(reviews_data)} отзывов в БД{RESET}")  # Выводим сообщение.
    except sqlite3.Error as e:
        print(f"{RED}Ошибка записи в БД: {e}{RESET}")  # Выводим сообщение об ошибке.
#endregion

#region Главная функция
async def main():
    """Главная функция, запускающая процесс парсинга."""
    print(f"Текущая рабочая директория: {os.getcwd()}")  # Выводим текущую рабочую директорию (для отладки).
    create_db()  # Создаем базу данных и таблицу.

    async with aiohttp.ClientSession() as session:  # Создаем сессию aiohttp.
        categories = await get_categories(session)  # Получаем список категорий.
        try:
            with open(MAT_WORDS_FILE, 'r', encoding='utf-8') as f:  # Открываем файл с ненормативными словами.
                mat_words = set(line.strip() for line in f)  # Загружаем слова в множество (для быстрого поиска).
        except FileNotFoundError:
            print(f"{RED}Ошибка: Файл '{MAT_WORDS_FILE}' не найден.{RESET}")  # Выводим сообщение об ошибке.
            return  # Завершаем работу, если файл не найден.

        processed_reviews = reload_from_disk(REVIEWS_FILE)["reviews"]  # Загружаем уже обработанные отзывы.

        # Загружаем прогресс парсинга из файла.
        if os.path.exists(PROGRESS_FILE):
            try:
                with open(PROGRESS_FILE, 'r', encoding='utf-8') as f:
                    progress = f.read().strip().split(',')
                    current_category_index = int(progress[0]) - 1  # Индекс текущей категории.
                    current_object_index = int(progress[1]) - 1  # Индекс текущего объекта.
            except (ValueError, IndexError) as e:
                print(f"{RED}Ошибка чтения {PROGRESS_FILE}: {e}.{RESET}")
                current_category_index = 0  # Начинаем с начала, если ошибка.
                current_object_index = 0
        else:
            current_category_index = 0  # Начинаем с первой категории.
            current_object_index = 0  # Начинаем с первого объекта.

        semaphore = asyncio.Semaphore(MAX_CONCURRENCY)  # Создаем семафор для ограничения количества одновременных запросов.
        file_queue = asyncio.Queue()  # Создаем очередь для асинхронной записи отзывов в файл.
        file_lock = asyncio.Lock()  # Создаем блокировку для синхронизации доступа к файлу.
        lock = asyncio.Lock()  # Создаем блокировку для синхронизации доступа к `processed_reviews`.

        file_saver_task = asyncio.create_task(save_to_file(file_queue, file_lock))  # Запускаем задачу записи в файл.

        #region Цикл по категориям и объектам
        for current_category_index in range(current_category_index, len(categories)):  # Перебираем категории.
            category_url = categories[current_category_index]  # URL текущей категории.
            category_name = category_url.split('/')[-1]  # Извлекаем имя категории из URL.
            print(f"{BLUE}Категория {current_category_index + 1}/{len(categories)}: {category_name}{RESET}")

            # Восстанавливаем индекс объекта для текущей категории (если нужно).
            if os.path.exists(PROGRESS_FILE):
                try:
                    with open(PROGRESS_FILE, 'r', encoding='utf-8') as f:
                        progress = f.read().strip().split(',')
                        # Если текущая категория совпадает с сохраненной, используем сохраненный индекс объекта.
                        if current_category_index == int(progress[0]) - 1:
                            current_object_index_loop = int(progress[1]) - 1
                        else:  # Иначе начинаем с начала.
                            current_object_index_loop = 0
                except (ValueError, IndexError) as e:
                    print(f"{RED}Ошибка чтения {PROGRESS_FILE}: {e}.{RESET}")
                    current_object_index_loop = 0  # Начинаем с начала при ошибке.
            else:
                current_object_index_loop = 0

            while True:  # Цикл по объектам в текущей категории.
                target_obj_url, page, obj_position = await get_target_object_url(session, category_url, current_object_index_loop)  # Получаем URL целевого объекта.
                if not target_obj_url:  # Если объектов больше нет.
                    print(f"{BLUE}Категория '{category_name}': Больше объектов нет.{RESET}")
                    break  # Переходим к следующей категории.

                object_name = target_obj_url.split('/')[-1]  # Извлекаем имя объекта из URL.
                print(f"{BLUE}Объект {current_object_index_loop + 1} (стр. {page}, поз. {obj_position + 1}): {object_name}{RESET}")

                review_page = 1  # Номер страницы отзывов.
                consecutive_errors = 0  # Счетчик последовательных ошибок.
                reviews_for_object = []  # Список для хранения отзывов по текущему объекту.

                while True:  # Цикл по страницам отзывов текущего объекта.
                    async with semaphore:  # Ограничиваем количество одновременных запросов.
                        review_urls = await get_reviews_urls(session, target_obj_url, review_page)  # Получаем URL отзывов с текущей страницы.
                        if not review_urls:  # Если отзывов больше нет (или ошибка).
                            break  # Переходим к следующему объекту.

                        tasks = []
                        for review_url in review_urls:  # Перебираем URL отзывов.
                            # Создаем задачу для обработки отзыва.  Передаем category_name и target_obj_url.
                            task = process_review(session, review_url, mat_words, lock, processed_reviews, file_queue, category_name, target_obj_url)
                            tasks.append(task)

                        results = await asyncio.gather(*tasks)  # Запускаем задачи асинхронно и ждем завершения.

                        for result in results:  # Перебираем результаты обработки отзывов.
                            if result:  # Если отзыв успешно обработан (process_review вернула не None).
                                reviews_for_object.append(result)  # Добавляем данные отзыва в список.

                        if not review_urls: # Если не получили отзывы со страницы
                            consecutive_errors += 1 # Увеличиваем счетчик ошибок
                        else: # Иначе
                            consecutive_errors = 0 # Сбрасываем счетчик

                        if consecutive_errors >= 10:  # Если было 10 последовательных ошибок.
                            print(f"{RED}Слишком много ошибок.{RESET}")
                            break  # Переходим к следующему объекту.
                        review_page += 1  # Переходим к следующей странице отзывов.

                save_reviews_to_db(reviews_for_object)  # Сохраняем все отзывы по текущему объекту в базу данных.
                current_object_index_loop += 1  # Переходим к следующему объекту в текущей категории.

                # Сохраняем прогресс (номер категории и объекта).
                with open(PROGRESS_FILE, 'w', encoding='utf-8') as f:
                    f.write(f"{current_category_index + 1},{current_object_index_loop}")


            current_object_index = 0  # Сбрасываем индекс объекта (для следующей категории).
        #endregion

        await file_queue.join()  # Дожидаемся завершения всех задач в очереди на запись в файл.
        file_saver_task.cancel()  # Останавливаем задачу записи в файл.

#endregion

if __name__ == "__main__":
    asyncio.run(main())  # Запускаем главную функцию.