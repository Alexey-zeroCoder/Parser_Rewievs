import requests
from bs4 import BeautifulSoup

# region Настройки запроса
# URL страницы, с которой будем получать данные о погоде
url = "https://www.gismeteo.ru/weather-zheleznogorsk-11995/"

# Заголовки, имитирующие запрос от браузера.  Это помогает избежать блокировки со стороны сайта.
headers = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36"
}
# endregion


try:
    # region Отправка запроса и получение HTML

    # Отправляем GET-запрос на указанный URL с заданными заголовками
    response = requests.get(url, headers=headers)

    # Проверяем код ответа HTTP.  Если код 200 (OK), продолжаем.
    response.raise_for_status()
    # endregion

    # region Парсинг HTML и извлечение данных

    # Создаем объект BeautifulSoup для парсинга HTML-кода страницы.
    soup = BeautifulSoup(response.text, 'html.parser')

    # Ищем родительский div-элемент с классом 'weather-value'.
    weather_value_div = soup.find('div', class_='weather-value')

    # Проверяем, найден ли элемент 'weather-value'
    if weather_value_div:
        temperature_element = weather_value_div.find('temperature-value')

        # Проверяем, найден ли элемент 'temperature-value'
        if temperature_element:
            # Если тег 'temperature-value' найден, извлекаем значение из его атрибута 'value'.
            temperature = temperature_element.get('value')

            if temperature:
                # Если значение получено, выводим его на экран.
                print(f"Текущая температура: {temperature}°C")
            else:
                # Если атрибут 'value' пуст или отсутствует, выводим сообщение об ошибке.
                print("Атрибут 'value' не найден.")
        else:
            # Если тег 'temperature-value' не найден внутри 'weather-value', выводим сообщение.
            print("Элемент 'temperature-value' не найден внутри 'weather-value'.")
    else:
        # Если элемент 'weather-value' не найден, выводим сообщение.
        print("Элемент 'weather-value' не найден.")
    # endregion

# region Обработка исключений
except requests.exceptions.RequestException as e:
    # Обрабатываем ошибки, связанные с сетевыми запросами (например, нет подключения к интернету, сайт недоступен, таймаут и т.д.).
    print(f"Сетевая ошибка: {e}")

except AttributeError as e:
    # Обрабатываем ошибки, возникающие при парсинге HTML, если какой-то элемент не найден. Например, если изменилась структура страницы и нужный тег отсутствует.
    print(f"Ошибка парсинга: {e}")

except Exception as e:
    # Это общий блок обработки ошибок, который ловит любые непредвиденные ошибки.
    print(f"Неизвестная ошибка: {e}")
# endregion