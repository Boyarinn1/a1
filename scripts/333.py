import os
import json
import requests

# Получаем токен бота и ID канала из переменных окружения
TELEGRAM_TOKEN = os.getenv("TELEGRAM_TOKEN")
TELEGRAM_CHAT_ID = os.getenv("TELEGRAM_CHAT_ID")

# Указываем правильный путь к JSON-файлу на Windows
JSON_FILE_PATH = r"C:\Users\boyar\a1\data\downloaded\20250116-1813.json"


def load_json_data(file_path):
    """Загружает данные из JSON-файла."""
    try:
        with open(file_path, "r", encoding="utf-8") as file:
            return json.load(file)
    except Exception as e:
        print(f"❌ Ошибка загрузки JSON: {e}")
        return None


def format_message(post_data):
    """Формирует текст сообщения для Telegram в HTML-формате."""
    topic = post_data.get("topic", {}).get("topic", "🚀 Без темы").strip()
    if topic.startswith('"') and topic.endswith('"'):
        topic = topic[1:-1]  # Убираем кавычки

    content = post_data.get("text_initial", {}).get("content", "").strip()
    sarcasm = post_data.get("sarcasm", {}).get("comment", "").strip()

    if sarcasm.startswith('"') and sarcasm.endswith('"'):
        sarcasm = sarcasm[1:-1]  # Убираем кавычки

    if not content:
        content = "ℹ️ Контент отсутствует."

    return f"""🏛 <b>{topic}</b>\n\n{content}\n\n〰〰〰〰〰〰〰〰〰〰〰〰\n\n🎭 <i>{sarcasm}</i>"""


def send_message(bot_token, chat_id, message):
    """Отправляет сообщение в Telegram-канал."""
    payload = {"chat_id": chat_id, "text": message, "parse_mode": "HTML"}
    response = requests.post(f"https://api.telegram.org/bot{bot_token}/sendMessage", json=payload)
    print(f"📩 Ответ Telegram API (сообщение): {response.status_code} {response.json()}")
    return response


def main():
    """Основная функция публикации в Telegram."""
    if not TELEGRAM_TOKEN or not TELEGRAM_CHAT_ID:
        print("❌ Ошибка: переменные окружения TELEGRAM_TOKEN или TELEGRAM_CHAT_ID не установлены!")
        return

    post_data = load_json_data(JSON_FILE_PATH)
    if not post_data:
        return

    # Отправка текста поста
    message = format_message(post_data)
    send_message(TELEGRAM_TOKEN, TELEGRAM_CHAT_ID, message)


if __name__ == "__main__":
    main()
