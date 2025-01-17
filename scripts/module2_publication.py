import os
import json
import requests
import re

# Определяем пути
BASE_DIR = os.path.dirname(os.path.abspath(__file__))  # scripts/
BASE_DIR = os.path.dirname(BASE_DIR)  # a1/
DOWNLOAD_DIR = os.path.join(BASE_DIR, "data", "downloaded")


def find_json_mp4_pairs():
    """Ищет пары файлов с одинаковыми именами, но разными расширениями (.json и .mp4)"""
    files = os.listdir(DOWNLOAD_DIR)
    json_files = {f[:-5] for f in files if f.endswith(".json")}
    mp4_files = {f[:-4] for f in files if f.endswith(".mp4")}

    pairs = json_files & mp4_files  # Пересечение имен без расширений
    return list(pairs)


def load_json_data(filename):
    """Загружает данные из JSON-файла"""
    json_path = os.path.join(DOWNLOAD_DIR, f"{filename}.json")
    try:
        with open(json_path, "r", encoding="utf-8") as file:
            return json.load(file)
    except Exception as e:
        print(f"❌ Ошибка загрузки JSON {json_path}: {e}")
        return None


def format_message(post_data):
    topic = post_data.get("topic", {}).get("topic", "🚀 Без темы")  # Заголовок
    content = post_data.get("text_initial", {}).get("content", "").strip()  # Основной текст
    sarcasm = post_data.get("sarcasm", {}).get("comment", "").strip()  # Сарказм

    if not content:
        content = "ℹ️ Контент в файле отсутствует или поврежден."

    return f"""🏛 **{topic}**

{content}

〰〰〰〰〰〰〰〰〰〰〰〰

🎭 *{sarcasm.capitalize()}*"""


def extract_poll(post_data):
    raw_poll = post_data.get("sarcasm", {}).get("poll", "").strip()
    if not raw_poll:
        print("❌ Опрос отсутствует в JSON!")
        return None
    try:
        raw_poll = raw_poll.replace("'", '"')
        poll_json = json.loads(f"{{{raw_poll}}}")
        question = poll_json.get("question", "")
        options = poll_json.get("options", [])
        if not question or len(options) < 2 or len(options) > 10:
            print(f"❌ Ошибка: Опрос некорректен! (question={question}, options={options})")
            return None
        poll_data = {"question": question, "options": options}
        print(f"✅ Опрос успешно обработан! Вопрос: {question}")
        return poll_data
    except Exception as e:
        print(f"❌ Ошибка парсинга poll: {e}\nИсходный poll: {raw_poll}")
        return None


def send_poll(bot_token, chat_id, poll_question, poll_options):
    payload = {
        "chat_id": chat_id,
        "question": poll_question,
        "options": poll_options,
        "is_anonymous": True,
        "type": "regular"
    }
    response = requests.post(f"https://api.telegram.org/bot{bot_token}/sendPoll", json=payload)
    print(f"📩 Ответ Telegram API (опрос): {response.status_code} {response.json()}")
    return response


def send_message(bot_token, chat_id, message):
    payload = {"chat_id": chat_id, "text": message, "parse_mode": "Markdown"}
    response = requests.post(f"https://api.telegram.org/bot{bot_token}/sendMessage", json=payload)
    print(f"📩 Ответ Telegram API: {response.status_code} {response.json()}")
    return response


def main():
    bot_token = os.getenv("TELEGRAM_TOKEN")
    chat_id = os.getenv("TELEGRAM_CHAT_ID")

    pairs = find_json_mp4_pairs()
    if not pairs:
        print("⚠️ Нет пар файлов .json + .mp4. Завершаем работу.")
        return

    for filename in pairs:
        post_data = load_json_data(filename)
        if not post_data:
            continue
        message = format_message(post_data)
        send_message(bot_token, chat_id, message)
        poll_data = extract_poll(post_data)
        if poll_data:
            send_poll(bot_token, chat_id, poll_data["question"], poll_data["options"])


if __name__ == "__main__":
    main()
