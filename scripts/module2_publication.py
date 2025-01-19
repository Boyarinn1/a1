import os
import json
import requests

# Определяем пути
BASE_DIR = os.path.dirname(os.path.dirname(__file__))  # Делаем так же, как в module1_preparation.py


DOWNLOAD_DIR = os.path.join(BASE_DIR, "data", "downloaded")  # Прямой путь без лишнего перехода вверх


print(f"📂 DOWNLOAD_DIR: {DOWNLOAD_DIR}")
print(f"📂 Содержимое папки: {os.listdir(DOWNLOAD_DIR) if os.path.exists(DOWNLOAD_DIR) else 'Папка не найдена'}")
print(f"📍 Текущая директория: {os.getcwd()}")


# Получаем токен и ID чата
TELEGRAM_TOKEN = os.getenv("TELEGRAM_TOKEN")
TELEGRAM_CHAT_ID = os.getenv("TELEGRAM_CHAT_ID")


def find_json_mp4_pairs():
    """Ищет пары файлов с одинаковыми именами, но разными расширениями (.json и .mp4)"""
    files = os.listdir(DOWNLOAD_DIR)
    json_files = {f[:-5] for f in files if f.endswith(".json")}
    mp4_files = {f[:-4] for f in files if f.endswith(".mp4")}
    return list(json_files & mp4_files)


def load_json_data(filename):
    """Загружает данные из JSON-файла."""
    json_path = os.path.join(DOWNLOAD_DIR, f"{filename}.json")
    try:
        with open(json_path, "r", encoding="utf-8") as file:
            return json.load(file)
    except Exception as e:
        print(f"❌ Ошибка загрузки JSON {json_path}: {e}")
        return None


def format_message(post_data):
    """Формирует текст сообщения для Telegram в HTML-формате."""
    topic = post_data.get("topic", {}).get("topic", "🚀 Без темы").strip()

    content = post_data.get("text_initial", {}).get("content", "").strip()
    sarcasm = post_data.get("sarcasm", {}).get("comment", "").strip()

    if topic.startswith('"') and topic.endswith('"'):
        topic = topic[1:-1]
    if sarcasm.startswith('"') and sarcasm.endswith('"'):
        sarcasm = sarcasm[1:-1]
    if not content:
        content = "ℹ️ Контент отсутствует."

    return f"""🏛 <b>{topic}</b>\n\n{content}\n\n〰〰〰〰〰〰〰〰〰〰〰〰\n\n🎭 <i>{sarcasm}</i>"""


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
        return {"question": question, "options": options}
    except Exception as e:
        print(f"❌ Ошибка парсинга poll: {e}\nИсходный poll: {raw_poll}")
        return None


def send_message(bot_token, chat_id, message):
    """Отправляет сообщение в Telegram-канал."""
    payload = {"chat_id": chat_id, "text": message, "parse_mode": "HTML"}
    response = requests.post(f"https://api.telegram.org/bot{bot_token}/sendMessage", json=payload)
    response_json = response.json()

    if response_json.get("ok") and response_json["result"].get("sender_chat", {}).get("title") == "1":
        print("⚠️ Telegram присвоил название канала '1', но это не часть сообщения.")

    print(f"📩 Ответ Telegram API (сообщение): {response.status_code} {response_json}")
    return response


def send_poll(bot_token, chat_id, poll_question, poll_options):
    """Отправляет опрос в Telegram."""
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


def main():

    # 🔍 Отладочный вывод путей
    print(f"📂 BASE_DIR в module2: {BASE_DIR}")
    print(f"📂 DOWNLOAD_DIR в module2: {DOWNLOAD_DIR}")
    print(f"📂 Содержимое папки в module2: {os.listdir(DOWNLOAD_DIR) if os.path.exists(DOWNLOAD_DIR) else '❌ Папки нет'}")

    """Основная функция обработки и отправки сообщений."""
    if not TELEGRAM_TOKEN or not TELEGRAM_CHAT_ID:
        print("❌ Ошибка: переменные окружения TELEGRAM_TOKEN или TELEGRAM_CHAT_ID не установлены!")
        return

    pairs = find_json_mp4_pairs()
    if not pairs:
        print("⚠️ Нет пар файлов .json + .mp4. Завершаем работу.")
        return

    for filename in pairs:
        post_data = load_json_data(filename)
        if not post_data:
            continue

        message = format_message(post_data)
        send_message(TELEGRAM_TOKEN, TELEGRAM_CHAT_ID, message)

        poll_data = extract_poll(post_data)
        if poll_data:
            send_poll(TELEGRAM_TOKEN, TELEGRAM_CHAT_ID, poll_data["question"], poll_data["options"])


if __name__ == "__main__":
    main()
