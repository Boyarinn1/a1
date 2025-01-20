import os
import json
import requests
import subprocess

# Используем правильный путь
BASE_DIR = os.path.dirname(os.path.dirname(__file__))
DOWNLOAD_DIR = os.path.join(BASE_DIR, "data", "downloaded")

print(f"📂 DOWNLOAD_DIR перед публикацией: {os.listdir(DOWNLOAD_DIR) if os.path.exists(DOWNLOAD_DIR) else '❌ Папка не найдена'}")

TELEGRAM_TOKEN = os.getenv("TELEGRAM_TOKEN")
TELEGRAM_CHAT_ID = os.getenv("TELEGRAM_CHAT_ID")


def find_json_mp4_pairs():
    """Ищет единственную пару файлов (JSON + MP4) для публикации."""
    files = os.listdir(DOWNLOAD_DIR)
    json_files = {f[:-5] for f in files if f.endswith(".json")}
    mp4_files = {f[:-4] for f in files if f.endswith(".mp4")}
    pairs = list(json_files & mp4_files)

    if not pairs:
        print("⚠️ Нет непубликованных файлов! Ожидание новых загрузок.")
        return None

    return pairs[0]  # Берём первую доступную пару

def restore_files_from_artifacts():
    """Скачивает артефакты перед публикацией."""
    print("📥 Восстановление файлов из артефактов перед публикацией...")
    subprocess.run(["gh", "run", "download", "--name", "downloaded_files", "--dir", DOWNLOAD_DIR], check=False)
    print(f"✅ Файлы восстановлены в {DOWNLOAD_DIR}")


def load_json_data(filename):
    """Загружает JSON-данные."""
    json_path = os.path.join(DOWNLOAD_DIR, f"{filename}.json")
    try:
        with open(json_path, "r", encoding="utf-8") as file:
            return json.load(file)
    except Exception as e:
        print(f"❌ Ошибка загрузки JSON {json_path}: {e}")
        return None


def send_message(bot_token, chat_id, message):
    """Отправляет сообщение в Telegram."""
    payload = {"chat_id": chat_id, "text": message, "parse_mode": "HTML"}
    response = requests.post(f"https://api.telegram.org/bot{bot_token}/sendMessage", json=payload)
    print(f"📩 Ответ Telegram API: {response.status_code} {response.json()}")


def main():
    
    restore_files_from_artifacts()
    pair = find_json_mp4_pairs()
    if not pair:
        return

    pair = find_json_mp4_pairs()
    if not pair:
        return

    post_data = load_json_data(pair)
    if not post_data:
        return

    message = f"🏛 {post_data.get('topic', {}).get('topic', 'Без темы')}\n\n{post_data.get('text_initial', {}).get('content', 'ℹ️ Контент отсутствует.')}"
    send_message(TELEGRAM_TOKEN, TELEGRAM_CHAT_ID, message)

    print(f"✅ Публикация {pair}.json завершена.")

    # ✅ После публикации запускаем module1_preparation.py
    print("🚀 Запуск module1_preparation.py для загрузки новой группы файлов...")
    subprocess.run(["python", "scripts/module1_preparation.py"], check=True)


if __name__ == "__main__":
    main()
