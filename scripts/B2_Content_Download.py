import os
import json
import b2sdk.v2
from telegram import Bot
from telegram.error import TelegramError

# 🔹 Определяем пути
BASE_DIR = os.path.dirname(os.path.dirname(__file__))  # a1/
DOWNLOAD_DIR = os.path.join(BASE_DIR, "data", "downloaded")  # a1/data/downloaded
CONFIG_PATH = os.path.join(BASE_DIR, "config", "config_public.json")  # a1/config/config_public.json

# 🔹 Загружаем переменные окружения
S3_KEY_ID = os.getenv("S3_KEY_ID")
S3_APPLICATION_KEY = os.getenv("S3_APPLICATION_KEY")
S3_BUCKET_NAME = os.getenv("S3_BUCKET_NAME")
S3_ENDPOINT = os.getenv("S3_ENDPOINT", "production")
TELEGRAM_TOKEN = os.getenv("TELEGRAM_TOKEN")
TELEGRAM_CHAT_ID = os.getenv("TELEGRAM_CHAT_ID")

# 🔹 Проверка переменных окружения
if not all([S3_KEY_ID, S3_APPLICATION_KEY, S3_BUCKET_NAME, TELEGRAM_TOKEN, TELEGRAM_CHAT_ID]):
    raise RuntimeError("❌ Ошибка: Не установлены все необходимые переменные окружения!")

# 🔹 Telegram Bot
bot = Bot(token=TELEGRAM_TOKEN)

# 🔹 Авторизация в B2
info = b2sdk.v2.InMemoryAccountInfo()
b2_api = b2sdk.v2.B2Api(info)
b2_api.authorize_account(S3_ENDPOINT, S3_KEY_ID, S3_APPLICATION_KEY)

# 🔹 Получаем bucket
bucket = b2_api.get_bucket_by_name(S3_BUCKET_NAME)

# 🔹 Поиск JSON-файлов в папке 444/
files_to_download = []
print("📥 Поиск файлов в B2 (папка 444/)...")
for file_version, _ in bucket.ls("444/", recursive=True):
    if file_version.file_name.endswith(".json"):  # Ищем только JSON-файлы
        files_to_download.append(file_version.file_name)

if not files_to_download:
    print("⚠️ Нет файлов для загрузки. Ожидание новых данных...")
    with open(CONFIG_PATH, "w", encoding="utf-8") as f:
        json.dump({"status": "waiting", "files": []}, f, indent=4)
    exit(0)

# 🔹 Создаём директорию для загрузки, если её нет
os.makedirs(DOWNLOAD_DIR, exist_ok=True)

# 🔹 Загружаем и обрабатываем файлы
for file_name in files_to_download:
    local_path = os.path.join(DOWNLOAD_DIR, os.path.basename(file_name))
    try:
        # Скачиваем файл
        print(f"📥 Скачивание {file_name} в {local_path}...")
        bucket.download_file_by_name(file_name).save_to(local_path)

        # Обрабатываем JSON-файлы
        with open(local_path, "r", encoding="utf-8") as f:
            data = json.load(f)

        # 🔹 Проверяем наличие ключа 'topik' или 'topic'
        topic_text = None
        if "topik" in data:
            topic_text = data["topik"]
        elif "topic" in data and isinstance(data["topic"], dict) and "topic" in data["topic"]:
            topic_text = data["topic"]["topic"]

        # 🔹 Отправка сообщения в Telegram
        if topic_text:
            message = f"**Топик:** {topic_text}\n\n{data.get('content', 'Контент отсутствует')}"
        else:
            message = f"📜 JSON-файл без ключа 'topik' или 'topic':\n```\n{json.dumps(data, indent=4, ensure_ascii=False)}\n```"

        try:
            bot.send_message(chat_id=TELEGRAM_CHAT_ID, text=message, parse_mode="Markdown")
            print(f"✅ Сообщение отправлено: {file_name}")
        except TelegramError as e:
            print(f"🚨 Ошибка отправки в Telegram: {e}")

        os.remove(local_path)  # Удаляем файл после обработки
    except Exception as e:
        print(f"🚨 Ошибка при обработке файла {file_name}: {e}")

# 🔹 Завершение работы
print("🚀 Скрипт завершён.")
