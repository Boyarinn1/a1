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
TELEGRAM_BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN")
TELEGRAM_CHAT_ID = os.getenv("TELEGRAM_CHAT_ID")

# 🔹 Проверка переменных
if not all([S3_KEY_ID, S3_APPLICATION_KEY, S3_BUCKET_NAME, TELEGRAM_BOT_TOKEN, TELEGRAM_CHAT_ID]):
    raise RuntimeError("❌ Ошибка: Не установлены все необходимые переменные окружения!")

# 🔹 Telegram Bot
bot = Bot(token=TELEGRAM_BOT_TOKEN)

# 🔹 Авторизация в B2
info = b2sdk.v2.InMemoryAccountInfo()
b2_api = b2sdk.v2.B2Api(info)
b2_api.authorize_account("production", S3_KEY_ID, S3_APPLICATION_KEY)

# 🔹 Получаем bucket
bucket = b2_api.get_bucket_by_name(S3_BUCKET_NAME)

# 🔹 Поиск файлов в 444/
files_to_download = []
for file_version, _ in bucket.ls("444/", recursive=True):
    if file_version.file_name.endswith(".json"):
        files_to_download.append(file_version.file_name)

if not files_to_download:
    print("⚠️ Нет файлов для загрузки. Ожидание новых данных...")
    exit(0)

# 🔹 Загружаем и обрабатываем файлы
for file_name in files_to_download:
    local_path = os.path.join(DOWNLOAD_DIR, os.path.basename(file_name))
    try:
        bucket.download_file_by_name(file_name).save_to(local_path)

        # Обрабатываем JSON-файлы
        with open(local_path, "r", encoding="utf-8") as f:
            data = json.load(f)

        if "topik" in data:
            message = f"**Топик:** {data['topik']}\n\n{data.get('content', 'Контент отсутствует')}"
            try:
                bot.send_message(chat_id=TELEGRAM_CHAT_ID, text=message, parse_mode="Markdown")
                print(f"✅ Топик опубликован: {file_name}")
            except TelegramError as e:
                print(f"🚨 Ошибка отправки сообщения в Telegram: {e}")

        os.remove(local_path)  # Удаляем файл после обработки
    except Exception as e:
        print(f"🚨 Ошибка при обработке файла {file_name}: {e}")

print("🚀 Скрипт завершён.")
