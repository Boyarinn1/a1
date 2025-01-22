import os
import json
import b2sdk.v2
import asyncio
import shutil
from telegram import Bot
from telegram.error import TelegramError
from telegram.helpers import escape_markdown

# 🔹 Определяем пути
BASE_DIR = os.path.dirname(os.path.dirname(__file__))
DOWNLOAD_DIR = os.path.join(BASE_DIR, "data", "downloaded")
CONFIG_PATH = os.path.join(BASE_DIR, "config", "config_public.json")

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

async def process_files():
    """Функция обработки и отправки JSON-файлов в Telegram"""

    # 🔍 Очистка папки загрузки перед скачиванием
    print("🗑 Полная очистка локальной папки перед скачиванием...")
    shutil.rmtree(DOWNLOAD_DIR, ignore_errors=True)
    os.makedirs(DOWNLOAD_DIR, exist_ok=True)

    # 🔍 Загружаем файлы из B2 (папка 666/)
    print("\n📥 Запрашиваем список файлов в B2 (папка 666/)...")
    files_to_download = [file_version.file_name for file_version, _ in bucket.ls("666/", recursive=True)]

    print(f"📌 Найдено файлов в B2: {len(files_to_download)}")
    if not files_to_download:
        print("⚠️ Нет новых файлов для загрузки.")
        return

    for file_name in files_to_download:
        local_path = os.path.join(DOWNLOAD_DIR, os.path.basename(file_name))

        # Пропускаем не JSON-файлы
        if not file_name.endswith(".json"):
            print(f"⏭ Пропускаем файл {file_name} (не JSON)")
            continue

        try:
            print(f"📥 Скачивание {file_name} в {local_path}...")
            bucket.download_file_by_name(file_name).save_to(local_path)

            # Читаем JSON
            with open(local_path, "r", encoding="utf-8") as f:
                data = json.load(f)

            # 🔹 Формируем первый пост (основной текст)
            topic_clean = data.get("topic", {}).get("topic", "").strip('"')
            text_content = data.get("text_initial", {}).get("content", "")
            text_content = text_content.replace("Сгенерированный текст на тему:", "").strip()
            formatted_text = escape_markdown(f"🏛 {topic_clean}\n\n{text_content}", version=2)

            # Отправляем первый пост
            await bot.send_message(chat_id=TELEGRAM_CHAT_ID, text=formatted_text, parse_mode="MarkdownV2")

            # 🔹 Формируем и отправляем интерактивный опрос
            if "sarcasm" in data and "poll" in data["sarcasm"]:
                question = data['sarcasm']['poll']['question']
                options_clean = [opt.strip('"') for opt in data['sarcasm']['poll']['options']]

                # Проверка прав бота на отправку опросов
                try:
                    await bot.send_poll(chat_id=TELEGRAM_CHAT_ID, question=question, options=options_clean, is_anonymous=False)
                except TelegramError as e:
                    print(f"🚨 Ошибка отправки опроса: {e}")

            print(f"✅ Сообщение отправлено: {file_name}")

            # 🔹 Перемещение файла в архив
            processed_dir = os.path.join(BASE_DIR, "data", "processed")
            os.makedirs(processed_dir, exist_ok=True)
            shutil.move(local_path, os.path.join(processed_dir, os.path.basename(local_path)))

            print(f"🗑 Файл {file_name} перемещён в архив processed.")

        except Exception as e:
            print(f"🚨 Ошибка при обработке файла {file_name}: {e}")

    print("🚀 Скрипт завершён.")

# Запуск асинхронного кода
if __name__ == "__main__":
    asyncio.run(process_files())
