import os
import json
import b2sdk.v2
import asyncio
import shutil
from telegram import Bot

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

if not all([S3_KEY_ID, S3_APPLICATION_KEY, S3_BUCKET_NAME, TELEGRAM_TOKEN, TELEGRAM_CHAT_ID]):
    raise RuntimeError("❌ Ошибка: Не установлены все необходимые переменные окружения!")

bot = Bot(token=TELEGRAM_TOKEN)

info = b2sdk.v2.InMemoryAccountInfo()
b2_api = b2sdk.v2.B2Api(info)
b2_api.authorize_account(S3_ENDPOINT, S3_KEY_ID, S3_APPLICATION_KEY)

bucket = b2_api.get_bucket_by_name(S3_BUCKET_NAME)


async def process_files():
    print("🗑 Полная очистка локальной папки перед скачиванием...")
    shutil.rmtree(DOWNLOAD_DIR, ignore_errors=True)
    os.makedirs(DOWNLOAD_DIR, exist_ok=True)

    print("\n📥 Запрашиваем список файлов в B2 (папка 666/)...")
    files_to_download = [file_version.file_name for file_version, _ in bucket.ls("666/", recursive=True)]

    if not files_to_download:
        print("⚠️ Нет новых файлов для загрузки.")
        return

    for file_name in files_to_download:
        local_path = os.path.join(DOWNLOAD_DIR, os.path.basename(file_name))

        if not file_name.endswith(".json"):
            print(f"⏭ Пропускаем файл {file_name} (не JSON)")
            continue

        try:
            print(f"📥 Скачивание {file_name} в {local_path}...")
            bucket.download_file_by_name(file_name).save_to(local_path)

            with open(local_path, "r", encoding="utf-8") as f:
                print(f"📂 Открыт JSON-файл: {local_path}")
                data = json.load(f)
                print("📊 Полное содержимое JSON:", json.dumps(data, indent=2, ensure_ascii=False))

            topic_clean = data.get("topic", {}).get("topic", "").strip("'\"")
            print("📝 Извлечённый заголовок:", topic_clean)
            text_content = data.get("text_initial", {}).get("content", "").strip()
            print("📜 Извлечённый текст:", text_content[:100], "...")
            if not text_content:
                print(f"⚠️ Пропуск пустого контента в {file_name}")
                continue

            # 📜 Отправка заголовка и основного текста
            formatted_text = f"🏛 <b>{topic_clean.strip()}</b>\n\n{text_content.strip()}"
            await bot.send_message(chat_id=TELEGRAM_CHAT_ID, text=formatted_text, parse_mode="HTML")
            await asyncio.sleep(1)

            # 🎭 Отправка саркастического комментария курсивом
            sarcasm_comment = data.get("sarcasm", {}).get("comment", "").strip()
            if sarcasm_comment:
                sarcasm_text = f"📜 <i>{sarcasm_comment}</i>"
                await bot.send_message(chat_id=TELEGRAM_CHAT_ID, text=sarcasm_text, parse_mode="HTML")
                await asyncio.sleep(1)

            # 📊 Отправка интерактивного опроса
            if "sarcasm" in data and "poll" in data["sarcasm"]:
                poll_data = data["sarcasm"].get("poll", {})

                # ✅ Если poll_data строка, превращаем в объект
                if isinstance(poll_data, str):
                    try:
                        poll_data = json.loads(poll_data)
                    except json.JSONDecodeError:
                        print("🚨 Ошибка: Опрос в некорректном формате!")
                        poll_data = {}

                question = poll_data.get("question", "").strip()
                options = poll_data.get("options", [])

                if question and isinstance(options, list) and len(options) >= 2:
                    print(f"📤 Отправка опроса: {question}")
                    await bot.send_poll(chat_id=TELEGRAM_CHAT_ID, question=question, options=options,
                                        is_anonymous=False)
                    await asyncio.sleep(1)

                if isinstance(poll_data, str):  # ✅ Проверяем, если poll передан как строка JSON
                    try:
                        poll_data = json.loads(poll_data)  # 🔄 Преобразуем строку JSON в объект
                    except json.JSONDecodeError:
                        print("🚨 Ошибка: Опрос в некорректном формате!")
                        poll_data = {}

                question = poll_data.get("question", "").strip()
                options = poll_data.get("options", [])

                if question and isinstance(options, list) and len(options) >= 2:
                    print(f"📤 Отправка опроса: {question}")
                    await bot.send_poll(chat_id=TELEGRAM_CHAT_ID, question=question, options=options,
                                        is_anonymous=False)
                    await asyncio.sleep(1)

            processed_dir = os.path.join(BASE_DIR, "data", "processed")
            os.makedirs(processed_dir, exist_ok=True)
            shutil.move(local_path, os.path.join(processed_dir, os.path.basename(local_path)))
            print(f"🗑 Файл {file_name} перемещён в архив processed.")

        except Exception as e:
            print(f"🚨 Ошибка при обработке файла {file_name}: {e}")

    print("🚀 Скрипт завершён.")


if __name__ == "__main__":
    asyncio.run(process_files())
