import os
import json
import b2sdk.v2
import asyncio
import shutil
from telegram import Bot

# 🔹 Определяем пути
BASE_DIR = os.path.dirname(os.path.dirname(__file__))
DOWNLOAD_DIR = os.path.join(BASE_DIR, "data", "downloaded")

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


def get_publish_status():
    """Скачивает config_public.json из B2 и возвращает список опубликованных папок."""
    try:
        local_config_path = os.path.join(DOWNLOAD_DIR, "config_public.json")
        bucket.download_file_by_name("config/config_public.json").save_to(local_config_path)

        with open(local_config_path, "r", encoding="utf-8") as f:
            config_data = json.load(f)

        return set(config_data.get("publish", "").split(", "))  # Возвращаем список опубликованных папок
    except Exception as e:
        print(f"🚨 Ошибка при загрузке config_public.json: {e}")
        return set()


def update_publish_status(new_status):
    """Добавляет новую папку в 'publish' в config_public.json, не удаляя старые данные."""
    try:
        local_config_path = os.path.join(DOWNLOAD_DIR, "config_public.json")

        # 📥 Загружаем существующий config_public.json
        if os.path.exists(local_config_path):
            with open(local_config_path, "r", encoding="utf-8") as f:
                config_data = json.load(f)
        else:
            config_data = {}

        # 🏷 Добавляем новую папку в publish
        existing_status = set(config_data.get("publish", "").split(", ")) if "publish" in config_data else set()
        existing_status.add(new_status)
        config_data["publish"] = ", ".join(existing_status)  # Сохраняем список

        # 📤 Загружаем обратно в B2
        with open(local_config_path, "w", encoding="utf-8") as f:
            json.dump(config_data, f, ensure_ascii=False, indent=4)

        bucket.upload_local_file(local_config_path, "config/config_public.json")
        print(f"✅ Обновлён config_public.json: {config_data['publish']}")
    except Exception as e:
        print(f"🚨 Ошибка при обновлении config_public.json: {e}")


async def process_files():
    print("🗑 Полная очистка локальной папки перед скачиванием...")
    shutil.rmtree(DOWNLOAD_DIR, ignore_errors=True)
    os.makedirs(DOWNLOAD_DIR, exist_ok=True)

    print("\n📥 Проверяем статус публикации в config_public.json...")
    published_folders = get_publish_status()

    # Определяем, из какой папки публиковать
    if "444/" not in published_folders:
        publish_folder = "444/"
    elif "555/" not in published_folders:
        publish_folder = "555/"
    elif "666/" not in published_folders:
        publish_folder = "666/"
    else:
        print("🚀 Все папки опубликованы, останавливаем работу.")
        return

    print(f"📥 Запрашиваем список файлов в B2 (папка {publish_folder})...")
    files_to_download = [file_version.file_name for file_version, _ in bucket.ls(publish_folder, recursive=True)]

    if not files_to_download:
        print(f"⚠️ Нет новых файлов для загрузки из {publish_folder}")
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
                data = json.load(f)

            topic_clean = data.get("topic", {}).get("topic", "").strip("'\"")
            text_content = data.get("text_initial", {}).get("content", "").strip()

            # 🛑 Убираем лишние заголовки
            clean_text = text_content.replace(f'Сгенерированный текст на тему: "{topic_clean}"', '').strip()
            clean_text = clean_text.replace("Интересный факт:", "").strip()

            # 🛑 Убираем сарказм и опрос из первого сообщения
            if "🔶 Саркастический комментарий:" in clean_text:
                clean_text = clean_text.split("🔶 Саркастический комментарий:")[0].strip()

            if "🔸 Саркастический вопрос:" in clean_text:
                clean_text = clean_text.split("🔸 Саркастический вопрос:")[0].strip()

            formatted_text = f"🏛 <b>{topic_clean.strip()}</b>\n\n{clean_text}"
            await bot.send_message(chat_id=TELEGRAM_CHAT_ID, text=formatted_text, parse_mode="HTML")
            await asyncio.sleep(1)

            sarcasm_comment = data.get("sarcasm", {}).get("comment", "").strip()
            if sarcasm_comment:
                sarcasm_text = f"📜 <i>{sarcasm_comment}</i>"
                await bot.send_message(chat_id=TELEGRAM_CHAT_ID, text=sarcasm_text, parse_mode="HTML")
                await asyncio.sleep(1)

            if "sarcasm" in data and "poll" in data["sarcasm"]:
                poll_data = data["sarcasm"]["poll"]
                question = poll_data.get("question", "").strip()
                options = poll_data.get("options", [])

                if question and options and len(options) >= 2:
                    await bot.send_poll(chat_id=TELEGRAM_CHAT_ID, question=f"🎭 {question}", options=options,
                                        is_anonymous=True)
                    await asyncio.sleep(1)

            update_publish_status(publish_folder)

        except Exception as e:
            print(f"🚨 Ошибка при обработке файла {file_name}: {e}")

    print("🚀 Скрипт завершён.")


if __name__ == "__main__":
    asyncio.run(process_files())
