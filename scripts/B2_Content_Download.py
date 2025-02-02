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


def get_published_generation_ids():
    """Загружает config_public.json из B2 и возвращает множество опубликованных generation_id."""
    try:
        local_config_path = os.path.join(DOWNLOAD_DIR, "config_public.json")
        bucket.download_file_by_name("config/config_public.json").save_to(local_config_path)

        with open(local_config_path, "r", encoding="utf-8") as f:
            config_data = json.load(f)

        return set(config_data.get("generation_id", []))
    except Exception as e:
        print(f"🚨 Ошибка при загрузке config_public.json: {e}")
        return set()


def update_generation_id_status(file_name: str) -> None:
    """Добавляет новый generation_id в config_public.json, если его там ещё нет."""
    try:
        local_config_path = os.path.join(DOWNLOAD_DIR, "config_public.json")

        # 📥 Загружаем config_public.json
        if os.path.exists(local_config_path):
            with open(local_config_path, "r", encoding="utf-8") as f:
                config_data = json.load(f)
        else:
            config_data = {}

        print(f"📂 Полный путь файла: {file_name}")
        file_name_only = os.path.basename(file_name)  # например, 20250201-1131.json
        print(f"📄 Имя файла: {file_name_only}")

        # Убираем .json (если есть) и разбиваем по '-'
        base_name = file_name_only.rsplit(".", 1)[0]  # 20250201-1131
        parts = base_name.split("-")
        print(f"🔍 Разделение имени файла по '-': {parts}")

        if len(parts) < 2:
            print(f"🚨 Ошибка: файл не содержит 'YYYYMMDD-HHMM'!")
            return

        generation_id = "-".join(parts[:2])  # 20250201-1131
        print(f"📌 Итоговый generation_id: {generation_id}")

        existing_ids = config_data.get("generation_id", [])
        if not isinstance(existing_ids, list):
            existing_ids = [existing_ids]

        if generation_id in existing_ids:
            print(f"⚠️ generation_id {generation_id} уже записан, пропускаем обновление.")
            return

        existing_ids.append(generation_id)
        config_data["generation_id"] = existing_ids

        # 📤 Сохраняем локально
        with open(local_config_path, "w", encoding="utf-8") as f:
            json.dump(config_data, f, ensure_ascii=False, indent=4)

        # 📤 Загружаем обратно в B2
        bucket.upload_local_file(local_config_path, "config/config_public.json")
        print(f"✅ Обновлён config_public.json: {config_data['generation_id']}")

    except Exception as e:
        print(f"🚨 Ошибка при обновлении config_public.json: {e}")


async def process_files():
    print("🗑 Полная очистка локальной папки перед скачиванием...")
    shutil.rmtree(DOWNLOAD_DIR, ignore_errors=True)
    os.makedirs(DOWNLOAD_DIR, exist_ok=True)

    print("\n📥 Проверяем статус публикации в config_public.json...")
    published_generation_ids = get_published_generation_ids()

    # Собираем список папок, в которых ищем файлы
    folders = ["444/", "555/", "666/"]

    files_to_download = []

    # Ищем json-файлы в каждой папке, которые ещё не публиковали
    for folder in folders:
        try:
            folder_files = [
                file_version.file_name
                for file_version, _ in bucket.ls(folder, recursive=True)
                if file_version.file_name.endswith(".json")
            ]

            # Фильтруем по unpublished generation_id
            for f_name in folder_files:
                file_name_only = os.path.basename(f_name)
                base_name = file_name_only.rsplit(".", 1)[0]
                parts = base_name.split("-")
                if len(parts) < 2:
                    # Если формат имени не подходит, пропустим
                    continue
                gen_id = "-".join(parts[:2])
                if gen_id not in published_generation_ids:
                    files_to_download.append(f_name)
        except Exception as e:
            print(f"🚨 Ошибка при получении списка файлов в {folder}: {e}")

    if not files_to_download:
        print(f"⚠️ Нет новых файлов для загрузки во всех папках ({', '.join(folders)})")
        return  # Останавливаем работу, если файлов нет

    message_count = 0

    for file_name in files_to_download:
        local_path = os.path.join(DOWNLOAD_DIR, os.path.basename(file_name))

        try:
            print(f"📥 Скачивание {file_name} в {local_path}...")
            bucket.download_file_by_name(file_name).save_to(local_path)

            with open(local_path, "r", encoding="utf-8") as f:
                data = json.load(f)

            topic_clean = data.get("topic", {}).get("topic", "").strip("'\"")
            text_content = data.get("text_initial", {}).get("content", "").strip()

            # 🛑 Очистка системных фраз / эмодзи
            clean_text = text_content.replace(f'Сгенерированный текст на тему: "{topic_clean}"', "").strip()
            clean_text = clean_text.replace("Интересный факт:", "").strip()
            clean_text = clean_text.replace("🔶 Саркастический комментарий:", "").strip()
            clean_text = clean_text.replace("🔸 Саркастический вопрос:", "").strip()
            clean_text = clean_text.lstrip("🏛").strip()  # Убираем лишнюю иконку, если есть

            if clean_text:
                formatted_text = f"🏛 <b>{topic_clean}</b>\n\n{clean_text}"
                print(f"📨 Отправляем в Telegram: {formatted_text}")
                await bot.send_message(chat_id=TELEGRAM_CHAT_ID, text=formatted_text, parse_mode="HTML")
                message_count += 1

            # Саркастический комментарий
            sarcasm_comment = data.get("sarcasm", {}).get("comment", "").strip()
            if sarcasm_comment:
                sarcasm_text = f"📜 <i>{sarcasm_comment}</i>"
                print(f"📨 Отправляем саркастический комментарий: {sarcasm_text}")
                await bot.send_message(chat_id=TELEGRAM_CHAT_ID, text=sarcasm_text, parse_mode="HTML")
                message_count += 1

            # Опрос (poll)
            if "sarcasm" in data and "poll" in data["sarcasm"]:
                poll_data = data["sarcasm"]["poll"]
                question = poll_data.get("question", "").strip()
                options = poll_data.get("options", [])

                if question and options and len(options) >= 2:
                    print(f"📊 Отправляем опрос: {question}")
                    await bot.send_poll(chat_id=TELEGRAM_CHAT_ID, question=f"🎭 {question}", options=options, is_anonymous=True)
                    message_count += 1
                else:
                    print("⚠️ Опрос не отправлен. Проверьте данные!")

            # Обновляем config_public.json
            update_generation_id_status(file_name)

        except Exception as e:
            print(f"🚨 Ошибка при обработке файла {file_name}: {e}")

    print(f"📊 Всего отправлено сообщений: {message_count}")
    print("🚀 Скрипт завершён.")


if __name__ == "__main__":
    asyncio.run(process_files())
