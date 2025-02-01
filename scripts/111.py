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


async def process_files():
    print("🗑 Полная очистка локальной папки перед скачиванием...")
    shutil.rmtree(DOWNLOAD_DIR, ignore_errors=True)
    os.makedirs(DOWNLOAD_DIR, exist_ok=True)

    print("\n📥 Проверяем статус публикации в config_public.json...")
    published_generation_ids = get_published_generation_ids()

    # Проверяем файлы во всех папках: 444/, 555/, 666/
    folders = ["444/", "555/", "666/"]
    files_to_download = []

    for folder in folders:
        folder_files = [
            file_version.file_name for file_version, _ in bucket.ls(folder, recursive=True)
            if file_version.file_name.endswith(".json")
        ]
        # Добавляем только новые файлы, которые ещё не опубликованы
        new_files = [file for file in folder_files if "-".join(file.split("/")[1].split("-")[:2]) not in published_generation_ids]
        files_to_download.extend(new_files)

    if not files_to_download:
        print(f"⚠️ Нет новых файлов для загрузки во всех папках ({', '.join(folders)})")
        return  # Останавливаем работу, если файлов нет

    for file_name in files_to_download:
        local_path = os.path.join(DOWNLOAD_DIR, os.path.basename(file_name))

        try:
            print(f"📥 Скачивание {file_name} в {local_path}...")
            bucket.download_file_by_name(file_name).save_to(local_path)

            with open(local_path, "r", encoding="utf-8") as f:
                data = json.load(f)

            topic_clean = data.get("topic", {}).get("topic", "").strip("'\"")
            text_content = data.get("text_initial", {}).get("content", "").strip()

            # 🛑 Очистка системных фраз
            clean_text = text_content.replace(f'Сгенерированный текст на тему: "{topic_clean}"', '').strip()
            clean_text = clean_text.replace("Интересный факт:", "").strip()
            clean_text = clean_text.replace("🔶 Саркастический комментарий:", "").strip()
            clean_text = clean_text.replace("🔸 Саркастический вопрос:", "").strip()

            # 🛑 Удаляем лишние эмодзи (оставляем только один в начале)
            clean_text = clean_text.replace("🏛", "").strip()
            formatted_text = f"🏛 <b>{topic_clean}</b>\n\n{clean_text}"

            await bot.send_message(chat_id=TELEGRAM_CHAT_ID, text=formatted_text, parse_mode="HTML")
            await asyncio.sleep(1)

            # 📜 Отправка саркастического комментария (если есть)
            sarcasm_comment = data.get("sarcasm", {}).get("comment", "").strip()
            if sarcasm_comment:
                sarcasm_text = f"📜 <i>{sarcasm_comment}</i>"
                await bot.send_message(chat_id=TELEGRAM_CHAT_ID, text=sarcasm_text, parse_mode="HTML")
                await asyncio.sleep(1)

            # 🎭 Отправка интерактивного опроса (если есть)
            if "sarcasm" in data and "poll" in data["sarcasm"]:
                poll_data = data["sarcasm"]["poll"]
                question = poll_data.get("question", "").strip()
                options = poll_data.get("options", [])

                if question and options and len(options) >= 2:
                    await bot.send_poll(chat_id=TELEGRAM_CHAT_ID, question=f"🎭 {question}", options=options, is_anonymous=True)
                    await asyncio.sleep(1)
                else:
                    print("⚠️ Опрос не отправлен. Проверьте данные!")

            update_generation_id_status(file_name)

        except Exception as e:
            print(f"🚨 Ошибка при обработке файла {file_name}: {e}")

    print("🚀 Скрипт завершён.")


def get_published_generation_ids():
    """Скачивает config_public.json из B2 и возвращает список опубликованных generation_id."""
    try:
        local_config_path = os.path.join(DOWNLOAD_DIR, "config_public.json")
        bucket.download_file_by_name("config/config_public.json").save_to(local_config_path)

        with open(local_config_path, "r", encoding="utf-8") as f:
            config_data = json.load(f)

        return set(config_data.get("generation_id", []))  # Возвращаем список опубликованных generation_id
    except Exception as e:
        print(f"🚨 Ошибка при загрузке config_public.json: {e}")
        return set()


def update_generation_id_status(file_name):
    """Добавляет новый generation_id в config_public.json, не удаляя старые записи."""
    try:
        local_config_path = os.path.join(DOWNLOAD_DIR, "config_public.json")

        # 📥 Загружаем config_public.json
        if os.path.exists(local_config_path):
            with open(local_config_path, "r", encoding="utf-8") as f:
                config_data = json.load(f)
        else:
            config_data = {}

        # 🏷 Извлекаем generation_id из имени файла
        generation_id = "-".join(file_name.split("/")[1].split("-")[:2]).split(".")[0]  # Берём ID группы из имени файла

        # ✅ Проверяем, есть ли уже generation_id, сохраняем как список
        existing_ids = config_data.get("generation_id", [])
        if not isinstance(existing_ids, list):
            existing_ids = [existing_ids]  # Преобразуем строку в список, если это старый формат

        if generation_id in existing_ids:
            print(f"⚠️ generation_id {generation_id} уже записан, пропускаем обновление.")
            return  # Если ID уже записан, не дублируем его

        existing_ids.append(generation_id)

        config_data["generation_id"] = existing_ids  # Записываем в JSON

        # 📤 Загружаем обратно в B2
        with open(local_config_path, "w", encoding="utf-8") as f:
            json.dump(config_data, f, ensure_ascii=False, indent=4)

        bucket.upload_local_file(local_config_path, "config/config_public.json")
        print(f"✅ Обновлён config_public.json: {config_data['generation_id']}")

    except Exception as e:
        print(f"🚨 Ошибка при обновлении config_public.json: {e}")


if __name__ == "__main__":
    asyncio.run(process_files())

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


async def process_files():
    print("🗑 Полная очистка локальной папки перед скачиванием...")
    shutil.rmtree(DOWNLOAD_DIR, ignore_errors=True)
    os.makedirs(DOWNLOAD_DIR, exist_ok=True)

    print("\n📥 Проверяем статус публикации в config_public.json...")
    published_generation_ids = get_published_generation_ids()

    # Определяем, какие файлы можно публиковать
    files_to_download = [
        file_version.file_name for file_version, _ in bucket.ls("444/", recursive=True)
        if file_version.file_name.endswith(".json")
    ]

    if not files_to_download:
        print(f"⚠️ Нет новых файлов для загрузки из 444/")
        return

    for file_name in files_to_download:
        local_path = os.path.join(DOWNLOAD_DIR, os.path.basename(file_name))

        try:
            print(f"📥 Скачивание {file_name} в {local_path}...")
            bucket.download_file_by_name(file_name).save_to(local_path)

            with open(local_path, "r", encoding="utf-8") as f:
                data = json.load(f)

            topic_clean = data.get("topic", {}).get("topic", "").strip("'\"")
            text_content = data.get("text_initial", {}).get("content", "").strip()

            # 🛑 Очистка системных фраз
            clean_text = text_content.replace(f'Сгенерированный текст на тему: "{topic_clean}"', '').strip()
            clean_text = clean_text.replace("Интересный факт:", "").strip()
            clean_text = clean_text.replace("🔶 Саркастический комментарий:", "").strip()
            clean_text = clean_text.replace("🔸 Саркастический вопрос:", "").strip()

            # 🛑 Удаляем лишние эмодзи (оставляем только один в начале)
            clean_text = clean_text.replace("🏛", "").strip()
            formatted_text = f"🏛 <b>{topic_clean}</b>\n\n{clean_text}"

            await bot.send_message(chat_id=TELEGRAM_CHAT_ID, text=formatted_text, parse_mode="HTML")
            await asyncio.sleep(1)

            # 📜 Отправка саркастического комментария (если есть)
            sarcasm_comment = data.get("sarcasm", {}).get("comment", "").strip()
            if sarcasm_comment:
                sarcasm_text = f"📜 <i>{sarcasm_comment}</i>"
                await bot.send_message(chat_id=TELEGRAM_CHAT_ID, text=sarcasm_text, parse_mode="HTML")
                await asyncio.sleep(1)

            # 🎭 Отправка интерактивного опроса (если есть)
            if "sarcasm" in data and "poll" in data["sarcasm"]:
                poll_data = data["sarcasm"]["poll"]
                question = poll_data.get("question", "").strip()
                options = poll_data.get("options", [])

                if question and options and len(options) >= 2:
                    await bot.send_poll(chat_id=TELEGRAM_CHAT_ID, question=f"🎭 {question}", options=options, is_anonymous=True)
                    await asyncio.sleep(1)
                else:
                    print("⚠️ Опрос не отправлен. Проверьте данные!")

            update_generation_id_status(file_name)

        except Exception as e:
            print(f"🚨 Ошибка при обработке файла {file_name}: {e}")

    print("🚀 Скрипт завершён.")


def get_published_generation_ids():
    """Скачивает config_public.json из B2 и возвращает список опубликованных generation_id."""
    try:
        local_config_path = os.path.join(DOWNLOAD_DIR, "config_public.json")
        bucket.download_file_by_name("config/config_public.json").save_to(local_config_path)

        with open(local_config_path, "r", encoding="utf-8") as f:
            config_data = json.load(f)

        return set(config_data.get("generation_id", []))  # Возвращаем список опубликованных generation_id
    except Exception as e:
        print(f"🚨 Ошибка при загрузке config_public.json: {e}")
        return set()


def update_generation_id_status(file_name):
    """Добавляет новый generation_id в config_public.json, не удаляя старые записи."""
    try:
        local_config_path = os.path.join(DOWNLOAD_DIR, "config_public.json")

        # 📥 Загружаем config_public.json
        if os.path.exists(local_config_path):
            with open(local_config_path, "r", encoding="utf-8") as f:
                config_data = json.load(f)
        else:
            config_data = {}

        # 🏷 Извлекаем generation_id из имени файла
        generation_id = file_name.split("/")[1].split("-")[0]  # Берём ID группы из имени файла

        # ✅ Проверяем, есть ли уже generation_id, сохраняем как список
        existing_ids = config_data.get("generation_id", [])
        if not isinstance(existing_ids, list):
            existing_ids = [existing_ids]  # Преобразуем строку в список, если это старый формат

        if generation_id not in existing_ids:
            existing_ids.append(generation_id)  # Добавляем новый ID в список

        config_data["generation_id"] = existing_ids  # Записываем в JSON

        # 📤 Загружаем обратно в B2
        with open(local_config_path, "w", encoding="utf-8") as f:
            json.dump(config_data, f, ensure_ascii=False, indent=4)

        bucket.upload_local_file(local_config_path, "config/config_public.json")
        print(f"✅ Обновлён config_public.json: {config_data['generation_id']}")

    except Exception as e:
        print(f"🚨 Ошибка при обновлении config_public.json: {e}")


if __name__ == "__main__":
    asyncio.run(process_files())
