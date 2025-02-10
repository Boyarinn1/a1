#!/usr/bin/env python3
import os
import json
import asyncio
import shutil
from telegram import Bot
import b2sdk.v2
import re

# ------------------------------------------------------------
# 1) Считываем переменные окружения
# ------------------------------------------------------------
S3_KEY_ID = os.getenv("S3_KEY_ID")
S3_APPLICATION_KEY = os.getenv("S3_APPLICATION_KEY")
S3_BUCKET_NAME = os.getenv("S3_BUCKET_NAME")
S3_ENDPOINT = os.getenv("S3_ENDPOINT", "production")

TELEGRAM_TOKEN = os.getenv("TELEGRAM_TOKEN")
TELEGRAM_CHAT_ID = os.getenv("TELEGRAM_CHAT_ID")

if not all([
    S3_KEY_ID,
    S3_APPLICATION_KEY,
    S3_BUCKET_NAME,
    TELEGRAM_TOKEN,
    TELEGRAM_CHAT_ID
]):
    raise RuntimeError("❌ Ошибка: Не установлены все необходимые переменные окружения!")

# ------------------------------------------------------------
# 2) Настраиваем пути, объект Telegram-бота и B2 SDK
# ------------------------------------------------------------
BASE_DIR = os.path.dirname(__file__)
DOWNLOAD_DIR = os.path.join(BASE_DIR, "downloaded")

bot = Bot(token=TELEGRAM_TOKEN)

info = b2sdk.v2.InMemoryAccountInfo()
b2_api = b2sdk.v2.B2Api(info)
b2_api.authorize_account(S3_ENDPOINT, S3_KEY_ID, S3_APPLICATION_KEY)
bucket = b2_api.get_bucket_by_name(S3_BUCKET_NAME)

# ------------------------------------------------------------
# Работа с config_public.json
# ------------------------------------------------------------
def load_published_ids() -> set:
    local_config = os.path.join(DOWNLOAD_DIR, "config_public.json")
    try:
        bucket.download_file_by_name("config/config_public.json").save_to(local_config)
        with open(local_config, "r", encoding="utf-8") as f:
            data = json.load(f)
        published = data.get("generation_id", [])
        if not isinstance(published, list):
            return set()
        return set(published)
    except Exception as e:
        print(f"⚠️ Не удалось загрузить config_public.json: {e}")
        return set()

def save_published_ids(pub_ids: set):
    local_config = os.path.join(DOWNLOAD_DIR, "config_public.json")
    try:
        if os.path.exists(local_config):
            with open(local_config, "r", encoding="utf-8") as f:
                data = json.load(f)
        else:
            data = {}
        data["generation_id"] = list(pub_ids)

        with open(local_config, "w", encoding="utf-8") as f:
            json.dump(data, f, ensure_ascii=False, indent=4)

        bucket.upload_local_file(local_config, "config/config_public.json")
        print(f"✅ Обновлён config_public.json: {data['generation_id']}")
    except Exception as e:
        print(f"⚠️ Не удалось сохранить config_public.json: {e}")

# ------------------------------------------------------------
# 3) Удаляем системные слова и сжимаем пустые строки
# ------------------------------------------------------------
def remove_system_phrases(text: str) -> str:
    system_phrases = [
        "Вступление:", "Основная часть:", "Интересный факт:", "Заключение:",
        "🔥Вступление", "📚Основная часть", "🔍Интересный факт"
    ]
    clean = text
    # Убираем системные заголовки
    for phrase in system_phrases:
        clean = clean.replace(phrase, "")

    # Сжимаем последовательности пустых строк до 1 двойного переноса
    clean = re.sub(r"\n\s*\n+", "\n\n", clean)

    return clean.strip()

# ------------------------------------------------------------
# 4) Публикация одного JSON (до 3 сообщений)
# ------------------------------------------------------------
async def publish_generation_id(gen_id: str, folder: str, published_ids: set) -> bool:
    # Ищем {gen_id}.json
    matches = []
    for file_version, _ in bucket.ls(folder, recursive=True):
        if file_version.file_name.endswith(".json"):
            basename = os.path.basename(file_version.file_name)
            base_noext = basename.rsplit(".", 1)[0]
            if base_noext == gen_id:
                matches.append(file_version.file_name)

    if not matches:
        print(f"⚠️ Не найден файл {gen_id}.json в папке {folder}")
        return False

    messages_sent = 0
    for file_key in matches:
        local_path = os.path.join(DOWNLOAD_DIR, os.path.basename(file_key))
        print(f"📥 Скачиваем {file_key} -> {local_path}")
        bucket.download_file_by_name(file_key).save_to(local_path)

        with open(local_path, "r", encoding="utf-8") as f:
            data = json.load(f)

        # ---------- ОБРАБОТКА TOPIC -----------
        raw_topic = data.get("topic", "")
        if isinstance(raw_topic, dict):
            # Если topic - словарь, берём full_topic
            topic = raw_topic.get("full_topic", "")
            if isinstance(topic, str):
                topic = topic.strip("'\"")
            else:
                topic = ""
        elif isinstance(raw_topic, str):
            # Если topic - строка, обрабатываем
            topic = raw_topic.strip("'\"")
        else:
            # Иначе пустая строка
            topic = ""

        # Удаляем системные фразы из контента
        content = data.get("content", "").strip()
        content = remove_system_phrases(content)

        # (1) Текстовое сообщение
        if content:
            if topic:
                text_send = f"🏛 <b>{topic}</b>\n\n{content}"
            else:
                text_send = content
            await bot.send_message(
                chat_id=TELEGRAM_CHAT_ID,
                text=text_send,
                parse_mode="HTML"
            )
            messages_sent += 1

        # (2) Сарказм
        sarcasm_comment = data.get("sarcasm", {}).get("comment", "").strip()
        if sarcasm_comment:
            sarcasm_text = f"📜 <i>{sarcasm_comment}</i>"
            await bot.send_message(
                chat_id=TELEGRAM_CHAT_ID,
                text=sarcasm_text,
                parse_mode="HTML"
            )
            messages_sent += 1

        # (3) Опрос
        poll = data.get("sarcasm", {}).get("poll", {})
        # Обрезаем вопрос и варианты до 99 символов, чтобы не вызвать BadRequest
        question = poll.get("question", "").strip()[:99]
        options = [opt.strip()[:99] for opt in poll.get("options", [])]

        if question and len(options) >= 2:
            poll_question = f"🎭 {question}"
            await bot.send_poll(
                chat_id=TELEGRAM_CHAT_ID,
                question=poll_question,
                options=options,
                is_anonymous=True  # Или False, если нужен открытый опрос
            )
            messages_sent += 1

        # Перемещаем обработанный файл
        processed_dir = os.path.join(DOWNLOAD_DIR, "processed")
        os.makedirs(processed_dir, exist_ok=True)
        shutil.move(local_path, os.path.join(processed_dir, os.path.basename(local_path)))
        print(f"🗑 Файл {file_key} перемещён в {processed_dir}")

    # Добавляем gen_id в published_ids и сохраняем
    published_ids.add(gen_id)
    save_published_ids(published_ids)

    return (messages_sent > 0)

# ------------------------------------------------------------
# 5) Основная логика (одна группа за запуск)
# ------------------------------------------------------------
async def main():
    print("🗑 Очищаем локальную папку для скачивания...")
    shutil.rmtree(DOWNLOAD_DIR, ignore_errors=True)
    os.makedirs(DOWNLOAD_DIR, exist_ok=True)

    published_ids = load_published_ids()
    folders = ["444/", "555/", "666/"]

    for folder in folders:
        print(f"🔎 Сканируем папку: {folder}")
        all_jsons = []
        for file_version, _ in bucket.ls(folder, recursive=True):
            if file_version.file_name.endswith(".json"):
                all_jsons.append(file_version.file_name)

        gen_ids_in_folder = set()
        for file_key in all_jsons:
            base_noext = os.path.splitext(os.path.basename(file_key))[0]
            gen_ids_in_folder.add(base_noext)

        print(f"DEBUG: Найдены JSON-файлы: {gen_ids_in_folder}")

        for gen_id in sorted(gen_ids_in_folder):
            if gen_id in published_ids:
                continue

            print(f"🔎 Найдена новая группа: {gen_id} в {folder}. Пытаемся опубликовать...")
            success = await publish_generation_id(gen_id, folder, published_ids)
            if success:
                print("✅ Опубликована одна группа, завершаем скрипт.")
                return
            else:
                print(f"⚠️ Публикация gen_id={gen_id} не дала сообщений. Продолжаем...")

        print(f"ℹ️ В папке {folder} не осталось новых групп для публикации.")

    print("🚀 Нет новых групп во всех папках. Скрипт завершён.")


if __name__ == "__main__":
    asyncio.run(main())
