import os
import json
import b2sdk.v2
import asyncio
import shutil
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


async def process_files():
    """Функция обработки и отправки JSON-файлов в Telegram"""

    # 🔍 Отладка: очищаем локальную папку перед загрузкой
    print("🗑 Полная очистка локальной папки перед скачиванием...")
    shutil.rmtree(DOWNLOAD_DIR, ignore_errors=True)
    os.makedirs(DOWNLOAD_DIR, exist_ok=True)

    # 🔍 Полный листинг папок в B2
    for folder in ["666/", "555/", "444/"]:
        print(f"\n📁 Список файлов в папке {folder}:")
        for file_version, _ in bucket.ls(folder, recursive=True):
            print(f"  🔹 {file_version.file_name}")

    # 🔍 Отладка: получаем список файлов из B2 (только из 444/)
    print("\n📥 Запрашиваем актуальный список файлов в B2 (папка 666/)...")
    files_to_download = []
    for file_version, _ in bucket.ls("666/", recursive=True):
        files_to_download.append(file_version.file_name)

    print(f"📌 Найдено файлов в B2: {len(files_to_download)}")
    for file_name in files_to_download:
        print(f"  🔹 {file_name}")

    if not files_to_download:
        print("⚠️ Нет новых файлов для загрузки. Ожидание новых данных...")
        with open(CONFIG_PATH, "w", encoding="utf-8") as f:
            json.dump({"status": "waiting", "files": []}, f, indent=4)
        return

    for file_name in files_to_download:
        local_path = os.path.join(DOWNLOAD_DIR, os.path.basename(file_name))

        # Пропускаем не JSON-файлы
        if not file_name.endswith(".json"):
            print(f"⏭ Пропускаем файл {file_name} (не JSON)")
            continue

        try:
            # 🔍 Отладка: скачивание файла
            print(f"📥 Скачивание {file_name} в {local_path}...")
            bucket.download_file_by_name(file_name).save_to(local_path)

            # 🔍 Отладка: проверяем содержимое файла перед обработкой
            with open(local_path, "r", encoding="utf-8") as f:
                data = f.read()

            print(f"📂 Загруженные данные ({file_name}): {type(data)}")
            print(f"🔍 Первые 300 символов файла: {data[:300]}")

            # 🔍 Если JSON закодирован в строке, пробуем декодировать дважды
            try:
                if isinstance(data, str):
                    data = json.loads(data)
                if isinstance(data, str):
                    data = json.loads(data)
            except json.JSONDecodeError as e:
                print(f"❌ Ошибка разбора JSON в {file_name}: {e}")
                continue

            # 🔍 Проверяем, что data — словарь
            if not isinstance(data, dict):
                print(f"🚨 Ошибка: JSON загружен в неправильном формате! Полный JSON:\n{data}")
                continue

            # 🔹 Извлекаем данные
            topic = data.get("topic", {}).get("topic", "Без темы")
            text_content = data.get("text_initial", {}).get("content", "Контент отсутствует.")
            critique = data.get("critique", {}).get("critique", "")
            sarcasm = data.get("sarcasm", {}).get("comment", "")
            poll = data.get("sarcasm", {}).get("poll", "")

            # 🔹 Формируем первый пост (основной текст)
            message = f"🏛 **{data['topic']['topic'].strip('\"')}**\n\n"
            message += f"{data['text_initial']['content'].split('Сгенерированный текст на тему: ')[-1]}\n\n"

            # Отправляем основной пост в Telegram
            await bot.send_message(chat_id=TELEGRAM_CHAT_ID, text=message, parse_mode="Markdown")

            # 🔹 Формируем второй пост (опрос)
            if "sarcasm" in data and "poll" in data["sarcasm"]:
                poll_message = f"📜 _{data['sarcasm']['comment']}_\n\n\n"
                poll_message += f"🎭 **{data['sarcasm']['poll']['question']}**\n"

                for option in data['sarcasm']['poll']['options']:
                    poll_message += f"🔹 {option.strip('\"')}\n"

                # Отправляем опрос в Telegram
                await bot.send_message(chat_id=TELEGRAM_CHAT_ID, text=poll_message, parse_mode="Markdown")

            if critique:
                message += f"\n\n💡 **Критический разбор**\n{critique}"

            if sarcasm:
                message += f"\n\n📢 **Сарказм**\n{sarcasm}"

            if poll:
                message += f"\n\n📊 **Опрос**\n{poll}"

            # 🔹 Отправляем сообщение в Telegram
            try:
                await bot.send_message(chat_id=TELEGRAM_CHAT_ID, text=message, parse_mode="Markdown")
                print(f"✅ Сообщение отправлено: {file_name}")
            except TelegramError as e:
                print(f"🚨 Ошибка отправки в Telegram: {e}")

            # 🔍 Отладка: удаляем файл после обработки
            processed_dir = os.path.join(BASE_DIR, "data", "processed")
            os.makedirs(processed_dir, exist_ok=True)  # Создаём папку, если её нет

            # Копируем файл перед удалением
            shutil.copy(local_path, os.path.join(processed_dir, os.path.basename(local_path)))

            # Теперь можно удалить оригинал
            os.remove(local_path)
            print(f"🗑 Файл {file_name} перемещён в архив processed и удалён из data/downloaded.")


        except Exception as e:
            print(f"🚨 Ошибка при обработке файла {file_name}: {e}")

    print("🚀 Скрипт завершён.")


# Запуск асинхронного кода
if __name__ == "__main__":
    asyncio.run(process_files())
