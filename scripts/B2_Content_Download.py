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
    """
    Скачивает config_public.json из B2 и возвращает множество (set) уже опубликованных generation_id.
    Например: {"20250201-1131", "20250201-1243"}
    """
    try:
        local_config_path = os.path.join(DOWNLOAD_DIR, "config_public.json")
        bucket.download_file_by_name("config/config_public.json").save_to(local_config_path)

        with open(local_config_path, "r", encoding="utf-8") as f:
            config_data = json.load(f)

        # Если в файле нет 'generation_id', возвращаем пустое множество
        return set(config_data.get("generation_id", []))
    except Exception as e:
        print(f"🚨 Ошибка при загрузке config_public.json: {e}")
        return set()

def save_published_generation_ids(published_ids: set):
    """
    Обновляет config_public.json, дополняя поле generation_id новыми значениями из published_ids.
    """
    try:
        local_config_path = os.path.join(DOWNLOAD_DIR, "config_public.json")

        # Загружаем текущее состояние (если файл уже есть)
        if os.path.exists(local_config_path):
            with open(local_config_path, "r", encoding="utf-8") as f:
                config_data = json.load(f)
        else:
            config_data = {}

        # Перезаписываем поле generation_id
        config_data["generation_id"] = list(published_ids)

        # Сохраняем локально
        with open(local_config_path, "w", encoding="utf-8") as f:
            json.dump(config_data, f, ensure_ascii=False, indent=4)

        # И загружаем обратно в B2
        bucket.upload_local_file(local_config_path, "config/config_public.json")
        print(f"✅ Обновлён config_public.json: {config_data['generation_id']}")
    except Exception as e:
        print(f"🚨 Ошибка при обновлении config_public.json: {e}")

async def process_one_generation_id(gen_id: str, folder: str, published_ids: set) -> bool:
    """
    Обрабатывает (скачивает и отправляет в TG) все файлы внутри папки `folder`,
    у которых basename (без .json) == gen_id.
    Возвращает True, если отправили хотя бы одно сообщение (успешная публикация).
    """

    # 1. Получаем список файлов .json в текущей папке
    all_files = [
        file_version.file_name
        for file_version, _ in bucket.ls(folder, recursive=True)
        if file_version.file_name.endswith(".json")
    ]

    # 2. Отбираем только те, у кого basename (без .json) совпадает с gen_id
    target_files = []
    for f_name in all_files:
        basename = os.path.basename(f_name)       # Пример: "20250201-1131.json"
        base_noext = basename.rsplit(".", 1)[0]   # "20250201-1131"
        if base_noext == gen_id:
            target_files.append(f_name)

    if not target_files:
        print(f"⚠️ Не найдено файлов для gen_id={gen_id} в папке {folder}")
        return False

    messages_sent = 0

    # 3. Последовательно обрабатываем каждый файл из выбранной группы
    for f_name in target_files:
        local_path = os.path.join(DOWNLOAD_DIR, os.path.basename(f_name))
        try:
            print(f"📥 Скачивание {f_name} в {local_path}...")
            bucket.download_file_by_name(f_name).save_to(local_path)

            # --- Добавляем отладку: вывод «сырых» данных и итогового парсинга ---
            with open(local_path, "r", encoding="utf-8") as f:
                raw_content = f.read()
                print(f"🔍 DEBUG (raw JSON) для файла {f_name}:\n{raw_content}")
                data = json.loads(raw_content)

            # Теперь извлекаем поля из JSON
            topic_clean = data.get("topic", {}).get("topic", "").strip("'\"")
            text_content = data.get("text_initial", {}).get("content", "").strip()

            # Отладочный принт для parsed-данных
            print(f"🔎 DEBUG (parsed) для файла {f_name}:")
            print(f"    topic_clean = '{topic_clean}'")
            print(f"    text_content = '{text_content}'")

            # --- Логика отправки в Telegram ---
            if text_content:
                formatted_text = f"🏛 <b>{topic_clean}</b>\n\n{text_content}"
                print(f"📨 Отправляем в Telegram: {formatted_text}")
                await bot.send_message(chat_id=TELEGRAM_CHAT_ID, text=formatted_text, parse_mode="HTML")
                messages_sent += 1

            # Отправка саркастического комментария
            sarcasm_comment = data.get("sarcasm", {}).get("comment", "").strip()
            if sarcasm_comment:
                sarcasm_text = f"📜 <i>{sarcasm_comment}</i>"
                print(f"📨 Отправляем саркастический комментарий: {sarcasm_text}")
                await bot.send_message(chat_id=TELEGRAM_CHAT_ID, text=sarcasm_text, parse_mode="HTML")
                messages_sent += 1

            # Отправка опроса (poll), если есть
            if "sarcasm" in data and "poll" in data["sarcasm"]:
                poll_data = data["sarcasm"]["poll"]
                question = poll_data.get("question", "").strip()
                options = poll_data.get("options", [])

                if question and options and len(options) >= 2:
                    print(f"📊 Отправляем опрос: {question}")
                    await bot.send_poll(
                        chat_id=TELEGRAM_CHAT_ID,
                        question=f"🎭 {question}",
                        options=options,
                        is_anonymous=True
                    )
                    messages_sent += 1
                else:
                    print("⚠️ Опрос не отправлен. Проверьте данные!")

            # --- Перемещаем файл в папку processed ---
            processed_dir = os.path.join(BASE_DIR, "data", "processed")
            os.makedirs(processed_dir, exist_ok=True)
            shutil.move(local_path, os.path.join(processed_dir, os.path.basename(local_path)))
            print(f"🗑 Файл {f_name} перемещён в архив processed.")

        except Exception as e:
            print(f"🚨 Ошибка при обработке файла {f_name}: {e}")

    # 4. Если хотя бы одно сообщение было отправлено, генерируем результат
    if messages_sent > 0:
        # Добавляем gen_id в опубликованные
        published_ids.add(gen_id)
        save_published_generation_ids(published_ids)
        return True
    else:
        print(f"⚠️ Группа {gen_id} не отправила ни одного сообщения (все тексты пустые?).")
        return False

async def process_files():
    print("🗑 Полная очистка локальной папки перед скачиванием...")
    shutil.rmtree(DOWNLOAD_DIR, ignore_errors=True)
    os.makedirs(DOWNLOAD_DIR, exist_ok=True)

    print("\n📥 Проверяем статус публикации в config_public.json...")
    published_ids = get_published_generation_ids()

    # Проходим папки по порядку (444 -> 555 -> 666)
    folders = ["444/", "555/", "666/"]

    for folder in folders:
        print(f"\n📁 Сканируем папку: {folder}")
        # Получаем список .json-файлов
        all_files = [
            file_version.file_name
            for file_version, _ in bucket.ls(folder, recursive=True)
            if file_version.file_name.endswith(".json")
        ]

        # Собираем все generation_id, которые встречаются в этой папке
        folder_generation_ids = set()
        for f_name in all_files:
            basename = os.path.basename(f_name)         # "20250201-1131.json"
            base_noext = basename.rsplit(".", 1)[0]     # "20250201-1131"
            folder_generation_ids.add(base_noext)

        # Сортируем для воспроизводимого порядка (необязательно)
        for gen_id in sorted(folder_generation_ids):
            # Если уже опубликован, пропускаем
            if gen_id in published_ids:
                continue

            # Публикуем (это будет "одна группа")
            print(f"🔎 Найдена новая группа: {gen_id}. Пытаемся опубликовать...")
            success = await process_one_generation_id(gen_id, folder, published_ids)
            if success:
                print("✅ Опубликовали одну группу, завершаем скрипт.")
                return  # Заканчиваем полностью
            else:
                # Если успеха не было (пустые данные?), переходим к следующему gen_id
                print(f"⚠️ Группа {gen_id} не опубликована, пробуем следующий.")

        print(f"ℹ️ В папке {folder} не осталось новых (неопубликованных) групп.")
        # Переходим к следующей папке

    print("🚀 Во всех папках нет новых групп для публикации. Скрипт завершён.")

if __name__ == "__main__":
    asyncio.run(process_files())
