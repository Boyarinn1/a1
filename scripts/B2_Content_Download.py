#!/usr/bin/env python3
import os
import json
import asyncio
import shutil
from telegram import Bot
import b2sdk.v2
import re
from typing import Set, List, Tuple

# ------------------------------------------------------------
# 1) Считываем переменные окружения
# ------------------------------------------------------------
S3_KEY_ID = os.getenv("S3_KEY_ID")
S3_APPLICATION_KEY = os.getenv("S3_APPLICATION_KEY")
S3_BUCKET_NAME = os.getenv("S3_BUCKET_NAME")
S3_ENDPOINT = os.getenv("S3_ENDPOINT", "production")

TELEGRAM_TOKEN = os.getenv("TELEGRAM_TOKEN")
TELEGRAM_CHAT_ID = os.getenv("TELEGRAM_CHAT_ID")

# Проверяем наличие всех необходимых переменных
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
PROCESSED_DIR = os.path.join(DOWNLOAD_DIR, "processed") # Папка для обработанных файлов

# Инициализация Telegram бота
bot = Bot(token=TELEGRAM_TOKEN)

# Инициализация B2 API
info = b2sdk.v2.InMemoryAccountInfo()
b2_api = b2sdk.v2.B2Api(info)
try:
    b2_api.authorize_account(S3_ENDPOINT, S3_KEY_ID, S3_APPLICATION_KEY)
    bucket = b2_api.get_bucket_by_name(S3_BUCKET_NAME)
    print(f"✅ Успешное подключение к B2 бакету: {S3_BUCKET_NAME}")
except Exception as e:
    raise RuntimeError(f"❌ Ошибка подключения к B2: {e}")

# ------------------------------------------------------------
# Работа с config_public.json (отслеживание опубликованных)
# ------------------------------------------------------------
def load_published_ids() -> Set[str]:
    """
    Загружает список ID уже опубликованных постов из config_public.json в B2.
    Возвращает set с ID.
    """
    local_config = os.path.join(DOWNLOAD_DIR, "config_public.json")
    config_key = "config/config_public.json"
    published_ids = set()
    try:
        print(f"📥 Пытаемся скачать {config_key}...")
        # Убедимся, что папка для скачивания существует
        os.makedirs(os.path.dirname(local_config), exist_ok=True)
        bucket.download_file_by_name(config_key).save_to(local_config)
        with open(local_config, "r", encoding="utf-8") as f:
            data = json.load(f)
        published = data.get("generation_id", [])
        if isinstance(published, list):
             published_ids = set(published)
        print(f"ℹ️ Загружено {len(published_ids)} опубликованных ID из {config_key}.")
    except b2sdk.exception.FileNotPresent as e:
         print(f"⚠️ Файл {config_key} не найден в B2. Будет создан новый.")
         # Если файла нет, создаем пустой set, он будет сохранен позже
    except Exception as e:
        print(f"⚠️ Не удалось загрузить или прочитать {config_key}: {e}. Используем пустой список.")
    return published_ids

def save_published_ids(pub_ids: Set[str]):
    """
    Сохраняет обновленный список опубликованных ID в config_public.json локально и загружает в B2.
    """
    local_config = os.path.join(DOWNLOAD_DIR, "config_public.json")
    config_key = "config/config_public.json"
    try:
        # Сначала создаем структуру данных для JSON
        data = {"generation_id": sorted(list(pub_ids))} # Сохраняем отсортированный список для удобства

        # Записываем в локальный файл
        os.makedirs(os.path.dirname(local_config), exist_ok=True) # Убедимся, что папка существует
        with open(local_config, "w", encoding="utf-8") as f:
            json.dump(data, f, ensure_ascii=False, indent=4)
        print(f"💾 Локально сохранен {local_config}")

        # Загружаем в B2
        print(f"📤 Загружаем обновленный {config_key} в B2...")
        bucket.upload_local_file(local_config, config_key)
        print(f"✅ Успешно обновлен {config_key} в B2. Всего ID: {len(pub_ids)}")
    except Exception as e:
        print(f"⚠️ Не удалось сохранить или загрузить {config_key}: {e}")

# ------------------------------------------------------------
# 3) Удаляем системные слова и сжимаем пустые строки
# ------------------------------------------------------------
def remove_system_phrases(text: str) -> str:
    """Очищает текст от стандартных заголовков и лишних пустых строк."""
    system_phrases = [
        "Вступление:", "Основная часть:", "Интересный факт:", "Заключение:",
        "🔥Вступление", "📚Основная часть", "🔍Интересный факт"
    ]
    clean = text
    for phrase in system_phrases:
        # Используем re.sub для удаления фразы без учета регистра и с возможными пробелами вокруг
        clean = re.sub(r'\s*' + re.escape(phrase) + r'\s*', '', clean, flags=re.IGNORECASE)
    # Заменяем множественные переводы строк на двойной перевод строки
    clean = re.sub(r"\n\s*\n+", "\n\n", clean)
    return clean.strip()

# ------------------------------------------------------------
# 4) Публикация одного JSON (с видео и до 3 сообщений)
# ------------------------------------------------------------
async def publish_generation_id(gen_id: str, folder: str, published_ids: Set[str]) -> bool:
    """
    Публикует контент для одного generation_id (JSON + опционально видео).
    Возвращает True, если хотя бы одно сообщение было отправлено, иначе False.
    """
    print(f"⚙️ Обрабатываем gen_id: {gen_id} из папки {folder}")
    json_file_key = f"{folder}{gen_id}.json"
    video_file_key = f"{folder}{gen_id}.mp4"

    # --- Скачивание файлов ---
    local_json_path = os.path.join(DOWNLOAD_DIR, f"{gen_id}.json")
    local_video_path = os.path.join(DOWNLOAD_DIR, f"{gen_id}.mp4")
    video_downloaded = False

    try:
        print(f"📥 Скачиваем JSON: {json_file_key} -> {local_json_path}")
        # Убедимся, что папка для скачивания существует
        os.makedirs(os.path.dirname(local_json_path), exist_ok=True)
        bucket.download_file_by_name(json_file_key).save_to(local_json_path)
    except b2sdk.exception.FileNotPresent:
        print(f"⚠️ JSON файл не найден: {json_file_key}")
        return False # Не можем продолжить без JSON
    except Exception as e:
        print(f"⚠️ Ошибка при скачивании JSON {json_file_key}: {e}")
        return False

    try:
        print(f"📥 Пытаемся скачать видео: {video_file_key} -> {local_video_path}")
         # Убедимся, что папка для скачивания существует
        os.makedirs(os.path.dirname(local_video_path), exist_ok=True)
        bucket.download_file_by_name(video_file_key).save_to(local_video_path)
        video_downloaded = True
        print(f"✅ Видео скачано: {local_video_path}")
    except b2sdk.exception.FileNotPresent:
        print(f"ℹ️ Видео не найдено для {gen_id}, продолжаем без него.")
        video_file_key = None # Сбрасываем ключ видео, если его нет
    except Exception as e:
        print(f"⚠️ Ошибка при скачивании видео {video_file_key}: {e}")
        # Продолжаем без видео, но логируем ошибку

    # --- Отправка в Telegram ---
    messages_sent = 0
    os.makedirs(PROCESSED_DIR, exist_ok=True) # Убедимся, что папка для обработанных существует

    # Отправляем видео, если оно было скачано
    if video_downloaded:
        try:
            print(f"✈️ Отправляем видео {gen_id}.mp4 в Telegram...")
            with open(local_video_path, "rb") as video_file:
                await bot.send_video(
                    chat_id=TELEGRAM_CHAT_ID,
                    video=video_file,
                    supports_streaming=True,
                    # caption=f"Видео для {gen_id}" # Можно добавить подпись к видео
                )
            messages_sent += 1
            print(f"✅ Видео {gen_id}.mp4 отправлено.")
            # Перемещаем видео в processed
            shutil.move(local_video_path, os.path.join(PROCESSED_DIR, f"{gen_id}.mp4"))
            print(f"📁 Видео {gen_id}.mp4 перемещено в {PROCESSED_DIR}")
        except Exception as e:
            print(f"⚠️ Ошибка при отправке видео {gen_id}.mp4: {e}")
            # Если видео не отправилось, оно останется в downloaded и может быть обработано позже

    # Обрабатываем JSON
    try:
        with open(local_json_path, "r", encoding="utf-8") as f:
            data = json.load(f)

        # ---------- ОБРАБОТКА TOPIC -----------
        raw_topic = data.get("topic", "")
        topic = ""
        if isinstance(raw_topic, dict):
            topic = raw_topic.get("full_topic", "")
            if isinstance(topic, str):
                topic = topic.strip("'\" ") # Убираем кавычки и пробелы по краям
            else:
                 topic = ""
        elif isinstance(raw_topic, str):
            topic = raw_topic.strip("'\" ")
        # Если topic пустой после очистки, делаем его None или пустой строкой
        topic = topic if topic else ""

        # Удаляем системные фразы из контента
        content = data.get("content", "").strip()
        content = remove_system_phrases(content)

        # (1) Текстовое сообщение
        if content:
            text_to_send = ""
            if topic:
                # Формируем заголовок жирным шрифтом
                text_to_send = f"🏛 <b>{topic}</b>\n\n{content}"
            else:
                text_to_send = content

            # Проверка длины сообщения (Telegram лимит 4096 символов)
            if len(text_to_send) > 4096:
                 print(f"⚠️ Текст для {gen_id} слишком длинный ({len(text_to_send)} символов). Обрезаем до 4090...")
                 text_to_send = text_to_send[:4090] + "..." # Обрезаем с запасом

            print(f"✈️ Отправляем текстовое сообщение для {gen_id}...")
            await bot.send_message(
                chat_id=TELEGRAM_CHAT_ID,
                text=text_to_send,
                parse_mode="HTML" # Используем HTML для форматирования (<b>)
            )
            messages_sent += 1
            print(f"✅ Текстовое сообщение для {gen_id} отправлено.")

        # (2) Сарказм
        sarcasm_comment = data.get("sarcasm", {}).get("comment", "").strip()
        if sarcasm_comment:
             # Форматируем курсивом
            sarcasm_text = f"📜 <i>{sarcasm_comment}</i>"
            if len(sarcasm_text) > 4096: # Проверка длины
                 print(f"⚠️ Текст сарказма для {gen_id} слишком длинный. Обрезаем...")
                 sarcasm_text = sarcasm_text[:4090] + "..."

            print(f"✈️ Отправляем сарказм для {gen_id}...")
            await bot.send_message(
                chat_id=TELEGRAM_CHAT_ID,
                text=sarcasm_text,
                parse_mode="HTML" # Используем HTML для форматирования (<i>)
            )
            messages_sent += 1
            print(f"✅ Сарказм для {gen_id} отправлен.")

        # (3) Опрос
        poll_data = data.get("sarcasm", {}).get("poll", {})
        question = poll_data.get("question", "").strip()
        options = [str(opt).strip() for opt in poll_data.get("options", []) if str(opt).strip()] # Берем только непустые опции

        # Ограничения Telegram на опросы:
        # Вопрос: 1-300 символов
        # Опции: 1-100 символов каждая, от 2 до 10 опций
        question = question[:300] # Обрезаем вопрос, если длиннее
        options = [opt[:100] for opt in options][:10] # Обрезаем опции и берем не больше 10

        if question and len(options) >= 2:
            poll_question = f"🎭 {question}" # Добавляем эмодзи к вопросу
            print(f"✈️ Отправляем опрос для {gen_id}...")
            await bot.send_poll(
                chat_id=TELEGRAM_CHAT_ID,
                question=poll_question,
                options=options,
                is_anonymous=True # Анонимный опрос
            )
            messages_sent += 1
            print(f"✅ Опрос для {gen_id} отправлен.")
        elif question and len(options) < 2:
             print(f"ℹ️ Опрос для {gen_id} имеет вопрос, но меньше 2 валидных опций. Пропускаем.")
        # Если нет вопроса, ничего не делаем

        # Перемещаем обработанный JSON-файл, только если что-то было отправлено
        if messages_sent > 0:
            # Убедимся, что папка processed существует
            os.makedirs(PROCESSED_DIR, exist_ok=True)
            shutil.move(local_json_path, os.path.join(PROCESSED_DIR, os.path.basename(local_json_path)))
            print(f"📁 JSON файл {gen_id}.json перемещён в {PROCESSED_DIR}")
            # Добавляем gen_id в published_ids ТОЛЬКО ПОСЛЕ УСПЕШНОЙ ОТПРАВКИ
            published_ids.add(gen_id)
            save_published_ids(published_ids) # Сохраняем обновленный список ID
            print(f"📝 ID {gen_id} добавлен в список опубликованных.")
            return True
        else:
            print(f"⚠️ Для gen_id={gen_id} не было отправлено ни одного сообщения (возможно, пустой JSON?). Файл не перемещен, ID не добавлен.")
            # Удаляем локальный JSON, если он не был обработан и перемещен
            if os.path.exists(local_json_path):
                os.remove(local_json_path)
                print(f"🗑 Удален необработанный JSON файл: {local_json_path}")
            return False

    except json.JSONDecodeError as e:
        print(f"⚠️ Ошибка декодирования JSON файла {local_json_path}: {e}")
        # Можно переместить поврежденный JSON в отдельную папку для ошибок
        error_dir = os.path.join(DOWNLOAD_DIR, "errors")
        os.makedirs(error_dir, exist_ok=True)
        if os.path.exists(local_json_path):
             shutil.move(local_json_path, os.path.join(error_dir, os.path.basename(local_json_path)))
             print(f"📁 Поврежденный JSON {gen_id}.json перемещен в {error_dir}")
        return False
    except Exception as e:
        print(f"⚠️ Непредвиденная ошибка при обработке JSON или отправке сообщений для {gen_id}: {e}")
        return False


# ------------------------------------------------------------
# 5) Основная логика (поиск и публикация)
# ------------------------------------------------------------
async def main():
    print("\n" + "="*40)
    print("🚀 Запуск скрипта публикации B2 -> Telegram")
    print("="*40)

    print("🗑 Очищаем локальную папку для скачивания...")
    shutil.rmtree(DOWNLOAD_DIR, ignore_errors=True)
    os.makedirs(DOWNLOAD_DIR, exist_ok=True)
    print(f"✅ Папка {DOWNLOAD_DIR} готова.")

    # Загружаем ID уже опубликованных постов
    published_ids = load_published_ids()

    # Папки в B2 для сканирования
    folders_to_scan = ["444/", "555/", "666/"]
    print(f"📂 Папки для сканирования: {', '.join(folders_to_scan)}")

    # Собираем все неопубликованные gen_id из всех папок
    unpublished_items: List[Tuple[str, str]] = [] # Список кортежей (gen_id, folder)

    for folder in folders_to_scan:
        print(f"\n🔎 Сканируем папку: {folder}")
        try:
            # Получаем список файлов в папке
            # ИСПРАВЛЕНО: Убран аргумент show_versions
            ls_result = bucket.ls(folder_to_list=folder, recursive=True)

            gen_ids_in_folder = set()
            for file_version, _folder_name in ls_result:
                file_name = file_version.file_name
                # Убедимся, что файл находится непосредственно в папке folder (или ее подпапках, если recursive=True)
                # и имеет расширение .json
                if file_name.startswith(folder) and file_name.endswith(".json"):
                     # Извлекаем имя файла без пути и расширения
                     base_name = os.path.basename(file_name)
                     gen_id = os.path.splitext(base_name)[0]
                     # Проверяем формат YYYYMMDD-HHMM (простая проверка)
                     if re.fullmatch(r"\d{8}-\d{4}", gen_id):
                          gen_ids_in_folder.add(gen_id)
                     else:
                          print(f"   ⚠️ Пропускаем файл с некорректным именем ID: {file_name}")

            print(f"   ℹ️ Найдено {len(gen_ids_in_folder)} уникальных ID формата YYYYMMDD-HHMM в {folder}")

            # Отбираем только неопубликованные ID из этой папки
            new_ids = gen_ids_in_folder - published_ids
            if new_ids:
                print(f"   ✨ Найдено {len(new_ids)} новых (неопубликованных) ID в {folder}.")
                for gen_id in new_ids:
                    unpublished_items.append((gen_id, folder))
            else:
                print(f"   ✅ Нет новых ID для публикации в {folder}.")

        except Exception as e:
            # Ловим и выводим ошибку, но продолжаем сканировать другие папки
            print(f"   ❌ Ошибка при сканировании папки {folder}: {e}")

    # Если есть неопубликованные элементы
    if unpublished_items:
        print(f"\n⏳ Всего найдено {len(unpublished_items)} неопубликованных групп.")

        # Сортируем все найденные неопубликованные элементы по gen_id (хронологически)
        # Стандартная сортировка строк 'YYYYMMDD-HHMM' работает правильно
        unpublished_items.sort(key=lambda item: item[0])
        print("   🔢 Сортировка по дате и времени (gen_id)...")

        # Берем самый старый неопубликованный элемент
        gen_id_to_publish, folder_to_publish = unpublished_items[0]

        print(f"\n🎯 Выбрана самая старая группа для публикации: ID={gen_id_to_publish} из папки {folder_to_publish}")
        print("-" * 40)

        # Пытаемся опубликовать выбранную группу
        success = await publish_generation_id(gen_id_to_publish, folder_to_publish, published_ids)

        print("-" * 40)
        if success:
            print(f"✅ Успешно опубликована группа {gen_id_to_publish}.")
            # Список published_ids уже обновлен и сохранен внутри publish_generation_id
        else:
            print(f"⚠️ Не удалось опубликовать группу {gen_id_to_publish}. Подробности см. выше.")
            # В этом случае ID не добавляется в published_ids, попробуем в следующий раз

    else:
        print("\n🎉 Нет новых групп для публикации во всех папках.")

    print("\n🏁 Скрипт завершил работу.")
    print("="*40 + "\n")

# Точка входа
if __name__ == "__main__":
    # Используем asyncio для асинхронных операций (отправка в Telegram)
    asyncio.run(main())
