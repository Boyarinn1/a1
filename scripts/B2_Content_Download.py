#!/usr/bin/env python3
import os
import json
import asyncio
import shutil
import re
from typing import Set, List, Tuple
# Импорты для Telegram API
from telegram import Bot, InputMediaPhoto, InputMediaVideo
# Импорты для B2 SDK и обработки ошибок
import b2sdk.v2
from b2sdk.v2.exception import FileNotPresent, B2Error

# ------------------------------------------------------------
# 1) Считываем переменные окружения
# ------------------------------------------------------------
S3_KEY_ID = os.getenv("S3_KEY_ID")
S3_APPLICATION_KEY = os.getenv("S3_APPLICATION_KEY")
S3_BUCKET_NAME = os.getenv("S3_BUCKET_NAME")
S3_ENDPOINT = os.getenv("S3_ENDPOINT", "production") # По умолчанию 'production', можно переопределить

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
    # Если чего-то не хватает, выводим ошибку и завершаем работу
    raise RuntimeError("❌ Ошибка: Не установлены все необходимые переменные окружения!")
else:
    print("✅ Все необходимые переменные окружения загружены.")

# ------------------------------------------------------------
# 2) Настраиваем пути, объект Telegram-бота и B2 SDK
# ------------------------------------------------------------
BASE_DIR = os.path.dirname(__file__) # Директория, где находится скрипт
DOWNLOAD_DIR = os.path.join(BASE_DIR, "downloaded") # Папка для временного хранения скачанных файлов
PROCESSED_DIR = os.path.join(DOWNLOAD_DIR, "processed") # Папка для успешно обработанных файлов
ERROR_DIR = os.path.join(DOWNLOAD_DIR, "errors") # Папка для файлов с ошибками (например, битый JSON)

# Инициализация Telegram бота
try:
    bot = Bot(token=TELEGRAM_TOKEN)
    print("✅ Telegram бот инициализирован.")
except Exception as e:
    raise RuntimeError(f"❌ Ошибка инициализации Telegram бота: {e}")

# Инициализация B2 API
try:
    print("⚙️ Подключаемся к Backblaze B2...")
    info = b2sdk.v2.InMemoryAccountInfo()
    b2_api = b2sdk.v2.B2Api(info)
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
    Загружает список ID уже опубликованных постов из config/config_public.json в B2.
    Возвращает set с ID. Если файл не найден или поврежден, возвращает пустой set.
    """
    local_config_path = os.path.join(DOWNLOAD_DIR, "config_public.json")
    config_key = "config/config_public.json" # Путь к файлу конфигурации в бакете
    published_ids = set()
    try:
        print(f"📥 Пытаемся скачать {config_key} для получения списка опубликованных ID...")
        # Убедимся, что папка для скачивания существует
        os.makedirs(os.path.dirname(local_config_path), exist_ok=True)
        # Скачиваем файл конфигурации из B2
        bucket.download_file_by_name(config_key).save_to(local_config_path)
        # Читаем JSON из скачанного файла
        with open(local_config_path, "r", encoding="utf-8") as f:
            data = json.load(f)
        # Извлекаем список ID из поля 'generation_id'
        published = data.get("generation_id", [])
        if isinstance(published, list):
             published_ids = set(published) # Преобразуем список в множество для быстрого поиска
             print(f"ℹ️ Загружено {len(published_ids)} опубликованных ID из {config_key}.")
        else:
             print(f"⚠️ Поле 'generation_id' в {config_key} не является списком. Используем пустой список.")
        # Удаляем локальную копию конфига после чтения
        os.remove(local_config_path)

    except FileNotPresent:
         # Если файл конфигурации не найден в B2, это нормально для первого запуска
         print(f"⚠️ Файл {config_key} не найден в B2. Будет создан новый при первой успешной публикации.")
    except json.JSONDecodeError as e:
         print(f"⚠️ Ошибка декодирования JSON в файле {config_key}: {e}. Используем пустой список.")
         # Если файл скачался, но битый, удаляем его локальную копию
         if os.path.exists(local_config_path): os.remove(local_config_path)
    except B2Error as e:
        # Обработка специфичных ошибок B2 SDK при скачивании
        print(f"⚠️ Ошибка B2 SDK при скачивании {config_key}: {e}. Используем пустой список.")
        if os.path.exists(local_config_path): os.remove(local_config_path)
    except Exception as e:
        # Обработка прочих непредвиденных ошибок
        print(f"⚠️ Не удалось загрузить или прочитать {config_key}: {e}. Используем пустой список.")
        if os.path.exists(local_config_path): os.remove(local_config_path)
    return published_ids

def save_published_ids(pub_ids: Set[str]):
    """
    Сохраняет обновленный список опубликованных ID в config_public.json локально
    и загружает его обратно в B2.
    """
    local_config_path = os.path.join(DOWNLOAD_DIR, "config_public.json")
    config_key = "config/config_public.json"
    try:
        # Создаем структуру данных для JSON: словарь с ключом 'generation_id'
        # и значением - отсортированным списком ID из множества
        data = {"generation_id": sorted(list(pub_ids))}

        # Записываем в локальный файл
        os.makedirs(os.path.dirname(local_config_path), exist_ok=True) # Убедимся, что папка существует
        with open(local_config_path, "w", encoding="utf-8") as f:
            # Сохраняем JSON с отступами для читаемости
            json.dump(data, f, ensure_ascii=False, indent=4)
        print(f"💾 Локально сохранен обновленный список ID в {local_config_path}")

        # Загружаем обновленный файл в B2, перезаписывая старый
        print(f"📤 Загружаем обновленный {config_key} в B2...")
        bucket.upload_local_file(local_config_path, config_key)
        print(f"✅ Успешно обновлен {config_key} в B2. Всего ID: {len(pub_ids)}")

        # Удаляем локальную копию после загрузки
        os.remove(local_config_path)

    except Exception as e:
        # Ловим любые ошибки при сохранении или загрузке
        print(f"⚠️ Не удалось сохранить или загрузить {config_key}: {e}")
        # Если локальный файл остался, удаляем его, чтобы не мешал при следующем запуске
        if os.path.exists(local_config_path):
            try:
                os.remove(local_config_path)
            except Exception as rm_err:
                print(f"  ⚠️ Не удалось удалить временный файл {local_config_path}: {rm_err}")

# ------------------------------------------------------------
# 3) Удаляем системные слова и сжимаем пустые строки
# ------------------------------------------------------------
def remove_system_phrases(text: str) -> str:
    """
    Очищает текст от стандартных заголовков ("Вступление:", "Основная часть:" и т.п.)
    и заменяет множественные переводы строк на двойной перевод строки.
    """
    if not isinstance(text, str): # Проверка на случай, если придет не строка
        return ""

    system_phrases = [
        "Вступление:", "Основная часть:", "Интересный факт:", "Заключение:",
        "🔥Вступление", "📚Основная часть", "🔍Интересный факт"
        # Можно добавить другие фразы при необходимости
    ]
    clean_text = text
    for phrase in system_phrases:
        # Используем регулярное выражение для удаления фразы без учета регистра
        # и с возможными пробелами/переводами строк вокруг нее.
        # re.escape экранирует спецсимволы в фразе, если они есть.
        clean_text = re.sub(r'^\s*' + re.escape(phrase) + r'\s*\n?', '', clean_text, flags=re.IGNORECASE | re.MULTILINE).strip()
        clean_text = re.sub(r'\n\s*' + re.escape(phrase) + r'\s*', '\n', clean_text, flags=re.IGNORECASE).strip()


    # Заменяем три и более перевода строки на два (сохраняет абзацы)
    clean_text = re.sub(r"\n\s*\n+", "\n\n", clean_text)
    # Удаляем возможные пустые строки в начале и конце текста
    return clean_text.strip()

# ------------------------------------------------------------
# 4) Публикация одного generation_id (Фото + Видео отдельно) - ОБНОВЛЕННАЯ ВЕРСИЯ
# ------------------------------------------------------------
async def publish_generation_id(gen_id: str, folder: str, published_ids: Set[str]) -> bool:
    """
    Скачивает JSON, PNG, Video.
    Если все 3 файла присутствуют:
    1. Отправляет Фото (PNG) без подписи.
    2. Отправляет Видео (MP4) с подписью (caption_text).
    3. Отправляет Отдельное сообщение с сарказмом.
    4. Отправляет Отдельное сообщение с опросом.
    Если хотя бы один из файлов (PNG или Video) отсутствует, пропускает публикацию.
    Возвращает True, если все необходимые медиа были успешно отправлены, иначе False.
    """
    print(f"⚙️ Обрабатываем gen_id: {gen_id} из папки {folder}")
    # Формируем ключи (пути) к файлам в B2
    json_file_key = f"{folder}{gen_id}.json"
    video_file_key = f"{folder}{gen_id}.mp4"
    png_file_key = f"{folder}{gen_id}.png"

    # Формируем локальные пути для скачивания
    local_json_path = os.path.join(DOWNLOAD_DIR, f"{gen_id}.json")
    local_video_path = os.path.join(DOWNLOAD_DIR, f"{gen_id}.mp4")
    local_png_path = os.path.join(DOWNLOAD_DIR, f"{gen_id}.png")

    # Флаги для отслеживания успешности скачивания
    json_downloaded = False
    video_downloaded = False
    png_downloaded = False

    # --- Скачивание файлов ---
    # 1. Скачиваем JSON (обязательно)
    try:
        print(f"📥 Скачиваем JSON: {json_file_key} -> {local_json_path}")
        os.makedirs(os.path.dirname(local_json_path), exist_ok=True)
        bucket.download_file_by_name(json_file_key).save_to(local_json_path)
        json_downloaded = True
        print(f"✅ JSON скачан: {local_json_path}")
    except FileNotPresent:
        print(f"⚠️ JSON файл не найден: {json_file_key}. Публикация невозможна.")
        return False # Без JSON продолжать нет смысла
    except B2Error as e:
        print(f"⚠️ Ошибка B2 SDK при скачивании JSON {json_file_key}: {e}")
        return False
    except Exception as e:
        print(f"⚠️ Неожиданная ошибка при скачивании JSON {json_file_key}: {e}")
        return False

    # 2. Пытаемся скачать PNG
    try:
        print(f"📥 Пытаемся скачать PNG: {png_file_key} -> {local_png_path}")
        os.makedirs(os.path.dirname(local_png_path), exist_ok=True)
        bucket.download_file_by_name(png_file_key).save_to(local_png_path)
        png_downloaded = True
        print(f"✅ PNG скачан: {local_png_path}")
    except FileNotPresent:
        print(f"⚠️ PNG файл не найден для {gen_id}.") # Важно, что это warning, а не info
    except B2Error as e:
        print(f"⚠️ Ошибка B2 SDK при скачивании PNG {png_file_key}: {e}")
    except Exception as e:
        print(f"⚠️ Неожиданная ошибка при скачивании PNG {png_file_key}: {e}")

    # 3. Пытаемся скачать видео
    try:
        print(f"📥 Пытаемся скачать видео: {video_file_key} -> {local_video_path}")
        os.makedirs(os.path.dirname(local_video_path), exist_ok=True)
        bucket.download_file_by_name(video_file_key).save_to(local_video_path)
        video_downloaded = True
        print(f"✅ Видео скачано: {local_video_path}")
    except FileNotPresent:
        print(f"⚠️ Видео файл не найден для {gen_id}.") # Важно, что это warning, а не info
    except B2Error as e:
        print(f"⚠️ Ошибка B2 SDK при скачивании видео {video_file_key}: {e}")
    except Exception as e:
        print(f"⚠️ Неожиданная ошибка при скачивании видео {video_file_key}: {e}")

    # --- НОВАЯ ПРОВЕРКА: Проверяем наличие ВСЕХ файлов (JSON уже проверен) ---
    if not (png_downloaded and video_downloaded):
        print(f"❌ Группа {gen_id} неполная (PNG: {png_downloaded}, Видео: {video_downloaded}). Публикация пропускается.")
        # Удаляем скачанные файлы, чтобы не мешались
        files_to_delete = [local_png_path, local_video_path, local_json_path]
        for file_path in files_to_delete:
            if os.path.exists(file_path):
                try: os.remove(file_path)
                except Exception as e: print(f"  ⚠️ Не удалось удалить временный файл {file_path}: {e}")
        return False # Возвращаем False, т.к. публикация не состоялась

    # --- Если все файлы на месте, продолжаем обработку JSON ---
    print(f"✅ Все файлы для {gen_id} (JSON, PNG, Видео) найдены. Продолжаем обработку...")
    caption_text = "" # Текст для подписи к ВИДЕО
    sarcasm_comment = "" # Текст сарказма
    poll_question = "" # Вопрос для опроса
    poll_options = [] # Варианты ответа для опроса
    json_processed_successfully = False

    try:
        # Открываем скачанный JSON
        with open(local_json_path, "r", encoding="utf-8") as f:
            data = json.load(f)

        # --- УЛУЧШЕННОЕ ИЗВЛЕЧЕНИЕ ТЕКСТА ПОДПИСИ (для видео) ---
        content_value = data.get("content")
        if isinstance(content_value, dict):
            caption_text = content_value.get("текст", "").strip()
            if not caption_text: print(f"⚠️ Ключ 'текст' отсутствует или пуст в объекте 'content' ({gen_id}).")
        elif isinstance(content_value, str) and content_value.strip():
            raw_content_str = content_value.strip()
            try:
                content_data = json.loads(raw_content_str)
                caption_text = content_data.get("текст", "").strip()
                if not caption_text: print(f"⚠️ Ключ 'текст' отсутствует или пуст во вложенном JSON поля 'content' ({gen_id}).")
            except json.JSONDecodeError:
                if raw_content_str not in ["{}"]:
                     print(f"ℹ️ Поле 'content' для {gen_id} не является валидным JSON, но содержит текст. Используем как есть.")
                     caption_text = raw_content_str
                else:
                     print(f"⚠️ Не удалось распарсить JSON из поля 'content' для {gen_id}, и строка пуста или '{{}}'. Подпись будет пустой.")
                     caption_text = ""
            except Exception as e:
                 print(f"⚠️ Неожиданная ошибка при обработке поля 'content' для {gen_id}: {e}")
                 caption_text = ""
        else:
            print(f"ℹ️ Поле 'content' пустое, отсутствует или имеет неожиданный тип в JSON для {gen_id}.")

        caption_text = remove_system_phrases(caption_text)
        print(f"DEBUG: Очищенный caption_text (для видео): '{caption_text}'")

        if len(caption_text) > 1024:
            print(f"⚠️ Подпись для видео {gen_id} слишком длинная ({len(caption_text)} симв). Обрезаем до 1020...")
            caption_text = caption_text[:1020] + "..."

        # --- УЛУЧШЕННОЕ ИЗВЛЕЧЕНИЕ САРКАЗМА ---
        sarcasm_data = data.get("sarcasm", {})
        comment_value = sarcasm_data.get("comment")
        if isinstance(comment_value, dict):
            sarcasm_comment = comment_value.get("комментарий", "").strip()
            if not sarcasm_comment: print(f"⚠️ Ключ 'комментарий' отсутствует или пуст в объекте 'sarcasm.comment' ({gen_id}).")
        elif isinstance(comment_value, str) and comment_value.strip():
            raw_comment_str = comment_value.strip()
            try:
                comment_data = json.loads(raw_comment_str)
                sarcasm_comment = comment_data.get("комментарий", "").strip()
                if not sarcasm_comment: print(f"⚠️ Ключ 'комментарий' отсутствует или пуст во вложенном JSON поля 'sarcasm.comment' ({gen_id}).")
            except json.JSONDecodeError:
                 if raw_comment_str not in ["{}"]:
                      print(f"ℹ️ Поле 'sarcasm.comment' для {gen_id} не является валидным JSON, но содержит текст. Используем как есть.")
                      sarcasm_comment = raw_comment_str
                 else:
                      print(f"⚠️ Не удалось распарсить JSON из поля 'sarcasm.comment' для {gen_id}, и строка пуста или '{{}}'. Сарказм будет пуст.")
                      sarcasm_comment = ""
            except Exception as e:
                 print(f"⚠️ Неожиданная ошибка при обработке 'sarcasm.comment' для {gen_id}: {e}")
                 sarcasm_comment = ""
        else:
             print(f"ℹ️ Поле 'sarcasm.comment' пустое, отсутствует или имеет неожиданный тип в JSON для {gen_id}.")
        print(f"DEBUG: Извлеченный sarcasm_comment: '{sarcasm_comment}'")

        # --- Извлечение опроса (без изменений) ---
        poll_data = sarcasm_data.get("poll", {})
        poll_question = poll_data.get("question", "").strip()
        poll_options = [str(opt).strip() for opt in poll_data.get("options", []) if str(opt).strip()]
        poll_question = poll_question[:300]
        poll_options = [opt[:100] for opt in poll_options][:10]
        print(f"DEBUG: Извлеченный poll_question: '{poll_question}'")
        print(f"DEBUG: Извлеченные poll_options: {poll_options}")

        json_processed_successfully = True
        print(f"DEBUG: json_processed_successfully установлен в True")

    except json.JSONDecodeError as e:
        print(f"❌ Ошибка декодирования основного JSON файла {local_json_path}: {e}")
        os.makedirs(ERROR_DIR, exist_ok=True)
        try:
            shutil.move(local_json_path, os.path.join(ERROR_DIR, os.path.basename(local_json_path)))
            print(f"📁 Поврежденный JSON {gen_id}.json перемещен в {ERROR_DIR}")
        except Exception as move_err:
             print(f"  ⚠️ Не удалось переместить поврежденный JSON: {move_err}")
        if os.path.exists(local_png_path): os.remove(local_png_path)
        if os.path.exists(local_video_path): os.remove(local_video_path)
        return False
    except Exception as e:
        print(f"❌ Непредвиденная ошибка при обработке JSON {gen_id}: {e}")
        if os.path.exists(local_json_path): os.remove(local_json_path)
        if os.path.exists(local_png_path): os.remove(local_png_path)
        if os.path.exists(local_video_path): os.remove(local_video_path)
        return False

    # --- Отправка в Telegram (Фото + Видео отдельно) ---
    os.makedirs(PROCESSED_DIR, exist_ok=True)
    sarcasm_sent = False
    poll_sent = False
    photo_sent_status = False # Устанавливаем в False, станет True при успехе
    video_sent_status = False # Устанавливаем в False, станет True при успехе

    # 1. Отправляем фото БЕЗ подписи
    # Эта проверка уже не нужна, т.к. мы вышли бы раньше, если бы png отсутствовал
    # if png_downloaded:
    png_file_handle = None
    try:
        print(f"✈️ Отправляем фото {gen_id}.png БЕЗ подписи...")
        png_file_handle = open(local_png_path, "rb")
        await bot.send_photo(
            chat_id=TELEGRAM_CHAT_ID,
            photo=png_file_handle,
            read_timeout=120, connect_timeout=120, write_timeout=120
        )
        photo_sent_status = True
        print("✅ Фото отправлено.")
    except Exception as e:
        print(f"❌ Ошибка при отправке фото для {gen_id}: {e}")
        photo_sent_status = False # Оставляем False
    finally:
        if png_file_handle: png_file_handle.close()

    # 2. Отправляем видео С подписью (только если фото отправилось успешно)
    # Эта проверка уже не нужна, т.к. мы вышли бы раньше, если бы video отсутствовал
    # if video_downloaded:
    if photo_sent_status: # Продолжаем только если фото ушло
        video_file_handle = None
        try:
            print(f"✈️ Отправляем видео {gen_id}.mp4 С подписью...")
            video_file_handle = open(local_video_path, "rb")
            await bot.send_video(
                chat_id=TELEGRAM_CHAT_ID,
                video=video_file_handle,
                caption=caption_text, # <-- Подпись здесь
                parse_mode="HTML",
                supports_streaming=True,
                read_timeout=120, connect_timeout=120, write_timeout=120
            )
            video_sent_status = True
            print("✅ Видео отправлено.")
        except Exception as e:
            print(f"❌ Ошибка при отправке видео для {gen_id}: {e}")
            video_sent_status = False # Оставляем False
        finally:
            if video_file_handle: video_file_handle.close()
    else:
        # Если фото не отправилось, видео тоже не отправляем и считаем неуспехом
        print(f"⚠️ Отправка видео для {gen_id} пропускается, т.к. фото не было отправлено.")
        video_sent_status = False


    # 3. Отправляем сарказм, если он есть
    # (можно добавить проверку на photo_sent_status и video_sent_status, если нужно)
    print(f"DEBUG: Проверка перед отправкой сарказма: json_processed_successfully={json_processed_successfully}, sarcasm_comment='{sarcasm_comment}'")
    if json_processed_successfully and sarcasm_comment:
        sarcasm_text_formatted = f"📜 <i>{sarcasm_comment}</i>"
        if len(sarcasm_text_formatted) > 4096:
            print(f"⚠️ Текст сарказма для {gen_id} слишком длинный. Обрезаем...")
            sarcasm_text_formatted = sarcasm_text_formatted[:4090] + "..."
        try:
            print(f"✈️ Отправляем сарказм для {gen_id}...")
            await bot.send_message(
                chat_id=TELEGRAM_CHAT_ID,
                text=sarcasm_text_formatted,
                parse_mode="HTML"
            )
            sarcasm_sent = True
            print(f"✅ Сарказм для {gen_id} отправлен.")
        except Exception as e:
            print(f"⚠️ Ошибка при отправке сарказма для {gen_id}: {e}")
    elif json_processed_successfully and not sarcasm_comment:
        print("DEBUG: Условие для отправки сарказма не выполнено (sarcasm_comment пуст).")
    elif not json_processed_successfully:
        print("DEBUG: Условие для отправки сарказма не выполнено (json_processed_successfully is False).")

    # 4. Отправляем опрос, если он валиден
    # (можно добавить проверку на photo_sent_status и video_sent_status, если нужно)
    print(f"DEBUG: Проверка перед отправкой опроса: json_processed_successfully={json_processed_successfully}, poll_question='{poll_question}', len(poll_options)={len(poll_options)}")
    if json_processed_successfully and poll_question and len(poll_options) >= 2:
        poll_question_formatted = f"🎭 {poll_question}"
        try:
            print(f"✈️ Отправляем опрос для {gen_id}...")
            await bot.send_poll(
                chat_id=TELEGRAM_CHAT_ID,
                question=poll_question_formatted,
                options=poll_options,
                is_anonymous=True
            )
            poll_sent = True
            print(f"✅ Опрос для {gen_id} отправлен.")
        except Exception as e:
            print(f"⚠️ Ошибка при отправке опроса для {gen_id}: {e}")
    elif json_processed_successfully:
         if not poll_question:
              print("DEBUG: Условие для отправки опроса не выполнено (poll_question пуст).")
         elif len(poll_options) < 2:
              print(f"DEBUG: Условие для отправки опроса не выполнено (опций: {len(poll_options)} < 2).")
    elif not json_processed_successfully:
         print("DEBUG: Условие для отправки опроса не выполнено (json_processed_successfully is False).")

    # --- Завершение и обработка файлов ---
    # Успех определяется отправкой ВСЕХ необходимых медиа (фото И видео)
    success = photo_sent_status and video_sent_status

    if success:
        print(f"✅ Успешная публикация основного контента (медиа) для {gen_id}.")
        published_ids.add(gen_id)
        save_published_ids(published_ids)

        files_to_move = [local_png_path, local_video_path, local_json_path]
        for file_path in files_to_move:
            if os.path.exists(file_path):
                try:
                    destination_path = os.path.join(PROCESSED_DIR, os.path.basename(file_path))
                    shutil.move(file_path, destination_path)
                    print(f"  📁 Файл {os.path.basename(file_path)} перемещен в {PROCESSED_DIR}")
                except Exception as e:
                    print(f"  ⚠️ Не удалось переместить файл {os.path.basename(file_path)} в processed: {e}")
    else:
        # Если основные медиа не были отправлены (из-за ошибки или неполной группы)
        # Сообщение об этом уже было выведено выше (либо "Группа неполная", либо "Ошибка при отправке...")
        if json_processed_successfully: # Добавляем деталей, если JSON был обработан
             print(f"⚠️ Отправка основного контента для {gen_id} НЕ УДАЛАСЬ. ID не добавлен в опубликованные.")
             print(f"   (Статус отправки: Фото - {photo_sent_status}, Видео - {video_sent_status}, Сарказм - {'Да' if sarcasm_sent else 'Нет'}, Опрос - {'Да' if poll_sent else 'Нет'})")
             print(f"   🗑️ Удаляем локальные файлы для {gen_id}, чтобы избежать дублирования при след. запуске.")

        # Удаляем все локальные файлы (JSON, PNG, Video), которые могли остаться в download
        files_to_delete = [local_png_path, local_video_path, local_json_path]
        for file_path in files_to_delete:
             if os.path.exists(file_path) and PROCESSED_DIR not in os.path.dirname(file_path) and ERROR_DIR not in os.path.dirname(file_path):
                  try:
                       os.remove(file_path)
                  except Exception as e:
                       print(f"  ⚠️ Не удалось удалить временный файл {file_path}: {e}")

    return success

# ------------------------------------------------------------
# 5) Основная логика (поиск и публикация)
# ------------------------------------------------------------
async def main():
    """
    Главная асинхронная функция скрипта.
    """
    print("\n" + "="*50)
    print("🚀 Запуск скрипта публикации B2 -> Telegram (v6: Пропуск неполных групп)") # <-- Обновлено название
    print("="*50)

    print("🧹 Очищаем/создаем локальные папки для скачивания и обработки...")
    shutil.rmtree(DOWNLOAD_DIR, ignore_errors=True)
    os.makedirs(DOWNLOAD_DIR, exist_ok=True)
    os.makedirs(PROCESSED_DIR, exist_ok=True)
    os.makedirs(ERROR_DIR, exist_ok=True)
    print(f"✅ Локальные папки готовы.")

    published_ids = load_published_ids()

    folders_to_scan = ["444/", "555/", "666/"]
    print(f"📂 Папки в бакете '{S3_BUCKET_NAME}' для сканирования: {', '.join(folders_to_scan)}")

    unpublished_items: List[Tuple[str, str]] = []

    for folder in folders_to_scan:
        print(f"\n🔎 Сканируем папку: {folder}")
        try:
            ls_result = bucket.ls(folder_to_list=folder, recursive=False)
            gen_ids_in_folder = set()
            for file_version, _folder_name in ls_result:
                file_name = file_version.file_name
                if file_name.startswith(folder) and file_name.endswith(".json"):
                     if os.path.dirname(file_name.replace(folder, '', 1)) == '':
                         base_name = os.path.basename(file_name)
                         gen_id = os.path.splitext(base_name)[0]
                         if re.fullmatch(r"\d{8}-\d{4}", gen_id):
                              gen_ids_in_folder.add(gen_id)
                         else:
                              print(f"   ⚠️ Пропускаем файл с некорректным именем ID: {file_name}")

            print(f"   ℹ️ Найдено {len(gen_ids_in_folder)} уникальных ID формата YYYYMMDD-HHMM в {folder}")

            new_ids = gen_ids_in_folder - published_ids
            if new_ids:
                print(f"   ✨ Найдено {len(new_ids)} новых (неопубликованных) ID в {folder}.")
                for gen_id in new_ids:
                    unpublished_items.append((gen_id, folder))
            else:
                print(f"   ✅ Нет новых ID для публикации в {folder}.")

        except B2Error as e:
            print(f"   ❌ Ошибка B2 SDK при сканировании папки {folder}: {e}")
        except Exception as e:
            print(f"   ❌ Неожиданная ошибка при сканировании папки {folder}: {e}")

    if unpublished_items:
        print(f"\n⏳ Всего найдено {len(unpublished_items)} неопубликованных групп для публикации.")
        unpublished_items.sort(key=lambda item: item[0])
        print("   🔢 Сортировка по дате и времени (gen_id)...")

        gen_id_to_publish, folder_to_publish = unpublished_items[0]

        print(f"\n🎯 Выбрана самая старая группа для публикации: ID={gen_id_to_publish} из папки {folder_to_publish}")
        print("-" * 50)

        success = await publish_generation_id(gen_id_to_publish, folder_to_publish, published_ids)

        print("-" * 50)
        if success:
            print(f"✅ Успешно опубликована группа {gen_id_to_publish}.")
        else:
            # Сообщение о причине неудачи (неполная группа или ошибка отправки) выводится внутри publish_generation_id
            print(f"ℹ️ Публикация группы {gen_id_to_publish} не была завершена успешно (подробности выше).")

    else:
        print("\n🎉 Нет новых групп для публикации во всех отсканированных папках.")

    print("\n🏁 Скрипт завершил работу.")
    print("="*50 + "\n")

# Точка входа в программу
if __name__ == "__main__":
    try:
        asyncio.run(main())
    except RuntimeError as e:
        print(f"\n💥 Критическая ошибка: {e}")
    except Exception as e:
         print(f"\n💥 Непредвиденная критическая ошибка: {e}")

