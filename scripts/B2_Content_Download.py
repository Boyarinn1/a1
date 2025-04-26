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
# 4) Публикация одного generation_id (альбом + отдельные сообщения) - ОБНОВЛЕННАЯ ВЕРСИЯ
# ------------------------------------------------------------
async def publish_generation_id(gen_id: str, folder: str, published_ids: Set[str]) -> bool:
    """
    Публикует контент для одного generation_id:
    1. Медиагруппа (PNG + MP4) с подписью из content.текст.
    2. Отдельное сообщение с сарказмом (из sarcasm.comment.комментарий).
    3. Отдельное сообщение с опросом (из sarcasm.poll).
    Возвращает True, если медиагруппа была успешно отправлена, иначе False.
    """
    print(f"⚙️ Обрабатываем gen_id: {gen_id} из папки {folder}")
    # Формируем ключи (пути) к файлам в B2
    json_file_key = f"{folder}{gen_id}.json"
    video_file_key = f"{folder}{gen_id}.mp4"
    png_file_key = f"{folder}{gen_id}.png" # Ключ для PNG файла

    # Формируем локальные пути для скачивания
    local_json_path = os.path.join(DOWNLOAD_DIR, f"{gen_id}.json")
    local_video_path = os.path.join(DOWNLOAD_DIR, f"{gen_id}.mp4")
    local_png_path = os.path.join(DOWNLOAD_DIR, f"{gen_id}.png") # Локальный путь для PNG

    # Флаги для отслеживания успешности скачивания
    json_downloaded = False
    video_downloaded = False
    png_downloaded = False

    # --- Скачивание файлов ---
    # 1. Скачиваем JSON (обязательно)
    try:
        print(f"📥 Скачиваем JSON: {json_file_key} -> {local_json_path}")
        os.makedirs(os.path.dirname(local_json_path), exist_ok=True) # Создаем папку, если ее нет
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

    # 2. Пытаемся скачать PNG (необязательно, но нужно для альбома)
    try:
        print(f"📥 Пытаемся скачать PNG: {png_file_key} -> {local_png_path}")
        os.makedirs(os.path.dirname(local_png_path), exist_ok=True)
        bucket.download_file_by_name(png_file_key).save_to(local_png_path)
        png_downloaded = True
        print(f"✅ PNG скачан: {local_png_path}")
    except FileNotPresent:
        print(f"ℹ️ PNG файл не найден для {gen_id}. Альбом будет отправлен без него, если есть видео.")
    except B2Error as e:
        print(f"⚠️ Ошибка B2 SDK при скачивании PNG {png_file_key}: {e}")
    except Exception as e:
        print(f"⚠️ Неожиданная ошибка при скачивании PNG {png_file_key}: {e}")

    # 3. Пытаемся скачать видео (необязательно, но нужно для альбома)
    try:
        print(f"📥 Пытаемся скачать видео: {video_file_key} -> {local_video_path}")
        os.makedirs(os.path.dirname(local_video_path), exist_ok=True)
        bucket.download_file_by_name(video_file_key).save_to(local_video_path)
        video_downloaded = True
        print(f"✅ Видео скачано: {local_video_path}")
    except FileNotPresent:
        print(f"ℹ️ Видео файл не найден для {gen_id}. Альбом будет отправлен без него, если есть PNG.")
    except B2Error as e:
        print(f"⚠️ Ошибка B2 SDK при скачивании видео {video_file_key}: {e}")
    except Exception as e:
        print(f"⚠️ Неожиданная ошибка при скачивании видео {video_file_key}: {e}")

    # --- Обработка JSON и подготовка контента ---
    caption_text = "" # Текст для подписи к альбому
    sarcasm_comment = "" # Текст сарказма
    poll_question = "" # Вопрос для опроса
    poll_options = [] # Варианты ответа для опроса
    json_processed_successfully = False # Флаг успешной обработки JSON

    try:
        # Открываем скачанный JSON
        with open(local_json_path, "r", encoding="utf-8") as f:
            data = json.load(f)

        # Извлекаем и парсим текст из поля "content"
        raw_content_str = data.get("content", "").strip()
        if raw_content_str:
            try:
                # Поле content само содержит строку JSON, парсим ее
                content_data = json.loads(raw_content_str)
                # Берем текст по ключу "текст"
                caption_text = content_data.get("текст", "").strip()
                if not caption_text:
                     print(f"⚠️ Ключ 'текст' отсутствует или пуст во вложенном JSON поля 'content' ({gen_id}).")
            except json.JSONDecodeError:
                # Если не удалось распарсить вложенный JSON
                print(f"⚠️ Не удалось распарсить JSON из поля 'content' для {gen_id}. Подпись будет пустой.")
                caption_text = "" # Оставляем подпись пустой
            except Exception as e:
                 print(f"⚠️ Неожиданная ошибка при обработке поля 'content' для {gen_id}: {e}")
                 caption_text = ""
        else:
            print(f"ℹ️ Поле 'content' пустое или отсутствует в JSON для {gen_id}.")

        # Очищаем текст подписи от системных фраз ("Вступление:" и т.д.)
        caption_text = remove_system_phrases(caption_text)

        # Обрезаем подпись до 1024 символов (лимит Telegram для медиагрупп)
        if len(caption_text) > 1024:
            print(f"⚠️ Подпись для {gen_id} слишком длинная ({len(caption_text)} симв). Обрезаем до 1020...")
            caption_text = caption_text[:1020] + "..." # Обрезаем и добавляем многоточие

        # Извлекаем сарказм из поля "sarcasm.comment"
        raw_sarcasm_comment_str = data.get("sarcasm", {}).get("comment", "").strip()
        if raw_sarcasm_comment_str:
             try:
                 # Поле comment тоже может содержать вложенный JSON
                 sarcasm_data = json.loads(raw_sarcasm_comment_str)
                 sarcasm_comment = sarcasm_data.get("комментарий", "").strip()
                 if not sarcasm_comment:
                      print(f"⚠️ Ключ 'комментарий' отсутствует или пуст во вложенном JSON поля 'sarcasm.comment' ({gen_id}).")
             except json.JSONDecodeError:
                  print(f"⚠️ Не удалось распарсить JSON из поля 'sarcasm.comment' для {gen_id}. Используем как есть (если это текст).")
                  # Проверяем, является ли исходная строка текстом, а не просто фигурными скобками
                  if raw_sarcasm_comment_str.strip() not in ["{}", ""]:
                       sarcasm_comment = raw_sarcasm_comment_str
                  else:
                       sarcasm_comment = ""
             except Exception as e:
                  print(f"⚠️ Неожиданная ошибка при обработке 'sarcasm.comment' для {gen_id}: {e}")
                  sarcasm_comment = ""

        # Извлекаем данные для опроса из поля "sarcasm.poll"
        poll_data = data.get("sarcasm", {}).get("poll", {})
        poll_question = poll_data.get("question", "").strip()
        # Получаем опции, преобразуем в строку, убираем пробелы и пустые строки
        poll_options = [str(opt).strip() for opt in poll_data.get("options", []) if str(opt).strip()]

        # Ограничиваем длину вопроса и опций (лимиты Telegram)
        poll_question = poll_question[:300]
        poll_options = [opt[:100] for opt in poll_options][:10] # Не более 10 опций

        json_processed_successfully = True # JSON успешно обработан

    except json.JSONDecodeError as e:
        # Ошибка при чтении основного JSON файла
        print(f"❌ Ошибка декодирования основного JSON файла {local_json_path}: {e}")
        # Перемещаем битый JSON в папку ошибок
        os.makedirs(ERROR_DIR, exist_ok=True)
        try:
            shutil.move(local_json_path, os.path.join(ERROR_DIR, os.path.basename(local_json_path)))
            print(f"📁 Поврежденный JSON {gen_id}.json перемещен в {ERROR_DIR}")
        except Exception as move_err:
             print(f"  ⚠️ Не удалось переместить поврежденный JSON: {move_err}")
        # Удаляем скачанные медиафайлы, если они есть, т.к. JSON поврежден
        if os.path.exists(local_png_path): os.remove(local_png_path)
        if os.path.exists(local_video_path): os.remove(local_video_path)
        return False # Выход, т.к. основная информация недоступна
    except Exception as e:
        # Любая другая ошибка при обработке JSON
        print(f"❌ Непредвиденная ошибка при обработке JSON {gen_id}: {e}")
        # Попытаемся удалить скачанные файлы, чтобы не мешались
        if os.path.exists(local_json_path): os.remove(local_json_path)
        if os.path.exists(local_png_path): os.remove(local_png_path)
        if os.path.exists(local_video_path): os.remove(local_video_path)
        return False

    # --- Отправка в Telegram ---
    os.makedirs(PROCESSED_DIR, exist_ok=True) # Убедимся, что папка для обработанных существует
    album_sent = False # Флаг успешной отправки альбома
    sarcasm_sent = False # Флаг успешной отправки сарказма
    poll_sent = False # Флаг успешной отправки опроса

    # 1. Отправляем медиагруппу (альбом)
    media_items = []
    png_file_handle = None
    video_file_handle = None

    try:
        # Подготавливаем элементы для медиагруппы
        if png_downloaded:
            png_file_handle = open(local_png_path, "rb")
            # Добавляем фото. Подпись добавляется только к первому элементу (или к фото, если оно первое)
            media_items.append(InputMediaPhoto(png_file_handle, caption=caption_text if not media_items else "", parse_mode="HTML"))
        if video_downloaded:
            video_file_handle = open(local_video_path, "rb")
             # Добавляем видео. Добавляем подпись, только если фото не было и это первый элемент
            media_items.append(InputMediaVideo(video_file_handle, caption=caption_text if not media_items else "", parse_mode="HTML", supports_streaming=True))

        # Отправляем медиагруппу, если есть хотя бы один элемент (PNG или Видео)
        if media_items:
            print(f"✈️ Пытаемся отправить медиагруппу ({'PNG' if png_downloaded else ''}{'+' if png_downloaded and video_downloaded else ''}{'MP4' if video_downloaded else ''}) для {gen_id}...")
            await bot.send_media_group(
                chat_id=TELEGRAM_CHAT_ID,
                media=media_items,
                read_timeout=120, # Увеличиваем таймауты для больших файлов
                connect_timeout=120,
                write_timeout=120
            )
            album_sent = True
            print(f"✅ Медиагруппа для {gen_id} отправлена.")
        else:
            # Сюда попадаем, если не скачался ни PNG, ни Видео
            print(f"⚠️ Не удалось скачать ни PNG, ни Видео для {gen_id}. Медиагруппа не будет отправлена.")
            # Если был текст подписи, он тоже не будет отправлен, т.к. нет медиа
            if caption_text:
                 print(f"   (Текст подписи '{caption_text[:50]}...' не будет отправлен без медиа)")

    except Exception as e:
        print(f"❌ Ошибка при отправке медиагруппы для {gen_id}: {e}")
        # Оставляем album_sent = False
    finally:
        # Важно: закрываем файловые дескрипторы, даже если была ошибка
        if png_file_handle:
            png_file_handle.close()
        if video_file_handle:
            video_file_handle.close()

    # 2. Отправляем сарказм, если он есть (независимо от альбома)
    # Проверяем, что JSON был обработан и есть текст сарказма
    if json_processed_successfully and sarcasm_comment:
        sarcasm_text_formatted = f"📜 <i>{sarcasm_comment}</i>" # Форматируем курсивом
        # Обрезаем, если слишком длинный (лимит Telegram 4096)
        if len(sarcasm_text_formatted) > 4096:
            print(f"⚠️ Текст сарказма для {gen_id} слишком длинный. Обрезаем...")
            sarcasm_text_formatted = sarcasm_text_formatted[:4090] + "..."
        try:
            print(f"✈️ Отправляем сарказм для {gen_id}...")
            await bot.send_message(
                chat_id=TELEGRAM_CHAT_ID,
                text=sarcasm_text_formatted,
                parse_mode="HTML" # Используем HTML для курсива
            )
            sarcasm_sent = True
            print(f"✅ Сарказм для {gen_id} отправлен.")
        except Exception as e:
            print(f"⚠️ Ошибка при отправке сарказма для {gen_id}: {e}")

    # 3. Отправляем опрос, если он валиден (независимо от альбома)
    # Проверяем, что JSON обработан, есть вопрос и минимум 2 опции
    if json_processed_successfully and poll_question and len(poll_options) >= 2:
        poll_question_formatted = f"🎭 {poll_question}" # Добавляем эмодзи к вопросу
        try:
            print(f"✈️ Отправляем опрос для {gen_id}...")
            await bot.send_poll(
                chat_id=TELEGRAM_CHAT_ID,
                question=poll_question_formatted,
                options=poll_options,
                is_anonymous=True # Анонимный опрос
            )
            poll_sent = True
            print(f"✅ Опрос для {gen_id} отправлен.")
        except Exception as e:
            print(f"⚠️ Ошибка при отправке опроса для {gen_id}: {e}")
    elif json_processed_successfully and poll_question and len(poll_options) < 2:
        # Если вопрос есть, а опций мало
        print(f"ℹ️ Опрос для {gen_id} имеет вопрос '{poll_question[:50]}...', но меньше 2 валидных опций. Пропускаем.")

    # --- Завершение и обработка файлов ---
    # Успех определяется отправкой основного контента - альбома (хотя бы с одним медиа)
    success = album_sent

    if success:
        print(f"✅ Успешная публикация основного контента (альбома) для {gen_id}.")
        # Добавляем ID в множество опубликованных и сохраняем в B2
        published_ids.add(gen_id)
        save_published_ids(published_ids) # Функция сохранения вызовет print внутри себя
        # print(f"📝 ID {gen_id} добавлен в список опубликованных.") # Этот print дублируется в save_published_ids

        # Перемещаем все связанные скачанные файлы в папку processed
        files_to_move = [local_png_path, local_video_path, local_json_path]
        for file_path in files_to_move:
            if os.path.exists(file_path): # Перемещаем только если файл существует (был скачан)
                try:
                    # Формируем целевой путь в папке processed
                    destination_path = os.path.join(PROCESSED_DIR, os.path.basename(file_path))
                    shutil.move(file_path, destination_path)
                    print(f"  📁 Файл {os.path.basename(file_path)} перемещен в {PROCESSED_DIR}")
                except Exception as e:
                    print(f"  ⚠️ Не удалось переместить файл {os.path.basename(file_path)} в processed: {e}")
                    # Можно добавить логику повторной попытки или оставить файл в download

    else:
        # Если альбом не был отправлен (ни PNG, ни Видео не скачались или ошибка отправки)
        if json_processed_successfully: # Но JSON был обработан
             print(f"⚠️ Основной контент (альбом) для {gen_id} не был отправлен. ID не добавлен в опубликованные.")
             # Выводим статус отправки доп. сообщений для информации
             print(f"   (Статус отправки: Сарказм - {'Да' if sarcasm_sent else 'Нет'}, Опрос - {'Да' if poll_sent else 'Нет'})")
             # Удаляем локальные файлы, чтобы не пытаться опубликовать их снова при следующем запуске
             # (если только не хотим реализовать механизм повторных попыток)
             print(f"   🗑️ Удаляем локальные файлы для {gen_id}, чтобы избежать дублирования при след. запуске.")
        # else: # Если была ошибка обработки JSON, файлы уже удалены или перемещены в errors

        # Удаляем все локальные файлы (JSON, PNG, Video), которые могли остаться в download
        files_to_delete = [local_png_path, local_video_path, local_json_path]
        for file_path in files_to_delete:
             # Проверяем, что файл существует и не находится в processed или errors
             if os.path.exists(file_path) and PROCESSED_DIR not in os.path.dirname(file_path) and ERROR_DIR not in os.path.dirname(file_path):
                  try:
                       os.remove(file_path)
                       # print(f"   🗑️ Удален временный файл: {file_path}") # Можно раскомментировать для отладки
                  except Exception as e:
                       print(f"  ⚠️ Не удалось удалить временный файл {file_path}: {e}")

    return success # Возвращаем True только если альбом был отправлен

# ------------------------------------------------------------
# 5) Основная логика (поиск и публикация)
# ------------------------------------------------------------
async def main():
    """
    Главная асинхронная функция скрипта.
    """
    print("\n" + "="*50)
    print("🚀 Запуск скрипта публикации B2 -> Telegram (v2: Альбом)")
    print("="*50)

    # Очищаем/создаем локальные папки перед началом работы
    print("🧹 Очищаем/создаем локальные папки для скачивания и обработки...")
    shutil.rmtree(DOWNLOAD_DIR, ignore_errors=True) # Удаляем старую папку download, игнорируя ошибки
    os.makedirs(DOWNLOAD_DIR, exist_ok=True)    # Создаем пустую папку download
    os.makedirs(PROCESSED_DIR, exist_ok=True) # Убеждаемся, что папка processed существует
    os.makedirs(ERROR_DIR, exist_ok=True)     # Убеждаемся, что папка errors существует
    print(f"✅ Локальные папки готовы.")

    # Загружаем ID уже опубликованных постов из B2
    published_ids = load_published_ids()

    # Папки в B2 для сканирования (можно изменить или добавить)
    folders_to_scan = ["444/", "555/", "666/"]
    print(f"📂 Папки в бакете '{S3_BUCKET_NAME}' для сканирования: {', '.join(folders_to_scan)}")

    # Собираем все неопубликованные gen_id из всех папок
    # Список будет содержать кортежи: (gen_id, folder_path)
    unpublished_items: List[Tuple[str, str]] = []

    # Сканируем каждую указанную папку
    for folder in folders_to_scan:
        print(f"\n🔎 Сканируем папку: {folder}")
        try:
            # Получаем список файлов в папке (рекурсивно, если нужно)
            # `recursive=False` - ищем только в корне папки folder
            # `recursive=True` - ищем в folder и всех ее подпапках
            ls_result = bucket.ls(folder_to_list=folder, recursive=False, show_versions=False) # show_versions=False - не показывать старые версии файлов

            gen_ids_in_folder = set() # Множество для хранения уникальных ID в этой папке
            # Обрабатываем результат листинга
            for file_version, _folder_name in ls_result:
                file_name = file_version.file_name # Полный путь к файлу в бакете

                # Нас интересуют только JSON файлы непосредственно в этой папке
                if file_name.startswith(folder) and file_name.endswith(".json"):
                     # Проверяем, что файл находится именно в этой папке, а не в подпапке (если recursive=False)
                     # Пример: для папки '444/', файл '444/subdir/file.json' будет проигнорирован
                     if os.path.dirname(file_name.replace(folder, '', 1)) == '':
                         # Извлекаем имя файла без пути и расширения - это и есть gen_id
                         base_name = os.path.basename(file_name)
                         gen_id = os.path.splitext(base_name)[0]
                         # Проверяем формат ID (YYYYMMDD-HHMM) с помощью регулярного выражения
                         if re.fullmatch(r"\d{8}-\d{4}", gen_id):
                              gen_ids_in_folder.add(gen_id)
                         else:
                              print(f"   ⚠️ Пропускаем файл с некорректным именем ID: {file_name}")

            print(f"   ℹ️ Найдено {len(gen_ids_in_folder)} уникальных ID формата YYYYMMDD-HHMM в {folder}")

            # Отбираем только те ID из этой папки, которых нет в списке опубликованных
            new_ids = gen_ids_in_folder - published_ids
            if new_ids:
                print(f"   ✨ Найдено {len(new_ids)} новых (неопубликованных) ID в {folder}.")
                # Добавляем новые ID и папку, где они найдены, в общий список
                for gen_id in new_ids:
                    unpublished_items.append((gen_id, folder))
            else:
                print(f"   ✅ Нет новых ID для публикации в {folder}.")

        except B2Error as e:
            # Ловим ошибки B2 SDK при листинге папки
            print(f"   ❌ Ошибка B2 SDK при сканировании папки {folder}: {e}")
        except Exception as e:
            # Ловим прочие ошибки при сканировании
            print(f"   ❌ Неожиданная ошибка при сканировании папки {folder}: {e}")
            # Продолжаем сканировать другие папки

    # Если после сканирования всех папок найдены неопубликованные элементы
    if unpublished_items:
        print(f"\n⏳ Всего найдено {len(unpublished_items)} неопубликованных групп для публикации.")

        # Сортируем все найденные неопубликованные элементы по gen_id (хронологически)
        # Стандартная сортировка строк 'YYYYMMDD-HHMM' работает правильно
        unpublished_items.sort(key=lambda item: item[0])
        print("   🔢 Сортировка по дате и времени (gen_id)...")

        # Берем самый старый неопубликованный элемент (первый после сортировки)
        gen_id_to_publish, folder_to_publish = unpublished_items[0]

        print(f"\n🎯 Выбрана самая старая группа для публикации: ID={gen_id_to_publish} из папки {folder_to_publish}")
        print("-" * 50)

        # Пытаемся опубликовать выбранную группу, передавая текущий список published_ids
        # Функция publish_generation_id сама обновит published_ids и сохранит его в B2 в случае успеха
        success = await publish_generation_id(gen_id_to_publish, folder_to_publish, published_ids)

        print("-" * 50)
        if success:
            # Если публикация (отправка альбома) прошла успешно
            print(f"✅ Успешно опубликована группа {gen_id_to_publish}.")
            # Список published_ids уже обновлен и сохранен внутри publish_generation_id
        else:
            # Если основная публикация (альбом) не удалась
            print(f"⚠️ Не удалось опубликовать основной контент группы {gen_id_to_publish}. Подробности см. выше.")
            # В этом случае ID не добавляется в published_ids, и скрипт попробует опубликовать
            # эту же группу при следующем запуске (если проблема не в самих файлах).

    else:
        # Если не найдено ни одного нового ID во всех папках
        print("\n🎉 Нет новых групп для публикации во всех отсканированных папках.")

    print("\n🏁 Скрипт завершил работу.")
    print("="*50 + "\n")

# Точка входа в программу
if __name__ == "__main__":
    # Запускаем основную асинхронную функцию main()
    try:
        asyncio.run(main())
    except RuntimeError as e:
        # Ловим ошибки уровня приложения (например, отсутствие переменных окружения)
        print(f"\n💥 Критическая ошибка: {e}")
    except Exception as e:
        # Ловим любые другие непредвиденные ошибки на верхнем уровне
         print(f"\n💥 Непредвиденная критическая ошибка: {e}")

