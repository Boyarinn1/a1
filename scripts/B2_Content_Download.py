#!/usr/bin/env python3
import os
import json
import asyncio
import shutil
import re
from typing import Set, List, Tuple, Any # Добавлен Any
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
# 4) Публикация одного generation_id (Альбом: Фото + Видео) - ИЗМЕНЕНА
# ------------------------------------------------------------
async def publish_generation_id(gen_id: str, folder: str, published_ids: Set[str]) -> bool:
    """
    Скачивает JSON, PNG, Video.
    Если все 3 файла присутствуют:
    1. Отправляет медиагруппу (Фото + Видео) с подписью у Фото.
    2. Отправляет Отдельное сообщение с сарказмом.
    3. Отправляет Отдельное сообщение с опросом.
    Если хотя бы один из файлов (JSON, PNG или Video) отсутствует,
    пропускает публикацию ПОЛНОСТЬЮ и возвращает False.
    Возвращает True, если медиагруппа была успешно отправлена, иначе False.
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
        print(f"❌ Группа {gen_id} неполная (отсутствует JSON). Публикация пропускается.")
        if os.path.exists(local_png_path): os.remove(local_png_path)
        if os.path.exists(local_video_path): os.remove(local_video_path)
        return False
    except B2Error as e:
        print(f"⚠️ Ошибка B2 SDK при скачивании JSON {json_file_key}: {e}")
        if os.path.exists(local_png_path): os.remove(local_png_path)
        if os.path.exists(local_video_path): os.remove(local_video_path)
        return False
    except Exception as e:
        print(f"⚠️ Неожиданная ошибка при скачивании JSON {json_file_key}: {e}")
        if os.path.exists(local_png_path): os.remove(local_png_path)
        if os.path.exists(local_video_path): os.remove(local_video_path)
        return False

    # 2. Пытаемся скачать PNG
    try:
        print(f"📥 Пытаемся скачать PNG: {png_file_key} -> {local_png_path}")
        os.makedirs(os.path.dirname(local_png_path), exist_ok=True)
        bucket.download_file_by_name(png_file_key).save_to(local_png_path)
        png_downloaded = True
        print(f"✅ PNG скачан: {local_png_path}")
    except FileNotPresent:
        print(f"⚠️ PNG файл не найден для {gen_id}.")
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
        print(f"⚠️ Видео файл не найден для {gen_id}.")
    except B2Error as e:
        print(f"⚠️ Ошибка B2 SDK при скачивании видео {video_file_key}: {e}")
    except Exception as e:
        print(f"⚠️ Неожиданная ошибка при скачивании видео {video_file_key}: {e}")

    # --- ПРОВЕРКА НА ПОЛНОТУ ГРУППЫ (PNG и VIDEO) ---
    if not (png_downloaded and video_downloaded):
        print(f"❌ Группа {gen_id} неполная (PNG: {png_downloaded}, Видео: {video_downloaded}). Публикация пропускается ПОЛНОСТЬЮ.")
        files_to_delete = [local_png_path, local_video_path, local_json_path]
        for file_path in files_to_delete:
            if os.path.exists(file_path):
                try: os.remove(file_path)
                except Exception as e: print(f"  ⚠️ Не удалось удалить временный файл {file_path}: {e}")
        return False # Возвращаем False, т.к. публикация не состоялась

    # --- Если все файлы на месте, продолжаем обработку и отправку ---
    print(f"✅ Все файлы для {gen_id} (JSON, PNG, Видео) найдены. Продолжаем обработку и отправку...")
    caption_text = "" # Текст для подписи к ФОТО
    sarcasm_comment = "" # Текст сарказма
    poll_question = "" # Вопрос для опроса
    poll_options = [] # Варианты ответа для опроса
    json_processed_successfully = False
    album_sent = False # Флаг для отслеживания успеха отправки медиагруппы
    sarcasm_sent = False
    poll_sent = False
    success = False # Инициализируем успех как False

    try:
        # --- Обработка JSON ---
        with open(local_json_path, "r", encoding="utf-8") as f:
            data = json.load(f)

        # --- ИЗВЛЕЧЕНИЕ ТЕКСТА ПОДПИСИ (УЛУЧШЕНО: "текст", "content", "post") ---
        content_value = data.get("content")
        possible_text_keys = ["текст", "content"]
        found_text = None
        content_data = None

        if isinstance(content_value, dict):
            content_data = content_value
        elif isinstance(content_value, str) and content_value.strip():
            raw_content_str = content_value.strip()
            try:
                content_data = json.loads(raw_content_str)
            except json.JSONDecodeError:
                if raw_content_str not in ["{}"]:
                     print(f"ℹ️ Поле 'content' для {gen_id} не является валидным JSON, но содержит текст. Используем как есть.")
                     found_text = raw_content_str
                else:
                     print(f"⚠️ Не удалось распарсить JSON из поля 'content' для {gen_id}, и строка пуста или '{{}}'. Подпись будет пустой.")
            except Exception as e:
                 print(f"⚠️ Неожиданная ошибка при обработке поля 'content' для {gen_id}: {e}")
        else:
            print(f"ℹ️ Поле 'content' пустое, отсутствует или имеет неожиданный тип в JSON для {gen_id}.")

        if content_data is not None:
            post_list = content_data.get("post")
            if isinstance(post_list, list):
                post_texts = []
                for item in post_list:
                    if isinstance(item, dict) and len(item) == 1:
                        post_texts.append(list(item.values())[0])
                if post_texts:
                    found_text = "\n\n".join(filter(None, post_texts))
                    print(f"ℹ️ Текст извлечен из структуры 'post' для {gen_id}.")

            if found_text is None:
                for key in possible_text_keys:
                    if key in content_data:
                        found_text = content_data[key]
                        break
                if found_text is None:
                     print(f"⚠️ Ни один из ключей {possible_text_keys} или структура 'post' не найдены в 'content' ({gen_id}).")

        caption_text = found_text.strip() if isinstance(found_text, str) else ""
        caption_text = remove_system_phrases(caption_text)
        print(f"DEBUG: Очищенный caption_text (для фото): '{caption_text}'") # <-- Уточнено

        if len(caption_text) > 1024:
            print(f"⚠️ Подпись для фото {gen_id} слишком длинная ({len(caption_text)} симв). Обрезаем до 1020...")
            caption_text = caption_text[:1020] + "..."

        # --- ИЗВЛЕЧЕНИЕ САРКАЗМА (с проверкой ключей "комментарий" и "sarcastic_comment") ---
        sarcasm_data = data.get("sarcasm", {})
        comment_value = sarcasm_data.get("comment")
        possible_comment_keys = ["комментарий", "sarcastic_comment"]
        found_comment = None
        comment_data_parsed = None

        if isinstance(comment_value, dict):
             comment_data_parsed = comment_value
        elif isinstance(comment_value, str) and comment_value.strip():
            raw_comment_str = comment_value.strip()
            try:
                comment_data_parsed = json.loads(raw_comment_str)
            except json.JSONDecodeError:
                 if raw_comment_str not in ["{}"]:
                      print(f"ℹ️ Поле 'sarcasm.comment' для {gen_id} не является валидным JSON, но содержит текст. Используем как есть.")
                      found_comment = raw_comment_str
                 else:
                      print(f"⚠️ Не удалось распарсить JSON из поля 'sarcasm.comment' для {gen_id}, и строка пуста или '{{}}'. Сарказм будет пуст.")
            except Exception as e:
                 print(f"⚠️ Неожиданная ошибка при обработке 'sarcasm.comment' для {gen_id}: {e}")
        else:
             print(f"ℹ️ Поле 'sarcasm.comment' пустое, отсутствует или имеет неожиданный тип в JSON для {gen_id}.")

        if comment_data_parsed is not None:
             for key in possible_comment_keys:
                 if key in comment_data_parsed:
                     found_comment = comment_data_parsed[key]
                     break
             if found_comment is None:
                  print(f"⚠️ Ни один из ключей {possible_comment_keys} не найден в 'sarcasm.comment' ({gen_id}).")

        sarcasm_comment = found_comment.strip() if isinstance(found_comment, str) else ""
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

        # --- Отправка в Telegram (Альбом: Фото + Видео) ---
        os.makedirs(PROCESSED_DIR, exist_ok=True)
        media_items = []
        png_file_handle = None
        video_file_handle = None

        try:
            # --- Подготовка списка медиа для альбома (ФОТО ПЕРВОЕ) ---
            current_caption = caption_text # Используем временную переменную для подписи

            # Добавляем фото ПЕРВЫМ с подписью
            png_file_handle = open(local_png_path, "rb")
            media_items.append(InputMediaPhoto(png_file_handle, caption=current_caption, parse_mode="HTML"))
            print(f"ℹ️ Добавлено PNG ПЕРВЫМ в медиагруппу. Подпись '{caption_text[:30]}...' будет добавлена (если не пустая).")
            current_caption = "" # Очищаем подпись для следующих элементов

            # Добавляем видео ВТОРЫМ без подписи
            video_file_handle = open(local_video_path, "rb")
            media_items.append(InputMediaVideo(video_file_handle, caption=current_caption, parse_mode="HTML", supports_streaming=True))
            print(f"ℹ️ Добавлено MP4 ВТОРЫМ в медиагруппу (без подписи).")

            # --- Отправка медиагруппы ---
            print(f"✈️ Пытаемся отправить медиагруппу ({len(media_items)} элемент(а), фото первое) для {gen_id}...")
            await bot.send_media_group(
                chat_id=TELEGRAM_CHAT_ID,
                media=media_items,
                read_timeout=120,
                connect_timeout=120,
                write_timeout=120
            )
            album_sent = True # Устанавливаем флаг успеха
            print(f"✅ Медиагруппа для {gen_id} отправлена.")

        except Exception as e:
            print(f"❌ Ошибка при отправке медиагруппы для {gen_id}: {e}")
            raise # Передаем ошибку выше, чтобы прервать публикацию
        finally:
            # Важно: закрываем файловые дескрипторы, даже если была ошибка
            if png_file_handle: png_file_handle.close()
            if video_file_handle: video_file_handle.close()

        # --- Отправка сарказма и опроса ---
        # (Логика отправки сарказма и опроса остается такой же, как в предыдущей версии)
        # 3. Отправляем сарказм, если он есть
        if sarcasm_comment:
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
        else:
             print("DEBUG: Сарказм пуст, отправка пропускается.")

        # 4. Отправляем опрос, если он валиден
        if poll_question and len(poll_options) >= 2:
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
        else:
             print("DEBUG: Опрос невалиден (нет вопроса или <2 опций), отправка пропускается.")

        # Если дошли до сюда без ошибок при отправке медиагруппы
        success = True

    except json.JSONDecodeError as e: # Ошибка обработки JSON
        print(f"❌ Ошибка декодирования основного JSON файла {local_json_path}: {e}")
        os.makedirs(ERROR_DIR, exist_ok=True)
        try:
            shutil.move(local_json_path, os.path.join(ERROR_DIR, os.path.basename(local_json_path)))
            print(f"📁 Поврежденный JSON {gen_id}.json перемещен в {ERROR_DIR}")
        except Exception as move_err:
             print(f"  ⚠️ Не удалось переместить поврежденный JSON: {move_err}")
        success = False
    except Exception as e: # Ловим ошибки отправки медиагруппы или другие непредвиденные
        print(f"❌ Непредвиденная ошибка во время обработки/отправки {gen_id}: {e}")
        success = False

    # --- Завершение и обработка файлов ---
    if success:
        print(f"✅ Успешная публикация основного контента (альбома) для {gen_id}.")
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
        # Сообщение об этом уже было выведено выше
        print(f"⚠️ Отправка основного контента для {gen_id} НЕ УДАЛАСЬ или была пропущена. ID не добавлен в опубликованные.")
        # Выводим статус для отладки, если JSON был обработан
        if json_processed_successfully:
             # Статус отправки медиа определяется флагом album_sent
             print(f"   (Статус отправки: Альбом - {'Да' if album_sent else 'Нет'}, Сарказм - {'Да' if sarcasm_sent else 'Нет'}, Опрос - {'Да' if poll_sent else 'Нет'})")
        print(f"   🗑️ Удаляем локальные файлы для {gen_id}, чтобы избежать дублирования при след. запуске.")

        # Удаляем все локальные файлы (JSON, PNG, Video), которые могли остаться в download
        files_to_delete = [local_png_path, local_video_path, local_json_path]
        for file_path in files_to_delete:
             if os.path.exists(file_path) and PROCESSED_DIR not in os.path.dirname(file_path) and ERROR_DIR not in os.path.dirname(file_path):
                  try:
                       os.remove(file_path)
                  except Exception as e:
                       print(f"  ⚠️ Не удалось удалить временный файл {file_path}: {e}")

    # Успех определяется отправкой АЛЬБОМА
    return success

# ------------------------------------------------------------
# 5) Основная логика (поиск и публикация) - Без изменений
# ------------------------------------------------------------
async def main():
    """
    Главная асинхронная функция скрипта.
    Ищет все неопубликованные группы, сортирует их.
    Пытается опубликовать их по очереди, начиная с самой старой.
    Пропускает неполные группы (без PNG или MP4).
    Останавливается после первой успешной публикации.
    """
    print("\n" + "="*50)
    print("🚀 Запуск скрипта публикации B2 -> Telegram (v11: Альбом фото+видео)") # <-- Обновлено название
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

            print(f"   ℹ️ Найдено {len(gen_ids_in_folder)} уникальных ID формата AbschlussMMDD-HHMM в {folder}")

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

        published_this_run = False # Флаг, показывающий, опубликовали ли мы что-то в этом запуске
        # --- НОВЫЙ ЦИКЛ: Перебираем все неопубликованные ID ---
        for gen_id_to_publish, folder_to_publish in unpublished_items:
            print(f"\n▶️ Пытаемся опубликовать следующую группу: ID={gen_id_to_publish} из папки {folder_to_publish}")
            print("-" * 50)

            # Пытаемся опубликовать выбранную группу
            # publish_generation_id вернет False, если группа неполная или произошла ошибка отправки
            success = await publish_generation_id(gen_id_to_publish, folder_to_publish, published_ids)

            print("-" * 50)
            if success:
                print(f"✅ Успешно опубликована группа {gen_id_to_publish}.")
                published_this_run = True # Устанавливаем флаг
                break # <-- ВАЖНО: Выходим из цикла после первой успешной публикации
            else:
                # Сообщение о причине неудачи выводится внутри publish_generation_id
                print(f"ℹ️ Публикация группы {gen_id_to_publish} не удалась или была пропущена. Переходим к следующей...")
                # Продолжаем цикл для следующего ID

        # --- После цикла ---
        if not published_this_run:
             print("\n⚠️ Не найдено полных групп для публикации в этом запуске.")

    else:
        # Если не найдено ни одного нового ID во всех папках
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

