#!/usr/bin/env python3
import os
import json
import asyncio
import shutil
import re
from typing import Set, List, Tuple, Any  # Добавлен Any
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
S3_ENDPOINT = os.getenv("S3_ENDPOINT", "production")  # По умолчанию 'production', можно переопределить

TELEGRAM_TOKEN = os.getenv("TELEGRAM_TOKEN")
TELEGRAM_CHAT_ID = os.getenv("TELEGRAM_CHAT_ID")

# Определяем суффикс сарказма (пока хардкод, т.к. нет ConfigManager)
SARCASM_SUFFIX = "_sarcasm.png"

# Проверяем наличие всех необходимых переменных
if not all([
    S3_KEY_ID,
    S3_APPLICATION_KEY,
    S3_BUCKET_NAME,
    TELEGRAM_TOKEN,
    TELEGRAM_CHAT_ID
]):
    raise RuntimeError("❌ Ошибка: Не установлены все необходимые переменные окружения!")
else:
    print("✅ Все необходимые переменные окружения загружены.")

# ------------------------------------------------------------
# 2) Настраиваем пути, объект Telegram-бота и B2 SDK
# ------------------------------------------------------------
BASE_DIR = os.path.dirname(__file__)
DOWNLOAD_DIR = os.path.join(BASE_DIR, "downloaded")
PROCESSED_DIR = os.path.join(DOWNLOAD_DIR, "processed")
ERROR_DIR = os.path.join(DOWNLOAD_DIR, "errors")

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
    config_key = "config/config_public.json"
    published_ids = set()
    try:
        print(f"📥 Пытаемся скачать {config_key} для получения списка опубликованных ID...")
        os.makedirs(os.path.dirname(local_config_path), exist_ok=True)
        bucket.download_file_by_name(config_key).save_to(local_config_path)
        with open(local_config_path, "r", encoding="utf-8") as f:
            data = json.load(f)
        published = data.get("generation_id", [])
        if isinstance(published, list):
            published_ids = set(published)
            print(f"ℹ️ Загружено {len(published_ids)} опубликованных ID из {config_key}.")
        else:
            print(f"⚠️ Поле 'generation_id' в {config_key} не является списком. Используем пустой список.")
        if os.path.exists(local_config_path):
            os.remove(local_config_path)
    except FileNotPresent:
        print(f"⚠️ Файл {config_key} не найден в B2. Будет создан новый при первой успешной публикации.")
    except json.JSONDecodeError as e:
        print(f"⚠️ Ошибка декодирования JSON в файле {config_key}: {e}. Используем пустой список.")
        if os.path.exists(local_config_path): os.remove(local_config_path)
    except B2Error as e:
        print(f"⚠️ Ошибка B2 SDK при скачивании {config_key}: {e}. Используем пустой список.")
        if os.path.exists(local_config_path): os.remove(local_config_path)
    except Exception as e:
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
        data = {"generation_id": sorted(list(pub_ids))}
        os.makedirs(os.path.dirname(local_config_path), exist_ok=True)
        with open(local_config_path, "w", encoding="utf-8") as f:
            json.dump(data, f, ensure_ascii=False, indent=4)
        print(f"💾 Локально сохранен обновленный список ID в {local_config_path}")
        print(f"📤 Загружаем обновленный {config_key} в B2...")
        bucket.upload_local_file(local_config_path, config_key)
        print(f"✅ Успешно обновлен {config_key} в B2. Всего ID: {len(pub_ids)}")
        if os.path.exists(local_config_path):
            os.remove(local_config_path)
    except Exception as e:
        print(f"⚠️ Не удалось сохранить или загрузить {config_key}: {e}")
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
    Очищает текст от стандартных заголовков и заменяет множественные переводы строк.
    """
    if not isinstance(text, str):
        return ""
    system_phrases = [
        "Вступление:", "Основная часть:", "Интересный факт:", "Заключение:",
        "🔥Вступление", "📚Основная часть", "🔍Интересный факт"
    ]
    clean_text = text
    for phrase in system_phrases:
        clean_text = re.sub(r'^\s*' + re.escape(phrase) + r'\s*\n?', '', clean_text,
                            flags=re.IGNORECASE | re.MULTILINE).strip()
        clean_text = re.sub(r'\n\s*' + re.escape(phrase) + r'\s*', '\n', clean_text, flags=re.IGNORECASE).strip()
    clean_text = re.sub(r"\n\s*\n+", "\n\n", clean_text)
    return clean_text.strip()


# ------------------------------------------------------------
# 4) Публикация одного generation_id
# ------------------------------------------------------------
async def publish_generation_id(gen_id: str, folder: str, published_ids: Set[str]) -> bool:
    """
    Скачивает, обрабатывает и публикует контент для одного generation_id.
    """
    print(f"⚙️ Обрабатываем gen_id: {gen_id} из папки {folder}")
    json_file_key = f"{folder}{gen_id}.json"
    video_file_key = f"{folder}{gen_id}.mp4"
    png_file_key = f"{folder}{gen_id}.png"
    sarcasm_png_file_key = f"{folder}{gen_id}{SARCASM_SUFFIX}"

    local_json_path = os.path.join(DOWNLOAD_DIR, f"{gen_id}.json")
    local_video_path = os.path.join(DOWNLOAD_DIR, f"{gen_id}.mp4")
    local_png_path = os.path.join(DOWNLOAD_DIR, f"{gen_id}.png")
    local_sarcasm_png_path = os.path.join(DOWNLOAD_DIR, f"{gen_id}{SARCASM_SUFFIX}")

    json_downloaded = False
    video_downloaded = False
    png_downloaded = False
    sarcasm_png_downloaded = False
    local_files_to_clean = [local_json_path, local_video_path, local_png_path, local_sarcasm_png_path]

    def cleanup_local_files():
        for file_path in local_files_to_clean:
            if os.path.exists(file_path):
                try:
                    os.remove(file_path)
                except Exception as e:
                    print(f"  ⚠️ Не удалось удалить временный файл {file_path}: {e}")

    try:
        print(f"📥 Скачиваем JSON: {json_file_key} -> {local_json_path}")
        os.makedirs(os.path.dirname(local_json_path), exist_ok=True)
        bucket.download_file_by_name(json_file_key).save_to(local_json_path)
        json_downloaded = True
        print(f"✅ JSON скачан: {local_json_path}")

        print(f"📥 Пытаемся скачать PNG: {png_file_key} -> {local_png_path}")
        os.makedirs(os.path.dirname(local_png_path), exist_ok=True)
        bucket.download_file_by_name(png_file_key).save_to(local_png_path)
        png_downloaded = True
        print(f"✅ PNG скачан: {local_png_path}")

        print(f"📥 Пытаемся скачать видео: {video_file_key} -> {local_video_path}")
        os.makedirs(os.path.dirname(local_video_path), exist_ok=True)
        bucket.download_file_by_name(video_file_key).save_to(local_video_path)
        video_downloaded = True
        print(f"✅ Видео скачано: {local_video_path}")

        print(f"📥 Пытаемся скачать Sarcasm PNG: {sarcasm_png_file_key} -> {local_sarcasm_png_path}")
        os.makedirs(os.path.dirname(local_sarcasm_png_path), exist_ok=True)
        bucket.download_file_by_name(sarcasm_png_file_key).save_to(local_sarcasm_png_path)
        sarcasm_png_downloaded = True
        print(f"✅ Sarcasm PNG скачан: {local_sarcasm_png_path}")

    except FileNotPresent:
        missing_file_key = ""
        if not json_downloaded:
            missing_file_key = json_file_key
        elif not png_downloaded:
            missing_file_key = png_file_key
        elif not video_downloaded:
            missing_file_key = video_file_key
        elif not sarcasm_png_downloaded:
            missing_file_key = sarcasm_png_file_key
        print(f"❌ Группа {gen_id} неполная ({missing_file_key} отсутствует). Публикация пропускается.")
        cleanup_local_files()
        return False
    except B2Error as e:
        print(f"⚠️ Ошибка B2 SDK при скачивании файлов для {gen_id}: {e}")
        cleanup_local_files()
        return False
    except Exception as e:
        print(f"⚠️ Неожиданная ошибка при скачивании файлов для {gen_id}: {e}")
        cleanup_local_files()
        return False

    if not (json_downloaded and png_downloaded and video_downloaded and sarcasm_png_downloaded):
        print(
            f"❌ Группа {gen_id} неполная (JSON:{json_downloaded}, PNG:{png_downloaded}, Видео:{video_downloaded}, Sarcasm:{sarcasm_png_downloaded}). Пропуск.")
        cleanup_local_files()
        return False

    print(f"✅ Все 4 файла для {gen_id} найдены. Продолжаем обработку и отправку...")
    caption_text = ""
    poll_question = ""
    poll_options = []
    json_processed_successfully = False
    album_sent = False
    sarcasm_photo_sent = False
    poll_sent = False
    success = False

    try:
        with open(local_json_path, "r", encoding="utf-8") as f:
            data = json.load(f)

        # --- ИЗВЛЕЧЕНИЕ ОСНОВНОГО ТЕКСТА ---
        content_value = data.get("content")
        possible_text_keys = ["текст", "content", "text"]
        found_text = None
        content_data = None
        if isinstance(content_value, dict):
            content_data = content_value
        elif isinstance(content_value, str) and content_value.strip():
            try:
                content_data = json.loads(content_value.strip())
            except json.JSONDecodeError:
                found_text = content_value.strip() if content_value.strip() not in ["{}"] else None
            except Exception as e:
                print(f"⚠️ Ошибка обработки 'content' {gen_id}: {e}")
        if content_data is not None:
            post_list = content_data.get("post")
            if isinstance(post_list, list):
                post_texts = [list(item.values())[0] for item in post_list if isinstance(item, dict) and len(item) == 1]
                if post_texts: found_text = "\n\n".join(filter(None, post_texts))
            if found_text is None:
                for key in possible_text_keys:
                    if key in content_data: found_text = content_data[key]; break
        main_text = found_text.strip() if isinstance(found_text, str) else ""
        main_text = remove_system_phrases(main_text)
        main_text = re.sub(r'(?<!\n)\n(?!\n)', '\n\n', main_text)

        # --- ИЗВЛЕЧЕНИЕ И ФОРМАТИРОВАНИЕ ХЕШТЕГОВ ---
        hashtags_list = data.get("hashtags")
        formatted_hashtags_str = ""
        if isinstance(hashtags_list, list):
            formatted_hashtags = [f"#{tag.strip()}" for tag in hashtags_list if tag.strip()]
            formatted_hashtags_str = " ".join(formatted_hashtags)
            print(f"ℹ️ Сформированы хештеги: {formatted_hashtags_str}")
        elif hashtags_list is not None:
            print(f"⚠️ Ключ 'hashtags' найден, но не является списком: {type(hashtags_list)}")

        # --- ИСПРАВЛЕННЫЙ БЛОК: Сборка финальной подписи (текст + ссылка + хештеги) ---
        # Этот блок теперь находится ВНЕ условий if/elif для hashtags_list
        link_html = '<b><a href="https://t.me/boyarinn7">Подпишись, забудешь</a></b>'
        parts_for_caption = []

        if main_text:  # Добавляем основной текст, если он есть
            parts_for_caption.append(main_text)

        parts_for_caption.append(link_html)  # Всегда добавляем ссылку

        if formatted_hashtags_str:  # Добавляем хештеги, только если они были сформированы
            parts_for_caption.append(formatted_hashtags_str)

        # Собираем все части вместе, разделяя двойным переносом строки
        # Только если часть не пустая
        caption_text = "\n\n".join(part for part in parts_for_caption if part)

        # Обрезаем подпись, если она слишком длинная
        if len(caption_text) > 1024:
            caption_text = caption_text[:1020] + "..."
            print(f"⚠️ Подпись была обрезана до 1024 символов.")

        # КЛЮЧЕВАЯ ОТЛАДОЧНАЯ СТРОКА (теперь должна работать всегда)
        print(f"DEBUG: Финальная подпись для фото: '{caption_text[:150]}...'")
        # --- Конец ИСПРАВЛЕННОГО блока сборки подписи ---

        # --- Извлечение опроса ---
        sarcasm_data = data.get("sarcasm", {})
        poll_data = sarcasm_data.get("poll", {})
        poll_question = poll_data.get("question", "").strip()[:300]
        poll_options = [str(opt).strip()[:100] for opt in poll_data.get("options", []) if str(opt).strip()][:10]
        print(f"DEBUG: Опрос: Q='{poll_question}', Opts={poll_options}")

        json_processed_successfully = True

        # --- Отправка в Telegram ---
        os.makedirs(PROCESSED_DIR, exist_ok=True)
        png_file_handle = None
        video_file_handle = None
        sarcasm_png_file_handle = None

        try:
            media_items = []
            current_caption = caption_text  # Используем обновленный caption_text

            png_file_handle = open(local_png_path, "rb")
            media_items.append(InputMediaPhoto(png_file_handle, caption=current_caption, parse_mode="HTML"))
            print(f"ℹ️ Добавлено PNG ПЕРВЫМ в медиагруппу (с подписью).")

            video_file_handle = open(local_video_path, "rb")
            media_items.append(
                InputMediaVideo(video_file_handle, caption="", parse_mode="HTML", supports_streaming=True))
            print(f"ℹ️ Добавлено MP4 ВТОРЫМ в медиагруппу (без подписи).")

            print(f"✈️ Пытаемся отправить медиагруппу ({len(media_items)} элемента) для {gen_id}...")
            await bot.send_media_group(
                chat_id=TELEGRAM_CHAT_ID, media=media_items,
                read_timeout=120, connect_timeout=120, write_timeout=120
            )
            album_sent = True
            print(f"✅ Медиагруппа (Фото+Видео) для {gen_id} отправлена.")
        except Exception as e:
            print(f"❌ Ошибка при отправке медиагруппы для {gen_id}: {e}")
            success = False
            raise
        finally:
            if png_file_handle: png_file_handle.close()
            if video_file_handle: video_file_handle.close()

        if album_sent:
            try:
                print(f"✈️ Отправляем фото сарказма для {gen_id}...")
                sarcasm_png_file_handle = open(local_sarcasm_png_path, "rb")
                await bot.send_photo(
                    chat_id=TELEGRAM_CHAT_ID,
                    photo=sarcasm_png_file_handle,
                    read_timeout=60, connect_timeout=60, write_timeout=60
                )
                sarcasm_photo_sent = True
                print(f"✅ Фото сарказма для {gen_id} отправлено.")
            except Exception as e:
                print(f"⚠️ Ошибка при отправке фото сарказма для {gen_id}: {e}")
            finally:
                if sarcasm_png_file_handle: sarcasm_png_file_handle.close()

        if album_sent:
            print("⏳ Пауза 1 секунда перед отправкой опроса...")
            await asyncio.sleep(1)
            if poll_question and len(poll_options) >= 2:
                poll_question_formatted = f"🎭 {poll_question}"
                try:
                    print(f"✈️ Отправляем опрос для {gen_id}...")
                    await bot.send_poll(
                        chat_id=TELEGRAM_CHAT_ID, question=poll_question_formatted,
                        options=poll_options, is_anonymous=True
                    )
                    poll_sent = True
                    print(f"✅ Опрос для {gen_id} отправлен.")
                except Exception as e:
                    print(f"⚠️ Ошибка при отправке опроса для {gen_id}: {e}")
            else:
                print("DEBUG: Опрос невалиден, отправка пропускается.")

        if album_sent and sarcasm_photo_sent:
            success = True
    except json.JSONDecodeError as e:
        print(f"❌ Ошибка декодирования JSON {local_json_path}: {e}")
        os.makedirs(ERROR_DIR, exist_ok=True)
        try:
            shutil.move(local_json_path, os.path.join(ERROR_DIR, os.path.basename(local_json_path)))
        except Exception as move_err:
            print(f"  ⚠️ Не удалось переместить поврежденный JSON: {move_err}")
        success = False
    except Exception as e:
        print(f"❌ Непредвиденная ошибка при обработке/отправке {gen_id}: {e}")
        success = False

    if success:
        print(f"✅ Успешная публикация контента для {gen_id}.")
        published_ids.add(gen_id)
        save_published_ids(published_ids)
        os.makedirs(PROCESSED_DIR, exist_ok=True)
        for file_path in local_files_to_clean:
            if os.path.exists(file_path):
                try:
                    destination_path = os.path.join(PROCESSED_DIR, os.path.basename(file_path))
                    shutil.move(file_path, destination_path)
                    print(f"  📁 Файл {os.path.basename(file_path)} перемещен в {PROCESSED_DIR}")
                except Exception as e:
                    print(f"  ⚠️ Не удалось переместить файл {os.path.basename(file_path)} в processed: {e}")
    else:
        print(f"⚠️ Публикация контента для {gen_id} НЕ УДАЛАСЬ или была пропущена. ID не добавлен в опубликованные.")
        if json_processed_successfully:
            print(
                f"   (Статус отправки: Альбом - {'Да' if album_sent else 'Нет'}, Фото сарказма - {'Да' if sarcasm_photo_sent else 'Нет'}, Опрос - {'Да' if poll_sent else 'Нет'})")
        print(f"   🗑️ Удаляем локальные файлы для {gen_id}...")
        cleanup_local_files()
    return success


# ------------------------------------------------------------
# 5) Основная логика (поиск и публикация)
# ------------------------------------------------------------
async def main():
    print("\n" + "=" * 50)
    print("🚀 Запуск скрипта публикации B2 -> Telegram (v23: Финальная проверка логики caption)")
    print("=" * 50)

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
                relative_path = file_name.replace(folder, '', 1)
                if '/' not in relative_path:
                    gen_id = None
                    if relative_path.endswith(SARCASM_SUFFIX):
                        gen_id = relative_path[:-len(SARCASM_SUFFIX)]
                    else:
                        gen_id = os.path.splitext(relative_path)[0]
                    if re.fullmatch(r"\d{8}-\d{4}", gen_id):
                        gen_ids_in_folder.add(gen_id)
                    else:
                        if not relative_path.endswith('.bzEmpty'):
                            print(f"   ⚠️ Пропускаем файл с некорректным именем ID: {file_name}")

            print(f"   ℹ️ Найдено {len(gen_ids_in_folder)} уникальных ID формата ГГГГММДД-ЧЧММ в {folder}")
            new_ids = gen_ids_in_folder - published_ids
            if new_ids:
                print(f"   ✨ Найдено {len(new_ids)} новых (неопубликованных) ID в {folder}.")
                for gen_id_item in new_ids:
                    unpublished_items.append((gen_id_item, folder))
            else:
                print(f"   ✅ Нет новых ID для публикации в {folder}.")
        except B2Error as e:
            print(f"   ❌ Ошибка B2 SDK при сканировании папки {folder}: {e}")
        except Exception as e:
            print(f"   ❌ Неожиданная ошибка при сканировании папки {folder}: {e}")

    if unpublished_items:
        print(f"\n⏳ Всего найдено {len(unpublished_items)} неопубликованных групп для проверки.")
        unpublished_items.sort(key=lambda item: item[0])
        print("   🔢 Сортировка по дате и времени (gen_id)...")
        published_this_run = False
        for gen_id_to_publish, folder_to_publish in unpublished_items:
            print(f"\n▶️ Пытаемся опубликовать следующую группу: ID={gen_id_to_publish} из папки {folder_to_publish}")
            print("-" * 50)
            success_flag = await publish_generation_id(gen_id_to_publish, folder_to_publish, published_ids)
            print("-" * 50)
            if success_flag:
                print(f"✅ Успешно опубликована группа {gen_id_to_publish}.")
                published_this_run = True
                break
            else:
                print(
                    f"ℹ️ Публикация группы {gen_id_to_publish} не удалась или была пропущена. Переходим к следующей...")
        if not published_this_run:
            print("\n⚠️ Не найдено полных групп (4 файла) для публикации в этом запуске.")
    else:
        print("\n🎉 Нет новых групп для публикации во всех отсканированных папках.")
    print("\n🏁 Скрипт завершил работу.")
    print("=" * 50 + "\n")


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except RuntimeError as e:
        print(f"\n💥 Критическая ошибка: {e}")
    except Exception as e:
        print(f"\n💥 Непредвиденная критическая ошибка: {e}")
