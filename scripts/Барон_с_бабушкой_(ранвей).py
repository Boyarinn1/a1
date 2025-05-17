import sys
import os
import logging

# --- Диагностика sys.path (оставим на всякий случай, хотя он выглядит корректно) ---
# print("--- PYTHON SYS.PATH DIAGNOSTICS ---")
# for p_idx, p_val in enumerate(sys.path):
#     print(f"Path[{p_idx}]: {p_val}")
# print("--- END OF SYS.PATH ---")
# print(f"Current Working Directory: {os.getcwd()}")
# print(f"Script Path: {os.path.abspath(__file__)}")
# --- Конец диагностики ---

import requests  # Остальные импорты оставляем как были
import json
import time
import base64
from pathlib import Path

# --- Настройка Логирования ---
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger("runway_video_generator")

# --- Импорт RunwayML SDK ---
RUNWAY_SDK_AVAILABLE = False
RunwayML = None
RunwayAPIError = Exception  # Общий fallback для ошибок API, если специфичный не найдется

try:
    logger.info("Попытка: import runwayml")
    import runwayml

    logger.info(
        f"УСПЕХ: 'import runwayml'. Модуль найден в: {getattr(runwayml, '__file__', 'Местоположение не определено')}")

    logger.info("Попытка: from runwayml import RunwayML")
    from runwayml import RunwayML

    logger.info("УСПЕХ: 'from runwayml import RunwayML'.")

    # Теперь импортируем класс исключения, основываясь на выводе dir() из 333.py
    if hasattr(runwayml, 'RunwayMLError'):  # Проверяем наличие RunwayMLError (с большой E)
        logger.info("Найден атрибут 'RunwayMLError' в модуле 'runwayml'. Попытка импорта...")
        from runwayml import RunwayMLError  # <--- КЛЮЧЕВОЙ ИМПОРТ ЗДЕСЬ

        RunwayAPIError = RunwayMLError
        logger.info("УСПЕХ: 'from runwayml import RunwayMLError' выполнен, RunwayAPIError установлен.")
    elif hasattr(runwayml, 'APIError'):  # Запасной вариант, если вдруг RunwayMLError нет
        logger.warning("'RunwayMLError' не найден. Попытка импорта 'APIError' из 'runwayml'...")
        from runwayml import APIError

        RunwayAPIError = APIError
        logger.info("УСПЕХ: 'from runwayml import APIError' выполнен, RunwayAPIError установлен.")
    else:
        logger.warning(
            "Ни 'RunwayMLError', ни 'APIError' не найдены как атрибуты 'runwayml'. Используется общий Exception для RunwayAPIError.")
        # RunwayAPIError уже установлен как Exception по умолчанию

    RUNWAY_SDK_AVAILABLE = True
    logger.info("RUNWAY_SDK_AVAILABLE установлен в True.")

except ImportError as e:
    logger.error(f"КРИТИЧЕСКАЯ ОШИБКА ИМПОРТА на одном из этапов runwayml: {e}", exc_info=True)
    logger.error("Убедитесь, что RunwayML SDK корректно установлен: pip install runwayml")
    # RUNWAY_SDK_AVAILABLE остается False, RunwayML остается None, RunwayAPIError остается Exception
except Exception as e_other:
    logger.error(f"НЕОЖИДАННАЯ ОШИБКА во время настройки runwayml: {e_other}", exc_info=True)
    # RUNWAY_SDK_AVAILABLE остается False, RunwayML остается None, RunwayAPIError остается Exception

# --- Константы ---
# URL вашего изображения для первого кадра
INPUT_IMAGE_URL = "https://i.postimg.cc/TYkcMTkW/Gen4-373363102.png"

# Промпт для Runway (из артефакта runway_baron_prompt)
RUNWAY_TEXT_PROMPT = """
Image of Baron Sarcasm at his desk, writing. Scene starts static for a moment.
He then slowly looks up from the letter, his gaze distant, a faint, knowing, slightly ironic smile forming on his face; his quill pen hovers above the paper.
His smile widens a little more. In the background, a ghostly, semi-transparent silhouette of an elderly lady (his grandmother) subtly nods approvingly, or her own smile brightens.
With a satisfied (silent) chuckle, the Baron looks back down at the letter and resumes writing with renewed energy. A very slow camera zoom out concludes the scene.

Maintain the warm, personal, nostalgic, and ironic atmosphere. The overall style should be cinematic, detailed, atmospheric, a blend of classic portraiture and subtle digital fantasy. Ensure textures, lighting, and character likeness are consistent with the initial detailed portrait. 16:9 aspect ratio.
"""

# Параметры Runway
RUNWAY_MODEL_NAME = "gen4_turbo"
VIDEO_DURATION_SECONDS = 10
ASPECT_RATIO = "1280:720"  # ИСПРАВЛЕНО на конкретное разрешение

# Параметры опроса
POLLING_INTERVAL_SECONDS = 20
MAX_POLLING_ATTEMPTS = 60
REQUEST_TIMEOUT_SECONDS = 60

# Директория для сохранения
OUTPUT_DIRECTORY = "zagruzki"
Path(OUTPUT_DIRECTORY).mkdir(parents=True, exist_ok=True)


def get_runway_api_key():
    """Получает API-ключ Runway из переменных окружения."""
    api_key = os.getenv("RUNWAY_API_KEY")
    if not api_key:
        logger.critical("Ошибка: Переменная окружения RUNWAY_API_KEY не установлена!")
        raise ValueError("RUNWAY_API_KEY не найден в переменных окружения.")
    return api_key


def image_url_to_base64_data_uri(image_url: str) -> str | None:
    """Скачивает изображение по URL и конвертирует его в base64 data URI."""
    try:
        logger.info(f"Скачивание изображения с URL: {image_url}")
        response = requests.get(image_url, timeout=REQUEST_TIMEOUT_SECONDS)
        response.raise_for_status()

        content_type = response.headers.get('content-type', 'image/png')
        if not content_type.startswith('image/'):
            logger.error(f"URL не указывает на изображение. Content-Type: {content_type}")
            return None

        base64_image = base64.b64encode(response.content).decode("utf-8")
        data_uri = f"data:{content_type};base64,{base64_image}"
        logger.info("Изображение успешно конвертировано в base64 data URI.")
        return data_uri
    except requests.exceptions.RequestException as e:
        logger.error(f"Ошибка скачивания изображения по URL {image_url}: {e}")
        return None
    except Exception as e:
        logger.error(f"Ошибка конвертации изображения в base64: {e}", exc_info=True)
        return None


def download_video(video_url: str, output_path: str) -> bool:
    """Скачивает видео по URL и сохраняет его."""
    try:
        logger.info(f"Скачивание видео с URL: {video_url} -> {output_path}")
        response = requests.get(video_url, stream=True, timeout=300)
        response.raise_for_status()
        with open(output_path, 'wb') as f:
            for chunk in response.iter_content(chunk_size=8192):
                f.write(chunk)
        logger.info(f"Видео успешно сохранено: {output_path}")
        return True
    except requests.exceptions.RequestException as e:
        logger.error(f"Ошибка скачивания видео {video_url}: {e}")
        return False
    except Exception as e:
        logger.error(f"Неизвестная ошибка при скачивании видео: {e}", exc_info=True)
        return False


def main():
    """Основная функция для генерации видео."""
    logger.info(f"Запуск main(). RUNWAY_SDK_AVAILABLE: {RUNWAY_SDK_AVAILABLE}")
    if not RUNWAY_SDK_AVAILABLE:
        logger.info("Завершение работы из main(), так как RunwayML SDK недоступен (RUNWAY_SDK_AVAILABLE is False).")
        return

    logger.info("--- 🎬 Запуск скрипта генерации видео в RunwayML ---")

    try:
        api_key = get_runway_api_key()
    except ValueError:
        return

        # 1. Подготовка исходного изображения
    image_data_uri = image_url_to_base64_data_uri(INPUT_IMAGE_URL)
    if not image_data_uri:
        logger.error("Не удалось подготовить исходное изображение. Завершение работы.")
        return

    # 2. Инициализация клиента RunwayML
    if RunwayML is None:
        logger.error("Класс RunwayML не был импортирован (RunwayML is None). Невозможно создать клиент.")
        return

    try:
        logger.info("Инициализация клиента RunwayML SDK...")
        client = RunwayML(api_key=api_key)
        logger.info("✅ Клиент RunwayML SDK инициализирован.")
    except Exception as e:
        logger.error(f"Ошибка инициализации клиента RunwayML: {e}", exc_info=True)
        return

    # 3. Создание задачи генерации видео
    generation_params = {
        "model": RUNWAY_MODEL_NAME,
        "prompt_image": image_data_uri,
        "prompt_text": RUNWAY_TEXT_PROMPT.strip(),
        "duration": VIDEO_DURATION_SECONDS,
        "ratio": ASPECT_RATIO  # Здесь используется "1280:720"
    }

    task_id = None
    try:
        logger.info("🚀 Создание задачи RunwayML Image-to-Video...")
        log_params_preview = {k: (v[:70] + '...' if isinstance(v, str) and len(v) > 70 else v) for k, v in
                              generation_params.items()}
        logger.debug(f"Параметры для Runway: {json.dumps(log_params_preview, indent=2)}")

        task = client.image_to_video.create(**generation_params)
        task_id = getattr(task, 'id', None)
        if not task_id:
            logger.error("Не удалось получить ID задачи от Runway.")
            logger.debug(f"Ответ от Runway (create task): {task}")
            return
        logger.info(f"✅ Задача Runway создана! ID: {task_id}")

    except RunwayAPIError as e:
        logger.error(f"❌ Ошибка SDK Runway при создании задачи: {e}", exc_info=True)
        return
    except Exception as e:
        logger.error(f"❌ Общая ошибка при создании задачи Runway: {e}", exc_info=True)
        return

    # 4. Опрос статуса задачи
    logger.info(f"⏳ Начало опроса статуса задачи Runway {task_id}...")
    final_video_url = None

    for attempt in range(MAX_POLLING_ATTEMPTS):
        try:
            task_status = client.tasks.retrieve(task_id)
            current_status = getattr(task_status, 'status', 'UNKNOWN').upper()
            logger.info(f"Попытка {attempt + 1}/{MAX_POLLING_ATTEMPTS}. Статус Runway {task_id}: {current_status}")

            if current_status == "SUCCEEDED":
                logger.info(f"🎉 Задача Runway {task_id} успешно завершена!")
                task_output = getattr(task_status, 'output', None)

                if isinstance(task_output, list) and len(task_output) > 0 and isinstance(task_output[0], str):
                    final_video_url = task_output[0]
                elif isinstance(task_output, dict) and task_output.get('url'):
                    final_video_url = task_output['url']
                elif isinstance(task_output, str) and task_output.startswith('http'):
                    final_video_url = task_output

                if final_video_url:
                    logger.info(f"Получен URL видео: {final_video_url}")
                else:
                    logger.warning(f"Статус SUCCEEDED, но URL видео не найден в ответе: {task_output}")
                break

            elif current_status == "FAILED":
                logger.error(f"❌ Задача Runway {task_id} завершилась с ошибкой (FAILED)!")
                error_details = getattr(task_status, 'error_message', 'Детали ошибки отсутствуют в ответе API.')
                logger.error(f"Детали ошибки Runway: {error_details}")
                logger.debug(f"Полный ответ статуса при ошибке: {task_status}")
                break

            elif current_status in ["PENDING", "PROCESSING", "QUEUED", "WAITING", "RUNNING"]:
                time.sleep(POLLING_INTERVAL_SECONDS)
            else:
                logger.warning(f"Неизвестный или неожиданный статус Runway: {current_status}. Прерывание опроса.")
                logger.debug(f"Полный ответ статуса: {task_status}")
                break

        except RunwayAPIError as e:
            logger.error(f"❌ Ошибка SDK Runway при опросе задачи {task_id}: {e}", exc_info=True)
            break
        except Exception as e:
            logger.error(f"❌ Общая ошибка при опросе статуса Runway {task_id}: {e}", exc_info=True)
            break
    else:
        logger.warning(
            f"⏰ Таймаут ({MAX_POLLING_ATTEMPTS * POLLING_INTERVAL_SECONDS} сек) ожидания завершения задачи Runway {task_id}.")

    # 5. Скачивание видео
    if final_video_url:
        timestamp = time.strftime("%Y%m%d-%H%M%S")
        output_filename = f"baron_video_{task_id}_{timestamp}.mp4"
        output_path = Path(OUTPUT_DIRECTORY) / output_filename
        if not download_video(final_video_url, str(output_path)):
            logger.error(f"Не удалось скачать финальное видео. URL: {final_video_url}")
        else:
            logger.info(f"🎉 Видео успешно сгенерировано и скачано: {output_path}")
    else:
        logger.error("Финальный URL видео не был получен. Скачивание невозможно.")

    logger.info("--- ✅ Завершение работы скрипта ---")


if __name__ == "__main__":
    logger.info(
        f"Запуск блока if __name__ == '__main__'. RunwayML определен как: {RunwayML}, RUNWAY_SDK_AVAILABLE: {RUNWAY_SDK_AVAILABLE}")
    if RunwayML is None or not RUNWAY_SDK_AVAILABLE:
        if not RUNWAY_SDK_AVAILABLE:
            print(
                "RunwayML SDK не установлен или не удалось импортировать. Пожалуйста, установите его командой: pip install runwayml")
        else:
            print("Неожиданное состояние: RunwayML SDK помечен как доступный, но RunwayML is None.")
    else:
        main()
