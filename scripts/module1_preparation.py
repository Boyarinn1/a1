import os
import json
import boto3
print(f"🛠 Версия boto3: {boto3.__version__}")


from botocore.exceptions import BotoCoreError, ClientError
from datetime import datetime
from dotenv import load_dotenv

# Явно указываем путь к .env
dotenv_path = os.path.join(os.path.dirname(__file__), ".env")
load_dotenv(dotenv_path)

# Корневая директория проекта
BASE_DIR = os.path.dirname(os.path.dirname(__file__))
DOWNLOAD_DIR = os.path.join(BASE_DIR, "data", "downloaded")

# Создание папки для скачивания
os.makedirs(DOWNLOAD_DIR, exist_ok=True)


print(f"S3_ENDPOINT: {os.getenv('S3_ENDPOINT')}")
print(f"S3_BUCKET_NAME: {os.getenv('S3_BUCKET_NAME')}")
print(f"S3_KEY_ID: {os.getenv('S3_KEY_ID')}")
print(f"S3_APPLICATION_KEY: {os.getenv('S3_APPLICATION_KEY')}")

# Загрузка переменных из .env
load_dotenv()

# Константы путей
PROCESSED_DIR = "data/processed"
LOG_FILE = "logs/operation_log.txt"

# Папки для поиска готовых групп
SEARCH_FOLDERS = ["444/", "555/", "666/"]

# Настройки B2 из переменных окружения
ENDPOINT = os.getenv("S3_ENDPOINT")
BUCKET_NAME = os.getenv("S3_BUCKET_NAME")
KEY_ID = os.getenv("S3_KEY_ID")
APPLICATION_KEY = os.getenv("S3_APPLICATION_KEY")

# Логирование
def log_message(message):
    os.makedirs(os.path.dirname(LOG_FILE), exist_ok=True)
    with open(LOG_FILE, "a") as log:
        log.write(f"[{datetime.now()}] {message}\n")
    print(message)

# Подключение к B2
def create_b2_client():
    try:
        client = boto3.client(
            's3',
            endpoint_url=ENDPOINT,
            aws_access_key_id=KEY_ID,
            aws_secret_access_key=APPLICATION_KEY
        )
        log_message("Успешное подключение к B2.")
        return client
    except Exception as e:
        log_message(f"Ошибка подключения к B2: {e}")
        raise

# Скачивание файлов из B2
def download_files(client, prefix=""):
    os.makedirs(DOWNLOAD_DIR, exist_ok=True)
    try:
        response = client.list_objects_v2(Bucket=BUCKET_NAME, Prefix=prefix)
        if 'Contents' not in response:
            log_message("Нет файлов для скачивания.")
            return

        for obj in response['Contents']:
            key = obj['Key']
            # Фильтруем файлы
            if key.endswith((".json", ".mp4", ".png")):
                local_path = os.path.join(DOWNLOAD_DIR, os.path.basename(key))
                log_message(f"Сохранение файла в: {local_path}")
                client.download_file(BUCKET_NAME, key, local_path)
                log_message(f"Скачан файл: {key}")
            else:
                log_message(f"Пропущен файл: {key}")

    except (BotoCoreError, ClientError) as e:
        log_message(f"Ошибка скачивания из B2: {e}")

# Обработка JSON
def process_json_file(json_path):
    os.makedirs(PROCESSED_DIR, exist_ok=True)
    try:
        with open(json_path, "r", encoding="utf-8") as file:
            data = json.load(file)

        # Проверка обязательных полей
        if not all(k in data for k in ("title", "content")):
            log_message(f"Файл {json_path} пропущен: отсутствуют обязательные поля.")
            return

        # Форматирование текста
        title = f"<b>{data['title']}</b>"
        content = data['content']
        post_text = f"{title}\n\n{content}"

        # Сохранение обработанных данных
        processed_path = os.path.join(PROCESSED_DIR, os.path.basename(json_path))
        with open(processed_path, "w", encoding="utf-8") as output_file:
            json.dump({"text": post_text, "poll": data.get("poll")}, output_file, indent=4, ensure_ascii=False)
        log_message(f"Обработан файл: {json_path}")

    except Exception as e:
        log_message(f"Ошибка обработки файла {json_path}: {e}")


def fetch_config_from_b2(client):
    """
    Скачивает текущий config_public.json из B2.
    Возвращает содержимое как словарь.
    """
    config_key = "config/config_public.json"
    local_config_path = os.path.join(DOWNLOAD_DIR, "config_public.json")

    try:
        client.download_file(BUCKET_NAME, config_key, local_config_path)
        with open(local_config_path, "r", encoding="utf-8") as config_file:
            config = json.load(config_file)
        log_message("Файл config_public.json успешно скачан и прочитан.")
        return config
    except (BotoCoreError, ClientError, json.JSONDecodeError) as e:
        log_message(f"Ошибка при скачивании или чтении config_public.json: {e}")
        return {}

def update_config_in_b2(client, folder):
    """
    Обновляет config/config_public.json в B2, добавляя папку публикации.
    """
    config_key = "config/config_public.json"
    local_config_path = os.path.join(DOWNLOAD_DIR, "config_public.json")

    # Скачиваем текущий конфиг
    try:
        client.download_file(BUCKET_NAME, config_key, local_config_path)
        with open(local_config_path, "r", encoding="utf-8") as config_file:
            config = json.load(config_file)
        log_message(f"Текущее содержимое config_public.json: {config}")
    except (BotoCoreError, ClientError, json.JSONDecodeError) as e:
        log_message(f"Ошибка при скачивании или чтении config_public.json: {e}")
        config = {}

    # Обновляем конфиг
    config["publish"] = folder
    with open(local_config_path, "w", encoding="utf-8") as config_file:
        json.dump(config, config_file, indent=4)
    log_message(f"Файл config_public.json обновлен локально: {config}")

    # Загрузка файла обратно в B2
    try:
        client.upload_file(local_config_path, BUCKET_NAME, config_key)
        log_message(f"Файл {config_key} успешно загружен в B2.")
        log_message(f"Проверка содержимого config_public.json: {config}")
    except (BotoCoreError, ClientError) as e:
        log_message(f"Ошибка при загрузке {config_key} в B2: {e}")

def find_ready_group(client):
    """
    Ищет первую подходящую группу файлов с одинаковым generation_id (.json и .mp4) в папках 444/, 555/, 666/.
    """
    for folder in ["444/", "555/", "666/"]:
        response = client.list_objects_v2(Bucket=BUCKET_NAME, Prefix=folder)
        if 'Contents' not in response:
            continue

        files = {}
        for obj in response['Contents']:
            key = obj['Key']
            name, ext = os.path.splitext(os.path.basename(key))
            if ext in (".json", ".mp4"):
                files.setdefault(name, set()).add(ext)

        # Проверяем, есть ли подходящие группы
        for name, extensions in files.items():
            if {".json", ".mp4"} <= extensions:
                log_message(f"Найдена группа файлов: {name} в папке {folder}")
                return folder, name  # Возвращаем папку и имя группы

    log_message("Готовая группа файлов не найдена.")
    return None, None


def update_config_in_b2(client, folder):
    """
    Обновляет config/config_public.json в B2, добавляя папку публикации.
    """
    config_key = "config/config_public.json"
    local_config_path = os.path.join(DOWNLOAD_DIR, "config_public.json")

    # Скачиваем текущий конфиг
    try:
        client.download_file(BUCKET_NAME, config_key, local_config_path)
        with open(local_config_path, "r", encoding="utf-8") as config_file:
            config = json.load(config_file)
    except (BotoCoreError, ClientError, json.JSONDecodeError) as e:
        log_message(f"Ошибка при скачивании или чтении config_public.json: {e}")
        config = {}

    # Обновляем конфиг
    config["publish"] = folder
    with open(local_config_path, "w", encoding="utf-8") as config_file:
        json.dump(config, config_file, indent=4)
    log_message(f"Файл config_public.json обновлен локально: {config}")

    # Загрузка файла обратно в B2
    try:
        client.upload_file(local_config_path, BUCKET_NAME, config_key)
        log_message("Файл config_public.json успешно загружен в B2.")
    except (BotoCoreError, ClientError) as e:
        log_message(f"Ошибка при загрузке config_public.json в B2: {e}")

def download_group(client, folder, group_name):
    """
    Скачивает группу файлов (.json и .mp4) из указанной папки.
    """
    group_files = [f"{folder}{group_name}.json", f"{folder}{group_name}.mp4"]
    os.makedirs(DOWNLOAD_DIR, exist_ok=True)

    for file_key in group_files:
        try:
            local_path = os.path.join(DOWNLOAD_DIR, os.path.basename(file_key))
            log_message(f"Сохранение файла в: {local_path}")
            client.download_file(BUCKET_NAME, file_key, local_path)
            log_message(f"Скачан файл: {file_key}")
        except Exception as e:
            log_message(f"Ошибка скачивания файла {file_key}: {e}")


def main():
    client = create_b2_client()

    # Ищем готовую группу
    folder, group_name = find_ready_group(client)
    if folder and group_name:
        # Скачиваем файлы группы
        download_group(client, folder, group_name)

        # Обновляем конфиг в B2
        update_config_in_b2(client, folder)
    else:
        log_message("Готовая группа не найдена.")

    log_message("Скрипт завершил выполнение.")

if __name__ == "__main__":
    main()


