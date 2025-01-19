import os
import json
import boto3
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

# Настройки B2 из переменных окружения
ENDPOINT = os.getenv("S3_ENDPOINT")
BUCKET_NAME = os.getenv("S3_BUCKET_NAME")
KEY_ID = os.getenv("S3_KEY_ID")
APPLICATION_KEY = os.getenv("S3_APPLICATION_KEY")

# Логирование
def log_message(message):
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
        log_message("✅ Успешное подключение к B2.")
        return client
    except Exception as e:
        log_message(f"❌ Ошибка подключения к B2: {e}")
        raise

# Функция поиска группы файлов
def find_ready_group(client):
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

        for name, extensions in files.items():
            if {".json", ".mp4"} <= extensions:
                log_message(f"✅ Найдена группа файлов: {name} в папке {folder}")
                return folder, name

    log_message("❌ Готовая группа файлов не найдена.")
    return None, None

# Скачивание группы файлов
def download_group(client, folder, group_name):
    group_files = [f"{folder}{group_name}.json", f"{folder}{group_name}.mp4"]
    os.makedirs(DOWNLOAD_DIR, exist_ok=True)

    for file_key in group_files:
        try:
            local_path = os.path.join(DOWNLOAD_DIR, os.path.basename(file_key))
            log_message(f"📂 Пытаемся скачать: {BUCKET_NAME}/{file_key}")
            client.download_file(BUCKET_NAME, file_key, local_path, ExtraArgs={"ChecksumMode": "NONE"})
            log_message(f"✅ Скачан файл: {file_key}")
        except Exception as e:
            log_message(f"❌ Ошибка скачивания файла {file_key}: {e}")

# Функция загрузки config_public.json
def fetch_config_from_b2(client):
    config_key = "config/config_public.json"
    local_config_path = os.path.join(DOWNLOAD_DIR, "config_public.json")

    try:
        log_message(f"📂 Пытаемся скачать config: {BUCKET_NAME}/{config_key}")
        client.download_file(BUCKET_NAME, config_key, local_config_path, ExtraArgs={"ChecksumMode": "NONE"})
        with open(local_config_path, "r", encoding="utf-8") as config_file:
            config = json.load(config_file)
        log_message("✅ Файл config_public.json успешно скачан и прочитан.")
        return config
    except (BotoCoreError, ClientError, json.JSONDecodeError) as e:
        log_message(f"❌ Ошибка при скачивании или чтении config_public.json: {e}")
        return {}

# Обновление config_public.json
def update_config_in_b2(client, folder):
    config_key = "config/config_public.json"
    local_config_path = os.path.join(DOWNLOAD_DIR, "config_public.json")

    try:
        log_message(f"📂 Проверяем путь к файлу в B2: {BUCKET_NAME}/{config_key}")
        client.download_file(BUCKET_NAME, config_key, local_config_path, ExtraArgs={"ChecksumMode": "NONE"})
        with open(local_config_path, "r", encoding="utf-8") as config_file:
            config = json.load(config_file)
        log_message(f"📄 Текущее содержимое config_public.json: {config}")
    except (BotoCoreError, ClientError, json.JSONDecodeError) as e:
        log_message(f"❌ Ошибка при скачивании или чтении config_public.json: {e}")
        config = {}

    config["publish"] = folder
    with open(local_config_path, "w", encoding="utf-8") as config_file:
        json.dump(config, config_file, indent=4)
    log_message(f"✅ Файл config_public.json обновлен локально: {config}")

    try:
        client.upload_file(local_config_path, BUCKET_NAME, config_key)
        log_message("✅ Файл config_public.json успешно загружен в B2.")
    except (BotoCoreError, ClientError) as e:
        log_message(f"❌ Ошибка при загрузке config_public.json в B2: {e}")

# Главная функция
def main():
    client = create_b2_client()

    folder, group_name = find_ready_group(client)
    if folder and group_name:
        download_group(client, folder, group_name)
        update_config_in_b2(client, folder)
    else:
        log_message("❌ Готовая группа не найдена.")

    log_message("✅ Скрипт завершил выполнение.")

if __name__ == "__main__":
    main()
