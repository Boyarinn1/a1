import os
import json
import boto3
from botocore.exceptions import BotoCoreError, ClientError
from datetime import datetime
from dotenv import load_dotenv

# Загрузка переменных окружения
dotenv_path = os.path.join(os.path.dirname(__file__), ".env")
load_dotenv(dotenv_path)

# Определяем пути
BASE_DIR = os.path.dirname(os.path.dirname(__file__))
DOWNLOAD_DIR = os.path.join(BASE_DIR, "data", "downloaded")

# Создание папки для скачивания
os.makedirs(DOWNLOAD_DIR, exist_ok=True)

# Настройки B2
ENDPOINT = os.getenv("S3_ENDPOINT")
BUCKET_NAME = os.getenv("S3_BUCKET_NAME")
KEY_ID = os.getenv("S3_KEY_ID")
APPLICATION_KEY = os.getenv("S3_APPLICATION_KEY")

# Подключение к B2
def create_b2_client():
    return boto3.client(
        's3',
        endpoint_url=ENDPOINT,
        aws_access_key_id=KEY_ID,
        aws_secret_access_key=APPLICATION_KEY
    )

# Поиск готовой группы файлов
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
                return folder, name
    return None, None

# Скачивание группы файлов
def download_group(client, folder, group_name):
    group_files = [f"{folder}{group_name}.json", f"{folder}{group_name}.mp4"]
    for file_key in group_files:
        local_path = os.path.join(DOWNLOAD_DIR, os.path.basename(file_key))
        client.download_file(BUCKET_NAME, file_key, local_path)

# Основная логика
def main():
    client = create_b2_client()
    folder, group_name = find_ready_group(client)
    if folder and group_name:
        download_group(client, folder, group_name)
    else:
        print("Готовая группа файлов не найдена.")

if __name__ == "__main__":
    main()
