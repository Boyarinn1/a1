import os
import boto3

from botocore.config import Config
from dotenv import load_dotenv


# Константы
BUCKET_NAME = os.getenv("S3_BUCKET_NAME")
DOWNLOAD_DIR = "data/downloaded/"
os.makedirs(DOWNLOAD_DIR, exist_ok=True)


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

def get_b2_client():
    """Создает клиент для работы с Backblaze B2."""
    return boto3.client(
        "s3",
        endpoint_url=os.getenv("S3_ENDPOINT"),
        aws_access_key_id=os.getenv("S3_KEY_ID"),
        aws_secret_access_key=os.getenv("S3_APPLICATION_KEY"),
        config=Config(signature_version="s3v4")
    )

# Подключение к B2
def create_b2_client():
    return get_b2_client()  # Используем рабочий метод из `b2_storage_manager.py`



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

# Скачивание файлов
def download_group(client, folder, group_name):
    group_files = [f"{folder}{group_name}.json", f"{folder}{group_name}.mp4"]

    for file_key in group_files:
        local_path = os.path.join(DOWNLOAD_DIR, os.path.basename(file_key))
        print(f"📥 Скачивание {file_key} в {local_path}")

        try:
            client.download_file(BUCKET_NAME, file_key, local_path)  # Используем проверенный метод
            print(f"✅ Файл скачан: {file_key}")
        except ClientError as e:
            print(f"❌ Ошибка скачивания {file_key}: {e.response['Error']['Message']}")

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
