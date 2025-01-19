import os
import boto3
from botocore.config import Config

def download_json_from_b2():
    # Получаем конфигурацию из переменных окружения GitHub Secrets
    S3_KEY_ID = os.getenv("S3_KEY_ID")
    S3_APPLICATION_KEY = os.getenv("S3_APPLICATION_KEY")
    S3_ENDPOINT = os.getenv("S3_ENDPOINT")
    S3_BUCKET_NAME = os.getenv("S3_BUCKET_NAME")
    FILE_NAME = "data.json"  # Имя файла в B2
    SAVE_PATH = "a1/data/downloaded/"  # Папка для сохранения

    os.makedirs(SAVE_PATH, exist_ok=True)

    # Подключение к B2
    s3 = boto3.client(
        "s3",
        aws_access_key_id=S3_KEY_ID,
        aws_secret_access_key=S3_APPLICATION_KEY,
        endpoint_url=S3_ENDPOINT,
        config=Config(signature_version="s3v4")
    )

    local_file_path = os.path.join(SAVE_PATH, FILE_NAME)

    try:
        # Скачивание файла
        s3.download_file(S3_BUCKET_NAME, FILE_NAME, local_file_path)
        print(f"✅ Файл {FILE_NAME} загружен в {local_file_path}")
    except Exception as e:
        print(f"❌ Ошибка при загрузке файла: {e}")

if __name__ == "__main__":
    download_json_from_b2()
