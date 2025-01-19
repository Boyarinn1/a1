import os
import boto3
from botocore.config import Config

def download_json_from_b2():
    # Получаем конфигурацию из переменных окружения GitHub Secrets
    S3_KEY_ID = os.getenv("S3_KEY_ID")
    S3_APPLICATION_KEY = os.getenv("S3_APPLICATION_KEY")
    S3_ENDPOINT = os.getenv("S3_ENDPOINT")
    S3_BUCKET_NAME = os.getenv("S3_BUCKET_NAME")
    FILE_NAME = "data.json"  # Загружаемый файл
    SAVE_PATH = "a1/data/downloaded/"

    # Отладочный вывод
    print(f"S3_KEY_ID: {S3_KEY_ID}")
    print(f"S3_APPLICATION_KEY: {'✔' if S3_APPLICATION_KEY else '❌ None'}")
    print(f"S3_ENDPOINT: {S3_ENDPOINT}")
    print(f"S3_BUCKET_NAME: {S3_BUCKET_NAME}")

    # Проверяем, что все переменные заданы
    if not all([S3_KEY_ID, S3_APPLICATION_KEY, S3_ENDPOINT, S3_BUCKET_NAME]):
        print("❌ Ошибка: не все переменные окружения заданы!")
        return

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
        # Проверка существования файла перед скачиванием
        response = s3.head_object(Bucket=S3_BUCKET_NAME, Key=FILE_NAME)
        print(f"📂 Файл найден в B2: {response}")

        # Скачивание файла
        s3.download_file(S3_BUCKET_NAME, FILE_NAME, local_file_path)
        print(f"✅ Файл {FILE_NAME} загружен в {local_file_path}")

    except s3.exceptions.NoSuchKey:
        print(f"❌ Ошибка: Файл {FILE_NAME} не найден в бакете {S3_BUCKET_NAME}!")
    except Exception as e:
        print(f"❌ Ошибка при загрузке файла: {e}")

if __name__ == "__main__":
    download_json_from_b2()
