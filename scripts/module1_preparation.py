import boto3
import os

# Получение переменных окружения
S3_KEY_ID = os.getenv("S3_KEY_ID")
S3_APPLICATION_KEY = os.getenv("S3_APPLICATION_KEY")
S3_ENDPOINT = os.getenv("S3_ENDPOINT")
S3_BUCKET_NAME = os.getenv("S3_BUCKET_NAME")

# Подключение к B2
client = boto3.client(
    "s3",
    aws_access_key_id=S3_KEY_ID,
    aws_secret_access_key=S3_APPLICATION_KEY,
    endpoint_url=S3_ENDPOINT,
)


# Функция для листинга файлов в папках
def list_files_in_folder(folder):
    response = client.list_objects_v2(Bucket=S3_BUCKET_NAME, Prefix=folder)
    files = response.get("Contents", [])

    print(f"📂 Папка: {folder}")
    if not files:
        print("❌ Файлы не найдены")
    else:
        for file in files:
            print(f"📄 {file['Key']} ({file['Size']} bytes)")
    print("-" * 40)


# Листинг файлов в трех папках
for folder in ["444/", "555/", "666/"]:
    list_files_in_folder(folder)
