import os
import boto3

# 🔹 Загружаем переменные окружения
S3_KEY_ID = os.getenv("S3_KEY_ID")
S3_APPLICATION_KEY = os.getenv("S3_APPLICATION_KEY")
S3_BUCKET_NAME = os.getenv("S3_BUCKET_NAME")
S3_ENDPOINT = os.getenv("S3_ENDPOINT")

# 🔹 Путь для сохранения файлов
LOCAL_DIR = r"C:\Users\boyar\core\b2"

# 🔹 Подключаемся к B2
s3 = boto3.client(
    's3',
    endpoint_url=S3_ENDPOINT,
    aws_access_key_id=S3_KEY_ID,
    aws_secret_access_key=S3_APPLICATION_KEY
)

# ✅ Создаём локальную папку, если её нет
os.makedirs(LOCAL_DIR, exist_ok=True)

# 🔹 Получаем список файлов в папке 666/
response = s3.list_objects_v2(Bucket=S3_BUCKET_NAME, Prefix="666/")

if "Contents" not in response:
    print("⚠️ В B2 нет файлов в папке 666/")
else:
    for obj in response["Contents"]:
        file_name = obj["Key"]  # Полный путь в B2 (666/...)
        local_path = os.path.join(LOCAL_DIR, os.path.basename(file_name))  # Локальный путь

        print(f"📥 Загружаем {file_name} в {local_path}...")

        # 🔹 Скачиваем файл
        with open(local_path, "wb") as f:
            s3.download_fileobj(S3_BUCKET_NAME, file_name, f)

    print("✅ Все файлы успешно загружены!")
