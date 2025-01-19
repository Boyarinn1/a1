import os
import boto3
from botocore.exceptions import ClientError

BUCKET_NAME = os.getenv("S3_BUCKET_NAME")
DOWNLOAD_DIR = "data/downloaded/"
os.makedirs(DOWNLOAD_DIR, exist_ok=True)

def get_b2_client():
    """Создает клиент B2 без лишних заголовков."""
    return boto3.client(
        "s3",
        endpoint_url=os.getenv("S3_ENDPOINT"),
        aws_access_key_id=os.getenv("S3_KEY_ID"),
        aws_secret_access_key=os.getenv("S3_APPLICATION_KEY")
    )

def download_group(client, folder, group_name):
    """Скачивает файлы .json и .mp4 из B2 в локальную папку."""
    group_files = [f"{folder}{group_name}.json", f"{folder}{group_name}.mp4"]

    for file_key in group_files:
        local_path = os.path.join(DOWNLOAD_DIR, os.path.basename(file_key))
        print(f"📥 Скачивание {file_key} в {local_path}")

        try:
            response = client.get_object(Bucket=BUCKET_NAME, Key=file_key)
            with open(local_path, "wb") as f:
                f.write(response["Body"].read())  # Читаем данные и записываем в файл
            print(f"✅ Файл скачан: {file_key}")
        except ClientError as e:
            print(f"❌ Ошибка скачивания {file_key}: {e.response['Error']['Message']}")
        except Exception as e:
            print(f"❌ Ошибка: {e}")

def main():
    client = get_b2_client()
    folder = "444/"
    group_name = "20250116-1932"
    download_group(client, folder, group_name)

if __name__ == "__main__":
    main()
