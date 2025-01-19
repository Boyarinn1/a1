import os
import boto3
from botocore.exceptions import NoCredentialsError, ClientError

S3_KEY_ID = os.getenv("S3_KEY_ID")
S3_APPLICATION_KEY = os.getenv("S3_APPLICATION_KEY")
S3_ENDPOINT = os.getenv("S3_ENDPOINT")
S3_BUCKET_NAME = os.getenv("S3_BUCKET_NAME")

session = boto3.session.Session()
client = session.client(
    service_name="s3",
    aws_access_key_id=S3_KEY_ID,
    aws_secret_access_key=S3_APPLICATION_KEY,
    endpoint_url=S3_ENDPOINT,
)

def list_files():
    try:
        for folder in ["444/", "555/", "666/"]:
            response = client.list_objects_v2(Bucket=S3_BUCKET_NAME, Prefix=folder)
            if "Contents" in response:
                print(f"\n📂 Папка: {folder}")
                for obj in response["Contents"]:
                    print(f"📄 {obj['Key']} ({obj['Size']} bytes)")
                print("-" * 40)
    except ClientError as e:
        print(f"❌ Ошибка листинга: {e}")

def download_file():
    file_key = "444/20250116-1932.json"
    local_path = "a1/data/downloaded/20250116-1932.json"
    try:
        client.download_file(S3_BUCKET_NAME, file_key, local_path)
        print(f"📥 Файл {file_key} загружен в {local_path}")
    except ClientError as e:
        print(f"❌ Ошибка скачивания {file_key}: {e}")

if __name__ == "__main__":
    list_files()
    download_file()
