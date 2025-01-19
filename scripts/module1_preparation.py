import os
import boto3
from botocore.exceptions import ClientError


def get_b2_client():
    return boto3.client(
        's3',
        endpoint_url=os.getenv('S3_ENDPOINT'),
        aws_access_key_id=os.getenv('S3_KEY_ID'),
        aws_secret_access_key=os.getenv('S3_APPLICATION_KEY')
    )


def list_b2_files(s3, bucket_name):
    response = s3.list_objects_v2(Bucket=bucket_name)
    if 'Contents' in response:
        for obj in response['Contents']:
            print(f"📄 {obj['Key']} ({obj['Size']} bytes)")
    else:
        print("❌ Нет файлов в хранилище B2")


def download_file(s3, bucket_name, file_key, local_path):
    try:
        os.makedirs(os.path.dirname(local_path), exist_ok=True)
        s3.download_file(bucket_name, file_key, local_path)
        print(f"✅ Файл {file_key} успешно скачан в {local_path}")
    except ClientError as e:
        print(f"❌ Ошибка скачивания {file_key}: {e.response['Error']['Message']}")


def main():
    bucket_name = os.getenv('S3_BUCKET_NAME')
    s3 = get_b2_client()

    print("\n📂 Листинг файлов в B2:")
    list_b2_files(s3, bucket_name)

    file_key = "444/20250116-1932.json"
    local_path = "a1/data/downloaded/20250116-1932.json"
    download_file(s3, bucket_name, file_key, local_path)


if __name__ == "__main__":
    main()
