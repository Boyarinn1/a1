import os
import json
import logging
from botocore.exceptions import ClientError
import boto3


def get_b2_client():
    session = boto3.session.Session()
    return session.client(
        's3',
        endpoint_url=os.getenv('S3_ENDPOINT'),
        aws_access_key_id=os.getenv('S3_KEY_ID'),
        aws_secret_access_key=os.getenv('S3_APPLICATION_KEY'),
        config=boto3.session.Config(signature_version='s3')
    )


def download_file(client, bucket_name, file_key, local_path):
    try:
        with open(local_path, 'wb') as f:
            client.download_fileobj(bucket_name, file_key, f)
        print(f"✅ Файл {file_key} успешно загружен в {local_path}")
    except ClientError as e:
        print(f"❌ Ошибка скачивания {file_key}: {e.response['Error']['Message']}")


def main():
    client = get_b2_client()
    bucket_name = os.getenv('S3_BUCKET_NAME')
    download_dir = "data/downloaded"
    os.makedirs(download_dir, exist_ok=True)

    files_to_download = [
        "444/20250116-1932.json",
        "444/20250116-1932.mp4"
    ]

    for file_key in files_to_download:
        local_path = os.path.join(download_dir, os.path.basename(file_key))
        download_file(client, bucket_name, file_key, local_path)


if __name__ == "__main__":
    main()
