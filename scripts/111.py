import os
from b2sdk.v2 import B2Api, InMemoryAccountInfo

# 🔄 Авторизация в B2
info = InMemoryAccountInfo()
b2_api = B2Api(info)

S3_KEY_ID = os.getenv("S3_KEY_ID")
S3_APPLICATION_KEY = os.getenv("S3_APPLICATION_KEY")
S3_BUCKET_NAME = os.getenv("S3_BUCKET_NAME")

if not S3_KEY_ID or not S3_APPLICATION_KEY or not S3_BUCKET_NAME:
    raise ValueError("❌ Ошибка: Переменные окружения S3_KEY_ID, S3_APPLICATION_KEY или S3_BUCKET_NAME не заданы!")

try:
    b2_api.authorize_account("production", S3_KEY_ID, S3_APPLICATION_KEY)
    print("✅ Авторизация в B2 успешна!")
except Exception as e:
    raise RuntimeError(f"❌ Ошибка авторизации в B2: {e}")

# 🔄 Получение bucket'а
try:
    bucket = b2_api.get_bucket_by_name(S3_BUCKET_NAME)
except Exception as e:
    raise RuntimeError(f"❌ Ошибка при получении bucket'а {S3_BUCKET_NAME}: {e}")

# ✅ Используем тот же путь, что и в `module2_publication.py`
BASE_DIR = os.path.dirname(os.path.dirname(__file__))
DOWNLOAD_DIR = os.path.join(BASE_DIR, "data", "downloaded")

if not os.path.exists(DOWNLOAD_DIR):
    os.makedirs(DOWNLOAD_DIR, exist_ok=True)


def download_file(file_name, local_folder):
    local_path = os.path.join(local_folder, os.path.basename(file_name))
    print(f"📥 Попытка скачать: {file_name} в {local_path}")

    try:
        with open(local_path, "wb") as f:
            bucket.download_file_by_name(file_name).save(f)
        print(f"✅ Файл {file_name} сохранён! Проверяем наличие...")

        # 🔍 Проверяем, действительно ли файл существует
        if os.path.exists(local_path):
            print(f"✅ Файл {file_name} присутствует в {local_folder}!")
        else:
            print(f"❌ Файл {file_name} НЕ существует после сохранения!")
    except Exception as e:
        print(f"❌ Ошибка скачивания {file_name}: {e}")


if __name__ == "__main__":
    download_file("444/20250116-1932.json", DOWNLOAD_DIR)
