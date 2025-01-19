from b2sdk.v2 import B2Api, InMemoryAccountInfo
import os

# Константы для работы с B2
B2_KEY_ID = os.getenv("S3_KEY_ID")
B2_APP_KEY = os.getenv("S3_APPLICATION_KEY")
B2_BUCKET_NAME = os.getenv("S3_BUCKET_NAME")
DOWNLOAD_DIR = "data/downloaded"

# Инициализация B2 API
info = InMemoryAccountInfo()
b2_api = B2Api(info)
b2_api.authorize_account("production", B2_KEY_ID, B2_APP_KEY)

# Получаем ссылку на бакет
bucket = b2_api.get_bucket_by_name(B2_BUCKET_NAME)

# Листинг файлов в папках 444/, 555/, 666/
folders = ["444/", "555/", "666/"]
for folder in folders:
    print(f"\n📂 Папка: {folder}")
    for file in bucket.ls(folder, recursive=True):
        print(f"📄 {file.file_name} ({file.size} bytes)")
    print("-" * 40)

# Скачивание конкретного файла
file_name = "444/20250116-1932.json"
local_path = os.path.join(DOWNLOAD_DIR, os.path.basename(file_name))

print(f"\n📥 Скачивание {file_name} в {local_path}")
downloaded_file = bucket.download_file_by_name(file_name, local_path)
print("✅ Файл успешно скачан!")
