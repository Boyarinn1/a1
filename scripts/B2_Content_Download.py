import os
import json
import b2sdk.v2
import subprocess

# 🔹 Определяем пути
BASE_DIR = os.path.dirname(os.path.dirname(__file__))  # a1/
DOWNLOAD_DIR = os.path.join(BASE_DIR, "data", "downloaded")  # a1/data/downloaded
CONFIG_PATH = os.path.join(BASE_DIR, "config", "config_public.json")  # a1/config/config_public.json

# 🔹 Создаём директории, если их нет
os.makedirs(DOWNLOAD_DIR, exist_ok=True)

# 🔹 Загружаем переменные окружения
S3_KEY_ID = os.getenv("S3_KEY_ID")
S3_APPLICATION_KEY = os.getenv("S3_APPLICATION_KEY")
S3_BUCKET_NAME = os.getenv("S3_BUCKET_NAME")
GH_TOKEN = os.getenv("GH_TOKEN")

# 🔹 Проверка переменных
if not all([S3_KEY_ID, S3_APPLICATION_KEY, S3_BUCKET_NAME]):
    raise RuntimeError("❌ Ошибка: Переменные S3_KEY_ID, S3_APPLICATION_KEY или S3_BUCKET_NAME не заданы!")

# 🔹 Авторизация в B2
print("✅ Авторизация в B2...")
info = b2sdk.v2.InMemoryAccountInfo()
b2_api = b2sdk.v2.B2Api(info)
b2_api.authorize_account("production", S3_KEY_ID, S3_APPLICATION_KEY)

# 🔹 Получаем bucket
bucket = b2_api.get_bucket_by_name(S3_BUCKET_NAME)

# 🔹 Поиск файлов в 444/
print("📥 Поиск файлов в B2 (папка 444/)...")
files_to_download = []
for file_version, _ in bucket.ls("444/", recursive=True):
    if file_version.file_name.endswith((".json", ".mp4", ".png")):  # Фильтр по типам
        files_to_download.append(file_version.file_name)

if not files_to_download:
    print("⚠️ Нет файлов для загрузки. Ожидание новых данных...")
    # Ставим статус ожидания в config_public.json
    with open(CONFIG_PATH, "w", encoding="utf-8") as f:
        json.dump({"status": "waiting", "files": []}, f, indent=4)
    exit(0)

# 🔹 Загружаем файлы в DOWNLOAD_DIR
downloaded_files = []
for file_name in files_to_download:
    local_path = os.path.join(DOWNLOAD_DIR, os.path.basename(file_name))
    print(f"📥 Скачивание {file_name} в {local_path}...")
    bucket.download_file_by_name(file_name, local_path)
    downloaded_files.append(os.path.basename(file_name))

print("✅ Загрузка завершена. Загруженные файлы:", downloaded_files)

# 🔹 Записываем список файлов в config_public.json
with open(CONFIG_PATH, "w", encoding="utf-8") as f:
    json.dump({"status": "ready", "files": downloaded_files}, f, indent=4)

# 🔹 Если работаем в GitHub Actions, создаём артефакт
if os.getenv("GITHUB_ACTIONS"):
    print("📂 Загружаем файлы как артефакт в GitHub Actions...")

    # Проверяем, передан ли GH_TOKEN
    if not GH_TOKEN:
        print("⚠️ Ошибка: GH_TOKEN не установлен! Артефакты не будут загружены.")
    else:
        subprocess.run(["zip", "-r", "downloaded_files.zip", DOWNLOAD_DIR], check=True)
        subprocess.run(["gh", "artifact", "upload", "downloaded_files", DOWNLOAD_DIR], check=True)

print("🚀 Скрипт завершён.")
