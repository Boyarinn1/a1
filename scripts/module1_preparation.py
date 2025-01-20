import os
import json
import subprocess
from b2sdk.v2 import B2Api, InMemoryAccountInfo

# 🔄 Авторизация в B2
info = InMemoryAccountInfo()
b2_api = B2Api(info)

S3_KEY_ID = os.getenv("S3_KEY_ID")
S3_APPLICATION_KEY = os.getenv("S3_APPLICATION_KEY")
S3_BUCKET_NAME = os.getenv("S3_BUCKET_NAME")

BASE_DIR = os.path.dirname(os.path.dirname(__file__))
DOWNLOAD_DIR = os.path.join(BASE_DIR, "data", "downloaded")
CONFIG_PATH = os.path.join(BASE_DIR, "config", "config_public.json")

if not os.path.exists(DOWNLOAD_DIR):
    os.makedirs(DOWNLOAD_DIR, exist_ok=True)

if not os.path.exists(CONFIG_PATH):
    with open(CONFIG_PATH, "w") as f:
        json.dump({}, f)

try:
    b2_api.authorize_account("production", S3_KEY_ID, S3_APPLICATION_KEY)
    print("✅ Авторизация в B2 успешна!")
except Exception as e:
    raise RuntimeError(f"❌ Ошибка авторизации в B2: {e}")

bucket = b2_api.get_bucket_by_name(S3_BUCKET_NAME)

def load_config():
    """Загружает config_public.json"""
    with open(CONFIG_PATH, "r", encoding="utf-8") as f:
        return json.load(f)

def save_config(data):
    """Сохраняет config_public.json"""
    with open(CONFIG_PATH, "w", encoding="utf-8") as f:
        json.dump(data, f)

def clear_old_files():
    """Удаляет старые файлы, но сохраняет системные файлы."""
    print(f"🗑️ Очистка {DOWNLOAD_DIR}...")
    for file in os.listdir(DOWNLOAD_DIR):
        if file in [".gitkeep", ".DS_Store"]:
            continue
        os.remove(os.path.join(DOWNLOAD_DIR, file))
    print("✅ Папка очищена.")

def download_new_files():
    """Загружает новую группу файлов из B2."""
    print("📥 Поиск новых файлов в B2...")
    json_file = None
    mp4_file = None

    for file_version, _ in bucket.ls("444/", recursive=True):
        file_name = file_version.file_name
        if file_name.endswith(".json"):
            json_file = file_name
            mp4_file = file_name.replace(".json", ".mp4")
            break

    if not json_file or not mp4_file:
        print("⚠️ Нет новых файлов для загрузки!")
        return

    print(f"📥 Скачиваем {json_file} и {mp4_file}...")
    for file_name in [json_file, mp4_file]:
        local_path = os.path.join(DOWNLOAD_DIR, os.path.basename(file_name))
        try:
            with open(local_path, "wb") as f:
                bucket.download_file_by_name(file_name).save(f)
            print(f"✅ {file_name} загружен в {local_path}")
        except Exception as e:
            print(f"❌ Ошибка загрузки {file_name}: {e}")

if __name__ == "__main__":
    clear_old_files()
    download_new_files()

    config = load_config()
    if config.get("status") == "no public":
        print("⚠️ Метка 'no public' найдена! Загружаем ещё одну группу и удаляем метку.")
        download_new_files()
        save_config({"status": "ready"})

    print("🚀 Запуск module2_publication.py...")
    subprocess.run(["python", "scripts/module2_publication.py"], check=True)
