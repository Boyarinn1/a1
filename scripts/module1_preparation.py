import os
import subprocess
from b2sdk.v2 import B2Api, InMemoryAccountInfo

# 🔄 Авторизация в B2
info = InMemoryAccountInfo()
b2_api = B2Api(info)

S3_KEY_ID = os.getenv("S3_KEY_ID")
S3_APPLICATION_KEY = os.getenv("S3_APPLICATION_KEY")
S3_BUCKET_NAME = os.getenv("S3_BUCKET_NAME")
GH_TOKEN = os.getenv("GH_TOKEN")

GH_TOKEN = os.getenv("GH_TOKEN")
if GH_TOKEN:
    os.environ["GH_TOKEN"] = GH_TOKEN
    print("✅ GH_TOKEN установлен.")
else:
    print("⚠️ ВНИМАНИЕ: GH_TOKEN не передан! GitHub CLI может не работать.")


print("🔄 module1_preparation.py запущен!")

if not GH_TOKEN:
    print("⚠️ ВНИМАНИЕ: GH_TOKEN не установлен! Артефакты не будут загружены.")
else:
    os.environ["GH_TOKEN"] = GH_TOKEN

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


def clear_old_files():
    """Удаляет старые файлы из артефактов и рабочей папки."""
    print(f"🗑️ Очистка {DOWNLOAD_DIR}...")
    for file in os.listdir(DOWNLOAD_DIR):
        file_path = os.path.join(DOWNLOAD_DIR, file)
        os.remove(file_path)
    print("✅ Папка очищена.")

    # Удаляем старые артефакты из GitHub Actions
    print("🗑️ Удаление старых артефактов из GitHub...")
    subprocess.run(["gh", "run", "delete", "downloaded_files"], check=False)
    print("✅ Старые артефакты удалены.")


def restore_files_from_artifacts():
    """Скачивает артефакты обратно в рабочую папку."""
    print("📥 Восстановление файлов из артефактов...")
    subprocess.run(["gh", "run", "download", "--name", "downloaded_files", "--dir", DOWNLOAD_DIR], check=False)
    print(f"✅ Файлы восстановлены в {DOWNLOAD_DIR}")


def download_new_files():
    """Загружает новую неопубликованную группу файлов из B2."""
    print("📥 Поиск новых файлов в B2...")
    json_file = None
    mp4_file = None

    for file_version, _ in bucket.ls("444/", recursive=True):
        file_name = file_version.file_name  # ✅ Получаем имя файла из FileVersion
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
    clear_old_files()  # Удаляем старую группу
    restore_files_from_artifacts()  # Восстанавливаем файлы из артефактов
    download_new_files()  # Загружаем новые файлы из B2
