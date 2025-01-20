import os
import subprocess
import json

from b2sdk.v2 import B2Api, InMemoryAccountInfo

# 🔄 Авторизация в B2
info = InMemoryAccountInfo()
b2_api = B2Api(info)

S3_KEY_ID = os.getenv("S3_KEY_ID")
S3_APPLICATION_KEY = os.getenv("S3_APPLICATION_KEY")
S3_BUCKET_NAME = os.getenv("S3_BUCKET_NAME")
GH_TOKEN = os.getenv("GH_TOKEN")

RUNNING_IN_GITHUB = os.getenv("GITHUB_ACTIONS") == "true"

# Выводим отладочную информацию
print(f"✅ GH_TOKEN: {'установлен' if GH_TOKEN else '❌ НЕ установлен'}")
print(f"✅ S3_KEY_ID: {'установлен' if S3_KEY_ID else '❌ НЕ установлен'}")
print(f"✅ S3_APPLICATION_KEY: {'установлен' if S3_APPLICATION_KEY else '❌ НЕ установлен'}")
print(f"✅ S3_BUCKET_NAME: {'установлен' if S3_BUCKET_NAME else '❌ НЕ установлен'}")

if RUNNING_IN_GITHUB and not GH_TOKEN:
    raise RuntimeError("❌ GH_TOKEN отсутствует в GitHub Actions!")

# Передаём GH_TOKEN в окружение GitHub CLI
if GH_TOKEN:
    os.environ["GH_TOKEN"] = GH_TOKEN
    print("✅ GH_TOKEN установлен.")
else:
    print("⚠️ ВНИМАНИЕ: GH_TOKEN не передан! GitHub CLI может не работать.")

if not S3_KEY_ID or not S3_APPLICATION_KEY or not S3_BUCKET_NAME:
    print("❌ Ошибка: Переменные окружения S3_KEY_ID, S3_APPLICATION_KEY или S3_BUCKET_NAME не заданы!")
    print("⚠️ Используй команду `set S3_KEY_ID=your_key_id` перед запуском.")
    exit(1)  # Завершаем скрипт, но не выбрасываем исключение

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

def delete_old_artifact():
    print("🗑️ Проверяем, существует ли артефакт downloaded_files...")
    result = subprocess.run(["gh", "api", "repos/OWNER/REPO/actions/artifacts"], capture_output=True, text=True)

    if "downloaded_files" in result.stdout:
        print("🗑️ Удаляем артефакт downloaded_files...")
        subprocess.run(["gh", "api", "-X", "DELETE", "/repos/OWNER/REPO/actions/artifacts/ID"], check=False)
        print("✅ Артефакт deleted_files удалён.")
    else:
        print("⚠️ Артефакт downloaded_files не найден, пропускаем удаление.")


delete_old_artifact()

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


def check_artifacts():
    print("📥 Проверяем доступные артефакты...")
    result = subprocess.run(["gh", "api", "repos/${{ github.repository }}/actions/artifacts"], capture_output=True,
                            text=True)

    try:
        artifacts = json.loads(result.stdout)
        if not artifacts["artifacts"]:
            print("⚠️ Нет доступных артефактов.")
            return False
        for artifact in artifacts["artifacts"]:
            print(f"📂 Найден артефакт: {artifact['name']}")
        return True
    except json.JSONDecodeError:
        print("❌ Ошибка: Невозможно распарсить список артефактов.")
        return False


if check_artifacts():
    restore_files_from_artifacts()
else:
    print("⚠️ Пропускаем восстановление артефактов.")


def restore_files_from_artifacts():
    print("📥 Восстановление файлов из артефактов...")
    artifact_name = "downloaded_files"

    # Получаем список артефактов
    result = subprocess.run(["gh", "api", "repos/Boyarinn1/a1/actions/artifacts"], capture_output=True, text=True)

    if artifact_name in result.stdout:
        print(f"✅ Артефакт {artifact_name} найден. Восстанавливаем файлы...")
        os.makedirs("/home/runner/work/a1/a1/data/downloaded", exist_ok=True)
        subprocess.run(["gh", "run", "download", "--name", artifact_name], check=False)
        print("✅ Файлы успешно восстановлены из артефактов.")
    else:
        print(f"⚠️ Артефакт {artifact_name} не найден, пропускаем восстановление.")


restore_files_from_artifacts()


if __name__ == "__main__":
    clear_old_files()  # Удаляем старую группу
    restore_files_from_artifacts()  # Восстанавливаем файлы из артефактов
    download_new_files()  # Загружаем новые файлы из B2
