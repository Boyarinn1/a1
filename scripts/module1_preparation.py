import os
from b2sdk.v2 import B2Api, InMemoryAccountInfo

# Подключение к B2
info = InMemoryAccountInfo()
b2_api = B2Api(info)
b2_api.authorize_account("production", os.getenv("S3_KEY_ID"), os.getenv("S3_APPLICATION_KEY"))

BUCKET_NAME = os.getenv("S3_BUCKET_NAME")
bucket = b2_api.get_bucket_by_name(BUCKET_NAME)

DOWNLOAD_DIR = "/home/runner/work/a1/a1/data/downloaded"
LOCAL_DOWNLOAD_DIR = "C:/Users/boyar/a1/data/downloaded"  # Локальная папка Windows


def list_files():
    """
    Выводит список файлов в B2.
    """
    for folder in ["444/", "555/", "666/"]:
        print(f"\n📂 Папка: {folder}")
        for file_info in bucket.ls(folder, recursive=True):
            print(f"📄 {file_info[0]} ({file_info[1]} bytes)")
        print("-" * 40)


def download_file(file_name, local_folder):
    """
    Скачивает файл из B2 и сохраняет локально.
    """
    print(f"📥 Скачивание {file_name} в {local_folder}/{file_name.split('/')[-1]}")

    local_path = os.path.join(local_folder, file_name.split("/")[-1])
    print(f"Файл сохраняется в: {local_path}")  # Отладочный вывод пути

    try:
        downloaded_file = bucket.download_file_by_name(file_name, local_path)
        print("✅ Файл успешно скачан!")
    except Exception as e:
        print(f"❌ Ошибка скачивания {file_name}: {e}")

    if os.path.exists(local_path):
        print("✅ Файл присутствует в папке!")
    else:
        print("❌ Файл НЕ скачался!")


def check_downloaded_files():
    """
    Проверяет, какие файлы уже скачаны.
    """
    print(f"\n📂 DOWNLOAD_DIR: {DOWNLOAD_DIR}")
    print(f"📂 Содержимое папки: {os.listdir(DOWNLOAD_DIR)}")


if __name__ == "__main__":
    list_files()  # Листинг файлов в B2
    download_file("444/20250116-1932.json", DOWNLOAD_DIR)  # Скачивание файла
    check_downloaded_files()  # Проверка скачанных файлов
