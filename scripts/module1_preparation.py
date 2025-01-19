import os
import json
from botocore.exceptions import ClientError
from modules.api_clients import get_b2_client
from modules.logger import get_logger
from modules.config_manager import ConfigManager
from modules.b2_storage_manager import list_files_in_folder, download_file, save_config_public, load_config_public, \
    handle_publish, get_ready_groups

dotenv_path = os.path.join(os.path.dirname(__file__), ".env")
BASE_DIR = os.path.dirname(os.path.dirname(__file__))
DOWNLOAD_DIR = os.path.join(BASE_DIR, "data", "downloaded")
os.makedirs(DOWNLOAD_DIR, exist_ok=True)

# === Инициализация логирования и конфигурации ===
config = ConfigManager()
logger = get_logger("module1_preparation")

B2_BUCKET_NAME = config.get("API_KEYS.b2.bucket_name")
CONFIG_PUBLIC_PATH = config.get("FILE_PATHS.config_public")

# Подключение к B2
client = get_b2_client()


def download_group(folder):
    files = list_files_in_folder(client, folder)
    ready_groups = get_ready_groups(files)

    if not ready_groups:
        logger.warning(f"⚠️ В папке {folder} нет готовых групп файлов.")
        return

    for group_id in ready_groups:
        for ext in ['.json', '.mp4']:
            file_key = f"{folder}{group_id}{ext}"
            local_path = os.path.join(DOWNLOAD_DIR, os.path.basename(file_key))
            download_file(client, file_key, local_path)


def update_config_in_b2(folder):
    config_data = load_config_public(client)
    config_data["publish"] = folder
    save_config_public(client, config_data)


def main():
    folder = "444/"  # Пример папки
    download_group(folder)
    update_config_in_b2(folder)
    handle_publish(client, load_config_public(client))
    logger.info("✅ Скрипт завершил выполнение.")


if __name__ == "__main__":
    main()
