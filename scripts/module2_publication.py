import os
import json
import subprocess

BASE_DIR = os.path.dirname(os.path.dirname(__file__))
DOWNLOAD_DIR = os.path.join(BASE_DIR, "data", "downloaded")
CONFIG_PATH = os.path.join(BASE_DIR, "config", "config_public.json")

if not os.path.exists(DOWNLOAD_DIR):
    os.makedirs(DOWNLOAD_DIR, exist_ok=True)

if not os.path.exists(CONFIG_PATH):
    with open(CONFIG_PATH, "w") as f:
        json.dump({}, f)


def load_json_data(filename):
    """Загружает JSON-файл, если он существует."""
    path = os.path.join(DOWNLOAD_DIR, filename)

    if not os.path.exists(path):
        print(f"❌ Файл {filename} не найден! Ожидание новых данных...")
        print("📂 Содержимое папки DOWNLOAD_DIR:", os.listdir(DOWNLOAD_DIR))
        return None

    try:
        with open(path, "r", encoding="utf-8") as file:
            return json.load(file)
    except json.JSONDecodeError:
        print(f"❌ Ошибка чтения JSON в {filename}: файл повреждён или пуст.")
        return None


def update_config_no_public():
    """Записывает метку 'no public' в config_public.json."""
    print("⚠️ Устанавливаем метку 'no public'...")
    with open(CONFIG_PATH, "w") as f:
        json.dump({"status": "no public"}, f)


def main():
    """Основной процесс публикации."""
    print("🚀 Запуск публикации...")

    json_filename = "20250116-1932.json"
    post_data = load_json_data(json_filename)

    if not post_data:
        print("⚠️ Нет данных для публикации. Ставим 'no public' и запускаем module1_preparation.py...")
        update_config_no_public()
        subprocess.run(["python", "scripts/module1_preparation.py"], check=True)
        return

    # ✅ Гарантируем, что JSON загружается корректно
    if isinstance(post_data, str):
        try:
            post_data = json.loads(post_data)
        except json.JSONDecodeError:
            print("❌ Ошибка: Некорректный JSON!")
            return

    message = f"🏛 {post_data.get('topic', 'Без темы')}\n\n{post_data.get('text', 'ℹ️ Контент отсутствует.')}"
    print(f"📩 Отправка сообщения: {message}")

    print("✅ Публикация завершена. Запускаем module1_preparation.py...")
    subprocess.run(["python", "scripts/module1_preparation.py"], check=True)


if __name__ == "__main__":
    main()
