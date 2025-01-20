import os
import json

BASE_DIR = os.path.dirname(os.path.dirname(__file__))
DOWNLOAD_DIR = os.path.join(BASE_DIR, "data", "downloaded")

if not os.path.exists(DOWNLOAD_DIR):
    os.makedirs(DOWNLOAD_DIR, exist_ok=True)

def load_json_data(filename):
    """Загружает JSON-файл, если он существует."""
    path = os.path.join(DOWNLOAD_DIR, filename)
    if not os.path.exists(path):
        print(f"❌ Файл {filename} не найден!")
        return None
    try:
        with open(path, "r", encoding="utf-8") as file:
            return json.load(file)
    except json.JSONDecodeError:
        print(f"❌ Ошибка чтения JSON в {filename}")
        return None

def main():
    """Основной процесс публикации."""
    print("🚀 Запуск публикации...")
    json_filename = "20250116-1932.json"

    post_data = load_json_data(json_filename)

    if isinstance(post_data, str):  # ✅ Гарантируем, что JSON загружается корректно
        try:
            post_data = json.loads(post_data)
        except json.JSONDecodeError:
            print("❌ Ошибка: Некорректный JSON!")
            return

    message = f"🏛 {post_data.get('topic', 'Без темы')}\n\n{post_data.get('text', 'ℹ️ Контент отсутствует.')}"
    print(f"📩 Отправка сообщения: {message}")

if __name__ == "__main__":
    main()
