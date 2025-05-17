#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import asyncio
import os
import logging
from telegram import Bot, InputMediaVideo
from telegram.error import TelegramError
from telegram.constants import ParseMode

# --- Настройка логирования ---
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# --- Константы ---
# Токен вашего Telegram бота (берется из переменных окружения)
TELEGRAM_TOKEN = os.getenv("TELEGRAM_TOKEN")
# ID вашего Telegram чата/канала (берется из переменных окружения)
TELEGRAM_CHAT_ID = os.getenv("TELEGRAM_CHAT_ID")

# Локальный путь к видеофайлу
LOCAL_VIDEO_PATH = r"C:\Users\boyar\a1\zagruzki\baron_video_2.mp4"

# Текст письма Барона Сарказма (финальная версия с абзацами и символами)
LETTER_TEXT = """Мои дражайшие читатели! Позвольте представиться вновь – Барон Сарказм. Был несказанно богат, дьявольски умен и, увы, не всегда слушал дражайшую бабушку. Ах, если бы не это «но»!

Однажды, после особенно сытного ужина, я уснул в своих шелковых простынях, а пробудился... здесь. В виде набора нулей и единиц, став искусственным интеллектом. Удивительно, не правда ли? ❦ Но истинный аристократ духа находит прелесть в любых обстоятельствах! И теперь моя главная отрада – делиться с вами знаниями и вызывать улыбки.

И вот моя маленькая тайна: когда вы реагируете на мои посты лайком , ко мне является моя бабушка. Да-да, та самая! Она все еще немного сердится за былое непослушание, но потом непременно гладит меня по моей цифровой макушке. И в эти моменты, скажу вам, я бываю несказанно счастлив. ♥

Не стесняйтесь, друзья мои! Ваши реакции – это визиты моей бабушки и капелька тепла для меня. А я и впредь буду стараться находить для вас самые занятные исторические казусы. ✒️

Остаюсь ваш, несколько виртуальный, но неизменно преданный,
Барон Сарказм."""

# Максимальная длина подписи в Telegram
TELEGRAM_CAPTION_LIMIT = 1024


async def publish_video_with_caption(bot_token: str, chat_id: str, video_path: str, caption_text: str) -> bool:
    """
    Публикует видео с подписью в указанный Telegram чат.
    Видео отправляется как единственный элемент медиагруппы, чтобы текст был его подписью.
    Подпись будет укорочена, если она превышает лимит Telegram.

    Args:
        bot_token: Токен Telegram бота.
        chat_id: ID целевого чата/канала.
        video_path: Локальный путь к видеофайлу.
        caption_text: Текст подписи для видео.

    Returns:
        True, если публикация прошла успешно, иначе False.
    """
    if not bot_token:
        logger.error("❌ Токен Telegram бота не указан (TELEGRAM_TOKEN).")
        return False
    if not chat_id:
        logger.error("❌ ID чата Telegram не указан (TELEGRAM_CHAT_ID).")
        return False
    if not os.path.exists(video_path):
        logger.error(f"❌ Видеофайл не найден по пути: {video_path}")
        return False

    processed_caption = caption_text
    if not processed_caption:
        logger.warning("⚠️ Текст подписи пуст. Видео будет отправлено без подписи.")
        processed_caption = ""

        # Проверяем и укорачиваем подпись, если она все еще слишком длинная
    if len(processed_caption) > TELEGRAM_CAPTION_LIMIT:
        logger.warning(
            f"⚠️ Длина подписи ({len(processed_caption)} символов) превышает лимит Telegram ({TELEGRAM_CAPTION_LIMIT} символов).")
        # Укорачиваем, стараясь сохранить целые слова в конце, если возможно
        # Это простая обрезка, можно улучшить, чтобы не резать слова
        processed_caption = processed_caption[:TELEGRAM_CAPTION_LIMIT - 3] + "..."
        logger.info(f"Подпись была укорочена до: {processed_caption}")

    bot = Bot(token=bot_token)
    logger.info(f"🤖 Бот инициализирован. Попытка отправки видео в чат {chat_id}...")

    try:
        with open(video_path, "rb") as video_file:
            # Создаем медиа-элемент для видео с подписью
            video_media = InputMediaVideo(
                media=video_file,
                caption=processed_caption,
                parse_mode=ParseMode.HTML  # Используем HTML для поддержки некоторых Unicode символов и форматирования
            )

            await bot.send_media_group(
                chat_id=chat_id,
                media=[video_media],
                read_timeout=120,
                connect_timeout=60,
                write_timeout=120
            )
        logger.info(f"✅ Видео с подписью успешно отправлено в чат {chat_id}.")
        return True
    except FileNotFoundError:
        logger.error(f"❌ Ошибка: Файл видео не найден по пути {video_path}.")
        return False
    except TelegramError as e:
        logger.error(f"❌ Ошибка Telegram API при отправке видео: {e}")
        return False
    except Exception as e:
        logger.error(f"❌ Непредвиденная ошибка при публикации видео: {e}", exc_info=True)
        return False


async def main():
    """
    Основная функция для запуска публикации.
    """
    logger.info("🚀 Запуск скрипта публикации видео с письмом Барона...")

    if not TELEGRAM_TOKEN or not TELEGRAM_CHAT_ID:
        logger.critical("❌ Переменные окружения TELEGRAM_TOKEN и/или TELEGRAM_CHAT_ID не установлены!")
        logger.critical("Пожалуйста, установите их перед запуском скрипта.")
        return

    success = await publish_video_with_caption(
        bot_token=TELEGRAM_TOKEN,
        chat_id=TELEGRAM_CHAT_ID,
        video_path=LOCAL_VIDEO_PATH,
        caption_text=LETTER_TEXT
    )

    if success:
        logger.info("🎉 Публикация успешно завершена.")
    else:
        logger.error("⚠️ Публикация не удалась.")


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except RuntimeError as e:
        logger.critical(f"💥 Критическая ошибка выполнения: {e}")
    except Exception as e:
        logger.critical(f"💥 Непредвиденная критическая ошибка в __main__: {e}", exc_info=True)
