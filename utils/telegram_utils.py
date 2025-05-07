# utils/telegram_utils.py

import logging
import asyncio
from typing import Optional

from telegram import Bot
from telegram.constants import ParseMode
from telegram.error import TelegramError

# Настройка логгера
logger = logging.getLogger("TelegramUtils")
# Базовая конфигурация логирования должна быть настроена в main.py или другом главном модуле

async def send_telegram_message(
    bot_token: str,
    chat_id: str,
    text: str,
    parse_mode: Optional[str] = None,
    disable_web_page_preview: Optional[bool] = None,
    disable_notification: Optional[bool] = None
) -> bool:
    """
    Асинхронно отправляет текстовое сообщение в указанный чат Telegram.

    Args:
        bot_token: Токен Telegram бота.
        chat_id: ID чата или имя канала (например, '@channelname').
        text: Текст сообщения.
        parse_mode: Режим парсинга ('MarkdownV2', 'HTML', None).
                   Используй ParseMode.MARKDOWN_V2 или ParseMode.HTML из telegram.constants.
        disable_web_page_preview: Отключить предпросмотр ссылок.
        disable_notification: Отправить сообщение без звукового уведомления.

    Returns:
        True, если сообщение успешно отправлено, иначе False.
    """
    if not bot_token:
        logger.error("Не указан токен Telegram бота.")
        return False
    if not chat_id:
        logger.error("Не указан ID чата/канала Telegram.")
        return False
    if not text:
        logger.warning("Попытка отправить пустое сообщение в Telegram.")
        # Можно вернуть True, если пустое сообщение - не ошибка, или False
        return False # Считаем ошибкой

    bot = None
    try:
        # Создаем экземпляр бота
        bot = Bot(token=bot_token)

        # Проверяем корректность parse_mode, если он задан
        valid_parse_modes = [ParseMode.MARKDOWN_V2, ParseMode.HTML]
        final_parse_mode = None
        if parse_mode and parse_mode.upper() in valid_parse_modes:
             final_parse_mode = parse_mode.upper()
        elif parse_mode:
             logger.warning(f"Некорректный parse_mode '{parse_mode}'. Отправка без форматирования.")

        logger.info(f"Попытка отправки сообщения в Telegram чат/канал: {chat_id} (ParseMode: {final_parse_mode or 'None'})...")

        # Асинхронная отправка сообщения
        await bot.send_message(
            chat_id=chat_id,
            text=text,
            parse_mode=final_parse_mode,
            disable_web_page_preview=disable_web_page_preview,
            disable_notification=disable_notification
            # connect_timeout=10, # Можно добавить таймауты
            # read_timeout=20
        )
        logger.info(f"Сообщение успешно отправлено в Telegram чат/канал: {chat_id}")
        return True

    except TelegramError as e:
        # Обрабатываем специфические ошибки Telegram API
        logger.error(f"Ошибка Telegram API при отправке сообщения в '{chat_id}': {e}", exc_info=True)
        # Можно добавить более детальную обработку разных кодов ошибок Telegram
        return False
    except Exception as e:
        # Ловим другие возможные ошибки (сеть, таймауты и т.д.)
        logger.error(f"Неожиданная ошибка при отправке сообщения в Telegram ('{chat_id}'): {e}", exc_info=True)
        return False
    finally:
        # Закрываем сессию бота асинхронно, если она была создана
        if bot:
             try:
                 # В новых версиях python-telegram-bot закрытие сессии обычно происходит автоматически
                 # при выходе из контекста или сборке мусора, но можно добавить для надежности,
                 # если используется кастомный Application или напрямую Bot API.
                 # await bot.shutdown() # Попробуем без явного shutdown сначала
                 pass
             except Exception as e:
                  logger.warning(f"Ошибка при попытке закрыть сессию Telegram бота: {e}")