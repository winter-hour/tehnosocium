# agents/publisher_agent.py

import asyncio
import json
import logging
import os
from typing import Any, Dict, List, Optional

# Импортируем базовый класс и нужные типы
from google.adk.agents.base_agent import BaseAgent

# Импортируем наши утилиты
from utils import db_utils, md_utils
# Импортируем утилиту для Telegram
from utils.telegram_utils import send_telegram_message

# Настройка логирования
logging.basicConfig(level=logging.INFO,
                    format='%(asctime)s - %(name)s - %(levelname)s - [%(filename)s:%(lineno)d] - %(message)s',
                    datefmt='%Y-%m-%d %H:%M:%S')
logger = logging.getLogger("PublisherAgent")

class PublisherAgent(BaseAgent):
    """
    Агент для публикации сгенерированного поста в Telegram-канал.
    Ищет статьи со статусом 'post_generated', читает файл поста,
    отправляет сообщение и обновляет статус статьи на 'published'
    или 'publish_failed'.
    """

    # --- Объявление полей класса ---
    config: Optional[Dict[str, Any]] = None
    bot_token: Optional[str] = None
    channel_id: Optional[str] = None
    parse_mode: Optional[str] = None
    disable_web_page_preview: bool = False

    def __init__(self, agent_id: str = "publisher_agent", config: Optional[Dict[str, Any]] = None):
        """
        Инициализирует PublisherAgent.
        """
        super().__init__(name=agent_id)
        self.config = config if config else {}
        telegram_config = self.config.get('telegram', {})
        publisher_config = self.config.get('publisher', {})

        self.bot_token = telegram_config.get('bot_token')
        self.channel_id = telegram_config.get('channel_id')
        self.parse_mode = publisher_config.get('parse_mode')
        self.disable_web_page_preview = publisher_config.get('disable_web_page_preview', False)

        if not self.bot_token: logger.error(f"Агент '{self.name}': Токен Telegram бота не найден!")
        if not self.channel_id: logger.error(f"Агент '{self.name}': ID или имя канала Telegram не найдено!")

        if self.bot_token and self.channel_id:
             logger.info(f"Агент '{self.name}' инициализирован. Канал: {self.channel_id}.")
        else:
             logger.warning(f"Агент '{self.name}': Не инициализирован полностью.")

    # --- ОСНОВНОЙ МЕТОД ЦИКЛА АГЕНТА ---
    async def run_publishing_cycle(self):
        """
        Выполняет один цикл поиска и публикации готового поста.
        """
        logger.info(f"Агент '{self.name}': Запуск цикла публикации...")

        if not self.bot_token or not self.channel_id:
             logger.error(f"Агент '{self.name}': Отсутствует токен или ID канала. Публикация невозможна.")
             return

        # 1. Получаем ОДНУ статью для публикации
        article_to_publish = None
        try:
            article_to_publish = db_utils.get_post_to_publish()
        except AttributeError:
             logger.error(f"Агент '{self.name}': Функция 'get_post_to_publish' не найдена в db_utils.")
             return # Прерываем цикл, если функция БД отсутствует
        except Exception as e:
            logger.error(f"Агент '{self.name}': Ошибка при получении статьи для публикации: {e}", exc_info=True)
            return # Прерываем цикл при ошибке БД

        if not article_to_publish:
            logger.info(f"Агент '{self.name}': Нет статей со статусом 'post_generated' для публикации.")
            # Это нормальное завершение цикла, если нет работы
            logger.info(f"Агент '{self.name}': Цикл публикации завершен (нет статей).")
            return

        # 2. Публикуем найденную статью
        article_id = article_to_publish.get('id')
        post_md_path = article_to_publish.get('post_md_path')

        if not article_id or not post_md_path:
            logger.error(f"Агент '{self.name}': Получены неполные данные для публикации статьи: {article_to_publish}.")
            if article_id:
                 # Помечаем ошибкой, чтобы не пытаться опубликовать снова
                 db_utils.update_article_status(article_id, 'publish_failed', error_msg="Неполные данные (нет ID или пути к посту)")
            logger.info(f"Агент '{self.name}': Цикл публикации завершен (из-за ошибки данных).")
            return

        # Вызываем внутренний метод для фактической публикации
        await self._publish_article(article_id, post_md_path)

        logger.info(f"Агент '{self.name}': Цикл публикации завершен.")

    # --- ВНУТРЕННИЙ МЕТОД ДЛЯ ПУБЛИКАЦИИ ОДНОЙ СТАТЬИ ---
    async def _publish_article(self, article_id: int, post_md_path: str):
        """
        Читает MD-файл поста и отправляет его текст в Telegram.
        Обновляет статус в БД.

        Args:
            article_id: ID статьи в БД.
            post_md_path: Путь к MD-файлу с постом.
        """
        logger.info(f"Агент '{self.name}': Публикация поста для статьи ID: {article_id} из файла {post_md_path}")

        # 1. Читаем MD файл поста
        post_text = None
        try:
            # Нам нужен только основной текст поста
            _, post_text = md_utils.read_md_file(post_md_path)
            if post_text is None:
                 raise ValueError(f"Не удалось прочитать текст поста из MD файла: {post_md_path}")
            post_text = post_text.strip() # Убираем пробелы по краям
            if not post_text:
                 raise ValueError(f"Текст поста в файле {post_md_path} пустой.")

        except Exception as e:
            error_msg = f"Ошибка чтения файла поста {post_md_path}: {e}"
            logger.error(f"Агент '{self.name}': {error_msg}", exc_info=True)
            db_utils.update_article_status(article_id, 'publish_failed', error_msg=error_msg)
            return # Прерываем публикацию этой статьи

        # 2. Отправляем сообщение в Telegram
        try:
            success = await send_telegram_message(
                bot_token=self.bot_token,
                chat_id=self.channel_id,
                text=post_text, # Передаем уже очищенный текст
                parse_mode=self.parse_mode, # Передаем режим парсинга из конфига
                disable_web_page_preview=self.disable_web_page_preview # Передаем настройку превью
            )

            if success:
                # 3. Обновляем статус в БД на 'published'
                db_utils.update_article_status(article_id, 'published')
                logger.info(f"Агент '{self.name}': Пост для статьи ID {article_id} успешно опубликован.")
            else:
                # Ошибка уже залогирована в send_telegram_message
                # Обновляем статус на 'publish_failed'
                db_utils.update_article_status(article_id, 'publish_failed', error_msg="Ошибка отправки сообщения в Telegram")
                logger.error(f"Агент '{self.name}': Не удалось опубликовать пост для статьи ID {article_id}.")

        except Exception as e: # Ловим совсем непредвиденные ошибки
             error_msg = f"Неожиданная ошибка при попытке публикации поста ID {article_id}: {e}"
             logger.error(f"Агент '{self.name}': {error_msg}", exc_info=True)
             db_utils.update_article_status(article_id, 'publish_failed', error_msg=error_msg)