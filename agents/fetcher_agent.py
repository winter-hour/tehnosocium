# agents/fetcher_agent.py
import asyncio  # Добавлен импорт asyncio
import json
import logging
from datetime import datetime
from time import mktime
from typing import Any, Dict, List, Optional, TYPE_CHECKING

import feedparser
import requests
from google.adk.agents.base_agent import BaseAgent
from google.cloud.aiplatform_v1beta1.types import Content, Part

from utils import db_utils, file_utils

# Используем TYPE_CHECKING для аннотации типа CleanerAgent, чтобы избежать циклического импорта
if TYPE_CHECKING:
    from .cleaner_agent import CleanerAgent

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger("FetcherAgent")


class FetcherAgent(BaseAgent):
    """
    Агент для получения статей из RSS-каналов, скачивания их HTML-содержимого,
    сохранения сырых данных и передачи ID статей для очистки другому агенту.
    """

    # --- Объявление полей класса ---
    config: Optional[Dict[str, Any]] = None
    cleaner_agent: Optional['CleanerAgent'] = None  # Ссылка на экземпляр CleanerAgent
    rss_feeds: List[Dict[str, str]] = []
    raw_html_path: str = "data/raw_html"
    request_headers: Dict[str, str] = {'User-Agent': 'TehnosociumRSSFetcher/1.0 (+https://t.me/tehnosocium)'}
    request_timeout: int = 15

    def __init__(self, agent_id: str = "fetcher_agent", config: Optional[Dict[str, Any]] = None,
                 cleaner_agent_instance: Optional['CleanerAgent'] = None):
        """
        Инициализирует FetcherAgent.

        Args:
            agent_id: Уникальный идентификатор агента.
            config: Словарь конфигурации.
            cleaner_agent_instance: Экземпляр CleanerAgent для прямого вызова.
        """
        super().__init__(name=agent_id)  # Инициализация родительского класса BaseAgent

        self.config = config if config else {}
        self.cleaner_agent = cleaner_agent_instance

        # --- Присваиваем значения полям из config или используем значения по умолчанию ---
        self.rss_feeds = self.config.get('rss_sources', self.rss_feeds)
        self.raw_html_path = self.config.get('paths', {}).get('raw_html', self.raw_html_path)
        # Опционально: сделать headers и timeout настраиваемыми через config
        # self.request_headers = self.config.get('fetcher', {}).get('headers', self.request_headers)
        # self.request_timeout = self.config.get('fetcher', {}).get('timeout', self.request_timeout)

        if not self.cleaner_agent:
            logger.warning(f"CleanerAgent instance was not provided to FetcherAgent '{self.name}'!")

        logger.info(f"FetcherAgent '{self.name}' initialized. Found {len(self.rss_feeds)} RSS sources.")
        logger.info(f"Raw HTML will be saved to: {self.raw_html_path}")

    async def run_fetch_cycle(self):
        """
        Запускает один цикл обхода всех RSS-источников, скачивания новых статей
        и вызова агента-очистителя.
        """
        logger.info(f"Starting new fetch cycle for agent '{self.name}'...")
        new_article_ids: List[int] = []

        for feed_info in self.rss_feeds:
            feed_name = feed_info.get('name', 'Unknown Source')
            feed_url = feed_info.get('url')

            if not feed_url:
                logger.warning(f"Skipping source '{feed_name}' due to missing URL.")
                continue

            logger.info(f"Processing RSS feed: {feed_name} ({feed_url})")
            try:
                # Используем feedparser для получения данных ленты
                feed_data = feedparser.parse(feed_url)

                # Проверка на ошибки парсинга RSS
                if feed_data.bozo:
                    logger.warning(f"Feed '{feed_name}' may be malformed. Bozo reason: {feed_data.bozo_exception}")

                for entry in feed_data.entries:
                    title = entry.get('title', 'No Title')
                    link = entry.get('link')

                    if not link:
                        logger.warning(f"Skipping entry '{title}' from '{feed_name}' due to missing link.")
                        continue

                    # Обработка даты публикации
                    published_time_struct = entry.get('published_parsed') or entry.get('updated_parsed')
                    pub_date_iso: str
                    if published_time_struct:
                        try:
                            dt_obj = datetime.fromtimestamp(mktime(published_time_struct))
                            pub_date_iso = dt_obj.isoformat()
                        except Exception as e:
                            logger.warning(f"Could not parse date for '{title}' from '{feed_name}': {e}. Using current time.")
                            pub_date_iso = datetime.now().isoformat()
                    else:
                        logger.warning(f"No publication date found for '{title}' from '{feed_name}'. Using current time.")
                        pub_date_iso = datetime.now().isoformat()

                    # 1. Проверка на дубликат в БД
                    if db_utils.check_article_exists(link):
                        # logger.debug(f"Article already exists, skipping: {link}") # Можно раскомментировать для отладки
                        continue

                    logger.info(f"Found new article: '{title}' from '{feed_name}'")

                    # 2. Скачивание HTML
                    html_content = None
                    try:
                        response = requests.get(link, headers=self.request_headers, timeout=self.request_timeout, allow_redirects=True)
                        response.raise_for_status()  # Проверка на HTTP ошибки (4xx, 5xx)
                        # Пытаемся определить кодировку, если requests не справился
                        response.encoding = response.apparent_encoding
                        html_content = response.text
                        logger.info(f"Successfully downloaded HTML (approx {len(html_content)} chars) for: {link}")
                    except requests.exceptions.RequestException as e:
                        logger.error(f"Failed to download HTML for {link}: {e}")
                        # Попытка добавить запись об ошибке скачивания в БД
                        article_id_for_fail = db_utils.add_article(feed_name, title, link, pub_date_iso, None)
                        if article_id_for_fail:
                            db_utils.update_article_status(article_id_for_fail, 'fetch_failed', error_msg=f"Download error: {e}")
                        else: # Если даже добавить не удалось (например, дубль URL появился)
                            existing_id = db_utils.get_article_id_by_url(link)
                            if existing_id:
                                db_utils.update_article_status(existing_id, 'fetch_failed', error_msg=f"Download error: {e}")
                        continue  # Переходим к следующей статье в ленте

                    # 3. Сохранение Raw HTML в JSON
                    json_filepath = file_utils.generate_filename(
                        self.raw_html_path, feed_name, title, "json"
                    )
                    raw_data = {"url": link, "raw_html": html_content}
                    try:
                        file_utils.save_json(raw_data, json_filepath)
                    except Exception as e:
                        logger.error(f"Failed to save raw HTML JSON for {link} to {json_filepath}: {e}")
                        # Если не смогли сохранить файл, нет смысла продолжать
                        continue

                    # 4. Запись в БД
                    article_id = db_utils.add_article(feed_name, title, link, pub_date_iso, json_filepath)
                    if article_id:
                        new_article_ids.append(article_id)
                    else:
                        # Эта ситуация маловероятна, если check_article_exists отработал
                        logger.error(f"Failed to add article to DB (returned None, possibly duplicate URL race condition): {link}")
                        # Возможно, стоит удалить json_filepath, если он был создан

            except Exception as e:
                # Ловим общие ошибки при обработке всей ленты
                logger.error(f"An unexpected error occurred while processing feed {feed_name} ({feed_url}): {e}", exc_info=True)
                # Продолжаем со следующей лентой

        # 5. Вызов CleanerAgent, если есть новые статьи и экземпляр агента доступен
        if new_article_ids:
            if self.cleaner_agent:
                logger.info(f"Collected {len(new_article_ids)} new articles. Calling cleaner agent directly.")
                # Упаковываем ID в JSON и передаем как текст в Content
                payload = {"article_ids": new_article_ids}
                try:
                    payload_str = json.dumps(payload)
                    cleaning_content = Content(
                        role="system",  # Или 'user', в зависимости от семантики
                        parts=[Part(text=payload_str)] # Создаем Part напрямую с параметром text
                    )
                    # Напрямую вызываем асинхронный метод cleaner'а
                    await self.cleaner_agent.handle_cleaning_request(cleaning_content)
                    logger.info(f"Successfully called cleaner agent for {len(new_article_ids)} IDs.")
                except json.JSONDecodeError as e:
                    logger.error(f"Failed to serialize article IDs to JSON: {e}")
                except Exception as e:
                    logger.error(f"Failed to call cleaner agent: {e}", exc_info=True)
            else:
                logger.error(f"Cannot send {len(new_article_ids)} IDs to cleaner: cleaner_agent instance is None.")
        else:
            logger.info("No new articles found in this cycle.")

        logger.info(f"Fetch cycle finished for agent '{self.name}'.")


# --- Напоминание: Убедитесь, что эта функция добавлена в utils/db_utils.py ---
# import sqlite3
# def get_article_id_by_url(url: str) -> int | None:
#     """Ищет ID статьи по её URL."""
#     conn = get_db_connection()
#     if not conn: return None
#     article_id = None
#     try:
#         cursor = conn.cursor()
#         cursor.execute("SELECT id FROM articles WHERE url = ?", (url,))
#         result = cursor.fetchone()
#         if result:
#             article_id = result['id'] # Доступ по имени колонки
#     except sqlite3.Error as e:
#         logging.error(f"Error fetching article ID by URL ({url}): {e}")
#     finally:
#         if conn:
#             conn.close()
#     return article_id
# --- Конец напоминания ---