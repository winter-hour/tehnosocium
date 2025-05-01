# agents/cleaner_agent.py
import asyncio
import json
import logging
import os
import time # Импортируем time для синхронной паузы, если понадобится
from typing import Any, Dict, List, Optional

import google.generativeai as genai
from google.adk.agents.base_agent import BaseAgent
from google.cloud.aiplatform_v1beta1.types import Content, Part
# Импортируем тип ResourceExhausted для более точной обработки ошибки
from google.api_core.exceptions import ResourceExhausted

from utils import db_utils, file_utils

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger("CleanerAgent")

# --- Класс CleanerAgent и __init__ остаются без изменений ---
class CleanerAgent(BaseAgent):
    # ... (объявление полей как раньше) ...
    config: Optional[Dict[str, Any]] = None
    api_key: Optional[str] = None
    model_name: Optional[str] = None
    cleaning_prompt_template: str = "Extract text:\n\n{raw_html}"
    cleaned_md_path: str = "data/cleaned_md"
    model: Optional[genai.GenerativeModel] = None
    generation_config: Optional[genai.GenerationConfig] = None

    def __init__(self, agent_id: str = "cleaner_agent", config: Optional[Dict[str, Any]] = None):
        # ... (код __init__ как раньше) ...
        super().__init__(name=agent_id)
        self.config = config if config else {}
        self.api_key = os.getenv("GEMINI_API_KEY")
        default_model = self.config.get('default_model', 'gemini-1.5-flash-latest')
        self.model_name = self.config.get('cleaning', {}).get('model', default_model)
        self.cleaning_prompt_template = self.config.get('cleaning', {}).get('prompt', self.cleaning_prompt_template)
        self.cleaned_md_path = self.config.get('paths', {}).get('cleaned_md', self.cleaned_md_path)
        if not self.api_key:
            logger.error(f"Agent '{self.name}': GEMINI_API_KEY not found!")
        elif not self.model_name:
            logger.error(f"Agent '{self.name}': AI model name is not configured!")
        else:
            try:
                genai.configure(api_key=self.api_key)
                self.generation_config = genai.GenerationConfig(candidate_count=1)
                self.model = genai.GenerativeModel(
                    model_name=self.model_name,
                    generation_config=self.generation_config,
                )
                logger.info(f"CleanerAgent '{self.name}' initialized. Using model: {self.model_name}")
                logger.info(f"Cleaned Markdown will be saved to: {self.cleaned_md_path}")
            except Exception as e:
                logger.error(f"Failed to initialize Google AI Client for agent '{self.name}': {e}", exc_info=True)
                self.model = None
                self.generation_config = None

    # --- ИЗМЕНЕННЫЙ МЕТОД handle_cleaning_request ---
    async def handle_cleaning_request(self, content: Content):
        """
        Обрабатывает запрос на очистку статей пакетами с паузами,
        чтобы не превышать лимиты API.
        """
        if not self.model:
            logger.error(f"Agent '{self.name}': Gemini model not initialized. Cannot process cleaning request.")
            return

        article_ids: List[int] = []
        try:
            if content.parts:
                payload_str = content.parts[0].text
                payload = json.loads(payload_str)
                article_ids = payload.get("article_ids", [])
                if not isinstance(article_ids, list):
                    logger.warning(f"Agent '{self.name}': 'article_ids' in payload is not a list. Payload: {payload_str}")
                    article_ids = []
            else:
                logger.warning(f"Agent '{self.name}': Received cleaning request with empty content parts.")
                return
        except Exception as e:
            logger.error(f"Agent '{self.name}': Error parsing cleaning request payload: {e}. Content: {content}", exc_info=True)
            return

        if not article_ids:
            logger.warning(f"Agent '{self.name}': Received cleaning request with empty or invalid article_ids list.")
            return

        # --- Настройки для батчинга ---
        # Лимит API (запросов в минуту) - берем чуть меньше для надежности
        api_limit_per_minute = 14 # Вместо 15
        # Размер пакета должен быть <= api_limit_per_minute
        batch_size = api_limit_per_minute
        # Пауза между пакетами в секундах (чуть больше минуты)
        delay_between_batches = 65

        total_articles = len(article_ids)
        processed_count = 0
        failed_count = 0
        logger.info(f"Agent '{self.name}': Received request to clean {total_articles} articles. Processing in batches of {batch_size} with {delay_between_batches}s delay...")

        # Обработка пакетами
        for i in range(0, total_articles, batch_size):
            batch_ids = article_ids[i:i + batch_size]
            if not batch_ids:
                continue

            logger.info(f"Agent '{self.name}': Processing batch {i//batch_size + 1}/{(total_articles + batch_size - 1)//batch_size} (IDs: {batch_ids})...")

            # Запускаем обработку текущего пакета параллельно
            tasks = [self.process_article(article_id) for article_id in batch_ids]
            results = await asyncio.gather(*tasks, return_exceptions=True)

            # Считаем результаты пакета
            batch_success = 0
            batch_failed = 0
            for j, result in enumerate(results):
                if isinstance(result, Exception):
                    batch_failed += 1
                    # Ошибки уже логируются внутри process_article или в gather
                    # Можно добавить доп. логирование тут при необходимости
                    # logger.error(f"Agent '{self.name}': Error in batch for article ID {batch_ids[j]}: {result}")
                else:
                    batch_success += 1

            processed_count += batch_success
            failed_count += batch_failed
            logger.info(f"Agent '{self.name}': Batch finished. Successful: {batch_success}, Failed: {batch_failed}.")

            # Пауза перед следующим пакетом, если это не последний пакет
            if i + batch_size < total_articles:
                logger.info(f"Agent '{self.name}': Waiting {delay_between_batches} seconds before next batch...")
                await asyncio.sleep(delay_between_batches)

        logger.info(f"Agent '{self.name}': Finished processing all batches. Total Successful: {processed_count}, Total Failed: {failed_count}.")


    # --- МЕТОД process_article с обработкой ResourceExhausted ---
    async def process_article(self, article_id: int):
        """
        Обрабатывает одну статью: читает HTML, вызывает AI модель для очистки,
        сохраняет результат в .md файл и обновляет статус в БД.
        Обрабатывает ошибку ResourceExhausted (429).
        """
        logger.info(f"Agent '{self.name}': Processing article ID: {article_id}")

        if not self.model:
            error_msg = f"Agent '{self.name}': Cannot process article {article_id}, model is not available."
            logger.error(error_msg)
            db_utils.update_article_status(article_id, 'cleaning_failed', error_msg="Cleaner agent model not initialized")
            raise RuntimeError(error_msg)

        article_data = db_utils.get_article_for_cleaning(article_id)
        if not article_data:
            logger.warning(f"Agent '{self.name}': Article ID {article_id} not found or not in 'raw_fetched' state. Skipping processing.")
            return # Просто выходим, не ошибка

        raw_json_path = article_data.get('raw_json_path')
        if not raw_json_path:
            error_msg = f"Missing raw_json_path for article ID {article_id}."
            logger.error(error_msg)
            db_utils.update_article_status(article_id, 'cleaning_failed', error_msg=error_msg)
            raise ValueError(error_msg)

        raw_content = file_utils.load_json(raw_json_path)
        if not raw_content or 'raw_html' not in raw_content:
            error_msg = f"Failed to load/parse raw HTML JSON from {raw_json_path} for article ID {article_id}."
            logger.error(error_msg)
            db_utils.update_article_status(article_id, 'cleaning_failed', error_msg="Failed to load/parse raw HTML JSON")
            raise IOError(error_msg)
        raw_html = raw_content['raw_html']

        cleaned_text = None
        try:
            max_html_length = 500_000
            if len(raw_html) > max_html_length:
                logger.warning(f"Article ID {article_id}: HTML content truncated from {len(raw_html)} to {max_html_length} chars for API call.")
                raw_html = raw_html[:max_html_length]

            if not raw_html.strip():
                logger.warning(f"Article ID {article_id}: Raw HTML is empty after potential truncation. Skipping API call.")
                db_utils.update_article_status(article_id, 'cleaning_failed', error_msg="Raw HTML content is empty")
                return # Не ошибка, просто нечего чистить

            prompt = self.cleaning_prompt_template.format(raw_html=raw_html)
            response = await self.model.generate_content_async(prompt)

            if not response.candidates or not response.candidates[0].content or not response.candidates[0].content.parts:
                 logger.warning(f"Agent '{self.name}': Model returned no valid content/parts for article ID {article_id}. URL: {article_data.get('url')}")
                 db_utils.update_article_status(article_id, 'cleaning_failed', error_msg="Model returned no content parts")
                 return # Не ошибка, но результат плохой

            cleaned_text = response.text.strip()

            if not cleaned_text:
                logger.warning(f"Agent '{self.name}': Model returned empty text after stripping for article ID {article_id}. URL: {article_data.get('url')}")
                db_utils.update_article_status(article_id, 'cleaning_failed', error_msg="Model returned empty text")
                return # Не ошибка

            logger.info(f"Agent '{self.name}': Successfully cleaned HTML for article ID {article_id} using {self.model_name}.")

        # --- Явная обработка ошибки лимита ---
        except ResourceExhausted as e:
            error_details = str(e)
            if hasattr(e, 'message'): error_details = getattr(e, 'message')
            logger.error(f"Agent '{self.name}': Rate limit exceeded (429) for article ID {article_id}: {error_details}", exc_info=False) # Не логируем весь traceback для 429
            db_utils.update_article_status(article_id, 'cleaning_failed', error_msg=f"Rate limit exceeded: {error_details[:400]}") # Записываем ошибку
            raise e # Передаем исключение в gather, чтобы цикл батчинга знал об ошибке

        except Exception as e:
            error_details = str(e)
            if hasattr(e, 'message'): error_details = getattr(e, 'message')
            elif hasattr(e, 'details'): error_details = getattr(e, 'details')()
            logger.error(f"Agent '{self.name}': Error calling Gemini API for article ID {article_id}: {error_details}", exc_info=True)
            db_utils.update_article_status(article_id, 'cleaning_failed', error_msg=f"Gemini API error: {error_details[:500]}")
            raise e # Передаем другие ошибки

        # --- Сохранение MD ---
        md_filepath = file_utils.generate_filename(
            self.cleaned_md_path,
            article_data['source_name'],
            article_data['title'],
            "md"
        )
        try:
            safe_title = article_data['title'].replace('\\', '\\\\').replace('"', '\\"')
            safe_source = article_data['source_name'].replace('\\', '\\\\').replace('"', '\\"')
            safe_url = article_data['url']
            safe_date = article_data['publication_date']

            if not cleaned_text or cleaned_text.isspace():
                 logger.warning(f"Agent '{self.name}': Cleaned text is empty or whitespace for article ID {article_id}. Saving placeholder.")
                 cleaned_text = "[Контент не извлечен]"

            front_matter = f"""---
title: "{safe_title}"
source: "{safe_source}"
url: "{safe_url}"
publication_date: "{safe_date}"
status: "cleaned"
---

{cleaned_text}
"""
            file_utils.save_md(front_matter, md_filepath)
        except Exception as e:
            logger.error(f"Agent '{self.name}': Failed to save cleaned MD file for article ID {article_id} to {md_filepath}: {e}")
            db_utils.update_article_status(article_id, 'cleaning_failed', error_msg=f"Failed to save MD file: {e}")
            raise e

        # --- Обновление Статуса в БД ---
        db_utils.update_article_status(article_id, 'cleaned', file_path=md_filepath)
        logger.info(f"Agent '{self.name}': Successfully processed article ID {article_id}. Result saved to {md_filepath}")