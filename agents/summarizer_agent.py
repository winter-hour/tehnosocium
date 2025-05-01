# agents/summarizer_agent.py

import asyncio
import json
import logging
import os
import time
from typing import Any, Dict, List, Optional

import google.generativeai as genai
# Импортируем базовый класс и нужные типы
from google.adk.agents.base_agent import BaseAgent
from google.api_core.exceptions import ResourceExhausted

# Импортируем наши утилиты
from utils import db_utils, md_utils

# Настраиваем логирование
logging.basicConfig(level=logging.INFO,
                    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
                    datefmt='%Y-%m-%d %H:%M:%S')
logger = logging.getLogger("SummarizerAgent")

class SummarizerAgent(BaseAgent):
    """
    Агент для генерации кратких резюме для очищенных статей.
    Ищет статьи со статусом 'cleaned', генерирует резюме с помощью LLM,
    добавляет резюме в YAML-секцию MD-файла статьи и обновляет статус
    статьи в БД на 'summarized' или 'summarize_failed'.
    """

    # --- Объявление полей класса (для Pydantic) ---
    config: Optional[Dict[str, Any]] = None
    api_key: Optional[str] = None
    model_name: Optional[str] = None
    summarizer_prompt_template: Optional[str] = None
    model: Optional[genai.GenerativeModel] = None
    generation_config: Optional[genai.GenerationConfig] = None
    # Опциональное ограничение на количество статей за один запуск цикла
    max_articles_per_run: Optional[int] = None
    # Настройки для контроля частоты запросов к API
    api_limit_per_minute: int = 14 # Лимит запросов к API Gemini (чуть меньше реального)
    delay_between_batches: int = 65 # Пауза между пакетами (секунды)


    def __init__(self, agent_id: str = "summarizer_agent", config: Optional[Dict[str, Any]] = None):
        """
        Инициализирует SummarizerAgent.

        Args:
            agent_id: Уникальный идентификатор агента.
            config: Словарь конфигурации приложения.
        """
        super().__init__(name=agent_id)
        self.config = config if config else {}

        # --- Загрузка конфигурации агента ---
        summarizer_config = self.config.get('summarizer', {})
        self.api_key = os.getenv("GEMINI_API_KEY")
        default_model = self.config.get('default_model', 'gemini-1.5-flash-latest')
        self.model_name = summarizer_config.get('model', default_model)
        self.summarizer_prompt_template = summarizer_config.get('prompt')
        self.max_articles_per_run = summarizer_config.get('max_articles_per_run') # Может быть None

        # --- Проверка наличия необходимых настроек ---
        if not self.api_key:
            logger.error(f"Агент '{self.name}': Ключ GEMINI_API_KEY не найден в переменных окружения!")
        if not self.model_name:
            logger.error(f"Агент '{self.name}': Имя модели ИИ не настроено в секции 'summarizer' или 'default_model'!")
        if not self.summarizer_prompt_template:
            logger.error(f"Агент '{self.name}': Шаблон промпта для резюмирования (prompt) не найден в секции 'summarizer' конфига!")
            # Можно установить промпт по умолчанию, если это приемлемо
            # self.summarizer_prompt_template = "Summarize this text: {cleaned_text}"

        # --- Инициализация клиента и модели Gemini ---
        if self.api_key and self.model_name and self.summarizer_prompt_template:
            try:
                genai.configure(api_key=self.api_key)
                self.generation_config = genai.GenerationConfig(candidate_count=1, temperature=0.3) # Температура чуть выше для резюме
                self.model = genai.GenerativeModel(
                    model_name=self.model_name,
                    generation_config=self.generation_config,
                    # safety_settings=... # Можно добавить настройки безопасности
                )
                logger.info(f"Агент '{self.name}' инициализирован. Используется модель: {self.model_name}.")
            except Exception as e:
                logger.error(f"Агент '{self.name}': Не удалось инициализировать клиент Google AI: {e}", exc_info=True)
                self.model = None # Сбрасываем модель при ошибке
        else:
             logger.warning(f"Агент '{self.name}': Не инициализирован клиент Google AI из-за отсутствия API-ключа, имени модели или промпта.")
             self.model = None

        logger.info(f"Агент '{self.name}': Настройки батчинга: размер пакета={self.api_limit_per_minute}, задержка={self.delay_between_batches} сек.")
        if self.max_articles_per_run:
            logger.info(f"Агент '{self.name}': Максимум статей за цикл: {self.max_articles_per_run}.")


    async def run_summarize_cycle(self):
        """
        Выполняет один цикл поиска и обработки статей для резюмирования.
        """
        logger.info(f"Агент '{self.name}': Запуск цикла генерации резюме...")

        if not self.model:
            logger.error(f"Агент '{self.name}': Модель ИИ не инициализирована. Цикл прерван.")
            return

        # 1. Получаем статьи со статусом 'cleaned'
        try:
            articles_to_process = db_utils.get_articles_for_summarizing(limit=self.max_articles_per_run)
        except AttributeError:
             logger.error(f"Агент '{self.name}': Функция 'get_articles_for_summarizing' не найдена в db_utils. Цикл прерван.")
             return
        except Exception as e:
             logger.error(f"Агент '{self.name}': Ошибка при получении статей из БД: {e}", exc_info=True)
             return

        if not articles_to_process:
            logger.info(f"Агент '{self.name}': Нет статей со статусом 'cleaned' для генерации резюме.")
            return

        total_articles = len(articles_to_process)
        logger.info(f"Агент '{self.name}': Найдено {total_articles} статей для резюмирования. Обработка пакетами...")

        # 2. Обработка пакетами (аналогично CleanerAgent)
        batch_size = self.api_limit_per_minute
        processed_count = 0
        failed_count = 0

        for i in range(0, total_articles, batch_size):
            batch_articles = articles_to_process[i:i + batch_size]
            if not batch_articles:
                continue

            batch_ids = [a['id'] for a in batch_articles] # Получаем ID для лога
            logger.info(f"Агент '{self.name}': Обработка пакета {i//batch_size + 1}/{(total_articles + batch_size - 1)//batch_size} (IDs: {batch_ids})...")

            tasks = [self.process_article_summary(article_info) for article_info in batch_articles]
            results = await asyncio.gather(*tasks, return_exceptions=True)

            # Подсчет результатов пакета
            batch_success = 0
            batch_failed = 0
            for j, result in enumerate(results):
                if isinstance(result, Exception):
                    batch_failed += 1
                    # Ошибки уже логируются и статус обновляется в process_article_summary
                else:
                    batch_success += 1

            processed_count += batch_success
            failed_count += batch_failed
            logger.info(f"Агент '{self.name}': Пакет завершен. Успешно: {batch_success}, Ошибки: {batch_failed}.")

            # Пауза между пакетами
            if i + batch_size < total_articles:
                logger.info(f"Агент '{self.name}': Ожидание {self.delay_between_batches} секунд перед следующим пакетом...")
                await asyncio.sleep(self.delay_between_batches)

        logger.info(f"Агент '{self.name}': Цикл генерации резюме завершен. Всего успешно: {processed_count}, Всего ошибок: {failed_count}.")


    async def process_article_summary(self, article_info: Dict[str, Any]):
        """
        Обрабатывает одну статью: читает текст, генерирует резюме, обновляет MD и БД.
        Обновляет статус в YAML-секции MD файла.
        """
        article_id = article_info.get('id')
        md_path = article_info.get('cleaned_md_path')

        if not article_id or not md_path:
            logger.error(f"Агент '{self.name}': Некорректные данные для обработки статьи: {article_info}")
            raise ValueError("Отсутствует ID или путь к MD файлу в информации о статье.")

        logger.info(f"Агент '{self.name}': Генерация резюме для статьи ID: {article_id} ({md_path})")

        yaml_data, text_content = md_utils.read_md_file(md_path)

        if text_content is None:
            error_msg = "Не удалось прочитать текст статьи из MD файла."
            logger.error(f"Агент '{self.name}': {error_msg} ID: {article_id}, Path: {md_path}")
            db_utils.update_article_status(article_id, 'summarize_failed', error_msg=error_msg)
            raise IOError(error_msg)

        if not text_content.strip():
            error_msg = "Текст статьи в MD файле пустой."
            logger.warning(f"Агент '{self.name}': {error_msg} ID: {article_id}, Path: {md_path}")
            db_utils.update_article_status(article_id, 'summarize_failed', error_msg=error_msg)
            return

        title = yaml_data.get('title', '') if yaml_data else ''

        summary_text = None
        try:
            # ... (код генерации промпта и вызова LLM как раньше) ...
            if not self.summarizer_prompt_template:
                raise ValueError("Шаблон промпта для резюмирования не определен.")

            prompt = self.summarizer_prompt_template.format(
                title=title,
                cleaned_text=text_content
            )

            response = await self.model.generate_content_async(prompt)

            if not response.candidates or not response.candidates[0].content or not response.candidates[0].content.parts:
                error_msg="Модель ИИ вернула некорректный ответ (нет content/parts)."
                logger.warning(f"Агент '{self.name}': {error_msg} ID: {article_id}")
                db_utils.update_article_status(article_id, 'summarize_failed', error_msg=error_msg)
                return

            summary_text = response.text.strip()

            if not summary_text:
                error_msg = "Модель ИИ вернула пустое резюме."
                logger.warning(f"Агент '{self.name}': {error_msg} ID: {article_id}")
                db_utils.update_article_status(article_id, 'summarize_failed', error_msg=error_msg)
                return

            logger.info(f"Агент '{self.name}': Резюме успешно сгенерировано для статьи ID: {article_id}")

        except KeyError as e:
             logger.error(f"Агент '{self.name}': Ошибка форматирования промпта для ID {article_id}. Отсутствует ключ: {e}", exc_info=False)
             db_utils.update_article_status(article_id, 'summarize_failed', error_msg=f"Ошибка шаблона промпта: ключ {e}")
             raise e
        except ResourceExhausted as e:
            error_details = str(e.message if hasattr(e, 'message') else e)
            logger.error(f"Агент '{self.name}': Превышен лимит API (429) при генерации резюме для ID {article_id}: {error_details}", exc_info=False)
            db_utils.update_article_status(article_id, 'summarize_failed', error_msg=f"Превышен лимит API: {error_details[:400]}")
            raise e
        except Exception as e:
            error_details = str(e.message if hasattr(e, 'message') else e)
            logger.error(f"Агент '{self.name}': Ошибка API Gemini при генерации резюме для ID {article_id}: {error_details}", exc_info=True)
            db_utils.update_article_status(article_id, 'summarize_failed', error_msg=f"Ошибка API Gemini: {error_details[:500]}")
            raise e

        # 3. Обновляем MD файл, добавляя резюме и ОБНОВЛЯЯ СТАТУС в YAML
        try:
            # --- ИЗМЕНЕНИЕ: Добавляем 'status': 'summarized' в данные для обновления ---
            data_to_update = {
                'summary': summary_text,
                'status': 'summarized' # Обновляем статус в YAML
            }
            success = md_utils.update_md_yaml(md_path, data_to_update)
            # --- КОНЕЦ ИЗМЕНЕНИЯ ---

            if not success:
                # Статус в БД не обновляем, так как файл не обновился
                error_msg="Не удалось обновить YAML в MD файле (включая статус)."
                logger.error(f"Агент '{self.name}': {error_msg} ID: {article_id}, Path: {md_path}")
                # Статус в БД остается 'cleaned', так как операция не завершилась
                # Если бы хотели пометить ошибкой, то:
                # db_utils.update_article_status(article_id, 'summarize_failed', error_msg=error_msg)
                return # Выходим, если не удалось обновить файл
        except Exception as e:
             logger.error(f"Агент '{self.name}': Неожиданная ошибка при обновлении MD файла {md_path} для ID {article_id}: {e}", exc_info=True)
             db_utils.update_article_status(article_id, 'summarize_failed', error_msg=f"Ошибка обновления MD: {e}")
             raise e # Передаем для gather

        # 4. Обновляем статус в БД (только если MD файл успешно обновлен)
        db_utils.update_article_status(article_id, 'summarized')
        logger.info(f"Агент '{self.name}': Статья ID {article_id} успешно обработана (статус 'summarized' в БД и MD файле).")