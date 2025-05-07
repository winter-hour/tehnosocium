# agents/generator_agent.py

import asyncio
import json
import logging
import os
from datetime import datetime, timedelta
from typing import Any, Dict, List, Optional

import google.generativeai as genai
from google.adk.agents.base_agent import BaseAgent
from google.api_core.exceptions import ResourceExhausted

# Импортируем наши утилиты
from utils import db_utils, md_utils, file_utils

# Настройка логирования
logging.basicConfig(level=logging.INFO,
                    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
                    datefmt='%Y-%m-%d %H:%M:%S')
logger = logging.getLogger("GeneratorAgent")

class GeneratorAgent(BaseAgent):
    """
    Агент для генерации текста поста Telegram на основе выбранной статьи ('selected').
    Создает MD-файл поста и обновляет статус статьи на 'post_generated'.
    """

    # --- Объявление полей класса ---
    config: Optional[Dict[str, Any]] = None
    api_key: Optional[str] = None
    model_name: Optional[str] = None
    content_prompt_template: Optional[str] = None
    model: Optional[genai.GenerativeModel] = None
    generation_config: Optional[genai.GenerationConfig] = None
    posts_md_path: str = "data/posts_md" # Путь по умолчанию

    def __init__(self, agent_id: str = "generator_agent", config: Optional[Dict[str, Any]] = None):
        """
        Инициализирует GeneratorAgent.

        Args:
            agent_id: Уникальный идентификатор агента.
            config: Словарь конфигурации приложения.
        """
        super().__init__(name=agent_id)
        self.config = config if config else {}

        # --- Загрузка конфигурации агента ---
        content_config = self.config.get('content', {}) # Используем секцию 'content'
        paths_config = self.config.get('paths', {})
        self.api_key = os.getenv("GEMINI_API_KEY")
        # Используем мощную модель по умолчанию, если не указана явно
        default_model = self.config.get('default_model', 'gemini-2.5-pro-exp-03-25')
        self.model_name = content_config.get('model', default_model)
        self.content_prompt_template = content_config.get('prompt')
        self.posts_md_path = paths_config.get('posts_md', self.posts_md_path)

        # --- Проверка наличия необходимых настроек ---
        if not self.api_key: logger.error(f"Агент '{self.name}': Ключ GEMINI_API_KEY не найден!")
        if not self.model_name: logger.error(f"Агент '{self.name}': Имя модели ИИ не настроено ('content.model' или 'default_model')!")
        if not self.content_prompt_template: logger.error(f"Агент '{self.name}': Шаблон промпта для генерации поста ('content.prompt') не найден!")
        if not os.path.isdir(self.posts_md_path): logger.warning(f"Агент '{self.name}': Папка для постов {self.posts_md_path} не найдена! Она может быть создана автоматически.")

        # --- Инициализация клиента и модели Gemini ---
        if self.api_key and self.model_name and self.content_prompt_template:
            try:
                genai.configure(api_key=self.api_key)
                # Для генерации поста может быть полезна более высокая температура
                self.generation_config = genai.GenerationConfig(candidate_count=1, temperature=0.6)
                self.model = genai.GenerativeModel(
                    model_name=self.model_name,
                    generation_config=self.generation_config,
                )
                logger.info(f"Агент '{self.name}' инициализирован. Используется модель: {self.model_name}.")
                logger.info(f"Агент '{self.name}': Посты будут сохраняться в: {self.posts_md_path}")
            except Exception as e:
                logger.error(f"Агент '{self.name}': Не удалось инициализировать Google AI: {e}", exc_info=True)
                self.model = None
        else:
             logger.warning(f"Агент '{self.name}': Клиент Google AI не инициализирован.")
             self.model = None

    

    async def run_generation_cycle(self):
        """
        Выполняет один цикл поиска выбранной статьи ('selected'),
        генерации текста поста Telegram, сохранения его в MD файл
        и обновления статуса статьи на 'post_generated'.
        """
        logger.info(f"Агент '{self.name}': Запуск цикла генерации поста...")

        if not self.model:
            logger.error(f"Агент '{self.name}': Модель ИИ не инициализирована. Цикл генерации прерван.")
            return

        # 1. Получаем статью со статусом 'selected'
        article_id = None # Инициализируем на случай ошибки
        selected_article_info = None
        try:
            # Получаем данные ОДНОЙ выбранной статьи
            selected_article_info = db_utils.get_selected_article()
        except AttributeError:
             logger.error(f"Агент '{self.name}': Функция 'get_selected_article' не найдена в db_utils. Цикл прерван.")
             return
        except Exception as e:
            logger.error(f"Агент '{self.name}': Ошибка при получении 'selected' статьи из БД: {e}", exc_info=True)
            return

        if not selected_article_info:
            logger.info(f"Агент '{self.name}': Нет статей со статусом 'selected' для генерации поста.")
            return

        # Извлекаем ключевые данные
        article_id = selected_article_info.get('id')
        cleaned_md_path = selected_article_info.get('cleaned_md_path')
        # --- Получаем URL из данных БД ---
        url = selected_article_info.get('url')
        # ---

        if not article_id or not cleaned_md_path or not url:
             error_msg = f"Получены неполные данные для выбранной статьи ID={article_id} (url={url}, path={cleaned_md_path}). Невозможно продолжить."
             logger.error(f"Агент '{self.name}': {error_msg}")
             # Если неполные данные, вернем статус 'summarized', чтобы дать шанс другой статье
             if article_id:
                  db_utils.update_article_status(article_id, 'summarized', error_msg="Неполные данные для генерации поста")
             return

        logger.info(f"Агент '{self.name}': Начинаем генерацию поста для статьи ID: {article_id} (URL: {url})")

        # 2. Читаем исходный MD файл статьи
        title = "" # Инициализируем заголовок
        text_content = None
        source_name = None
        publication_date = None
        try:
            yaml_data, text_content = md_utils.read_md_file(cleaned_md_path)
            if text_content is None:
                 raise ValueError(f"Не удалось прочитать текст из {cleaned_md_path}")
            if not text_content.strip():
                 raise ValueError(f"Текст статьи в {cleaned_md_path} пустой")

            # Извлекаем метаданные из YAML (или из данных БД, если надежнее)
            if yaml_data:
                 title = yaml_data.get('title', selected_article_info.get('title', '')) # Берем из YAML или БД
                 source_name = yaml_data.get('source', selected_article_info.get('source_name', 'unknown_source'))
                 publication_date = yaml_data.get('publication_date', selected_article_info.get('publication_date', None))
            else: # Если YAML нет, берем данные из БД
                 title = selected_article_info.get('title', '')
                 source_name = selected_article_info.get('source_name', 'unknown_source')
                 publication_date = selected_article_info.get('publication_date', None)

            if not title:
                 logger.warning(f"Агент '{self.name}': Не удалось определить заголовок для статьи ID {article_id}.")

        except Exception as e:
            error_msg = f"Ошибка чтения исходного MD файла {cleaned_md_path} для ID {article_id}: {e}"
            logger.error(f"Агент '{self.name}': {error_msg}", exc_info=True)
            db_utils.update_article_status(article_id, 'generation_failed', error_msg=error_msg)
            return

        # 3. Генерируем пост с помощью LLM
        generated_post_text = None
        try:
            if not self.content_prompt_template:
                raise ValueError("Шаблон промпта для генерации поста не определен.")

            # --- ФОРМИРУЕМ ПРОМПТ с title, text и url ---
            prompt = self.content_prompt_template.format(
                title=title,
                text=text_content,
                url=url # <-- Передаем URL
            )
            # ---

            logger.info(f"Агент '{self.name}': Отправка запроса LLM (модель: {self.model_name}) для генерации поста ID: {article_id}...")
            response = await self.model.generate_content_async(prompt)

            # Проверка ответа
            if not response.candidates or not response.candidates[0].content or not response.candidates[0].content.parts:
                 # Иногда модель может заблокировать ответ из-за safety settings
                 block_reason = "Неизвестно"
                 if response.prompt_feedback and response.prompt_feedback.block_reason:
                      block_reason = response.prompt_feedback.block_reason.name
                 raise ValueError(f"Модель ИИ вернула некорректный ответ (нет content/parts). Причина блокировки: {block_reason}")

            generated_post_text = response.text.strip()

            if not generated_post_text:
                 raise ValueError("Модель ИИ вернула пустой текст поста.")

            logger.info(f"Агент '{self.name}': Текст поста успешно сгенерирован для статьи ID: {article_id}")

        except KeyError as e:
            error_msg = f"Ошибка форматирования промпта: ключ {e}"
            logger.error(f"Агент '{self.name}': {error_msg} (ID: {article_id})", exc_info=False)
            db_utils.update_article_status(article_id, 'generation_failed', error_msg=error_msg)
            return
        except ResourceExhausted as e:
            error_details = str(e.message if hasattr(e, 'message') else e)
            error_msg = f"Превышен лимит API (429): {error_details[:400]}"
            logger.error(f"Агент '{self.name}': {error_msg} (ID: {article_id})", exc_info=False)
            db_utils.update_article_status(article_id, 'generation_failed', error_msg=error_msg)
            return
        except Exception as e: # Ловим ValueError и другие ошибки API/форматирования
            error_details = str(e.message if hasattr(e, 'message') else e)
            error_msg = f"Ошибка генерации поста: {error_details[:500]}"
            logger.error(f"Агент '{self.name}': {error_msg} (ID: {article_id})", exc_info=True)
            db_utils.update_article_status(article_id, 'generation_failed', error_msg=error_msg)
            return

        # 4. Создаем новый MD файл для поста
        post_md_path = None
        try:
            # Генерируем имя файла для поста
            post_md_path = file_utils.generate_filename(
                self.posts_md_path,
                source_name,
                title if title else f"post_{article_id}",
                "md"
            )

            # Готовим YAML данные для файла поста
            post_yaml_data = {
                "title": title,
                "source": source_name,
                "url": url, # Используем URL из БД
                "publication_date": publication_date, # Используем дату из YAML/БД
                "original_article_id": article_id,
                "generated_at": datetime.now().isoformat(), # Добавляем время генерации
                "status": "post_generated"
            }
            # Убираем ключи с None значениями
            post_yaml_data = {k: v for k, v in post_yaml_data.items() if v is not None}

            # Создаем MD файл
            success = md_utils.create_post_md_file(post_md_path, post_yaml_data, generated_post_text)
            if not success:
                raise IOError(f"Не удалось создать MD файл поста: {post_md_path}")

        except Exception as e:
            error_msg = f"Ошибка создания MD файла поста: {e}"
            logger.error(f"Агент '{self.name}': {error_msg} (ID: {article_id})", exc_info=True)
            db_utils.update_article_status(article_id, 'generation_failed', error_msg=error_msg)
            return

        # 5. Обновляем статус и путь в БД статьи
        # Передаем путь к СОЗДАННОМУ файлу поста
        db_utils.update_article_status(article_id, 'post_generated', file_path=post_md_path)
        logger.info(f"Агент '{self.name}': Пост для статьи ID {article_id} успешно сгенерирован и сохранен в {post_md_path}. Статус в БД обновлен.")

        logger.info(f"Агент '{self.name}': Цикл генерации поста завершен.")