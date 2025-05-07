# agents/selector_agent.py

import asyncio
import json
import logging
import os
import re # Для извлечения URL
from typing import Any, Dict, List, Optional

import google.generativeai as genai
from google.adk.agents.base_agent import BaseAgent
from google.api_core.exceptions import ResourceExhausted

from utils import db_utils, md_utils # Импортируем наши утилиты

# Настройка логирования
logging.basicConfig(level=logging.INFO,
                    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
                    datefmt='%Y-%m-%d %H:%M:%S')
logger = logging.getLogger("SelectorAgent")

class SelectorAgent(BaseAgent):
    """
    Агент для выбора наилучшей статьи для публикации из числа недавних статей
    с готовыми резюме (статус 'summarized').
    Обновляет статус выбранной статьи на 'selected'.
    """

    # --- Объявление полей класса ---
    config: Optional[Dict[str, Any]] = None
    api_key: Optional[str] = None
    model_name: Optional[str] = None
    selector_prompt_template: Optional[str] = None
    model: Optional[genai.GenerativeModel] = None
    generation_config: Optional[genai.GenerationConfig] = None
    max_summaries_per_prompt: int = 50
    selection_timespan_hours: int = 24 # По умолчанию 24 часа

    def __init__(self, agent_id: str = "selector_agent", config: Optional[Dict[str, Any]] = None):
        """
        Инициализирует SelectorAgent.
        """
        super().__init__(name=agent_id)
        self.config = config if config else {}
        selector_config = self.config.get('selector', {})
        self.api_key = os.getenv("GEMINI_API_KEY")
        default_model = self.config.get('default_model', 'gemini-1.5-pro-latest')
        self.model_name = selector_config.get('model', default_model)
        self.selector_prompt_template = selector_config.get('prompt')
        self.max_summaries_per_prompt = selector_config.get('max_summaries_in_prompt', self.max_summaries_per_prompt)
        self.selection_timespan_hours = selector_config.get('selection_timespan_hours', self.selection_timespan_hours)

        # --- Проверки и инициализация модели ---
        if not self.api_key: logger.error(f"Агент '{self.name}': Ключ GEMINI_API_KEY не найден!")
        if not self.model_name: logger.error(f"Агент '{self.name}': Имя модели ИИ не настроено!")
        if not self.selector_prompt_template: logger.error(f"Агент '{self.name}': Шаблон промпта для выбора не найден!")

        if self.api_key and self.model_name and self.selector_prompt_template:
            try:
                genai.configure(api_key=self.api_key)
                self.generation_config = genai.GenerationConfig(candidate_count=1, temperature=0.2)
                self.model = genai.GenerativeModel(
                    model_name=self.model_name,
                    generation_config=self.generation_config,
                )
                logger.info(f"Агент '{self.name}' инициализирован. Модель: {self.model_name}.")
            except Exception as e:
                logger.error(f"Агент '{self.name}': Не удалось инициализировать Google AI: {e}", exc_info=True)
                self.model = None
        else:
             logger.warning(f"Агент '{self.name}': Клиент Google AI не инициализирован.")
             self.model = None

        logger.info(f"Агент '{self.name}': Максимум резюме за запрос: {self.max_summaries_per_prompt}")
        logger.info(f"Агент '{self.name}': Период для выбора недавних статей: {self.selection_timespan_hours} ч.")

    async def run_selection_cycle(self):
        """
        Выполняет один цикл выбора лучшей статьи из недавних ('summarized').
        """
        logger.info(f"Агент '{self.name}': Запуск цикла выбора статьи...")

        if not self.model:
            logger.error(f"Агент '{self.name}': Модель ИИ не инициализирована. Цикл выбора прерван.")
            return

        # 1. Получаем НЕДАВНИЕ статьи со статусом 'summarized'
        try:
            # Используем новую функцию и параметр из конфига
            candidate_articles_info = db_utils.get_recent_summarized_articles(
                timespan_hours=self.selection_timespan_hours
            )
        except AttributeError:
             logger.error(f"Агент '{self.name}': Функция 'get_recent_summarized_articles' не найдена в db_utils. Цикл прерван.")
             return
        except Exception as e:
            logger.error(f"Агент '{self.name}': Ошибка при получении недавних статей из БД: {e}", exc_info=True)
            return

        if not candidate_articles_info:
            logger.info(f"Агент '{self.name}': Нет недавних статей со статусом 'summarized' для выбора.")
            return

        # 2. Читаем резюме из MD-файлов
        candidates_with_summaries = []
        for article_info in candidate_articles_info:
            md_path = article_info.get('cleaned_md_path')
            article_id = article_info.get('id')
            if not md_path:
                logger.warning(f"Агент '{self.name}': Отсутствует путь к MD файлу для статьи ID {article_id}. Пропуск.")
                continue

            try: # Добавляем обработку ошибок чтения файла
                yaml_data, _ = md_utils.read_md_file(md_path)
                if yaml_data and 'summary' in yaml_data:
                    summary = yaml_data['summary']
                    if summary and isinstance(summary, str) and summary.strip():
                        candidates_with_summaries.append({
                            "id": article_id,
                            "title": article_info.get('title', 'Без заголовка'),
                            "url": article_info.get('url'),
                            "summary": summary.strip()
                        })
                    else:
                        logger.warning(f"Агент '{self.name}': Пустое или некорректное резюме в YAML для статьи ID {article_id}. Пропуск.")
                else:
                    logger.warning(f"Агент '{self.name}': Не удалось прочитать YAML или отсутствует ключ 'summary' для статьи ID {article_id}. Пропуск.")
            except Exception as e:
                 logger.error(f"Агент '{self.name}': Ошибка чтения MD файла {md_path} для статьи ID {article_id}: {e}. Пропуск.", exc_info=True)


        if not candidates_with_summaries:
            logger.info(f"Агент '{self.name}': Не найдено статей с корректными резюме для выбора.")
            return

        logger.info(f"Агент '{self.name}': Подготовлено {len(candidates_with_summaries)} кандидатов для выбора.")

        # 3. Ограничиваем количество кандидатов
        candidates_to_send = candidates_with_summaries
        if len(candidates_to_send) > self.max_summaries_per_prompt:
            logger.warning(f"Агент '{self.name}': Количество кандидатов ({len(candidates_to_send)}) превышает лимит ({self.max_summaries_per_prompt}). Используем первые {self.max_summaries_per_prompt}.")
            candidates_to_send = candidates_to_send[:self.max_summaries_per_prompt]

        # 4. Формируем блок резюме для промпта
        summaries_block = ""
        if not candidates_to_send: # Доп. проверка после возможного среза
             logger.info(f"Агент '{self.name}': После применения лимита не осталось кандидатов для отправки.")
             return

        for i, cand in enumerate(candidates_to_send):
            summaries_block += f"Кандидат {i+1}:\n" # Нумеруем для ясности
            summaries_block += f"Заголовок: {cand['title']}\n"
            summaries_block += f"URL: {cand['url']}\n"
            summaries_block += f"Резюме: {cand['summary']}\n"
            summaries_block += "---\n"

        # 5. Вызываем LLM для выбора
        selected_url = None
        try:
            if not self.selector_prompt_template:
                 raise ValueError("Шаблон промпта для выбора не определен.")

            prompt = self.selector_prompt_template.format(summaries_block=summaries_block.strip())

            logger.info(f"Агент '{self.name}': Отправка запроса LLM для выбора из {len(candidates_to_send)} кандидатов...")
            # print(f"\n--- PROMPT ДЛЯ ВЫБОРА ---\n{prompt}\n------------------------\n") # Для отладки
            response = await self.model.generate_content_async(prompt)

            raw_response_text = response.text.strip()
            logger.info(f"Агент '{self.name}': Получен ответ от LLM: '{raw_response_text}'")

            # 6. Извлекаем URL из ответа (улучшенная проверка)
            # Ищем URL, окруженный пробелами или в начале/конце строки, чтобы избежать частичных совпадений
            url_pattern = r'(?:^|\s)(https?://\S+)(?:\s|$)'
            url_match = re.search(url_pattern, raw_response_text)

            # Проверяем, что найден только один URL и он занимает большую часть строки
            # (допускаем небольшие артефакты в начале/конце ответа модели)
            potential_urls = re.findall(r'https?://\S+', raw_response_text)

            if len(potential_urls) == 1 and potential_urls[0] == url_match.group(1):
                 selected_url = potential_urls[0]
                 # Дополнительная проверка: не слишком ли много лишнего текста вокруг?
                 if len(raw_response_text) > len(selected_url) + 15: # Допуск 15 символов на артефакты
                     logger.warning(f"Агент '{self.name}': Ответ LLM содержит URL '{selected_url}', но также лишний текст: '{raw_response_text}'. Используем извлеченный URL.")
                 else:
                      logger.info(f"Агент '{self.name}': Из ответа LLM успешно извлечен URL: {selected_url}")

            else:
                 logger.error(f"Агент '{self.name}': Ответ LLM не содержит единственный и четкий URL. Ответ: '{raw_response_text}'. Выбор не сделан.")

        except ResourceExhausted as e:
            error_details = str(e.message if hasattr(e, 'message') else e)
            logger.error(f"Агент '{self.name}': Превышен лимит API (429) при выборе статьи: {error_details}", exc_info=False)
        except Exception as e:
            error_details = str(e.message if hasattr(e, 'message') else e)
            logger.error(f"Агент '{self.name}': Ошибка API Gemini или другая ошибка при выборе статьи: {error_details}", exc_info=True)

        # 7. Обновляем статус выбранной статьи (в файле и БД)
        if selected_url:
            selected_article_id = None
            selected_md_path = None # Нам нужен путь к MD файлу выбранной статьи
            # Ищем ID и ПУТЬ статьи по URL среди тех, что отправляли на выбор
            for cand in candidates_to_send:
                if cand['url'] == selected_url:
                    selected_article_id = cand['id']
                    # Нам нужно найти исходную информацию для этого ID, включая cleaned_md_path
                    # Пройдемся по списку из БД еще раз (или сохраним его заранее)
                    for original_info in candidate_articles_info: # candidate_articles_info - список из БД
                         if original_info['id'] == selected_article_id:
                              selected_md_path = original_info.get('cleaned_md_path')
                              break
                    break # Выходим из внешнего цикла, как только нашли

            if selected_article_id and selected_md_path:
                logger.info(f"Агент '{self.name}': Выбрана статья ID: {selected_article_id} (URL: {selected_url}). Обновление статуса на 'selected'...")

                # --- СНАЧАЛА ОБНОВЛЯЕМ YAML В MD ФАЙЛЕ ---
                try:
                    data_to_update = {'status': 'selected'}
                    success = md_utils.update_md_yaml(selected_md_path, data_to_update)
                    if success:
                        logger.info(f"Агент '{self.name}': Статус в YAML файла {selected_md_path} обновлен на 'selected'.")
                        # --- ТОЛЬКО ПОСЛЕ УСПЕШНОГО ОБНОВЛЕНИЯ ФАЙЛА, ОБНОВЛЯЕМ БД ---
                        db_utils.update_article_status(selected_article_id, 'selected')
                    else:
                        # Ошибка обновления MD файла (уже залогирована в md_utils)
                        # НЕ обновляем статус в БД, чтобы сохранить консистентность
                        logger.error(f"Агент '{self.name}': Не удалось обновить статус в YAML файла {selected_md_path}. Статус в БД НЕ изменен.")
                        # Можно добавить обновление статуса в БД на 'selection_failed' здесь, если нужно
                        # db_utils.update_article_status(selected_article_id, 'selection_failed', error_msg="Ошибка обновления MD файла при выборе")

                except Exception as e:
                    logger.error(f"Агент '{self.name}': Неожиданная ошибка при обновлении MD файла {selected_md_path} для ID {selected_article_id}: {e}", exc_info=True)
                    # Статус в БД НЕ обновляем
                    # Можно пометить как 'selection_failed'
                    # db_utils.update_article_status(selected_article_id, 'selection_failed', error_msg=f"Ошибка обновления MD: {e}")

            elif selected_article_id and not selected_md_path:
                 logger.error(f"Агент '{self.name}': Найден ID={selected_article_id}, но не найден путь к MD файлу для URL '{selected_url}'. Невозможно обновить статус в файле.")
                 # Статус в БД не обновляем, так как файл не можем обновить
            else:
                logger.error(f"Агент '{self.name}': LLM вернула URL '{selected_url}', но статья с таким URL не найдена среди кандидатов, отправленных на выбор. Выбор не сделан.")
        else:
            logger.info(f"Агент '{self.name}': URL не был выбран моделью или не удалось извлечь.")

        logger.info(f"Агент '{self.name}': Цикл выбора статьи завершен.")