# main.py

import asyncio
import json
import logging
import os
import re
import yaml
from dotenv import load_dotenv
from google.cloud.aiplatform_v1beta1.types import Content, Part
from utils import db_utils
from agents.fetcher_agent import FetcherAgent
from agents.cleaner_agent import CleanerAgent
from agents.summarizer_agent import SummarizerAgent
from agents.selector_agent import SelectorAgent
from agents.generator_agent import GeneratorAgent 

logging.basicConfig(level=logging.INFO,
                    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
                    datefmt='%Y-%m-%d %H:%M:%S')
logger = logging.getLogger("MainApp")

# --- load_config ---
def load_config():
    """Загружает конфигурацию из YAML и обрабатывает плейсхолдеры переменных окружения."""
    try:
        with open("config.yaml", 'r', encoding='utf-8') as f:
            config_str = f.read()
        def replace_env_var(match):
            var_name = match.group(1)
            return os.getenv(var_name, f"%{var_name}%")
        config_str_expanded = re.sub(r"%(\w+)%", replace_env_var, config_str)
        config = yaml.safe_load(config_str_expanded)
        logger.info("Конфигурация успешно загружена.")
        return config
    except FileNotFoundError: logger.error("Ошибка: config.yaml не найден!"); return None
    except yaml.YAMLError as e: logger.error(f"Ошибка парсинга config.yaml: {e}"); return None
    except Exception as e: logger.error(f"Неожиданная ошибка при загрузке конфигурации: {e}"); return None

# --- main ---
async def main():
    """Основная асинхронная функция для запуска приложения."""
    load_dotenv()
    config = load_config()
    if not config: return

    db_path = config.get('paths', {}).get('db', 'db/articles.db')
    db_utils.set_db_path(db_path)
    db_utils.init_db() # Создаст или обновит таблицу

    # --- Пересборка моделей ---
    logger.info("Пересборка моделей агентов...")
    try:
        FetcherAgent.model_rebuild(_types_namespace={'CleanerAgent': CleanerAgent})
        # Для других агентов model_rebuild не требуется, т.к. у них нет forward refs
        logger.info("Модели агентов успешно пересобраны.")
    except Exception as e:
        logger.error(f"Не удалось пересобрать модели агентов: {e}", exc_info=True); return

    # --- Создание экземпляров агентов ---
    logger.info("Создание экземпляров агентов...")
    try:
        cleaner = CleanerAgent(agent_id="cleaner_agent", config=config)
        fetcher = FetcherAgent(agent_id="fetcher_agent", config=config, cleaner_agent_instance=cleaner)
        summarizer = SummarizerAgent(agent_id="summarizer_agent", config=config)
        selector = SelectorAgent(agent_id="selector_agent", config=config)
        generator = GeneratorAgent(agent_id="generator_agent", config=config) # <-- Создаем GeneratorAgent
        logger.info("Экземпляры агентов успешно созданы.")
    except Exception as e:
        logger.error(f"Не удалось создать экземпляры агентов: {e}", exc_info=True); return

    # --- Блок обработки "застрявших" статей при старте ---
    # Этот блок теперь обрабатывает последовательно: raw -> cleaned -> summarized -> selected -> post_generated

    # 1. Очистка 'raw_fetched'
    logger.info("Проверка наличия 'raw_fetched' статей для немедленной очистки...")
    try:
        raw_ids_to_clean = db_utils.get_articles_by_status('raw_fetched')
        if raw_ids_to_clean:
             logger.info(f"Найдено {len(raw_ids_to_clean)} 'raw_fetched'. Запуск cleaner...")
             payload = {"article_ids": raw_ids_to_clean}
             payload_str = json.dumps(payload)
             cleaning_content = Content(role="system", parts=[Part(text=payload_str)])
             await cleaner.handle_cleaning_request(cleaning_content)
             logger.info("Запуск cleaner для существующих 'raw_fetched' завершен.")
        else: logger.info("Статьи 'raw_fetched' для немедленной очистки не найдены.")
    except AttributeError as e: logger.error(f"Пропущена проверка 'raw_fetched': {e}.")
    except Exception as e: logger.error(f"Ошибка при очистке существующих 'raw_fetched': {e}", exc_info=True)

    # 2. Резюмирование 'cleaned'
    logger.info("Проверка наличия 'cleaned' статей для немедленного резюмирования...")
    try:
        # Summarizer сам найдет статьи 'cleaned' внутри
        await summarizer.run_summarize_cycle()
        logger.info("Запуск summarizer для существующих 'cleaned' завершен.")
    except Exception as e:
        logger.error(f"Ошибка при запуске немедленного цикла резюмирования: {e}", exc_info=True)

    # 3. Выбор 'summarized'
    logger.info("Проверка наличия 'summarized' статей для немедленного выбора...")
    try:
        # Selector сам найдет 'summarized' внутри
        await selector.run_selection_cycle()
        logger.info("Запуск selector для существующих 'summarized' завершен.")
    except Exception as e:
        logger.error(f"Ошибка при запуске немедленного цикла выбора: {e}", exc_info=True)


    # 4. Генерация постов для 'selected'
    logger.info("Проверка наличия 'selected' статей для немедленной генерации поста...")
    try:
        # Generator сам найдет 'selected' внутри
        await generator.run_generation_cycle() # <-- Вызываем генератор при старте
        logger.info("Запуск generator для существующей 'selected' статьи (если была) завершен.")
    except Exception as e:
         logger.error(f"Ошибка при запуске немедленного цикла генерации поста: {e}", exc_info=True)

    logger.info("Предварительная обработка завершена.")
    # --- Конец блока обработки "застрявших" ---


    # --- Логика периодического запуска агентов ---
    async def run_periodic_cycle():
        """Запускает циклы агентов: Fetcher -> Summarizer -> Selector -> Generator."""
        while True:
            logger.info("--- Начало нового периодического цикла ---")

            # --- Fetcher ---
            logger.info("Запуск периодического цикла Fetcher...")
            try:
                await fetcher.run_fetch_cycle()
                logger.info("Периодический цикл Fetcher завершен.")
            except Exception as e:
                logger.error(f"Ошибка в цикле Fetcher: {e}", exc_info=True); await asyncio.sleep(60)

            # --- Summarizer ---
            logger.info("Запуск периодического цикла Summarizer...")
            try:
                await summarizer.run_summarize_cycle()
                logger.info("Периодический цикл Summarizer завершен.")
            except Exception as e:
                 logger.error(f"Ошибка в цикле Summarizer: {e}", exc_info=True); await asyncio.sleep(60)

            # --- Selector ---
            logger.info("Запуск периодического цикла Selector...")
            try:
                await selector.run_selection_cycle()
                logger.info("Периодический цикл Selector завершен.")
            except Exception as e:
                 logger.error(f"Ошибка в цикле Selector: {e}", exc_info=True); await asyncio.sleep(60)

            # --- Generator ---
            logger.info("Запуск периодического цикла Generator...")
            try:
                await generator.run_generation_cycle() # <-- Вызываем Generator
                logger.info("Периодический цикл Generator завершен.")
            except Exception as e:
                 logger.error(f"Ошибка в цикле Generator: {e}", exc_info=True); await asyncio.sleep(60)

            # --- Ожидание ---
            cycle_interval_seconds = int(config.get('cycle_interval', 3600)) # Берем из конфига или 1 час
            logger.info(f"--- Цикл завершен. Ожидание {cycle_interval_seconds} секунд... ---")
            await asyncio.sleep(cycle_interval_seconds)


    logger.info("Запуск основного цикла приложения...")
    try:
        await run_periodic_cycle()
    except KeyboardInterrupt: logger.info("Получен сигнал завершения (KeyboardInterrupt).")
    except asyncio.CancelledError: logger.info("Основной цикл отменен.")
    finally: logger.info("Приложение остановлено.")

# --- Точка входа ---
if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt: logger.info("Приложение завершено пользователем.")
    except Exception as e: logger.critical(f"Критическая необработанная ошибка в main: {e}", exc_info=True)