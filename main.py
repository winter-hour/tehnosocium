# main.py
# --- Импорты ---
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
from agents.selector_agent import SelectorAgent # <-- Импортируем SelectorAgent

logging.basicConfig(level=logging.INFO,
                    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
                    datefmt='%Y-%m-%d %H:%M:%S')
logger = logging.getLogger("MainApp")

# --- load_config ---
def load_config():
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
    load_dotenv()
    config = load_config()
    if not config: return

    db_path = config.get('paths', {}).get('db', 'db/articles.db')
    db_utils.set_db_path(db_path)
    db_utils.init_db()

    # --- Пересборка моделей ---
    logger.info("Пересборка моделей агентов...")
    try:
        FetcherAgent.model_rebuild(_types_namespace={'CleanerAgent': CleanerAgent})
        logger.info("Модели агентов успешно пересобраны.")
    except Exception as e:
        logger.error(f"Не удалось пересобрать модели агентов: {e}", exc_info=True); return

    # --- Создание экземпляров агентов ---
    logger.info("Создание экземпляров агентов...")
    try:
        cleaner = CleanerAgent(agent_id="cleaner_agent", config=config)
        fetcher = FetcherAgent(agent_id="fetcher_agent", config=config, cleaner_agent_instance=cleaner)
        summarizer = SummarizerAgent(agent_id="summarizer_agent", config=config)
        selector = SelectorAgent(agent_id="selector_agent", config=config) # <-- Создаем Selector
        logger.info("Экземпляры агентов успешно созданы.")
    except Exception as e:
        logger.error(f"Не удалось создать экземпляры агентов: {e}", exc_info=True); return

    # --- Блок очистки 'raw_fetched' ---
    logger.info("Проверка наличия 'raw_fetched' статей для немедленной очистки...")
    try:
        raw_ids_to_clean = db_utils.get_articles_by_status('raw_fetched')
        if raw_ids_to_clean:
             logger.info(f"Найдено {len(raw_ids_to_clean)} статей со статусом 'raw_fetched'. Запуск cleaner...")
             payload = {"article_ids": raw_ids_to_clean}
             payload_str = json.dumps(payload)
             cleaning_content = Content(role="system", parts=[Part(text=payload_str)])
             await cleaner.handle_cleaning_request(cleaning_content)
             logger.info("Запуск cleaner для существующих статей завершен.")
        else: logger.info("Статьи со статусом 'raw_fetched' для немедленной очистки не найдены.")
    except AttributeError as e: logger.error(f"Пропущена проверка 'raw_fetched': {e}.")
    except Exception as e: logger.error(f"Ошибка при очистке существующих статей: {e}", exc_info=True)

    # --- Блок резюмирования 'cleaned' ---
    logger.info("Проверка наличия 'cleaned' статей для немедленного резюмирования...")
    try:
        await summarizer.run_summarize_cycle()
        logger.info("Запуск summarizer для существующих статей завершен.")
    except Exception as e:
        logger.error(f"Ошибка при запуске немедленного цикла резюмирования: {e}", exc_info=True)

    # --- Логика периодического запуска агентов ---
    async def run_periodic_cycle():
        """Запускает циклы агентов: Fetcher -> Summarizer -> Selector."""
        while True:
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
                await selector.run_selection_cycle() # <-- Вызываем Selector
                logger.info("Периодический цикл Selector завершен.")
            except Exception as e:
                 logger.error(f"Ошибка в цикле Selector: {e}", exc_info=True); await asyncio.sleep(60)

            # --- Ожидание ---
            cycle_interval_seconds = int(config.get('cycle_interval', 3600))
            logger.info(f"Ожидание {cycle_interval_seconds} секунд до следующего полного цикла...")
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