# main.py
# --- Стандартные импорты ---
import asyncio
import json # Убедимся, что json импортирован
import logging
import os
import re
import yaml

# --- Импорты зависимостей ---
from dotenv import load_dotenv
from google.cloud.aiplatform_v1beta1.types import Content, Part # Убедимся, что Part импортирован

# --- Импорты проекта ---
from utils import db_utils
# --- Сначала импортируем ОБА класса агентов ---
from agents.fetcher_agent import FetcherAgent
from agents.cleaner_agent import CleanerAgent # Убедимся, что CleanerAgent импортирован

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger("MainApp")

# --- Функция load_config ---
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
        logger.info("Configuration loaded successfully.")
        return config
    except FileNotFoundError:
        logger.error("Error: config.yaml not found!")
        return None
    except yaml.YAMLError as e:
        logger.error(f"Error parsing config.yaml: {e}")
        return None
    except Exception as e:
         logger.error(f"An unexpected error occurred during config loading: {e}")
         return None

# --- Функция main ---
async def main():
    """Основная асинхронная функция для запуска приложения."""
    load_dotenv() # Загрузка переменных окружения из .env
    config = load_config() # Загрузка конфигурации из config.yaml
    if not config:
        return # Выход, если конфигурация не загружена

    # Инициализация базы данных
    db_path = config.get('paths', {}).get('db', 'db/articles.db')
    db_utils.set_db_path(db_path)
    db_utils.init_db() # Убедимся, что таблица создана

    # Пересборка модели FetcherAgent для разрешения форвардных ссылок (на CleanerAgent)
    logger.info("Rebuilding FetcherAgent model to resolve forward references...")
    try:
        FetcherAgent.model_rebuild(
            _types_namespace={'CleanerAgent': CleanerAgent}
        )
        logger.info("FetcherAgent model rebuilt successfully.")
    except Exception as e:
        logger.error(f"Failed to rebuild FetcherAgent model: {e}", exc_info=True)
        return # Не можем продолжать без корректной модели

    # Создание экземпляров агентов
    logger.info("Creating agent instances...")
    try:
        cleaner = CleanerAgent(agent_id="cleaner_agent", config=config)
        # Передаем ссылку на cleaner_agent в fetcher для прямого вызова
        fetcher = FetcherAgent(agent_id="fetcher_agent", config=config, cleaner_agent_instance=cleaner)
        logger.info("Agents created successfully.")
    except Exception as e:
        logger.error(f"Failed to create agent instances: {e}", exc_info=True)
        return # Не можем продолжать без агентов

    # --- БЛОК: Запуск очистки для уже существующих статей со статусом 'raw_fetched' ---
    logger.info("Checking for existing 'raw_fetched' articles to clean...")
    try:
        # Получаем ID статей, ожидающих очистки (требует наличия функции в db_utils)
        raw_ids_to_clean = db_utils.get_articles_by_status('raw_fetched')

        if raw_ids_to_clean:
            logger.info(f"Found {len(raw_ids_to_clean)} articles with 'raw_fetched' status. Triggering cleaner...")
            # Формируем полезную нагрузку для вызова cleaner'а
            payload = {"article_ids": raw_ids_to_clean}
            try:
                payload_str = json.dumps(payload)
                # Создаем Content с использованием Part(text=...)
                cleaning_content = Content(role="system", parts=[Part(text=payload_str)])
                # Напрямую вызываем обработчик в CleanerAgent
                await cleaner.handle_cleaning_request(cleaning_content)
                logger.info("Manual cleaner trigger for existing articles finished processing.")
            except json.JSONDecodeError as e:
                logger.error(f"Failed to serialize existing article IDs to JSON: {e}")
            except Exception as e:
                # Ловим ошибки при вызове cleaner'а
                logger.error(f"Error during manual cleaner trigger for existing articles: {e}", exc_info=True)
        else:
            logger.info("No articles with 'raw_fetched' status found in DB to clean immediately.")
    except AttributeError:
        logger.error("Function 'get_articles_by_status' not found in db_utils. Please add it.")
    except Exception as e:
        logger.error(f"Error checking/triggering cleaning for existing articles: {e}", exc_info=True)
    # --- КОНЕЦ БЛОКА ---

    # --- Логика периодического запуска Fetcher'а ---
    async def run_periodic_fetch():
        """Запускает первый цикл Fetcher'а немедленно, затем периодически."""

        # --- Запускаем первый цикл сразу ---
        logger.info("Running initial fetch cycle immediately...")
        try:
            await fetcher.run_fetch_cycle()
        except Exception as e:
            logger.error(f"Error during initial fetcher cycle: {e}", exc_info=True)
            # Добавим небольшую паузу после ошибки перед началом основного цикла
            await asyncio.sleep(60)

        # --- Основной цикл ожидания и запуска ---
        while True:
            # Интервал между циклами проверки RSS (в секундах)
            fetch_interval_seconds = int(config.get('fetch_interval', 3600)) # Берем из конфига или по умолчанию 1 час
            logger.info(f"Waiting {fetch_interval_seconds} seconds for next fetch cycle...")
            await asyncio.sleep(fetch_interval_seconds) # Ожидание

            # Запуск следующего цикла Fetcher'а
            logger.info("Triggering periodic FetcherAgent cycle...")
            try:
                await fetcher.run_fetch_cycle()
            except Exception as e:
                logger.error(f"Error during periodic fetcher cycle: {e}", exc_info=True)
                # Пауза после ошибки, чтобы не перегружать систему/логи
                await asyncio.sleep(60)

    logger.info("Starting periodic fetch loop...")
    try:
        # Запускаем асинхронную функцию с циклом
        await run_periodic_fetch()
    except KeyboardInterrupt:
        # Обработка прерывания пользователем (Ctrl+C)
        logger.info("Shutdown signal received (KeyboardInterrupt).")
    except asyncio.CancelledError:
        # Обработка отмены задачи (если потребуется в будущем)
        logger.info("Fetch loop task cancelled.")
    finally:
        # Выполняется при любом завершении цикла (нормальном или по ошибке/прерыванию)
        logger.info("Application stopped.")

# --- Точка входа в скрипт ---
if __name__ == "__main__":
    try:
        # Запуск основного асинхронного приложения
        asyncio.run(main())
    except KeyboardInterrupt:
        # Позволяем asyncio корректно обработать прерывание
        logger.info("Application terminated by user.")
    except Exception as e:
        # Ловим любые другие неожиданные ошибки на самом верхнем уровне
        logger.critical(f"Unhandled exception occurred in main execution: {e}", exc_info=True)