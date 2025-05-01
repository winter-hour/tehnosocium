# main.py
# --- Стандартные импорты ---
import asyncio
import json
import logging
import os
import re
import yaml

# --- Импорты зависимостей ---
from dotenv import load_dotenv
from google.cloud.aiplatform_v1beta1.types import Content, Part

# --- Импорты проекта ---
from utils import db_utils
# --- Импортируем ВСЕ нужные классы агентов ---
from agents.fetcher_agent import FetcherAgent
from agents.cleaner_agent import CleanerAgent
from agents.summarizer_agent import SummarizerAgent # <-- Импорт SummarizerAgent

logging.basicConfig(level=logging.INFO,
                    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
                    datefmt='%Y-%m-%d %H:%M:%S')
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
        logger.info("Конфигурация успешно загружена.")
        return config
    except FileNotFoundError:
        logger.error("Ошибка: config.yaml не найден!")
        return None
    except yaml.YAMLError as e:
        logger.error(f"Ошибка парсинга config.yaml: {e}")
        return None
    except Exception as e:
         logger.error(f"Неожиданная ошибка при загрузке конфигурации: {e}")
         return None

# --- Функция main ---
async def main():
    """Основная асинхронная функция для запуска приложения."""
    load_dotenv()
    config = load_config()
    if not config:
        return

    # Инициализация базы данных
    db_path = config.get('paths', {}).get('db', 'db/articles.db')
    db_utils.set_db_path(db_path)
    db_utils.init_db() # Создаст или обновит таблицу

    # Пересборка модели FetcherAgent
    logger.info("Пересборка модели FetcherAgent для разрешения forward references...")
    try:
        FetcherAgent.model_rebuild(
            _types_namespace={'CleanerAgent': CleanerAgent}
        )
        # SummarizerAgent.model_rebuild() # Не требуется, т.к. нет forward refs
        logger.info("Модель FetcherAgent успешно пересобрана.")
    except Exception as e:
        logger.error(f"Не удалось пересобрать модель FetcherAgent: {e}", exc_info=True)
        return

    # Создание экземпляров агентов
    logger.info("Создание экземпляров агентов...")
    try:
        cleaner = CleanerAgent(agent_id="cleaner_agent", config=config)
        fetcher = FetcherAgent(agent_id="fetcher_agent", config=config, cleaner_agent_instance=cleaner)
        summarizer = SummarizerAgent(agent_id="summarizer_agent", config=config) # <-- Создаем Summarizer
        logger.info("Экземпляры агентов успешно созданы.")
    except Exception as e:
        logger.error(f"Не удалось создать экземпляры агентов: {e}", exc_info=True)
        return

    # --- БЛОК: Запуск очистки для существующих 'raw_fetched' ---
    logger.info("Проверка наличия 'raw_fetched' статей для немедленной очистки...")
    try:
        # Используем get_articles_by_status, которая возвращает список ID
        raw_ids_to_clean = db_utils.get_articles_by_status('raw_fetched')

        if raw_ids_to_clean:
            logger.info(f"Найдено {len(raw_ids_to_clean)} статей со статусом 'raw_fetched'. Запуск cleaner...")
            payload = {"article_ids": raw_ids_to_clean}
            payload_str = json.dumps(payload)
            cleaning_content = Content(role="system", parts=[Part(text=payload_str)])
            await cleaner.handle_cleaning_request(cleaning_content)
            logger.info("Запуск cleaner для существующих статей завершен.")
        else:
            logger.info("Статьи со статусом 'raw_fetched' для немедленной очистки не найдены.")
    except AttributeError as e:
         logger.error(f"Пропущена проверка 'raw_fetched': функция get_articles_by_status не найдена в db_utils ({e}).")
    except Exception as e:
        logger.error(f"Ошибка при проверке/запуске очистки существующих статей: {e}", exc_info=True)
    # --- КОНЕЦ БЛОКА ---

    # --- НОВЫЙ БЛОК: Запуск резюмирования для существующих 'cleaned' ---
    logger.info("Проверка наличия 'cleaned' статей для немедленного резюмирования...")
    try:
        # Вызываем цикл SummarizerAgent один раз при старте, чтобы обработать
        # все статьи, которые могли быть очищены ранее, но не резюмированы.
        # Summarizer сам найдет статьи со статусом 'cleaned' внутри себя.
        await summarizer.run_summarize_cycle()
        logger.info("Запуск summarizer для существующих статей завершен.")
    except Exception as e:
        logger.error(f"Ошибка при запуске немедленного цикла резюмирования: {e}", exc_info=True)
    # --- КОНЕЦ БЛОКА ---

    # --- Логика периодического запуска агентов ---
    async def run_periodic_cycle():
        """Запускает циклы агентов: Fetcher, затем Summarizer."""

        # --- Основной цикл ожидания и запуска ---
        while True:
            # Сначала запускаем Fetcher
            logger.info("Запуск периодического цикла Fetcher...")
            try:
                await fetcher.run_fetch_cycle()
                logger.info("Периодический цикл Fetcher завершен.")
            except Exception as e:
                logger.error(f"Ошибка в периодическом цикле Fetcher: {e}", exc_info=True)
                await asyncio.sleep(60) # Пауза после ошибки Fetcher

            # Затем запускаем Summarizer
            logger.info("Запуск периодического цикла Summarizer...")
            try:
                await summarizer.run_summarize_cycle() # <-- Вызываем Summarizer
                logger.info("Периодический цикл Summarizer завершен.")
            except Exception as e:
                 logger.error(f"Ошибка в периодическом цикле Summarizer: {e}", exc_info=True)
                 await asyncio.sleep(60) # Пауза после ошибки Summarizer

            # Интервал ожидания перед СЛЕДУЮЩЕЙ парой Fetcher->Summarizer
            fetch_interval_seconds = int(config.get('fetch_interval', 3600))
            logger.info(f"Ожидание {fetch_interval_seconds} секунд до следующего цикла...")
            await asyncio.sleep(fetch_interval_seconds)


    logger.info("Запуск основного цикла приложения...")
    try:
        # Запускаем асинхронную функцию с основным циклом
        await run_periodic_cycle()
    except KeyboardInterrupt:
        logger.info("Получен сигнал завершения (KeyboardInterrupt).")
    except asyncio.CancelledError:
        logger.info("Основной цикл отменен.")
    finally:
        logger.info("Приложение остановлено.")

# --- Точка входа ---
if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        logger.info("Приложение завершено пользователем.")
    except Exception as e:
        logger.critical(f"Критическая необработанная ошибка в main: {e}", exc_info=True)