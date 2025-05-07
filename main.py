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
from agents.selector_agent import SelectorAgent
from agents.generator_agent import GeneratorAgent
from agents.publisher_agent import PublisherAgent # <-- Импорт PublisherAgent

logging.basicConfig(level=logging.INFO,
                    format='%(asctime)s - %(name)s - %(levelname)s - [%(filename)s:%(lineno)d] - %(message)s', # Добавил имя файла и строку
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
            return os.getenv(var_name, f"%{var_name}%") # Возвращаем плейсхолдер, если переменной нет
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
        logger.info("Модели агентов успешно пересобраны.")
    except Exception as e:
        logger.error(f"Не удалось пересобрать модели агентов: {e}", exc_info=True); return

    # --- Создание экземпляров агентов ---
    logger.info("Создание экземпляров агентов...")
    try:
        # Передаем конфиг каждому агенту
        cleaner = CleanerAgent(agent_id="cleaner_agent", config=config)
        fetcher = FetcherAgent(agent_id="fetcher_agent", config=config, cleaner_agent_instance=cleaner)
        summarizer = SummarizerAgent(agent_id="summarizer_agent", config=config)
        selector = SelectorAgent(agent_id="selector_agent", config=config)
        generator = GeneratorAgent(agent_id="generator_agent", config=config)
        publisher = PublisherAgent(agent_id="publisher_agent", config=config) # <-- Создаем Publisher
        logger.info("Экземпляры агентов успешно созданы.")
    except Exception as e:
        logger.error(f"Не удалось создать экземпляры агентов: {e}", exc_info=True); return

    # --- Блок обработки "застрявших" статей при старте ---
    logger.info("--- Начало предварительной обработки ---")

    # Создаем список шагов предварительной обработки
    startup_pipeline = [
        (cleaner.handle_cleaning_request, db_utils.get_articles_by_status, 'raw_fetched', "очистки 'raw_fetched'"),
        (summarizer.run_summarize_cycle, lambda: True, None, "резюмирования 'cleaned'"), # Summarizer ищет сам
        (selector.run_selection_cycle, lambda: True, None, "выбора 'summarized'"),       # Selector ищет сам
        (generator.run_generation_cycle, lambda: True, None, "генерации 'selected'"),   # Generator ищет сам
        (publisher.run_publishing_cycle, lambda: True, None, "публикации 'post_generated'") # Publisher ищет сам
    ]

    for agent_method, get_ids_func, status_to_check, step_name in startup_pipeline:
        logger.info(f"Проверка наличия статей для немедленного этапа: {step_name}...")
        try:
            # Для первых шагов (очистка) мы передаем ID явно
            if status_to_check == 'raw_fetched':
                 ids_to_process = get_ids_func(status_to_check)
                 if ids_to_process:
                     logger.info(f"Найдено {len(ids_to_process)} статей со статусом '{status_to_check}'. Запуск агента...")
                     payload = {"article_ids": ids_to_process}
                     payload_str = json.dumps(payload)
                     content_arg = Content(role="system", parts=[Part(text=payload_str)])
                     await agent_method(content_arg) # Вызываем с аргументом Content
                 else:
                     logger.info(f"Статьи со статусом '{status_to_check}' не найдены.")
            # Для последующих шагов агент сам ищет нужные статьи внутри своего цикла
            elif get_ids_func(): # Просто проверяем, что функция существует (lambda всегда True)
                 await agent_method() # Вызываем без аргументов

            logger.info(f"Запуск агента для этапа '{step_name}' завершен.")

        except AttributeError as e:
             logger.error(f"Пропущена проверка '{step_name}': необходимая функция не найдена ({e}).")
        except Exception as e:
            logger.error(f"Ошибка при запуске немедленного цикла '{step_name}': {e}", exc_info=True)

    logger.info("--- Предварительная обработка завершена ---")

    # --- Логика периодического запуска агентов ---
    async def run_periodic_cycle():
        """Запускает циклы агентов: Fetcher -> Summarizer -> Selector -> Generator -> Publisher."""
        # Определяем конвейер агентов и их методов цикла
        pipeline = [
            (fetcher, fetcher.run_fetch_cycle, "Fetcher"),
            (summarizer, summarizer.run_summarize_cycle, "Summarizer"),
            (selector, selector.run_selection_cycle, "Selector"),
            (generator, generator.run_generation_cycle, "Generator"),
            (publisher, publisher.run_publishing_cycle, "Publisher") # <-- Добавлен Publisher
        ]

        while True:
            logger.info("--- Начало нового периодического цикла ---")

            # Последовательно выполняем каждый шаг конвейера
            for agent_instance, cycle_method, agent_name in pipeline:
                logger.info(f"Запуск периодического цикла {agent_name}...")
                try:
                    # Вызываем метод цикла для текущего агента
                    await cycle_method()
                    logger.info(f"Периодический цикл {agent_name} завершен.")
                    # Добавим короткую паузу между агентами для разгрузки
                    await asyncio.sleep(2)
                except Exception as e:
                    logger.error(f"Ошибка в цикле {agent_name}: {e}", exc_info=True)
                    logger.info(f"Пауза 60 секунд после ошибки агента {agent_name}...")
                    await asyncio.sleep(60)
                    # Решаем, прерывать ли весь цикл конвейера при ошибке одного агента
                    # Если раскомментировать break, то после ошибки Fetcher'а остальные не запустятся
                    # break

            # --- Ожидание перед следующим полным циклом ---
            try:
                # Пытаемся получить интервал из конфига, иначе используем 1 час
                cycle_interval_seconds = int(config.get('cycle_interval', 3600))
                if cycle_interval_seconds <= 0:
                    logger.warning("Интервал цикла 'cycle_interval' в конфиге <= 0. Используется 3600 секунд.")
                    cycle_interval_seconds = 3600
            except ValueError:
                logger.warning("Некорректное значение 'cycle_interval' в конфиге. Используется 3600 секунд.")
                cycle_interval_seconds = 3600

            logger.info(f"--- Полный цикл конвейера завершен. Ожидание {cycle_interval_seconds} секунд... ---")
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