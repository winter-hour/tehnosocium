# utils/md_utils.py

import yaml
import os
import logging
import re # Для поиска разделителей YAML
from typing import Optional, Tuple, Dict, Any

# Настраиваем логирование для этого модуля на русском
logging.basicConfig(level=logging.INFO,
                    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
                    datefmt='%Y-%m-%d %H:%M:%S') # Формат времени можно оставить стандартным
logger = logging.getLogger("MDUtils")

# Регулярное выражение для поиска YAML Front Matter (остается без изменений)
YAML_FRONT_MATTER_REGEX = re.compile(r'^\s*---\s*\n(.*?\n?)\s*---\s*\n?(.*)', re.DOTALL | re.MULTILINE)

def read_md_file(filepath: str) -> Tuple[Optional[Dict[str, Any]], Optional[str]]:
    """
    Читает MD-файл, разделяет YAML Front Matter и основной текст.

    Args:
        filepath: Путь к MD-файлу.

    Returns:
        Кортеж (yaml_data, text_content):
        - yaml_data: Словарь с данными из YAML-секции (или None, если секции нет или ошибка).
        - text_content: Строка с основным текстом после YAML-секции (или None, если ошибка).
                      Если YAML-секции нет, возвращается всё содержимое файла как text_content.
    """
    yaml_data: Optional[Dict[str, Any]] = None
    text_content: Optional[str] = None

    if not os.path.exists(filepath):
        logger.error(f"MD файл не найден: {filepath}")
        return None, None

    try:
        with open(filepath, 'r', encoding='utf-8') as f:
            content = f.read()

        match = YAML_FRONT_MATTER_REGEX.match(content)

        if match:
            yaml_string = match.group(1)
            text_content = match.group(2).strip()
            try:
                yaml_data = yaml.safe_load(yaml_string)
                if not isinstance(yaml_data, dict):
                    logger.warning(f"YAML секция в файле {filepath} не является словарем. Считаем как None.")
                    yaml_data = None
            except yaml.YAMLError as e:
                logger.error(f"Ошибка парсинга YAML секции в файле {filepath}: {e}")
                yaml_data = None # YAML есть, но некорректный
        else:
            logger.debug(f"YAML секция не найдена в файле {filepath}. Читаем всё содержимое как текст.")
            text_content = content.strip()
            yaml_data = {} # Файл прочитан, но YAML нет

        return yaml_data, text_content

    except IOError as e:
        logger.error(f"Ошибка чтения MD файла {filepath}: {e}")
        return None, None
    except Exception as e:
        logger.error(f"Неожиданная ошибка при обработке MD файла {filepath}: {e}", exc_info=True)
        return None, None


def update_md_yaml(filepath: str, data_to_update: Dict[str, Any]) -> bool:
    """
    Обновляет/добавляет данные в YAML-секцию существующего MD-файла,
    сохраняя основной текст неизменным. Если YAML-секции нет, она будет создана.

    Args:
        filepath: Путь к MD-файлу.
        data_to_update: Словарь с данными для добавления или обновления.

    Returns:
        True при успехе, False при ошибке.
    """
    if not data_to_update:
        logger.warning(f"Нет данных для обновления YAML в файле {filepath}.")
        return True # Считаем успехом, так как делать нечего

    yaml_data, text_content = read_md_file(filepath)

    if yaml_data is None and text_content is None:
        logger.error(f"Не удалось обновить YAML для файла {filepath}, ошибка чтения исходного файла.")
        return False

    if yaml_data is None:
         yaml_data = {} # Инициализируем, если YAML секции не было

    # Обновляем словарь новыми данными
    yaml_data.update(data_to_update)

    try:
        # Преобразуем обновленный словарь обратно в YAML строку
        updated_yaml_string = yaml.dump(
            yaml_data,
            allow_unicode=True,
            sort_keys=False,
            default_flow_style=False,
            indent=2
        )

        # Формируем новое содержимое файла
        separator = "\n\n" if text_content else "\n"
        new_content = f"---\n{updated_yaml_string.strip()}\n---\n{separator}{text_content or ''}"

        # Перезаписываем файл
        with open(filepath, 'w', encoding='utf-8') as f:
            f.write(new_content)

        logger.info(f"YAML секция успешно обновлена в файле {filepath}. Добавлены/обновлены ключи: {list(data_to_update.keys())}")
        return True

    except yaml.YAMLError as e:
        logger.error(f"Ошибка преобразования данных в YAML для файла {filepath}: {e}")
        return False
    except IOError as e:
        logger.error(f"Ошибка записи обновленного MD файла {filepath}: {e}")
        return False
    except Exception as e:
        logger.error(f"Неожиданная ошибка при обновлении MD файла {filepath}: {e}", exc_info=True)
        return False


def create_post_md_file(filepath: str, yaml_data: Dict[str, Any], post_text: str) -> bool:
    """
    Создает новый MD-файл для поста с YAML Front Matter и основным текстом.

    Args:
        filepath: Полный путь для создания нового файла поста.
        yaml_data: Словарь с метаданными для YAML-секции.
        post_text: Основной текст сгенерированного поста.

    Returns:
        True при успехе, False при ошибке.
    """
    try:
        # Убедимся, что директория существует (импортируем из file_utils)
        from .file_utils import ensure_dir_exists # Локальный импорт
        ensure_dir_exists(os.path.dirname(filepath))

        # Формируем YAML-секцию
        yaml_string = yaml.dump(
            yaml_data if yaml_data else {},
            allow_unicode=True,
            sort_keys=False,
            default_flow_style=False,
            indent=2
        )

        # Формируем полное содержимое файла
        content = f"---\n{yaml_string.strip()}\n---\n\n{post_text.strip()}"

        # Записываем файл
        with open(filepath, 'w', encoding='utf-8') as f:
            f.write(content)

        logger.info(f"MD файл поста успешно создан: {filepath}")
        return True

    except yaml.YAMLError as e:
        logger.error(f"Ошибка преобразования YAML данных для нового файла поста {filepath}: {e}")
        return False
    except IOError as e:
        logger.error(f"Ошибка записи нового MD файла поста {filepath}: {e}")
        return False
    except ImportError:
         logger.error("Не удалось импортировать ensure_dir_exists из file_utils. Убедитесь, что функция существует.")
         return False
    except Exception as e:
        logger.error(f"Неожиданная ошибка при создании MD файла поста {filepath}: {e}", exc_info=True)
        return False