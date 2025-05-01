# utils/md_utils.py

import yaml
import os
import logging
import re # Для поиска разделителей YAML
from typing import Optional, Tuple, Dict, Any

# Настраиваем логирование для этого модуля
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger("MDUtils")

# Регулярное выражение для поиска YAML Front Matter
# Оно ищет блок, начинающийся с '---' в начале строки,
# затем любой текст (нежадно), и заканчивающийся '---' в начале строки.
# re.DOTALL позволяет '.' соответствовать и символу новой строки.
# re.MULTILINE позволяет '^' и '$' соответствовать началу/концу строки, а не всего текста.
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
        logger.error(f"MD file not found: {filepath}")
        return None, None

    try:
        with open(filepath, 'r', encoding='utf-8') as f:
            content = f.read()

        match = YAML_FRONT_MATTER_REGEX.match(content)

        if match:
            yaml_string = match.group(1)
            text_content = match.group(2).strip() # Удаляем лишние пробелы/переносы в начале/конце текста
            try:
                yaml_data = yaml.safe_load(yaml_string)
                if not isinstance(yaml_data, dict):
                    # Если YAML не является словарем, считаем его некорректным для наших целей
                    logger.warning(f"YAML front matter in {filepath} is not a dictionary. Treating as None.")
                    yaml_data = None
            except yaml.YAMLError as e:
                logger.error(f"Error parsing YAML front matter in {filepath}: {e}")
                # YAML есть, но с ошибкой. Текст все равно вернем.
                yaml_data = None # Считаем YAML невалидным
        else:
            # YAML секция не найдена, считаем все содержимое текстом
            logger.debug(f"No YAML front matter found in {filepath}. Reading entire content as text.")
            text_content = content.strip()
            yaml_data = {} # Возвращаем пустой словарь, чтобы показать, что файл прочитан, но YAML нет

        return yaml_data, text_content

    except IOError as e:
        logger.error(f"Error reading MD file {filepath}: {e}")
        return None, None
    except Exception as e: # Ловим другие непредвиденные ошибки
        logger.error(f"Unexpected error processing MD file {filepath}: {e}", exc_info=True)
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
    if not data_to_update: # Нечего обновлять
        logger.warning(f"No data provided to update YAML in {filepath}.")
        return True # Считаем успехом, так как делать нечего

    yaml_data, text_content = read_md_file(filepath)

    if yaml_data is None and text_content is None:
        # Ошибка чтения исходного файла
        logger.error(f"Cannot update YAML for {filepath}, failed to read original file.")
        return False

    # Если YAML не был найден, read_md_file вернет пустой словарь
    if yaml_data is None:
         yaml_data = {} # Инициализируем, если его не было

    # Обновляем словарь новыми данными
    yaml_data.update(data_to_update)

    try:
        # Преобразуем обновленный словарь обратно в YAML строку
        # allow_unicode=True сохраняет не-ASCII символы
        # sort_keys=False сохраняет порядок ключей (может быть полезно для читаемости)
        # default_flow_style=False предпочитает блочный стиль YAML
        updated_yaml_string = yaml.dump(
            yaml_data,
            allow_unicode=True,
            sort_keys=False,
            default_flow_style=False,
            indent=2 # Добавим отступ для читаемости
        )

        # Формируем новое содержимое файла
        # Добавляем пустую строку после YAML, если есть текст
        separator = "\n\n" if text_content else "\n"
        new_content = f"---\n{updated_yaml_string.strip()}\n---\n{separator}{text_content or ''}"

        # Перезаписываем файл
        with open(filepath, 'w', encoding='utf-8') as f:
            f.write(new_content)

        logger.info(f"Successfully updated YAML in {filepath} with keys: {list(data_to_update.keys())}")
        return True

    except yaml.YAMLError as e:
        logger.error(f"Error converting data to YAML for {filepath}: {e}")
        return False
    except IOError as e:
        logger.error(f"Error writing updated MD file {filepath}: {e}")
        return False
    except Exception as e:
        logger.error(f"Unexpected error updating MD file {filepath}: {e}", exc_info=True)
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
        from .file_utils import ensure_dir_exists # Локальный импорт для избежания цикличности
        ensure_dir_exists(os.path.dirname(filepath))

        # Формируем YAML-секцию
        yaml_string = yaml.dump(
            yaml_data if yaml_data else {}, # Обработка случая пустого yaml_data
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

        logger.info(f"Successfully created post MD file: {filepath}")
        return True

    except yaml.YAMLError as e:
        logger.error(f"Error converting YAML data for new post file {filepath}: {e}")
        return False
    except IOError as e:
        logger.error(f"Error writing new post MD file {filepath}: {e}")
        return False
    except ImportError: # Если file_utils не найден
         logger.error("Could not import ensure_dir_exists from file_utils. Make sure it exists.")
         return False
    except Exception as e:
        logger.error(f"Unexpected error creating post MD file {filepath}: {e}", exc_info=True)
        return False