# utils/file_utils.py
import json
import os
import logging
import re
from slugify import slugify # Используем python-slugify для генерации имен
from datetime import datetime

def ensure_dir_exists(dir_path: str):
    """Убеждается, что директория существует, создавая ее при необходимости."""
    try:
        os.makedirs(dir_path, exist_ok=True)
    except OSError as e:
        logging.error(f"Error creating directory {dir_path}: {e}")
        raise # Передаем исключение дальше, так как без папки не можем работать

def generate_filename(base_path: str, source_name: str, title: str, extension: str) -> str:
    """Генерирует безопасное имя файла на основе даты, источника и заголовка."""
    now = datetime.now().strftime("%Y%m%d%H%M%S")
    safe_title = slugify(title, max_length=50, word_boundary=True) # Ограничиваем длину slug'а
    safe_source = slugify(source_name, max_length=20)
    filename = f"{now}_{safe_source}_{safe_title}.{extension}"
    return os.path.join(base_path, filename)

def save_json(data: dict, filepath: str):
    """Сохраняет данные в JSON-файл."""
    try:
        ensure_dir_exists(os.path.dirname(filepath))
        with open(filepath, 'w', encoding='utf-8') as f:
            json.dump(data, f, ensure_ascii=False, indent=4)
        logging.info(f"Successfully saved JSON to {filepath}")
    except (IOError, TypeError) as e:
        logging.error(f"Error saving JSON to {filepath}: {e}")
        # Возможно, стоит пробросить ошибку выше, чтобы отметить статью как 'fetch_failed'

def load_json(filepath: str) -> dict | None:
    """Загружает данные из JSON-файла."""
    if not os.path.exists(filepath):
        logging.error(f"JSON file not found: {filepath}")
        return None
    try:
        with open(filepath, 'r', encoding='utf-8') as f:
            data = json.load(f)
        logging.info(f"Successfully loaded JSON from {filepath}")
        return data
    except (IOError, json.JSONDecodeError) as e:
        logging.error(f"Error loading JSON from {filepath}: {e}")
        return None

def save_md(content: str, filepath: str):
    """Сохраняет текст в .md файл."""
    try:
        ensure_dir_exists(os.path.dirname(filepath))
        with open(filepath, 'w', encoding='utf-8') as f:
            f.write(content)
        logging.info(f"Successfully saved Markdown to {filepath}")
    except IOError as e:
        logging.error(f"Error saving Markdown to {filepath}: {e}")
        # Возможно, стоит пробросить ошибку