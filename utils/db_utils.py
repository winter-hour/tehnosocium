# utils/db_utils.py
import sqlite3
import logging # Убедись, что logging импортирован
from datetime import datetime
from typing import Optional, List, Dict, Any # Добавим импорты типов

# --- Настраиваем базовую конфигурацию логирования ---
# Это настроит корневой логгер, полезно для библиотек
logging.basicConfig(level=logging.INFO,
                    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
                    datefmt='%Y-%m-%d %H:%M:%S')

# --- Определяем ИМЕНОВАННЫЙ логгер для этого модуля ---
# Используем именно его внутри функций этого файла
logger = logging.getLogger("DBUtils") # <--- Создаем логгер

# --- Глобальная переменная для пути к БД ---
DB_PATH = "db/articles.db" # Значение по умолчанию

def set_db_path(path: str):
    """Устанавливает путь к файлу БД."""
    global DB_PATH
    DB_PATH = path
    logger.info(f"Путь к базе данных установлен: {DB_PATH}") # Используем logger

def get_db_connection():
    """Устанавливает соединение с БД SQLite."""
    try:
        conn = sqlite3.connect(DB_PATH)
        conn.row_factory = sqlite3.Row
        return conn
    except sqlite3.Error as e:
        logger.error(f"Ошибка подключения к базе данных {DB_PATH}: {e}") # Используем logger
        return None

def init_db():
    """Инициализирует БД, создает таблицу, если она не существует."""
    conn = get_db_connection()
    if not conn:
        return

    try:
        cursor = conn.cursor()
        # Добавляем post_md_path, если еще не добавили
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS articles (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                source_name TEXT,
                title TEXT,
                url TEXT UNIQUE NOT NULL,
                publication_date TEXT,
                fetched_at TEXT DEFAULT CURRENT_TIMESTAMP,
                status TEXT NOT NULL DEFAULT 'new',
                raw_json_path TEXT,
                cleaned_md_path TEXT,
                post_md_path TEXT, -- Добавлена колонка для пути к файлу поста
                error_message TEXT
            )
        """)
        # Попытка добавить колонку, если таблица уже существует (игнорирует ошибку, если колонка есть)
        try:
            cursor.execute("ALTER TABLE articles ADD COLUMN post_md_path TEXT")
            logger.info("Колонка 'post_md_path' успешно добавлена в таблицу 'articles'.")
        except sqlite3.OperationalError as e:
            if "duplicate column name" in str(e):
                pass # Колонка уже существует, это нормально
            else:
                raise e # Другая ошибка

        cursor.execute("CREATE INDEX IF NOT EXISTS idx_url ON articles (url)")
        cursor.execute("CREATE INDEX IF NOT EXISTS idx_status ON articles (status)")
        conn.commit()
        logger.info("База данных успешно инициализирована.") # Используем logger
    except sqlite3.Error as e:
        logger.error(f"Ошибка инициализации таблицы базы данных: {e}") # Используем logger
    finally:
        if conn:
            conn.close()

# --- Функции check_article_exists, add_article, get_article_for_cleaning, ---
# --- update_article_status, get_article_id_by_url ИСПОЛЬЗУЮТ logger внутри себя ---
# (Убедись, что заменил logging.info/error/warning на logger.info/error/warning во всех них)

def check_article_exists(url: str) -> bool:
    conn = get_db_connection()
    if not conn: return True
    exists = False
    try:
        cursor = conn.cursor()
        cursor.execute("SELECT 1 FROM articles WHERE url = ?", (url,))
        exists = cursor.fetchone() is not None
    except sqlite3.Error as e:
        logger.error(f"Ошибка проверки существования статьи (URL: {url}): {e}") # logger
        exists = True
    finally:
        if conn:
            conn.close()
    return exists

def add_article(source_name: str, title: str, url: str, pub_date_str: str, raw_json_path: Optional[str]) -> Optional[int]:
    conn = get_db_connection()
    if not conn: return None
    article_id = None
    try:
        cursor = conn.cursor()
        cursor.execute("""
            INSERT INTO articles (source_name, title, url, publication_date, raw_json_path, status, fetched_at)
            VALUES (?, ?, ?, ?, ?, ?, ?)
        """, (source_name, title, url, pub_date_str, raw_json_path, 'raw_fetched', datetime.now().isoformat()))
        conn.commit()
        article_id = cursor.lastrowid
        logger.info(f"Статья добавлена в БД: ID={article_id}, Заголовок='{title}', URL={url}") # logger
    except sqlite3.IntegrityError:
        logger.warning(f"Статья уже существует (IntegrityError): URL={url}") # logger
    except sqlite3.Error as e:
        logger.error(f"Ошибка добавления статьи в БД (URL: {url}): {e}") # logger
        conn.rollback()
    finally:
        if conn:
            conn.close()
    return article_id

def get_article_for_cleaning(article_id: int) -> Optional[Dict[str, Any]]:
    conn = get_db_connection()
    if not conn: return None
    article_data = None
    try:
        cursor = conn.cursor()
        cursor.execute("""
            SELECT id, raw_json_path, title, publication_date, url, source_name
            FROM articles
            WHERE id = ? AND status = 'raw_fetched'
        """, (article_id,))
        row = cursor.fetchone()
        if row:
            article_data = dict(row)
    except sqlite3.Error as e:
        logger.error(f"Ошибка получения статьи для очистки (ID: {article_id}): {e}") # logger
    finally:
        if conn:
            conn.close()
    return article_data

def update_article_status(article_id: int, status: str, file_path: Optional[str] = None, error_msg: Optional[str] = None):
    conn = get_db_connection()
    if not conn: return

    sql = ""
    params = []

    try:
        cursor = conn.cursor()
        # Определяем, какой запрос выполнить в зависимости от статуса и наличия путей
        if status == 'cleaned' and file_path:
            sql = "UPDATE articles SET status = ?, cleaned_md_path = ?, error_message = NULL WHERE id = ?"
            params = (status, file_path, article_id)
            log_msg = f"Статья ID={article_id}: статус обновлен на '{status}', путь к MD: '{file_path}'"
        elif status == 'post_generated' and file_path: # Добавляем обработку для post_generated
             sql = "UPDATE articles SET status = ?, post_md_path = ?, error_message = NULL WHERE id = ?"
             params = (status, file_path, article_id)
             log_msg = f"Статья ID={article_id}: статус обновлен на '{status}', путь к посту: '{file_path}'"
        elif status.endswith('_failed') and error_msg:
            sql = "UPDATE articles SET status = ?, error_message = ? WHERE id = ?"
            params = (status, error_msg, article_id)
            log_msg = f"Статья ID={article_id}: статус обновлен на '{status}', ошибка: '{error_msg}'"
        else: # Обновление только статуса (например, summarized, selected, published)
            sql = "UPDATE articles SET status = ?, error_message = NULL WHERE id = ?" # Очищаем ошибку при успешном статусе
            params = (status, article_id)
            log_msg = f"Статья ID={article_id}: статус обновлен на '{status}'"

        if sql: # Выполняем, только если запрос сформирован
            cursor.execute(sql, params)
            conn.commit()
            if "_failed" in status:
                 logger.warning(log_msg) # Используем logger
            else:
                 logger.info(log_msg) # Используем logger
        else:
             logger.warning(f"Не удалось сформировать SQL для обновления статуса статьи ID={article_id} на '{status}'") # logger

    except sqlite3.Error as e:
        logger.error(f"Ошибка обновления статуса статьи (ID: {article_id}, Статус: {status}): {e}") # logger
        conn.rollback()
    finally:
        if conn:
            conn.close()


def get_article_id_by_url(url: str) -> Optional[int]:
    conn = get_db_connection()
    if not conn: return None
    article_id = None
    try:
        cursor = conn.cursor()
        cursor.execute("SELECT id FROM articles WHERE url = ?", (url,))
        result = cursor.fetchone()
        if result:
            article_id = result['id']
    except sqlite3.Error as e:
        logger.error(f"Ошибка получения ID статьи по URL ({url}): {e}") # logger
    finally:
        if conn:
            conn.close()
    return article_id

def get_articles_by_status(status: str) -> List[int]:
    """Возвращает список ID статей с указанным статусом."""
    conn = get_db_connection()
    ids = []
    if not conn: return ids
    try:
        cursor = conn.cursor()
        cursor.execute("SELECT id FROM articles WHERE status = ?", (status,))
        rows = cursor.fetchall()
        ids = [row['id'] for row in rows]
        logger.info(f"Найдено {len(ids)} статей со статусом '{status}'.") # logger
    except sqlite3.Error as e:
        logger.error(f"Ошибка получения статей по статусу ({status}): {e}") # logger
    finally:
        if conn:
            conn.close()
    return ids

# --- НОВАЯ ФУНКЦИЯ ДЛЯ SummarizerAgent ---
def get_articles_for_summarizing(limit: Optional[int] = None) -> List[Dict[str, Any]]:
    """
    Возвращает список статей со статусом 'cleaned' для генерации резюме.

    Args:
        limit: Опциональное ограничение на количество возвращаемых статей.

    Returns:
        Список словарей, каждый из которых содержит 'id' и 'cleaned_md_path'.
    """
    conn = get_db_connection()
    articles = []
    if not conn:
        logger.error("Не удалось получить статьи для резюмирования: нет соединения с БД.") # logger
        return articles
    try:
        cursor = conn.cursor()
        sql = "SELECT id, cleaned_md_path FROM articles WHERE status = 'cleaned'"
        params = []
        if limit:
            sql += " LIMIT ?"
            params.append(limit)

        cursor.execute(sql, params)
        rows = cursor.fetchall()
        articles = [dict(row) for row in rows]
        count = len(articles)
        limit_info = f" (лимит: {limit})" if limit else ""
        logger.info(f"Найдено {count} статей со статусом 'cleaned' для резюмирования{limit_info}.") # logger

    except sqlite3.Error as e:
        logger.error(f"Ошибка получения статей для резюмирования из БД: {e}") # logger
    finally:
        if conn:
            conn.close()
    return articles