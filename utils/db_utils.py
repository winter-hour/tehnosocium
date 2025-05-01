# utils/db_utils.py
import sqlite3
import logging
from datetime import datetime

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# --- Глобальные переменные для пути к БД ---
# Лучше передавать путь при инициализации, но для простоты пока так
DB_PATH = "db/articles.db" # Значение по умолчанию, будет переопределено

def set_db_path(path: str):
    """Устанавливает путь к файлу БД."""
    global DB_PATH
    DB_PATH = path
    logging.info(f"Database path set to: {DB_PATH}")

def get_db_connection():
    """Устанавливает соединение с БД SQLite."""
    try:
        conn = sqlite3.connect(DB_PATH)
        conn.row_factory = sqlite3.Row # Возвращать строки как словари
        return conn
    except sqlite3.Error as e:
        logging.error(f"Error connecting to database {DB_PATH}: {e}")
        return None

def init_db():
    """Инициализирует БД, создает таблицу, если она не существует."""
    conn = get_db_connection()
    if not conn:
        return # Ошибка уже залогирована в get_db_connection

    try:
        cursor = conn.cursor()
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS articles (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                source_name TEXT,
                title TEXT,
                url TEXT UNIQUE NOT NULL,
                publication_date TEXT, -- Храним как строку ISO 8601 для простоты
                fetched_at TEXT DEFAULT CURRENT_TIMESTAMP,
                status TEXT NOT NULL DEFAULT 'new',
                raw_json_path TEXT,
                cleaned_md_path TEXT,
                error_message TEXT
            )
        """)
        # Можно добавить индексы для ускорения поиска
        cursor.execute("CREATE INDEX IF NOT EXISTS idx_url ON articles (url)")
        cursor.execute("CREATE INDEX IF NOT EXISTS idx_status ON articles (status)")
        conn.commit()
        logging.info("Database initialized successfully.")
    except sqlite3.Error as e:
        logging.error(f"Error initializing database table: {e}")
    finally:
        if conn:
            conn.close()

def check_article_exists(url: str) -> bool:
    """Проверяет, существует ли статья с таким URL в БД."""
    conn = get_db_connection()
    if not conn: return True # Если нет соединения, лучше пропустить, чем дублировать

    exists = False
    try:
        cursor = conn.cursor()
        cursor.execute("SELECT 1 FROM articles WHERE url = ?", (url,))
        exists = cursor.fetchone() is not None
    except sqlite3.Error as e:
        logging.error(f"Error checking if article exists (URL: {url}): {e}")
        exists = True # Осторожный подход: считаем, что существует, если ошибка
    finally:
        if conn:
            conn.close()
    return exists

def add_article(source_name: str, title: str, url: str, pub_date_str: str, raw_json_path: str) -> int | None:
    """Добавляет новую статью в БД со статусом 'raw_fetched'. Возвращает ID добавленной статьи или None при ошибке."""
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
        logging.info(f"Article added to DB: ID={article_id}, Title='{title}', URL={url}")
    except sqlite3.IntegrityError:
        # Это может случиться, если между check_article_exists и INSERT кто-то добавил статью
        logging.warning(f"Article likely already exists (IntegrityError): URL={url}")
    except sqlite3.Error as e:
        logging.error(f"Error adding article to DB (URL: {url}): {e}")
        conn.rollback() # Откатываем изменения при ошибке
    finally:
        if conn:
            conn.close()
    return article_id

def get_article_for_cleaning(article_id: int) -> dict | None:
    """Получает данные статьи, готовой к очистке."""
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
            article_data = dict(row) # Преобразуем sqlite3.Row в обычный dict
    except sqlite3.Error as e:
        logging.error(f"Error fetching article for cleaning (ID: {article_id}): {e}")
    finally:
        if conn:
            conn.close()
    return article_data

def update_article_status(article_id: int, status: str, file_path: str | None = None, error_msg: str | None = None):
    """Обновляет статус статьи, путь к файлу и сообщение об ошибке."""
    conn = get_db_connection()
    if not conn: return

    try:
        cursor = conn.cursor()
        if status == 'cleaned' and file_path:
            cursor.execute("""
                UPDATE articles
                SET status = ?, cleaned_md_path = ?, error_message = NULL
                WHERE id = ?
            """, (status, file_path, article_id))
            logging.info(f"Article ID={article_id} status updated to '{status}', path='{file_path}'")
        elif status == 'cleaning_failed' and error_msg:
            cursor.execute("""
                UPDATE articles
                SET status = ?, error_message = ?
                WHERE id = ?
            """, (status, error_msg, article_id))
            logging.warning(f"Article ID={article_id} status updated to '{status}', error='{error_msg}'")
        else: # Обновление только статуса (например, 'fetch_failed')
             cursor.execute("UPDATE articles SET status = ?, error_message = ? WHERE id = ?", (status, error_msg, article_id))
             logging.info(f"Article ID={article_id} status updated to '{status}'" + (f", error='{error_msg}'" if error_msg else ""))

        conn.commit()
    except sqlite3.Error as e:
        logging.error(f"Error updating article status (ID: {article_id}, Status: {status}): {e}")
        conn.rollback()
    finally:
        if conn:
            conn.close()


# Добавить в конец utils/db_utils.py

import sqlite3 # Убедитесь, что импорт есть в начале файла

def get_article_id_by_url(url: str) -> int | None:
    """Ищет ID статьи по её URL."""
    conn = get_db_connection()
    if not conn: return None
    article_id = None
    try:
        cursor = conn.cursor()
        cursor.execute("SELECT id FROM articles WHERE url = ?", (url,))
        result = cursor.fetchone()
        if result:
            article_id = result['id'] # Доступ по имени колонки, т.к. conn.row_factory = sqlite3.Row
    except sqlite3.Error as e:
        logging.error(f"Error fetching article ID by URL ({url}): {e}")
    finally:
        if conn:
            conn.close()
    return article_id

def get_articles_by_status(status: str) -> list[int]:
    """Возвращает список ID статей с указанным статусом."""
    conn = get_db_connection()
    ids = []
    if not conn: return ids
    try:
        cursor = conn.cursor()
        cursor.execute("SELECT id FROM articles WHERE status = ?", (status,))
        rows = cursor.fetchall()
        # Преобразуем результат в список ID
        ids = [row['id'] for row in rows] # Используем доступ по имени колонки
        logging.info(f"Found {len(ids)} articles with status '{status}'.")
    except sqlite3.Error as e:
        logging.error(f"Error fetching articles by status ({status}): {e}")
    finally:
        if conn:
            conn.close()
    return ids