import aiosqlite
import asyncio
import json
import logging
from typing import Dict, Any, Optional, List
import os
from datetime import datetime
import threading
import time
import random
from concurrent.futures import ThreadPoolExecutor
import weakref

logger = logging.getLogger(__name__)

DATABASE_FILE = 'bot_state.db'

# Глобальные настройки для предотвращения блокировок
DB_CONFIG = {
    'max_connections': 1,  # Только одно соединение
    'connection_timeout': 10.0,  # Сокращен таймаут
    'busy_timeout': 5000,  # Сокращен таймаут занятости (5 сек)
    'retry_attempts': 5,  # Увеличено количество попыток
    'retry_delay_base': 0.1,
    'max_retry_delay': 1.0,  # Сокращена максимальная задержка
    'lock_timeout': 5.0,  # Сокращен таймаут блокировки
}

# Глобальная блокировка для всей базы данных
_global_db_lock = asyncio.Lock()
_connection_cache = None
_last_connection_time = 0

class DatabaseError(Exception):
    """Исключение для ошибок базы данных"""
    pass

class DatabaseLockError(DatabaseError):
    """Исключение для ошибок блокировки базы данных"""
    pass

async def _exponential_backoff(attempt: int, base_delay: float = 0.1, max_delay: float = 1.0) -> float:
    """Экспоненциальная задержка с джиттером"""
    delay = min(base_delay * (2 ** attempt), max_delay)
    jitter = random.uniform(0, 0.1 * delay)
    return delay + jitter

async def _get_single_connection():
    """Получение единственного глобального соединения с базой данных"""
    global _connection_cache, _last_connection_time
    
    async with _global_db_lock:
        current_time = time.time()
        
        # Проверяем, нужно ли пересоздать соединение
        if (_connection_cache is None or 
            current_time - _last_connection_time > 300):  # Пересоздаем каждые 5 минут
            
            # Закрываем старое соединение если есть
            if _connection_cache:
                try:
                    await _connection_cache.close()
                except:
                    pass
                _connection_cache = None
            
            # Создаем новое соединение
            try:
                conn = await aiosqlite.connect(
                    DATABASE_FILE,
                    timeout=DB_CONFIG['connection_timeout'],
                    isolation_level=None  # autocommit режим
                )
                
                # Настраиваем соединение для предотвращения блокировок
                await conn.execute('PRAGMA journal_mode = WAL')
                await conn.execute('PRAGMA synchronous = NORMAL')
                await conn.execute('PRAGMA cache_size = 1000')
                await conn.execute('PRAGMA temp_store = memory')
                await conn.execute(f'PRAGMA busy_timeout = {DB_CONFIG["busy_timeout"]}')
                await conn.execute('PRAGMA foreign_keys = ON')
                await conn.execute('PRAGMA wal_autocheckpoint = 100')  # Автоматический checkpoint
                
                _connection_cache = conn
                _last_connection_time = current_time
                
                logger.debug("Создано новое соединение с базой данных")
                
            except Exception as e:
                logger.error(f"Ошибка создания соединения с БД: {e}")
                raise DatabaseError(f"Не удалось создать соединение: {e}")
        
        return _connection_cache

async def _execute_with_retry(operation_func, *args, **kwargs):
    """Выполнение операции с повторными попытками"""
    last_exception = None
    
    for attempt in range(DB_CONFIG['retry_attempts']):
        try:
            conn = await _get_single_connection()
            
            # Проверяем соединение
            try:
                await conn.execute('SELECT 1')
            except:
                # Соединение мертво, пересоздаем
                global _connection_cache
                _connection_cache = None
                conn = await _get_single_connection()
            
            return await operation_func(conn, *args, **kwargs)
            
        except aiosqlite.OperationalError as e:
            error_msg = str(e).lower()
            if 'database is locked' in error_msg or 'database is busy' in error_msg:
                last_exception = DatabaseLockError(f"База данных заблокирована: {e}")
                delay = await _exponential_backoff(attempt)
                logger.warning(f"База данных заблокирована (попытка {attempt + 1}), ожидание {delay:.2f}с")
                await asyncio.sleep(delay)
                continue
            else:
                raise DatabaseError(f"Ошибка базы данных: {e}")
                
        except Exception as e:
            last_exception = e
            if attempt < DB_CONFIG['retry_attempts'] - 1:
                delay = await _exponential_backoff(attempt)
                logger.warning(f"Ошибка БД (попытка {attempt + 1}): {e}, повтор через {delay:.2f}с")
                await asyncio.sleep(delay)
            continue
    
    logger.error(f"Все попытки исчерпаны для операции с БД")
    raise last_exception or DatabaseError("Неизвестная ошибка базы данных")

class BotDatabase:
    def __init__(self, db_file: str = DATABASE_FILE):
        self.db_file = db_file
        self._initialized = False
        self._init_lock = asyncio.Lock()

    async def _ensure_initialized(self):
        """Убеждаемся, что база данных инициализирована"""
        if not self._initialized:
            async with self._init_lock:
                if not self._initialized:
                    await self._init_database()
                    self._initialized = True

    async def _init_database(self):
        """Инициализация базы данных и таблиц"""
        async def init_operation(conn):
            # Создаем таблицы
            await conn.execute('''
                CREATE TABLE IF NOT EXISTS bot_state (
                    key TEXT PRIMARY KEY,
                    value TEXT,
                    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
            ''')
            
            await conn.execute('''
                CREATE TABLE IF NOT EXISTS user_sessions (
                    user_id INTEGER PRIMARY KEY,
                    session_data TEXT,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
            ''')
            
            await conn.execute('''
                CREATE TABLE IF NOT EXISTS statistics (
                    id INTEGER PRIMARY KEY,
                    comments_sent INTEGER DEFAULT 0,
                    channels_processed INTEGER DEFAULT 0,
                    reactions_set INTEGER DEFAULT 0,
                    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
            ''')
            
            await conn.execute('''
                CREATE TABLE IF NOT EXISTS processed_channels (
                    username TEXT PRIMARY KEY,
                    processed_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
            ''')
            
            await conn.execute('''
                CREATE TABLE IF NOT EXISTS channel_statistics (
                    channel_username TEXT PRIMARY KEY,
                    comments_count INTEGER DEFAULT 0,
                    reactions_count INTEGER DEFAULT 0,
                    last_processed TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
            ''')
            
            await conn.execute('''
                CREATE TABLE IF NOT EXISTS comment_links (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    channel_username TEXT,
                    comment_link TEXT,
                    post_link TEXT,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    FOREIGN KEY (channel_username) REFERENCES channel_statistics (channel_username)
                )
            ''')
            
            # Создаем индексы для производительности
            await conn.execute('CREATE INDEX IF NOT EXISTS idx_bot_state_key ON bot_state(key)')
            await conn.execute('CREATE INDEX IF NOT EXISTS idx_user_sessions_user_id ON user_sessions(user_id)')
            await conn.execute('CREATE INDEX IF NOT EXISTS idx_channel_stats_username ON channel_statistics(channel_username)')
            await conn.execute('CREATE INDEX IF NOT EXISTS idx_comment_links_channel ON comment_links(channel_username)')
            
            # Принудительный commit для WAL режима
            await conn.execute('PRAGMA wal_checkpoint(TRUNCATE)')
        
        await _execute_with_retry(init_operation)
        logger.info("База данных инициализирована")

    async def save_bot_state_batch(self, key: str, value: Any) -> None:
        """Упрощенное сохранение состояния бота (используем обычный метод)"""
        return await self.save_bot_state(key, value)

    async def save_bot_state(self, key: str, value: Any) -> None:
        """Сохранение состояния бота с защитой от блокировок"""
        await self._ensure_initialized()
        
        async def save_operation(conn):
            json_value = json.dumps(value, ensure_ascii=False, default=str)
            await conn.execute('''
                INSERT OR REPLACE INTO bot_state (key, value, updated_at)
                VALUES (?, ?, CURRENT_TIMESTAMP)
            ''', (key, json_value))
            # Принудительный checkpoint для немедленной записи
            await conn.execute('PRAGMA wal_checkpoint(PASSIVE)')
        
        await _execute_with_retry(save_operation)

    async def load_bot_state(self, key: str, default: Any = None) -> Any:
        """Загрузка состояния бота с защитой от блокировок"""
        await self._ensure_initialized()
        
        async def load_operation(conn):
            async with conn.execute('SELECT value FROM bot_state WHERE key = ?', (key,)) as cursor:
                row = await cursor.fetchone()
                if row:
                    return json.loads(row[0])
                return default
        
        return await _execute_with_retry(load_operation)

    async def save_statistics_batch(self, stats: Dict) -> None:
        """Упрощенное сохранение статистики (используем обычный метод)"""  
        return await self.save_statistics(stats)

    async def save_statistics(self, stats: Dict) -> None:
        """Сохранение статистики с защитой от блокировок"""
        await self._ensure_initialized()
        
        async def save_stats_operation(conn):
            await conn.execute('''
                INSERT OR REPLACE INTO statistics (id, comments_sent, channels_processed, reactions_set, updated_at)
                VALUES (1, ?, ?, ?, CURRENT_TIMESTAMP)
            ''', (stats.get('comments_sent', 0), stats.get('channels_processed', 0), stats.get('reactions_set', 0)))
            await conn.execute('PRAGMA wal_checkpoint(PASSIVE)')
        
        await _execute_with_retry(save_stats_operation)

    async def load_statistics(self) -> Dict:
        """Загрузка статистики с защитой от блокировок"""
        await self._ensure_initialized()
        
        async def load_stats_operation(conn):
            async with conn.execute('SELECT comments_sent, channels_processed, reactions_set FROM statistics WHERE id = 1') as cursor:
                row = await cursor.fetchone()
                if row:
                    return {
                        'comments_sent': row[0],
                        'channels_processed': row[1],
                        'reactions_set': row[2]
                    }
                return {'comments_sent': 0, 'channels_processed': 0, 'reactions_set': 0}
        
        return await _execute_with_retry(load_stats_operation)

    async def save_user_session(self, user_id: int, session_data: Dict) -> None:
        """Сохранение данных сессии пользователя"""
        await self._ensure_initialized()
        
        async def save_session_operation(conn):
            json_data = json.dumps(session_data, ensure_ascii=False, default=str)
            await conn.execute('''
                INSERT OR REPLACE INTO user_sessions (user_id, session_data, updated_at)
                VALUES (?, ?, CURRENT_TIMESTAMP)
            ''', (user_id, json_data))
            await conn.execute('PRAGMA wal_checkpoint(PASSIVE)')
        
        await _execute_with_retry(save_session_operation)

    async def load_user_session(self, user_id: int) -> Dict:
        """Загрузка данных сессии пользователя"""
        await self._ensure_initialized()
        
        async def load_session_operation(conn):
            async with conn.execute('SELECT session_data FROM user_sessions WHERE user_id = ?', (user_id,)) as cursor:
                row = await cursor.fetchone()
                if row:
                    return json.loads(row[0])
                return {}
        
        return await _execute_with_retry(load_session_operation)

    async def add_processed_channel(self, username: str) -> None:
        """Добавление обработанного канала"""
        await self._ensure_initialized()
        
        async def add_channel_operation(conn):
            await conn.execute('''
                INSERT OR REPLACE INTO processed_channels (username, processed_at)
                VALUES (?, CURRENT_TIMESTAMP)
            ''', (username,))
            await conn.execute('PRAGMA wal_checkpoint(PASSIVE)')
        
        await _execute_with_retry(add_channel_operation)

    async def get_processed_channels(self) -> set:
        """Получение списка обработанных каналов"""
        await self._ensure_initialized()
        
        async def get_channels_operation(conn):
            async with conn.execute('SELECT username FROM processed_channels') as cursor:
                rows = await cursor.fetchall()
                return {row[0] for row in rows}
        
        return await _execute_with_retry(get_channels_operation)

    async def clear_old_processed_channels(self, days: int = 30) -> None:
        """Очистка старых обработанных каналов"""
        await self._ensure_initialized()
        
        async def clear_channels_operation(conn):
            await conn.execute('''
                DELETE FROM processed_channels 
                WHERE processed_at < datetime('now', '-{} days')
            '''.format(days))
            await conn.execute('PRAGMA wal_checkpoint(PASSIVE)')
        
        await _execute_with_retry(clear_channels_operation)

    async def add_channel_comment(self, channel_username: str, comment_link: str = None, post_link: str = None) -> None:
        """Добавление комментария к каналу"""
        await self._ensure_initialized()
        
        async def add_comment_operation(conn):
            # Обновляем или создаем запись канала
            await conn.execute('''
                INSERT OR IGNORE INTO channel_statistics (channel_username, comments_count, reactions_count)
                VALUES (?, 0, 0)
            ''', (channel_username,))
            
            # Увеличиваем счетчик комментариев
            await conn.execute('''
                UPDATE channel_statistics 
                SET comments_count = comments_count + 1, last_processed = CURRENT_TIMESTAMP
                WHERE channel_username = ?
            ''', (channel_username,))
            
            # Добавляем ссылку на комментарий если есть
            if comment_link:
                await conn.execute('''
                    INSERT INTO comment_links (channel_username, comment_link, post_link)
                    VALUES (?, ?, ?)
                ''', (channel_username, comment_link, post_link))
            
            await conn.execute('PRAGMA wal_checkpoint(PASSIVE)')
        
        await _execute_with_retry(add_comment_operation)

    async def add_channel_reaction(self, channel_username: str) -> None:
        """Добавление реакции к каналу"""
        await self._ensure_initialized()
        
        async def add_reaction_operation(conn):
            # Обновляем или создаем запись канала
            await conn.execute('''
                INSERT OR IGNORE INTO channel_statistics (channel_username, comments_count, reactions_count)
                VALUES (?, 0, 0)
            ''', (channel_username,))
            
            # Увеличиваем счетчик реакций
            await conn.execute('''
                UPDATE channel_statistics 
                SET reactions_count = reactions_count + 1, last_processed = CURRENT_TIMESTAMP
                WHERE channel_username = ?
            ''', (channel_username,))
            
            await conn.execute('PRAGMA wal_checkpoint(PASSIVE)')
        
        await _execute_with_retry(add_reaction_operation)

    async def get_detailed_channel_statistics(self) -> Dict:
        """Получение подробной статистики по каналам"""
        await self._ensure_initialized()
        
        async def get_detailed_stats_operation(conn):
            # Получаем статистику каналов
            async with conn.execute('''
                SELECT channel_username, comments_count, reactions_count, last_processed
                FROM channel_statistics
                ORDER BY last_processed DESC
            ''') as cursor:
                channels_data = await cursor.fetchall()
            
            result = {}
            for row in channels_data:
                channel_username, comments_count, reactions_count, last_processed = row
                
                # Получаем ссылки на комментарии для этого канала
                async with conn.execute('''
                    SELECT comment_link, post_link FROM comment_links
                    WHERE channel_username = ?
                    ORDER BY created_at DESC
                ''', (channel_username,)) as link_cursor:
                    links_data = await link_cursor.fetchall()
                
                comment_links = [link[0] for link in links_data if link[0]]
                post_links = list(set([link[1] for link in links_data if link[1]]))
                
                result[channel_username] = {
                    'comments': comments_count,
                    'reactions': reactions_count,
                    'comment_links': comment_links,
                    'post_links': post_links,
                    'last_processed': last_processed
                }
            
            return result
        
        return await _execute_with_retry(get_detailed_stats_operation)

# Singleton database instance
db = BotDatabase()

# Глобальные функции для совместимости
async def init_database():
    """Инициализация базы данных"""
    await db._ensure_initialized()
    logger.info("База данных инициализирована")

async def close_database():
    """Закрытие базы данных и очистка ресурсов"""
    global _connection_cache, _global_db_lock
    
    async with _global_db_lock:
        if _connection_cache:
            try:
                # Принудительный checkpoint перед закрытием
                await _connection_cache.execute('PRAGMA wal_checkpoint(TRUNCATE)')
                await _connection_cache.close()
                logger.info("Соединение с базой данных закрыто")
            except Exception as e:
                logger.error(f"Ошибка при закрытии БД: {e}")
            finally:
                _connection_cache = None

# Функции для пакетных операций (рекомендуемые для использования)
async def save_bot_state_batch(key: str, value: Any) -> None:
    """Пакетное сохранение состояния бота"""
    return await db.save_bot_state_batch(key, value)

async def save_statistics_batch(stats: Dict) -> None:
    """Пакетное сохранение статистики"""
    return await db.save_statistics_batch(stats)