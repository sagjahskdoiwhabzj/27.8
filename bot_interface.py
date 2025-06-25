import logging
import json
import os
import re
import asyncio
from datetime import datetime
from typing import Dict, List, Optional
import base64
import signal
import sys
import atexit
import threading
from concurrent.futures import ThreadPoolExecutor

# Настройка логирования
log_filename = 'run_log.log'

# Проверяем, нужно ли создавать новый файл
file_mode = 'w' if not os.path.exists(log_filename) else 'a'

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler(log_filename, mode=file_mode, encoding='utf-8'),
        logging.StreamHandler()
    ]
)
 
logger = logging.getLogger(__name__)

try:
    import nest_asyncio
    # Проверяем среду выполнения  
    try:
        current_loop = asyncio.get_running_loop()
        import sys
        if any(name in sys.modules for name in ['IPython', 'google.colab']):
            nest_asyncio.apply()
            logger.info("nest_asyncio применен для Jupyter/Colab среды")
        else:
            logger.info("Event loop обнаружен, но среда не требует nest_asyncio")
    except RuntimeError:
        logger.info("Запуск в стандартной среде без активного event loop")
        pass
except ImportError:
    logger.info("nest_asyncio не установлен, продолжаем без него")
    pass

# Настройка уровней логирования для внешних библиотек
logging.getLogger('telegram').setLevel(logging.WARNING)
logging.getLogger('telethon').setLevel(logging.WARNING)
logging.getLogger('urllib3').setLevel(logging.WARNING)
logging.getLogger('httpx').setLevel(logging.WARNING)

try:
    from telegram import Update, InlineKeyboardButton, InlineKeyboardMarkup, KeyboardButton, ReplyKeyboardMarkup
    from telegram.ext import Application, CommandHandler, CallbackQueryHandler, ContextTypes, MessageHandler, filters
    from telegram.constants import ParseMode
    from telethon import TelegramClient, events
    from telethon.errors import SessionPasswordNeededError, PhoneCodeInvalidError, PasswordHashInvalidError
    from telethon.tl.functions.messages import GetDiscussionMessageRequest
    from telethon.tl.functions.messages import GetRepliesRequest
    import g4f
    import aiosqlite
except ImportError as e:
    logger.error(f"Ошибка импорта библиотек: {e}")
    raise

user_sessions_cache = {}

# Импорт модуля базы данных
from database import db, init_database, close_database

# Thread pool для долгих операций
executor = ThreadPoolExecutor(max_workers=4, thread_name_prefix="bot_worker")

# Дефолтные промты
DEFAULT_COMMENT_PROMPT = """Создай короткий, естественный комментарий к посту на русском языке. 

Текст поста: {text_of_the_post}

Тематика канала: {topics}

Другие комментарии под постом: {comments}

Требования к комментарию:
- Максимум 2-3 предложения
- Естественный стиль общения
- Положительная или нейтральная тональность
- Без спама и навязчивости
- Соответствует тематике поста
- Выглядит как реальный отзыв пользователя
- Без эмодзи
- Без ссылок
- Без рекламы

Пример комментариев:
- "Интересная мысль, согласен с автором"
- "Полезная информация, спасибо за пост"
- "Актуальная тема, хорошо раскрыта"
- "Действительно важный вопрос"
- "Качественный материал"

Создай комментарий:"""

DEFAULT_ANALYSIS_PROMPT = """Данные канала:

{full_text}

———————————————————————

Ты — профессиональный аналитик Telegram-каналов. Проанализируй название, описание и посты канала и в результате [...]

📌 1. Сгенерируй **ТОЧНЫЕ** ключевые слова, которые могли бы встречаться в **названиях других каналов точно по этой теме**.

- Ключевые слова должны **на 100% отражать основную тему канала**.
- Тема считается основной, если она присутствует в **описании** или явно **доминирует в 90%+ постов по смыслу** (не [...]
- **Запрещено использовать любые слова**, которые:
  - связаны с темой **косвенно**;
  - абстрактны ("ощущение", "эмоции", "реализация", "настроение", "стиль", "идея" и подобные);
  - упоминаются в канале **случайно, единично или как пример**.
- **Разрешено использовать только те слова**, которые:
  - короткие и точные;
  - могли бы быть в названии другого Telegram-канала с точно такой же темой;
  - прямо и явно обозначают основную тематику.

📌 2. Определи основную тему или темы канала, строго выбрав их из следующего списка:

{topics}

- Выбирай только те темы, которые **на 100% соответствуют смыслу канала**.
- **Запрещено выбирать темы, если они связаны только частично или косвенно.**
- Если ни одна тема не подходит точно — укажи "Другое".
- **Запрещено придумывать темы вне списка.**
- **Категорически запрещено выбирать темы которых нет в списке**

🎯 Главные правила:
- Описание канала — **главный ориентир**. Посты нужны только как подтверждение.
- Игнорируй слова и темы, встречающиеся **в одном или нескольких постах**, если они **не повторяются стабильно по всему каналу**.
- Не используй обобщения, эмоции, художественные слова, метафоры, стили и прочий мусор — **только суть**.

📤 Формат ответа:

ТЕМЫ: укажи только темы из списка. Если тем несколько, то пиши каждую через запятую.
КЛЮЧЕВЫЕ_СЛОВА: только короткие, точные, релевантные слова, для названия канала по этой теме. Каждое слово через запятую.

Отвечай строго в заданном формате."""

# Глобальные переменные
bot_data = {
    'settings': {
        'max_channels': 150,
        'posts_range': (1, 5),
        'delay_range': (20, 1000),
        'target_channel': 'https://t.me/cosmoptichka5',
        'topics': ['Мода и красота', 'Бизнес и стартапы', 'Маркетинг, PR, реклама'],
        'keywords': ['бренд', 'мода', 'fashion', 'beauty', 'запуск бренда', 'маркетинг', 'упаковка', 'WB', 'Wildberries', 'Ozon', 'стратегия маркетинга', 'продвижение бренда'],
        'track_new_posts': False
    },
    'prompts': {
        'comment_prompt': DEFAULT_COMMENT_PROMPT,
        'analysis_prompt': DEFAULT_ANALYSIS_PROMPT
    },
    'statistics': {
        'comments_sent': 0,
        'channels_processed': 0,
        'reactions_set': 0
    },
    'active_users': set(),
    'admin_user': None,
    'is_running': False,
    'telethon_client': None,  # Единый клиент для всего приложения
    'selected_topics': set(),
    'pending_manual_setup': {},
    'user_states': {},
    'detailed_statistics': {
        'processed_channels': {},
        'queue_channels': [],
        'found_channels': []
    },
    'initialization_complete': False,
    'new_post_tracker': None,
    'active_messages': {
        'statistics': {},  # {user_id: {'message_id': id, 'chat_id': chat_id}}
        'settings': {}     # {user_id: {'message_id': id, 'chat_id': chat_id}}
    }
}

# Доступные темы
AVAILABLE_TOPICS = [
    'Бизнес и стартапы', 'Блоги', 'Букмекерство', 'Видео и фильмы', 'Даркнет',
    'Дизайн', 'Для взрослых', 'Еда и кулинария', 'Здоровье и Фитнес', 'Игры',
    'Инстаграм', 'Интерьер и строительство', 'Искусство', 'Картинки и фото',
    'Карьера', 'Книги', 'Криптовалюты', 'Курсы и гайды', 'Лингвистика',
    'Маркетинг, PR, реклама', 'Медицина', 'Мода и красота', 'Музыка',
    'Новости и СМИ', 'Образование', 'Познавательное', 'Политика', 'Право',
    'Природа', 'Продажи', 'Психология', 'Путешествия', 'Религия', 'Рукоделие',
    'Семья и дети', 'Софт и приложения', 'Спорт', 'Технологии', 'Транспорт',
    'Цитаты', 'Шок-контент', 'Эзотерика', 'Экономика', 'Эроктика',
    'Юмор и развлечения', 'Другое'
]

def simple_encrypt(text, key="telegram_mass_looker_2024"):
    """Простое шифрование"""
    if not text:
        return ""
    key_nums = [ord(c) for c in key]
    encrypted = []
    for i, char in enumerate(text):
        key_char = key_nums[i % len(key_nums)]
        encrypted_char = chr((ord(char) + key_char) % 256)
        encrypted.append(encrypted_char)
    encrypted_text = ''.join(encrypted)
    return base64.b64encode(encrypted_text.encode('latin-1')).decode()

def simple_decrypt(encrypted_text, key="telegram_mass_looker_2024"):
    """Простая расшифровка"""
    if not encrypted_text:
        return ""
    try:
        encrypted_bytes = base64.b64decode(encrypted_text.encode())
        encrypted = encrypted_bytes.decode('latin-1')
        key_nums = [ord(c) for c in key]
        decrypted = []
        for i, char in enumerate(encrypted):
            key_char = key_nums[i % len(key_nums)]
            decrypted_char = chr((ord(char) - key_char) % 256)
            decrypted.append(decrypted_char)
        return ''.join(decrypted)
    except Exception:
        return ""

async def save_bot_state():
    """Сохранение полного состояния бота"""
    try:
        # Сохраняем состояние бота пакетно для лучшей производительности
        bot_state_data = [
            ('settings', bot_data['settings']),
            ('prompts', bot_data['prompts']),
            ('admin_user', bot_data['admin_user']),
            ('is_running', bot_data['is_running']),
            ('detailed_statistics', bot_data['detailed_statistics'])
        ]
        
        # Используем пакетное сохранение для состояния бота
        for key, value in bot_state_data:
            await db.save_bot_state(key, value)
        
        await db.save_statistics(bot_data['statistics'])
        
        # ИСПРАВЛЕНИЕ: НЕ перезаписываем всю сессию, а только обновляем state
        for user_id, state in bot_data['user_states'].items():
            # Загружаем существующую сессию
            existing_session = await db.load_user_session(user_id)
            # Обновляем только state, сохраняя остальные данные
            existing_session['state'] = state
            await db.save_user_session(user_id, existing_session)
        
        logger.info("Состояние бота сохранено в базу данных")
    except Exception as e:
        logger.error(f"Ошибка сохранения состояния бота: {e}")

async def load_bot_state():
    """Загрузка полного состояния бота с восстановлением кеша сессий пользователей"""
    try:
        settings = await db.load_bot_state('settings', bot_data['settings'])
        if settings:
            bot_data['settings'] = settings
        
        prompts = await db.load_bot_state('prompts', bot_data['prompts'])
        if prompts:
            bot_data['prompts'] = prompts
        
        admin_user = await db.load_bot_state('admin_user')
        if admin_user:
            bot_data['admin_user'] = admin_user
        
        is_running = await db.load_bot_state('is_running', False)
        bot_data['is_running'] = is_running
        
        detailed_statistics = await db.load_bot_state('detailed_statistics', bot_data['detailed_statistics'])
        bot_data['detailed_statistics'] = detailed_statistics
        
        statistics = await db.load_statistics()
        bot_data['statistics'] = statistics
        
        # ПОДРОБНОЕ ЛОГИРОВАНИЕ: восстанавливаем кеш сессий всех пользователей из БД
        try:
            import aiosqlite
            import os
            logger.info("🔍 DEBUG: Начинаем восстановление кеша сессий из БД")
            
            async with aiosqlite.connect(db.db_file) as conn:
                logger.info(f"🔍 DEBUG: Подключились к БД: {db.db_file}")
                
                async with conn.execute('SELECT user_id, session_data FROM user_sessions') as cursor:
                    rows = await cursor.fetchall()
                    logger.info(f"🔍 DEBUG: Найдено {len(rows)} записей в таблице user_sessions")
                    
                    for row in rows:
                        user_id, session_json = row
                        logger.info(f"🔍 DEBUG: Обрабатываем запись для пользователя {user_id}")
                        logger.info(f"🔍 DEBUG: Сырые данные из БД: {session_json[:200] if session_json else 'None'}...")
                        
                        if session_json:
                            try:
                                session_data = json.loads(session_json)
                                logger.info(f"🔍 DEBUG: Распарсенные данные сессии: {session_data}")
                                
                                # КРИТИЧЕСКОЕ ИСПРАВЛЕНИЕ: проверяем существование файла сессии Telethon
                                session_file = f'user_session_{user_id}.session'
                                session_file_exists = os.path.exists(session_file)
                                logger.info(f"🔍 DEBUG: Файл сессии {session_file} существует: {session_file_exists}")
                                
                                if session_file_exists:
                                    # Если файл сессии существует, значит пользователь реально авторизован
                                    user_sessions_cache[int(user_id)] = session_data
                                    logger.info(f"✅ DEBUG: Восстановлена сессия пользователя {user_id} в кеше с файлом сессии")
                                    logger.info(f"🔍 DEBUG: Содержимое кеша для {user_id}: {user_sessions_cache[int(user_id)]}")
                                else:
                                    # Если файла сессии нет, сбрасываем флаги авторизации в БД
                                    logger.warning(f"⚠️ DEBUG: Файл сессии для пользователя {user_id} не найден, сбрасываем авторизацию")
                                    session_data['has_active_client'] = False
                                    session_data['is_running'] = False
                                    await db.save_user_session(int(user_id), session_data)
                                    user_sessions_cache[int(user_id)] = session_data
                                    logger.info(f"🔍 DEBUG: Обновленное содержимое кеша для {user_id}: {user_sessions_cache[int(user_id)]}")
                                    
                            except Exception as e:
                                logger.error(f"❌ DEBUG: Ошибка восстановления сессии пользователя {user_id}: {e}")
                        else:
                            logger.warning(f"⚠️ DEBUG: Пустые данные сессии для пользователя {user_id}")
        except Exception as e:
            logger.error(f"❌ DEBUG: Ошибка восстановления кеша сессий: {e}")
        
        logger.info(f"✅ DEBUG: Состояние бота загружено из базы данных. Восстановлено {len(user_sessions_cache)} сессий в кеше")
        logger.info(f"🔍 DEBUG: Содержимое всего кеша: {user_sessions_cache}")
    except Exception as e:
        logger.error(f"❌ DEBUG: Ошибка загрузки состояния бота: {e}")

async def get_user_telethon_client(user_id: int) -> Optional[TelegramClient]:
    """Получение Telethon клиента для конкретного пользователя"""
    try:
        # ИСПРАВЛЕНИЕ: загружаем актуальные данные из БД, а НЕ из кеша
        session_data = await db.load_user_session(user_id)
        client_config = session_data.get('client_config')
        
        if not client_config:
            logger.debug(f"Нет конфигурации клиента для пользователя {user_id}")
            return None
            
        if not all(key in client_config for key in ['api_id', 'api_hash', 'phone']):
            logger.debug(f"Неполная конфигурация клиента для пользователя {user_id}")
            return None
        
        # Проверяем что api_id является числом
        try:
            api_id = int(client_config['api_id'])
        except (ValueError, TypeError):
            logger.error(f"Неверный формат API ID для пользователя {user_id}")
            return None
            
        session_file = f'user_session_{user_id}'
        
        # КРИТИЧЕСКОЕ ИСПРАВЛЕНИЕ: проверяем существование файла сессии
        import os
        if not os.path.exists(f'{session_file}.session'):
            logger.warning(f"Файл сессии {session_file}.session не найден для пользователя {user_id}")
            # Сбрасываем флаги авторизации в БД и кеше
            session_data['has_active_client'] = False
            session_data['is_running'] = False
            await db.save_user_session(user_id, session_data)
            if user_id in user_sessions_cache:
                user_sessions_cache[user_id] = session_data
            return None
            
        loop = asyncio.get_event_loop()
        
        client = TelegramClient(
            session_file,
            api_id,
            client_config['api_hash'],
            loop=loop,
            timeout=30,
            retry_delay=1,
            flood_sleep_threshold=60
        )
        
        await client.connect()
        
        if await client.is_user_authorized():
            logger.info(f"Клиент для пользователя {user_id} создан и авторизован")
            return client
        else:
            await client.disconnect()
            logger.debug(f"Клиент для пользователя {user_id} не авторизован")
            # Сбрасываем флаги авторизации
            session_data['has_active_client'] = False
            session_data['is_running'] = False
            await db.save_user_session(user_id, session_data)
            if user_id in user_sessions_cache:
                user_sessions_cache[user_id] = session_data
            return None
            
    except Exception as e:
        logger.error(f"Ошибка получения клиента для пользователя {user_id}: {e}")
        return None

async def show_main_menu(update: Update, context: ContextTypes.DEFAULT_TYPE, edit=True):
    """Показать главное меню с персональными настройками"""
    user_id = update.effective_user.id
    
    welcome_text = """🤖 Система нейрокомментинга и массреакшена

Добро пожаловать! Выберите действие:"""
    
    logger.info(f"🔍 DEBUG: show_main_menu вызвана для пользователя {user_id}")
    logger.info(f"🔍 DEBUG: Текущее содержимое кеша для {user_id}: {user_sessions_cache.get(user_id, 'НЕТ В КЕШЕ')}")
    
    # ПОДРОБНОЕ ЛОГИРОВАНИЕ: загружаем данные из БД
    try:
        logger.info(f"🔍 DEBUG: Загружаем данные из БД для пользователя {user_id}")
        user_session = await db.load_user_session(user_id)
        logger.info(f"🔍 DEBUG: Данные из БД: {user_session}")
        
        # Проверяем отличается ли БД от кеша
        cached_data = user_sessions_cache.get(user_id, {})
        if user_session != cached_data:
            logger.warning(f"⚠️ DEBUG: НЕСООТВЕТСТВИЕ! БД ≠ КЕШ")
            logger.warning(f"🔍 DEBUG: БД содержит: {user_session}")
            logger.warning(f"🔍 DEBUG: КЕШ содержит: {cached_data}")
        else:
            logger.info(f"✅ DEBUG: БД и КЕШ совпадают")
        
        # КРИТИЧЕСКОЕ ИСПРАВЛЕНИЕ: обновляем кеш актуальными данными
        user_sessions_cache[user_id] = user_session.copy()
        logger.info(f"🔍 DEBUG: Кеш обновлен для пользователя {user_id}: {user_sessions_cache[user_id]}")
        
        has_client = user_session.get('has_active_client', False)
        client_config = user_session.get('client_config', {})
        is_running = user_session.get('is_running', False)
        
        logger.info(f"🔍 DEBUG: Извлеченные значения:")
        logger.info(f"🔍 DEBUG: has_active_client = {has_client}")
        logger.info(f"🔍 DEBUG: client_config = {client_config}")
        logger.info(f"🔍 DEBUG: is_running = {is_running}")
        
        logger.info(f"Главное меню - состояние для пользователя {user_id}: has_client={has_client}, config_present={bool(client_config)}, is_running={is_running}")
        
        # ИСПРАВЛЕНИЕ: дополнительная проверка существования файла сессии
        if has_client and client_config:
            import os
            session_file = f'user_session_{user_id}.session'
            session_file_exists = os.path.exists(session_file)
            logger.info(f"🔍 DEBUG: Проверяем файл сессии {session_file}: существует = {session_file_exists}")
            
            if not session_file_exists:
                logger.warning(f"⚠️ DEBUG: Файл сессии отсутствует для пользователя {user_id}, сбрасываем флаги")
                has_client = False
                is_running = False
                # Обновляем БД и кеш
                user_session['has_active_client'] = False
                user_session['is_running'] = False
                await db.save_user_session(user_id, user_session)
                user_sessions_cache[user_id] = user_session.copy()
                logger.info(f"🔍 DEBUG: Флаги сброшены в БД и кеше")
            else:
                # Проверяем что все необходимые данные есть
                required_fields = ['api_id', 'api_hash', 'phone']
                missing_fields = [field for field in required_fields if not client_config.get(field)]
                logger.info(f"🔍 DEBUG: Проверка полей конфигурации: отсутствуют {missing_fields}")
                
                if missing_fields:
                    logger.warning(f"⚠️ DEBUG: Неполная конфигурация клиента для пользователя {user_id}, отсутствуют поля: {missing_fields}")
                    has_client = False  # Локально меняем для отображения
        
        logger.info(f"✅ DEBUG: Главное меню - финальное состояние для пользователя {user_id}: has_client={has_client}, is_running={is_running}")
        
    except Exception as e:
        logger.error(f"❌ DEBUG: Ошибка загрузки сессии пользователя {user_id}: {e}")
        import traceback
        logger.error(f"❌ DEBUG: Полный traceback: {traceback.format_exc()}")
        has_client = False
        is_running = False
    
    account_button_text = "👤 Сменить аккаунт" if has_client else "➕ Добавить аккаунт"
    run_button_text = "⏹️ Остановить рассылку" if is_running else "▶️ Запустить рассылку"
    
    logger.info(f"🔍 DEBUG: Создаем кнопки: account='{account_button_text}', run='{run_button_text}'")
    
    keyboard = [
        [InlineKeyboardButton(account_button_text, callback_data="account_setup")],
        [InlineKeyboardButton("📺 Выбрать целевой канал", callback_data="target_channel")],
        [InlineKeyboardButton("⚙️ Параметры масслукинга", callback_data="settings")],
        [InlineKeyboardButton("📋 Промты", callback_data="prompts")],
        [InlineKeyboardButton(run_button_text, callback_data="toggle_run")],
        [InlineKeyboardButton("📊 Статистика", callback_data="statistics")]
    ]
    
    reply_markup = InlineKeyboardMarkup(keyboard)
    
    bot_data['user_states'][user_id] = 'main_menu'
    
    channel_message_id = context.user_data.get('channel_selection_message_id')
    awaiting_channel = context.user_data.get('awaiting_channel')
    
    if channel_message_id:
        try:
            await context.bot.delete_message(
                chat_id=user_id,
                message_id=channel_message_id
            )
        except Exception as e:
            logger.debug(f"Не удалось удалить сообщение выбора канала: {e}")
    
    if awaiting_channel:
        try:
            from telegram import ReplyKeyboardRemove
            await context.bot.send_message(
                chat_id=user_id,
                text="",
                reply_markup=ReplyKeyboardRemove()
            )
        except:
            pass
    
    context.user_data.clear()
    
    asyncio.create_task(save_bot_state())
    
    if edit and update.callback_query:
        try:
            await update.callback_query.edit_message_text(welcome_text, reply_markup=reply_markup)
        except Exception as e:
            logger.warning(f"Не удалось отредактировать сообщение: {e}")
            await update.callback_query.message.reply_text(welcome_text, reply_markup=reply_markup)
    else:
        if update.callback_query:
            await update.callback_query.message.reply_text(welcome_text, reply_markup=reply_markup)
        else:
            await update.message.reply_text(welcome_text, reply_markup=reply_markup)

async def update_user_session_cache(user_id: int):
    """Обновление кеша сессии пользователя"""
    try:
        user_session = await db.load_user_session(user_id)
        user_sessions_cache[user_id] = user_session.copy()
        logger.info(f"Кеш сессии обновлен для пользователя {user_id}: {user_session}")
    except Exception as e:
        logger.error(f"Ошибка обновления кеша сессии для пользователя {user_id}: {e}")

async def process_telegram_code(update: Update, context: ContextTypes.DEFAULT_TYPE, code: str):
    """Обработка кода авторизации для конкретного пользователя"""
    user_id = update.effective_user.id
    
    try:
        client = context.user_data['client']
        phone_code_hash = context.user_data['phone_code_hash']
        config = context.user_data.get('config', {})
        
        logger.info(f"Попытка авторизации с кодом для пользователя {user_id}")
        
        await client.sign_in(
            phone=config['phone'],
            code=code,
            phone_code_hash=phone_code_hash
        )
        
        logger.info(f"Авторизация по коду успешна для пользователя {user_id}, сохраняем конфигурацию")
        logger.info(f"Конфигурация для сохранения: {config}")
        
        # ИСПРАВЛЕНИЕ: правильно сохраняем данные пользователя в БД
        session_data = {
            'client_config': config,
            'has_active_client': True,
            'state': 'main_menu'
        }
        
        await db.save_user_session(user_id, session_data)
        logger.info(f"Конфигурация клиента сохранена для пользователя {user_id}")
        
        # ИСПРАВЛЕНИЕ: обновляем кеш ПОСЛЕ сохранения в БД
        await update_user_session_cache(user_id)
        
        # Проверяем что сохранилось
        saved_session = await db.load_user_session(user_id)
        logger.info(f"Проверка сохранения конфигурации клиента: {saved_session}")
        
        await client.disconnect()
        
        await update.callback_query.edit_message_text("✅ Успешный вход в аккаунт!")
        context.user_data.clear()
        
        await show_main_menu(update, context, edit=False)
        return
        
    except SessionPasswordNeededError:
        logger.info(f"Пользователю {user_id} требуется двухфакторная аутентификация")
        keyboard = [[get_back_button()]]
        reply_markup = InlineKeyboardMarkup(keyboard)
        
        await update.callback_query.edit_message_text(
            "🔐 Требуется пароль двухфакторной аутентификации.\n\n"
            "Отправьте ваш пароль:",
            reply_markup=reply_markup
        )
        context.user_data['setup_step'] = 'password'
        bot_data['user_states'][user_id] = 'password'
        return
        
    except PhoneCodeInvalidError:
        logger.warning(f"Неверный код для пользователя {user_id}")
        await update.callback_query.edit_message_text(
            "❌ Неверный код. Попробуйте еще раз.\n\n"
            f"Введенный код: {context.user_data['entered_code']}\n\n"
            "Введите код с помощью кнопок ниже:",
            reply_markup=get_code_input_keyboard()
        )
        context.user_data['entered_code'] = ''
        return
        
    except Exception as e:
        logger.error(f"Ошибка входа с кодом для пользователя {user_id}: {e}")
        keyboard = [[get_back_button()]]
        reply_markup = InlineKeyboardMarkup(keyboard)
        await update.callback_query.edit_message_text(f"❌ Ошибка входа: {e}", reply_markup=reply_markup)

async def handle_telegram_password(update: Update, context: ContextTypes.DEFAULT_TYPE, password: str):
    """Обработка пароля Telegram для конкретного пользователя"""
    user_id = update.effective_user.id
    
    try:
        client = context.user_data.get('client')
        if not client:
            await show_error_with_back_button(update, context, "❌ Сессия истекла. Начните настройку заново.")
            return
        
        logger.info(f"🔍 DEBUG: Попытка авторизации с паролем для пользователя {user_id}")
        
        await client.sign_in(password=password)
        
        config = context.user_data.get('config', {})
        config['password'] = password
        
        logger.info(f"✅ DEBUG: Авторизация с паролем успешна для пользователя {user_id}")
        logger.info(f"🔍 DEBUG: Конфигурация для сохранения: {config}")
        
        # ИСПРАВЛЕНИЕ: правильно сохраняем данные пользователя в БД
        session_data = {
            'client_config': config,
            'has_active_client': True,
            'state': 'main_menu'
        }
        
        logger.info(f"🔍 DEBUG: Сохраняем в БД для пользователя {user_id}: {session_data}")
        await db.save_user_session(user_id, session_data)
        logger.info(f"✅ DEBUG: Данные сохранены в БД")
        
        # ИСПРАВЛЕНИЕ: обновляем кеш ПОСЛЕ сохранения в БД
        user_sessions_cache[user_id] = session_data.copy()
        logger.info(f"🔍 DEBUG: Кеш обновлен: {user_sessions_cache[user_id]}")
        
        # Проверяем что сохранилось
        saved_session = await db.load_user_session(user_id)
        logger.info(f"🔍 DEBUG: Проверяем что сохранилось в БД: {saved_session}")
        
        # Проверяем файл сессии
        import os
        session_file = f'user_session_{user_id}.session'
        session_file_exists = os.path.exists(session_file)
        logger.info(f"🔍 DEBUG: Файл сессии {session_file} существует: {session_file_exists}")
        
        await client.disconnect()
        
        await update.message.reply_text("✅ Успешный вход в аккаунт!")
        context.user_data.clear()
        await show_main_menu(update, context, edit=False)
        
    except PasswordHashInvalidError:
        logger.warning(f"⚠️ DEBUG: Неверный пароль для пользователя {user_id}")
        await show_error_with_back_button(update, context, "❌ Неверный пароль. Попробуйте еще раз:")
    except Exception as e:
        logger.error(f"❌ DEBUG: Ошибка входа с паролем для пользователя {user_id}: {e}")
        import traceback
        logger.error(f"❌ DEBUG: Полный traceback: {traceback.format_exc()}")
        await show_error_with_back_button(update, context, f"❌ Ошибка входа: {e}")
        
        if 'client' in context.user_data:
            try:
                await context.user_data['client'].disconnect()
            except:
                pass
            context.user_data.clear()

def load_user_config():
    """Загрузка конфигурации пользователя"""
    config_file = 'config.json'
    if os.path.exists(config_file):
        try:
            with open(config_file, 'r', encoding='utf-8') as f:
                config = json.load(f)
            for key in ['api_id', 'api_hash', 'phone', 'password']:
                if key in config and config[key]:
                    config[key] = simple_decrypt(config[key])
            return config
        except Exception as e:
            logger.error(f"Ошибка загрузки конфигурации: {e}")
    return {}

async def update_user_session_cache(user_id: int):
    """Обновление кеша сессии пользователя"""
    try:
        user_session = await db.load_user_session(user_id)
        user_sessions_cache[user_id] = user_session
        logger.info(f"Кеш сессии обновлен для пользователя {user_id}: {user_session}")
    except Exception as e:
        logger.error(f"Ошибка обновления кеша сессии для пользователя {user_id}: {e}")

def save_user_config(config):
    """Сохранение конфигурации пользователя"""
    config_file = 'config.json'
    try:
        existing_config = {}
        if os.path.exists(config_file):
            with open(config_file, 'r', encoding='utf-8') as f:
                existing_config = json.load(f)
        
        existing_config.update(config)
        
        # Шифруем данные
        encrypted_config = existing_config.copy()
        for key in ['api_id', 'api_hash', 'phone', 'password']:
            if key in encrypted_config and encrypted_config[key]:
                encrypted_config[key] = simple_encrypt(encrypted_config[key])
        
        with open(config_file, 'w', encoding='utf-8') as f:
            json.dump(encrypted_config, f, indent=2)
    except Exception as e:
        logger.error(f"Ошибка сохранения конфигурации: {e}")

def check_access(user_id):
    """Проверка доступа пользователя"""
    return True

def get_back_button():
    """Получение кнопки Назад"""
    return InlineKeyboardButton("◀️ Назад", callback_data="back")

def get_main_menu_keyboard(user_id: int = None):
    """Получение клавиатуры главного меню для пользователя"""
    if user_id:
        has_client = False
        is_running = False
    else:
        has_client = False
        is_running = bot_data.get('is_running', False)
    
    account_button_text = "👤 Сменить аккаунт" if has_client else "➕ Добавить аккаунт"
    run_button_text = "⏹️ Остановить рассылку" if is_running else "▶️ Запустить рассылку"
    
    keyboard = [
        [InlineKeyboardButton(account_button_text, callback_data="account_setup")],
        [InlineKeyboardButton("📺 Выбрать целевой канал", callback_data="target_channel")],
        [InlineKeyboardButton("⚙️ Параметры масслукинга", callback_data="settings")],
        [InlineKeyboardButton("📋 Промты", callback_data="prompts")],
        [InlineKeyboardButton(run_button_text, callback_data="toggle_run")],
        [InlineKeyboardButton("📊 Статистика", callback_data="statistics")]
    ]
    
    return InlineKeyboardMarkup(keyboard)

def get_code_input_keyboard():
    """Получение правильной клавиатуры для ввода кода"""
    keyboard = [
        [InlineKeyboardButton("1", callback_data="code_1"),
         InlineKeyboardButton("2", callback_data="code_2"),
         InlineKeyboardButton("3", callback_data="code_3")],
        [InlineKeyboardButton("4", callback_data="code_4"),
         InlineKeyboardButton("5", callback_data="code_5"),
         InlineKeyboardButton("6", callback_data="code_6")],
        [InlineKeyboardButton("7", callback_data="code_7"),
         InlineKeyboardButton("8", callback_data="code_8"),
         InlineKeyboardButton("9", callback_data="code_9")],
        [InlineKeyboardButton("отправить ✅", callback_data="code_send"),
         InlineKeyboardButton("0", callback_data="code_0"),
         InlineKeyboardButton("стереть ⬅️", callback_data="code_delete")],
        [InlineKeyboardButton("📞 Отправить код повторно", callback_data="code_resend")],
        [InlineKeyboardButton("Отмена ❌", callback_data="code_cancel")]
    ]
    
    return InlineKeyboardMarkup(keyboard)

async def get_post_comments(message_id: int, channel_entity) -> str:
    """Получение комментариев к посту"""
    try:
        if not bot_data['telethon_client']:
            logger.warning("Telethon клиент не инициализирован")
            return ""
        
        # Получаем discussion message через GetDiscussionMessageRequest
        discussion_info = await bot_data['telethon_client'](GetDiscussionMessageRequest(
            peer=channel_entity,
            msg_id=message_id
        ))
        
        if not discussion_info or not discussion_info.messages:
            return ""
        
        discussion_message = discussion_info.messages[0]
        discussion_group = discussion_message.peer_id
        reply_to_msg_id = discussion_message.id
        
        # Получаем ответы на этот пост (комментарии)
        replies = await bot_data['telethon_client'](GetRepliesRequest(
            peer=discussion_group,
            msg_id=reply_to_msg_id,
            offset_date=None,
            offset_id=0,
            offset_peer=None,
            limit=50
        ))
        
        if not replies or not replies.messages:
            return ""
        
        comments = []
        total_length = 0
        max_length = 10000
        
        for msg in replies.messages:
            if msg.message and msg.message.strip():
                # Получаем имя отправителя
                sender_name = "Аноним"
                try:
                    if hasattr(msg, 'from_id') and msg.from_id:
                        sender = await bot_data['telethon_client'].get_entity(msg.from_id)
                        if hasattr(sender, 'first_name'):
                            sender_name = sender.first_name
                            if hasattr(sender, 'last_name') and sender.last_name:
                                sender_name += f" {sender.last_name}"
                        elif hasattr(sender, 'title'):
                            sender_name = sender.title
                except:
                    pass
                
                comment_text = f"{sender_name}: {msg.message.strip()}"
                
                # Проверяем лимит длины
                if total_length + len(comment_text) + 2 > max_length:
                    break
                
                comments.append(comment_text)
                total_length += len(comment_text) + 2
        
        return "\n\n".join(comments)
        
    except Exception as e:
        logger.error(f"Ошибка получения комментариев: {e}")
        return ""

async def show_prompts_menu(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Показ меню управления промтами"""
    query = update.callback_query
    await query.answer()
    
    user_id = query.from_user.id
    if not check_access(user_id):
        await query.answer("❌ Доступ ограничен", show_alert=True)
        return
    
    bot_data['user_states'][user_id] = 'prompts_menu'
    asyncio.create_task(save_bot_state())
    
    comment_prompt = bot_data['prompts']['comment_prompt']
    analysis_prompt = bot_data['prompts']['analysis_prompt']
    
    # Экранируем HTML символы
    def escape_html(text):
        return text.replace('&', '&amp;').replace('<', '&lt;').replace('>', '&gt;')
    
    message_text = f"""<b>✍🏻 ПОЛНЫЙ промт на написание комментариев:</b>

<code>{escape_html(comment_prompt)}</code>

————————————————————————

<b>🔍 ПОЛНЫЙ промт на анализ канала:</b>

<code>{escape_html(analysis_prompt)}</code>

————————————————————————

<b>🔁 Как изменить промты:</b>

<b>Для смены промта комментариев:</b>
<code>Промт для комментариев: ваш новый промт</code>

<b>Для смены промта анализа:</b>
<code>Промт для анализа: ваш новый промт</code>

<b>Плейсхолдеры:</b>
• <code>{{text_of_the_post}}</code> - текст поста
• <code>{{topics}}</code> - темы канала  
• <code>{{comments}}</code> - комментарии
• <code>{{full_text}}</code> - данные канала"""
    
    keyboard = [
        [InlineKeyboardButton("◀️ Назад", callback_data="back")],
        [InlineKeyboardButton("🔁 Сбросить", callback_data="reset_prompts")]
    ]
    
    reply_markup = InlineKeyboardMarkup(keyboard)
    
    await query.edit_message_text(
        message_text, 
        reply_markup=reply_markup, 
        parse_mode=ParseMode.HTML
    )

async def handle_prompt_change(update: Update, context: ContextTypes.DEFAULT_TYPE, text: str):
    """Обработка изменения промтов"""
    user_id = update.effective_user.id
    
    if text.startswith("Промт для комментариев:"):
        new_prompt = text.replace("Промт для комментариев:", "").strip()
        
        # Проверяем обязательные плейсхолдеры
        if "{text_of_the_post}" not in new_prompt:
            await update.message.reply_text(
                "❌ Ошибка: В промте для комментариев должен быть обязательный плейсхолдер <code>{text_of_the_post}</code>",
                parse_mode=ParseMode.HTML
            )
            return
        
        bot_data['prompts']['comment_prompt'] = new_prompt
        await save_bot_state()
        
        await update.message.reply_text("✅ Промت для комментариев обновлен!")
        
        # Обновляем сообщение с промтами
        await show_prompts_menu_updated(update, context)
        
    elif text.startswith("Промт для анализа:"):
        new_prompt = text.replace("Промт для анализа:", "").strip()
        
        # Проверяем обязательные плейсхолдеры
        required_placeholders = ["{full_text}", "{topics}"]
        missing_placeholders = []
        
        for placeholder in required_placeholders:
            if placeholder not in new_prompt:
                missing_placeholders.append(placeholder)
        
        if missing_placeholders:
            escaped_placeholders = [f"<code>{p}</code>" for p in missing_placeholders]
            await update.message.reply_text(
                f"❌ Ошибка: В промте для анализа должны быть обязательные плейсхолдеры: {', '.join(escaped_placeholders)}",
                parse_mode=ParseMode.HTML
            )
            return
        
        bot_data['prompts']['analysis_prompt'] = new_prompt
        await save_bot_state()
        
        await update.message.reply_text("✅ Промт для анализа обновлен!")
        
        # Обновляем сообщение с промтами
        await show_prompts_menu_updated(update, context)
    else:
        # Проверяем, содержит ли сообщение оба промта
        if "Промт для комментариев:" in text and "Промт для анализа:" in text:
            lines = text.split('\n')
            comment_section = []
            analysis_section = []
            current_section = None
            
            for line in lines:
                if line.startswith("Промт для комментариев:"):
                    current_section = "comment"
                    comment_section.append(line.replace("Промт для комментариев:", "").strip())
                elif line.startswith("Промт для анализа:"):
                    current_section = "analysis"
                    analysis_section.append(line.replace("Промт для анализа:", "").strip())
                elif current_section == "comment":
                    comment_section.append(line)
                elif current_section == "analysis":
                    analysis_section.append(line)
            
            comment_prompt = '\n'.join(comment_section).strip()
            analysis_prompt = '\n'.join(analysis_section).strip()
            
            # Валидация промта комментариев
            if "{text_of_the_post}" not in comment_prompt:
                await update.message.reply_text(
                    "❌ Ошибка: В промте для комментариев должен быть обязательный плейсхолдер <code>{text_of_the_post}</code>",
                    parse_mode=ParseMode.HTML
                )
                return
            
            # Валидация промта анализа
            required_placeholders = ["{full_text}", "{topics}"]
            missing_placeholders = []
            
            for placeholder in required_placeholders:
                if placeholder not in analysis_prompt:
                    missing_placeholders.append(placeholder)
            
            if missing_placeholders:
                escaped_placeholders = [f"<code>{p}</code>" for p in missing_placeholders]
                await update.message.reply_text(
                    f"❌ Ошибка: В промте для анализа должны быть обязательные плейсхолдеры: {', '.join(escaped_placeholders)}",
                    parse_mode=ParseMode.HTML
                )
                return
            
            # Сохраняем оба промта
            bot_data['prompts']['comment_prompt'] = comment_prompt
            bot_data['prompts']['analysis_prompt'] = analysis_prompt
            await save_bot_state()
            
            await update.message.reply_text("✅ Оба промта обновлены!")
            
            # Обновляем сообщение с промтами
            await show_prompts_menu_updated(update, context)
        else:
            await update.message.reply_text(
                "❌ Неверный формат. Используйте:\n<code>Промт для комментариев: ваш промт</code> или\n<code>Промт для анализа: ваш промт</code>",
                parse_mode=ParseMode.HTML
            )

async def show_prompts_menu_updated(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Обновленное меню промтов"""
    user_id = update.effective_user.id
    
    comment_prompt = bot_data['prompts']['comment_prompt']
    analysis_prompt = bot_data['prompts']['analysis_prompt']
    
    # Экранируем HTML символы
    def escape_html(text):
        return text.replace('&', '&amp;').replace('<', '&lt;').replace('>', '&gt;')
    
    message_text = f"""<b>✍🏻 ПОЛНЫЙ промт на написание комментариев:</b>

<code>{escape_html(comment_prompt)}</code>

————————————————————————

<b>🔍 ПОЛНЫЙ промт на анализ канала:</b>

<code>{escape_html(analysis_prompt)}</code>

————————————————————————

<b>🔁 Как изменить промты:</b>

<b>Для смены промта комментариев:</b>
<code>Промт для комментариев: ваш новый промт</code>

<b>Для смены промта анализа:</b>
<code>Промт для анализа: ваш новый промт</code>

<b>Плейсхолдеры:</b>
• <code>{{text_of_the_post}}</code> - текст поста
• <code>{{topics}}</code> - темы канала  
• <code>{{comments}}</code> - комментарии
• <code>{{full_text}}</code> - данные канала"""
    
    keyboard = [
        [InlineKeyboardButton("◀️ Назад", callback_data="back")],
        [InlineKeyboardButton("🔁 Сбросить", callback_data="reset_prompts")]
    ]
    
    reply_markup = InlineKeyboardMarkup(keyboard)
    
    await context.bot.send_message(
        chat_id=user_id,
        text=message_text,
        reply_markup=reply_markup,
        parse_mode=ParseMode.HTML
    )

async def reset_prompts(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Сброс промтов на дефолтные"""
    query = update.callback_query
    await query.answer()
    
    bot_data['prompts']['comment_prompt'] = DEFAULT_COMMENT_PROMPT
    bot_data['prompts']['analysis_prompt'] = DEFAULT_ANALYSIS_PROMPT
    await save_bot_state()
    
    await query.answer("✅ Промты сброшены на дефолтные", show_alert=True)
    
    # Обновляем сообщение
    await show_prompts_menu(update, context)

async def show_main_menu(update: Update, context: ContextTypes.DEFAULT_TYPE, edit=True):
    """Показать главное меню с персональными настройками"""
    user_id = update.effective_user.id
    
    welcome_text = """🤖 Система нейрокомментинга и массреакшена

Добро пожаловать! Выберите действие:"""
    
    # ИСПРАВЛЕНИЕ: ВСЕГДА загружаем свежие данные из БД, игнорируем кеш
    try:
        user_session = await db.load_user_session(user_id)
        logger.info(f"Главное меню - загружена СВЕЖАЯ сессия пользователя {user_id}: {user_session}")
        
        # КРИТИЧЕСКОЕ ИСПРАВЛЕНИЕ: обновляем кеш актуальными данными
        user_sessions_cache[user_id] = user_session.copy()
        logger.info(f"Главное меню - кеш обновлен для пользователя {user_id}: {user_sessions_cache[user_id]}")
        
        has_client = user_session.get('has_active_client', False)
        client_config = user_session.get('client_config', {})
        is_running = user_session.get('is_running', False)
        
        logger.info(f"Главное меню - состояние для пользователя {user_id}: has_client={has_client}, config_present={bool(client_config)}, is_running={is_running}")
        
        # ИСПРАВЛЕНИЕ: дополнительная проверка существования файла сессии
        if has_client and client_config:
            import os
            session_file = f'user_session_{user_id}.session'
            if not os.path.exists(session_file):
                logger.warning(f"Файл сессии отсутствует для пользователя {user_id}, сбрасываем флаги")
                has_client = False
                is_running = False
                # Обновляем БД и кеш
                user_session['has_active_client'] = False
                user_session['is_running'] = False
                await db.save_user_session(user_id, user_session)
                user_sessions_cache[user_id] = user_session.copy()
            else:
                # Проверяем что все необходимые данные есть
                required_fields = ['api_id', 'api_hash', 'phone']
                missing_fields = [field for field in required_fields if not client_config.get(field)]
                
                if missing_fields:
                    logger.warning(f"Неполная конфигурация клиента для пользователя {user_id}, отсутствуют поля: {missing_fields}")
                    has_client = False  # Локально меняем для отображения
        
        logger.info(f"Главное меню - финальное состояние для пользователя {user_id}: has_client={has_client}, is_running={is_running}")
        
    except Exception as e:
        logger.error(f"Ошибка загрузки сессии пользователя {user_id}: {e}")
        has_client = False
        is_running = False
    
    account_button_text = "👤 Сменить аккаунт" if has_client else "➕ Добавить аккаунт"
    run_button_text = "⏹️ Остановить рассылку" if is_running else "▶️ Запустить рассылку"
    
    keyboard = [
        [InlineKeyboardButton(account_button_text, callback_data="account_setup")],
        [InlineKeyboardButton("📺 Выбрать целевой канал", callback_data="target_channel")],
        [InlineKeyboardButton("⚙️ Параметры масслукинга", callback_data="settings")],
        [InlineKeyboardButton("📋 Промты", callback_data="prompts")],
        [InlineKeyboardButton(run_button_text, callback_data="toggle_run")],
        [InlineKeyboardButton("📊 Статистика", callback_data="statistics")]
    ]
    
    reply_markup = InlineKeyboardMarkup(keyboard)
    
    bot_data['user_states'][user_id] = 'main_menu'
    
    channel_message_id = context.user_data.get('channel_selection_message_id')
    awaiting_channel = context.user_data.get('awaiting_channel')
    
    if channel_message_id:
        try:
            await context.bot.delete_message(
                chat_id=user_id,
                message_id=channel_message_id
            )
        except Exception as e:
            logger.debug(f"Не удалось удалить сообщение выбора канала: {e}")
    
    if awaiting_channel:
        try:
            from telegram import ReplyKeyboardRemove
            await context.bot.send_message(
                chat_id=user_id,
                text="",
                reply_markup=ReplyKeyboardRemove()
            )
        except:
            pass
    
    context.user_data.clear()
    
    asyncio.create_task(save_bot_state())
    
    if edit and update.callback_query:
        try:
            await update.callback_query.edit_message_text(welcome_text, reply_markup=reply_markup)
        except Exception as e:
            logger.warning(f"Не удалось отредактировать сообщение: {e}")
            await update.callback_query.message.reply_text(welcome_text, reply_markup=reply_markup)
    else:
        if update.callback_query:
            await update.callback_query.message.reply_text(welcome_text, reply_markup=reply_markup)
        else:
            await update.message.reply_text(welcome_text, reply_markup=reply_markup)

async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Обработчик команды /start"""
    user_id = update.effective_user.id
    
    if not check_access(user_id):
        keyboard = [[get_back_button()]]
        reply_markup = InlineKeyboardMarkup(keyboard)
        await update.message.reply_text("❌ Доступ к боту ограничен", reply_markup=reply_markup)
        return
    
    if bot_data['admin_user'] is None:
        bot_data['admin_user'] = user_id
        asyncio.create_task(save_bot_state())
    
    bot_data['active_users'].add(user_id)
    logger.info(f"Пользователь {user_id} запустил бота")
    
    await show_main_menu(update, context, edit=False)

async def handle_back_button(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Обработка нажатия кнопки 'Назад'"""
    try:
        user_id = update.effective_user.id
        if not check_access(user_id):
            return

        # Очищаем отслеживание активных сообщений для пользователя
        bot_data['active_messages']['statistics'].pop(user_id, None)
        bot_data['active_messages']['settings'].pop(user_id, None)

        # Возвращаемся в главное меню
        await show_main_menu(update, context)

    except Exception as e:
        logger.error(f"Ошибка при обработке кнопки 'Назад': {e}")
        await show_error_with_back_button(update, context, "Ошибка при возврате в главное меню")

async def show_error_with_back_button(update: Update, context: ContextTypes.DEFAULT_TYPE, error_message: str):
    """Показать сообщение об ошибке с кнопкой назад"""
    keyboard = [[get_back_button()]]
    reply_markup = InlineKeyboardMarkup(keyboard)
    
    if update.callback_query:
        await update.callback_query.edit_message_text(error_message, reply_markup=reply_markup)
    else:
        await update.message.reply_text(error_message, reply_markup=reply_markup)

async def account_setup(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Настройка аккаунта"""
    query = update.callback_query
    await query.answer()
    
    user_id = query.from_user.id
    if not check_access(user_id):
        await query.answer("❌ Доступ ограничен", show_alert=True)
        return
    
    bot_data['user_states'][user_id] = 'account_setup'
    asyncio.create_task(save_bot_state())
    
    # Проверяем сохраненную конфигурацию пользователя
    user_session = await db.load_user_session(user_id)
    config = user_session.get('client_config', {})
    
    if not config.get('api_id') or not config.get('api_hash'):
        keyboard = [[get_back_button()]]
        reply_markup = InlineKeyboardMarkup(keyboard)
        
        await query.edit_message_text(
            "📱 Настройка аккаунта\n\n"
            "Для работы с Telegram аккаунтом необходимо получить API ID и API Hash.\n\n"
            "1. Перейдите на https://my.telegram.org\n"
            "2. Войдите в свой аккаунт\n"
            "3. Перейдите в 'API development tools'\n"
            "4. Создайте приложение\n\n"
            "Отправьте API ID:",
            reply_markup=reply_markup
        )
        context.user_data['setup_step'] = 'api_id'
        bot_data['user_states'][user_id] = 'api_id'
        asyncio.create_task(save_bot_state())
        return
    
    keyboard = [[get_back_button()]]
    reply_markup = InlineKeyboardMarkup(keyboard)
    
    await query.edit_message_text(
        "📱 Настройка аккаунта\n\n"
        "API ID и API Hash найдены.\n\n"
        "Отправьте номер телефона в международном формате (например, +79123456789):",
        reply_markup=reply_markup
    )
    context.user_data['setup_step'] = 'phone'
    bot_data['user_states'][user_id] = 'phone'
    asyncio.create_task(save_bot_state())

async def parse_settings(update: Update, context: ContextTypes.DEFAULT_TYPE, text: str):
    """Парсинг настроек масслукинга"""
    try:
        lines = [line.strip() for line in text.split('\n') if line.strip()]
        
        new_settings = {}
        
        for line in lines:
            line_lower = line.lower()
            
            if 'максимальное количество каналов:' in line_lower:
                value = line.split(':')[1].strip()
                if value == '∞':
                    new_settings['max_channels'] = float('inf')
                else:
                    new_settings['max_channels'] = int(value)
            
            elif 'количество последних постов:' in line_lower:
                value = line.split(':')[1].strip()
                if '-' in value:
                    min_val, max_val = map(int, value.split('-'))
                    new_settings['posts_range'] = (min_val, max_val)
                else:
                    posts_num = int(value)
                    new_settings['posts_range'] = (posts_num, posts_num)
            
            elif 'задержка между действиями:' in line_lower:
                value = line.split(':')[1].strip()
                if value == '_':
                    new_settings['delay_range'] = (0, 0)
                elif '-' in value:
                    clean_value = value.replace('секунд', '').replace('секундах', '').strip()
                    parts = clean_value.split('-')
                    min_val, max_val = map(int, parts)
                    new_settings['delay_range'] = (min_val, max_val)
                else:
                    delay = int(value.replace('секунд', '').replace('секундах', '').strip())
                    new_settings['delay_range'] = (delay, delay)
            
            elif 'отслеживание новых постов:' in line_lower:
                value = line.split(':')[1].strip().lower()
                new_settings['track_new_posts'] = value in ['да', 'yes', 'true', '1', 'включено']
        
        if new_settings:
            bot_data['settings'].update(new_settings)
            asyncio.create_task(save_bot_state())
            
            # Обновляем сообщение с новыми параметрами
            await update.message.reply_text("✅ Настройки успешно обновлены!")
            await settings_menu_updated(update, context)
        else:
            await show_error_with_back_button(update, context,
                "❌ Неверный формат. Используйте формат:\n\n"
                "Максимальное количество каналов: 150\n"
                "Количество последних постов: 1-5\n"
                "Задержка между действиями: 20-1000\n"
                "Отслеживание новых постов: да"
            )
    
    except Exception as e:
        logger.error(f"Ошибка парсинга настроек: {e}")
        await show_error_with_back_button(update, context, "❌ Ошибка в формате настроек. Проверьте правильность ввода.")

async def settings_menu(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Меню настроек масслукинга"""
    query = update.callback_query
    await query.answer()
    
    user_id = query.from_user.id
    if not check_access(user_id):
        await query.answer("❌ Доступ ограничен", show_alert=True)
        return
    
    bot_data['user_states'][user_id] = 'settings'
    asyncio.create_task(save_bot_state())
    
    settings = bot_data['settings']
    
    max_channels = "∞" if settings['max_channels'] == float('inf') else str(settings['max_channels'])
    posts_range = f"{settings['posts_range'][0]}-{settings['posts_range'][1]}" if settings['posts_range'][0] != settings['posts_range'][1] else str(settings['posts_range'][0])
    delay_range = "_" if settings['delay_range'] == (0, 0) else f"{settings['delay_range'][0]}-{settings['delay_range'][1]}"
    track_new_posts = "да" if settings.get('track_new_posts', False) else "нет"
    
    message_text = f"""⚙️ Параметры масслукинга

📊 Текущие параметры:

🎯 Максимальное количество каналов для масслукинга: {max_channels}

📝 Количество последних постов для комментариев и реакций: {posts_range}

⏱️ Задержка между действиями: {delay_range} секунд

🔄 Отслеживание новых постов: {track_new_posts}

Для смены параметров отправьте сообщение с параметрами в следующем формате:

Максимальное количество каналов: число или ∞ для неограниченного количества

Количество последних постов: число минимум-максимум фиксированное число (отправка комментариев под фиксированное количество последних постов)

Задержка между действиями: минимум-максимум секунд или _ для отключения задержки (отключать задержку категорически не рекомендуется)

Отслеживание новых постов: да/нет

🔧 Пример:

<code>Максимальное количество каналов: 150
Количество последних постов: 1-5
Задержка между действиями: 20-1000
Отслеживание новых постов: да</code>"""
    
    keyboard = [[get_back_button()]]
    reply_markup = InlineKeyboardMarkup(keyboard)
    
    # Сохраняем ID сообщения для последующего редактирования
    edited_message = await query.edit_message_text(message_text, reply_markup=reply_markup, parse_mode='HTML')
    context.user_data['settings_message_id'] = edited_message.message_id
    context.user_data['setup_step'] = 'settings'

async def settings_menu_updated(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Обновленное меню настроек с актуальными параметрами"""
    user_id = update.effective_user.id
    bot_data['user_states'][user_id] = 'settings'
    asyncio.create_task(save_bot_state())
    
    settings = bot_data['settings']
    
    max_channels = "∞" if settings['max_channels'] == float('inf') else str(settings['max_channels'])
    posts_range = f"{settings['posts_range'][0]}-{settings['posts_range'][1]}" if settings['posts_range'][0] != settings['posts_range'][1] else str(settings['posts_range'][0])
    delay_range = "_" if settings['delay_range'] == (0, 0) else f"{settings['delay_range'][0]}-{settings['delay_range'][1]}"
    track_new_posts = "да" if settings.get('track_new_posts', False) else "нет"
    
    message_text = f"""⚙️ Параметры масслукинга

📊 Текущие параметры:

🎯 Максимальное количество каналов для масслукинга: {max_channels}

📝 Количество последних постов для комментариев и реакций: {posts_range}

⏱️ Задержка между действиями: {delay_range} секунд

🔄 Отслеживание новых постов: {track_new_posts}

Для смены параметров отправьте сообщение с параметрами в следующем формате:

Максимальное количество каналов: число или ∞ для неограниченного количества

Количество последних постов: число минимум-максимум фиксированное число (отправка комментариев под фиксированное количество последних постов)

Задержка между действиями: минимум-максимум секунд или _ для отключения задержки (отключать задержку категорически не рекомендуется)

Отслеживание новых постов: да/нет

🔧 Пример:

<code>Максимальное количество каналов: 150
Количество последних постов: 1-5
Задержка между действиями: 20-1000
Отслеживание новых постов: да</code>"""
    
    keyboard = [[get_back_button()]]
    reply_markup = InlineKeyboardMarkup(keyboard)
    
    # Проверяем, есть ли предыдущее сообщение настроек для редактирования
    if 'settings_message_id' in context.user_data:
        try:
            await context.bot.edit_message_text(
                chat_id=update.effective_chat.id,
                message_id=context.user_data['settings_message_id'],
                text=message_text,
                reply_markup=reply_markup,
                parse_mode='HTML'
            )
        except Exception as e:
            # Если редактирование не удалось, отправляем новое сообщение
            sent_message = await update.message.reply_text(message_text, reply_markup=reply_markup, parse_mode='HTML')
            context.user_data['settings_message_id'] = sent_message.message_id
    else:
        # Отправляем новое сообщение и сохраняем его ID
        sent_message = await update.message.reply_text(message_text, reply_markup=reply_markup, parse_mode='HTML')
        context.user_data['settings_message_id'] = sent_message.message_id
    
    context.user_data['setup_step'] = 'settings'

async def handle_text_message(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Обработка текстовых сообщений"""
    user_id = update.effective_user.id
    if not check_access(user_id):
        await show_error_with_back_button(update, context, "❌ Доступ ограничен")
        return
    
    text = update.message.text
    step = context.user_data.get('setup_step')
    current_state = bot_data['user_states'].get(user_id, 'main_menu')
    
    logger.info(f"Обработка текстового сообщения от пользователя {user_id}: step={step}, state={current_state}, text={text[:50]}...")
    
    if current_state == 'prompts_menu' and (text.startswith("Промт для комментариев:") or text.startswith("Промт для анализа:") or ("Промт для комментариев:" in text and "Промт для анализа:" in text)):
        await handle_prompt_change(update, context, text)
        return
    
    if step == 'api_id':
        logger.info(f"Обработка API ID: {text}")
        if text.isdigit() and len(text) >= 6:  # API ID обычно 6-8 цифр
            user_session = await db.load_user_session(user_id)
            logger.info(f"Текущая сессия перед сохранением API ID: {user_session}")
            
            if 'client_config' not in user_session:
                user_session['client_config'] = {}
            user_session['client_config']['api_id'] = text
            
            # ИСПРАВЛЕНИЕ: сохраняем state отдельно чтобы не перезаписывать в bot_data
            user_session['state'] = 'api_hash'
            await db.save_user_session(user_id, user_session)
            logger.info(f"API ID сохранен: {text}")
            
            # Проверяем что сохранилось
            saved_session = await db.load_user_session(user_id)
            logger.info(f"Проверка сохранения API ID: {saved_session}")
            
            keyboard = [[get_back_button()]]
            reply_markup = InlineKeyboardMarkup(keyboard)
            
            await update.message.reply_text(
                "✅ API ID сохранен.\n\nТеперь отправьте API Hash:",
                reply_markup=reply_markup
            )
            context.user_data['setup_step'] = 'api_hash'
            bot_data['user_states'][user_id] = 'api_hash'
            # НЕ ВЫЗЫВАЕМ save_bot_state() чтобы избежать перезаписи
        else:
            logger.warning(f"Неверный API ID: {text}")
            await show_error_with_back_button(update, context, "❌ API ID должен состоять из 6-8 цифр. Попробуйте еще раз:")
    
    elif step == 'api_hash':
        logger.info(f"Обработка API Hash: {text[:10]}...")
        if len(text) >= 20:  # API Hash обычно длинная строка
            user_session = await db.load_user_session(user_id)
            logger.info(f"Текущая сессия перед сохранением API Hash: {user_session}")
            
            # ИСПРАВЛЕНИЕ: проверяем что client_config существует и содержит api_id
            if 'client_config' not in user_session:
                user_session['client_config'] = {}
            
            # Проверяем что api_id не потерялся
            if 'api_id' not in user_session['client_config']:
                logger.error("API ID потерялся! Начинаем заново.")
                await show_error_with_back_button(update, context, "❌ API ID потерялся. Начните настройку заново.")
                return
            
            # Сохраняем API Hash к существующему api_id
            user_session['client_config']['api_hash'] = text
            user_session['state'] = 'phone'
            await db.save_user_session(user_id, user_session)
            logger.info(f"API Hash сохранен")
            
            # Проверяем что сохранилось
            saved_session = await db.load_user_session(user_id)
            logger.info(f"Проверка сохранения API Hash: {saved_session}")
            
            keyboard = [[get_back_button()]]
            reply_markup = InlineKeyboardMarkup(keyboard)
            
            await update.message.reply_text(
                "✅ API Hash сохранен.\n\nОтправьте номер телефона в международном формате (например, +79123456789):",
                reply_markup=reply_markup
            )
            context.user_data['setup_step'] = 'phone'
            bot_data['user_states'][user_id] = 'phone'
        else:
            logger.warning(f"Неверный API Hash (длина {len(text)})")
            await show_error_with_back_button(update, context, "❌ API Hash слишком короткий. Попробуйте еще раз:")
    
    elif step == 'phone':
        logger.info(f"Обработка номера телефона: {text}")
        if re.match(r'^\+\d{10,15}$', text):
            user_session = await db.load_user_session(user_id)
            logger.info(f"Текущая сессия перед сохранением телефона: {user_session}")
            
            # ИСПРАВЛЕНИЕ: проверяем что client_config существует и содержит api_id и api_hash
            if 'client_config' not in user_session:
                user_session['client_config'] = {}
            
            # Проверяем что api_id и api_hash не потерялись
            if 'api_id' not in user_session['client_config'] or 'api_hash' not in user_session['client_config']:
                logger.error(f"API данные потерялись! Текущая конфигурация: {user_session.get('client_config', {})}")
                await show_error_with_back_button(update, context, "❌ API данные потерялись. Начните настройку заново.")
                return
            
            # Сохраняем телефон к существующим api_id и api_hash
            user_session['client_config']['phone'] = text
            await db.save_user_session(user_id, user_session)
            logger.info(f"Номер телефона сохранен")
            
            # Перезагружаем сессию для финальной проверки
            final_session = await db.load_user_session(user_id)
            client_config = final_session.get('client_config', {})
            logger.info(f"Финальная конфигурация перед отправкой кода: {client_config}")
            
            # Проверяем что все данные есть перед отправкой кода
            if not client_config.get('api_id') or not client_config.get('api_hash'):
                logger.error(f"Отсутствуют API данные при отправке кода. Config: {client_config}")
                await show_error_with_back_button(update, context, "❌ Отсутствуют API данные. Начните настройку заново.")
                return
                
            asyncio.create_task(send_telegram_code(update, context, text, client_config))
        else:
            logger.warning(f"Неверный формат номера телефона: {text}")
            await show_error_with_back_button(update, context, "❌ Неверный формат номера телефона. Используйте международный формат (+79123456789)")
    
    elif step == 'password':
        asyncio.create_task(handle_telegram_password(update, context, text))
    
    elif step == 'settings':
        await parse_settings(update, context, text)
    
    elif step == 'manual_keywords':
        await handle_manual_keywords(update, context, text)

async def send_telegram_code(update: Update, context: ContextTypes.DEFAULT_TYPE, phone: str, config: dict):
    """Отправка кода Telegram для конкретного пользователя"""
    user_id = update.effective_user.id
    
    try:
        # Проверяем наличие всех необходимых данных
        if not config.get('api_id') or not config.get('api_hash'):
            await show_error_with_back_button(update, context, "❌ Отсутствуют API ID или API Hash. Начните настройку заново.")
            return
        
        # Проверяем что api_id является числом
        try:
            api_id = int(config['api_id'])
        except (ValueError, TypeError):
            await show_error_with_back_button(update, context, "❌ Неверный формат API ID. Начните настройку заново.")
            return
        
        # Проверяем существующий клиент
        existing_client = await get_user_telethon_client(user_id)
        if existing_client:
            await existing_client.disconnect()
            session_data = await db.load_user_session(user_id)
            session_data['client_config'] = config
            session_data['has_active_client'] = True
            await db.save_user_session(user_id, session_data)
            await update_user_session_cache(user_id)
            
            await update.message.reply_text("✅ Пользователь уже авторизован!")
            await show_main_menu(update, context, edit=False)
            return
        
        session_file = f'user_session_{user_id}'
        loop = asyncio.get_event_loop()
        
        client = TelegramClient(
            session_file,
            api_id,
            config['api_hash'],
            loop=loop,
            timeout=30,
            retry_delay=1,
            flood_sleep_threshold=60
        )
        
        await client.connect()
        
        if await client.is_user_authorized():
            await client.disconnect()
            session_data = await db.load_user_session(user_id)
            session_data['client_config'] = config
            session_data['has_active_client'] = True
            await db.save_user_session(user_id, session_data)
            await update_user_session_cache(user_id)
            
            await update.message.reply_text("✅ Пользователь уже авторизован!")
            await show_main_menu(update, context, edit=False)
            return
        
        result = await client.send_code_request(phone)
        context.user_data['phone_code_hash'] = result.phone_code_hash
        context.user_data['client'] = client
        context.user_data['phone'] = phone
        context.user_data['config'] = config
        
        reply_markup = get_code_input_keyboard()
        
        await update.message.reply_text(
            "📱 Код подтверждения отправлен на ваш номер.\n\n"
            "Введенный код: \n\n"
            "Введите код с помощью кнопок ниже:",
            reply_markup=reply_markup
        )
        
        context.user_data['setup_step'] = 'code'
        context.user_data['entered_code'] = ''
        bot_data['user_states'][user_id] = 'code'
        asyncio.create_task(save_bot_state())
        
    except Exception as e:
        logger.error(f"Ошибка отправки кода для пользователя {user_id}: {e}")
        if 'client' in locals():
            try:
                await client.disconnect()
            except:
                pass
        await show_error_with_back_button(update, context, f"❌ Ошибка отправки кода: {e}\n\nПроверьте правильность API ID и API Hash.")

async def resend_telegram_code(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Повторная отправка кода подтверждения"""
    try:
        phone = context.user_data.get('phone')
        config = context.user_data.get('config')
        
        if not phone or not config:
            await update.callback_query.answer("❌ Данные для отправки кода не найдены", show_alert=True)
            return
        
        client = context.user_data.get('client')
        if not client:
            await update.callback_query.answer("❌ Клиент недоступен для повторной отправки", show_alert=True)
            return
        
        await update.callback_query.answer("📞 Отправляем код повторно...")
        
        try:
            # Отправляем код повторно используя существующий клиент
            result = await client.send_code_request(phone)
            context.user_data['phone_code_hash'] = result.phone_code_hash
            context.user_data['entered_code'] = ''  # Очищаем введенный код
            
            reply_markup = get_code_input_keyboard()
            
            short_message = ("📱 Код отправлен повторно!\n\n"
                           "Введенный код: \n\n"
                           "Введите код кнопками ниже:")
           
            await update.callback_query.edit_message_text(
                short_message,
                reply_markup=reply_markup
            )
            
            logger.info(f"Код повторно отправлен на номер {phone} используя существующий клиент")
            
        except Exception as send_error:
            logger.error(f"Ошибка при повторной отправке кода: {send_error}")
            # Если ошибка отправки, показываем сообщение об ошибке
            await update.callback_query.edit_message_text(
                "❌ Ошибка повторной отправки кода.\nПопробуйте позже.",
                reply_markup=get_code_input_keyboard()
            )
        
    except Exception as e:
        logger.error(f"Ошибка повторной отправки кода: {e}")
        # Проверяем если ошибка связана с длиной сообщения
        if "Message_too_long" in str(e):
            try:
                # Отправляем сообщение
                await update.callback_query.edit_message_text(
                    "❌ Ошибка отправки",
                    reply_markup=get_code_input_keyboard()
                )
            except:
                await update.callback_query.answer("❌ Ошибка повторной отправки", show_alert=True)
        else:
            await update.callback_query.answer(f"❌ Ошибка: {str(e)[:100]}", show_alert=True)

async def handle_telegram_password(update: Update, context: ContextTypes.DEFAULT_TYPE, password: str):
    """Обработка пароля Telegram для конкретного пользователя"""
    user_id = update.effective_user.id
    
    try:
        client = context.user_data.get('client')
        if not client:
            await show_error_with_back_button(update, context, "❌ Сессия истекла. Начните настройку заново.")
            return
        
        logger.info(f"Попытка авторизации с паролем для пользователя {user_id}")
        
        await client.sign_in(password=password)
        
        config = context.user_data.get('config', {})
        config['password'] = password
        
        logger.info(f"Авторизация успешна для пользователя {user_id}, сохраняем конфигурацию")
        logger.info(f"Конфигурация для сохранения: {config}")
        
        # ИСПРАВЛЕНИЕ: правильно сохраняем данные пользователя
        session_data = {
            'client_config': config,
            'has_active_client': True,
            'state': 'main_menu'
        }
        
        await db.save_user_session(user_id, session_data)
        logger.info(f"Конфигурация клиента сохранена для пользователя {user_id}")
        
        # ИСПРАВЛЕНИЕ: обновляем кеш ПОСЛЕ сохранения
        user_sessions_cache[user_id] = session_data
        
        # Проверяем что сохранилось
        saved_session = await db.load_user_session(user_id)
        logger.info(f"Проверка сохранения конфигурации клиента: {saved_session}")
        
        await client.disconnect()
        
        await update.message.reply_text("✅ Успешный вход в аккаунт!")
        context.user_data.clear()
        await show_main_menu(update, context, edit=False)
        
    except PasswordHashInvalidError:
        logger.warning(f"Неверный пароль для пользователя {user_id}")
        await show_error_with_back_button(update, context, "❌ Неверный пароль. Попробуйте еще раз:")
    except Exception as e:
        logger.error(f"Ошибка входа с паролем для пользователя {user_id}: {e}")
        await show_error_with_back_button(update, context, f"❌ Ошибка входа: {e}")
        
        if 'client' in context.user_data:
            try:
                await context.user_data['client'].disconnect()
            except:
                pass
            context.user_data.clear()

async def handle_manual_keywords(update: Update, context: ContextTypes.DEFAULT_TYPE, text: str):
    """Обработка ключевых слов для ручной настройки"""
    user_id = update.effective_user.id
    user_data = bot_data['pending_manual_setup'].get(user_id, {})
    keywords = [kw.strip() for kw in text.split(',') if kw.strip()]
    user_data['keywords'] = keywords
    bot_data['pending_manual_setup'][user_id] = user_data
    
    if user_data.get('topics'):
        bot_data['settings']['keywords'] = keywords
        bot_data['settings']['topics'] = user_data['topics']
        bot_data['settings']['target_channel'] = ''
        asyncio.create_task(save_bot_state())
        
        await update.message.reply_text(
            f"✅ Настройки сохранены!\n\n"
            f"Ключевые слова: {', '.join(keywords)}\n"
            f"Темы: {', '.join(user_data['topics'])}"
        )
        
        del bot_data['pending_manual_setup'][user_id]
        context.user_data.clear()
        
        await show_main_menu(update, context, edit=False)
    else:
        await update.message.reply_text(
            "✅ Ключевые слова сохранены. Теперь выберите темы и нажмите 'Готово ✅'"
        )

async def handle_code_input(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Обработка ввода кода подтверждения без таймаутов"""
    query = update.callback_query
    await query.answer()
    
    user_id = query.from_user.id
    if not context.user_data.get('setup_step') == 'code':
        return
    
    data = query.data
    
    if data.startswith('code_'):
        action = data.split('_')[1]
        
        if action == 'delete':
            if context.user_data['entered_code']:
                context.user_data['entered_code'] = context.user_data['entered_code'][:-1]
        
        elif action == 'send':
            code = context.user_data['entered_code']
            if len(code) >= 5:
                # Долгая операция авторизации - выносим в отдельную задачу
                asyncio.create_task(process_telegram_code(update, context, code))
                return
            else:
                await query.answer("Код должен содержать минимум 5 цифр", show_alert=True)
                return
        
        elif action == 'resend':
            # Обрабатываем повторную отправку кода
            await resend_telegram_code(update, context)
            return
        
        elif action == 'cancel':
            if 'client' in context.user_data:
                try:
                    await context.user_data['client'].disconnect()
                except:
                    pass
            context.user_data.clear()
            await query.edit_message_text("❌ Настройка аккаунта отменена.")
            await show_main_menu(update, context, edit=False)
            return
        
        elif action.isdigit():
            if len(context.user_data['entered_code']) < 10:
                context.user_data['entered_code'] += action
       
        entered_code = context.user_data['entered_code']
        await query.edit_message_text(
            f"📱 Код подтверждения отправлен на ваш номер.\n\n"
            f"Введенный код: {entered_code}\n\n"
            f"Введите код с помощью кнопок ниже:",
            reply_markup=get_code_input_keyboard()
        )

async def process_telegram_code(update: Update, context: ContextTypes.DEFAULT_TYPE, code: str):
    """Обработка кода авторизации для конкретного пользователя"""
    user_id = update.effective_user.id
    
    try:
        client = context.user_data['client']
        phone_code_hash = context.user_data['phone_code_hash']
        config = context.user_data.get('config', {})
        
        logger.info(f"🔍 DEBUG: Попытка авторизации с кодом для пользователя {user_id}")
        logger.info(f"🔍 DEBUG: Конфигурация для авторизации: {config}")
        
        await client.sign_in(
            phone=config['phone'],
            code=code,
            phone_code_hash=phone_code_hash
        )
        
        logger.info(f"✅ DEBUG: Авторизация по коду успешна для пользователя {user_id}")
        
        # ИСПРАВЛЕНИЕ: правильно сохраняем данные пользователя в БД
        session_data = {
            'client_config': config,
            'has_active_client': True,
            'state': 'main_menu'
        }
        
        logger.info(f"🔍 DEBUG: Сохраняем в БД для пользователя {user_id}: {session_data}")
        await db.save_user_session(user_id, session_data)
        logger.info(f"✅ DEBUG: Данные сохранены в БД")
        
        # ИСПРАВЛЕНИЕ: обновляем кеш ПОСЛЕ сохранения в БД
        user_sessions_cache[user_id] = session_data.copy()
        logger.info(f"🔍 DEBUG: Кеш обновлен: {user_sessions_cache[user_id]}")
        
        # Проверяем что сохранилось
        saved_session = await db.load_user_session(user_id)
        logger.info(f"🔍 DEBUG: Проверяем что сохранилось в БД: {saved_session}")
        
        # Проверяем файл сессии
        import os
        session_file = f'user_session_{user_id}.session'
        session_file_exists = os.path.exists(session_file)
        logger.info(f"🔍 DEBUG: Файл сессии {session_file} существует: {session_file_exists}")
        
        await client.disconnect()
        
        await update.callback_query.edit_message_text("✅ Успешный вход в аккаунт!")
        context.user_data.clear()
        
        await show_main_menu(update, context, edit=False)
        return
        
    except SessionPasswordNeededError:
        logger.info(f"🔍 DEBUG: Пользователю {user_id} требуется двухфакторная аутентификация")
        keyboard = [[get_back_button()]]
        reply_markup = InlineKeyboardMarkup(keyboard)
        
        await update.callback_query.edit_message_text(
            "🔐 Требуется пароль двухфакторной аутентификации.\n\n"
            "Отправьте ваш пароль:",
            reply_markup=reply_markup
        )
        context.user_data['setup_step'] = 'password'
        bot_data['user_states'][user_id] = 'password'
        return
        
    except PhoneCodeInvalidError:
        logger.warning(f"⚠️ DEBUG: Неверный код для пользователя {user_id}")
        await update.callback_query.edit_message_text(
            "❌ Неверный код. Попробуйте еще раз.\n\n"
            f"Введенный код: {context.user_data['entered_code']}\n\n"
            "Введите код с помощью кнопок ниже:",
            reply_markup=get_code_input_keyboard()
        )
        context.user_data['entered_code'] = ''
        return
        
    except Exception as e:
        logger.error(f"❌ DEBUG: Ошибка входа с кодом для пользователя {user_id}: {e}")
        import traceback
        logger.error(f"❌ DEBUG: Полный traceback: {traceback.format_exc()}")
        keyboard = [[get_back_button()]]
        reply_markup = InlineKeyboardMarkup(keyboard)
        await update.callback_query.edit_message_text(f"❌ Ошибка входа: {e}", reply_markup=reply_markup)

async def target_channel_setup(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Настройка целевого канала"""
    query = update.callback_query
    await query.answer()
    
    user_id = query.from_user.id
    if not check_access(user_id):
        await query.answer("❌ Доступ ограничен", show_alert=True)
        return
    
    bot_data['user_states'][user_id] = 'target_channel'
    asyncio.create_task(save_bot_state())
    
    settings = bot_data['settings']
    
    current_channel = settings.get('target_channel', 'Не выбран')
    topics_text = ', '.join([f'"{topic}"' for topic in settings['topics']])
    keywords_text = ', '.join(settings['keywords'])
    
    message_text = f"""📺 Выбор целевого канала

Вы можете выбрать канал и бот будет рассылать комментарии и ставить реакции похожим каналам. Похожие каналы определяются по ключевым словам и тематике.

{'Текущий канал: ' + current_channel if current_channel != 'Не выбран' else ''}

Тематика: {topics_text}

Ключевые слова для поиска: {keywords_text}"""
    
    keyboard = [
        [InlineKeyboardButton("📺 Выбрать канал", callback_data="select_channel")],
        [InlineKeyboardButton("✏️ Настроить вручную", callback_data="manual_setup")],
        [get_back_button()]
    ]
    
    reply_markup = InlineKeyboardMarkup(keyboard)
    
    await query.edit_message_text(message_text, reply_markup=reply_markup)

async def select_channel(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Показать инструкцию по выбору канала"""
    query = update.callback_query
    await query.answer()
    
    user_id = query.from_user.id
    
    # Сохраняем состояние пользователя
    bot_data['user_states'][user_id] = 'channel_selection'
    context.user_data['awaiting_channel'] = True
    
    keyboard = [
        [KeyboardButton(
            "📺 Поделиться каналом",
            request_chat={
                'request_id': 1,
                'chat_is_channel': True
            }
        )],
        [InlineKeyboardButton("◀️ Назад", callback_data="target_channel")],
        [InlineKeyboardButton("🏠 Главное меню", callback_data="main_menu")]
    ]
    
    reply_markup = ReplyKeyboardMarkup([[keyboard[0][0]]], one_time_keyboard=True, resize_keyboard=True)
    inline_markup = InlineKeyboardMarkup([[keyboard[1][0], keyboard[2][0]]])
    
    await query.edit_message_text(
        "📺 Выбор канала\n\n"
        "Нажмите кнопку ниже, чтобы выбрать канал для анализа.\n"
        "После выбора канал будет проанализирован с помощью GPT-4.",
        reply_markup=inline_markup
    )
    
    channel_selection_msg = await context.bot.send_message(
        chat_id=user_id,
        text="👇 Нажмите кнопку для выбора канала:",
        reply_markup=reply_markup
    )
    
    # Сохраняем ID сообщения для последующего удаления
    context.user_data['channel_selection_message_id'] = channel_selection_msg.message_id

async def manual_setup(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Ручная настройка тем и ключевых слов"""
    query = update.callback_query
    await query.answer()
    
    user_id = query.from_user.id
    if not check_access(user_id):
        await query.answer("❌ Доступ ограничен", show_alert=True)
        return
    
    bot_data['user_states'][user_id] = 'manual_setup'
    asyncio.create_task(save_bot_state())
    
    bot_data['pending_manual_setup'][user_id] = {'topics': [], 'keywords': []}
    
    keyboard = []
    for i in range(0, len(AVAILABLE_TOPICS), 4):
        row = []
        for j in range(4):
            if i + j < len(AVAILABLE_TOPICS):
                topic = AVAILABLE_TOPICS[i + j]
                row.append(InlineKeyboardButton(topic, callback_data=f"topic_{i+j}"))
        keyboard.append(row)
    
    keyboard.append([InlineKeyboardButton("✅ Готово", callback_data="topics_done")])
    keyboard.append([get_back_button()])
    
    reply_markup = InlineKeyboardMarkup(keyboard)
    
    await query.edit_message_text(
        "✏️ Ручная настройка\n\n"
        "Пожалуйста, отправьте список ключевых слов через запятую и выберите темы из списка ниже. Нажмите 'Готово ✅' когда закончите.\n\n"
        "📝 Отправьте ключевые слова одним сообщением:",
        reply_markup=reply_markup
    )
    
    context.user_data['setup_step'] = 'manual_keywords'
    bot_data['user_states'][user_id] = 'topic_selection'
    asyncio.create_task(save_bot_state())

async def handle_topic_selection(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Обработка выбора тем"""
    query = update.callback_query
    await query.answer()
    
    user_id = query.from_user.id
    data = query.data
    
    if data.startswith('topic_'):
        topic_index = int(data.split('_')[1])
        topic = AVAILABLE_TOPICS[topic_index]
        
        user_data = bot_data['pending_manual_setup'].get(user_id, {'topics': [], 'keywords': []})
        
        if topic in user_data['topics']:
            user_data['topics'].remove(topic)
        else:
            user_data['topics'].append(topic)
        
        bot_data['pending_manual_setup'][user_id] = user_data
        
        keyboard = []
        for i in range(0, len(AVAILABLE_TOPICS), 4):
            row = []
            for j in range(4):
                if i + j < len(AVAILABLE_TOPICS):
                    topic_name = AVAILABLE_TOPICS[i + j]
                    if topic_name in user_data['topics']:
                        display_name = f"✅ {topic_name}"
                    else:
                        display_name = topic_name
                    row.append(InlineKeyboardButton(display_name, callback_data=f"topic_{i+j}"))
            keyboard.append(row)
        
        keyboard.append([InlineKeyboardButton("✅ Готово", callback_data="topics_done")])
        keyboard.append([get_back_button()])
        
        reply_markup = InlineKeyboardMarkup(keyboard)
        
        await query.edit_message_reply_markup(reply_markup=reply_markup)
    
    elif data == 'topics_done':
        user_data = bot_data['pending_manual_setup'].get(user_id, {'topics': [], 'keywords': []})
        
        if not user_data['topics']:
            await query.answer("❌ Выберите хотя бы одну тему", show_alert=True)
            return
        
        if user_data.get('keywords'):
            bot_data['settings']['keywords'] = user_data['keywords']
            bot_data['settings']['topics'] = user_data['topics']
            bot_data['settings']['target_channel'] = ''
            asyncio.create_task(save_bot_state())
            
            keyboard = [[get_back_button()]]
            reply_markup = InlineKeyboardMarkup(keyboard)
            
            await query.edit_message_text(
                f"✅ Настройки сохранены!\n\n"
                f"🔑 Ключевые слова: {', '.join(user_data['keywords'])}\n\n"
                f"🏷️ Темы: {', '.join(user_data['topics'])}",
                reply_markup=reply_markup
            )
            
            del bot_data['pending_manual_setup'][user_id]
            context.user_data.clear()
        else:
            keyboard = [[get_back_button()]]
            reply_markup = InlineKeyboardMarkup(keyboard)
            
            await query.edit_message_text(
                f"✅ Темы выбраны: {', '.join(user_data['topics'])}\n\n"
                "📝 Теперь отправьте список ключевых слов через запятую:",
                reply_markup=reply_markup
            )

async def toggle_run(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Запуск/остановка рассылки для конкретного пользователя"""
    query = update.callback_query
    await query.answer()
    
    user_id = query.from_user.id
    if not check_access(user_id):
        await query.answer("❌ Доступ ограничен", show_alert=True)
        return
    
    # ИСПРАВЛЕНИЕ: используем кеш, но проверяем его актуальность
    if user_id in user_sessions_cache:
        user_session = user_sessions_cache[user_id].copy()
        logger.info(f"Используем данные из кеша для пользователя {user_id}: {user_session}")
    else:
        user_session = await db.load_user_session(user_id)
        user_sessions_cache[user_id] = user_session
        logger.info(f"Загружаем данные из БД для пользователя {user_id}: {user_session}")
    
    is_running = user_session.get('is_running', False)
    has_active_client = user_session.get('has_active_client', False)
    client_config = user_session.get('client_config', {})
    
    logger.info(f"Toggle run для пользователя {user_id}: is_running={is_running}, has_active_client={has_active_client}")
    logger.info(f"Client config присутствует: {bool(client_config)}")
        
    if is_running:
        user_session['is_running'] = False
        await db.save_user_session(user_id, user_session)
        await update_user_session_cache(user_id)
        
        keyboard = [[get_back_button()]]
        reply_markup = InlineKeyboardMarkup(keyboard)
        
        await query.edit_message_text("⏹️ Рассылка остановлена", reply_markup=reply_markup)
        
        try:
            await stop_user_services(user_id)
        except Exception as e:
            logger.error(f"Ошибка остановки сервисов для пользователя {user_id}: {e}")
    else:
        # ИСПРАВЛЕНИЕ: убираем лишние проверки, которые блокируют запуск
        if not has_active_client or not client_config:
            logger.warning(f"Пользователь {user_id} не имеет активного клиента или конфигурации")
            keyboard = [[get_back_button()]]
            reply_markup = InlineKeyboardMarkup(keyboard)
            await query.edit_message_text("❌ Сначала добавьте аккаунт", reply_markup=reply_markup)
            return
        
        # Проверяем наличие всех необходимых полей в конфигурации
        required_fields = ['api_id', 'api_hash', 'phone']
        missing_fields = [field for field in required_fields if not client_config.get(field)]
        
        if missing_fields:
            logger.warning(f"Пользователь {user_id} имеет неполную конфигурацию. Отсутствуют поля: {missing_fields}")
            keyboard = [[get_back_button()]]
            reply_markup = InlineKeyboardMarkup(keyboard)
            await query.edit_message_text("❌ Конфигурация аккаунта неполная. Добавьте аккаунт заново", reply_markup=reply_markup)
            return
        
        user_session['is_running'] = True
        await db.save_user_session(user_id, user_session)
        await update_user_session_cache(user_id)
        
        keyboard = [[get_back_button()]]
        reply_markup = InlineKeyboardMarkup(keyboard)
        
        await query.edit_message_text("▶️ Рассылка запущена", reply_markup=reply_markup)
        
        try:
            await start_user_services(user_id)
        except Exception as e:
            logger.error(f"Ошибка запуска сервисов для пользователя {user_id}: {e}")

async def get_user_telethon_client(user_id: int) -> Optional[TelegramClient]:
    """Получение Telethon клиента для конкретного пользователя"""
    try:
        # ИСПРАВЛЕНИЕ: загружаем свежие данные из БД, а не из кеша
        session_data = await db.load_user_session(user_id)
        client_config = session_data.get('client_config')
        
        if not client_config:
            logger.debug(f"Нет конфигурации клиента для пользователя {user_id}")
            return None
            
        if not all(key in client_config for key in ['api_id', 'api_hash', 'phone']):
            logger.debug(f"Неполная конфигурация клиента для пользователя {user_id}")
            return None
        
        # Проверяем что api_id является числом
        try:
            api_id = int(client_config['api_id'])
        except (ValueError, TypeError):
            logger.error(f"Неверный формат API ID для пользователя {user_id}")
            return None
            
        session_file = f'user_session_{user_id}'
        loop = asyncio.get_event_loop()
        
        client = TelegramClient(
            session_file,
            api_id,
            client_config['api_hash'],
            loop=loop,
            timeout=30,
            retry_delay=1,
            flood_sleep_threshold=60
        )
        
        await client.connect()
        
        if await client.is_user_authorized():
            logger.info(f"Клиент для пользователя {user_id} создан и авторизован")
            return client
        else:
            await client.disconnect()
            logger.debug(f"Клиент для пользователя {user_id} не авторизован")
            return None
            
    except Exception as e:
        logger.error(f"Ошибка получения клиента для пользователя {user_id}: {e}")
        return None

async def start_user_services(user_id: int):
    """Запуск сервисов для конкретного пользователя"""
    try:
        # ИСПРАВЛЕНИЕ: загружаем свежие данные из БД
        user_session = await db.load_user_session(user_id)
        client_config = user_session.get('client_config', {})
        
        if not client_config or not all(client_config.get(field) for field in ['api_id', 'api_hash', 'phone']):
            logger.error(f"Неполная конфигурация клиента для пользователя {user_id}")
            return
        
        # Создаем клиент для пользователя
        client = await get_user_telethon_client(user_id)
        if not client:
            logger.error(f"Не удалось создать клиент для пользователя {user_id}")
            return
        
        # Получаем настройки пользователя или используем глобальные
        user_settings = user_session.get('settings', bot_data['settings'])
        
        try:
            import channel_search_engine
            await channel_search_engine.start_search(user_settings, client, user_id)
            logger.info(f"Поисковик запущен для пользователя {user_id}")
        except Exception as e:
            logger.error(f"Ошибка запуска поисковика для пользователя {user_id}: {e}")
        
        try:
            import masslooker
            asyncio.create_task(masslooker.start_masslooking(client, user_settings, user_id))
            logger.info(f"Масслукер запущен для пользователя {user_id}")
        except Exception as e:
            logger.error(f"Ошибка запуска масслукера для пользователя {user_id}: {e}")
        
        # НЕ отключаем клиент здесь, он нужен для работы сервисов
        
    except Exception as e:
        logger.error(f"Ошибка запуска сервисов для пользователя {user_id}: {e}")

async def show_statistics(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Показать текущую статистику"""
    try:
        user_id = update.effective_user.id
        if not check_access(user_id):
            return

        stats = bot_data['statistics']
        stats_text = (
            "📊 Статистика\n\n"
            f"💬 Отправлено комментариев: {stats['comments_sent']}\n\n"
            f"📺 Обработано каналов: {stats['channels_processed']}\n\n"
            f"👍 Поставлено реакций: {stats['reactions_set']}"
        )

        keyboard = InlineKeyboardMarkup([
            [InlineKeyboardButton("Подробная статистика", callback_data="detailed_statistics")],
            [get_back_button()]
        ])

        if update.callback_query:
            # Если это обновление через callback, обновляем существующее сообщение
            message = await update.callback_query.edit_message_text(
                stats_text,
                reply_markup=keyboard,
                parse_mode=ParseMode.HTML
            )
        else:
            # Если это новое сообщение
            message = await context.bot.send_message(
                chat_id=update.effective_chat.id,
                text=stats_text,
                reply_markup=keyboard,
                parse_mode=ParseMode.HTML
            )

        # Сохраняем ID сообщения для будущих обновлений
        bot_data['active_messages']['statistics'][user_id] = {
            'message_id': message.message_id,
            'chat_id': update.effective_chat.id
        }

    except Exception as e:
        logger.error(f"Ошибка при показе статистики: {e}")
        await show_error_with_back_button(update, context, "Ошибка при показе статистики")

def update_statistics(comments=0, channels=0, reactions=0):
    """Обновление статистики с автоматическим обновлением сообщения"""
    try:
        if comments:
            bot_data['statistics']['comments_sent'] += comments
        if channels:
            bot_data['statistics']['channels_processed'] += channels
        if reactions:
            bot_data['statistics']['reactions_set'] += reactions

        # Создаем и запускаем корутину для обновления сообщений
        asyncio.create_task(update_statistics_message())
    except Exception as e:
        logger.error(f"Ошибка при обновлении статистики: {e}")

async def update_statistics_message(user_id: int = None):
    """Обновить сообщение со статистикой для конкретного пользователя или всех пользователей"""
    try:
        stats = bot_data['statistics']
        stats_text = (
            "📊 Статистика\n\n"
            f"💬 Отправлено комментариев: {stats['comments_sent']}\n\n"
            f"📺 Обработано каналов: {stats['channels_processed']}\n\n"
            f"👍 Поставлено реакций: {stats['reactions_set']}"
        )

        keyboard = InlineKeyboardMarkup([
            [InlineKeyboardButton("Подробная статистика", callback_data="detailed_statistics")],
            [get_back_button()]
        ])

        if user_id:
            users_to_update = [user_id]
        else:
            users_to_update = list(bot_data['active_messages']['statistics'].keys())

        for uid in users_to_update:
            message_data = bot_data['active_messages']['statistics'].get(uid)
            if not message_data:
                continue

            try:
                app = Application.get_running_application()
                if not app or not app.bot:
                    logger.error("Не удалось получить экземпляр бота")
                    continue

                # Проверяем существование сообщения перед обновлением
                try:
                    await app.bot.get_chat(message_data['chat_id'])
                except Exception as e:
                    logger.warning(f"Чат {message_data['chat_id']} недоступен: {e}")
                    bot_data['active_messages']['statistics'].pop(uid, None)
                    continue

                await app.bot.edit_message_text(
                    text=stats_text,
                    chat_id=message_data['chat_id'],
                    message_id=message_data['message_id'],
                    reply_markup=keyboard,
                    parse_mode=ParseMode.HTML
                )
                logger.debug(f"Обновлено сообщение статистики для пользователя {uid}")

            except Exception as e:
                error_str = str(e).lower()
                if "message is not modified" in error_str:
                    
                    continue
                elif "message to edit not found" in error_str or "chat not found" in error_str:
                    # Удаляем неактуальное сообщение из отслеживания
                    logger.warning(f"Сообщение статистики для пользователя {uid} не найдено")
                    bot_data['active_messages']['statistics'].pop(uid, None)
                else:
                    logger.error(f"Ошибка обновления сообщения статистики для пользователя {uid}: {e}")

    except Exception as e:
        logger.error(f"Общая ошибка при обновлении сообщения статистики: {e}")

async def update_settings_message(user_id: int = None):
    """Обновить сообщение с настройками для конкретного пользователя или всех пользователей"""
    try:
        settings = bot_data['settings']
        settings_text = (
            "⚙️ Текущие настройки:\n\n"
            f"🎯 Целевой канал: {settings['target_channel']}\n\n"
            f"📊 Максимум каналов: {settings['max_channels']}\n\n"
            f"📝 Диапазон постов: {settings['posts_range']}\n\n"
            f"⏱ Задержка (сек): {settings['delay_range']}\n\n"
            f"🔄 Отслеживание новых постов: {'Включено' if settings['track_new_posts'] else 'Выключено'}\n\n"
            f"📌 Темы поиска: {', '.join(settings['topics'])}\n\n"
            f"🔍 Ключевые слова: {', '.join(settings['keywords'])}"
        )

        keyboard = InlineKeyboardMarkup([
            [InlineKeyboardButton("Изменить настройки", callback_data="settings_edit")],
            [get_back_button()]
        ])

        if user_id:
            users_to_update = [user_id]
        else:
            users_to_update = list(bot_data['active_messages']['settings'].keys())

        for uid in users_to_update:
            message_data = bot_data['active_messages']['settings'].get(uid)
            if not message_data:
                continue

            try:
                app = Application.get_running_application()
                if not app or not app.bot:
                    logger.error("Не удалось получить экземпляр бота")
                    continue

                # Проверяем существование сообщения перед обновлением
                try:
                    await app.bot.get_chat(message_data['chat_id'])
                except Exception as e:
                    logger.warning(f"Чат {message_data['chat_id']} недоступен: {e}")
                    bot_data['active_messages']['settings'].pop(uid, None)
                    continue

                await app.bot.edit_message_text(
                    text=settings_text,
                    chat_id=message_data['chat_id'],
                    message_id=message_data['message_id'],
                    reply_markup=keyboard,
                    parse_mode=ParseMode.HTML
                )
                logger.debug(f"Обновлено сообщение настроек для пользователя {uid}")

            except Exception as e:
                error_str = str(e).lower()
                if "message is not modified" in error_str:
                    continue
                elif "message to edit not found" in error_str or "chat not found" in error_str:
                    # Удаляем неактуальное сообщение из отслеживания
                    logger.warning(f"Сообщение настроек для пользователя {uid} не найдено")
                    bot_data['active_messages']['settings'].pop(uid, None)
                else:
                    logger.error(f"Ошибка обновления сообщения настроек для пользователя {uid}: {e}")

    except Exception as e:
        logger.error(f"Общая ошибка при обновлении сообщения настроек: {e}")

async def show_detailed_statistics(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Показать подробную статистику с отправкой файлов без блокировок"""
    query = update.callback_query
    await query.answer()
    
    user_id = query.from_user.id
    
    # Генерируем и отправляем файлы в фоне
    asyncio.create_task(generate_and_send_statistics_files(user_id, context))

async def generate_and_send_statistics_files(user_id: int, context: ContextTypes.DEFAULT_TYPE):
    """Генерация и отправка файлов статистики в фоне"""
    try:
        await generate_detailed_statistics_files()
        
        files_to_send = [
            'processed_channels.txt',
            'queue_channels.txt', 
            'found_channels.txt'
        ]
        
        for file_name in files_to_send:
            if os.path.exists(file_name):
                try:
                    with open(file_name, 'rb') as file:
                        await context.bot.send_document(
                            chat_id=user_id,
                            document=file,
                            filename=file_name,
                            caption=f"📊 Файл статистики: {file_name}"
                        )
                    
                    try:
                        os.remove(file_name)
                    except Exception as remove_error:
                        logger.warning(f"Не удалось удалить файл {file_name}: {remove_error}")
                        
                except Exception as e:
                    logger.error(f"Ошибка отправки файла {file_name}: {e}")
                    keyboard = [[get_back_button()]]
                    reply_markup = InlineKeyboardMarkup(keyboard)
                    await context.bot.send_message(
                        chat_id=user_id,
                        text=f"❌ Ошибка отправки файла {file_name}: {e}",
                        reply_markup=reply_markup
                    )
                    
                    try:
                        if os.path.exists(file_name):
                            os.remove(file_name)
                    except Exception as remove_error:
                        logger.warning(f"Не удалось удалить файл {file_name} после ошибки: {remove_error}")
        
        keyboard = [[get_back_button()]]
        reply_markup = InlineKeyboardMarkup(keyboard)
        await context.bot.send_message(
            chat_id=user_id,
            text="✅ Все файлы подробной статистики отправлены",
            reply_markup=reply_markup
        )
        
    except Exception as e:
        logger.error(f"Ошибка генерации файлов статистики: {e}")
        keyboard = [[get_back_button()]]
        reply_markup = InlineKeyboardMarkup(keyboard)
        await context.bot.send_message(
            chat_id=user_id,
            text=f"❌ Ошибка генерации статистики: {e}",
            reply_markup=reply_markup
        )


async def generate_detailed_statistics_files():
    """Генерация файлов подробной статистики"""
    try:
        processed_content = "📊 ОБРАБОТАННЫЕ КАНАЛЫ\n"
        processed_content += "=" * 50 + "\n\n"
        
        # Получаем данные о обработанных каналах из базы данных
        try:
            from database import db
            channel_stats = await db.get_detailed_channel_statistics()
            
            processed_count = 0
            
            if not channel_stats:
                processed_content += "Обработанных каналов пока нет.\n"
            else:
                for channel_username, data in channel_stats.items():
                    comments_count = data.get('comments', 0)
                    reactions_count = data.get('reactions', 0)
                    
                    if comments_count > 0 or reactions_count > 0:
                        processed_count += 1
                        processed_content += f"**Канал: {channel_username}**\n\n"
                        processed_content += f"💬 Отправлено комментариев: {comments_count}\n"
                        processed_content += f"👍🏻 Поставлено реакций: {reactions_count}\n\n"
                        
                        comment_links = data.get('comment_links', [])
                        if comment_links:
                            processed_content += "🔗💬 Ссылки на комментарии которые были отправлены:\n"
                            for link in comment_links:
                                processed_content += f"{link}\n"
                            processed_content += "\n"
                        
                        post_links = data.get('post_links', [])
                        if post_links:
                            processed_content += "🔗📺 Ссылки на посты под которыми были отправлены комментарии:\n"
                            for link in post_links:
                                processed_content += f"{link}\n"
                            processed_content += "\n"
                        
                        processed_content += "-" * 50 + "\n\n"
                
                if processed_count == 0:
                    processed_content += "Обработанных каналов пока нет.\n"
                else:
                    # Добавляем общую статистику в начало
                    summary = f"📊 ИТОГО ОБРАБОТАНО: {processed_count} каналов\n\n" + "=" * 50 + "\n\n"
                    processed_content = processed_content.replace("=" * 50 + "\n\n", summary)
                    
        except Exception as e:
            logger.error(f"Ошибка получения данных из БД: {e}")
            processed_content += "Ошибка получения данных из базы данных.\n"
        
        with open('processed_channels.txt', 'w', encoding='utf-8-sig', newline='\n') as f:
            f.write(processed_content)
        
        # 2. Файл с очередью
        queue_content = "📋 ОЧЕРЕДЬ КАНАЛОВ\n"
        queue_content += "=" * 50 + "\n\n"
        
        queue_channels = bot_data['detailed_statistics']['queue_channels']
        if not queue_channels:
            queue_content += "Очередь каналов пуста.\n"
        else:
            queue_content += f"Каналов в очереди на обработку: {len(queue_channels)}\n\n"
            for i, channel in enumerate(queue_channels, 1):
                queue_content += f"{i}. {channel}\n"
        
        with open('queue_channels.txt', 'w', encoding='utf-8-sig', newline='\n') as f:
            f.write(queue_content)
       
        found_content = "🔍 НАЙДЕННЫЕ КАНАЛЫ\n"
        found_content += "=" * 50 + "\n\n"
        
        found_channels = bot_data['detailed_statistics']['found_channels']
        if not found_channels:
            found_content += "Найденных каналов пока нет.\n"
        else:
            found_content += f"Всего найдено поисковиком каналов: {len(found_channels)}\n\n"
            for i, channel in enumerate(found_channels, 1):
                found_content += f"{i}. {channel}\n"
        
        with open('found_channels.txt', 'w', encoding='utf-8-sig', newline='\n') as f:
            f.write(found_content)
        
        logger.info("Файлы подробной статистики успешно созданы с корректной кодировкой UTF-8")
        
    except Exception as e:
        logger.error(f"Ошибка создания файлов статистики: {e}")

def update_queue_statistics(queue_list):
    """Обновление статистики очереди"""
    bot_data['detailed_statistics']['queue_channels'] = queue_list
    asyncio.create_task(save_bot_state())

def update_found_channels_statistics(found_channels_list):
    """Обновление статистики найденных каналов"""
    bot_data['detailed_statistics']['found_channels'] = found_channels_list
    asyncio.create_task(save_bot_state())

async def handle_channel_selection(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Обработка выбора канала без блокировок"""
    if update.message and hasattr(update.message, 'chat_shared'):
        chat_shared = update.message.chat_shared
        if chat_shared.request_id == 1:
            chat_id = chat_shared.chat_id
            
            from telegram import ReplyKeyboardRemove
            await update.message.reply_text("📺 Канал получен, анализируем...", reply_markup=ReplyKeyboardRemove())
            
            # Очищаем состояние ожидания канала
            context.user_data.pop('awaiting_channel', None)
            
            # Анализ канала в фоне
            asyncio.create_task(analyze_selected_channel(update, context, chat_id))

async def analyze_selected_channel(update: Update, context: ContextTypes.DEFAULT_TYPE, chat_id: int):
    """Анализ выбранного канала в фоне"""
    user_id = update.effective_user.id
    
    try:
        # ИСПРАВЛЕНИЕ: получаем клиент пользователя, а не глобальный
        client = await get_user_telethon_client(user_id)
        if not client:
            keyboard = [[get_back_button()]]
            reply_markup = InlineKeyboardMarkup(keyboard)
            await update.message.reply_text("❌ Сначала добавьте аккаунт", reply_markup=reply_markup)
            return
            
        try:
            entity = await client.get_entity(chat_id)
            channel_username = entity.username if hasattr(entity, 'username') and entity.username else None
            
            if channel_username:
                channel_link = f"https://t.me/{channel_username}"
                
                try:
                    import channel_search_engine
                    # Устанавливаем клиент пользователя для анализа
                    channel_search_engine.shared_telethon_client = client
                    topics, keywords = await channel_search_engine.analyze_channel(chat_id)
                    
                    bot_data['settings']['target_channel'] = channel_link
                    bot_data['settings']['topics'] = topics
                    bot_data['settings']['keywords'] = keywords
                    asyncio.create_task(save_bot_state())
                    
                    await update.message.reply_text(
                        f"✅ Канал выбран и проанализирован!\n\n"
                        f"📺 Канал: {channel_link}\n\n"
                        f"🏷️ Темы: {', '.join(topics)}\n\n"
                        f"🔑 Ключевые слова: {', '.join(keywords)}"
                    )
                    
                    await show_main_menu(update, context, edit=False)
                except Exception as e:
                    logger.error(f"Ошибка анализа канала: {e}")
                    keyboard = [[get_back_button()]]
                    reply_markup = InlineKeyboardMarkup(keyboard)
                    await update.message.reply_text(f"❌ Ошибка анализа канала: {e}", reply_markup=reply_markup)
                finally:
                    # Закрываем клиент после использования
                    await client.disconnect()
            else:
                await client.disconnect()
                keyboard = [[get_back_button()]]
                reply_markup = InlineKeyboardMarkup(keyboard)
                await update.message.reply_text("❌ Канал должен быть публичным (иметь username)", reply_markup=reply_markup)
        except Exception as e:
            await client.disconnect()
            logger.error(f"Ошибка получения информации о канале: {e}")
            keyboard = [[get_back_button()]]
            reply_markup = InlineKeyboardMarkup(keyboard)
            await update.message.reply_text(f"❌ Ошибка получения информации о канале: {e}", reply_markup=reply_markup)
            
    except Exception as e:
        logger.error(f"Ошибка получения информации о канале: {e}")
        keyboard = [[get_back_button()]]
        reply_markup = InlineKeyboardMarkup(keyboard)
        await update.message.reply_text(f"❌ Ошибка получения информации о канале: {e}", reply_markup=reply_markup)

async def handle_callback_query(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Общий обработчик callback запросов"""
    query = update.callback_query
    data = query.data
    
    try:
        if data == "back":
            await handle_back_button(update, context)
        elif data == "account_setup":
            await account_setup(update, context)
        elif data == "target_channel":
            # Специальная обработка для кнопки "назад" из меню выбора канала
            channel_message_id = context.user_data.get('channel_selection_message_id')
            awaiting_channel = context.user_data.get('awaiting_channel')
            
            # Удаляем сообщение "👇 Нажмите кнопку для выбора канала:" если он есть
            if channel_message_id:
                try:
                    await context.bot.delete_message(
                        chat_id=update.effective_user.id,
                        message_id=channel_message_id
                    )
                    logger.info(f"Удалено сообщение выбора канала при возврате")
                except Exception as e:
                    logger.debug(f"Не удалось удалить сообщение выбора канала: {e}")
            
            # Удаляем клавиатуру "поделиться каналом" если ожидание было активно
            if awaiting_channel:
                try:
                    from telegram import ReplyKeyboardRemove
                    await context.bot.send_message(
                        chat_id=update.effective_user.id,
                        text="",
                        reply_markup=ReplyKeyboardRemove()
                    )
                except:
                    pass
            
            # Очищаем состояние
            context.user_data.clear()
            await target_channel_setup(update, context)
        elif data == "main_menu":
            # Сохраняем ID сообщения для удаления до очистки состояния
            channel_message_id = context.user_data.get('channel_selection_message_id')
            awaiting_channel = context.user_data.get('awaiting_channel')
            
            # Удаляем сообщение "👇 Нажмите кнопку для выбора канала:" если он есть
            if channel_message_id:
                try:
                    await context.bot.delete_message(
                        chat_id=update.effective_user.id,
                        message_id=channel_message_id
                    )
                    logger.info(f"Удалено сообщение выбора канала при переходе в главное меню")
                except Exception as e:
                    logger.debug(f"Не удалось удалить сообщение выбора канала: {e}")
            
            # Удаляем клавиатуру "поделиться каналом" если ожидание было активно
            if awaiting_channel:
                try:
                    from telegram import ReplyKeyboardRemove
                    await context.bot.send_message(
                        chat_id=update.effective_user.id,
                        text="",
                        reply_markup=ReplyKeyboardRemove()
                    )
                except:
                    pass
            
            context.user_data.clear()
            await show_main_menu(update, context)
        elif data == "select_channel":
            await select_channel(update, context)
        elif data == "manual_setup":
            await manual_setup(update, context)
        elif data.startswith("topic_") or data == "topics_done":
            await handle_topic_selection(update, context)
        elif data == "settings":
            await settings_menu(update, context)
        elif data == "prompts":
            await show_prompts_menu(update, context)
        elif data == "reset_prompts":
            await reset_prompts(update, context)
        elif data == "toggle_run":
            await toggle_run(update, context)
        elif data == "statistics":
            await show_statistics(update, context)
        elif data == "detailed_statistics":
            await show_detailed_statistics(update, context)
        elif data.startswith("code_"):
            await handle_code_input(update, context)
    except Exception as e:
        logger.error(f"Ошибка обработки callback запроса {data}: {e}")
        try:
            keyboard = [[get_back_button()]]
            reply_markup = InlineKeyboardMarkup(keyboard)
            await query.answer("❌ Произошла ошибка. Попробуйте еще раз.", show_alert=True)
            await query.edit_message_text("❌ Произошла ошибка. Попробуйте еще раз.", reply_markup=reply_markup)
        except:
            pass

def add_processed_channel_statistics(channel_username, comment_link=None, post_link=None, reaction_added=False, found_topic=None):
    """функция добавления статистики по каналу"""
    try:
        if comment_link or reaction_added:
            if channel_username not in bot_data['detailed_statistics']['processed_channels']:
                bot_data['detailed_statistics']['processed_channels'][channel_username] = {
                    'processed_at': datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                    'comments': 0,
                    'reactions': 0,
                    'comment_links': [],
                    'post_links': [],
                    'found_topic': found_topic or 'Другое',
                    'found_for_target': bot_data['settings'].get('target_channel', ''),
                    'found_for_keywords': bot_data['settings'].get('keywords', []),
                    'found_for_topics': bot_data['settings'].get('topics', [])
                }
            
            channel_stats = bot_data['detailed_statistics']['processed_channels'][channel_username]
            
            if comment_link and post_link:
                channel_stats['comments'] += 1
                channel_stats['comment_links'].append(comment_link)
                if post_link not in channel_stats['post_links']:
                    channel_stats['post_links'].append(post_link)
                logger.info(f"Добавлен комментарий для канала {channel_username}, всего: {channel_stats['comments']}")
            
            if reaction_added:
                channel_stats['reactions'] += 1
                logger.info(f"Добавлена реакция для канала {channel_username}, всего: {channel_stats['reactions']}")
                
            # сохраняем в базу данных
            try:
                from database import db
                if comment_link and post_link:
                    asyncio.create_task(db.add_channel_comment(channel_username, comment_link, post_link))
                if reaction_added:
                    asyncio.create_task(db.add_channel_reaction(channel_username))
            except Exception as db_error:
                logger.error(f"Ошибка сохранения в БД: {db_error}")
        else:
            if found_topic and channel_username not in bot_data['detailed_statistics']['processed_channels']:
                # Добавляем в найденные каналы для статистики поиска
                if channel_username not in bot_data['detailed_statistics']['found_channels']:
                    bot_data['detailed_statistics']['found_channels'].append(channel_username)
                    logger.info(f"Канал {channel_username} добавлен в найденные")
            
    except Exception as e:
        logger.error(f"Ошибка добавления статистики канала {channel_username}: {e}")

async def ensure_telethon_client_initialized(user_id: int):
    """Проверка и инициализация Telethon клиента для пользователя"""
    try:
        session_data = await db.load_user_session(user_id)
        config = session_data.get('client_config', {})
        
        if not config.get('api_id') or not config.get('api_hash') or not config.get('phone'):
            return False
            
        client = await get_user_telethon_client(user_id)
        if client:
            await client.disconnect()
            return True
            
        return False
        
    except Exception as e:
        logger.error(f"Ошибка инициализации клиента для пользователя {user_id}: {e}")
        return False

def get_shared_telethon_client():
    """Получение единого Telethon клиента для всех модулей"""
    return bot_data.get('telethon_client')

async def fast_initialization():
    """Быстрая инициализация бота"""
    try:
        logger.info("Начинаем инициализацию...")
        
        init_task = asyncio.create_task(init_database())
        
        config = load_user_config()
        
        await init_task
        
        await load_bot_state()
        
        # Убираем вызов ensure_telethon_client_initialized без user_id
        # Инициализация клиентов будет происходить индивидуально для каждого пользователя
        
        bot_data['initialization_complete'] = True
        
    except Exception as e:
        logger.error(f"Ошибка инициализации: {e}")
        bot_data['initialization_complete'] = True

async def start_new_post_tracking():
    """Запуск отслеживания новых постов"""
    try:
        import masslooker
        await masslooker.start_new_post_tracking()
        logger.info("Отслеживание новых постов запущено")
    except Exception as e:
        logger.error(f"Ошибка запуска отслеживания новых постов: {e}")

async def stop_new_post_tracking():
    """Остановка отслеживания новых постов"""
    try:
        import masslooker
        await masslooker.stop_new_post_tracking()
        logger.info("Отслеживание новых постов остановлено")
    except Exception as e:
        logger.error(f"Ошибка остановки отслеживания новых постов: {e}")

async def run_bot(bot_token):
    """Асинхронная функция запуска бота"""
    logger.info("Запуск бота интерфейса...")
    
    try:
        # Создаем приложение
        application = Application.builder().token(bot_token).build()
        
        # Добавляем обработчики
        application.add_handler(CommandHandler("start", start))
        application.add_handler(CallbackQueryHandler(handle_callback_query))
        application.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, handle_text_message))
        application.add_handler(MessageHandler(filters.StatusUpdate.CHAT_SHARED, handle_channel_selection))
        
        # Запускаем инициализацию в фоне
        init_task = asyncio.create_task(fast_initialization())
        
        logger.info("✅ Бот запущен и готов к работе (инициализация продолжается в фоне)")
        
        # Запускаем бота
        async with application:
            await application.start()
            await application.updater.start_polling(
                poll_interval=1.0,
                bootstrap_retries=-1,  # Бесконечные попытки переподключения
                read_timeout=30,       #  таймаут чтения
                write_timeout=30,      #  таймаут записи
                connect_timeout=30,    #  таймаут подключения
                pool_timeout=30        #  таймаут пула соединений
            )
            
            try:
                # Ждем завершения инициализации
                await init_task
                
                logger.info("Инициализация завершена")
                
                # Основной цикл работы без таймаутов
                while True:
                    await asyncio.sleep(1)
                    
            except asyncio.CancelledError:
                logger.info("Получен сигнал завершения бота")
            except KeyboardInterrupt:
                logger.info("Получен сигнал прерывания от пользователя")
            except Exception as e:
                logger.error(f"Ошибка в основном цикле: {e}")
            finally:
                logger.info("🔄 Завершение работы бота...")
                
                # Остановка отслеживания новых постов
                if bot_data['new_post_tracker'] is not None:
                    await stop_new_post_tracking()
                
                # Сохраняем состояние
                try:
                    await save_bot_state()
                    await close_database()
                except Exception as e:
                    logger.error(f"Ошибка при сохранении состояния: {e}")
                
                # Закрываем Telethon клиент
                try:
                    if bot_data['telethon_client']:
                        await bot_data['telethon_client'].disconnect()
                        logger.info("Telethon клиент отключен корректно")
                except Exception as e:
                    logger.error(f"Ошибка закрытия Telethon клиента: {e}")
                
                # Закрываем thread pool
                try:
                                        executor.shutdown(wait=False)
                except Exception as e:
                    logger.error(f"Ошибка закрытия thread pool: {e}")
                
                await application.updater.stop()
                await application.stop()
        
    except Exception as e:
        logger.error(f"Критическая ошибка в run_bot: {e}")
        raise

def setup_signal_handlers():
    """Настройка обработчиков сигналов для корректного завершения"""
    def signal_handler(signum, frame):
        logger.info(f"Получен сигнал {signum}, инициируем корректное завершение...")
        # Устанавливаем флаг для корректного завершения
        import sys
        sys.exit(0)
    
    try:
        signal.signal(signal.SIGINT, signal_handler)
        signal.signal(signal.SIGTERM, signal_handler)
        logger.info("Обработчики сигналов установлены")
    except Exception as e:
        logger.warning(f"Не удалось установить обработчики сигналов: {e}")

def main(bot_token):
    """Основная функция бота с улучшенным управлением"""
    try:
        # Настраиваем обработчики сигналов
        setup_signal_handlers()
        
        # Запускаем бота
        asyncio.run(run_bot(bot_token))
        
    except KeyboardInterrupt:
        logger.info("Получено прерывание от пользователя")
    except Exception as e:
        logger.error(f"Критическая ошибка в main: {e}")
        raise
    finally:
        logger.info("🏁 Завершение работы программы")

# Функции для обратной совместимости с другими модулями
def get_bot_data():
    """Получение данных бота для других модулей"""
    return bot_data

def is_bot_running():
    """Проверка запущен ли бот"""
    return bot_data.get('is_running', False)

def get_telethon_client():
    """Получение Telethon клиента (для обратной совместимости)"""
    return bot_data.get('telethon_client')

def get_bot_settings():
    """Получение настроек бота"""
    return bot_data.get('settings', {})

def get_bot_prompts():
    """Получение промтов бота"""
    return bot_data.get('prompts', {})

# Функции для интеграции с другими модулями
async def notify_bot_status(message: str, user_id: int = None):
    """Уведомление пользователей о статусе бота"""
    try:
        if user_id and user_id in bot_data['active_users']:
            # Здесь можно добавить отправку сообщения конкретному пользователю
            logger.info(f"Уведомление для пользователя {user_id}: {message}")
        else:
            # Уведомление для всех активных пользователей
            logger.info(f"Общее уведомление: {message}")
    except Exception as e:
        logger.error(f"Ошибка отправки уведомления: {e}")

def register_external_handlers():
    """Регистрация внешних обработчиков для интеграции"""
    try:
        # Здесь можно зарегистрировать обработчики от других модулей
        pass
    except Exception as e:
        logger.error(f"Ошибка регистрации внешних обработчиков: {e}")

# Контекстный менеджер для работы с ботом
class BotContext:
    """Контекстный менеджер для работы с ботом"""
    
    def __init__(self, bot_token: str):
        self.bot_token = bot_token
        self.bot_task = None
    
    async def __aenter__(self):
        """Вход в контекст - запуск бота"""
        logger.info("Запуск бота через контекстный менеджер")
        self.bot_task = asyncio.create_task(run_bot(self.bot_token))
        
        # Даем время на инициализацию
        await asyncio.sleep(2)
        
        return self
    
    async def __aexit__(self, exc_type, exc_val, exc_tb):
        """Выход из контекста - остановка бота"""
        logger.info("Остановка бота через контекстный менеджер")
        
        if self.bot_task:
            self.bot_task.cancel()
            try:
                await self.bot_task
            except asyncio.CancelledError:
                pass
        
        # Принудительное сохранение состояния
        try:
            await save_bot_state()
            await close_database()
        except Exception as e:
            logger.error(f"Ошибка при завершении работы: {e}")

async def get_user_telethon_client(user_id: int) -> Optional[TelegramClient]:
    """Получение Telethon клиента для конкретного пользователя"""
    try:
        session_data = await db.load_user_session(user_id)
        client_config = session_data.get('client_config')
        
        if not client_config:
            logger.debug(f"Нет конфигурации клиента для пользователя {user_id}")
            return None
            
        if not all(key in client_config for key in ['api_id', 'api_hash', 'phone']):
            logger.debug(f"Неполная конфигурация клиента для пользователя {user_id}")
            return None
        
        # Проверяем что api_id является числом
        try:
            api_id = int(client_config['api_id'])
        except (ValueError, TypeError):
            logger.error(f"Неверный формат API ID для пользователя {user_id}")
            return None
            
        session_file = f'user_session_{user_id}'
        loop = asyncio.get_event_loop()
        
        client = TelegramClient(
            session_file,
            api_id,
            client_config['api_hash'],
            loop=loop,
            timeout=30,
            retry_delay=1,
            flood_sleep_threshold=60
        )
        
        await client.connect()
        
        if await client.is_user_authorized():
            return client
        else:
            await client.disconnect()
            logger.debug(f"Клиент для пользователя {user_id} не авторизован")
            return None
            
    except Exception as e:
        logger.error(f"Ошибка получения клиента для пользователя {user_id}: {e}")
        return None

async def set_user_telethon_client(user_id: int, client: TelegramClient):
    """Установка Telethon клиента для конкретного пользователя"""
    try:
        session_data = await db.load_user_session(user_id)
        session_data['has_active_client'] = True
        await db.save_user_session(user_id, session_data)
    except Exception as e:
        logger.error(f"Ошибка сохранения клиента для пользователя {user_id}: {e}")

async def remove_user_telethon_client(user_id: int):
    """Удаление Telethon клиента пользователя"""
    try:
        client = await get_user_telethon_client(user_id)
        if client:
            try:
                if client.is_connected():
                    await client.disconnect()
            except:
                pass
        
        session_data = await db.load_user_session(user_id)
        session_data['has_active_client'] = False
        session_data.pop('client_config', None)
        await db.save_user_session(user_id, session_data)
    except Exception as e:
        logger.error(f"Ошибка удаления клиента для пользователя {user_id}: {e}")

async def start_user_services(user_id: int):
    """Запуск сервисов для конкретного пользователя"""
    try:
        if user_id not in user_sessions_cache:
            logger.error(f"Нет данных сессии для пользователя {user_id}")
            return
            
        user_session = user_sessions_cache[user_id]
        client_config = user_session.get('client_config', {})
        
        if not client_config or not all(client_config.get(field) for field in ['api_id', 'api_hash', 'phone']):
            logger.error(f"Неполная конфигурация клиента для пользователя {user_id}")
            return
        
        # Создаем клиент для пользователя
        client = await get_user_telethon_client(user_id)
        if not client:
            logger.error(f"Не удалось создать клиент для пользователя {user_id}")
            return
        
        # Получаем настройки пользователя или используем глобальные
        user_settings = user_session.get('settings', bot_data['settings'])
        
        try:
            import channel_search_engine
            await channel_search_engine.start_search(user_settings, client, user_id)
            logger.info(f"Поисковик запущен для пользователя {user_id}")
        except Exception as e:
            logger.error(f"Ошибка запуска поисковика для пользователя {user_id}: {e}")
        
        try:
            import masslooker
            asyncio.create_task(masslooker.start_masslooking(client, user_settings, user_id))
            logger.info(f"Масслукер запущен для пользователя {user_id}")
        except Exception as e:
            logger.error(f"Ошибка запуска масслукера для пользователя {user_id}: {e}")
        
        # НЕ отключаем клиент здесь, он нужен для работы сервисов
        
    except Exception as e:
        logger.error(f"Ошибка запуска сервисов для пользователя {user_id}: {e}")

async def get_user_telethon_client(user_id: int) -> Optional[TelegramClient]:
    """Получение Telethon клиента для конкретного пользователя"""
    try:
        if user_id not in user_sessions_cache:
            logger.debug(f"Нет данных сессии в кеше для пользователя {user_id}")
            return None
            
        session_data = user_sessions_cache[user_id]
        client_config = session_data.get('client_config')
        
        if not client_config:
            logger.debug(f"Нет конфигурации клиента для пользователя {user_id}")
            return None
            
        if not all(key in client_config for key in ['api_id', 'api_hash', 'phone']):
            logger.debug(f"Неполная конфигурация клиента для пользователя {user_id}")
            return None
        
        # Проверяем что api_id является числом
        try:
            api_id = int(client_config['api_id'])
        except (ValueError, TypeError):
            logger.error(f"Неверный формат API ID для пользователя {user_id}")
            return None
            
        session_file = f'user_session_{user_id}'
        loop = asyncio.get_event_loop()
        
        client = TelegramClient(
            session_file,
            api_id,
            client_config['api_hash'],
            loop=loop,
            timeout=30,
            retry_delay=1,
            flood_sleep_threshold=60
        )
        
        await client.connect()
        
        if await client.is_user_authorized():
            logger.info(f"Клиент для пользователя {user_id} создан и авторизован")
            return client
        else:
            await client.disconnect()
            logger.debug(f"Клиент для пользователя {user_id} не авторизован")
            return None
            
    except Exception as e:
        logger.error(f"Ошибка получения клиента для пользователя {user_id}: {e}")
        return None

async def stop_user_services(user_id: int):
    """Остановка сервисов для конкретного пользователя"""
    try:
        import channel_search_engine
        await channel_search_engine.stop_search(user_id)
    except Exception as e:
        logger.error(f"Ошибка остановки поисковика для пользователя {user_id}: {e}")
        
    try:
        import masslooker
        await masslooker.stop_masslooking(user_id)
    except Exception as e:
        logger.error(f"Ошибка остановки масслукера для пользователя {user_id}: {e}")

if __name__ == "__main__":
    # Для тестирования
    import sys
    if len(sys.argv) > 1:
        main(sys.argv[1])
    else:
        print("Укажите токен бота как аргумент")
        print("Пример: python bot_interface.py YOUR_BOT_TOKEN")