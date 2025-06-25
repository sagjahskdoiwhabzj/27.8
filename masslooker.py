import asyncio
import logging
import random
import time
from datetime import datetime
from typing import List, Optional, Set, Callable, Any, Dict
import os

logger = logging.getLogger(__name__)

try:
    from telethon import TelegramClient, events
    from telethon.errors import ChannelPrivateError, ChatWriteForbiddenError, FloodWaitError, UserNotParticipantError, UserNotMutualContactError
    from telethon.tl.types import Channel, Chat, MessageMediaPhoto, MessageMediaDocument
    from telethon.tl.functions.channels import JoinChannelRequest, LeaveChannelRequest, GetFullChannelRequest
    from telethon.tl.functions.messages import SendReactionRequest, GetAvailableReactionsRequest, GetDiscussionMessageRequest, GetRepliesRequest
    from telethon.tl.types import ReactionEmoji, ReactionCustomEmoji
    import g4f
except ImportError as e:
    logger.error(f"Ошибка импорта библиотек: {e}")
    raise

# Глобальные переменные
masslooking_active = False
shared_client: Optional[TelegramClient] = None
settings = {}
processed_channels: Set[str] = set()
masslooking_progress = {'current_channel': '', 'processed_count': 0}
statistics = {
    'comments_sent': 0,
    'reactions_set': 0,
    'channels_processed': 0,
    'errors': 0,
    'flood_waits': 0,
    'total_flood_wait_time': 0
}

# Круговая обработка каналов
channel_processing_queue = {}
current_channel_iterator = None
channels_in_rotation = []

# Флаг для отслеживания первого действия подписки
first_subscription_made = False

# Отслеживание новых постов
new_post_tracking_active = False
tracked_channels = {}  # {username: {'entity_id': id, 'last_message_id': id}}
user_clients = {}
user_settings = {}
user_processed_channels = {}
user_statistics = {}
user_masslooking_active = {}
user_channel_processing_queue = {}
user_tracked_channels = {}

# Настройки FloodWait
FLOOD_WAIT_SETTINGS = {
    'max_retries': 5,
    'max_wait_time': 7200,
    'enable_exponential_backoff': True,
    'check_interval': 10,
    'backoff_multiplier': 1.5
}

# Положительные реакции для Telegram
DEFAULT_POSITIVE_REACTIONS = [
    '👍', '❤️', '🔥', '🥰', '👏', '😍', '🤩', '💯', '⭐',
    '🎉', '🙏', '💪', '👌', '✨', '🌟', '🚀'
]

def check_bot_running(user_id: int) -> bool:
    """Проверка состояния работы бота для пользователя"""
    try:
        # ИСПРАВЛЕНИЕ: убираем ссылку на неопределенную переменную settings
        import bot_interface
        import asyncio
        
        # Получаем event loop безопасно
        try:
            loop = asyncio.get_event_loop()
        except RuntimeError:
            return user_masslooking_active.get(user_id, False)
        
        # Проверяем загруженные данные пользователя
        try:
            user_session = loop.run_until_complete(bot_interface.db.load_user_session(user_id))
            is_running = user_session.get('is_running', False)
            logger.debug(f"Состояние пользователя {user_id} из БД: is_running={is_running}")
            return is_running
        except Exception as e:
            logger.warning(f"Ошибка проверки состояния пользователя {user_id} из БД: {e}")
            # Fallback на локальное состояние
            return user_masslooking_active.get(user_id, False)
        
    except Exception as e:
        logger.warning(f"Ошибка проверки состояния бота для пользователя {user_id}: {e}")
        # В случае любой ошибки используем локальное состояние
        return user_masslooking_active.get(user_id, False)

async def check_subscription_status(entity, username: str) -> bool:
    """Проверка статуса подписки на канал"""
    try:
        if not shared_client:
            return False
            
        try:
            full_channel = await get_full_channel_safe(entity)
            if not full_channel:
                return False
            
            return True
            
        except (ChannelPrivateError, UserNotParticipantError):
            return False
        except Exception as e:
            logger.warning(f"Ошибка проверки подписки на канал {username}: {e}")
            return False
            
    except Exception as e:
        logger.error(f"Критическая ошибка проверки подписки на канал {username}: {e}")
        return False

async def apply_subscription_delay(username: str, action_type: str = "подписки"):
    """Применение задержки с учетом флага первого действия"""
    global first_subscription_made
    
    if not first_subscription_made:
        logger.info(f"Первая подписка на канал {username}")
        first_subscription_made = True
        return True
    
    delay_range = settings.get('delay_range', (20, 1000))
    if delay_range == (0, 0):
        return True
    
    try:
        if not isinstance(delay_range, (list, tuple)) or len(delay_range) != 2:
            logger.warning(f"Некорректный диапазон задержки {delay_range}")
            delay_range = (20, 1000)
        
        min_delay, max_delay = delay_range
        if not isinstance(min_delay, (int, float)) or not isinstance(max_delay, (int, float)):
            logger.warning(f"Некорректные типы задержки {delay_range}")
            delay_range = (20, 1000)
            min_delay, max_delay = delay_range
        
        if min_delay < 0 or max_delay < 0 or min_delay > max_delay:
            logger.warning(f"Некорректные значения задержки {delay_range}")
            delay_range = (20, 1000)
            min_delay, max_delay = delay_range
        
        subscription_delay = random.uniform(min_delay, max_delay)
        logger.info(f"Ожидание {subscription_delay:.1f} секунд перед {action_type} на канал {username}")
        
        delay_chunks = int(subscription_delay)
        remaining_delay = subscription_delay - delay_chunks
        
        for _ in range(delay_chunks):
            if not check_bot_running():
                logger.info(f"Остановка запрошена во время задержки {action_type}")
                return False
            
            try:
                import bot_interface
                new_settings = bot_interface.get_bot_settings()
                if new_settings != settings:
                    settings.update(new_settings)
            except Exception as e:
                pass
            
            await asyncio.sleep(1)
        
        if remaining_delay > 0:
            if not check_bot_running():
                logger.info(f"Остановка запрошена во время задержки {action_type}")
                return False
            await asyncio.sleep(remaining_delay)
        
        logger.info(f"Задержка {action_type} завершена для канала {username}")
        return True
        
    except Exception as e:
        logger.error(f"Ошибка при обработке задержки {action_type}: {e}")
        await asyncio.sleep(20)
        return True

async def ensure_subscription(username: str) -> bool:
    """Обеспечение подписки на канал с проверкой и переподпиской при необходимости"""
    try:
        entity = await get_entity_safe(username)
        if not entity:
            logger.error(f"Не удалось получить сущность канала {username}")
            return False
        
        is_subscribed = await check_subscription_status(entity, username)
        
        if not is_subscribed:
            logger.info(f"Обнаружена отписка от канала {username}, подписываемся заново")
            
            delay_success = await apply_subscription_delay(username, "переподписки")
            if not delay_success:
                return False
            
            join_result = await join_channel_safe(entity)
            if join_result is None:
                logger.error(f"Не удалось переподписаться на канал {username}")
                return False
            
            logger.info(f"Успешно переподписались на канал {username}")
            return True
        else:
            return True
            
    except Exception as e:
        logger.error(f"Ошибка обеспечения подписки на канал {username}: {e}")
        return False

async def smart_wait(wait_time: int, operation_name: str = "operation") -> bool:
    """Умное ожидание с возможностью прерывания и экспоненциальным backoff"""
    original_wait_time = wait_time
    
    if wait_time > FLOOD_WAIT_SETTINGS['max_wait_time']:
        wait_time = FLOOD_WAIT_SETTINGS['max_wait_time']
        logger.warning(f"FloodWait для {operation_name}: {original_wait_time}с ограничен до {wait_time}с")
    
    logger.info(f"Ожидание FloodWait для {operation_name}: {wait_time} секунд")
    
    statistics['flood_waits'] += 1
    statistics['total_flood_wait_time'] += wait_time
    
    check_interval = FLOOD_WAIT_SETTINGS['check_interval']
    chunks = wait_time // check_interval
    remainder = wait_time % check_interval
    
    for i in range(chunks):
        if not check_bot_running():
            logger.info(f"Остановка запрошена во время FloodWait для {operation_name}")
            return False
        
        progress = (i + 1) * check_interval
        remaining = wait_time - progress
        
        await asyncio.sleep(check_interval)
    
    if remainder > 0:
        if not check_bot_running():
            return False
        await asyncio.sleep(remainder)
    
    logger.info(f"FloodWait для {operation_name} завершен")
    return True

async def handle_flood_wait(func: Callable, *args, operation_name: str = None, max_retries: int = None, **kwargs) -> Any:
    """Универсальная обработка FloodWait для любых функций"""
    if operation_name is None:
        operation_name = func.__name__ if hasattr(func, '__name__') else "operation"
    
    if max_retries is None:
        max_retries = FLOOD_WAIT_SETTINGS['max_retries']
    
    base_delay = 1
    
    for attempt in range(max_retries):
        try:
            if not check_bot_running():
                logger.info(f"Остановка запрошена перед выполнением {operation_name}")
                return None
            
            return await func(*args, **kwargs)
            
        except FloodWaitError as e:
            wait_time = e.seconds
            logger.warning(f"FloodWait при {operation_name} (попытка {attempt + 1}): {wait_time} секунд")
            
            if attempt < max_retries - 1:
                if not await smart_wait(wait_time, operation_name):
                    logger.info(f"Прерываем {operation_name} из-за остановки бота")
                    return None
                
                if FLOOD_WAIT_SETTINGS['enable_exponential_backoff']:
                    extra_delay = base_delay * (FLOOD_WAIT_SETTINGS['backoff_multiplier'] ** attempt)
                    await asyncio.sleep(extra_delay)
                
                continue
            else:
                logger.error(f"Превышено количество попыток для {operation_name} после FloodWait")
                statistics['errors'] += 1
                return None
                
        except Exception as e:
            logger.error(f"Ошибка при выполнении {operation_name} (попытка {attempt + 1}): {e}")
            if attempt < max_retries - 1:
                await asyncio.sleep(random.uniform(1, 3))
                continue
            else:
                logger.error(f"Превышено количество попыток для {operation_name}")
                statistics['errors'] += 1
                return None
    
    return None

def extract_message_text(message) -> str:
    """Извлекает текст из сообщения, включая сообщения с медиа"""
    text = ""
    
    try:
        if hasattr(message, 'message') and message.message:
            text = str(message.message).strip()
        elif hasattr(message, 'text') and message.text:
            text = str(message.text).strip()
        
        if not text and hasattr(message, 'media') and message.media:
            if hasattr(message, 'message') and message.message:
                text = str(message.message).strip()
        
        return text
    except Exception as e:
        logger.error(f"Ошибка извлечения текста из сообщения: {e}")
        return ""

def has_commentable_content(message) -> bool:
    """Проверяет, есть ли в сообщении контент для комментирования"""
    try:
        if not hasattr(message, 'id') or not message.id or message.id <= 0:
            return False
        
        text = extract_message_text(message)
        
        if text and len(text.strip()) > 0:
            return True
        
        if hasattr(message, 'media') and message.media:
            return True
        
        return False
    except Exception as e:
        logger.error(f"Ошибка проверки контента сообщения: {e}")
        return False

async def get_post_comments(message, channel_entity) -> str:
    """Получение комментариев к посту"""
    try:
        if not shared_client:
            logger.warning("Telethon клиент не инициализирован")
            return ""
        
        discussion_info = await shared_client(GetDiscussionMessageRequest(
            peer=channel_entity,
            msg_id=message.id
        ))
        
        if not discussion_info or not discussion_info.messages:
            return ""
        
        discussion_message = discussion_info.messages[0]
        discussion_group = discussion_message.peer_id
        reply_to_msg_id = discussion_message.id
        
        replies = await shared_client(GetRepliesRequest(
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
                sender_name = "Аноним"
                try:
                    if hasattr(msg, 'from_id') and msg.from_id:
                        sender = await shared_client.get_entity(msg.from_id)
                        if hasattr(sender, 'first_name'):
                            sender_name = sender.first_name
                            if hasattr(sender, 'last_name') and sender.last_name:
                                sender_name += f" {sender.last_name}"
                        elif hasattr(sender, 'title'):
                            sender_name = sender.title
                except:
                    pass
                
                comment_text = f"{sender_name}: {msg.message.strip()}"
                
                if total_length + len(comment_text) + 2 > max_length:
                    break
                
                comments.append(comment_text)
                total_length += len(comment_text) + 2
        
        return "\n\n".join(comments)
        
    except Exception as e:
        logger.error(f"Ошибка получения комментариев: {e}")
        return ""

async def generate_comment(post_text: str, topics: List[str], message=None, channel_entity=None) -> str:
    """Генерация комментария с помощью GPT-4 с использованием промта из bot_interface"""
    try:
        try:
            import bot_interface
            prompts = bot_interface.get_bot_prompts()
            comment_prompt = prompts.get('comment_prompt', '')
            if not comment_prompt:
                raise Exception("Промт для комментариев не найден в bot_interface")
        except Exception as e:
            logger.error(f"Ошибка получения промта из bot_interface: {e}")
            comment_prompt = """Создай короткий, естественный комментарий к посту на русском языке. 

Текст поста: {text_of_the_post}

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

Создай комментарий:"""
        
        topics_text = ', '.join(topics) if topics else 'общая тематика'
        
        comments_text = ""
        if '{comments}' in comment_prompt and message and channel_entity:
            comments_text = await get_post_comments(message, channel_entity)
        
        prompt = comment_prompt
        
        if '{text_of_the_post}' in prompt:
            prompt = prompt.replace('{text_of_the_post}', post_text[:1000])
        else:
            prompt = prompt + f"\n\nТекст поста: {post_text[:1000]}"
        
        if '{topics}' in prompt:
            prompt = prompt.replace('{topics}', topics_text)
        
        if '{comments}' in prompt:
            prompt = prompt.replace('{comments}', comments_text if comments_text else "Комментариев пока нет")
        
        response = g4f.ChatCompletion.create(
            model=g4f.models.gpt_4,
            messages=[{"role": "user", "content": prompt}],
            stream=False
        )
        
        comment = response.strip()
        
        if comment.startswith('"') and comment.endswith('"'):
            comment = comment[1:-1]
        
        if comment.startswith("'") and comment.endswith("'"):
            comment = comment[1:-1]
        
        logger.info(f"Сгенерирован комментарий: {comment[:50]}...")
        return comment
        
    except Exception as e:
        logger.error(f"Ошибка генерации комментария: {e}")
        fallback_comments = [
            "Интересно, спасибо за пост!",
            "Полезная информация",
            "Актуальная тема",
            "Хороший материал",
            "Согласен с автором"
        ]
        return random.choice(fallback_comments)

async def get_entity_safe(identifier):
    """Безопасное получение сущности с обработкой FloodWait"""
    async def _get_entity():
        return await shared_client.get_entity(identifier)
    
    return await handle_flood_wait(_get_entity, operation_name=f"get_entity({identifier})")

async def get_full_channel_safe(entity):
    """Безопасное получение полной информации о канале с обработкой FloodWait"""
    async def _get_full_channel():
        return await shared_client(GetFullChannelRequest(entity))
    
    return await handle_flood_wait(_get_full_channel, operation_name=f"get_full_channel({entity.id})")

async def join_channel_safe(entity):
    """Безопасная подписка на канал с обработкой FloodWait"""
    async def _join_channel():
        return await shared_client(JoinChannelRequest(entity))
    
    return await handle_flood_wait(_join_channel, operation_name=f"join_channel({entity.username or entity.id})")

async def leave_channel_safe(entity):
    """Безопасная отписка от канала с проверкой участия"""
    try:
        try:
            full_channel = await get_full_channel_safe(entity)
            if not full_channel:
                return True
        except (ChannelPrivateError, UserNotParticipantError):
            return True
        except Exception as e:
            logger.warning(f"Ошибка проверки участия в канале {getattr(entity, 'username', entity.id)}: {e}")
    
        async def _leave_channel():
            return await shared_client(LeaveChannelRequest(entity))
        
        result = await handle_flood_wait(_leave_channel, operation_name=f"leave_channel({getattr(entity, 'username', entity.id)})")
        
        if result is not None:
            logger.info(f"Успешно покинули канал {getattr(entity, 'username', entity.id)}")
            return True
        else:
            logger.warning(f"Не удалось покинуть канал {getattr(entity, 'username', entity.id)}")
            return False
            
    except (UserNotParticipantError, UserNotMutualContactError) as e:
        return True
    except Exception as e:
        logger.error(f"Ошибка при отписке от канала {getattr(entity, 'username', entity.id)}: {e}")
        return False

async def send_message_safe(peer, message, **kwargs):
    """Безопасная отправка сообщения с обработкой FloodWait"""
    async def _send_message():
        return await shared_client.send_message(peer, message, **kwargs)
    
    peer_name = getattr(peer, 'username', None) or getattr(peer, 'id', 'unknown')
    return await handle_flood_wait(_send_message, operation_name=f"send_message_to({peer_name})")

async def send_reaction_safe(peer, msg_id, reaction):
    """Безопасная отправка реакции с обработкой FloodWait и ошибки лимита реакций"""
    async def _send_reaction():
        return await shared_client(SendReactionRequest(
            peer=peer,
            msg_id=msg_id,
            reaction=[ReactionEmoji(emoticon=reaction)]
        ))
    
    peer_name = getattr(peer, 'username', None) or getattr(peer, 'id', 'unknown')
    
    for attempt in range(FLOOD_WAIT_SETTINGS['max_retries']):
        try:
            if not check_bot_running():
                logger.info(f"Остановка запрошена перед отправкой реакции")
                return None
            
            return await _send_reaction()
            
        except Exception as e:
            error_str = str(e).lower()
            
            if "reactions_uniq_max" in error_str or "reaction emojis" in error_str:
                logger.warning(f"Достигнут лимит уникальных реакций для поста {msg_id}")
                return None
            
            if "flood" in error_str:
                try:
                    wait_time = int(''.join(filter(str.isdigit, str(e))))
                    if wait_time > 0:
                        logger.warning(f"FloodWait при отправке реакции: {wait_time} секунд")
                        if not await smart_wait(wait_time, f"send_reaction_to({peer_name}, {msg_id})"):
                            return None
                        continue
                except:
                    pass
            
            logger.error(f"Ошибка отправки реакции (попытка {attempt + 1}): {e}")
            if attempt < FLOOD_WAIT_SETTINGS['max_retries'] - 1:
                await asyncio.sleep(random.uniform(1, 3))
                continue
            else:
                logger.error(f"Не удалось отправить реакцию после {FLOOD_WAIT_SETTINGS['max_retries']} попыток")
                statistics['errors'] += 1
                return None
    
    return None

async def get_discussion_message_safe(peer, msg_id):
    """Безопасное получение discussion message с обработкой FloodWait"""
    async def _get_discussion_message():
        return await shared_client(GetDiscussionMessageRequest(peer=peer, msg_id=msg_id))
    
    peer_name = getattr(peer, 'username', None) or getattr(peer, 'id', 'unknown')
    return await handle_flood_wait(_get_discussion_message, operation_name=f"get_discussion_message({peer_name}, {msg_id})")

async def iter_messages_safe(entity, limit=None):
    """Безопасная итерация по сообщениям с улучшенной обработкой ошибок"""
    messages = []
    try:
        if not shared_client:
            logger.error("Shared client не инициализирован")
            return messages
            
        message_count = 0
        async for message in shared_client.iter_messages(entity, limit=limit):
            if message and hasattr(message, 'id') and message.id:
                messages.append(message)
                message_count += 1
                
                if message_count % 10 == 0:
                    await asyncio.sleep(0.5)
                else:
                    await asyncio.sleep(0.1)
                    
    except FloodWaitError as e:
        logger.warning(f"FloodWait при получении сообщений: {e.seconds} секунд")
        if await smart_wait(e.seconds, "iter_messages"):
            try:
                message_count = 0
                async for message in shared_client.iter_messages(entity, limit=limit):
                    if message and hasattr(message, 'id') and message.id:
                        messages.append(message)
                        message_count += 1
                        
                        if message_count % 10 == 0:
                            await asyncio.sleep(0.5)
                        else:
                            await asyncio.sleep(0.1)
                            
            except Exception as retry_error:
                logger.error(f"Ошибка при повторной попытке получения сообщений: {retry_error}")
        else:
            logger.info("Прерываем получение сообщений из-за остановки бота")
    except (ChannelPrivateError, UserNotParticipantError) as e:
        logger.warning(f"Нет доступа к сообщениям: {e}")
    except Exception as e:
        logger.error(f"Ошибка при получении сообщений: {e}")
    
    logger.info(f"Получено {len(messages)} валидных сообщений")
    return messages

async def get_channel_available_reactions(entity) -> List[str]:
    """Получение доступных реакций конкретно в канале"""
    try:
        full_channel = await get_full_channel_safe(entity)
        if not full_channel:
            logger.warning("Не удалось получить информацию о канале")
            return DEFAULT_POSITIVE_REACTIONS
        
        if hasattr(full_channel.full_chat, 'available_reactions'):
            available_reactions = full_channel.full_chat.available_reactions
            
            if available_reactions and hasattr(available_reactions, 'reactions'):
                channel_reactions = []
                for reaction in available_reactions.reactions:
                    if hasattr(reaction, 'emoticon'):
                        emoji = reaction.emoticon
                        if emoji in DEFAULT_POSITIVE_REACTIONS:
                            channel_reactions.append(emoji)
                
                if channel_reactions:
                    logger.info(f"Найдено {len(channel_reactions)} доступных положительных реакций в канале")
                    return channel_reactions
        
        logger.info("Используем базовый набор положительных реакций")
        return DEFAULT_POSITIVE_REACTIONS
        
    except Exception as e:
        logger.warning(f"Ошибка получения доступных реакций канала: {e}")
        return DEFAULT_POSITIVE_REACTIONS

async def add_reaction_to_post(message, channel_username):
    """Добавление реакции к посту с полной обработкой FloodWait"""
    try:
        if not check_bot_running():
            logger.info("Остановка запрошена, прерываем добавление реакции")
            return False
        
        entity = await get_entity_safe(message.peer_id)
        if not entity:
            logger.error("Не удалось получить информацию о канале для реакции")
            return False
        
        available_reactions = await get_channel_available_reactions(entity)
        
        if not available_reactions:
            logger.warning("Нет доступных реакций в канале")
            return False
        
        reaction = random.choice(available_reactions)
       
        result = await send_reaction_safe(message.peer_id, message.id, reaction)
        
        if result is not None:
            logger.info(f"Поставлена реакция {reaction} к посту {message.id}")
            statistics['reactions_set'] += 1
            
            try:
                import bot_interface
                bot_interface.update_statistics(reactions=1)
                bot_interface.add_processed_channel_statistics(channel_username, reaction_added=True)
            except:
                pass
            
            return True
        else:
            logger.warning(f"Не удалось поставить реакцию на пост {message.id}")
            return False
            
    except Exception as e:
        logger.error(f"Критическая ошибка добавления реакции: {e}")
        statistics['errors'] += 1
        return False

async def check_post_comments_available(message) -> bool:
    """Проверка доступности комментариев под конкретным постом через replies.comments"""
    try:
        if not hasattr(message, 'replies') or not message.replies:
            return False
        
        if hasattr(message.replies, 'comments') and message.replies.comments:
            logger.info(f"Пост {message.id} поддерживает комментарии")
            return True
        else:
            logger.info(f"Пост {message.id} не поддерживает комментарии")
            return False
        
    except Exception as e:
        logger.warning(f"Ошибка проверки комментариев поста: {e}")
        return False

async def send_comment_to_post(message, comment_text: str, channel_username: str):
    """Отправка комментария к посту с правильной обработкой ошибки вступления в группу"""
    try:
        if not shared_client:
            logger.warning("Telethon клиент не инициализирован")
            return False
        
        if not hasattr(message, 'replies') or not message.replies:
            logger.info(f"Пост {message.id} в канале {channel_username} не поддерживает комментарии")
            return False
        
        try:
            async def _get_discussion_message():
                return await shared_client(GetDiscussionMessageRequest(
                    peer=message.peer_id,
                    msg_id=message.id
                ))
            
            discussion_info = await handle_flood_wait(
                _get_discussion_message,
                operation_name="get_discussion_info"
            )
            
            if not discussion_info or not discussion_info.messages:
                logger.warning(f"Не удалось получить информацию о группе обсуждений для поста {message.id} в канале {channel_username}")
                return False
            
            discussion_group = discussion_info.messages[0].peer_id
            reply_to_msg_id = discussion_info.messages[0].id
            
        except Exception as e:
            error_str = str(e).lower()
            
            if "message id used in the peer was invalid" in error_str:
                logger.info(f"Пост {message.id} в канале {channel_username} не доступен для комментирования (недействительный ID)")
                return False
            elif "msg_id invalid" in error_str:
                logger.info(f"Недействительный ID сообщения {message.id} в канале {channel_username}")
                return False
            elif "peer_id_invalid" in error_str:
                logger.warning(f"Недействительный peer_id для канала {channel_username}")
                return False
            else:
                logger.error(f"Ошибка получения информации о группе обсуждений для поста {message.id}: {e}")
                return False
        
        async def _send_comment():
            return await shared_client.send_message(
                discussion_group,
                message=comment_text,
                reply_to=reply_to_msg_id
            )
        
        try:
            comment = await handle_flood_wait(
                _send_comment,
                operation_name="send_comment"
            )
            
            if comment:
                logger.info(f"Комментарий успешно отправлен к посту {message.id} в {channel_username}")
                statistics['comments_sent'] += 1
                
                try:
                    import bot_interface
                    bot_interface.update_statistics(comments=1)
                    comment_link = f"https://t.me/{channel_username.replace('@', '')}/{message.id}"
                    post_link = f"https://t.me/{channel_username.replace('@', '')}/{message.id}"
                    bot_interface.add_processed_channel_statistics(channel_username, comment_link=comment_link, post_link=post_link)
                except:
                    pass
                
                return True
                
        except Exception as e:
            error_str = str(e).lower()
            logger.warning(f"Ошибка при отправке комментария в {channel_username}: {e}")
            
            join_required_patterns = [
                "you join the discussion group before commenting",
                "join the discussion group before commenting", 
                "must join the discussion group",
                "need to join the discussion group",
                "must join",
                "need to join"
            ]
            
            requires_join = any(pattern in error_str for pattern in join_required_patterns)
            
            if requires_join:
                logger.info(f"Требуется вступить в группу обсуждений для {channel_username}")
                
                try:
                    channel_entity = await handle_flood_wait(
                        lambda: shared_client.get_entity(message.peer_id),
                        operation_name="get_channel_entity_for_join"
                    )
                    
                    if not channel_entity:
                        logger.error(f"Не удалось получить сущность канала для {channel_username}")
                        return False
                    
                    full_channel = await handle_flood_wait(
                        lambda: shared_client(GetFullChannelRequest(channel=channel_entity)),
                        operation_name="get_full_channel_for_join"
                    )
                    
                    if not full_channel or not hasattr(full_channel.full_chat, 'linked_chat_id') or not full_channel.full_chat.linked_chat_id:
                        logger.error(f"Канал {channel_username} не имеет связанной группы обсуждений")
                        return False
                    
                    discussion_group_entity = await handle_flood_wait(
                        lambda: shared_client.get_entity(full_channel.full_chat.linked_chat_id),
                        operation_name="get_discussion_group_entity"
                    )
                    
                    if not discussion_group_entity:
                        logger.error(f"Не удалось получить сущность группы обсуждений для {channel_username}")
                        return False
                    
                    join_result = await handle_flood_wait(
                        lambda: shared_client(JoinChannelRequest(discussion_group_entity)),
                        operation_name="join_discussion_group"
                    )
                    
                    if join_result is None:
                        logger.error(f"Не удалось вступить в группу обсуждений для {channel_username}")
                        return False
                    
                    logger.info(f"Успешно вступили в группу обсуждений для {channel_username}")
                    
                    await asyncio.sleep(2)
                    
                    try:
                        comment = await handle_flood_wait(
                            _send_comment,
                            operation_name="send_comment_after_join"
                        )
                        
                        if comment:
                            logger.info(f"Комментарий успешно отправлен после вступления в группу {channel_username}")
                            statistics['comments_sent'] += 1
                            
                            try:
                                import bot_interface
                                bot_interface.update_statistics(comments=1)
                                comment_link = f"https://t.me/{channel_username.replace('@', '')}/{message.id}"
                                post_link = f"https://t.me/{channel_username.replace('@', '')}/{message.id}"
                                bot_interface.add_processed_channel_statistics(channel_username, comment_link=comment_link, post_link=post_link)
                            except:
                                pass
                            
                            return True
                        else:
                            logger.error(f"Не удалось отправить комментарий даже после вступления в группу {channel_username}")
                            return False
                            
                    except Exception as retry_error:
                        logger.error(f"Ошибка при повторной отправке комментария после вступления в группу {channel_username}: {retry_error}")
                        return False
                        
                except Exception as join_error:
                    logger.error(f"Критическая ошибка при попытке вступить в группу обсуждений для {channel_username}: {join_error}")
                    return False
            
            elif "message id used in the peer was invalid" in error_str:
                logger.warning(f"Пост {message.id} больше не доступен для комментирования в {channel_username}")
                return False
            elif "chat_write_forbidden" in error_str:
                logger.warning(f"Запрещена запись в группу обсуждений канала {channel_username}")
                return False
            elif "user_banned_in_channel" in error_str:
                logger.warning(f"Пользователь заблокирован в канале/группе {channel_username}")
                return False
            else:
                logger.error(f"Неизвестная ошибка при отправке комментария в {channel_username}: {e}")
                return False
        
        return False
                
    except Exception as e:
        logger.error(f"Критическая ошибка при отправке комментария: {e}")
        return False

async def save_masslooking_progress():
    """Сохранение прогресса масслукинга в базу данных"""
    try:
        from database import db
        
        serializable_queue = {}
        for username, data in channel_processing_queue.items():
            serializable_queue[username] = {
                'entity_id': data.get('entity_id'),
                'entity_username': data.get('entity_username'),
                'message_ids': data.get('message_ids', []),
                'total_posts': data.get('total_posts', 0),
                'posts_processed': data.get('posts_processed', 0),
                'last_processed': data.get('last_processed').isoformat() if data.get('last_processed') else None,
                'actions_performed': data.get('actions_performed', False),
                'found_topic': data.get('found_topic', 'Другое')
            }
        
        serializable_tracked = {}
        for username, data in tracked_channels.items():
            serializable_tracked[username] = {
                'entity_id': data.get('entity_id'),
                'last_message_id': data.get('last_message_id', 0)
            }
        
        progress_data = [
            ('masslooking_progress', masslooking_progress),
            ('processed_channels', list(processed_channels)),
            ('channel_processing_queue', serializable_queue),
            ('tracked_channels', serializable_tracked)
        ]
        
        for key, value in progress_data:
            await db.save_bot_state(key, value)
            
    except Exception as e:
        logger.error(f"Ошибка сохранения прогресса масслукинга: {e}")

async def load_masslooking_progress():
    """Загрузка прогресса масслукинга из базы данных"""
    global masslooking_progress, processed_channels, channel_processing_queue, tracked_channels
    try:
        from database import db
        
        saved_progress = await db.load_bot_state('masslooking_progress', {})
        if saved_progress:
            masslooking_progress.update(saved_progress)
        
        saved_channels = await db.load_bot_state('processed_channels', [])
        if saved_channels:
            processed_channels.update(saved_channels)
        
        saved_queue = await db.load_bot_state('channel_processing_queue', {})
        if saved_queue:
            for username, data in saved_queue.items():
                channel_processing_queue[username] = {
                    'entity_id': data.get('entity_id'),
                    'entity_username': data.get('entity_username', username),
                    'message_ids': data.get('message_ids', []),
                    'total_posts': data.get('total_posts', 0),
                    'posts_processed': data.get('posts_processed', 0),
                    'last_processed': datetime.fromisoformat(data['last_processed']) if data.get('last_processed') else None,
                    'actions_performed': data.get('actions_performed', False),
                    'found_topic': data.get('found_topic', 'Другое')
                }
        
        saved_tracked = await db.load_bot_state('tracked_channels', {})
        if saved_tracked:
            for username, data in saved_tracked.items():
                tracked_channels[username] = {
                    'entity_id': data.get('entity_id'),
                    'last_message_id': data.get('last_message_id', 0)
                }
        
        logger.info(f"Загружен прогресс масслукинга")
    except Exception as e:
        logger.error(f"Ошибка загрузки прогресса масслукинга: {e}")

async def prepare_channel_for_processing(username: str, user_id: int):
    """Подготовка канала к обработке с детальным логированием"""
    try:
        queue = user_channel_processing_queue.get(user_id, {})
        processed = user_processed_channels.get(user_id, set())
        
        if username in queue:
            logger.info(f"Канал {username} уже в очереди обработки для пользователя {user_id}")
            return False
        
        if username in processed:
            logger.info(f"Канал {username} уже был полностью обработан для пользователя {user_id}")
            return False
        
        logger.info(f"Начинаем подготовку канала {username} для пользователя {user_id}")
        
        client = user_clients.get(user_id)
        if not client:
            logger.error(f"Нет клиента для пользователя {user_id}")
            return False
        
        entity = await get_entity_safe(username)
        if not entity:
            logger.error(f"Не удалось получить сущность канала {username}")
            return False
        
        logger.info(f"Сущность канала {username} получена: ID={entity.id}")
        
        full_channel = await get_full_channel_safe(entity)
        if not full_channel:
            logger.error(f"Не удалось получить полную информацию о канале {username}")
            return False
        
        logger.info(f"Полная информация о канале {username} получена")
        
        if not (hasattr(full_channel.full_chat, 'linked_chat_id') and full_channel.full_chat.linked_chat_id):
            logger.warning(f"Канал {username} не имеет группы обсуждений")
            return False
        
        logger.info(f"Канал {username} имеет группу обсуждений")
        
        delay_success = await apply_subscription_delay(username, "подписки")
        if not delay_success:
            logger.error(f"Прервана задержка подписки для канала {username}")
            return False
        
        join_result = await join_channel_safe(entity)
        if join_result is None:
            logger.error(f"Не удалось вступить в канал {username}")
            return False
        
        logger.info(f"Успешно подписались на канал {username}")
        
        try:
            settings = user_settings.get(user_id, {})
            posts_range = settings.get('posts_range', (1, 5))
            limit = posts_range[1] if isinstance(posts_range, (list, tuple)) and len(posts_range) >= 2 else 5
            
            fetch_limit = min(limit * 10, 200)
            logger.info(f"Начинаем получение до {fetch_limit} сообщений из канала {username}")
            
            all_messages = []
            message_count = 0
            
            try:
                logger.info(f"Подключаемся к каналу {username} для получения сообщений")
                
                async for message in client.iter_messages(entity, limit=fetch_limit):
                    message_count += 1
                    
                    if message and hasattr(message, 'id') and message.id:
                        all_messages.append(message)
                        
                        if message_count % 20 == 0:
                            logger.info(f"Получено {message_count} сообщений")
                            await asyncio.sleep(1)
                            
                logger.info(f"Завершено получение сообщений из {username}: получено {len(all_messages)} валидных")
                
            except Exception as fetch_error:
                logger.error(f"Ошибка при получении сообщений из канала {username}: {fetch_error}")
                await leave_channel_safe(entity)
                return False
            
            if not all_messages:
                logger.error(f"Не получено ни одного сообщения из канала {username}")
                await leave_channel_safe(entity)
                return False
            
            logger.info(f"Начинаем фильтрацию {len(all_messages)} сообщений из канала {username}")
            
            message_ids = []
            valid_message_count = 0
            
            for i, message in enumerate(all_messages):
                if not hasattr(message, 'id') or not message.id or message.id <= 0:
                    continue
                
                text = extract_message_text(message)
                has_text = text and len(text.strip()) > 0
                has_media = hasattr(message, 'media') and message.media
                
                if has_text or has_media:
                    message_ids.append(message.id)
                    valid_message_count += 1
                    
                    if len(message_ids) >= limit:
                        logger.info(f"Достигнуто целевое количество сообщений: {limit}")
                        break
            
            logger.info(f"Статистика фильтрации сообщений из {username}:")
            logger.info(f"  Всего получено: {len(all_messages)}")
            logger.info(f"  Подходящих найдено: {valid_message_count}")
            logger.info(f"  ID добавлено в очередь: {len(message_ids)}")
            
            if not message_ids:
                logger.error(f"В канале {username} нет подходящих сообщений для комментирования")
                await leave_channel_safe(entity)
                return False
            
            logger.info(f"Найдено {len(message_ids)} подходящих сообщений для обработки в канале {username}")
            
            if user_id not in user_channel_processing_queue:
                user_channel_processing_queue[user_id] = {}
            
            user_channel_processing_queue[user_id][username] = {
                'entity_id': entity.id,
                'entity_username': username,
                'message_ids': message_ids,
                'total_posts': len(message_ids),
                'posts_processed': 0,
                'last_processed': None,
                'actions_performed': False
            }
            
            logger.info(f"Канал {username} успешно подготовлен для обработки пользователем {user_id}")
            return True
            
        except Exception as e:
            logger.error(f"Ошибка при получении сообщений канала {username}: {e}")
            await leave_channel_safe(entity)
            return False
        
    except Exception as e:
        logger.error(f"Ошибка подготовки канала {username}: {e}")
        return False

async def process_single_post_from_channel(username: str, user_id: int) -> bool:
    """Обработка одного поста с получением сообщения по ID"""
    try:
        # ИСПРАВЛЕНИЕ: инициализируем user_processed_channels если не существует
        if user_id not in user_processed_channels:
            user_processed_channels[user_id] = set()
            
        queue = user_channel_processing_queue.get(user_id, {})
        if username not in queue:
            logger.warning(f"Канал {username} не найден в очереди обработки для пользователя {user_id}")
            return False
        
        channel_data = queue[username]
        client = user_clients.get(user_id)
        
        if not client:
            logger.error(f"Нет клиента для пользователя {user_id}")
            return False
        
        if channel_data['posts_processed'] >= channel_data['total_posts']:
            logger.info(f"Все посты канала {username} обработаны для пользователя {user_id}")
            return False
        
        subscription_ok = await ensure_subscription(username)
        if not subscription_ok:
            logger.error(f"Не удалось обеспечить подписку на канал {username}")
            return False
        
        message_ids = channel_data.get('message_ids', [])
        if not message_ids or len(message_ids) == 0:
            logger.error(f"Нет ID сообщений для канала {username}")
            channel_data['posts_processed'] = channel_data['total_posts']
            return False
        
        posts_processed = channel_data['posts_processed']
        if posts_processed >= len(message_ids):
            logger.warning(f"Индекс {posts_processed} выходит за границы массива ID сообщений для канала {username}")
            channel_data['posts_processed'] = channel_data['total_posts']
            return False
        
        current_message_id = message_ids[posts_processed]
        
        logger.info(f"Получаем сообщение {current_message_id} из канала {username} ({posts_processed + 1}/{len(message_ids)})")
        
        try:
            entity = await get_entity_safe(username)
            if not entity:
                logger.error(f"Не удалось получить сущность канала {username}")
                channel_data['posts_processed'] += 1
                return True
        except Exception as e:
            logger.error(f"Ошибка получения сущности канала {username}: {e}")
            channel_data['posts_processed'] += 1
            return True
        
        current_post = None
        try:
            messages = await client.get_messages(entity, ids=current_message_id)
            if messages and isinstance(messages, list) and len(messages) > 0:
                current_post = messages[0]
            elif messages and not isinstance(messages, list):
                current_post = messages
            else:
                logger.warning(f"Сообщение {current_message_id} не найдено")
                channel_data['posts_processed'] += 1
                return True
        except Exception as e:
            logger.error(f"Ошибка получения сообщения {current_message_id}: {e}")
            channel_data['posts_processed'] += 1
            return True
        
        if not current_post:
            logger.warning(f"Получен пустой объект сообщения {current_message_id} в канале {username}")
            channel_data['posts_processed'] += 1
            return True
        
        if not hasattr(current_post, 'id') or current_post.id != current_message_id:
            logger.warning(f"ID сообщения не совпадает")
            channel_data['posts_processed'] += 1
            return True
       
        if not has_commentable_content(current_post):
            logger.info(f"Сообщение {current_message_id} в канале {username} больше не подходит для комментирования")
            channel_data['posts_processed'] += 1
            return True
        
        post_text = extract_message_text(current_post)
        if not post_text:
            logger.info(f"Пост {current_message_id} в канале {username} не содержит текста для комментирования")
            channel_data['posts_processed'] += 1
            return True
        
        logger.info(f"Обрабатываем пост {current_message_id} в канале {username}")
        
        channel_topic = channel_data.get('found_topic', 'Другое')
        
        comment_sent = False
        reaction_added = False
        actions_performed = False
        
        try:
            comment = await generate_comment(post_text, [channel_topic], current_post)
            
            if comment:
                comments_available = await check_post_comments_available(current_post)
                
                if comments_available:
                    logger.info(f"Отправляем комментарий к посту {current_message_id} в канале {username}")
                    comment_sent = await send_comment_to_post(current_post, comment, username)
                    if comment_sent:
                        logger.info(f"Комментарий успешно отправлен к посту {current_message_id} в канале {username}")
                        actions_performed = True
                        
                        # ИСПРАВЛЕНИЕ: инициализируем user_statistics если не существует
                        if user_id not in user_statistics:
                            user_statistics[user_id] = {
                                'comments_sent': 0,
                                'reactions_set': 0,
                                'channels_processed': 0,
                                'errors': 0,
                                'flood_waits': 0,
                                'total_flood_wait_time': 0
                            }
                        
                        user_stats = user_statistics[user_id]
                        user_stats['comments_sent'] = user_stats.get('comments_sent', 0) + 1
                        user_statistics[user_id] = user_stats
                        
                        try:
                            import bot_interface
                            bot_interface.update_statistics(comments=1)
                            comment_link = f"https://t.me/{username.replace('@', '')}/{current_message_id}"
                            post_link = f"https://t.me/{username.replace('@', '')}/{current_message_id}"
                            bot_interface.add_processed_channel_statistics(username, comment_link=comment_link, post_link=post_link)
                        except Exception as e:
                            pass
                    else:
                        logger.warning(f"Не удалось отправить комментарий к посту {current_message_id} в канале {username}")
                else:
                    logger.info(f"Пост {current_message_id} в канале {username} не поддерживает комментарии")
            else:
                logger.warning(f"Не удалось сгенерировать комментарий для поста {current_message_id} в канале {username}")
        except Exception as e:
            logger.error(f"Ошибка при обработке комментария для поста {current_message_id} в канале {username}: {e}")
        
        try:
            logger.info(f"Добавляем реакцию к посту {current_message_id} в канале {username}")
            reaction_added = await add_reaction_to_post(current_post, username)
            if reaction_added:
                logger.info(f"Реакция добавлена к посту {current_message_id} в канале {username}")
                actions_performed = True
                
                # ИСПРАВЛЕНИЕ: инициализируем user_statistics если не существует
                if user_id not in user_statistics:
                    user_statistics[user_id] = {
                        'comments_sent': 0,
                        'reactions_set': 0,
                        'channels_processed': 0,
                        'errors': 0,
                        'flood_waits': 0,
                        'total_flood_wait_time': 0
                    }
                
                user_stats = user_statistics[user_id]
                user_stats['reactions_set'] = user_stats.get('reactions_set', 0) + 1
                user_statistics[user_id] = user_stats
                
                try:
                    import bot_interface
                    bot_interface.update_statistics(reactions=1)
                except Exception as e:
                    pass
            else:
                logger.warning(f"Не удалось добавить реакцию к посту {current_message_id} в канале {username}")
        except Exception as e:
            logger.error(f"Ошибка при добавлении реакции к посту {current_message_id} в канале {username}: {e}")
        
        channel_data['posts_processed'] += 1
        channel_data['last_processed'] = datetime.now()
        
        if actions_performed:
            channel_data['actions_performed'] = True
            logger.info(f"Выполнены действия для поста {current_message_id} в канале {username}")
        else:
            logger.warning(f"Не выполнено ни одного действия для поста {current_message_id} в канале {username}")
        
        return True
        
    except Exception as e:
        logger.error(f"Критическая ошибка обработки поста из канала {username} для пользователя {user_id}: {e}")
        # ИСПРАВЛЕНИЕ: инициализируем user_statistics если не существует
        if user_id not in user_statistics:
            user_statistics[user_id] = {
                'comments_sent': 0,
                'reactions_set': 0,
                'channels_processed': 0,
                'errors': 0,
                'flood_waits': 0,
                'total_flood_wait_time': 0
            }
        user_stats = user_statistics[user_id]
        user_stats['errors'] = user_stats.get('errors', 0) + 1
        user_statistics[user_id] = user_stats
        return False

async def finalize_channel_processing(username: str, user_id: int):
    """Завершение обработки канала с улучшенной обработкой отписки"""
    try:
        queue = user_channel_processing_queue.get(user_id, {})
        channel_data = queue.get(username)
        if not channel_data:
            logger.warning(f"Данные канала {username} не найдены при финализации для пользователя {user_id}")
            return
        
        actions_performed = channel_data.get('actions_performed', False)
        
        if not actions_performed:
            logger.warning(f"В канале {username} не было выполнено ни одного действия для пользователя {user_id}")
        else:
            if user_id not in user_processed_channels:
                user_processed_channels[user_id] = set()
            user_processed_channels[user_id].add(username)
            
            user_stats = user_statistics.get(user_id, {})
            user_stats['channels_processed'] = user_stats.get('channels_processed', 0) + 1
            user_statistics[user_id] = user_stats
            
            logger.info(f"Канал {username} добавлен в список полностью обработанных каналов для пользователя {user_id}")
            
            try:
                import bot_interface
                bot_interface.update_statistics(channels=1)
            except:
                pass
        
        settings = user_settings.get(user_id, {})
        track_new_posts = settings.get('track_new_posts', False)

        try:
            client = user_clients.get(user_id)
            if not client:
                logger.warning(f"Нет клиента для финализации канала {username} для пользователя {user_id}")
            else:
                entity = await get_entity_safe(username)
                if not entity:
                    logger.warning(f"Не удалось получить сущность канала {username} для финализации")
                else:
                    if track_new_posts and actions_performed:
                        logger.info(f"Добавляем канал {username} в отслеживание новых постов для пользователя {user_id}")
                        try:
                            messages = await iter_messages_safe(entity, limit=1)
                            last_message_id = messages[0].id if messages else 0
                            
                            if user_id not in user_tracked_channels:
                                user_tracked_channels[user_id] = {}
                            
                            user_tracked_channels[user_id][username] = {
                                'entity_id': entity.id,
                                'last_message_id': last_message_id
                            }
                            logger.info(f"Канал {username} добавлен в отслеживание для пользователя {user_id}")
                        except Exception as e:
                            logger.error(f"Ошибка добавления канала {username} в отслеживание для пользователя {user_id}: {e}")
                    else:
                        reason = "track_new_posts = False" if not track_new_posts else "не выполнены действия"
                        logger.info(f"Отписываемся от канала {username} ({reason})")
                        leave_result = await leave_channel_safe(entity)
                        if leave_result:
                            logger.info(f"Успешно отписались от канала {username}")
                        else:
                            logger.warning(f"Не удалось отписаться от канала {username}")
        except Exception as e:
            logger.error(f"Ошибка получения entity для финализации канала {username}: {e}")

        if username in queue:
            del queue[username]
        
    except Exception as e:
        logger.error(f"Ошибка при финализации канала {username} для пользователя {user_id}: {e}")
        user_stats = user_statistics.get(user_id, {})
        user_stats['errors'] = user_stats.get('errors', 0) + 1
        user_statistics[user_id] = user_stats

async def check_new_posts_in_tracked_channels(user_id: int):
    """Проверка новых постов в отслеживаемых каналах пользователя"""
    tracked_channels = user_tracked_channels.get(user_id, {})
    client = user_clients.get(user_id)
    
    if not tracked_channels or not client:
        return
    
    logger.info(f"Проверяем новые посты в {len(tracked_channels)} отслеживаемых каналах для пользователя {user_id}")
    
    for username, channel_data in list(tracked_channels.items()):
        try:
            if not check_bot_running(user_id):
                logger.info(f"Остановка запрошена, прерываем проверку новых постов для пользователя {user_id}")
                break
            
            await asyncio.sleep(2)
            
        except Exception as e:
            logger.error(f"Ошибка проверки новых постов в канале {username} для пользователя {user_id}: {e}")
            continue
    
    logger.info(f"Проверка новых постов завершена для пользователя {user_id}")

async def new_post_tracking_worker(user_id: int):
    """Рабочий процесс отслеживания новых постов для пользователя"""
    logger.info(f"Запущен worker отслеживания новых постов для пользователя {user_id}")
    
    while user_masslooking_active.get(user_id, False):
        try:
            if not check_bot_running(user_id):
                logger.info(f"Остановка запрошена, завершаем отслеживание новых постов для пользователя {user_id}")
                break
            
            tracked_channels = user_tracked_channels.get(user_id, {})
            if tracked_channels:
                await check_new_posts_in_tracked_channels(user_id)
            
            for _ in range(300):
                if not check_bot_running(user_id) or not user_masslooking_active.get(user_id, False):
                    break
                await asyncio.sleep(1)
                
        except Exception as e:
            logger.error(f"Ошибка в worker отслеживания новых постов для пользователя {user_id}: {e}")
            await asyncio.sleep(60)
    
    logger.info(f"Worker отслеживания новых постов завершен для пользователя {user_id}")

async def start_new_post_tracking(user_id: int):
    """Запуск отслеживания новых постов для пользователя"""
    try:
        asyncio.create_task(new_post_tracking_worker(user_id))
        logger.info(f"Отслеживание новых постов запущено для пользователя {user_id}")
    except Exception as e:
        logger.error(f"Ошибка запуска отслеживания новых постов для пользователя {user_id}: {e}")

async def stop_new_post_tracking(user_id: int):
    """Остановка отслеживания новых постов для пользователя"""
    logger.info(f"Отслеживание новых постов остановлено для пользователя {user_id}")

async def masslooking_worker(user_id: int):
    """Рабочий процесс масслукинга для конкретного пользователя"""
    logger.info(f"Рабочий процесс масслукинга запущен для пользователя {user_id}")
    
    while user_masslooking_active.get(user_id, False):
        try:
            if not check_bot_running(user_id):
                logger.info(f"Остановка запрошена для пользователя {user_id}")
                user_masslooking_active[user_id] = False
                break
            
            client = user_clients.get(user_id)
            if not client:
                logger.error(f"Нет клиента для пользователя {user_id}")
                user_masslooking_active[user_id] = False
                break
            
            try:
                # ИСПРАВЛЕНИЕ: правильный порядок объявления global переменных
                global shared_client, settings
                shared_client = client
                settings = user_settings.get(user_id, {})
                
                # Проверяем есть ли каналы в очереди для обработки
                queue = user_channel_processing_queue.get(user_id, {})
                if queue:
                    logger.info(f"В очереди пользователя {user_id} найдено {len(queue)} каналов для обработки")
                    
                    # Обрабатываем один канал из очереди
                    for username in list(queue.keys()):
                        if not check_bot_running(user_id):
                            break
                        
                        try:
                            success = await process_single_post_from_channel(username, user_id)
                            if success:
                                logger.info(f"Успешно обработан пост в канале {username} для пользователя {user_id}")
                                
                                # Обновляем статистику пользователя
                                channel_data = queue[username]
                                if channel_data['posts_processed'] >= channel_data['total_posts']:
                                    # Канал полностью обработан
                                    await finalize_channel_processing(username, user_id)
                                    # ИСПРАВЛЕНИЕ: инициализируем user_processed_channels если не существует
                                    if user_id not in user_processed_channels:
                                        user_processed_channels[user_id] = set()
                                    user_processed_channels[user_id].add(username)
                                    
                                    # ИСПРАВЛЕНИЕ: инициализируем user_statistics если не существует
                                    if user_id not in user_statistics:
                                        user_statistics[user_id] = {
                                            'comments_sent': 0,
                                            'reactions_set': 0,
                                            'channels_processed': 0,
                                            'errors': 0,
                                            'flood_waits': 0,
                                            'total_flood_wait_time': 0
                                        }
                                    user_statistics[user_id]['channels_processed'] += 1
                                    del queue[username]
                                    logger.info(f"Канал {username} полностью обработан для пользователя {user_id}")
                            else:
                                logger.warning(f"Не удалось обработать пост в канале {username}")
                                
                        except Exception as e:
                            logger.error(f"Ошибка обработки канала {username} для пользователя {user_id}: {e}")
                            # Удаляем проблемный канал из очереди
                            if username in queue:
                                del queue[username]
                        
                        break  # Обрабатываем по одному каналу за итерацию
                else:
                    logger.info(f"Нет каналов в очереди для пользователя {user_id}, ждем новые каналы...")
                    await asyncio.sleep(30)
                
            except Exception as e:
                logger.error(f"Ошибка в логике масслукинга для пользователя {user_id}: {e}")
                await asyncio.sleep(60)
            
            # Обновляем настройки пользователя из БД
            try:
                import bot_interface
                user_session = await bot_interface.db.load_user_session(user_id)
                new_settings = user_session.get('settings', {})
                if new_settings and new_settings != user_settings.get(user_id, {}):
                    user_settings[user_id].update(new_settings)
                    logger.info(f"Настройки масслукинга обновлены для пользователя {user_id}")
            except Exception as e:
                logger.warning(f"Не удалось обновить настройки для пользователя {user_id}: {e}")
            
            # Основная задержка между итерациями
            await asyncio.sleep(10)
            
        except Exception as e:
            logger.error(f"Ошибка в рабочем процессе масслукинга для пользователя {user_id}: {e}")
            await asyncio.sleep(30)
    
    logger.info(f"Рабочий процесс масслукинга завершен для пользователя {user_id}")

async def add_channel_to_queue(username: str, user_id: int = None):
    """Добавление канала в очередь обработки для конкретного пользователя"""
    if not user_id:
        # ИСПРАВЛЕНИЕ: если user_id не передан, используем глобальную логику как в оригинале
        # ИСПРАВЛЕНИЕ: правильный порядок объявления global переменных
        global shared_client, settings
        settings = settings if 'settings' in globals() else {}
        
        max_channels = settings.get('max_channels', 150)
        
        if max_channels != float('inf') and len(processed_channels) >= max_channels:
            logger.info(f"Достигнут лимит полностью обработанных каналов ({max_channels}), канал {username} не добавлен в очередь")
            return
        
        if username not in processed_channels and username not in channel_processing_queue:
            success = await prepare_channel_for_processing(username)
            if success:
                logger.info(f"Канал {username} добавлен в очередь круговой обработки")
                
                try:
                    import bot_interface
                    queue_list = list(channel_processing_queue.keys())
                    bot_interface.update_queue_statistics(queue_list)
                except:
                    pass
            else:
                logger.warning(f"Не удалось подготовить канал {username} для обработки")
        else:
            logger.info(f"Канал {username} уже в обработке или полностью обработан, пропускаем")
        return
    
    # ИСПРАВЛЕНИЕ: используем реальную логику подготовки канала для конкретного пользователя
    if user_id not in user_channel_processing_queue:
        user_channel_processing_queue[user_id] = {}
    
    # ИСПРАВЛЕНИЕ: инициализируем user_processed_channels если не существует
    if user_id not in user_processed_channels:
        user_processed_channels[user_id] = set()
    
    queue = user_channel_processing_queue[user_id]
    processed = user_processed_channels[user_id]
    
    if username in processed:
        logger.info(f"Канал {username} уже обработан для пользователя {user_id}")
        return
    
    if username in queue:
        logger.info(f"Канал {username} уже в очереди пользователя {user_id}")
        return
    
    # Устанавливаем глобальный клиент для совместимости с существующими функциями
    global shared_client, settings
    shared_client = user_clients.get(user_id)
    settings = user_settings.get(user_id, {})
    
    if not shared_client:
        logger.error(f"Нет клиента для подготовки канала {username} для пользователя {user_id}")
        return
    
    # ИСПРАВЛЕНИЕ: используем существующую функцию подготовки канала
    success = await prepare_channel_for_processing(username, user_id)
    if success:
        # Копируем подготовленные данные в очередь пользователя
        if username in channel_processing_queue:
            queue[username] = channel_processing_queue[username].copy()
            # Удаляем из глобальной очереди
            del channel_processing_queue[username]
            logger.info(f"Канал {username} добавлен в очередь пользователя {user_id}")
        else:
            logger.warning(f"Канал {username} не найден в глобальной очереди после подготовки")
    else:
        logger.warning(f"Не удалось подготовить канал {username} для пользователя {user_id}")

async def start_masslooking(telegram_client: TelegramClient, masslooking_settings: dict, user_id: int):
    """Запуск масслукинга для конкретного пользователя"""
    global user_clients, user_settings, user_processed_channels, user_statistics, user_masslooking_active
    
    if user_id in user_clients and user_masslooking_active.get(user_id, False):
        logger.warning(f"Масслукинг для пользователя {user_id} уже запущен")
        return
    
    logger.info(f"Запуск масслукинга для пользователя {user_id}")
    
    user_clients[user_id] = telegram_client
    user_settings[user_id] = masslooking_settings.copy()
    user_processed_channels[user_id] = set()
    user_channel_processing_queue[user_id] = {}
    user_tracked_channels[user_id] = {}
    user_masslooking_active[user_id] = True
    user_statistics[user_id] = {
        'comments_sent': 0,
        'reactions_set': 0,
        'channels_processed': 0,
        'errors': 0,
        'flood_waits': 0,
        'total_flood_wait_time': 0
    }
    
    # ИСПРАВЛЕНИЕ: запускаем реальный worker процесс
    logger.info(f"Запуск рабочего процесса масслукинга для пользователя {user_id}")
    asyncio.create_task(masslooking_worker(user_id))
    
    # ИСПРАВЛЕНИЕ: запускаем отслеживание новых постов если включено
    if masslooking_settings.get('track_new_posts', False):
        await start_new_post_tracking(user_id)
    
    logger.info(f"Масслукинг запущен для пользователя {user_id}")

async def stop_masslooking(user_id: int):
    """Остановка масслукинга для конкретного пользователя"""
    global user_clients, user_settings, user_processed_channels, user_statistics, user_masslooking_active
    
    logger.info(f"Остановка масслукинга для пользователя {user_id}")
    
    if user_id in user_masslooking_active:
        user_masslooking_active[user_id] = False
    
    await stop_new_post_tracking(user_id)
    
    if user_id in user_clients:
        try:
            client = user_clients[user_id]
            if client and client.is_connected():
                await client.disconnect()
        except Exception as e:
            logger.error(f"Ошибка отключения клиента для пользователя {user_id}: {e}")
        
        del user_clients[user_id]
    
    user_settings.pop(user_id, None)
    user_processed_channels.pop(user_id, None)
    user_channel_processing_queue.pop(user_id, None)
    user_tracked_channels.pop(user_id, None)
    user_statistics.pop(user_id, None)
    
    logger.info(f"Масслукинг остановлен для пользователя {user_id}")

def get_statistics(user_id: int = None):
    """Получение статистики масслукинга для пользователя"""
    if user_id and user_id in user_statistics:
        stats = user_statistics[user_id].copy()
        queue_size = len(user_channel_processing_queue.get(user_id, {}))
        tracked_count = len(user_tracked_channels.get(user_id, {}))
        
        stats.update({
            'queue_size': queue_size,
            'tracked_channels_count': tracked_count,
            'masslooking_active': user_masslooking_active.get(user_id, False)
        })
        return stats
    elif user_id is None:
        return {uid: {
            **stats,
            'queue_size': len(user_channel_processing_queue.get(uid, {})),
            'tracked_channels_count': len(user_tracked_channels.get(uid, {})),
            'masslooking_active': user_masslooking_active.get(uid, False)
        } for uid, stats in user_statistics.items()}
    else:
        return {
            'comments_sent': 0,
            'reactions_set': 0,
            'channels_processed': 0,
            'errors': 0,
            'flood_waits': 0,
            'total_flood_wait_time': 0,
            'queue_size': 0,
            'tracked_channels_count': 0,
            'masslooking_active': False
        }

def reset_statistics():
    """Сброс статистики"""
    global statistics, masslooking_progress, first_subscription_made
    statistics = {
        'comments_sent': 0,
        'reactions_set': 0,
        'channels_processed': 0,
        'errors': 0,
        'flood_waits': 0,
        'total_flood_wait_time': 0
    }
    masslooking_progress = {'current_channel': '', 'processed_count': 0}
    first_subscription_made = False

def update_flood_wait_settings(new_settings: dict):
    """Обновление настроек FloodWait"""
    global FLOOD_WAIT_SETTINGS
    FLOOD_WAIT_SETTINGS.update(new_settings)
    logger.info(f"Настройки FloodWait обновлены: {FLOOD_WAIT_SETTINGS}")

async def main():
    """Тестирование модуля"""
    test_settings = {
        'delay_range': (5, 10),
        'posts_range': (1, 3),
        'max_channels': 5,
        'track_new_posts': False
    }
    
    logger.info("Тестирование модуля masslooker...")
    print("Статистика масслукинга:", get_statistics())

if __name__ == "__main__":
    asyncio.run(main())