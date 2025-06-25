import asyncio
import logging
import time
import random
import threading
import os
from datetime import datetime
from typing import List, Tuple, Set, Optional
import re

logger = logging.getLogger(__name__)

try:
    from seleniumbase import Driver
    from selenium.webdriver.common.by import By
    from selenium.webdriver.support.ui import WebDriverWait
    from selenium.webdriver.support import expected_conditions as EC
    from selenium.webdriver.common.keys import Keys
    from selenium.common.exceptions import TimeoutException, WebDriverException
    from telethon import TelegramClient
    from telethon.errors import ChannelPrivateError, ChannelInvalidError, FloodWaitError
    from telethon.tl.functions.channels import GetFullChannelRequest
    import g4f
except ImportError as e:
    logger.error(f"Ошибка импорта библиотек: {e}")
    raise

# Глобальные переменные
search_active = False
found_channels: Set[str] = set()
driver = None
current_settings = {}
shared_telethon_client = None
search_progress = {'current_keyword': '', 'current_topic': ''}
sent_to_queue_count = 0

user_search_active = {}
user_found_channels = {}
user_drivers = {}
user_settings = {}
user_telethon_clients = {}
user_search_progress = {}
user_sent_to_queue_count = {}

last_api_call_time = 0
api_call_interval = 1.0
flood_wait_delays = {}

async def handle_flood_wait(func, *args, **kwargs):
    """Универсальная обертка для обработки FloodWaitError"""
    max_retries = 3
    retry_count = 0
    
    while retry_count < max_retries:
        try:
            global last_api_call_time, api_call_interval
            current_time = time.time()
            time_since_last_call = current_time - last_api_call_time
            
            if time_since_last_call < api_call_interval:
                wait_time = api_call_interval - time_since_last_call
                await asyncio.sleep(wait_time)
            
            last_api_call_time = time.time()
            
            if asyncio.iscoroutinefunction(func):
                result = await func(*args, **kwargs)
            else:
                result = func(*args, **kwargs)
            
            return result
            
        except FloodWaitError as e:
            retry_count += 1
            wait_seconds = e.seconds
            
            logger.warning(f"FloodWaitError: необходимо подождать {wait_seconds} секунд. Попытка {retry_count}/{max_retries}")
            
            api_call_interval = min(api_call_interval * 1.5, 10.0)
           
            if retry_count >= max_retries:
                logger.error(f"Достигнуто максимальное количество попыток для обработки FloodWait")
                raise
            
            try:
                import bot_interface
                user_session = asyncio.get_event_loop().run_until_complete(bot_interface.db.load_user_session(kwargs.get('user_id', 0)))
                if not user_session.get('is_running', True):
                    logger.info("Остановка запрошена во время FloodWait, прерываем ожидание")
                    raise asyncio.CancelledError("Остановка запрошена")
            except ImportError:
                pass
            
            remaining_wait = wait_seconds
            while remaining_wait > 0:
                check_interval = min(10, remaining_wait)
                await asyncio.sleep(check_interval)
                remaining_wait -= check_interval
                
                try:
                    import bot_interface
                    user_session = asyncio.get_event_loop().run_until_complete(bot_interface.db.load_user_session(kwargs.get('user_id', 0)))
                    if not user_session.get('is_running', True):
                        logger.info("Остановка запрошена во время FloodWait, прерываем ожидание")
                        raise asyncio.CancelledError("Остановка запрошена")
                except ImportError:
                    pass
                
                if remaining_wait > 0:
                    logger.info(f"FloodWait: осталось ждать {remaining_wait} секунд")
            
            logger.info(f"Ожидание FloodWait завершено, повторяем попытку {retry_count + 1}")
            
        except Exception as e:
            raise e
    
    raise Exception(f"Не удалось выполнить операцию после {max_retries} попыток")

def check_bot_running(user_id: int) -> bool:
    """Проверка состояния работы бота для пользователя"""
    try:
        # ИСПРАВЛЕНИЕ: используем прямую проверку из БД без вызова событийного цикла внутри цикла
        import bot_interface
        import asyncio
        
        # Получаем event loop безопасно
        try:
            loop = asyncio.get_event_loop()
            if loop.is_running():
                # ИСПРАВЛЕНИЕ: если цикл уже запущен, используем локальное состояние
                return user_search_active.get(user_id, False)
        except RuntimeError:
            return user_search_active.get(user_id, False)
        
        # Проверяем состояние пользователя из БД только если цикл не запущен
        try:
            user_session = loop.run_until_complete(bot_interface.db.load_user_session(user_id))
            is_running = user_session.get('is_running', False)
            logger.debug(f"Состояние пользователя {user_id} из БД: is_running={is_running}")
            return is_running
        except Exception as e:
            logger.warning(f"Ошибка проверки состояния пользователя {user_id} из БД: {e}")
            return user_search_active.get(user_id, False)
        
    except Exception as e:
        logger.warning(f"Ошибка проверки состояния бота для пользователя {user_id}: {e}")
        return user_search_active.get(user_id, False)

def setup_driver_threaded():
    """Настройка и инициализация веб-драйвера в отдельном потоке"""
    logger.info("Инициализация веб-драйвера")
    
    try:
        driver = Driver(uc=True, headless=False)
        driver.set_window_size(600, 1200)
        
        desktop_user_agent = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
        driver.execute_cdp_cmd('Network.setUserAgentOverride', {"userAgent": desktop_user_agent})
        
        driver.get("about:blank")
        logger.info("Веб-драйвер успешно инициализирован")
        
        return driver
    except Exception as e:
        logger.error(f"Ошибка инициализации драйвера: {e}")
        return None

def wait_and_find_element(driver, selectors, timeout=5):
    """Поиск элемента по нескольким селекторам"""
    if isinstance(selectors, str):
        selectors = [selectors]
    
    for selector in selectors:
        try:
            if selector.startswith('//'):
                element = WebDriverWait(driver, timeout).until(
                    EC.presence_of_element_located((By.XPATH, selector))
                )
            else:
                element = WebDriverWait(driver, timeout).until(
                    EC.presence_of_element_located((By.CSS_SELECTOR, selector))
                )
            return element
        except TimeoutException:
            continue
        except Exception as e:
            logger.warning(f"Ошибка поиска элемента {selector}: {e}")
            continue
    
    return None

def wait_and_click_element(driver, selectors, timeout=5):
    """Клик по элементу с ожиданием его доступности"""
    element = wait_and_find_element(driver, selectors, timeout)
    if element:
        try:
            if isinstance(selectors, str):
                selectors = [selectors]
            
            for selector in selectors:
                try:
                    if selector.startswith('//'):
                        clickable_element = driver.find_element(By.XPATH, selector)
                    else:
                        clickable_element = driver.find_element(By.CSS_SELECTOR, selector)
                    
                    if clickable_element.is_displayed() and clickable_element.is_enabled():
                        clickable_element.click()
                        return True
                    else:
                        time.sleep(0.1)
                        if clickable_element.is_displayed() and clickable_element.is_enabled():
                            clickable_element.click()
                            return True
                except:
                    continue
        except Exception as e:
            logger.warning(f"Ошибка клика по элементу: {e}")
    
    return False

def navigate_to_channel_search(driver):
    """Навигация к странице поиска каналов"""
    try:
        logger.info("Переход на tgstat.ru...")
        driver.get("https://tgstat.ru/")
        time.sleep(1)
        
        logger.info("Открываем меню...")
        menu_selectors = [
            'a.d-flex.d-lg-none.nav-user',
            '.nav-user',
            '[data-toggle="collapse"]',
            'i.uil-bars'
        ]
        
        if not wait_and_click_element(driver, menu_selectors, 3):
            logger.warning("Не удалось найти кнопку меню")
        
        time.sleep(0.5)
        
        logger.info("Открываем каталог...")
        catalog_selectors = [
            '#topnav-catalog',
            'a[id="topnav-catalog"]',
            '.nav-link.dropdown-toggle',
            '//a[contains(text(), "Каталог")]'
        ]
        
        if not wait_and_click_element(driver, catalog_selectors, 3):
            logger.error("Не удалось найти кнопку каталога")
            return False
        
        time.sleep(0.3)
        
        logger.info("Переходим к поиску каналов...")
        search_selectors = [
            'a[href="/channels/search"]',
            '//a[contains(text(), "Поиск каналов")]',
            '.dropdown-item[href="/channels/search"]'
        ]
        
        if not wait_and_click_element(driver, search_selectors, 3):
            logger.error("Не удалось найти кнопку поиска каналов")
            return False
        
        time.sleep(1)
        logger.info("Успешно перешли на страницу поиска каналов")
        return True
        
    except Exception as e:
        logger.error(f"Ошибка навигации: {e}")
        return False

def search_channels_sync(driver, keyword: str, topic: str, first_search: bool = False):
    """Функция поиска каналов"""
    try:
        logger.info(f"Поиск каналов по ключевому слову: '{keyword}', тема: '{topic}'")
        
        keyword_input = wait_and_find_element(driver, [
            'input[name="q"]',
            '#q',
            '.form-control',
            'input[placeholder*="канал"]',
            'input.form-control'
        ])
        
        if keyword_input:
            keyword_input.clear()
            time.sleep(0.1)
            keyword_input.send_keys(keyword)
            logger.info(f"Введено ключевое слово: {keyword}")
        else:
            logger.error("Не удалось найти поле ввода ключевого слова")
            return []
        
        topic_input = wait_and_find_element(driver, [
            '.select2-search__field',
            'input[role="searchbox"]',
            '.select2-search input'
        ])
        
        if topic_input:
            topic_input.clear()
            time.sleep(0.1)
            topic_input.send_keys(topic)
            time.sleep(0.3)
            topic_input.send_keys(Keys.ENTER)
            logger.info(f"Введена тема: {topic}")
        else:
            logger.error("Не удалось найти поле ввода темы")
            return []
        
        if first_search:
            description_checkbox = wait_and_find_element(driver, [
                '#inabout',
                'input[name="inAbout"]',
                '.custom-control-input[name="inAbout"]'
            ])
            
            if description_checkbox and not description_checkbox.is_selected():
                driver.execute_script("arguments[0].click();", description_checkbox)
                logger.info("Отмечен поиск в описании")
            
            channel_type_select = wait_and_find_element(driver, [
                '#channeltype',
                'select[name="channelType"]',
                '.custom-select[name="channelType"]'
            ])
            
            if channel_type_select:
                driver.execute_script("arguments[0].value = 'public';", channel_type_select)
                logger.info("Выбран тип канала: публичный")
        
        search_button = wait_and_find_element(driver, [
            '#search-form-submit-btn',
            'button[type="button"].btn-primary',
            '.btn.btn-primary.w-100'
        ])
        
        if search_button:
            driver.execute_script("arguments[0].click();", search_button)
            logger.info("Нажата кнопка поиска")
        else:
            logger.error("Не удалось найти кнопку поиска")
            return []
       
        wait_time = 0
        max_wait = 10
        while wait_time < max_wait:
            time.sleep(1)
            wait_time += 1
            
            try:
                results = driver.find_elements(By.CSS_SELECTOR, '.card.peer-item-row, .peer-item-row')
                if results:
                    break
            except:
                continue
        
        channels = extract_channel_usernames_sync(driver)
        logger.info(f"Найдено каналов: {len(channels)}")
        
        return channels
        
    except Exception as e:
        logger.error(f"Ошибка поиска каналов: {e}")
        return []

def extract_channel_usernames_sync(driver) -> List[str]:
    """Функция извлечения юзернеймов каналов"""
    usernames = []
    
    try:
        WebDriverWait(driver, 10).until(
            EC.presence_of_element_located((By.CSS_SELECTOR, '.card.peer-item-row, .peer-item-row'))
        )
        
        channel_cards = driver.find_elements(By.CSS_SELECTOR, '.card.peer-item-row, .peer-item-row')
        
        if not channel_cards:
            logger.warning("Не найдено карточек каналов")
            return usernames
        
        for card in channel_cards:
            try:
                link_elements = card.find_elements(By.CSS_SELECTOR, 'a[href*="/channel/@"]')
                
                for link in link_elements:
                    href = link.get_attribute('href')
                    if href and '/channel/@' in href:
                        match = re.search(r'/channel/(@[^/]+)', href)
                        if match:
                            username = match.group(1)
                            if username not in usernames:
                                usernames.append(username)
                                logger.info(f"Найден канал: {username}")
                                break
                
            except Exception as e:
                logger.warning(f"Ошибка обработки карточки канала: {e}")
                continue
        
        return usernames
        
    except TimeoutException:
        logger.warning("Результаты поиска не загрузились за отведенное время")
        return usernames
    except Exception as e:
        logger.error(f"Ошибка извлечения юзернеймов: {e}")
        return usernames

async def has_textual_posts(client: TelegramClient, username: str) -> bool:
    """Проверяет наличие текстовых постов в канале (включая посты с медиа и текстом)"""
    try:
        if not username.startswith('@'):
            username = '@' + username
        
        async def get_entity_safe():
            return await client.get_entity(username)
        
        entity = await handle_flood_wait(get_entity_safe)
        
        text_posts_found = 0
        total_checked = 0
        
        async for message in client.iter_messages(entity, limit=20):
            total_checked += 1
            
            has_text = False
            
            if hasattr(message, 'message') and message.message:
                text = message.message.strip()
                if text:
                    has_text = True
            elif hasattr(message, 'text') and message.text:
                text = message.text.strip()
                if text:
                    has_text = True
            
            if not has_text and hasattr(message, 'media') and message.media:
                if hasattr(message, 'message') and message.message:
                    caption = message.message.strip()
                    if caption:
                        has_text = True
            
            if has_text:
                text_posts_found += 1
                
                if text_posts_found >= 3 and total_checked <= 10:
                    logger.info(f"Канал {username} содержит достаточно текстовых постов")
                    return True
        
        text_ratio = text_posts_found / total_checked if total_checked > 0 else 0
        
        logger.info(f"Канал {username}: найдено {text_posts_found} текстовых постов из {total_checked} проверенных")
        
        return text_ratio >= 0.3
        
    except (ChannelPrivateError, ChannelInvalidError):
        logger.warning(f"Канал {username} недоступен")
        return False
    except FloodWaitError as e:
        logger.error(f"FloodWaitError при проверке канала {username}: {e.seconds} секунд")
        raise
    except Exception as e:
        logger.warning(f"Ошибка проверки текстовых постов канала {username}: {e}")
        return False

async def check_channel_comments_available(client: TelegramClient, username: str) -> bool:
    """Проверка доступности комментариев путем анализа последних 20 постов"""
    try:
        if not username.startswith('@'):
            username = '@' + username
        
        async def get_entity_safe():
            return await client.get_entity(username)
        
        entity = await handle_flood_wait(get_entity_safe)
        
        posts_with_comments = 0
        total_posts_checked = 0
        
        async for message in client.iter_messages(entity, limit=20):
            total_posts_checked += 1
            
            if hasattr(message, 'replies') and message.replies:
                if hasattr(message.replies, 'comments') and message.replies.comments:
                    posts_with_comments += 1
        
        logger.info(f"Канал {username}: {posts_with_comments} из {total_posts_checked} постов поддерживают комментарии")
        
        return posts_with_comments > 0
        
    except (ChannelPrivateError, ChannelInvalidError):
        logger.warning(f"Канал {username} недоступен")
        return False
    except FloodWaitError as e:
        logger.error(f"FloodWaitError при проверке канала {username}: {e.seconds} секунд")
        raise
    except Exception as e:
        logger.warning(f"Ошибка проверки канала {username}: {e}")
        return False

async def analyze_channel(channel_id: int) -> Tuple[List[str], List[str]]:
    """Анализ канала для определения тематики и ключевых слов с обработкой FloodWait"""
    try:
        global shared_telethon_client
        if not shared_telethon_client:
            logger.error("Telethon клиент не инициализирован в поисковике")
            return [], []

        if not shared_telethon_client.is_connected():
            try:
                await shared_telethon_client.connect()
            except Exception as e:
                logger.error(f"Не удалось подключить Telethon клиент: {e}")
                return [], []

        async def get_entity_safe():
            if not shared_telethon_client:
                raise Exception("Telethon клиент не инициализирован")
            return await shared_telethon_client.get_entity(channel_id)
        
        async def iter_messages_safe(entity, target_text_posts=30):
            """Получаем именно target_text_posts текстовых постов"""
            if not shared_telethon_client:
                raise Exception("Telethon клиент не инициализирован")
            
            text_messages = []
            total_checked = 0
            
            async for message in shared_telethon_client.iter_messages(entity, limit=1000):
                total_checked += 1
                
                message_text = ""
                
                if hasattr(message, 'message') and message.message:
                    message_text = message.message.strip()
                elif hasattr(message, 'text') and message.text:
                    message_text = message.text.strip()
                
                if not message_text and hasattr(message, 'media') and message.media:
                    if hasattr(message, 'message') and message.message:
                        message_text = message.message.strip()
                
                if message_text:
                    text_messages.append(message_text)
                    
                    if len(text_messages) >= target_text_posts:
                        break
                
                if total_checked >= 1000:
                    logger.warning(f"Достигнут лимит проверки 1000 сообщений")
                    break
            
            logger.info(f"Проверено {total_checked} сообщений, найдено {len(text_messages)} текстовых постов")
            return text_messages
        
        entity = await handle_flood_wait(get_entity_safe)
        
        channel_info = []
        
        if hasattr(entity, 'title'):
            channel_info.append(f"Название: {entity.title}")
        
        if hasattr(entity, 'about') and entity.about:
            channel_info.append(f"Описание: {entity.about}")
        
        posts_text = await handle_flood_wait(iter_messages_safe, entity, 50)
        
        posts_count = len(posts_text) if posts_text else 0
        logger.info(f"Получено текстовых постов для анализа канала {channel_id}: {posts_count}")
        
        if posts_text:
            posts_text.reverse()
            
            formatted_posts = []
            for post in posts_text:
                formatted_posts.append(f"**Пост:**\n\n{post}\n\n——————————————————")
            
            channel_info.append("\n\n".join(formatted_posts))
            
            logger.info(f"В промт добавлено {len(formatted_posts)} постов")
        else:
            channel_info.append("Посты не найдены")
            logger.info("Посты не найдены для добавления в промт")
        
        full_text = "\n".join(channel_info)
        
        try:
            import bot_interface
            prompts = bot_interface.get_bot_prompts()
            analysis_prompt = prompts.get('analysis_prompt', '')
            if not analysis_prompt:
                raise Exception("Промт для анализа не найден в bot_interface")
        except Exception as e:
            logger.error(f"Ошибка получения промта из bot_interface: {e}")
            analysis_prompt = """Данные канала:

{full_text}

———————————————————————

Ты — профессиональный аналитик Telegram-каналов. Проанализируй название, описание и посты канала и в результате:

📌 1. Сгенерируй **ТОЧНЫЕ** ключевые слова, которые могли бы встречаться в **названиях других каналов точно по этой теме**.

📌 2. Определи основную тему или темы канала, строго выбрав их из следующего списка:

{topics}

📤 Формат ответа:

ТЕМЫ: укажи только темы из списка. Если тем несколько, то пиши каждую через запятую.
КЛЮЧЕВЫЕ_СЛОВА: только короткие, точные, релевантные слова, для названия канала по этой теме. Каждое слово через запятую.

Отвечай строго в заданном формате."""
       
        topics_list_text = '["Бизнес и стартапы", "Блоги", "Букмекерство", "Видео и фильмы", "Даркнет", "Дизайн", "Для взрослых", "Еда и кулинария", "Здоровье и медицина", "Игры", "Искусство", "История", "Книги", "Красота и мода", "Криптовалюты", "Лайфхаки", "Маркетинг, PR, реклама", "Музыка", "Наука", "Новости и СМИ", "Образование", "Политика", "Психология", "Путешествия", "Работа и карьера", "Развлечения", "Религия", "Семья и дети", "Спорт", "Технологии", "Фото", "Эзотерика", "Юмор", "Другое"]'
        
        prompt = analysis_prompt
        
        if '{full_text}' in prompt:
            prompt = prompt.replace('{full_text}', full_text)
        else:
            prompt = prompt + f"\n\nДанные канала:\n{full_text}"
        
        if '{topics}' in prompt:
            prompt = prompt.replace('{topics}', topics_list_text)
        
        try:
            response = await g4f.ChatCompletion.create_async(
                model="gemini-2.5-flash",
                messages=[{"role": "user", "content": prompt}]
            )
            
            print("="*50)
            print("ОТВЕТ ОТ ИИ:")
            print("="*50)
            print(response)
            print("="*50)
            
            topics = []
            keywords = []
            
            lines = response.split('\n')
            for line in lines:
                if line.startswith('ТЕМЫ:'):
                    topics_text = line.replace('ТЕМЫ:', '').strip()
                    topics = [topic.strip() for topic in topics_text.split(',') if topic.strip()]
                elif line.startswith('КЛЮЧЕВЫЕ_СЛОВА:'):
                    keywords_text = line.replace('КЛЮЧЕВЫЕ_СЛОВА:', '').strip()
                    keywords = [kw.strip() for kw in keywords_text.split(',') if kw.strip()]
            
            print(f"Исходные темы от ИИ: {topics}")
            print(f"Исходные ключевые слова от ИИ: {keywords}")
            
            valid_topics = [
                "Бизнес и стартапы", "Блоги", "Букмекерство", "Видео и фильмы", "Даркнет", "Дизайн", 
                "Для взрослых", "Еда и кулинария", "Здоровье и медицина", "Игры", "Искусство", "История", 
                "Книги", "Красота и мода", "Криптовалюты", "Лайфхаки", "Маркетинг, PR, реклама", "Музыка", 
                "Наука", "Новости и СМИ", "Образование", "Политика", "Психология", "Путешествия", 
                "Работа и карьера", "Развлечения", "Религия", "Семья и дети", "Спорт", "Технологии", 
                "Фото", "Эзотерика", "Юмор", "Другое"
            ]
            
            filtered_topics = []
            
            topics_text = ', '.join(topics)
            for valid_topic in valid_topics:
                if topics_text.lower() == valid_topic.lower():
                    filtered_topics.append(valid_topic)
                    break
            
            if not filtered_topics:
                for topic in topics:
                    for valid_topic in valid_topics:
                        if topic.lower() == valid_topic.lower():
                            if valid_topic not in filtered_topics:
                                filtered_topics.append(valid_topic)
                            break
            
            unique_keywords = []
            for keyword in keywords:
                keyword_clean = keyword.strip()
                if keyword_clean and keyword_clean not in unique_keywords:
                    unique_keywords.append(keyword_clean)
            
            if not filtered_topics:
                filtered_topics = ['Другое']
                logger.warning("Не найдено подходящих тем, установлена тема 'Другое'")
            if not unique_keywords:
                unique_keywords = ['общее', 'контент']
                logger.warning("Не найдено ключевых слов, установлены дефолтные")
            
            print(f"Отфильтрованные темы: {filtered_topics}")
            print(f"Уникальные ключевые слова: {unique_keywords}")
            
            logger.info(f"Анализ канала завершен. Темы: {filtered_topics}, Ключевые слова: {unique_keywords}")
            return filtered_topics, unique_keywords
            
        except Exception as e:
            logger.error(f"Ошибка анализа с GPT-4: {e}")
            return ['Бизнес и стартапы', 'Маркетинг, PR, реклама'], ['бизнес', 'маркетинг', 'продвижение']
    
    except FloodWaitError as e:
        logger.error(f"FloodWaitError при анализе канала: {e.seconds} секунд")
        raise
    except Exception as e:
        logger.error(f"Ошибка анализа канала: {e}")
        return [], []

def get_actually_processed_count():
    """Получение реального количества обработанных каналов из масслукера"""
    try:
        import masslooker
        return len(masslooker.processed_channels)
    except Exception as e:
        logger.debug(f"Не удалось получить количество реально обработанных каналов: {e}")
        return 0

async def process_found_channels_for_user(channels: List[str], user_id: int):
    """Обработка найденных каналов для конкретного пользователя"""
    logger.info(f"Начинаем обработку {len(channels)} найденных каналов для пользователя {user_id}")
    
    settings = user_settings.get(user_id, {})
    client = user_telethon_clients.get(user_id)
    
    if not client:
        logger.error(f"Нет клиента для обработки каналов пользователя {user_id}")
        return
    
    max_channels = settings.get('max_channels', 150)
    
    for channel in channels:
        try:
            if not user_search_active.get(user_id, False):
                logger.info(f"Поиск остановлен, прерываем обработку каналов для пользователя {user_id}")
                break
            
            if not check_bot_running(user_id):
                logger.info(f"Остановка запрошена, прерываем обработку каналов для пользователя {user_id}")
                break
            
            processed_count = len(user_processed_channels.get(user_id, set()))
            if max_channels != float('inf') and processed_count >= max_channels:
                logger.info(f"Достигнут лимит обработанных каналов: {max_channels} для пользователя {user_id}")
                break
            
            # ИСПРАВЛЕНИЕ: используем существующие функции проверки каналов
            has_text_posts = await has_textual_posts(client, channel)
            if not has_text_posts:
                logger.info(f"Канал {channel} не содержит достаточно текстовых постов для пользователя {user_id}")
                continue
            
            has_comments = await check_channel_comments_available(client, channel)
            if not has_comments:
                logger.info(f"Канал {channel} не поддерживает комментарии для пользователя {user_id}")
                continue
            
            # Добавляем канал в статистику найденных
            user_found_channels[user_id].add(channel)
            
            try:
                import bot_interface
                bot_interface.add_processed_channel_statistics(
                    channel,
                    found_topic='Другое'
                )
                found_channels_list = bot_interface.bot_data['detailed_statistics']['found_channels']
                if channel not in found_channels_list:
                    found_channels_list.append(channel)
                    bot_interface.update_found_channels_statistics(found_channels_list)
            except Exception as e:
                logger.error(f"Ошибка добавления канала {channel} в статистику: {e}")
            
            # ИСПРАВЛЕНИЕ: используем реальную функцию добавления в очередь
            try:
                import masslooker
                await masslooker.add_channel_to_queue(channel, user_id)
                user_sent_to_queue_count[user_id] += 1
                logger.info(f"✅ Канал {channel} отправлен в очередь масслукинга для пользователя {user_id}")
            except Exception as e:
                logger.error(f"Ошибка добавления канала {channel} в очередь масслукинга для пользователя {user_id}: {e}")
            
            await asyncio.sleep(random.uniform(1, 3))
            
        except Exception as e:
            logger.error(f"Ошибка обработки канала {channel} для пользователя {user_id}: {e}")
            continue
    
    logger.info(f"Обработка найденных каналов завершена для пользователя {user_id}")

async def save_search_progress():
    """Сохранение прогресса поиска в базу данных"""
    try:
        from database import db
        progress_data = [
            ('search_progress', search_progress),
            ('found_channels', list(found_channels)),
            ('sent_to_queue_count', sent_to_queue_count)
        ]
        
        for key, value in progress_data:
            await db.save_bot_state(key, value)
    except Exception as e:
        logger.error(f"Ошибка сохранения прогресса поиска: {e}")

async def load_search_progress():
    """Загрузка прогресса поиска из базы данных"""
    global search_progress, found_channels, sent_to_queue_count
    try:
        from database import db
        
        saved_progress = await db.load_bot_state('search_progress', {})
        if saved_progress:
            search_progress.update(saved_progress)
        
        saved_channels = await db.load_bot_state('found_channels', [])
        if saved_channels:
            found_channels.update(saved_channels)
        
        sent_to_queue_count = await db.load_bot_state('sent_to_queue_count', 0)
        
        if sent_to_queue_count == 0:
            sent_to_queue_count = await db.load_bot_state('processed_channels_count', 0)
        
        actual_processed = get_actually_processed_count()
        
        logger.info(f"Загружен прогресс поиска")
    except Exception as e:
        logger.error(f"Ошибка загрузки прогресса поиска: {e}")

async def search_loop_async(user_id: int):
    """Основная функция поиска для пользователя"""
    logger.info(f"Начинаем основной цикл поиска каналов для пользователя {user_id}")
    
    # ИСПРАВЛЕНИЕ: инициализация драйвера для реального поиска
    driver = None
    try:
        logger.info(f"Инициализация веб-драйвера для пользователя {user_id}")
        # Запускаем инициализацию драйвера в отдельном потоке
        import concurrent.futures
        with concurrent.futures.ThreadPoolExecutor() as executor:
            future = executor.submit(setup_driver_threaded)
            driver = future.result(timeout=60)  # Ждем максимум 60 секунд
        
        if not driver:
            logger.error(f"Не удалось инициализировать драйвер для пользователя {user_id}")
            return
        
        user_drivers[user_id] = driver
        logger.info(f"Драйвер успешно инициализирован для пользователя {user_id}")
        
        # Переходим на страницу поиска
        if not navigate_to_channel_search(driver):
            logger.error(f"Не удалось перейти на страницу поиска для пользователя {user_id}")
            return
        
    except Exception as e:
        logger.error(f"Ошибка инициализации драйвера для пользователя {user_id}: {e}")
        return
    
    first_search = True
    
    while user_search_active.get(user_id, False):
        try:
            if not check_bot_running(user_id):
                logger.info(f"Остановка запрошена для пользователя {user_id}")
                user_search_active[user_id] = False
                break
            
            client = user_telethon_clients.get(user_id)
            if not client:
                logger.error(f"Нет клиента для пользователя {user_id}, поиск невозможен")
                user_search_active[user_id] = False
                break
            
            settings = user_settings.get(user_id, {})
            keywords = settings.get('keywords', [])
            topics = settings.get('topics', [])
            
            if not keywords or not topics:
                logger.warning(f"Ключевые слова или темы не настроены для пользователя {user_id}")
                await asyncio.sleep(300)
                continue
            
            # ИСПРАВЛЕНИЕ: используем реальный поиск каналов
            try:
                # Выбираем случайные ключевое слово и тему
                import random
                keyword = random.choice(keywords)
                topic = random.choice(topics)
                
                user_search_progress[user_id] = {
                    'current_keyword': keyword,
                    'current_topic': topic
                }
                
                logger.info(f"Поиск каналов для пользователя {user_id} по ключевому слову '{keyword}' и теме '{topic}'")
                
                # ИСПРАВЛЕНИЕ: используем реальную функцию поиска через веб-драйвер
                import concurrent.futures
                with concurrent.futures.ThreadPoolExecutor() as executor:
                    future = executor.submit(search_channels_sync, driver, keyword, topic, first_search)
                    found_channels = future.result(timeout=120)
                
                first_search = False
                
                if found_channels:
                    logger.info(f"Найдено {len(found_channels)} каналов для пользователя {user_id}")
                    
                    # ИСПРАВЛЕНИЕ: используем реальную функцию обработки найденных каналов
                    # Устанавливаем глобальный клиент для совместимости
                    global shared_telethon_client
                    shared_telethon_client = client
                    
                    await process_found_channels_for_user(found_channels, user_id)
                else:
                    logger.warning(f"Каналы не найдены для пользователя {user_id}")
                
                # Задержка между поисками
                await asyncio.sleep(random.uniform(300, 600))  # 5-10 минут
                
            except Exception as e:
                logger.error(f"Ошибка в цикле поиска для пользователя {user_id}: {e}")
                await asyncio.sleep(60)
            
        except Exception as e:
            logger.error(f"Критическая ошибка в цикле поиска для пользователя {user_id}: {e}")
            await asyncio.sleep(60)
    
    # Закрываем драйвер
    if driver:
        try:
            driver.quit()
            logger.info(f"Драйвер закрыт для пользователя {user_id}")
        except Exception as e:
            logger.error(f"Ошибка закрытия драйвера для пользователя {user_id}: {e}")
    
    logger.info(f"Цикл поиска каналов завершен для пользователя {user_id}")

async def start_search(settings: dict, shared_client: Optional[TelegramClient] = None, user_id: int = None):
    """Запуск поиска каналов для конкретного пользователя"""
    global user_search_active, user_settings, user_telethon_clients, user_found_channels, user_search_progress, user_sent_to_queue_count
    
    if not user_id:
        logger.error("Не указан user_id для запуска поиска")
        return
    
    if user_id in user_search_active and user_search_active[user_id]:
        logger.warning(f"Поиск для пользователя {user_id} уже запущен")
        return
    
    logger.info(f"Запуск поиска каналов для пользователя {user_id}")
    user_settings[user_id] = settings.copy()
    user_found_channels[user_id] = set()
    user_search_progress[user_id] = {'current_keyword': '', 'current_topic': ''}
    user_sent_to_queue_count[user_id] = 0
    
    if shared_client and isinstance(shared_client, TelegramClient):
        user_telethon_clients[user_id] = shared_client
        logger.info(f"Используем клиент для пользователя {user_id}")
        
        if not shared_client.is_connected():
            try:
                await shared_client.connect()
                logger.info(f"Telethon клиент подключен для пользователя {user_id}")
            except Exception as e:
                logger.error(f"Не удалось подключить клиент для пользователя {user_id}: {e}")
                return
    else:
        logger.error(f"Не передан корректный клиент для пользователя {user_id}")
        return

    user_search_active[user_id] = True
    
    try:
        # ИСПРАВЛЕНИЕ: запускаем реальный процесс поиска
        logger.info(f"Запуск поиска каналов для пользователя {user_id}")
        asyncio.create_task(search_loop_async(user_id))
        logger.info(f"Поиск каналов запущен для пользователя {user_id}")
    except Exception as e:
        logger.error(f"Не удалось запустить поиск для пользователя {user_id}: {e}")
        user_search_active[user_id] = False

async def stop_search(user_id: int = None):
    """Остановка поиска каналов для пользователя"""
    global user_search_active, user_drivers, user_telethon_clients
    
    if not user_id:
        logger.error("Не указан user_id для остановки поиска")
        return
    
    logger.info(f"Остановка поиска каналов для пользователя {user_id}")
    
    if user_id in user_search_active:
        user_search_active[user_id] = False
    
    if user_id in user_drivers and user_drivers[user_id]:
        try:
            user_drivers[user_id].quit()
            logger.info(f"Драйвер закрыт для пользователя {user_id}")
        except Exception as e:
            logger.error(f"Ошибка закрытия драйвера для пользователя {user_id}: {e}")
        user_drivers[user_id] = None
    
    if user_id in user_telethon_clients:
        try:
            client = user_telethon_clients[user_id]
            if client and client.is_connected():
                await client.disconnect()
        except Exception as e:
            logger.error(f"Ошибка отключения клиента поиска для пользователя {user_id}: {e}")
        del user_telethon_clients[user_id]
    
    user_settings.pop(user_id, None)
    user_found_channels.pop(user_id, None)
    user_search_progress.pop(user_id, None)
    user_sent_to_queue_count.pop(user_id, None)
    
    logger.info(f"Поиск каналов остановлен для пользователя {user_id}")

def get_statistics(user_id: int = None):
    """Получение статистики поиска для пользователя"""
    if user_id and user_id in user_found_channels:
        return {
            'found_channels': len(user_found_channels[user_id]),
            'search_active': user_search_active.get(user_id, False),
            'current_progress': user_search_progress.get(user_id, {}),
            'sent_to_queue_count': user_sent_to_queue_count.get(user_id, 0)
        }
    elif user_id is None:
        return {uid: {
            'found_channels': len(channels),
            'search_active': user_search_active.get(uid, False),
            'current_progress': user_search_progress.get(uid, {}),
            'sent_to_queue_count': user_sent_to_queue_count.get(uid, 0)
        } for uid, channels in user_found_channels.items()}
    else:
        return {
            'found_channels': 0,
            'search_active': False,
            'current_progress': {},
            'sent_to_queue_count': 0
        }

async def reset_search_state():
    """Принудительный сброс состояния поиска для устранения блокировок"""
    global search_active, driver, shared_telethon_client, sent_to_queue_count
    
    logger.info("Принудительный сброс состояния поиска...")
    search_active = False
    
    if driver:
        try:
            driver.quit()
        except Exception as e:
            logger.debug(f"Ошибка закрытия драйвера при сбросе: {e}")
        driver = None
    
    logger.info("Состояние поиска сброшено")

def is_search_really_active():
    """Проверка реального состояния поиска"""
    global search_active, driver, shared_telethon_client
    
    if search_active and not driver and not shared_telethon_client:
        logger.warning("Обнаружено неконсистентное состояние поиска - сбрасываем флаг")
        search_active = False
        return False
    
    return search_active

async def main():
    """Тестирование модуля"""
    test_settings = {
        'keywords': ['тест', 'пример'],
        'topics': ['Технологии', 'Образование']
    }
    
    await start_search(test_settings)
    
    await asyncio.sleep(120)
    
    await stop_search()

if __name__ == "__main__":
    asyncio.run(main())