import telebot
import sqlite3
import random
from datetime import datetime
import logging
import time
import html

# ==================== НАСТРОЙКИ ====================
BOT_TOKEN = "8359702603:AAGho4yLhl1GCWXENtFVU9Y3tvaPaVuBiY4"
DB_NAME = "archive.db"

# Фотографии
PHOTOS = {
    'start': 'https://ibb.co/5gc6GcCt',
    'new_topic': 'https://ibb.co/C5Zy1VwQ',
    'random': 'https://ibb.co/N645QgdB',
    'my_topics': 'https://ibb.co/mVfrSdJy',
    'popular': 'https://ibb.co/vC4GvZyV',
    'topic_created': 'https://ibb.co/MLS0xmc',
    'reply_created': 'https://ibb.co/RpMkjtKf',
    'view_topic': 'https://ibb.co/zWdFvwTF',
    'notification': 'https://ibb.co/mCDDWKyG'
}

# Настройка логирования
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('bot.log', encoding='utf-8'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

# ==================== БАЗА ДАННЫХ ====================
def init_db():
    """Инициализация базы данных"""
    conn = sqlite3.connect(DB_NAME, check_same_thread=False)
    c = conn.cursor()
    
    c.execute('''
        CREATE TABLE IF NOT EXISTS topics (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            text TEXT NOT NULL,
            user_id INTEGER NOT NULL,
            is_active BOOLEAN DEFAULT 1,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    ''')
    
    c.execute('''
        CREATE TABLE IF NOT EXISTS replies (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            topic_id INTEGER NOT NULL,
            text TEXT NOT NULL,
            user_id INTEGER NOT NULL,
            is_active BOOLEAN DEFAULT 1,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            FOREIGN KEY (topic_id) REFERENCES topics(id)
        )
    ''')
    
    conn.commit()
    return conn

db = init_db()

# ==================== ВСПОМОГАТЕЛЬНЫЕ ФУНКЦИИ ====================
def format_datetime(dt_str):
    """Форматирование даты для пользователя"""
    try:
        dt = datetime.strptime(dt_str, '%Y-%m-%d %H:%M:%S')
        return dt.strftime('%d.%m.%Y %H:%M')
    except:
        return dt_str

def add_topic(text, user_id):
    """Добавление новой темы"""
    c = db.cursor()
    clean_text = ' '.join(text.strip().split())
    c.execute('INSERT INTO topics (text, user_id) VALUES (?, ?)', (clean_text, user_id))
    db.commit()
    return c.lastrowid

def add_reply(topic_id, text, user_id):
    """Добавление ответа к теме с уведомлением автора"""
    c = db.cursor()
    clean_text = ' '.join(text.strip().split())
    
    # Получаем автора темы
    c.execute('SELECT user_id FROM topics WHERE id = ?', (topic_id,))
    topic = c.fetchone()
    
    if not topic:
        return None
    
    topic_author_id = topic[0]
    
    # Добавляем ответ
    c.execute('INSERT INTO replies (topic_id, text, user_id) VALUES (?, ?, ?)', 
              (topic_id, clean_text, user_id))
    c.execute('UPDATE topics SET updated_at = CURRENT_TIMESTAMP WHERE id = ?', (topic_id,))
    db.commit()
    
    reply_id = c.lastrowid
    
    # Отправляем уведомление автору темы (кроме случая, когда автор отвечает сам себе)
    if topic_author_id != user_id:
        send_reply_notification(topic_author_id, topic_id, reply_id, clean_text)
    
    return reply_id

def send_reply_notification(user_id, topic_id, reply_id, reply_text):
    """Отправка уведомления о новом ответе"""
    try:
        # Получаем текст темы для уведомления
        c = db.cursor()
        c.execute('SELECT text FROM topics WHERE id = ?', (topic_id,))
        topic = c.fetchone()
        
        if not topic:
            return
        
        topic_text = topic[0]
        preview = topic_text[:60] + "..." if len(topic_text) > 60 else topic_text
        reply_preview = reply_text
        
        text = f"""🔔 <b>НОВЫЙ ОТВЕТ НА ВАШУ ТЕМУ</b>

<b>Тема #{topic_id}:</b>
{html.escape(preview)}

<b>Ответ #{reply_id}:</b>
{html.escape(reply_preview)}

📅 <i>Время: {datetime.now().strftime('%d.%m.%Y %H:%M')}</i>"""
        
        markup = telebot.types.InlineKeyboardMarkup()
        markup.add(
            telebot.types.InlineKeyboardButton("📄 ПЕРЕЙТИ К ТЕМЕ", callback_data=f"view_topic_{topic_id}_1")
        )
        
        # Попытка отправить фото с улучшенной обработкой ошибок
        try:
            photo_url = PHOTOS.get('notification', PHOTOS['start'])
            bot.send_photo(
                user_id,
                photo_url,
                caption=text,
                reply_markup=markup,
                parse_mode='HTML'
            )
        except telebot.apihelper.ApiTelegramException as photo_error:
            # Если не удалось отправить фото, отправляем текстовое сообщение
            logger.warning(f"Не удалось отправить фото уведомления, отправляем текст. Ошибка: {photo_error}")
            try:
                bot.send_message(
                    user_id,
                    text,
                    reply_markup=markup,
                    parse_mode='HTML'
                )
            except telebot.apihelper.ApiTelegramException as msg_error:
                if msg_error.error_code == 403:
                    logger.warning(f"Пользователь {user_id} заблокировал бота, уведомление не отправлено")
                else:
                    logger.error(f"Ошибка при отправке текстового уведомления: {msg_error}")
        except Exception as e:
            logger.error(f"Неожиданная ошибка при отправке уведомления: {e}")
        
        logger.info(f"Уведомление отправлено автору темы #{topic_id} (пользователь: {user_id})")
        
    except Exception as e:
        logger.error(f"Критическая ошибка в функции send_reply_notification: {e}")

def get_topic(topic_id, user_id=None):
    """Получение темы"""
    c = db.cursor()
    if user_id:
        c.execute('SELECT * FROM topics WHERE id = ?', (topic_id,))
    else:
        c.execute('SELECT * FROM topics WHERE id = ? AND is_active = 1', (topic_id,))
    return c.fetchone()

def close_topic(topic_id, user_id):
    """Закрытие темы"""
    c = db.cursor()
    
    # Проверяем, является ли пользователь автором
    c.execute('SELECT user_id FROM topics WHERE id = ?', (topic_id,))
    topic = c.fetchone()
    
    if not topic:
        return False, "Тема не найдена"
    
    if topic[0] != user_id:
        return False, "Вы не автор этой темы"
    
    # Закрываем тему
    c.execute('UPDATE topics SET is_active = 0 WHERE id = ?', (topic_id,))
    db.commit()
    return True, "✅ Тема закрыта"

def delete_topic(topic_id, user_id):
    """Удаление темы со всеми ответами"""
    c = db.cursor()
    
    # Проверяем, является ли пользователь автором
    c.execute('SELECT user_id FROM topics WHERE id = ?', (topic_id,))
    topic = c.fetchone()
    
    if not topic:
        return False, "Тема не найдена"
    
    if topic[0] != user_id:
        return False, "Вы не автор этой темы"
    
    # Удаляем все ответы темы
    c.execute('DELETE FROM replies WHERE topic_id = ?', (topic_id,))
    
    # Удаляем тему
    c.execute('DELETE FROM topics WHERE id = ?', (topic_id,))
    
    db.commit()
    return True, "✅ Тема и все ответы удалены"

def get_random_topic(exclude_user_id=None, viewed_topics=None):
    """Получение случайной активной темы с исключением просмотренных"""
    c = db.cursor()
    
    if viewed_topics:
        # Преобразуем список в строку для SQL запроса
        viewed_str = ','.join(map(str, viewed_topics))
        
        if exclude_user_id:
            c.execute(f'''
                SELECT * FROM topics 
                WHERE is_active = 1 
                AND user_id != ? 
                AND id NOT IN ({viewed_str if viewed_topics else '0'})
                ORDER BY RANDOM() 
                LIMIT 1
            ''', (exclude_user_id,))
        else:
            c.execute(f'''
                SELECT * FROM topics 
                WHERE is_active = 1 
                AND id NOT IN ({viewed_str if viewed_topics else '0'})
                ORDER BY RANDOM() 
                LIMIT 1
            ''')
    else:
        if exclude_user_id:
            c.execute('SELECT * FROM topics WHERE is_active = 1 AND user_id != ? ORDER BY RANDOM() LIMIT 1', 
                     (exclude_user_id,))
        else:
            c.execute('SELECT * FROM topics WHERE is_active = 1 ORDER BY RANDOM() LIMIT 1')
    
    return c.fetchone()

def get_all_active_topics_count(exclude_user_id=None):
    """Получение количества всех активных тем"""
    c = db.cursor()
    if exclude_user_id:
        c.execute('SELECT COUNT(*) FROM topics WHERE is_active = 1 AND user_id != ?', (exclude_user_id,))
    else:
        c.execute('SELECT COUNT(*) FROM topics WHERE is_active = 1')
    return c.fetchone()[0]

def get_user_topics(user_id, limit=10, offset=0):
    """Получение тем пользователя"""
    c = db.cursor()
    c.execute('''
        SELECT t.*, COUNT(r.id) as replies_count
        FROM topics t
        LEFT JOIN replies r ON t.id = r.topic_id AND r.is_active = 1
        WHERE t.user_id = ?
        GROUP BY t.id
        ORDER BY t.updated_at DESC 
        LIMIT ? OFFSET ?
    ''', (user_id, limit, offset))
    return c.fetchall()

def get_topic_replies(topic_id, user_id=None, limit=5, offset=0):
    """Получение ответов к теме"""
    c = db.cursor()
    c.execute('''
        SELECT r.*
        FROM replies r
        WHERE r.topic_id = ? AND r.is_active = 1
        ORDER BY r.created_at ASC
        LIMIT ? OFFSET ?
    ''', (topic_id, limit, offset))
    return c.fetchall()

def get_replies_count(topic_id):
    """Количество активных ответов"""
    c = db.cursor()
    c.execute('SELECT COUNT(*) FROM replies WHERE topic_id = ? AND is_active = 1', (topic_id,))
    return c.fetchone()[0]

def get_popular_topics(limit=5, user_id=None):
    """Популярные темы с пометкой авторства"""
    c = db.cursor()
    if user_id:
        c.execute('''
            SELECT t.*, COUNT(r.id) as replies_count,
                   CASE WHEN t.user_id = ? THEN 1 ELSE 0 END as is_owner
            FROM topics t
            LEFT JOIN replies r ON t.id = r.topic_id AND r.is_active = 1
            WHERE 1=1  
            GROUP BY t.id
            ORDER BY replies_count DESC, t.updated_at DESC
            LIMIT ?
        ''', (user_id, limit))
    else:
        c.execute('''
            SELECT t.*, COUNT(r.id) as replies_count, 0 as is_owner
            FROM topics t
            LEFT JOIN replies r ON t.id = r.topic_id AND r.is_active = 1
            WHERE 1=1  
            GROUP BY t.id
            ORDER BY replies_count DESC, t.updated_at DESC
            LIMIT ?
        ''', (limit,))
    return c.fetchall()

# ==================== БОТ ====================
bot = telebot.TeleBot(BOT_TOKEN, parse_mode='HTML')
user_states = {}
user_last_messages = {}  # Словарь для хранения ID последних сообщений пользователя
user_viewed_topics = {}  # Словарь для хранения просмотренных тем пользователем
user_topic_counters = {}  # Словарь для счетчиков просмотренных тем

def delete_previous_messages(chat_id, user_id):
    """Удаление предыдущих сообщений бота для конкретного пользователя"""
    try:
        if user_id in user_last_messages:
            for msg_id in user_last_messages[user_id]:
                try:
                    bot.delete_message(chat_id, msg_id)
                except:
                    pass  # Игнорируем ошибки удаления (сообщение могло быть уже удалено)
            user_last_messages[user_id] = []
    except Exception as e:
        logger.error(f"Ошибка при удалении предыдущих сообщений: {e}")

def add_message_to_delete(user_id, message_id):
    """Добавление ID сообщения в список для удаления"""
    if user_id not in user_last_messages:
        user_last_messages[user_id] = []
    user_last_messages[user_id].append(message_id)
    
    # Ограничиваем список последних 5 сообщений
    if len(user_last_messages[user_id]) > 5:
        user_last_messages[user_id] = user_last_messages[user_id][-5:]

def send_photo_message(chat_id, photo_type, text, reply_markup=None):
    """Отправка сообщения с фото"""
    try:
        photo_url = PHOTOS.get(photo_type, PHOTOS['start'])
        msg = bot.send_photo(
            chat_id,
            photo_url,
            caption=text,
            reply_markup=reply_markup,
            parse_mode='HTML'
        )
        return msg.message_id
    except telebot.apihelper.ApiTelegramException as e:
        # Если ошибка связана с загрузкой фото, отправляем текстовое сообщение
        if "failed to get HTTP URL content" in str(e) or "Bad Request" in str(e):
            logger.warning(f"Ошибка при отправке фото {photo_type}, отправляем текст: {e}")
            try:
                msg = bot.send_message(chat_id, text, reply_markup=reply_markup, parse_mode='HTML')
                return msg.message_id
            except Exception as e2:
                logger.error(f"Ошибка при отправке сообщения: {e2}")
                return None
        else:
            logger.error(f"Ошибка при отправке фото {photo_type}: {e}")
            return None
    except Exception as e:
        logger.error(f"Неожиданная ошибка в send_photo_message: {e}")
        return None

def send_message_with_delete(chat_id, user_id, photo_type, text, reply_markup=None):
    """Отправка сообщения с автоматическим удалением предыдущих"""
    delete_previous_messages(chat_id, user_id)
    message_id = send_photo_message(chat_id, photo_type, text, reply_markup)
    if message_id:
        add_message_to_delete(user_id, message_id)
    return message_id

def reset_user_viewed_topics(user_id):
    """Сброс списка просмотренных тем для пользователя"""
    if user_id in user_viewed_topics:
        user_viewed_topics[user_id] = []
    if user_id in user_topic_counters:
        user_topic_counters[user_id] = 0

def add_viewed_topic(user_id, topic_id):
    """Добавление темы в список просмотренных"""
    if user_id not in user_viewed_topics:
        user_viewed_topics[user_id] = []
    
    if topic_id not in user_viewed_topics[user_id]:
        user_viewed_topics[user_id].append(topic_id)
        
        # Обновляем счетчик
        if user_id not in user_topic_counters:
            user_topic_counters[user_id] = 0
        user_topic_counters[user_id] += 1

def check_all_topics_viewed(user_id, exclude_user_id=None):
    """Проверка, просмотрены ли все темы"""
    if user_id not in user_viewed_topics:
        return False
    
    viewed_count = len(user_viewed_topics[user_id])
    total_count = get_all_active_topics_count(exclude_user_id)
    
    return viewed_count >= total_count and total_count > 0

# ==================== ГЛАВНОЕ МЕНЮ ====================
@bot.message_handler(commands=['start'])
def start_command(message):
    """Обработка команды /start"""
    user_id = message.from_user.id
    chat_id = message.chat.id
    
    # Удаляем команду /start
    try:
        bot.delete_message(chat_id, message.message_id)
    except:
        pass
    
    if user_id in user_states:
        del user_states[user_id]
    
    # Сбрасываем прогресс просмотра
    reset_user_viewed_topics(user_id)
    
    show_main_menu(chat_id, user_id)

def show_main_menu(chat_id, user_id):
    """Показать главное меню"""
    # Получаем статистику
    viewed_count = len(user_viewed_topics.get(user_id, []))
    total_topics = get_all_active_topics_count(user_id)
    
    if viewed_count > 0 and total_topics > 0:
        progress = min(100, int((viewed_count / total_topics) * 100))
        
        text = f"""<b>🗄️ АРХИВ МЫСЛЕЙ</b>

📌 <b>Основные функции:</b>
• Создавайте анонимные темы
• Отвечайте на чужие мысли
• Читайте популярные обсуждения
• Управляйте своими темами

🔔 <b>Уведомления:</b>
• Получайте оповещения при ответах на ваши темы
• Анонимные ответы других пользователей

🔒 <b>Управление темами:</b>
• Закрытие своих тем
• Удаление своих темы вместе с ответами

<i>Без имён. Без осуждения. Только мысли.</i>"""
    else:
        text = """<b>🗄️ АРХИВ МЫСЛЕЙ</b>

📌 <b>Основные функции:</b>
• Создавайте анонимные темы
• Отвечайте на чужие мысли
• Читайте популярные обсуждения
• Управляйте своими темами

🔔 <b>Уведомления:</b>
• Получайте оповещения при ответах на ваши темы
• Анонимные ответы других пользователей

🔒 <b>Управление темами:</b>
• Закрытие своих тем
• Удаление своих темы вместе с ответами

<i>Без имён. Без осуждения. Только мысли.</i>"""
    
    markup = telebot.types.InlineKeyboardMarkup(row_width=2)
    markup.add(
        telebot.types.InlineKeyboardButton("➕ НОВАЯ ТЕМА", callback_data="new_topic"),
        telebot.types.InlineKeyboardButton("🎲 СЛУЧАЙНАЯ", callback_data="random_topic"),
        telebot.types.InlineKeyboardButton("📁 МОИ ТЕМЫ", callback_data="my_topics_1"),
        telebot.types.InlineKeyboardButton("🔥 ПОПУЛЯРНЫЕ", callback_data="popular_1")
    )
    
    send_message_with_delete(chat_id, user_id, 'start', text, markup)

# ==================== НОВАЯ ТЕМА ====================
@bot.callback_query_handler(func=lambda call: call.data == "new_topic")
def new_topic_callback(call):
    """Создание новой темы"""
    user_states[call.from_user.id] = {'state': 'new_topic'}
    
    text = """<b>✍️ СОЗДАНИЕ НОВОЙ ТЕМЫ</b>

Напишите свою мысль, вопрос или идею.

<b>Требования:</b>
• От 2 до 2000 символов
• Сохраняется анонимно
• Без личных данных

🔔 <b>Вы получите уведомление</b>, когда кто-то ответит на вашу тему."""
    
    markup = telebot.types.InlineKeyboardMarkup()
    markup.add(telebot.types.InlineKeyboardButton("🔙 В МЕНЮ", callback_data="menu"))
    
    send_message_with_delete(call.message.chat.id, call.from_user.id, 'new_topic', text, markup)
    bot.answer_callback_query(call.id)

# ==================== СЛУЧАЙНАЯ ТЕМА ====================
@bot.callback_query_handler(func=lambda call: call.data == "random_topic")
def random_topic_callback(call):
    """Случайная тема без повторений"""
    user_id = call.from_user.id
    
    # Проверяем, просмотрены ли все темы
    if check_all_topics_viewed(user_id, user_id):
        # Сбрасываем список просмотренных и показываем сообщение
        reset_user_viewed_topics(user_id)
        
        text = """🎉 <b>ВЫ ПРОСМОТРЕЛИ ВСЕ ТЕМЫ!</b>

Вы увидели все доступные темы в архиве.
Список просмотренных тем сброшен.

<b>Что дальше?</b>
• Начните новый цикл просмотра
• Создайте свою тему
• Ответьте на понравившиеся мысли

🌟 <i>Архив мыслей обновляется сразу</i>"""
        
        markup = telebot.types.InlineKeyboardMarkup(row_width=1)
        markup.add(
            telebot.types.InlineKeyboardButton("🔄 НАЧАТЬ НОВЫЙ ЦИКЛ", callback_data="random_topic"),
            telebot.types.InlineKeyboardButton("🔙 В МЕНЮ", callback_data="menu")
        )
        
        send_message_with_delete(call.message.chat.id, user_id, 'random', text, markup)
        bot.answer_callback_query(call.id)
        return
    
    # Получаем случайную тему, исключая просмотренные
    viewed_list = user_viewed_topics.get(user_id, [])
    topic = get_random_topic(exclude_user_id=user_id, viewed_topics=viewed_list)
    
    if not topic:
        # Если нет непросмотренных тем, сбрасываем список
        reset_user_viewed_topics(user_id)
        topic = get_random_topic(exclude_user_id=user_id)
        
        if not topic:
            text = """<b>📭 АРХИВ ПУСТ</b>

Пока нет ни одной темы.
Создайте первую и начните обсуждение!"""
            
            markup = telebot.types.InlineKeyboardMarkup()
            markup.add(
                telebot.types.InlineKeyboardButton("➕ СОЗДАТЬ ТЕМУ", callback_data="new_topic"),
                telebot.types.InlineKeyboardButton("🔙 В МЕНЮ", callback_data="menu")
            )
            
            send_message_with_delete(call.message.chat.id, user_id, 'start', text, markup)
            bot.answer_callback_query(call.id)
            return
        
        # Показываем сообщение о новом цикле
        text = """🔄 <b>НОВЫЙ ЦИКЛ ПРОСМОТРА</b>

Вы начали новый цикл просмотра тем.
Предыдущие темы снова доступны.

<b>Статистика предыдущего цикла:</b>
• Просмотрено тем: {} • Начата новая сессия""".format(user_topic_counters.get(user_id, 0))
        
        markup = telebot.types.InlineKeyboardMarkup(row_width=1)
        markup.add(
            telebot.types.InlineKeyboardButton("➡️ ПРОДОЛЖИТЬ", callback_data="random_topic"),
            telebot.types.InlineKeyboardButton("🔙 В МЕНЮ", callback_data="menu")
        )
        
        send_message_with_delete(call.message.chat.id, user_id, 'random', text, markup)
        bot.answer_callback_query(call.id)
        return
    
    topic_id, topic_text, _, is_active, created_at, _ = topic
    replies_count = get_replies_count(topic_id)
    
    # Добавляем тему в список просмотренных
    add_viewed_topic(user_id, topic_id)
    
    # Получаем статистику
    total_topics = get_all_active_topics_count(user_id)
    viewed_count = len(user_viewed_topics.get(user_id, []))
    remaining = max(0, total_topics - viewed_count)
    
    text = f"""<b>🎲 СЛУЧАЙНАЯ ТЕМА #{topic_id}</b>

{html.escape(topic_text)}

<b>📊 Информация:</b>
• Ответов: {replies_count}
• Создана: {format_datetime(created_at)}
• Статус: {"🟢 Активна" if is_active else "🔴 Закрыта"}

<b>📈 Ваш прогресс:</b>
• Просмотрено: {viewed_count}/{total_topics} открытых тем"""
    
    markup = telebot.types.InlineKeyboardMarkup(row_width=2)
    
    if is_active:
        markup.add(telebot.types.InlineKeyboardButton("💬 ОТВЕТИТЬ", callback_data=f"reply_topic_{topic_id}"))
    
    markup.add(
        telebot.types.InlineKeyboardButton("📄 ПОДРОБНЕЕ", callback_data=f"view_topic_{topic_id}_1"),
        telebot.types.InlineKeyboardButton("🎲 СЛЕДУЮЩАЯ", callback_data="random_topic"),
        telebot.types.InlineKeyboardButton("🔙 В МЕНЮ", callback_data="menu")
    )
    
    send_message_with_delete(call.message.chat.id, user_id, 'random', text, markup)
    bot.answer_callback_query(call.id)

# ==================== МОИ ТЕМЫ (С ПАГИНАЦИЕЙ) ====================
@bot.callback_query_handler(func=lambda call: call.data.startswith("my_topics_"))
def my_topics_callback(call):
    """Мои темы с пагинацией"""
    try:
        user_id = call.from_user.id
        page = int(call.data.split("_")[2])
        per_page = 5
        offset = (page - 1) * per_page
        
        topics = get_user_topics(user_id, limit=per_page, offset=offset)
        
        # Получаем общее количество
        c = db.cursor()
        c.execute('SELECT COUNT(*) FROM topics WHERE user_id = ?', (user_id,))
        total_topics = c.fetchone()[0]
        
        if not topics and page == 1:
            text = """<b>📭 НЕТ ВАШИХ ТЕМ</b>

У вас пока нет созданных тем.
Начните обсуждение первым!"""
            
            markup = telebot.types.InlineKeyboardMarkup()
            markup.add(
                telebot.types.InlineKeyboardButton("➕ СОЗДАТЬ", callback_data="new_topic"),
                telebot.types.InlineKeyboardButton("🔙 В МЕНЮ", callback_data="menu")
            )
            
            send_message_with_delete(call.message.chat.id, user_id, 'my_topics', text, markup)
            bot.answer_callback_query(call.id)
            return
        
        total_pages = max(1, (total_topics + per_page - 1) // per_page)
        
        text = f"""<b>📁 ВАШИ ТЕМЫ</b>

Страница {page} из {total_pages}
Всего тем: {total_topics}

🔔 <i>Вы получите уведомление при новых ответах</i>

<b>Список:</b>"""
        
        for i, topic in enumerate(topics, 1):
            topic_id, topic_text, _, is_active, _, _, replies_count = topic
            preview = topic_text[:70] + "..." if len(topic_text) > 70 else topic_text
            status = "🟢" if is_active else "🔴"
            text += f"\n\n{status} <b>{offset + i}. #{topic_id}</b>"
            text += f"\n{html.escape(preview)}"
            text += f"\n💬 Ответов: {replies_count}"
        
        markup = telebot.types.InlineKeyboardMarkup(row_width=3)
        
        # Кнопки для тем
        for topic in topics:
            topic_id = topic[0]
            replies_count = topic[6]
            btn_text = f"#{topic_id}"
            if replies_count > 0:
                btn_text += f" 💬{replies_count}"
            markup.add(
                telebot.types.InlineKeyboardButton(btn_text, callback_data=f"view_topic_{topic_id}_1")
            )
        
        # Пагинация
        pagination_buttons = []
        
        if page > 1:
            pagination_buttons.append(
                telebot.types.InlineKeyboardButton("◀️", callback_data=f"my_topics_{page-1}")
            )
        
        pagination_buttons.append(
            telebot.types.InlineKeyboardButton(f"{page}/{total_pages}", callback_data=f"my_topics_{page}")
        )
        
        if page < total_pages:
            pagination_buttons.append(
                telebot.types.InlineKeyboardButton("▶️", callback_data=f"my_topics_{page+1}")
            )
        
        if pagination_buttons:
            markup.add(*pagination_buttons)
        
        # Навигация
        markup.add(
            telebot.types.InlineKeyboardButton("🔙 В МЕНЮ", callback_data="menu")
        )
        
        send_message_with_delete(call.message.chat.id, user_id, 'my_topics', text, markup)
        bot.answer_callback_query(call.id)
        
    except Exception as e:
        logger.error(f"Ошибка в my_topics_callback: {e}")
        bot.answer_callback_query(call.id, "❌ Ошибка", show_alert=True)

# ==================== ПОПУЛЯРНЫЕ ТЕМЫ (С ПАГИНАЦИЕЙ) - ОБНОВЛЕНО ====================
@bot.callback_query_handler(func=lambda call: call.data.startswith("popular_"))
def popular_topics_callback(call):
    """Популярные темы с пагинацией и пометкой авторства"""
    try:
        user_id = call.from_user.id
        page = int(call.data.split("_")[1])
        per_page = 5
        offset = (page - 1) * per_page
        
        # Получаем популярные темы с информацией о принадлежности текущему пользователю
        all_topics = get_popular_topics(limit=100, user_id=user_id)
        topics = all_topics[offset:offset + per_page]
        total_topics = len(all_topics)
        
        if not topics and page == 1:
            text = """<b>📭 НЕТ ПОПУЛЯРНЫХ ТЕМ</b>

Пока нет тем с ответами.
Станьте первым, кто начнет обсуждение!"""
            
            markup = telebot.types.InlineKeyboardMarkup(row_width=1)
            markup.add(
                telebot.types.InlineKeyboardButton("➕ СОЗДАТЬ ТЕМУ", callback_data="new_topic"),
                telebot.types.InlineKeyboardButton("🎲 СЛУЧАЙНАЯ", callback_data="random_topic"),
                telebot.types.InlineKeyboardButton("🔙 В МЕНЮ", callback_data="menu")
            )
            
            send_message_with_delete(call.message.chat.id, user_id, 'popular', text, markup)
            bot.answer_callback_query(call.id)
            return
        
        total_pages = max(1, (total_topics + per_page - 1) // per_page)
        
        text = f"""<b>🔥 ПОПУЛЯРНЫЕ ТЕМЫ</b>

Страница {page} из {total_pages}
Топ обсуждений по количеству ответов
🟢 - Тема открыта
🔴 - Тема закрыта

<b>Список:</b>"""
        
        for i, topic in enumerate(topics, 1):
            topic_id, topic_text, topic_user_id, is_active, _, _, replies_count, is_owner = topic
            preview = topic_text[:70] + "..." if len(topic_text) > 70 else topic_text
            status = "🟢" if is_active else "🔴"
            
            # Добавляем пометку (Вы) для тем текущего пользователя
            author_mark = " 👤<b>(Вы)</b>" if is_owner == 1 else ""
            
            text += f"\n\n{status} <b>{offset + i}. #{topic_id}{author_mark}</b>"
            text += f"\n{html.escape(preview)}"
            text += f"\n💬 Ответов: {replies_count}"
        
        markup = telebot.types.InlineKeyboardMarkup(row_width=3)
        
        # Кнопки для тем
        for topic in topics:
            topic_id = topic[0]
            replies_count = topic[6]
            is_owner = topic[7]
            
            btn_text = f"#{topic_id}"
            if is_owner == 1:
                btn_text += " 👤"
            if replies_count > 0:
                btn_text += f" 💬{replies_count}"
                
            markup.add(
                telebot.types.InlineKeyboardButton(btn_text, callback_data=f"view_topic_{topic_id}_1")
            )
        
        # Пагинация
        pagination_buttons = []
        
        if page > 1:
            pagination_buttons.append(
                telebot.types.InlineKeyboardButton("◀️", callback_data=f"popular_{page-1}")
            )
        
        pagination_buttons.append(
            telebot.types.InlineKeyboardButton(f"{page}/{total_pages}", callback_data=f"popular_{page}")
        )
        
        if page < total_pages:
            pagination_buttons.append(
                telebot.types.InlineKeyboardButton("▶️", callback_data=f"popular_{page+1}")
            )
        
        if pagination_buttons:
            markup.add(*pagination_buttons)
        
        # Навигация
        markup.add(
            telebot.types.InlineKeyboardButton("🔙 В МЕНЮ", callback_data="menu")
        )
        
        send_message_with_delete(call.message.chat.id, user_id, 'popular', text, markup)
        bot.answer_callback_query(call.id)
        
    except Exception as e:
        logger.error(f"Ошибка в popular_topics_callback: {e}")
        bot.answer_callback_query(call.id, "❌ Ошибка", show_alert=True)

# ==================== ПРОСМОТР ТЕМЫ (С ПАГИНАЦИЕЙ ОТВЕТОВ) ====================
@bot.callback_query_handler(func=lambda call: call.data.startswith("view_topic_"))
def view_topic_callback(call):
    """Просмотр темы с пагинацией ответов"""
    try:
        parts = call.data.split("_")
        topic_id = int(parts[2])
        reply_page = int(parts[3]) if len(parts) > 3 else 1
        
        topic = get_topic(topic_id, call.from_user.id)
        
        if not topic:
            bot.answer_callback_query(call.id, "❌ Тема не найдена", show_alert=True)
            show_main_menu(call.message.chat.id, call.from_user.id)
            return
        
        topic_id, topic_text, topic_user_id, is_active, created_at, updated_at = topic
        
        # Получаем ответы с пагинацией
        per_page = 3
        offset = (reply_page - 1) * per_page
        replies = get_topic_replies(topic_id, limit=per_page, offset=offset)
        total_replies = get_replies_count(topic_id)
        total_pages = max(1, (total_replies + per_page - 1) // per_page)
        
        is_author = (topic_user_id == call.from_user.id)
        
        text = f"""<b>📄 ТЕМА #{topic_id}</b>

{html.escape(topic_text)}

<b>📊 Информация:</b>
• Ответов: {total_replies}
• Создана: {format_datetime(created_at)}
• Обновлена: {format_datetime(updated_at)}
• Статус: {"🟢 Активна" if is_active else "🔴 Закрыта"}
• Автор: {"Вы 👤" if is_author else "Аноним"}"""
        
        if total_replies > 0:
            text += f"\n\n<b>📝 ОТВЕТЫ (стр. {reply_page}/{total_pages}):</b>"
            
            for i, reply in enumerate(replies, 1):
                reply_id = reply[0]
                reply_text = reply[2]
                reply_created_at = reply[5]
                
                preview = reply_text
                text += f"\n\n{offset + i}. {html.escape(preview)}"
                text += f"\n📅 {format_datetime(reply_created_at)}"
        else:
            text += "\n\n💭 Пока нет ответов. Будьте первым!"
        
        markup = telebot.types.InlineKeyboardMarkup(row_width=3)
        
        # Пагинация ответов (если есть ответы)
        if total_replies > per_page:
            pagination_row = []
            if reply_page > 1:
                pagination_row.append(
                    telebot.types.InlineKeyboardButton("◀️ Ответы", callback_data=f"view_topic_{topic_id}_{reply_page-1}")
                )
            
            pagination_row.append(
                telebot.types.InlineKeyboardButton(f"{reply_page}/{total_pages}", callback_data=f"view_topic_{topic_id}_{reply_page}")
            )
            
            if reply_page < total_pages:
                pagination_row.append(
                    telebot.types.InlineKeyboardButton("Ответы ▶️", callback_data=f"view_topic_{topic_id}_{reply_page+1}")
                )
            
            if pagination_row:
                markup.add(*pagination_row)
        
        # Основные кнопки
        if is_active:
            markup.add(telebot.types.InlineKeyboardButton("💬 ОТВЕТИТЬ", callback_data=f"reply_topic_{topic_id}"))
        
        # Кнопки управления (только для автора)
        if is_author:
            if is_active:
                markup.add(
                    telebot.types.InlineKeyboardButton("🔒 ЗАКРЫТЬ", callback_data=f"close_topic_{topic_id}"),
                    telebot.types.InlineKeyboardButton("🗑️ УДАЛИТЬ", callback_data=f"delete_topic_{topic_id}")
                )
            else:
                markup.add(
                    telebot.types.InlineKeyboardButton("🗑️ УДАЛИТЬ", callback_data=f"delete_topic_{topic_id}")
                )
        
        # Навигация
        nav_buttons = []
        
        nav_buttons.append(telebot.types.InlineKeyboardButton("🔙 В МЕНЮ", callback_data="menu"))
        markup.add(*nav_buttons)
        
        send_message_with_delete(call.message.chat.id, call.from_user.id, 'view_topic', text, markup)
        bot.answer_callback_query(call.id)
        
    except Exception as e:
        logger.error(f"Ошибка в view_topic_callback: {e}")
        bot.answer_callback_query(call.id, "❌ Ошибка при загрузке темы", show_alert=True)

# ==================== ЗАКРЫТИЕ ТЕМЫ ====================
@bot.callback_query_handler(func=lambda call: call.data.startswith("close_topic_"))
def close_topic_callback(call):
    """Закрытие темы"""
    try:
        topic_id = int(call.data.split("_")[2])
        user_id = call.from_user.id
        
        text = f"""<b>🔒 ЗАКРЫТИЕ ТЕМЫ #{topic_id}</b>

Вы уверены, что хотите закрыть эту тему?

⚠️ <b>После закрытия:</b>
• Новые ответы невозможны
• Существующие ответы остаются
• Тема отмечена как закрытая

<b>Это действие необратимо.</b>"""
        
        markup = telebot.types.InlineKeyboardMarkup(row_width=2)
        markup.add(
            telebot.types.InlineKeyboardButton("✅ ДА, ЗАКРЫТЬ", callback_data=f"confirm_close_{topic_id}"),
            telebot.types.InlineKeyboardButton("❌ НЕТ, ОТМЕНА", callback_data=f"view_topic_{topic_id}_1")
        )
        
        send_message_with_delete(call.message.chat.id, user_id, 'view_topic', text, markup)
        bot.answer_callback_query(call.id)
        
    except Exception as e:
        logger.error(f"Ошибка в close_topic_callback: {e}")
        bot.answer_callback_query(call.id, "❌ Ошибка", show_alert=True)

@bot.callback_query_handler(func=lambda call: call.data.startswith("confirm_close_"))
def confirm_close_callback(call):
    """Подтверждение закрытия темы"""
    try:
        topic_id = int(call.data.split("_")[2])
        user_id = call.from_user.id
        
        success, message = close_topic(topic_id, user_id)
        
        if success:
            bot.answer_callback_query(call.id, "✅ Тема закрыта")
            text = f"""✅ <b>ТЕМА ЗАКРЫТА</b>

Тема #{topic_id} успешно закрыта.

📊 <b>Что изменилось:</b>
• Новые ответы невозможны
• Тема отмечена как закрытая
• Существующие ответы остаются"""
        else:
            bot.answer_callback_query(call.id, f"❌ {message}", show_alert=True)
            text = f"""❌ <b>ОШИБКА</b>

{message}"""
        
        markup = telebot.types.InlineKeyboardMarkup()
        markup.add(
            telebot.types.InlineKeyboardButton("📄 К ТЕМЕ", callback_data=f"view_topic_{topic_id}_1"),
            telebot.types.InlineKeyboardButton("🔙 В МЕНЮ", callback_data="menu")
        )
        
        send_message_with_delete(call.message.chat.id, user_id, 'view_topic', text, markup)
        
    except Exception as e:
        logger.error(f"Ошибка в confirm_close_callback: {e}")
        bot.answer_callback_query(call.id, "❌ Ошибка", show_alert=True)

# ==================== УДАЛЕНИЕ ТЕМЫ ====================
@bot.callback_query_handler(func=lambda call: call.data.startswith("delete_topic_"))
def delete_topic_callback(call):
    """Удаление темы"""
    try:
        topic_id = int(call.data.split("_")[2])
        replies_count = get_replies_count(topic_id)
        
        text = f"""<b>🗑️ УДАЛЕНИЕ ТЕМЫ #{topic_id}</b>

Вы уверены, что хотите удалить эту тему?

⚠️ <b>Внимание:</b>
• Тема удалится полностью
• Все ответы ({replies_count}) удалятся
• Действие необратимо

<b>Это действие нельзя отменить!</b>"""
        
        markup = telebot.types.InlineKeyboardMarkup(row_width=2)
        markup.add(
            telebot.types.InlineKeyboardButton("🗑️ ДА, УДАЛИТЬ", callback_data=f"confirm_delete_{topic_id}"),
            telebot.types.InlineKeyboardButton("❌ НЕТ, ОТМЕНА", callback_data=f"view_topic_{topic_id}_1")
        )
        
        send_message_with_delete(call.message.chat.id, call.from_user.id, 'view_topic', text, markup)
        bot.answer_callback_query(call.id)
        
    except Exception as e:
        logger.error(f"Ошибка в delete_topic_callback: {e}")
        bot.answer_callback_query(call.id, "❌ Ошибка", show_alert=True)

@bot.callback_query_handler(func=lambda call: call.data.startswith("confirm_delete_"))
def confirm_delete_callback(call):
    """Подтверждение удаления темы"""
    try:
        topic_id = int(call.data.split("_")[2])
        user_id = call.from_user.id
        
        success, message = delete_topic(topic_id, user_id)
        
        if success:
            bot.answer_callback_query(call.id, "✅ Тема удалена")
            text = f"""✅ <b>ТЕМА УДАЛЕНА</b>

Тема #{topic_id} и все ответы удалены.

🗄️ <b>Архив обновлен:</b>
• Тема удалена полностью
• Все ответы удалены
• Ничего не осталось"""
        else:
            bot.answer_callback_query(call.id, f"❌ {message}", show_alert=True)
            text = f"""❌ <b>ОШИБКА</b>

{message}"""
        
        markup = telebot.types.InlineKeyboardMarkup()
        markup.add(telebot.types.InlineKeyboardButton("🔙 В МЕНЮ", callback_data="menu"))
        
        send_message_with_delete(call.message.chat.id, user_id, 'view_topic', text, markup)
        
    except Exception as e:
        logger.error(f"Ошибка в confirm_delete_callback: {e}")
        bot.answer_callback_query(call.id, "❌ Ошибка", show_alert=True)

# ==================== ОТВЕТ НА ТЕМУ ====================
@bot.callback_query_handler(func=lambda call: call.data.startswith("reply_topic_"))
def reply_topic_callback(call):
    """Ответ на тему"""
    try:
        topic_id = int(call.data.split("_")[2])
        
        # Проверяем, активна ли тема
        topic = get_topic(topic_id)
        if not topic or not topic[3]:  # is_active
            bot.answer_callback_query(call.id, "❌ Тема закрыта", show_alert=True)
            return
        
        user_states[call.from_user.id] = {'state': 'reply_topic', 'topic_id': topic_id}
        
        topic_text = topic[1]
        preview = topic_text[:100] + "..." if len(topic_text) > 100 else topic_text
        
        text = f"""<b>💬 ОТВЕТ НА ТЕМУ #{topic_id}</b>

{html.escape(preview)}

<b>Напишите ваш ответ:</b>
• От 2 до 1000 символов
• Анонимный ответ
• Будьте уважительны

🔔 <i>Автор темы получит уведомление о вашем ответе</i>

❌ <b>Отмена:</b> /start"""
        
        markup = telebot.types.InlineKeyboardMarkup()
        markup.add(telebot.types.InlineKeyboardButton("🔙 НАЗАД", callback_data=f"view_topic_{topic_id}_1"))
        
        send_message_with_delete(call.message.chat.id, call.from_user.id, 'new_topic', text, markup)
        bot.answer_callback_query(call.id)
        
    except Exception as e:
        logger.error(f"Ошибка в reply_topic_callback: {e}")
        bot.answer_callback_query(call.id, "❌ Ошибка", show_alert=True)

# ==================== ВОЗВРАТ В МЕНЮ ====================
@bot.callback_query_handler(func=lambda call: call.data == "menu")
def menu_callback(call):
    """Возврат в меню"""
    show_main_menu(call.message.chat.id, call.from_user.id)
    bot.answer_callback_query(call.id)

# ==================== ОБРАБОТКА ТЕКСТА ====================
@bot.message_handler(func=lambda message: True)
def text_handler(message):
    """Обработка текстовых сообщений"""
    user_id = message.from_user.id
    chat_id = message.chat.id
    text = message.text.strip()
    
    # Удаляем сообщение пользователя
    try:
        bot.delete_message(chat_id, message.message_id)
    except:
        pass
    
    if user_id not in user_states:
        show_main_menu(chat_id, user_id)
        return
    
    state = user_states[user_id]
    
    if state['state'] == 'new_topic':
        if len(text) < 2:
            msg = bot.send_message(chat_id, "❌ Слишком коротко. Минимум 2 символа.")
            add_message_to_delete(user_id, msg.message_id)
            return
        if len(text) > 2000:
            msg = bot.send_message(chat_id, "❌ Слишком длинно. Максимум 2000 символов.")
            add_message_to_delete(user_id, msg.message_id)
            return
        
        topic_id = add_topic(text, user_id)
        del user_states[user_id]
        
        response = f"""✅ <b>ТЕМА #{topic_id} СОЗДАНА</b>

{html.escape(text[:100])}{'...' if len(text) > 100 else ''}

<b>🎲 Теперь вы можете:</b>
• Ответить на случайную тему
• Управлять своей темой
• Получать уведомления при ответах"""
        
        markup = telebot.types.InlineKeyboardMarkup(row_width=1)
        markup.add(
            telebot.types.InlineKeyboardButton("📄 ПЕРЕЙТИ К ТЕМЕ", callback_data=f"view_topic_{topic_id}_1"),
            telebot.types.InlineKeyboardButton("🎲 СЛУЧАЙНАЯ", callback_data="random_topic"),
            telebot.types.InlineKeyboardButton("🏠 В МЕНЮ", callback_data="menu")
        )
        
        send_message_with_delete(chat_id, user_id, 'topic_created', response, markup)
        
    elif state['state'] == 'reply_topic':
        topic_id = state['topic_id']
        
        # Проверяем, активна ли тема
        topic = get_topic(topic_id)
        if not topic or not topic[3]:  # is_active
            msg = bot.send_message(chat_id, "❌ Тема закрыта, нельзя оставить ответ.")
            add_message_to_delete(user_id, msg.message_id)
            show_main_menu(chat_id, user_id)
            del user_states[user_id]
            return
        
        if len(text) < 2:
            msg = bot.send_message(chat_id, "❌ Слишком короткий ответ. Минимум 2 символа.")
            add_message_to_delete(user_id, msg.message_id)
            return
        if len(text) > 1000:
            msg = bot.send_message(chat_id, "❌ Слишком длинный ответ. Максимум 1000 символов.")
            add_message_to_delete(user_id, msg.message_id)
            return
        
        reply_id = add_reply(topic_id, text, user_id)
        del user_states[user_id]
        
        response = f"""✅ <b>ОТВЕТ #{reply_id} СОХРАНЕН</b>

Вы ответили на тему #{topic_id}.

<b>💭 Что дальше?</b>
• Автор темы получил уведомление
• Ответ доступен всем пользователям
• Вы можете ответить еще"""
        
        markup = telebot.types.InlineKeyboardMarkup(row_width=1)
        markup.add(
            telebot.types.InlineKeyboardButton("📄 ПЕРЕЙТИ К ТЕМЕ", callback_data=f"view_topic_{topic_id}_1"),
            telebot.types.InlineKeyboardButton("🎲 СЛУЧАЙНАЯ", callback_data="random_topic"),
            telebot.types.InlineKeyboardButton("🏠 В МЕНУ", callback_data="menu")
        )
        
        send_message_with_delete(chat_id, user_id, 'reply_created', response, markup)

# ==================== WEB SERVER FOR RAILWAY ====================
from flask import Flask
import threading
import os

# Создаем простой Flask сервер для Railway
web_app = Flask(__name__)

@web_app.route('/')
def home():
    return """<h1>🤖 Telegram Bot - Archive of Thoughts</h1>
    <p>Bot is running on Railway!</p>
    <p>Status: ✅ Online</p>
    <p>Database: thoughts_archive.db</p>
    <p>Daily limit: 5 topics per user</p>"""

@web_app.route('/health')
def health():
    return {"status": "ok", "bot": "running", "service": "telegram-bot"}

@web_app.route('/ping')
def ping():
    return "pong"

def run_web_server():
    """Запуск веб-сервера для Railway"""
    port = int(os.environ.get("PORT", 5000))
    web_app.run(host='0.0.0.0', port=port, debug=False, use_reloader=False)

# ==================== ЗАПУСК ====================
if __name__ == '__main__':
    logger.info("🗄️ Бот 'Архив мыслей' запущен...")
    logger.info(f"📂 Новая база данных: {DB_NAME}")
    logger.info("👤 Система уникальных имен 'аноним_XXXX' активирована")
    logger.info("🔔 Система уведомлений активирована")
    logger.info("🧹 Функция удаления предыдущих сообщений активирована")
    logger.info("🔄 Система уникального просмотра тем активирована")
    logger.info("⚠️ Система жалоб и модерации активирована")
    logger.info("👤 Система личных кабинетов активирована")
    logger.info("✏️ Система имен пользователей активирована")
    logger.info("🏆 Команда /top активирована")
    logger.info(f"📊 Система статусов активирована ({len(RANK_SYSTEM)} рангов)")
    logger.info(f"📅 Дневной лимит тем: {DAILY_TOPIC_LIMIT}")
    logger.info("📌 В групповых чатах бот игнорирует все сообщения кроме /top")
    logger.info("💬 В личных чатах работает полный функционал")
    
    # Очищаем невалидные жалобы при запуске
    cleanup_invalid_reports()
    
    if ADMIN_ID:
        logger.info(f"⚙️ Администратор: {ADMIN_ID}")
    else:
        logger.warning("⚠️ ID администратора не установлен. Установите ADMIN_ID в настройках.")
    
    # Удаляем вебхук и запускаем бота
    bot.remove_webhook()
    
    # ЗАПУСКАЕМ ВЕБ-СЕРВЕР ДЛЯ RAILWAY В ОТДЕЛЬНОМ ПОТОКЕ
    logger.info("🚀 Запускаю веб-сервер для Railway...")
    web_thread = threading.Thread(target=run_web_server, daemon=True)
    web_thread.start()
    
    port = int(os.environ.get("PORT", 5000))
    logger.info(f"🌐 Веб-сервер запущен на порту {port}")
    logger.info(f"🌐 URL: http://0.0.0.0:{port}")
    logger.info(f"🌐 Health check: http://0.0.0.0:{port}/health")
    
    # Запускаем Telegram бота
    logger.info("🤖 Запускаю Telegram бота...")
    try:
        bot.polling(
            none_stop=True,
            timeout=30,
            interval=2,
            skip_pending=True
        )
    except KeyboardInterrupt:
        logger.info("Бот остановлен пользователем")
    except Exception as e:
        logger.error(f"Критическая ошибка: {e}")
