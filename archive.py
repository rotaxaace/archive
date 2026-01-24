import telebot
import psycopg2
import random
from datetime import datetime, timedelta
import logging
import html
import re
import os
import urllib.parse as urlparse

# ==================== НАСТРОЙКИ ====================
BOT_TOKEN = os.environ.get("BOT_TOKEN")
ADMIN_ID = int(os.environ.get("ADMIN_ID", 0))
DAILY_TOPIC_LIMIT = 5

# PostgreSQL подключение
DATABASE_URL = os.environ.get("DATABASE_URL")
if DATABASE_URL:
    url = urlparse.urlparse(DATABASE_URL)
    db_params = {
        'database': url.path[1:],
        'user': url.username,
        'password': url.password,
        'host': url.hostname,
        'port': url.port
    }
else:
    db_params = {
        'database': 'railway',
        'user': 'postgres',
        'password': 'vaUPCSdlOJSRxhdTLjwzreixKFTQCtDy',
        'host': 'tramway.proxy.rlwy.net',
        'port': 38575
    }

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
    'notification': 'https://ibb.co/mCDDWKyG',
    'profile': 'https://ibb.co/YBynCpDG',
    'admin': 'https://ibb.co/5gc6GcCt',
    'report': 'https://ibb.co/N25WXBsz',
    'top': 'https://ibb.co/hxqVGCHV',
    'limit': 'https://ibb.co/xqZZBn1v'
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

# ==================== БАЗА ДАННЫХ POSTGRESQL ====================
def get_db_connection():
    """Получение соединения с PostgreSQL"""
    try:
        conn = psycopg2.connect(**db_params)
        return conn
    except Exception as e:
        logger.error(f"Ошибка подключения к PostgreSQL: {e}")
        raise

def init_db():
    """Инициализация базы данных PostgreSQL"""
    conn = get_db_connection()
    c = conn.cursor()
    
    # Таблица тем
    c.execute('''
        CREATE TABLE IF NOT EXISTS topics (
            id SERIAL PRIMARY KEY,
            text TEXT NOT NULL,
            user_id BIGINT NOT NULL,
            is_active BOOLEAN DEFAULT TRUE,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    ''')
    
    # Таблица ответов
    c.execute('''
        CREATE TABLE IF NOT EXISTS replies (
            id SERIAL PRIMARY KEY,
            topic_id INTEGER NOT NULL REFERENCES topics(id) ON DELETE CASCADE,
            text TEXT NOT NULL,
            user_id BIGINT NOT NULL,
            is_active BOOLEAN DEFAULT TRUE,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    ''')
    
    # Таблица жалоб
    c.execute('''
        CREATE TABLE IF NOT EXISTS reports (
            id SERIAL PRIMARY KEY,
            topic_id INTEGER NOT NULL REFERENCES topics(id) ON DELETE CASCADE,
            reporter_id BIGINT NOT NULL,
            reason TEXT NOT NULL,
            status TEXT DEFAULT 'pending',
            admin_action TEXT,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            resolved_at TIMESTAMP,
            admin_id BIGINT
        )
    ''')
    
    # Таблица банов
    c.execute('''
        CREATE TABLE IF NOT EXISTS bans (
            id SERIAL PRIMARY KEY,
            user_id BIGINT NOT NULL UNIQUE,
            reason TEXT NOT NULL,
            admin_id BIGINT NOT NULL,
            banned_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            unbanned_at TIMESTAMP,
            is_active BOOLEAN DEFAULT TRUE
        )
    ''')
    
    # Таблица статистики пользователей
    c.execute('''
        CREATE TABLE IF NOT EXISTS user_stats (
            user_id BIGINT PRIMARY KEY,
            topics_created INTEGER DEFAULT 0,
            replies_written INTEGER DEFAULT 0,
            replies_received INTEGER DEFAULT 0,
            last_active TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    ''')
    
    # Таблица никнеймов пользователей
    c.execute('''
        CREATE TABLE IF NOT EXISTS user_names (
            user_id BIGINT PRIMARY KEY,
            username TEXT NOT NULL UNIQUE,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    ''')
    
    # Таблица для дневных лимитов
    c.execute('''
        CREATE TABLE IF NOT EXISTS daily_limits (
            user_id BIGINT NOT NULL,
            date DATE NOT NULL,
            topics_created INTEGER DEFAULT 0,
            PRIMARY KEY (user_id, date)
        )
    ''')
    
    # Таблица настроек уведомлений пользователей
    c.execute('''
        CREATE TABLE IF NOT EXISTS user_notifications (
            user_id BIGINT PRIMARY KEY,
            reply_notifications BOOLEAN DEFAULT TRUE,
            updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    ''')
    
    # Создаем индексы
    c.execute('CREATE INDEX IF NOT EXISTS idx_topics_user_id ON topics(user_id)')
    c.execute('CREATE INDEX IF NOT EXISTS idx_topics_active ON topics(is_active)')
    c.execute('CREATE INDEX IF NOT EXISTS idx_replies_topic_id ON replies(topic_id)')
    c.execute('CREATE INDEX IF NOT EXISTS idx_replies_user_id ON replies(user_id)')
    c.execute('CREATE INDEX IF NOT EXISTS idx_reports_status ON reports(status)')
    c.execute('CREATE INDEX IF NOT EXISTS idx_bans_active ON bans(is_active)')
    c.execute('CREATE INDEX IF NOT EXISTS idx_bans_unbanned ON bans(unbanned_at)')
    c.execute('CREATE INDEX IF NOT EXISTS idx_user_names_username ON user_names(username)')
    c.execute('CREATE INDEX IF NOT EXISTS idx_daily_limits_date ON daily_limits(date)')
    
    conn.commit()
    c.close()
    conn.close()
    logger.info("База данных PostgreSQL инициализирована")

init_db()

# ==================== ФУНКЦИИ ДЛЯ УВЕДОМЛЕНИЙ ====================
def get_user_notification_setting(user_id):
    """Получение настройки уведомлений пользователя"""
    try:
        conn = get_db_connection()
        c = conn.cursor()
        c.execute('SELECT reply_notifications FROM user_notifications WHERE user_id = %s', (user_id,))
        result = c.fetchone()
        c.close()
        conn.close()
        
        if result:
            return result[0]
        else:
            conn = get_db_connection()
            c = conn.cursor()
            c.execute('INSERT INTO user_notifications (user_id, reply_notifications) VALUES (%s, TRUE)', (user_id,))
            conn.commit()
            c.close()
            conn.close()
            return True
    except Exception as e:
        logger.error(f"Ошибка при получении настроек уведомлений пользователя {user_id}: {e}")
        return True

def toggle_user_notifications(user_id):
    """Переключение уведомлений пользователя"""
    try:
        current_setting = get_user_notification_setting(user_id)
        new_setting = not current_setting
        
        conn = get_db_connection()
        c = conn.cursor()
        c.execute('''
            INSERT INTO user_notifications (user_id, reply_notifications, updated_at) 
            VALUES (%s, %s, CURRENT_TIMESTAMP)
            ON CONFLICT (user_id) 
            DO UPDATE SET reply_notifications = %s, updated_at = CURRENT_TIMESTAMP
        ''', (user_id, new_setting, new_setting))
        conn.commit()
        c.close()
        conn.close()
        
        return new_setting
    except Exception as e:
        logger.error(f"Ошибка при переключении уведомлений пользователя {user_id}: {e}")
        return current_setting

# ==================== СИСТЕМА ГЕНЕРАЦИИ УНИКАЛЬНЫХ ИМЕН ====================
def generate_unique_username():
    """Генерация уникального имени пользователя формата 'аноним_XXXX'"""
    while True:
        random_digits = ''.join([str(random.randint(0, 9)) for _ in range(4)])
        username = f"аноним_{random_digits}"
        
        try:
            conn = get_db_connection()
            c = conn.cursor()
            c.execute('SELECT user_id FROM user_names WHERE username = %s', (username,))
            result = c.fetchone()
            c.close()
            conn.close()
            
            if not result:
                return username
        except Exception as e:
            logger.error(f"Ошибка при генерации уникального имени: {e}")
            return f"аноним_{random.randint(1000, 9999)}"

def get_username(user_id):
    """Получение имени пользователя, создание уникального если нет"""
    try:
        conn = get_db_connection()
        c = conn.cursor()
        c.execute('SELECT username FROM user_names WHERE user_id = %s', (user_id,))
        result = c.fetchone()
        
        if result and result[0]:
            username = result[0]
            c.close()
            conn.close()
            return username
        else:
            username = generate_unique_username()
            c.execute('''
                INSERT INTO user_names (user_id, username) 
                VALUES (%s, %s)
                ON CONFLICT (user_id) DO NOTHING
            ''', (user_id, username))
            conn.commit()
            c.close()
            conn.close()
            return username
    except Exception as e:
        logger.error(f"Ошибка при получении имени пользователя {user_id}: {e}")
        return f"аноним_{user_id % 10000:04d}"

# ==================== СИСТЕМА СТАТУСОВ ====================
RANK_SYSTEM = {
    1: {'name': '👶 НОВИЧОК', 'emoji': '👶', 'requirements': {'max_topics': 4, 'max_replies': 9}, 'next_rank': 2},
    2: {'name': '🧒 ПОСЕТИТЕЛЬ', 'emoji': '🧒', 'requirements': {'max_topics': 9, 'max_replies': 24}, 'next_rank': 3},
    3: {'name': '👨 УЧАСТНИК', 'emoji': '👨', 'requirements': {'max_topics': 19, 'max_replies': 49}, 'next_rank': 4},
    4: {'name': '👨‍💼 АКТИВИСТ', 'emoji': '👨‍💼', 'requirements': {'max_topics': 34, 'max_replies': 99}, 'next_rank': 5},
    5: {'name': '👨‍🔬 АВТОР', 'emoji': '👨‍🔬', 'requirements': {'max_topics': 54, 'max_replies': 199}, 'next_rank': 6},
    6: {'name': '👨‍🎓 МЫСЛИТЕЛЬ', 'emoji': '👨‍🎓', 'requirements': {'max_topics': 84, 'max_replies': 399}, 'next_rank': 7},
    7: {'name': '👨‍🚀 ДИСКУТАНТ', 'emoji': '👨‍🚀', 'requirements': {'max_topics': 129, 'max_replies': 699}, 'next_rank': 8},
    8: {'name': '👨‍✈️ ФИЛОСОФ', 'emoji': '👨‍✈️', 'requirements': {'max_topics': 199, 'max_replies': 1199}, 'next_rank': 9},
    9: {'name': '👑 МАСТЕР', 'emoji': '👑', 'requirements': {'max_topics': 299, 'max_replies': 1999}, 'next_rank': 10},
    10: {'name': '⚡ ЛЕГЕНДА', 'emoji': '⚡', 'requirements': {'max_topics': 999999, 'max_replies': 999999}, 'next_rank': None}
}

def get_user_rank(user_id):
    stats = get_user_statistics(user_id)
    return get_user_rank_by_stats(stats)

def get_user_rank_by_stats(stats):
    topics = stats['topics_created']
    replies = stats['replies_written']
    
    for rank_id, rank_info in RANK_SYSTEM.items():
        req = rank_info['requirements']
        if topics <= req['max_topics'] and replies <= req['max_replies']:
            return rank_id
    
    return 10

def get_rank_progress(user_id):
    stats = get_user_statistics(user_id)
    current_rank = get_user_rank_by_stats(stats)
    
    if current_rank >= 10:
        return {
            'current_rank': current_rank,
            'next_rank': None,
            'progress': 100,
            'remaining': {'topics': 0, 'replies': 0}
        }
    
    next_rank = current_rank + 1
    next_req = RANK_SYSTEM[next_rank]['requirements']
    
    topics_progress = min(100, int((stats['topics_created'] / next_req['max_topics']) * 100)) if next_req['max_topics'] > 0 else 100
    replies_progress = min(100, int((stats['replies_written'] / next_req['max_replies']) * 100)) if next_req['max_replies'] > 0 else 100
    
    total_progress = (topics_progress + replies_progress) // 2
    
    return {
        'current_rank': current_rank,
        'next_rank': next_rank,
        'progress': total_progress,
        'remaining': {
            'topics': max(0, next_req['max_topics'] - stats['topics_created']),
            'replies': max(0, next_req['max_replies'] - stats['replies_written'])
        }
    }

def get_progress_bar(progress, length=10):
    filled = int(progress / 100 * length)
    empty = length - filled
    return '▰' * filled + '▱' * empty

# ==================== ВСПОМОГАТЕЛЬНЫЕ ФУНКЦИИ ====================
def format_datetime(dt_str):
    try:
        dt = datetime.strptime(dt_str, '%Y-%m-%d %H:%M:%S')
        return dt.strftime('%d.%m.%Y %H:%M')
    except:
        return dt_str

def format_timedelta(td):
    try:
        if not td or not hasattr(td, 'days'):
            return "неизвестно"
        
        if td.days > 0:
            return f"{td.days} дн. назад"
        elif td.seconds >= 3600:
            hours = td.seconds // 3600
            return f"{hours} ч. назад"
        elif td.seconds >= 60:
            minutes = td.seconds // 60
            return f"{minutes} мин. назад"
        else:
            return "только что"
    except Exception:
        return "неизвестно"

def sanitize_html(text):
    if not text:
        return text
    
    text = html.escape(text)
    text = text.replace('&lt;b&gt;', '<b>').replace('&lt;/b&gt;', '</b>')
    text = text.replace('&lt;i&gt;', '<i>').replace('&lt;/i&gt;', '</i>')
    
    return text

def validate_username(username):
    if not username:
        return False, "Имя не может быть пустым"
    
    if len(username) < 3:
        return False, "Имя должно быть не менее 3 символов"
    
    if len(username) > 12:
        return False, "Имя должно быть не более 12 символов"
    
    pattern = r'^[a-zA-Zа-яА-ЯёЁ0-9_]+$'
    if not re.match(pattern, username):
        return False, "Можно использовать только буквы, цифры и нижнее подчеркивание"
    
    return True, "OK"

def set_username(user_id, username):
    try:
        conn = get_db_connection()
        c = conn.cursor()
        
        c.execute('SELECT user_id FROM user_names WHERE username = %s AND user_id != %s', (username, user_id))
        if c.fetchone():
            c.close()
            conn.close()
            return False, "Это имя уже занято другим пользователем"
        
        c.execute('''
            INSERT INTO user_names (user_id, username, updated_at) 
            VALUES (%s, %s, CURRENT_TIMESTAMP)
            ON CONFLICT (user_id) 
            DO UPDATE SET username = %s, updated_at = CURRENT_TIMESTAMP
        ''', (user_id, username, username))
        conn.commit()
        c.close()
        conn.close()
        return True, "Имя успешно изменено"
    except Exception as e:
        logger.error(f"Ошибка при установке имени пользователя {user_id}: {e}")
        return False, f"Ошибка: {str(e)}"

# ==================== СИСТЕМА ЛИМИТОВ ====================
def check_daily_topic_limit(user_id):
    try:
        conn = get_db_connection()
        c = conn.cursor()
        today = datetime.now().strftime('%Y-%m-%d')
        
        c.execute('SELECT topics_created FROM daily_limits WHERE user_id = %s AND date = %s', (user_id, today))
        result = c.fetchone()
        c.close()
        conn.close()
        
        if result:
            topics_today = result[0]
            remaining = max(0, DAILY_TOPIC_LIMIT - topics_today)
            return remaining, topics_today
        else:
            return DAILY_TOPIC_LIMIT, 0
    except Exception as e:
        logger.error(f"Ошибка при проверке лимита тем пользователя {user_id}: {e}")
        return DAILY_TOPIC_LIMIT, 0

def increment_daily_topic_count(user_id):
    try:
        conn = get_db_connection()
        c = conn.cursor()
        today = datetime.now().strftime('%Y-%m-%d')
        
        c.execute('''
            INSERT INTO daily_limits (user_id, date, topics_created)
            VALUES (%s, %s, 1)
            ON CONFLICT (user_id, date) 
            DO UPDATE SET topics_created = daily_limits.topics_created + 1
        ''', (user_id, today))
        conn.commit()
        c.close()
        conn.close()
        return True
    except Exception as e:
        logger.error(f"Ошибка при увеличении счетчика тем пользователя {user_id}: {e}")
        return False

# ==================== ФУНКЦИЯ ПРОВЕРКИ БАНА ====================
def check_user_ban(user_id):
    try:
        conn = get_db_connection()
        c = conn.cursor()
        c.execute('''
            SELECT id, reason, unbanned_at FROM bans 
            WHERE user_id = %s 
            AND is_active = TRUE 
            AND unbanned_at > CURRENT_TIMESTAMP
        ''', (user_id,))
        result = c.fetchone()
        c.close()
        conn.close()
        return result
    except Exception as e:
        logger.error(f"Ошибка при проверке бана пользователя {user_id}: {e}")
        return None

def is_user_banned(user_id):
    ban_info = check_user_ban(user_id)
    return ban_info is not None

# ==================== ОСНОВНЫЕ ФУНКЦИИ БАЗЫ ДАННЫХ ====================
def add_topic(text, user_id):
    if is_user_banned(user_id):
        logger.error(f"🚨 ПОЛЬЗОВАТЕЛЬ {user_id} ЗАБАНЕН! Тема НЕ создана.")
        return None
    
    remaining, topics_today = check_daily_topic_limit(user_id)
    if remaining <= 0:
        logger.warning(f"Пользователь {user_id} достиг дневного лимита тем ({topics_today}/{DAILY_TOPIC_LIMIT})")
        return "limit_exceeded"
    
    clean_text = ' '.join(text.strip().split())
    
    try:
        conn = get_db_connection()
        c = conn.cursor()
        
        c.execute('INSERT INTO topics (text, user_id) VALUES (%s, %s) RETURNING id', (clean_text, user_id))
        topic_id = c.fetchone()[0]
        
        c.execute('''
            INSERT INTO user_stats (user_id, topics_created, replies_written, replies_received) 
            VALUES (%s, 1, 0, 0)
            ON CONFLICT (user_id) 
            DO UPDATE SET topics_created = user_stats.topics_created + 1,
                         last_active = CURRENT_TIMESTAMP
        ''', (user_id,))
        
        increment_daily_topic_count(user_id)
        
        conn.commit()
        c.close()
        conn.close()
        
        logger.info(f"✅ Тема #{topic_id} создана пользователем {user_id}")
        return topic_id
        
    except Exception as e:
        logger.error(f"❌ Ошибка при создании темы пользователем {user_id}: {e}")
        return None

def add_reply(topic_id, text, user_id):
    if is_user_banned(user_id):
        logger.error(f"🚨 ПОЛЬЗОВАТЕЛЬ {user_id} ЗАБАНЕН! Ответ НЕ создан.")
        return None
    
    clean_text = ' '.join(text.strip().split())
    
    try:
        conn = get_db_connection()
        c = conn.cursor()
        
        c.execute('SELECT user_id, is_active FROM topics WHERE id = %s', (topic_id,))
        topic = c.fetchone()
        
        if not topic:
            logger.error(f"❌ Тема #{topic_id} не найдена")
            c.close()
            conn.close()
            return None
        
        topic_author_id = topic[0]
        is_active = topic[1]
        
        if not is_active:
            logger.error(f"❌ Тема #{topic_id} закрыта")
            c.close()
            conn.close()
            return "closed"
        
        c.execute('INSERT INTO replies (topic_id, text, user_id) VALUES (%s, %s, %s) RETURNING id', 
                  (topic_id, clean_text, user_id))
        reply_id = c.fetchone()[0]
        
        c.execute('UPDATE topics SET updated_at = CURRENT_TIMESTAMP WHERE id = %s', (topic_id,))
        
        c.execute('''
            INSERT INTO user_stats (user_id, topics_created, replies_written, replies_received) 
            VALUES (%s, 0, 1, 0)
            ON CONFLICT (user_id) 
            DO UPDATE SET replies_written = user_stats.replies_written + 1,
                         last_active = CURRENT_TIMESTAMP
        ''', (user_id,))
        
        c.execute('''
            INSERT INTO user_stats (user_id, topics_created, replies_written, replies_received) 
            VALUES (%s, 0, 0, 1)
            ON CONFLICT (user_id) 
            DO UPDATE SET replies_received = user_stats.replies_received + 1
        ''', (topic_author_id,))
        
        conn.commit()
        c.close()
        conn.close()
        
        if topic_author_id != user_id:
            notifications_enabled = get_user_notification_setting(topic_author_id)
            if notifications_enabled:
                send_reply_notification(topic_author_id, topic_id, reply_id, clean_text)
        
        logger.info(f"✅ Ответ #{reply_id} создан пользователем {user_id} к теме #{topic_id}")
        return reply_id
        
    except Exception as e:
        logger.error(f"❌ Ошибка при создании ответа пользователем {user_id}: {e}")
        return None

def get_topic(topic_id, user_id=None):
    conn = get_db_connection()
    c = conn.cursor()
    if user_id:
        c.execute('SELECT * FROM topics WHERE id = %s', (topic_id,))
    else:
        c.execute('SELECT * FROM topics WHERE id = %s AND is_active = TRUE', (topic_id,))
    result = c.fetchone()
    c.close()
    conn.close()
    return result

def close_topic(topic_id, user_id):
    conn = get_db_connection()
    c = conn.cursor()
    
    c.execute('SELECT user_id FROM topics WHERE id = %s', (topic_id,))
    topic = c.fetchone()
    
    if not topic:
        c.close()
        conn.close()
        return False, "Тема не найдена"
    
    if topic[0] != user_id:
        c.close()
        conn.close()
        return False, "Вы не автор этой темы"
    
    c.execute('UPDATE topics SET is_active = FALSE WHERE id = %s', (topic_id,))
    conn.commit()
    c.close()
    conn.close()
    return True, "✅ Тема закрыта"

def delete_topic(topic_id, user_id):
    conn = get_db_connection()
    c = conn.cursor()
    
    c.execute('SELECT user_id FROM topics WHERE id = %s', (topic_id,))
    topic = c.fetchone()
    
    if not topic:
        c.close()
        conn.close()
        return False, "Тема не найдена"
    
    if topic[0] != user_id:
        c.close()
        conn.close()
        return False, "Вы не автор этой темы"
    
    c.execute('DELETE FROM replies WHERE topic_id = %s', (topic_id,))
    c.execute('DELETE FROM topics WHERE id = %s', (topic_id,))
    
    conn.commit()
    c.close()
    conn.close()
    return True, "✅ Тема и все ответы удалены"

def get_random_topic(exclude_user_id=None, viewed_topics=None):
    conn = get_db_connection()
    c = conn.cursor()
    
    query = 'SELECT * FROM topics WHERE is_active = TRUE'
    params = []
    
    if exclude_user_id:
        query += ' AND user_id != %s'
        params.append(exclude_user_id)
    
    if viewed_topics and len(viewed_topics) > 0:
        query += ' AND id NOT IN %s'
        params.append(tuple(viewed_topics))
    
    query += ' ORDER BY RANDOM() LIMIT 1'
    
    c.execute(query, params)
    result = c.fetchone()
    c.close()
    conn.close()
    return result

def get_all_active_topics_count(exclude_user_id=None):
    conn = get_db_connection()
    c = conn.cursor()
    
    if exclude_user_id:
        c.execute('SELECT COUNT(*) FROM topics WHERE is_active = TRUE AND user_id != %s', (exclude_user_id,))
    else:
        c.execute('SELECT COUNT(*) FROM topics WHERE is_active = TRUE')
    
    result = c.fetchone()[0] or 0
    c.close()
    conn.close()
    return result

def get_user_topics(user_id, limit=10, offset=0):
    conn = get_db_connection()
    c = conn.cursor()
    c.execute('''
        SELECT t.*, COUNT(r.id) as replies_count
        FROM topics t
        LEFT JOIN replies r ON t.id = r.topic_id AND r.is_active = TRUE
        WHERE t.user_id = %s
        GROUP BY t.id
        ORDER BY t.updated_at DESC 
        LIMIT %s OFFSET %s
    ''', (user_id, limit, offset))
    result = c.fetchall()
    c.close()
    conn.close()
    return result

def get_topic_replies(topic_id, limit=5, offset=0):
    conn = get_db_connection()
    c = conn.cursor()
    c.execute('''
        SELECT r.*
        FROM replies r
        WHERE r.topic_id = %s AND r.is_active = TRUE
        ORDER BY r.created_at ASC
        LIMIT %s OFFSET %s
    ''', (topic_id, limit, offset))
    result = c.fetchall()
    c.close()
    conn.close()
    return result

def get_replies_count(topic_id):
    conn = get_db_connection()
    c = conn.cursor()
    c.execute('SELECT COUNT(*) FROM replies WHERE topic_id = %s AND is_active = TRUE', (topic_id,))
    result = c.fetchone()[0] or 0
    c.close()
    conn.close()
    return result

def get_popular_topics(limit=5):
    conn = get_db_connection()
    c = conn.cursor()
    c.execute('''
        SELECT t.*, COUNT(r.id) as replies_count
        FROM topics t
        LEFT JOIN replies r ON t.id = r.topic_id AND r.is_active = TRUE
        WHERE t.is_active = TRUE
        GROUP BY t.id
        ORDER BY replies_count DESC, t.updated_at DESC
        LIMIT %s
    ''', (limit,))
    result = c.fetchall()
    c.close()
    conn.close()
    return result

def get_popular_topics_with_ownership(user_id, limit=5, offset=0):
    conn = get_db_connection()
    c = conn.cursor()
    c.execute('''
        SELECT t.*, COUNT(r.id) as replies_count,
               CASE WHEN t.user_id = %s THEN 1 ELSE 0 END as is_owner
        FROM topics t
        LEFT JOIN replies r ON t.id = r.topic_id AND r.is_active = TRUE
        WHERE t.is_active = TRUE
        GROUP BY t.id
        ORDER BY replies_count DESC, t.updated_at DESC
        LIMIT %s OFFSET %s
    ''', (user_id, limit, offset))
    result = c.fetchall()
    c.close()
    conn.close()
    return result

def add_report(topic_id, reporter_id, reason):
    try:
        conn = get_db_connection()
        c = conn.cursor()
        c.execute('''
            INSERT INTO reports (topic_id, reporter_id, reason, status) 
            VALUES (%s, %s, %s, 'pending')
            RETURNING id
        ''', (topic_id, reporter_id, reason))
        report_id = c.fetchone()[0]
        conn.commit()
        c.close()
        conn.close()
        return report_id
    except Exception as e:
        logger.error(f"Ошибка при добавлении жалобы: {e}")
        return None

def get_report(report_id):
    try:
        conn = get_db_connection()
        c = conn.cursor()
        c.execute('''
            SELECT r.*, t.text as topic_text, t.user_id as topic_author_id
            FROM reports r
            LEFT JOIN topics t ON r.topic_id = t.id
            WHERE r.id = %s
        ''', (report_id,))
        result = c.fetchone()
        c.close()
        conn.close()
        return result
    except Exception as e:
        logger.error(f"Ошибка при получении жалобы #{report_id}: {e}")
        return None

def get_pending_reports(limit=10, offset=0):
    try:
        conn = get_db_connection()
        c = conn.cursor()
        c.execute('''
            SELECT r.*, t.text as topic_text, t.user_id as topic_author_id
            FROM reports r
            LEFT JOIN topics t ON r.topic_id = t.id
            WHERE r.status = 'pending'
            ORDER BY r.created_at ASC
            LIMIT %s OFFSET %s
        ''', (limit, offset))
        result = c.fetchall()
        c.close()
        conn.close()
        return result
    except Exception as e:
        logger.error(f"Ошибка при получении списка жалоб: {e}")
        return []

def ban_user(user_id, reason, admin_id, days=1):
    conn = get_db_connection()
    c = conn.cursor()
    
    try:
        c.execute('DELETE FROM bans WHERE user_id = %s', (user_id,))
        
        unbanned_at = datetime.now() + timedelta(days=days)
        c.execute('''
            INSERT INTO bans (user_id, reason, admin_id, unbanned_at) 
            VALUES (%s, %s, %s, %s)
        ''', (user_id, reason, admin_id, unbanned_at))
        
        conn.commit()
        send_ban_notification(user_id, reason, days, unbanned_at.strftime('%d.%m.%Y %H:%M'))
        
        logger.info(f"Пользователь {user_id} забанен на {days} дней администратором {admin_id}")
        c.close()
        conn.close()
        return True
    except Exception as e:
        logger.error(f"Ошибка при бане пользователя {user_id}: {e}")
        conn.rollback()
        c.close()
        conn.close()
        return False

def unban_user(user_id):
    conn = get_db_connection()
    c = conn.cursor()
    c.execute('UPDATE bans SET is_active = FALSE WHERE user_id = %s', (user_id,))
    conn.commit()
    c.close()
    conn.close()
    return True

def get_user_statistics(user_id):
    conn = get_db_connection()
    c = conn.cursor()
    c.execute('SELECT * FROM user_stats WHERE user_id = %s', (user_id,))
    stats = c.fetchone()
    c.close()
    conn.close()
    
    if not stats:
        return {'topics_created': 0, 'replies_written': 0, 'replies_received': 0}
    
    return {
        'topics_created': stats[1],
        'replies_written': stats[2],
        'replies_received': stats[3]
    }

def get_top_users(limit=10):
    try:
        conn = get_db_connection()
        c = conn.cursor()
        
        c.execute('''
            SELECT 
                us.user_id,
                COALESCE(un.username, 'user_' || us.user_id) as username,
                us.topics_created,
                us.replies_written,
                (us.topics_created + us.replies_written) as total_activity
            FROM user_stats us
            LEFT JOIN user_names un ON us.user_id = un.user_id
            WHERE us.topics_created > 0 OR us.replies_written > 0
            ORDER BY total_activity DESC, us.replies_written DESC, us.topics_created DESC
            LIMIT %s
        ''', (limit,))
        
        result = c.fetchall()
        c.close()
        conn.close()
        
        if not result or len(result) == 0:
            conn = get_db_connection()
            c = conn.cursor()
            c.execute('''
                SELECT DISTINCT 
                    t.user_id as user_id,
                    COALESCE(un.username, 'user_' || t.user_id) as username,
                    COUNT(t.id) as topics_created,
                    0 as replies_written,
                    COUNT(t.id) as total_activity
                FROM topics t
                LEFT JOIN user_names un ON t.user_id = un.user_id
                WHERE t.user_id IS NOT NULL
                GROUP BY t.user_id
                ORDER BY topics_created DESC
                LIMIT %s
            ''', (limit,))
            
            result = c.fetchall()
            c.close()
            conn.close()
        
        return result
        
    except Exception as e:
        logger.error(f"Ошибка в get_top_users: {e}")
        return []

def get_weekly_record():
    conn = get_db_connection()
    c = conn.cursor()
    c.execute('''
        SELECT 
            t.id as topic_id,
            t.text,
            COUNT(r.id) as replies_count,
            COALESCE(un.username, 'user_' || t.user_id) as author_name
        FROM topics t
        LEFT JOIN replies r ON t.id = r.topic_id
        LEFT JOIN user_names un ON t.user_id = un.user_id
        WHERE t.created_at > CURRENT_TIMESTAMP - INTERVAL '7 days'
        AND t.is_active = TRUE
        GROUP BY t.id
        ORDER BY replies_count DESC
        LIMIT 1
    ''')
    result = c.fetchone()
    c.close()
    conn.close()
    return result

def get_replies_leader():
    conn = get_db_connection()
    c = conn.cursor()
    c.execute('''
        SELECT 
            us.user_id,
            COALESCE(un.username, 'user_' || us.user_id) as username,
            us.replies_written
        FROM user_stats us
        LEFT JOIN user_names un ON us.user_id = un.user_id
        WHERE us.replies_written > 0
        ORDER BY us.replies_written DESC
        LIMIT 1
    ''')
    result = c.fetchone()
    c.close()
    conn.close()
    return result

def get_top_statistics():
    active_topics = get_all_active_topics_count()
    weekly_record = get_weekly_record()
    replies_leader = get_replies_leader()
    top_users = get_top_users(limit=3)
    
    return {
        'active_topics': active_topics,
        'weekly_record': weekly_record,
        'replies_leader': replies_leader,
        'top_users': top_users
    }

def get_admin_statistics():
    conn = get_db_connection()
    c = conn.cursor()
    
    try:
        c.execute('''
            SELECT COUNT(DISTINCT user_id) FROM (
                SELECT user_id FROM topics
                UNION
                SELECT user_id FROM replies
                UNION
                SELECT user_id FROM user_names
                UNION
                SELECT user_id FROM user_stats
            )
        ''')
        total_users = c.fetchone()[0] or 0
        
        c.execute('''
            SELECT COUNT(DISTINCT user_id) FROM (
                SELECT user_id FROM topics WHERE created_at > CURRENT_TIMESTAMP - INTERVAL '24 hours'
                UNION
                SELECT user_id FROM replies WHERE created_at > CURRENT_TIMESTAMP - INTERVAL '24 hours'
            )
        ''')
        active_24h = c.fetchone()[0] or 0
        
        c.execute('''
            SELECT COUNT(DISTINCT user_id) FROM (
                SELECT user_id, MIN(created_at) as first_action FROM (
                    SELECT user_id, created_at FROM topics
                    UNION ALL
                    SELECT user_id, created_at FROM replies
                ) 
                GROUP BY user_id
                HAVING first_action > CURRENT_TIMESTAMP - INTERVAL '24 hours'
            )
        ''')
        new_24h = c.fetchone()[0] or 0
        
        if new_24h == 0:
            c.execute('''
                SELECT COUNT(DISTINCT user_id) FROM user_stats 
                WHERE last_active > CURRENT_TIMESTAMP - INTERVAL '24 hours'
                AND user_id NOT IN (
                    SELECT DISTINCT user_id FROM topics 
                    WHERE created_at <= CURRENT_TIMESTAMP - INTERVAL '24 hours'
                    UNION
                    SELECT DISTINCT user_id FROM replies 
                    WHERE created_at <= CURRENT_TIMESTAMP - INTERVAL '24 hours'
                )
            ''')
            new_24h = c.fetchone()[0] or 0
        
        c.execute("SELECT COUNT(*) FROM topics")
        total_topics = c.fetchone()[0] or 0
        
        c.execute("SELECT COUNT(*) FROM topics WHERE created_at > CURRENT_TIMESTAMP - INTERVAL '24 hours'")
        new_topics_24h = c.fetchone()[0] or 0
        
        c.execute("SELECT COUNT(*) FROM replies")
        total_replies = c.fetchone()[0] or 0
        
        c.execute("SELECT COUNT(*) FROM reports WHERE status = 'pending'")
        active_reports = c.fetchone()[0] or 0
        
        c.execute("SELECT COUNT(*) FROM reports WHERE created_at > CURRENT_TIMESTAMP - INTERVAL '24 hours'")
        reports_24h = c.fetchone()[0] or 0
        
        c.execute("SELECT COUNT(*) FROM bans WHERE banned_at > CURRENT_TIMESTAMP - INTERVAL '24 hours'")
        bans_24h = c.fetchone()[0] or 0
        
        c.close()
        conn.close()
        
        return {
            'total_users': total_users,
            'active_24h': active_24h,
            'new_24h': new_24h,
            'total_topics': total_topics,
            'new_topics_24h': new_topics_24h,
            'total_replies': total_replies,
            'active_reports': active_reports,
            'reports_24h': reports_24h,
            'bans_24h': bans_24h
        }
        
    except Exception as e:
        logger.error(f"Ошибка при получении статистики админа: {e}")
        c.close()
        conn.close()
        return {
            'total_users': 0,
            'active_24h': 0,
            'new_24h': 0,
            'total_topics': 0,
            'new_topics_24h': 0,
            'total_replies': 0,
            'active_reports': 0,
            'reports_24h': 0,
            'bans_24h': 0
        }

def update_report_status(report_id, status, admin_id, action=None):
    conn = get_db_connection()
    c = conn.cursor()
    
    try:
        c.execute('''
            UPDATE reports 
            SET status = %s, admin_action = %s, admin_id = %s, resolved_at = CURRENT_TIMESTAMP 
            WHERE id = %s
        ''', (status, action, admin_id, report_id))
        
        conn.commit()
        c.close()
        conn.close()
        return True
    except Exception as e:
        logger.error(f"Ошибка при обновлении статуса жалобы #{report_id}: {e}")
        conn.rollback()
        c.close()
        conn.close()
        return False

def cleanup_invalid_reports():
    try:
        conn = get_db_connection()
        c = conn.cursor()
        c.execute('''
            DELETE FROM reports 
            WHERE id IN (
                SELECT r.id 
                FROM reports r
                LEFT JOIN topics t ON r.topic_id = t.id
                WHERE t.id IS NULL AND r.status = 'pending'
            )
        ''')
        deleted_count = c.rowcount
        if deleted_count > 0:
            logger.info(f"Удалено {deleted_count} невалидных жалоб")
        conn.commit()
        c.close()
        conn.close()
    except Exception as e:
        logger.error(f"Ошибка при очистке жалоб: {e}")

def delete_topic_admin(topic_id, admin_id, reason):
    conn = get_db_connection()
    c = conn.cursor()
    
    try:
        c.execute('SELECT user_id, text FROM topics WHERE id = %s', (topic_id,))
        topic_info = c.fetchone()
        
        if not topic_info:
            c.close()
            conn.close()
            return False, "Тема не найдена"
        
        topic_author_id = topic_info[0]
        topic_text = topic_info[1]
        
        c.execute('DELETE FROM replies WHERE topic_id = %s', (topic_id,))
        c.execute('DELETE FROM topics WHERE id = %s', (topic_id,))
        c.execute('DELETE FROM reports WHERE topic_id = %s', (topic_id,))
        
        conn.commit()
        c.close()
        conn.close()
        
        if topic_author_id and topic_author_id != admin_id:
            send_topic_deleted_notification(topic_author_id, topic_id, reason)
        
        logger.info(f"Тема #{topic_id} удалена администратором {admin_id}. Причина: {reason}")
        
        return True, f"Тема #{topic_id} удалена"
        
    except Exception as e:
        logger.error(f"Ошибка при удалении темы #{topic_id}: {e}")
        conn.rollback()
        c.close()
        conn.close()
        return False, f"Ошибка при удалении: {str(e)}"

# ==================== ФУНКЦИИ УВЕДОМЛЕНИЙ ПОЛЬЗОВАТЕЛЯМ ====================
def send_safe_message(user_id, text):
    try:
        text = sanitize_html(text)
        bot.send_message(user_id, text, parse_mode='HTML')
        return True
    except telebot.apihelper.ApiTelegramException as e:
        if e.error_code == 403:
            logger.warning(f"Пользователь {user_id} заблокировал бота")
        else:
            logger.error(f"Ошибка при отправке сообщения: {e}")
        return False
    except Exception as e:
        logger.error(f"Неожиданная ошибка при отправке сообщения: {e}")
        return False

def send_ban_notification(user_id, reason, days, until_date):
    try:
        text = f"""🚫 <b>ВАШ АККАУНТ ОГРАНИЧЕН</b>

Ваш аккаунт временно ограничен за нарушение правил.

<b>Причина:</b>
{reason}

<b>Срок ограничения:</b> {days} день(дней)
<b>Разблокировка:</b> {until_date}"""
        
        send_safe_message(user_id, text)
        logger.info(f"Уведомление об ограничении отправлено пользователю {user_id}")
        
    except Exception as e:
        logger.error(f"Неожиданная ошибка при отправке уведомления об ограничении: {e}")

def send_topic_deleted_notification(user_id, topic_id, reason):
    try:
        text = f"""🗑️ <b>ВАША ТЕМА УДАЛЕНА</b>

<b>Тема #{topic_id} была удалена администратором.</b>

<b>Причина удаления:</b>
{reason}

⚠️ <i>При удалении темы:</i>
• Все ответы к теме также удалены
• Тема больше не отображается в архиве
• Уведомления о новых ответах прекращаются

📌 <b>Рекомендации:</b>
• Соблюдайте правила сообщества
• Не публикуйте запрещенный контент
• Уважайте другие участников

🔑 /start"""
        
        send_safe_message(user_id, text)
        logger.info(f"Уведомление об удалении темы #{topic_id} отправлено пользователю {user_id}")
        
    except Exception as e:
        logger.error(f"Неожиданная ошибка при отправке уведомления об удалении темы: {e}")

def send_reply_notification(user_id, topic_id, reply_id, reply_text):
    try:
        if is_user_banned(user_id):
            return
            
        conn = get_db_connection()
        c = conn.cursor()
        c.execute('SELECT text FROM topics WHERE id = %s', (topic_id,))
        topic = c.fetchone()
        c.close()
        conn.close()
        
        if not topic:
            return
        
        topic_text = topic[0]
        preview = topic_text[:60] + "..." if len(topic_text) > 60 else topic_text
        reply_preview = reply_text[:100] + "..." if len(reply_text) > 100 else reply_text
        
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
        
        try:
            photo_url = PHOTOS.get('notification', PHOTOS['start'])
            bot.send_photo(
                user_id,
                photo_url,
                caption=text,
                reply_markup=markup,
                parse_mode='HTML'
            )
        except:
            bot.send_message(
                user_id,
                text,
                reply_markup=markup,
                parse_mode='HTML'
            )
        
        logger.info(f"Уведомление отправлено автору темы #{topic_id} (пользователь: {user_id})")
        
    except Exception as e:
        logger.error(f"Ошибка в функции send_reply_notification: {e}")

# ==================== БОТ ====================
bot = telebot.TeleBot(BOT_TOKEN, parse_mode='HTML')
user_states = {}
user_last_messages = {}
user_viewed_topics = {}

def delete_previous_messages(chat_id, user_id):
    try:
        if user_id in user_last_messages:
            for msg_id in user_last_messages[user_id]:
                try:
                    bot.delete_message(chat_id, msg_id)
                except:
                    pass
            user_last_messages[user_id] = []
    except Exception as e:
        logger.error(f"Ошибка при удалении предыдущих сообщений: {e}")

def add_message_to_delete(user_id, message_id):
    if user_id not in user_last_messages:
        user_last_messages[user_id] = []
    user_last_messages[user_id].append(message_id)
    
    if len(user_last_messages[user_id]) > 5:
        user_last_messages[user_id] = user_last_messages[user_id][-5:]

def send_photo_message(chat_id, photo_type, text, reply_markup=None):
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
    except:
        try:
            msg = bot.send_message(chat_id, text, reply_markup=reply_markup, parse_mode='HTML')
            return msg.message_id
        except Exception as e:
            logger.error(f"Ошибка при отправке сообщения: {e}")
            return None

def send_message_with_delete(chat_id, user_id, photo_type, text, reply_markup=None):
    delete_previous_messages(chat_id, user_id)
    message_id = send_photo_message(chat_id, photo_type, text, reply_markup)
    if message_id:
        add_message_to_delete(user_id, message_id)
    return message_id

def reset_user_viewed_topics(user_id):
    if user_id in user_viewed_topics:
        user_viewed_topics[user_id] = []

def add_viewed_topic(user_id, topic_id):
    if user_id not in user_viewed_topics:
        user_viewed_topics[user_id] = []
    
    if topic_id not in user_viewed_topics[user_id]:
        user_viewed_topics[user_id].append(topic_id)

def check_all_topics_viewed(user_id, exclude_user_id=None):
    if user_id not in user_viewed_topics:
        return False
    
    viewed_count = len(user_viewed_topics[user_id])
    total_count = get_all_active_topics_count(exclude_user_id)
    
    return viewed_count >= total_count and total_count > 0

# ==================== ОБРАБОТКА СООБЩЕНИЙ В ГРУППАХ ====================
@bot.message_handler(func=lambda message: message.chat.type in ['group', 'supergroup'])
def handle_group_messages(message):
    """Обработка только команды /top в групповых чатах"""
    if message.text and message.text.strip() == '/top':
        user_id = message.from_user.id
        
        try:
            stats = get_top_statistics()
            
            text = "<b>🏆 ТОП АРХИВА МЫСЛЕЙ</b>\n\n"
            
            top_users = stats['top_users']
            medals = ["🥇", "🥈", "🥉"]
            
            if top_users and len(top_users) > 0:
                for i, user in enumerate(top_users[:3]):
                    try:
                        user_id_db = user[0]
                        username = user[1] if user[1] else f"аноним_{user_id_db % 10000:04d}"
                        topics_created = user[2] if len(user) > 2 else 0
                        replies_written = user[3] if len(user) > 3 else 0
                        
                        text += f"{medals[i]} <b>{username}</b>\n"
                        text += f"• {topics_created} тем • {replies_written} ответов\n\n"
                    except:
                        continue
            else:
                text += "📭 Пока нет активных пользователей.\n\n"
            
            text += f"<b>📊 Всего активных тем:</b> {stats['active_topics']}\n\n"
            text += "<i>Для полного функционала перейдите в личные сообщения с ботом</i>"
            
            bot.send_message(message.chat.id, text, parse_mode='HTML')
            
        except Exception as e:
            logger.error(f"Ошибка при выполнении /top в группе: {e}")
    
    # Все остальные сообщения в группах игнорируем
    return

@bot.callback_query_handler(func=lambda call: call.message.chat.type in ['group', 'supergroup'])
def ignore_group_callbacks(call):
    logger.info(f"Игнорируем колбэк в групповом чате: {call.data}")
    return

# ==================== ЛИЧНЫЕ СООБЩЕНИЯ ====================
@bot.message_handler(commands=['start'])
def start_command(message):
    """Обработка команды /start только в личных чатах"""
    if message.chat.type != 'private':
        return
    
    user_id = message.from_user.id
    chat_id = message.chat.id
    
    ban_info = check_user_ban(user_id)
    if ban_info:
        try:
            unbanned_at_str = ban_info[2]
            unbanned_at = datetime.strptime(unbanned_at_str, '%Y-%m-%d %H:%M:%S')
            now = datetime.now()
            
            if unbanned_at <= now:
                unban_user(user_id)
                try:
                    bot.delete_message(chat_id, message.message_id)
                except:
                    pass
                
                if user_id in user_states:
                    del user_states[user_id]
                
                reset_user_viewed_topics(user_id)
                show_main_menu(chat_id, user_id)
                return
            else:
                time_left = unbanned_at - now
                hours_left = int(time_left.total_seconds() // 3600)
                minutes_left = int((time_left.total_seconds() % 3600) // 60)
                
                text = f"""🚫 <b>ДОСТУП ОГРАНИЧЕН</b>

Ваш аккаунт ограничен за нарушение правил.

<b>Причина:</b> {ban_info[1]}
<b>Ограничен до:</b> {unbanned_at.strftime('%d.%m.%Y %H:%M')}
<b>Осталось:</b> {hours_left}ч {minutes_left}м

⚠️ <i>Пожалуйста, соблюдайте правила сообщества.</i>"""
                
                bot.send_message(chat_id, text, parse_mode='HTML')
                return
            
        except Exception as e:
            logger.error(f"Ошибка при обработке ограничения пользователя {user_id}: {e}")
    
    try:
        bot.delete_message(chat_id, message.message_id)
    except:
        pass
    
    if user_id in user_states:
        del user_states[user_id]
    
    reset_user_viewed_topics(user_id)
    
    conn = get_db_connection()
    c = conn.cursor()
    c.execute('''
        INSERT INTO user_stats (user_id, topics_created, replies_written, replies_received) 
        VALUES (%s, 0, 0, 0)
        ON CONFLICT (user_id) 
        DO UPDATE SET last_active = CURRENT_TIMESTAMP
    ''', (user_id,))
    conn.commit()
    c.close()
    conn.close()
    
    get_username(user_id)
    
    show_main_menu(chat_id, user_id)

def show_main_menu(chat_id, user_id):
    if is_user_banned(user_id):
        show_main_menu_for_banned_user(chat_id, user_id)
        return
    
    username = get_username(user_id)
    
    text = f"""<b>🗄️ АРХИВ МЫСЛЕЙ</b>

Привет, <b>{username}</b>! 👋

📌 <b>Основные функции:</b>
• Создавайте анонимные темы (макс. {DAILY_TOPIC_LIMIT}/день)
• Отвечайте на чужие мысли
• Читайте популярные обсуждения
• Управляйте своими темами

🔔 <b>Уведомления:</b>
• Получайте оповещения при ответах на ваши темы
• Анонимные ответы других пользователей

🔒 <b>Управление темы:</b>
• Закрытие своих тем
• Удаление своих тем вместе с ответами

<i>Без имён. Без осуждения. Только мысли.</i>"""
    
    markup = telebot.types.InlineKeyboardMarkup()
    
    markup.add(
        telebot.types.InlineKeyboardButton("👤 МОЙ ПРОФИЛЬ", callback_data="my_profile")
    )
    
    markup.add(
        telebot.types.InlineKeyboardButton("➕ НОВАЯ ТЕМА", callback_data="new_topic"),
        telebot.types.InlineKeyboardButton("🎲 СЛУЧАЙНАЯ", callback_data="random_topic")
    )
    
    markup.add(
        telebot.types.InlineKeyboardButton("📁 МОИ ТЕМЫ", callback_data="my_topics_1"),
        telebot.types.InlineKeyboardButton("🔥 ПОПУЛЯРНЫЕ", callback_data="popular_1")
    )
    
    if ADMIN_ID and user_id == ADMIN_ID:
        markup.add(
            telebot.types.InlineKeyboardButton("⚙️ АДМИН-ПАНЕЛЬ", callback_data="admin_panel")
        )
    
    send_message_with_delete(chat_id, user_id, 'start', text, markup)

def show_main_menu_for_banned_user(chat_id, user_id):
    username = get_username(user_id)
    
    text = f"""<b>🚫 РЕЖИМ ТОЛЬКО ПРОСМОТР</b>

Привет, <b>{username}</b>!

Ваш аккаунт временно ограничен.
Вы можете только просматривать темы.

<b>Доступные функции:</b>
• Просмотр случайных тем
• Чтение популярных обсуждений
• Просмотр своих тем

<b>Недоступно:</b>
• Создание новых тем
• Ответы на темы
• Получение уведомлений

<i>Дождитесь окончания срока ограничения</i>"""
    
    markup = telebot.types.InlineKeyboardMarkup()
    
    markup.add(
        telebot.types.InlineKeyboardButton("👤 МОЙ ПРОФИЛЬ", callback_data="my_profile")
    )
    
    markup.add(
        telebot.types.InlineKeyboardButton("🎲 СЛУЧАЙНАЯ", callback_data="random_topic"),
        telebot.types.InlineKeyboardButton("🔥 ПОПУЛЯРНЫЕ", callback_data="popular_1")
    )
    
    markup.add(
        telebot.types.InlineKeyboardButton("📁 МОИ ТЕМЫ", callback_data="my_topics_1")
    )
    
    send_message_with_delete(chat_id, user_id, 'start', text, markup)

# ==================== КОМАНДА /TOP В ЛИЧНЫХ ЧАТАХ ====================
@bot.message_handler(commands=['top'])
def top_command(message):
    """Обработка команды /top в личных и групповых чатах"""
    user_id = message.from_user.id
    chat_id = message.chat.id
    
    stats = get_top_statistics()
    
    text = """<b>🏆 ТОП АРХИВА</b>

<b>Лучшие участники сообщества:</b>
"""
    
    top_users = stats['top_users']
    medals = ["🥇", "🥈", "🥉"]
    
    if top_users and len(top_users) > 0:
        for i, user in enumerate(top_users[:3]):
            try:
                user_id_db = user[0]
                username = user[1] if user[1] else f"аноним_{user_id_db % 10000:04d}"
                topics_created = user[2] if len(user) > 2 else 0
                replies_written = user[3] if len(user) > 3 else 0
                
                user_stats = {
                    'topics_created': topics_created,
                    'replies_written': replies_written,
                    'replies_received': 0
                }
                rank_id = get_user_rank_by_stats(user_stats)
                rank_name = RANK_SYSTEM[rank_id]['name']
                
                text += f"\n{medals[i]} <b>{username}</b>"
                text += f"\n• {topics_created} тем • {replies_written} ответов"
                text += f"\n🏅 Ранг: {rank_name}\n"
            except Exception as e:
                logger.error(f"Ошибка при форматировании пользователя {user}: {e}")
                continue
    else:
        text += "\n\n📭 Пока нет активных пользователей."
    
    text += f"\n<b>📊 Всего активных тем:</b> {stats['active_topics']}"
    
    weekly_record = stats['weekly_record']
    if weekly_record and len(weekly_record) >= 4:
        topic_id = weekly_record[0]
        replies_count = weekly_record[2]
        author_name = weekly_record[3] if weekly_record[3] else "Аноним"
        text += f"\n<b>🔥 Рекорд недели:</b> {replies_count} ответов на тему #{topic_id} ({author_name})"
    
    replies_leader = stats['replies_leader']
    if replies_leader and len(replies_leader) >= 3:
        leader_name = replies_leader[1] if replies_leader[1] else f"аноним_{replies_leader[0] % 10000:04d}"
        leader_replies = replies_leader[2]
        text += f"\n<b>👤 Рекорд по ответам:</b> {leader_name} ({leader_replies} ответов)"
    
    # В групповом чате
    if message.chat.type in ['group', 'supergroup']:
        try:
            bot.delete_message(chat_id, message.message_id)
        except:
            pass
        bot.send_message(chat_id, text, parse_mode='HTML')
    else:
        # В личном чате
        try:
            bot.delete_message(chat_id, message.message_id)
        except:
            pass
        send_message_with_delete(chat_id, user_id, 'top', text)

# ==================== ЛИЧНЫЙ КАБИНЕТ ====================
@bot.callback_query_handler(func=lambda call: call.data == "my_profile")
def my_profile_callback(call):
    if call.message.chat.type != 'private':
        bot.answer_callback_query(call.id, "Эта функция доступна только в личных сообщениях", show_alert=True)
        return
    
    user_id = call.from_user.id
    chat_id = call.message.chat.id
    
    is_banned = is_user_banned(user_id)
    stats = get_user_statistics(user_id)
    rank_id = get_user_rank(user_id)
    rank_info = RANK_SYSTEM[rank_id]
    username = get_username(user_id)
    
    progress_info = get_rank_progress(user_id)
    progress_bar = get_progress_bar(progress_info['progress'])
    
    notifications_enabled = get_user_notification_setting(user_id)
    notification_status = "🔔 ВКЛ" if notifications_enabled else "🔕 ВЫКЛ"
    
    if is_banned:
        ban_info = check_user_ban(user_id)
        if ban_info:
            try:
                unbanned_at_str = ban_info[2]
                unbanned_at = datetime.strptime(unbanned_at_str, '%Y-%m-%d %H:%M:%S')
                time_left = unbanned_at - datetime.now()
                hours_left = int(time_left.total_seconds() // 3600)
                minutes_left = int((time_left.total_seconds() % 3600) // 60)
                status_text = f"🚫 <b>ОГРАНИЧЕН</b> (осталось: {hours_left}ч {minutes_left}м)"
            except:
                status_text = "🚫 <b>ОГРАНИЧЕН</b>"
        else:
            status_text = "🟢 <b>АКТИВЕН</b>"
    else:
        status_text = "🟢 <b>АКТИВЕН</b>"
    
    remaining, topics_today = check_daily_topic_limit(user_id)
    
    text = f"""<b>👤 МОЙ ПРОФИЛЬ</b>

<b>📛 ИМЯ:</b> {username}
<b>🏅 РАНГ:</b> {rank_info['name']}
<b>📈 СТАТУС:</b> {status_text}
<b>🔔 УВЕДОМЛЕНИЯ:</b> {notification_status}

<b>📊 СТАТИСТИКА:</b>
• Тем создано: {stats['topics_created']}
• Ответов написано: {stats['replies_written']}
• Ответов получено: {stats['replies_received']}

<b>📅 ДНЕВНОЙ ЛИМИТ:</b>
• Создано сегодня: {topics_today}/{DAILY_TOPIC_LIMIT} тем"""

    if progress_info['next_rank']:
        next_rank_info = RANK_SYSTEM[progress_info['next_rank']]
        text += f"\n\n<b>📈 ПРОГРЕСС ДО {next_rank_info['name']}:</b>"
        text += f"\n{progress_bar} {progress_info['progress']}%"
        
        rem = progress_info['remaining']
        if rem['topics'] > 0 or rem['replies'] > 0:
            text += "\n<b>Осталось:</b>"
            if rem['topics'] > 0:
                text += f"\n• {rem['topics']} тем"
            if rem['replies'] > 0:
                text += f"\n• {rem['replies']} ответов"
    
    text += "\n\n<i>Статистика обновляется в реальном времени</i>"
    
    markup = telebot.types.InlineKeyboardMarkup(row_width=1)
    
    notifications_btn_text = "🔕 ВЫКЛ УВЕДОМЛЕНИЯ" if notifications_enabled else "🔔 ВКЛ УВЕДОМЛЕНИЯ"
    markup.add(
        telebot.types.InlineKeyboardButton(notifications_btn_text, callback_data="toggle_notifications"),
        telebot.types.InlineKeyboardButton("✏️ ИЗМЕНИТЬ ИМЯ", callback_data="change_username")
    )
    
    if is_banned:
        markup.add(telebot.types.InlineKeyboardButton("🔙 НАЗАД", callback_data="menu_banned"))
    else:
        markup.add(telebot.types.InlineKeyboardButton("🔙 НАЗАД", callback_data="menu"))
    
    try:
        bot.delete_message(chat_id, call.message.message_id)
    except:
        pass
    
    send_message_with_delete(chat_id, user_id, 'profile', text, markup)
    bot.answer_callback_query(call.id)

@bot.callback_query_handler(func=lambda call: call.data == "toggle_notifications")
def toggle_notifications_callback(call):
    if call.message.chat.type != 'private':
        bot.answer_callback_query(call.id, "Эта функция доступна только в личных сообщениях", show_alert=True)
        return
    
    user_id = call.from_user.id
    chat_id = call.message.chat.id
    
    new_setting = toggle_user_notifications(user_id)
    status = "включены" if new_setting else "выключены"
    
    text = f"""✅ <b>НАСТРОЙКИ ИЗМЕНЕНЫ</b>

Уведомления о новых ответах на ваши темы теперь <b>{status}</b>.

<b>Что это значит:</b>
• {"Вы будете получать уведомления при каждом новом ответе" if new_setting else "Вы НЕ будете получать уведомления о новых ответах"}
• Настройка применяется моментально
• Вы можете изменить её в любой момент

<i>Уведомления помогают не пропустить интересные обсуждения!</i>"""
    
    markup = telebot.types.InlineKeyboardMarkup()
    markup.add(
        telebot.types.InlineKeyboardButton("👤 В ПРОФИЛЬ", callback_data="my_profile")
    )
    
    try:
        bot.delete_message(chat_id, call.message.message_id)
    except:
        pass
    
    send_message_with_delete(chat_id, user_id, 'profile', text, markup)
    bot.answer_callback_query(call.id)

@bot.callback_query_handler(func=lambda call: call.data == "change_username")
def change_username_callback(call):
    if call.message.chat.type != 'private':
        bot.answer_callback_query(call.id, "Эта функция доступна только в личных сообщениях", show_alert=True)
        return
    
    user_id = call.from_user.id
    chat_id = call.message.chat.id
    
    current_username = get_username(user_id)
    
    text = f"""<b>✏️ ИЗМЕНЕНИЕ ИМЕНИ</b>

<b>Текущее имя:</b> {current_username}

<b>Правила выбора имени:</b>
• От 3 до 12 символов
• Можно использовать: буквы (русские/английские), цифры, нижнее подчёркивание (_)
• Нельзя использовать: пробелы, специальные символы
• Имя должно быть уникальным

<b>Примеры допустимых имён:</b>
• user_123
• Иван_2024
• Best_Writer
• мыслитель

<i>Введите новое имя:</i>"""
    
    markup = telebot.types.InlineKeyboardMarkup()
    markup.add(
        telebot.types.InlineKeyboardButton("🔙 НАЗАД В ПРОФИЛЬ", callback_data="my_profile")
    )
    
    try:
        bot.delete_message(chat_id, call.message.message_id)
    except:
        pass
    
    send_message_with_delete(chat_id, user_id, 'profile', text, markup)
    bot.answer_callback_query(call.id)
    
    user_states[user_id] = {'state': 'change_username'}

# ==================== ОБРАБОТКА ИМЕНИ ПОЛЬЗОВАТЕЛЯ ====================
@bot.message_handler(func=lambda message: message.from_user.id in user_states and user_states[message.from_user.id]['state'] == 'change_username')
def handle_username_input(message):
    if message.chat.type != 'private':
        return
    
    user_id = message.from_user.id
    chat_id = message.chat.id
    new_username = message.text.strip()
    
    try:
        bot.delete_message(chat_id, message.message_id)
    except:
        pass
    
    is_valid, error_message = validate_username(new_username)
    
    if not is_valid:
        text = f"""❌ <b>НЕВЕРНЫЙ ФОРМАТ ИМЕНИ</b>

{error_message}

<b>Требования:</b>
• От 3 до 12 символов
• Только буквы (русские/английские), цифры и нижнее подчеркивание
• Без пробелов и специальных символов

<b>Пример:</b> user_123, Иван_2024, мыслитель

<i>Попробуйте снова:</i>"""
        
        markup = telebot.types.InlineKeyboardMarkup()
        markup.add(
            telebot.types.InlineKeyboardButton("❌ ОТМЕНА", callback_data="my_profile")
        )
        
        send_message_with_delete(chat_id, user_id, 'profile', text, markup)
        return
    
    success, result_message = set_username(user_id, new_username)
    
    if success:
        text = f"""✅ <b>ИМЯ УСПЕШНО ИЗМЕНЕНО!</b>

Теперь вас будут знать как:
<b>{new_username}</b>

📝 <b>Ваше имя будет отображаться:</b>
• В вашем профиле
• В топе участников (/top)
• В статистике админа

🌟 <i>Теперь вы - полноправный участник Архива Мыслей!</i>"""
    else:
        text = f"""❌ <b>ОШИБКА ПРИ ИЗМЕНЕНИИ ИМЕНИ</b>

{result_message}

<i>Попробуйте другое имя:</i>"""
    
    markup = telebot.types.InlineKeyboardMarkup()
    markup.add(
        telebot.types.InlineKeyboardButton("👤 В ПРОФИЛЬ", callback_data="my_profile")
    )
    
    if user_id in user_states:
        del user_states[user_id]
    
    send_message_with_delete(chat_id, user_id, 'profile', text, markup)

# ==================== АДМИН-ПАНЕЛЬ ====================
@bot.callback_query_handler(func=lambda call: call.data == "admin_panel")
def admin_panel_callback(call):
    if call.message.chat.type != 'private':
        bot.answer_callback_query(call.id, "Эта функция доступна только в личных сообщениях", show_alert=True)
        return
    
    user_id = call.from_user.id
    chat_id = call.message.chat.id
    
    if not ADMIN_ID or user_id != ADMIN_ID:
        bot.answer_callback_query(call.id, "❌ Доступ запрещен", show_alert=True)
        return
    
    stats = get_admin_statistics()
    
    text = f"""<b>⚙️ АДМИН-ПАНЕЛЬ</b>

<b>📊 ОБЩАЯ СТАТИСТИКА:</b>
• Всего пользователей: {stats['total_users']:,}
• Новых за 24ч: {stats['new_24h']}
• Активных за 24ч: {stats['active_24h']}

<b>📝 КОНТЕНТ:</b>
• Всего тем: {stats['total_topics']:,}
• Новых тем за 24ч: {stats['new_topics_24h']}
• Всего ответов: {stats['total_replies']:,}

<b>⚠️ МОДЕРАЦИЯ:</b>
• Активных жалоб: {stats['active_reports']}
• Всего за 24ч: {stats['reports_24h']}
• Ограничений за 24ч: {stats['bans_24h']}"""
    
    markup = telebot.types.InlineKeyboardMarkup(row_width=1)
    
    if stats['active_reports'] > 0:
        markup.add(telebot.types.InlineKeyboardButton(f"📋 ЖАЛОБЫ ({stats['active_reports']})", callback_data="admin_reports_1"))
    
    markup.add(telebot.types.InlineKeyboardButton("🔙 НАЗАД", callback_data="menu"))
    
    try:
        bot.delete_message(chat_id, call.message.message_id)
    except:
        pass
    
    send_message_with_delete(chat_id, user_id, 'admin', text, markup)
    bot.answer_callback_query(call.id)

@bot.callback_query_handler(func=lambda call: call.data.startswith("admin_reports_"))
def admin_reports_callback(call):
    if call.message.chat.type != 'private':
        bot.answer_callback_query(call.id, "Эта функция доступна только в личных сообщениях", show_alert=True)
        return
    
    user_id = call.from_user.id
    chat_id = call.message.chat.id
    
    if not ADMIN_ID or user_id != ADMIN_ID:
        bot.answer_callback_query(call.id, "❌ Доступ запрещен", show_alert=True)
        return
    
    try:
        page = int(call.data.split("_")[2])
        per_page = 5
        offset = (page - 1) * per_page
        
        reports = get_pending_reports(limit=per_page, offset=offset)
        
        conn = get_db_connection()
        c = conn.cursor()
        c.execute('SELECT COUNT(*) FROM reports WHERE status = %s', ('pending',))
        total_reports = c.fetchone()[0] or 0
        c.close()
        conn.close()
        
        if not reports and page == 1:
            text = """<b>📋 ЖАЛОБЫ</b>

На данный момент нет активных жалоб.
Все жалобы обработаны!"""
            
            markup = telebot.types.InlineKeyboardMarkup()
            markup.add(
                telebot.types.InlineKeyboardButton("🔙 В АДМИН-ПАНЕЛЬ", callback_data="admin_panel")
            )
            
            try:
                bot.delete_message(chat_id, call.message.message_id)
            except:
                pass
            
            send_message_with_delete(chat_id, user_id, 'report', text, markup)
            bot.answer_callback_query(call.id)
            return
        
        total_pages = max(1, (total_reports + per_page - 1) // per_page)
        
        text = f"""<b>📋 ЖАЛОБЫ</b>

От старых к новым
Страница {page} из {total_pages}

<b>Список:</b>"""
        
        for i, report in enumerate(reports, 1):
            try:
                report_id = report[0]
                topic_id = report[1]
                reason = report[3]
                created_at = report[6]
                
                if created_at:
                    try:
                        report_time = datetime.strptime(created_at, '%Y-%m-%d %H:%M:%S')
                        time_ago = format_timedelta(datetime.now() - report_time)
                    except:
                        time_ago = "неизвестно"
                else:
                    time_ago = "неизвестно"
                
                text += f"\n\n{offset + i}. <b>#{report_id}</b> — {time_ago}"
                text += f"\nТема: #{topic_id} • Причина: {reason}"
                
            except Exception as e:
                logger.error(f"Ошибка форматирования жалобы {report}: {e}")
                text += f"\n\n{offset + i}. <b>#{report[0] if report else '?'}</b> — ошибка данных"
        
        markup = telebot.types.InlineKeyboardMarkup(row_width=3)
        
        for report in reports:
            if report and len(report) > 0:
                report_id = report[0]
                markup.add(
                    telebot.types.InlineKeyboardButton(f"🔍 #{report_id}", callback_data=f"view_report_{report_id}"),
                    telebot.types.InlineKeyboardButton(f"❌ #{report_id}", callback_data=f"reject_report_{report_id}"),
                    telebot.types.InlineKeyboardButton(f"✅ #{report_id}", callback_data=f"resolve_report_{report_id}")
                )
        
        pagination_buttons = []
        
        if page > 1:
            pagination_buttons.append(
                telebot.types.InlineKeyboardButton("◀️", callback_data=f"admin_reports_{page-1}")
            )
        
        pagination_buttons.append(
            telebot.types.InlineKeyboardButton(f"{page}/{total_pages}", callback_data=f"admin_reports_{page}")
        )
        
        if page < total_pages:
            pagination_buttons.append(
                telebot.types.InlineKeyboardButton("▶️", callback_data=f"admin_reports_{page+1}")
            )
        
        if pagination_buttons:
            markup.add(*pagination_buttons)
        
        markup.add(
            telebot.types.InlineKeyboardButton("🔙 В АДМИН-ПАНЕЛЬ", callback_data="admin_panel")
        )
        
        try:
            bot.delete_message(chat_id, call.message.message_id)
        except:
            pass
        
        send_message_with_delete(chat_id, user_id, 'report', text, markup)
        bot.answer_callback_query(call.id)
        
    except Exception as e:
        logger.error(f"Ошибка в admin_reports_callback: {e}", exc_info=True)
        bot.answer_callback_query(call.id, "❌ Ошибка", show_alert=True)

@bot.callback_query_handler(func=lambda call: call.data.startswith("view_report_"))
def view_report_callback(call):
    if call.message.chat.type != 'private':
        bot.answer_callback_query(call.id, "Эта функция доступна только в личных сообщениях", show_alert=True)
        return
    
    user_id = call.from_user.id
    
    if not ADMIN_ID or user_id != ADMIN_ID:
        bot.answer_callback_query(call.id, "❌ Доступ запрещен", show_alert=True)
        return
    
    try:
        report_id = int(call.data.split("_")[2])
        report = get_report(report_id)
        
        if not report:
            bot.answer_callback_query(call.id, "❌ Жалоба не найдена", show_alert=True)
            return
        
        chat_id = call.message.chat.id
        
        report_id = report[0]
        topic_id = report[1]
        reporter_id = report[2]
        reason = report[3]
        status = report[4]
        created_at = report[6]
        topic_text = report[8] if len(report) > 8 else "Не найдено"
        topic_author_id = report[9] if len(report) > 9 else None
        
        topic_preview = topic_text[:200] + "..." if len(topic_text) > 200 else topic_text
        
        text = f"""<b>🔍 ПРОСМОТР ЖАЛОБЫ #{report_id}</b>

<b>Тема:</b> #{topic_id}
<b>Жалобщик:</b> {reporter_id}
<b>Причина:</b> {reason}
<b>Статус:</b> {status}
<b>Дата:</b> {format_datetime(created_at) if created_at else "Неизвестно"}

<b>Текст темы:</b>
{html.escape(topic_preview)}

<b>Действия:</b>"""
        
        markup = telebot.types.InlineKeyboardMarkup(row_width=2)
        markup.add(
            telebot.types.InlineKeyboardButton("✅ ПРИНЯТЬ", callback_data=f"resolve_report_{report_id}"),
            telebot.types.InlineKeyboardButton("❌ ОТКЛОНИТЬ", callback_data=f"reject_report_{report_id}"),
            telebot.types.InlineKeyboardButton("🚫 ЗАБАНИТЬ АВТОРА", callback_data=f"ban_author_{topic_author_id}_{report_id}" if topic_author_id else "ban_author_none"),
            telebot.types.InlineKeyboardButton("🗑️ УДАЛИТЬ ТЕМУ", callback_data=f"delete_topic_admin_{topic_id}_{report_id}"),
            telebot.types.InlineKeyboardButton("🔙 К СПИСКУ", callback_data="admin_reports_1")
        )
        
        try:
            bot.delete_message(chat_id, call.message.message_id)
        except:
            pass
        
        send_message_with_delete(chat_id, user_id, 'report', text, markup)
        bot.answer_callback_query(call.id)
        
    except Exception as e:
        logger.error(f"Ошибка в view_report_callback: {e}")
        bot.answer_callback_query(call.id, "❌ Ошибка", show_alert=True)

@bot.callback_query_handler(func=lambda call: call.data.startswith("resolve_report_"))
def resolve_report_callback(call):
    if call.message.chat.type != 'private':
        bot.answer_callback_query(call.id, "Эта функция доступна только в личных сообщениях", show_alert=True)
        return
    
    user_id = call.from_user.id
    
    if not ADMIN_ID or user_id != ADMIN_ID:
        bot.answer_callback_query(call.id, "❌ Доступ запрещен", show_alert=True)
        return
    
    try:
        report_id = int(call.data.split("_")[2])
        success = update_report_status(report_id, 'resolved', user_id, 'Жалоба принята')
        
        if success:
            text = f"""✅ <b>ЖАЛОБА #{report_id} ПРИНЯТА</b>

Жалоба отмечена как принятая.
Модератор рассмотрел её и принял соответствующие меры.

<b>Статус:</b> ✅ Решена
<b>Время:</b> {datetime.now().strftime('%d.%m.%Y %H:%M')}

<i>Спасибо за вашу работу!</i>"""
        else:
            text = "❌ <b>ОШИБКА</b>\n\nНе удалось обновить статус жалобы."
        
        markup = telebot.types.InlineKeyboardMarkup()
        markup.add(
            telebot.types.InlineKeyboardButton("🔙 К СПИСКУ ЖАЛОБ", callback_data="admin_reports_1")
        )
        
        try:
            bot.delete_message(call.message.chat.id, call.message.message_id)
        except:
            pass
        
        send_message_with_delete(call.message.chat.id, user_id, 'report', text, markup)
        bot.answer_callback_query(call.id)
        
    except Exception as e:
        logger.error(f"Ошибка в resolve_report_callback: {e}")
        bot.answer_callback_query(call.id, "❌ Ошибка", show_alert=True)

@bot.callback_query_handler(func=lambda call: call.data.startswith("reject_report_"))
def reject_report_callback(call):
    if call.message.chat.type != 'private':
        bot.answer_callback_query(call.id, "Эта функция доступна только в личных сообщениях", show_alert=True)
        return
    
    user_id = call.from_user.id
    
    if not ADMIN_ID or user_id != ADMIN_ID:
        bot.answer_callback_query(call.id, "❌ Доступ запрещен", show_alert=True)
        return
    
    try:
        report_id = int(call.data.split("_")[2])
        success = update_report_status(report_id, 'rejected', user_id, 'Жалоба отклонена')
        
        if success:
            text = f"""❌ <b>ЖАЛОБА #{report_id} ОТКЛОНЕНА</b>

Жалоба отклонена как необоснованная.
Тема остаётся в архиве без изменений.

<b>Статус:</b> ❌ Отклонена
<b>Время:</b> {datetime.now().strftime('%d.%m.%Y %H:%M')}

<i>Ложные жалобы могут привести к ограничению аккаунта жалобщика.</i>"""
        else:
            text = "❌ <b>ОШИБКА</b>\n\nНе удалось обновить статус жалобы."
        
        markup = telebot.types.InlineKeyboardMarkup()
        markup.add(
            telebot.types.InlineKeyboardButton("🔙 К СПИСКУ ЖАЛОБ", callback_data="admin_reports_1")
        )
        
        try:
            bot.delete_message(call.message.chat.id, call.message.message_id)
        except:
            pass
        
        send_message_with_delete(call.message.chat.id, user_id, 'report', text, markup)
        bot.answer_callback_query(call.id)
        
    except Exception as e:
        logger.error(f"Ошибка в reject_report_callback: {e}")
        bot.answer_callback_query(call.id, "❌ Ошибка", show_alert=True)

@bot.callback_query_handler(func=lambda call: call.data.startswith("ban_author_"))
def ban_author_callback(call):
    if call.message.chat.type != 'private':
        bot.answer_callback_query(call.id, "Эта функция доступна только в личных сообщениях", show_alert=True)
        return
    
    user_id = call.from_user.id
    
    if not ADMIN_ID or user_id != ADMIN_ID:
        bot.answer_callback_query(call.id, "❌ Доступ запрещен", show_alert=True)
        return
    
    try:
        parts = call.data.split("_")
        author_id = int(parts[2])
        report_id = int(parts[3]) if len(parts) > 3 else None
        
        if author_id == 0 or author_id == 'none':
            bot.answer_callback_query(call.id, "❌ Не удалось определить автора темы", show_alert=True)
            return
        
        user_states[user_id] = {
            'state': 'ban_user',
            'user_id_to_ban': author_id,
            'report_id': report_id
        }
        
        text = f"""<b>🚫 БАН ПОЛЬЗОВАТЕЛЯ {author_id}</b>

Вы собираетесь забанить пользователя.

<b>Введите причину бана:</b>
• Максимум 200 символов
• Будет показана пользователю
• Сохранится в истории

<b>Или введите количество дней:</b>
• 1-30 дней (например: 7)
• По умолчанию: 1 день

<i>Для отмены нажмите кнопку ниже</i>"""
        
        markup = telebot.types.InlineKeyboardMarkup()
        markup.add(
            telebot.types.InlineKeyboardButton("❌ ОТМЕНА", callback_data=f"view_report_{report_id}" if report_id else "admin_reports_1")
        )
        
        try:
            bot.delete_message(call.message.chat.id, call.message.message_id)
        except:
            pass
        
        send_message_with_delete(call.message.chat.id, user_id, 'admin', text, markup)
        bot.answer_callback_query(call.id)
        
    except Exception as e:
        logger.error(f"Ошибка в ban_author_callback: {e}")
        bot.answer_callback_query(call.id, "❌ Ошибка", show_alert=True)

@bot.callback_query_handler(func=lambda call: call.data.startswith("delete_topic_admin_"))
def delete_topic_admin_callback(call):
    if call.message.chat.type != 'private':
        bot.answer_callback_query(call.id, "Эта функция доступна только в личных сообщениях", show_alert=True)
        return
    
    user_id = call.from_user.id
    
    if not ADMIN_ID or user_id != ADMIN_ID:
        bot.answer_callback_query(call.id, "❌ Доступ запрещен", show_alert=True)
        return
    
    try:
        parts = call.data.split("_")
        topic_id = int(parts[3])
        report_id = int(parts[4]) if len(parts) > 4 else None
        
        user_states[user_id] = {
            'state': 'delete_topic_admin',
            'topic_id': topic_id,
            'report_id': report_id
        }
        
        text = f"""<b>🗑️ УДАЛЕНИЕ ТЕМЫ #{topic_id}</b>

Вы собираетесь удалить тему администраторскими правами.

<b>Введите причину удаления:</b>
• Будет отправлена автору темы
• Сохранится в истории действий
• Максимум 200 символов

<i>Тема и все ответы будут безвозвратно удалены!</i>"""
        
        markup = telebot.types.InlineKeyboardMarkup()
        markup.add(
            telebot.types.InlineKeyboardButton("❌ ОТМЕНА", callback_data=f"view_report_{report_id}" if report_id else "admin_reports_1")
        )
        
        try:
            bot.delete_message(call.message.chat.id, call.message.message_id)
        except:
            pass
        
        send_message_with_delete(call.message.chat.id, user_id, 'admin', text, markup)
        bot.answer_callback_query(call.id)
        
    except Exception as e:
        logger.error(f"Ошибка в delete_topic_admin_callback: {e}")
        bot.answer_callback_query(call.id, "❌ Ошибка", show_alert=True)

# ==================== НОВАЯ ТЕМА ====================
@bot.callback_query_handler(func=lambda call: call.data == "new_topic")
def new_topic_callback(call):
    if call.message.chat.type != 'private':
        bot.answer_callback_query(call.id, "Эта функция доступна только в личных сообщениях", show_alert=True)
        return
    
    user_id = call.from_user.id
    chat_id = call.message.chat.id
    
    if is_user_banned(user_id):
        bot.answer_callback_query(call.id, "🚫 Вы не можете создавать темы во время ограничения", show_alert=True)
        return
    
    remaining, topics_today = check_daily_topic_limit(user_id)
    
    if remaining <= 0:
        text = f"""🚫 <b>ДНЕВНОЙ ЛИМИТ ИСЧЕРПАН</b>

Вы создали максимальное количество тем на сегодня.

<b>Статистика:</b>
• Создано сегодня: {topics_today}/{DAILY_TOPIC_LIMIT} тем
• Доступно снова: завтра

<b>Что можно делать:</b>
• Отвечать на чужие темы
• Просматривать архив
• Управлять своими темами

📅 <i>Лимит обновляется каждый день в 00:00</i>"""
        
        markup = telebot.types.InlineKeyboardMarkup()
        markup.add(
            telebot.types.InlineKeyboardButton("🎲 СЛУЧАЙНАЯ ТЕМА", callback_data="random_topic"),
            telebot.types.InlineKeyboardButton("🔙 В МЕНЮ", callback_data="menu")
        )
        
        try:
            bot.delete_message(chat_id, call.message.message_id)
        except:
            pass
        
        send_message_with_delete(chat_id, user_id, 'limit', text, markup)
        bot.answer_callback_query(call.id)
        return
    
    user_states[user_id] = {'state': 'new_topic'}
    
    text = f"""<b>✍️ СОЗДАНИЕ НОВОЙ ТЕМЫ</b>

Напишите свою мысль, вопрос или идею.

<b>Требования:</b>
• От 2 до 2000 символов
• Сохраняется анонимно
• Без личных данных

<b>📊 ДНЕВНОЙ ЛИМИТ:</b>
• Создано сегодня: {topics_today}/{DAILY_TOPIC_LIMIT} тем

🔔 <b>Вы получите уведомление</b>, когда кто-то ответит на вашу тему."""
    
    markup = telebot.types.InlineKeyboardMarkup()
    markup.add(telebot.types.InlineKeyboardButton("🔙 В МЕНЮ", callback_data="menu"))
    
    try:
        bot.delete_message(chat_id, call.message.message_id)
    except:
        pass
    
    send_message_with_delete(chat_id, user_id, 'new_topic', text, markup)
    bot.answer_callback_query(call.id)

# ==================== СЛУЧАЙНАЯ ТЕМА ====================
@bot.callback_query_handler(func=lambda call: call.data == "random_topic")
def random_topic_callback(call):
    if call.message.chat.type != 'private':
        bot.answer_callback_query(call.id, "Эта функция доступна только в личных сообщениях", show_alert=True)
        return
    
    user_id = call.from_user.id
    chat_id = call.message.chat.id
    
    if check_all_topics_viewed(user_id, user_id):
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
        
        try:
            bot.delete_message(chat_id, call.message.message_id)
        except:
            pass
        
        send_message_with_delete(chat_id, user_id, 'random', text, markup)
        bot.answer_callback_query(call.id)
        return
    
    viewed_list = user_viewed_topics.get(user_id, [])
    topic = get_random_topic(exclude_user_id=user_id, viewed_topics=viewed_list)
    
    if not topic:
        reset_user_viewed_topics(user_id)
        topic = get_random_topic(exclude_user_id=user_id)
        
        if not topic:
            text = """<b>📭 АРХИВ ПУСТ</b>

Пока нет ни одной темы.
Создайте первую и начните обсуждение!"""
            
            markup = telebot.types.InlineKeyboardMarkup()
            markup.add(
                telebot.types.InlineKeyboardButton("➕ СОЗДАТЬ ТЕМУ", callback_data="new_topic"),
                telebot.types.InlineKeyboardButton("🔙 В МЕНУ", callback_data="menu")
            )
            
            try:
                bot.delete_message(chat_id, call.message.message_id)
            except:
                pass
            
            send_message_with_delete(chat_id, user_id, 'start', text, markup)
            bot.answer_callback_query(call.id)
            return
        
        text = """🔄 <b>НОВЫЙ ЦИКЛ ПРОСМОТРА</b>

Вы начали новый цикл просмотра тем.
Предыдущие темы снова доступны.

<b>Статистика предыдущего цикла:</b>
• Просмотрено тем: {} • Начата новая сессия""".format(len(viewed_list))
        
        markup = telebot.types.InlineKeyboardMarkup(row_width=1)
        markup.add(
            telebot.types.InlineKeyboardButton("➡️ ПРОДОЛЖИТЬ", callback_data="random_topic"),
            telebot.types.InlineKeyboardButton("🔙 В МЕНЮ", callback_data="menu")
        )
        
        try:
            bot.delete_message(chat_id, call.message.message_id)
        except:
            pass
        
        send_message_with_delete(chat_id, user_id, 'random', text, markup)
        bot.answer_callback_query(call.id)
        return
    
    topic_id, topic_text, _, is_active, created_at, _ = topic
    replies_count = get_replies_count(topic_id)
    
    add_viewed_topic(user_id, topic_id)
    
    total_topics = get_all_active_topics_count(user_id)
    viewed_count = len(user_viewed_topics.get(user_id, []))
    
    text = f"""<b>🎲 СЛУЧАЙНАЯ ТЕМА #{topic_id}</b>

{html.escape(topic_text)}

<b>📊 Информация:</b>
• Ответов: {replies_count}
• Создана: {format_datetime(created_at)}
• Статус: {"🟢 Активна" if is_active else "🔴 Закрыта"}

<b>📈 Ваш прогресс:</b>
• Просмотрено: {viewed_count}/{total_topics} тем"""
    
    markup = telebot.types.InlineKeyboardMarkup(row_width=2)
    
    if is_active and not is_user_banned(user_id):
        markup.add(telebot.types.InlineKeyboardButton("💬 ОТВЕТИТЬ", callback_data=f"reply_topic_{topic_id}"))
    
    markup.add(
        telebot.types.InlineKeyboardButton("📄 ПОДРОБНЕЕ", callback_data=f"view_topic_{topic_id}_1"),
        telebot.types.InlineKeyboardButton("🎲 СЛЕДУЮЩАЯ", callback_data="random_topic"),
        telebot.types.InlineKeyboardButton("⚠️ ЖАЛОБА", callback_data=f"report_topic_{topic_id}"),
        telebot.types.InlineKeyboardButton("🔙 В МЕНЮ", callback_data="menu")
    )
    
    try:
        bot.delete_message(chat_id, call.message.message_id)
    except:
        pass
    
    send_message_with_delete(chat_id, user_id, 'random', text, markup)
    bot.answer_callback_query(call.id)

# ==================== МОИ ТЕМЫ ====================
@bot.callback_query_handler(func=lambda call: call.data.startswith("my_topics_"))
def my_topics_callback(call):
    if call.message.chat.type != 'private':
        bot.answer_callback_query(call.id, "Эта функция доступна только в личных сообщениях", show_alert=True)
        return
    
    try:
        user_id = call.from_user.id
        chat_id = call.message.chat.id
        page = int(call.data.split("_")[2])
        per_page = 5
        offset = (page - 1) * per_page
        
        topics = get_user_topics(user_id, limit=per_page, offset=offset)
        
        conn = get_db_connection()
        c = conn.cursor()
        c.execute('SELECT COUNT(*) FROM topics WHERE user_id = %s', (user_id,))
        total_topics = c.fetchone()[0] or 0
        c.close()
        conn.close()
        
        if not topics and page == 1:
            text = """<b>📭 НЕТ ВАШИХ ТЕМ</b>

У вас пока нет созданных тем.
Начните обсуждение первым!"""
            
            markup = telebot.types.InlineKeyboardMarkup()
            markup.add(
                telebot.types.InlineKeyboardButton("➕ СОЗДАТЬ", callback_data="new_topic"),
                telebot.types.InlineKeyboardButton("🔙 В МЕНЮ", callback_data="menu_banned" if is_user_banned(user_id) else "menu")
            )
            
            try:
                bot.delete_message(chat_id, call.message.message_id)
            except:
                pass
            
            send_message_with_delete(chat_id, user_id, 'my_topics', text, markup)
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
        
        for topic in topics:
            topic_id = topic[0]
            replies_count = topic[6]
            btn_text = f"#{topic_id}"
            if replies_count > 0:
                btn_text += f" 💬{replies_count}"
            markup.add(
                telebot.types.InlineKeyboardButton(btn_text, callback_data=f"view_topic_{topic_id}_1")
            )
        
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
        
        markup.add(
            telebot.types.InlineKeyboardButton("🔙 В МЕНЮ", callback_data="menu_banned" if is_user_banned(user_id) else "menu")
        )
        
        try:
            bot.delete_message(chat_id, call.message.message_id)
        except:
            pass
        
        send_message_with_delete(chat_id, user_id, 'my_topics', text, markup)
        bot.answer_callback_query(call.id)
        
    except Exception as e:
        logger.error(f"Ошибка в my_topics_callback: {e}")
        bot.answer_callback_query(call.id, "❌ Ошибка", show_alert=True)

# ==================== ПОПУЛЯРНЫЕ ТЕМЫ ====================
@bot.callback_query_handler(func=lambda call: call.data.startswith("popular_"))
def popular_topics_callback(call):
    if call.message.chat.type != 'private':
        bot.answer_callback_query(call.id, "Эта функция доступна только в личных сообщениях", show_alert=True)
        return
    
    try:
        user_id = call.from_user.id
        chat_id = call.message.chat.id
        page = int(call.data.split("_")[1])
        per_page = 5
        offset = (page - 1) * per_page
        
        topics = get_popular_topics_with_ownership(user_id, limit=per_page, offset=offset)
        
        conn = get_db_connection()
        c = conn.cursor()
        c.execute('SELECT COUNT(*) FROM topics WHERE is_active = TRUE')
        total_topics = c.fetchone()[0] or 0
        c.close()
        conn.close()
        
        if not topics and page == 1:
            text = """<b>📭 НЕТ ПОПУЛЯРНЫХ ТЕМ</b>

Пока нет тем с ответами.
Станьте первым, кто начнет обсуждение!"""
            
            markup = telebot.types.InlineKeyboardMarkup(row_width=1)
            markup.add(
                telebot.types.InlineKeyboardButton("➕ СОЗДАТЬ ТЕМУ", callback_data="new_topic"),
                telebot.types.InlineKeyboardButton("🎲 СЛУЧАЙНАЯ", callback_data="random_topic"),
                telebot.types.InlineKeyboardButton("🔙 В МЕНЮ", callback_data="menu_banned" if is_user_banned(user_id) else "menu")
            )
            
            try:
                bot.delete_message(chat_id, call.message.message_id)
            except:
                pass
            
            send_message_with_delete(chat_id, user_id, 'popular', text, markup)
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
            topic_id, topic_text, _, is_active, _, _, replies_count, is_owner = topic
            preview = topic_text[:70] + "..." if len(topic_text) > 70 else topic_text
            status = "🟢" if is_active else "🔴"
            
            author_mark = " 👤<b>(Вы)</b>" if is_owner == 1 else ""
            
            text += f"\n\n{status} <b>{offset + i}. #{topic_id}{author_mark}</b>"
            text += f"\n{html.escape(preview)}"
            text += f"\n💬 Ответов: {replies_count}"
        
        markup = telebot.types.InlineKeyboardMarkup(row_width=3)
        
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
        
        markup.add(
            telebot.types.InlineKeyboardButton("🔙 В МЕНЮ", callback_data="menu_banned" if is_user_banned(user_id) else "menu")
        )
        
        try:
            bot.delete_message(chat_id, call.message.message_id)
        except:
            pass
        
        send_message_with_delete(chat_id, user_id, 'popular', text, markup)
        bot.answer_callback_query(call.id)
        
    except Exception as e:
        logger.error(f"Ошибка в popular_topics_callback: {e}")
        bot.answer_callback_query(call.id, "❌ Ошибка", show_alert=True)

# ==================== ПРОСМОТР ТЕМЫ ====================
@bot.callback_query_handler(func=lambda call: call.data.startswith("view_topic_"))
def view_topic_callback(call):
    if call.message.chat.type != 'private':
        bot.answer_callback_query(call.id, "Эта функция доступна только в личных сообщениях", show_alert=True)
        return
    
    try:
        user_id = call.from_user.id
        chat_id = call.message.chat.id
        parts = call.data.split("_")
        topic_id = int(parts[2])
        reply_page = int(parts[3]) if len(parts) > 3 else 1
        
        topic = get_topic(topic_id, user_id)
        
        if not topic:
            bot.answer_callback_query(call.id, "❌ Тема не найдена", show_alert=True)
            show_main_menu(chat_id, user_id)
            return
        
        topic_id, topic_text, topic_user_id, is_active, created_at, updated_at = topic
        
        per_page = 3
        offset = (reply_page - 1) * per_page
        replies = get_topic_replies(topic_id, limit=per_page, offset=offset)
        total_replies = get_replies_count(topic_id)
        total_pages = max(1, (total_replies + per_page - 1) // per_page)
        
        is_author = (topic_user_id == user_id)
        is_banned = is_user_banned(user_id)
        
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
                
                preview = reply_text[:100] + "..." if len(reply_text) > 100 else reply_text
                text += f"\n\n{offset + i}. {html.escape(preview)}"
                text += f"\n📅 {format_datetime(reply_created_at)}"
        else:
            text += "\n\n💭 Пока нет ответов. Будьте первым!"
        
        markup = telebot.types.InlineKeyboardMarkup(row_width=3)
        
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
        
        if is_active and not is_banned:
            markup.add(telebot.types.InlineKeyboardButton("💬 ОТВЕТИТЬ", callback_data=f"reply_topic_{topic_id}"))
        
        if not is_author and not is_banned:
            markup.add(telebot.types.InlineKeyboardButton("⚠️ ПОЖАЛОВАТЬСЯ", callback_data=f"report_topic_{topic_id}"))
        
        if is_author and not is_banned:
            if is_active:
                markup.add(
                    telebot.types.InlineKeyboardButton("🔒 ЗАКРЫТЬ", callback_data=f"close_topic_{topic_id}"),
                    telebot.types.InlineKeyboardButton("🗑️ УДАЛИТЬ", callback_data=f"delete_topic_{topic_id}")
                )
            else:
                markup.add(
                    telebot.types.InlineKeyboardButton("🗑️ УДАЛИТЬ", callback_data=f"delete_topic_{topic_id}")
                )
        
        markup.add(
            telebot.types.InlineKeyboardButton("🔙 В МЕНЮ", callback_data="menu_banned" if is_banned else "menu")
        )
        
        try:
            bot.delete_message(chat_id, call.message.message_id)
        except:
            pass
        
        send_message_with_delete(chat_id, user_id, 'view_topic', text, markup)
        bot.answer_callback_query(call.id)
        
    except Exception as e:
        logger.error(f"Ошибка в view_topic_callback: {e}")
        bot.answer_callback_query(call.id, "❌ Ошибка при загрузке темы", show_alert=True)

# ==================== СИСТЕМА ЖАЛОБ ====================
@bot.callback_query_handler(func=lambda call: call.data.startswith("report_topic_"))
def report_topic_callback(call):
    if call.message.chat.type != 'private':
        bot.answer_callback_query(call.id, "Эта функция доступна только в личных сообщениях", show_alert=True)
        return
    
    user_id = call.from_user.id
    chat_id = call.message.chat.id
    
    if is_user_banned(user_id):
        bot.answer_callback_query(call.id, "🚫 Вы не можете подавать жалобы во время ограничения", show_alert=True)
        return
    
    try:
        topic_id = int(call.data.split("_")[2])
        
        conn = get_db_connection()
        c = conn.cursor()
        c.execute('SELECT id FROM reports WHERE topic_id = %s AND reporter_id = %s AND status = %s', (topic_id, user_id, 'pending'))
        existing_report = c.fetchone()
        c.close()
        conn.close()
        
        if existing_report:
            bot.answer_callback_query(call.id, "⚠️ Вы уже жаловались на эту тему", show_alert=True)
            return
        
        user_states[user_id] = {'state': 'report_topic', 'topic_id': topic_id}
        
        text = f"""<b>⚠️ ЖАЛОБА НА ТЕМУ #{topic_id}</b>

Выберите причину жалобы:

1. <b>Спам</b> — реклама, флуд, боты
2. <b>Оскорбления</b> — ненормативная лексика, унижения
3. <b>Мошенничество</b> — обман, вымогательство
4. <b>Контент 18+</b> — порнография, эротика
5. <b>Нарушение законов</b> — призывы к насилию, экстремизм
6. <b>Другое</b> — иная причина

<i>Жалобы проверяются администратором вручную.
Ложные жалобы могут привести к ограничению.</i>"""
        
        markup = telebot.types.InlineKeyboardMarkup(row_width=2)
        markup.add(
            telebot.types.InlineKeyboardButton("1", callback_data=f"report_reason_{topic_id}_Спам"),
            telebot.types.InlineKeyboardButton("2", callback_data=f"report_reason_{topic_id}_Оскорбления"),
            telebot.types.InlineKeyboardButton("3", callback_data=f"report_reason_{topic_id}_Мошенничество"),
            telebot.types.InlineKeyboardButton("4", callback_data=f"report_reason_{topic_id}_Контент 18+"),
            telebot.types.InlineKeyboardButton("5", callback_data=f"report_reason_{topic_id}_Нарушение законов"),
            telebot.types.InlineKeyboardButton("6", callback_data=f"report_reason_{topic_id}_Другое"),
            telebot.types.InlineKeyboardButton("🔙 НАЗАД", callback_data=f"view_topic_{topic_id}_1")
        )
        
        try:
            bot.delete_message(chat_id, call.message.message_id)
        except:
            pass
        
        send_message_with_delete(chat_id, user_id, 'report', text, markup)
        bot.answer_callback_query(call.id)
        
    except Exception as e:
        logger.error(f"Ошибка в report_topic_callback: {e}")
        bot.answer_callback_query(call.id, "❌ Ошибка", show_alert=True)

@bot.callback_query_handler(func=lambda call: call.data.startswith("report_reason_"))
def report_reason_callback(call):
    if call.message.chat.type != 'private':
        bot.answer_callback_query(call.id, "Эта функция доступна только в личных сообщениях", show_alert=True)
        return
    
    user_id = call.from_user.id
    chat_id = call.message.chat.id
    
    if is_user_banned(user_id):
        bot.answer_callback_query(call.id, "🚫 Вы не можете подавать жалобы во время ограничения", show_alert=True)
        return
    
    try:
        parts = call.data.split("_")
        topic_id = int(parts[2])
        reason = parts[3]
        
        report_id = add_report(topic_id, user_id, reason)
        
        if report_id:
            text = f"""✅ <b>ЖАЛОБА #{report_id} ПРИНЯТА</b>

<b>Тема:</b> #{topic_id}
<b>Причина:</b> {reason}
<b>Время:</b> {datetime.now().strftime('%d.%m.%Y %H:%M')}

🙏 <b>Спасибо за вашу бдительность!</b>
<i>Ваша жалоба помогает нам поддерживать порядок
и создавать безопасное пространство для всех участников.
Мы ценим ваше участие в жизни сообщества!</i>

Администратор рассмотрит вашу жалобу в ближайшее время."""
        else:
            text = "❌ <b>ОШИБКА</b>\n\nНе удалось отправить жалобу. Попробуйте позже."
        
        markup = telebot.types.InlineKeyboardMarkup()
        markup.add(
            telebot.types.InlineKeyboardButton("🔙 К ТЕМЕ", callback_data=f"view_topic_{topic_id}_1"),
            telebot.types.InlineKeyboardButton("🏠 В МЕНЮ", callback_data="menu")
        )
        
        try:
            bot.delete_message(chat_id, call.message.message_id)
        except:
            pass
        
        send_message_with_delete(chat_id, user_id, 'report', text, markup)
        bot.answer_callback_query(call.id)
        
    except Exception as e:
        logger.error(f"Ошибка в report_reason_callback: {e}")
        bot.answer_callback_query(call.id, "❌ Ошибка", show_alert=True)

# ==================== ОТВЕТ НА ТЕМУ ====================
@bot.callback_query_handler(func=lambda call: call.data.startswith("reply_topic_"))
def reply_topic_callback(call):
    if call.message.chat.type != 'private':
        bot.answer_callback_query(call.id, "Эта функция доступна только в личных сообщениях", show_alert=True)
        return
    
    user_id = call.from_user.id
    chat_id = call.message.chat.id
    
    if is_user_banned(user_id):
        bot.answer_callback_query(call.id, "🚫 Вы не можете отвечать на темы во время ограничения", show_alert=True)
        return
    
    try:
        topic_id = int(call.data.split("_")[2])
        
        topic = get_topic(topic_id)
        if not topic or not topic[3]:
            bot.answer_callback_query(call.id, "❌ Тема закрыта", show_alert=True)
            return
        
        user_states[user_id] = {'state': 'reply_topic', 'topic_id': topic_id}
        
        topic_text = topic[1]
        preview = topic_text[:100] + "..." if len(topic_text) > 100 else topic_text
        
        text = f"""<b>💬 ОТВЕТ НА ТЕМУ #{topic_id}</b>

{html.escape(preview)}

<b>Напишите ваш ответ:</b>
• От 2 до 1000 символов
• Анонимный ответ
• Будьте уважительны

🔔 <i>Автор темы получит уведомление о вашем ответе</i>"""
        
        markup = telebot.types.InlineKeyboardMarkup()
        markup.add(telebot.types.InlineKeyboardButton("🔙 НАЗАД", callback_data=f"view_topic_{topic_id}_1"))
        
        try:
            bot.delete_message(chat_id, call.message.message_id)
        except:
            pass
        
        send_message_with_delete(chat_id, user_id, 'new_topic', text, markup)
        bot.answer_callback_query(call.id)
        
    except Exception as e:
        logger.error(f"Ошибка в reply_topic_callback: {e}")
        bot.answer_callback_query(call.id, "❌ Ошибка", show_alert=True)

# ==================== ЗАКРЫТИЕ ТЕМЫ ====================
@bot.callback_query_handler(func=lambda call: call.data.startswith("close_topic_"))
def close_topic_callback(call):
    if call.message.chat.type != 'private':
        bot.answer_callback_query(call.id, "Эта функция доступна только в личных сообщениях", show_alert=True)
        return
    
    user_id = call.from_user.id
    chat_id = call.message.chat.id
    
    if is_user_banned(user_id):
        bot.answer_callback_query(call.id, "🚫 Вы не можете закрывать темы во время ограничения", show_alert=True)
        return
    
    try:
        topic_id = int(call.data.split("_")[2])
        
        success, message = close_topic(topic_id, user_id)
        
        text = f"""{"✅" if success else "❌"} <b>{message}</b>

Тема #{topic_id} {"закрыта для новых ответов." if success else "не закрыта."}

<i>{"Вы можете удалить тему полностью или оставить её в архиве для чтения." if success else ""}</i>"""
        
        markup = telebot.types.InlineKeyboardMarkup()
        if success:
            markup.add(
                telebot.types.InlineKeyboardButton("🗑️ УДАЛИТЬ ТЕМУ", callback_data=f"delete_topic_{topic_id}"),
                telebot.types.InlineKeyboardButton("📄 К ТЕМЕ", callback_data=f"view_topic_{topic_id}_1"),
                telebot.types.InlineKeyboardButton("🏠 В МЕНЮ", callback_data="menu")
            )
        else:
            markup.add(
                telebot.types.InlineKeyboardButton("📄 К ТЕМЕ", callback_data=f"view_topic_{topic_id}_1"),
                telebot.types.InlineKeyboardButton("🏠 В МЕНЮ", callback_data="menu")
            )
        
        try:
            bot.delete_message(chat_id, call.message.message_id)
        except:
            pass
        
        send_message_with_delete(chat_id, user_id, 'view_topic', text, markup)
        bot.answer_callback_query(call.id)
        
    except Exception as e:
        logger.error(f"Ошибка в close_topic_callback: {e}")
        bot.answer_callback_query(call.id, "❌ Ошибка", show_alert=True)

# ==================== УДАЛЕНИЕ ТЕМЫ ====================
@bot.callback_query_handler(func=lambda call: call.data.startswith("delete_topic_"))
def delete_topic_callback(call):
    if call.message.chat.type != 'private':
        bot.answer_callback_query(call.id, "Эта функция доступна только в личных сообщениях", show_alert=True)
        return
    
    user_id = call.from_user.id
    chat_id = call.message.chat.id
    
    if is_user_banned(user_id):
        bot.answer_callback_query(call.id, "🚫 Вы не можете удалять темы во время ограничения", show_alert=True)
        return
    
    try:
        topic_id = int(call.data.split("_")[2])
        
        text = f"""<b>🗑️ ПОДТВЕРЖДЕНИЕ УДАЛЕНИЯ</b>

Вы собираетесь удалить тему #{topic_id}.

<b>⚠️ ВНИМАНИЕ:</b>
• Тема будет удалена безвозвратно
• Все ответы к теме также удалятся
• Действие нельзя отменить

<b>Вы уверены, что хотите удалить тему?</b>"""
        
        markup = telebot.types.InlineKeyboardMarkup(row_width=2)
        markup.add(
            telebot.types.InlineKeyboardButton("✅ ДА, УДАЛИТЬ", callback_data=f"confirm_delete_{topic_id}"),
            telebot.types.InlineKeyboardButton("❌ НЕТ, ОТМЕНА", callback_data=f"view_topic_{topic_id}_1")
        )
        
        try:
            bot.delete_message(chat_id, call.message.message_id)
        except:
            pass
        
        send_message_with_delete(chat_id, user_id, 'view_topic', text, markup)
        bot.answer_callback_query(call.id)
        
    except Exception as e:
        logger.error(f"Ошибка в delete_topic_callback: {e}")
        bot.answer_callback_query(call.id, "❌ Ошибка", show_alert=True)

@bot.callback_query_handler(func=lambda call: call.data.startswith("confirm_delete_"))
def confirm_delete_callback(call):
    if call.message.chat.type != 'private':
        bot.answer_callback_query(call.id, "Эта функция доступна только в личных сообщениях", show_alert=True)
        return
    
    user_id = call.from_user.id
    chat_id = call.message.chat.id
    
    if is_user_banned(user_id):
        bot.answer_callback_query(call.id, "🚫 Вы не можете удалять темы во время ограничения", show_alert=True)
        return
    
    try:
        topic_id = int(call.data.split("_")[2])
        
        success, message = delete_topic(topic_id, user_id)
        
        text = f"""{"✅" if success else "❌"} <b>{message}</b>

Тема #{topic_id} и все ответы к ней {"удалены из архива." if success else "не удалены."}

<i>{"Вы можете создать новую тему или просмотреть другие обсуждения." if success else ""}</i>"""
        
        markup = telebot.types.InlineKeyboardMarkup()
        if success:
            markup.add(
                telebot.types.InlineKeyboardButton("➕ НОВАЯ ТЕМА", callback_data="new_topic"),
                telebot.types.InlineKeyboardButton("🎲 СЛУЧАЙНАЯ", callback_data="random_topic"),
                telebot.types.InlineKeyboardButton("🏠 В МЕНЮ", callback_data="menu")
            )
        else:
            markup.add(
                telebot.types.InlineKeyboardButton("🏠 В МЕНЮ", callback_data="menu")
            )
        
        try:
            bot.delete_message(chat_id, call.message.message_id)
        except:
            pass
        
        send_message_with_delete(chat_id, user_id, 'view_topic', text, markup)
        bot.answer_callback_query(call.id)
        
    except Exception as e:
        logger.error(f"Ошибка в confirm_delete_callback: {e}")
        bot.answer_callback_query(call.id, "❌ Ошибка", show_alert=True)

# ==================== ВОЗВРАТ В МЕНЮ ====================
@bot.callback_query_handler(func=lambda call: call.data == "menu")
def menu_callback(call):
    if call.message.chat.type != 'private':
        bot.answer_callback_query(call.id, "Эта функция доступна только в личных сообщениях", show_alert=True)
        return
    
    user_id = call.from_user.id
    chat_id = call.message.chat.id
    
    try:
        bot.delete_message(chat_id, call.message.message_id)
    except:
        pass
    
    show_main_menu(chat_id, user_id)
    bot.answer_callback_query(call.id)

@bot.callback_query_handler(func=lambda call: call.data == "menu_banned")
def menu_banned_callback(call):
    if call.message.chat.type != 'private':
        bot.answer_callback_query(call.id, "Эта функция доступна только в личных сообщениях", show_alert=True)
        return
    
    user_id = call.from_user.id
    chat_id = call.message.chat.id
    
    try:
        bot.delete_message(chat_id, call.message.message_id)
    except:
        pass
    
    show_main_menu_for_banned_user(chat_id, user_id)
    bot.answer_callback_query(call.id)

# ==================== ОБРАБОТКА ТЕКСТОВЫХ СООБЩЕНИЙ ====================
@bot.message_handler(func=lambda message: True)
def text_handler(message):
    """Обработка текстовых сообщений только в личных чатах"""
    if message.chat.type != 'private':
        return
    
    user_id = message.from_user.id
    chat_id = message.chat.id
    text = message.text.strip()
    
    logger.info(f"Получено сообщение от {user_id}: '{text[:50]}...'")
    
    if text.startswith('/top'):
        top_command(message)
        return
    
    if is_user_banned(user_id):
        logger.warning(f"Забаненный пользователь {user_id} пытается отправить сообщение")
        ban_info = check_user_ban(user_id)
        if ban_info:
            try:
                unbanned_at_str = ban_info[2]
                unbanned_at = datetime.strptime(unbanned_at_str, '%Y-%m-%d %H:%M:%S')
                time_left = unbanned_at - datetime.now()
                hours_left = int(time_left.total_seconds() // 3600)
                minutes_left = int((time_left.total_seconds() % 3600) // 60)
                
                response = f"""🚫 <b>ДОСТУП ОГРАНИЧЕН</b>

Ваш аккаунт ограничен за нарушение правил.

<b>Причина:</b> {ban_info[1]}
<b>Ограничен до:</b> {unbanned_at.strftime('%d.%m.%Y %H:%M')}
<b>Осталось:</b> {hours_left}ч {minutes_left}м

⚠️ <i>Пожалуйста, соблюдайте правила сообщества.</i>"""
                
                bot.send_message(chat_id, response, parse_mode='HTML')
                
                try:
                    bot.delete_message(chat_id, message.message_id)
                except:
                    pass
                
                if user_id in user_states:
                    logger.info(f"Сбрасываем состояние для забаненного пользователя {user_id}")
                    del user_states[user_id]
                
                return
                
            except Exception as e:
                logger.error(f"Ошибка при обработке бана: {e}")
    
    try:
        bot.delete_message(chat_id, message.message_id)
    except:
        pass
    
    if user_id not in user_states:
        logger.info(f"Пользователь {user_id} не в состоянии, показываем меню")
        show_main_menu(chat_id, user_id)
        return
    
    state = user_states[user_id]
    logger.info(f"Пользователь {user_id} в состоянии: {state['state']}")
    
    if state['state'] == 'new_topic':
        logger.info(f"Попытка создания темы пользователем {user_id}")
        
        if len(text) < 2:
            msg = bot.send_message(chat_id, "❌ Слишком коротко. Минимум 2 символа.")
            add_message_to_delete(user_id, msg.message_id)
            return
        if len(text) > 2000:
            msg = bot.send_message(chat_id, "❌ Слишком длинно. Максимум 2000 символов.")
            add_message_to_delete(user_id, msg.message_id)
            return
        
        result = add_topic(text, user_id)
        
        if result is None:
            logger.warning(f"add_topic вернул None для пользователя {user_id} - ЗАБАНЕН")
            msg = bot.send_message(chat_id, "🚫 Не удалось создать тему. Ваш аккаунт ограничен.")
            add_message_to_delete(user_id, msg.message_id)
            show_main_menu_for_banned_user(chat_id, user_id)
        elif result == "limit_exceeded":
            logger.warning(f"Пользователь {user_id} достиг дневного лимита")
            remaining, topics_today = check_daily_topic_limit(user_id)
            
            text_limit = f"""🚫 <b>ДНЕВНОЙ ЛИМИТ ИСЧЕРПАН</b>

Вы создали максимальное количество тем на сегодня.

<b>Статистика:</b>
• Создано сегодня: {topics_today}/{DAILY_TOPIC_LIMIT} тем
• Доступно снова: завтра

<b>Что можно делать:</b>
• Отвечать на чужие темы
• Просматривать архив
• Управлять своими темами

📅 <i>Лимит обновляется каждый день в 00:00</i>"""
            
            markup = telebot.types.InlineKeyboardMarkup()
            markup.add(
                telebot.types.InlineKeyboardButton("🎲 СЛУЧАЙНАЯ ТЕМА", callback_data="random_topic"),
                telebot.types.InlineKeyboardButton("🔙 В МЕНЮ", callback_data="menu")
            )
            
            send_message_with_delete(chat_id, user_id, 'limit', text_limit, markup)
        else:
            topic_id = result
            logger.info(f"Тема #{topic_id} успешно создана пользователем {user_id}")
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
        
        if user_id in user_states:
            del user_states[user_id]
        
    elif state['state'] == 'reply_topic':
        logger.info(f"Попытка создания ответа пользователем {user_id}")
        topic_id = state['topic_id']
        
        topic = get_topic(topic_id)
        if not topic or not topic[3]:
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
        
        if reply_id is None:
            logger.warning(f"add_reply вернул None для пользователя {user_id} - ЗАБАНЕН")
            msg = bot.send_message(chat_id, "🚫 Не удалось создать ответ. Ваш аккаунт ограничен.")
            add_message_to_delete(user_id, msg.message_id)
            show_main_menu_for_banned_user(chat_id, user_id)
        elif reply_id == "closed":
            msg = bot.send_message(chat_id, "❌ Тема закрыта, нельзя оставить ответ.")
            add_message_to_delete(user_id, msg.message_id)
        else:
            logger.info(f"Ответ #{reply_id} успешно создан пользователем {user_id}")
            response = f"""✅ <b>ОТВЕТ #{reply_id} СОХРАНЕН</b>

Вы ответили на тему #{topic_id}.

<b>💭 Что дальше?</b>
• Автор темы получил уведомление
• Ответ доступен всем пользователей
• Вы можете ответить еще"""
            
            markup = telebot.types.InlineKeyboardMarkup(row_width=1)
            markup.add(
                telebot.types.InlineKeyboardButton("📄 ПЕРЕЙТИ К ТЕМЕ", callback_data=f"view_topic_{topic_id}_1"),
                telebot.types.InlineKeyboardButton("🎲 СЛУЧАЙНАЯ", callback_data="random_topic"),
                telebot.types.InlineKeyboardButton("🏠 В МЕНЮ", callback_data="menu")
            )
            
            send_message_with_delete(chat_id, user_id, 'reply_created', response, markup)
        
        del user_states[user_id]
    
    elif state['state'] == 'change_username':
        pass
    
    elif state['state'] == 'ban_user':
        if not ADMIN_ID or user_id != ADMIN_ID:
            del user_states[user_id]
            show_main_menu(chat_id, user_id)
            return
        
        user_id_to_ban = state['user_id_to_ban']
        report_id = state.get('report_id')
        
        if text.isdigit() and 1 <= int(text) <= 30:
            days = int(text)
            reason = "Нарушение правил сообщества"
        else:
            days = 1
            reason = text[:200]
        
        success = ban_user(user_id_to_ban, reason, user_id, days)
        
        if success:
            if report_id:
                update_report_status(report_id, 'resolved', user_id, f'Пользователь забанен на {days} дней')
            
            text_response = f"""✅ <b>ПОЛЬЗОВАТЕЛЬ ЗАБАНЕН</b>

Пользователь {user_id_to_ban} забанен на {days} день(дней).

<b>Причина:</b>
{reason}

<b>Администратор:</b> {user_id}
<b>Время:</b> {datetime.now().strftime('%d.%m.%Y %H:%M')}

<i>Бан применен успешно.</i>"""
        else:
            text_response = "❌ <b>ОШИБКА</b>\n\nНе удалось забанить пользователя."
        
        markup = telebot.types.InlineKeyboardMarkup()
        if report_id:
            markup.add(
                telebot.types.InlineKeyboardButton("🔙 К ЖАЛОБЕ", callback_data=f"view_report_{report_id}")
            )
        else:
            markup.add(
                telebot.types.InlineKeyboardButton("🔙 К СПИСКУ ЖАЛОБ", callback_data="admin_reports_1")
            )
        
        send_message_with_delete(chat_id, user_id, 'admin', text_response, markup)
        del user_states[user_id]
    
    elif state['state'] == 'delete_topic_admin':
        if not ADMIN_ID or user_id != ADMIN_ID:
            del user_states[user_id]
            show_main_menu(chat_id, user_id)
            return
        
        topic_id = state['topic_id']
        report_id = state.get('report_id')
        reason = text[:200]
        
        success, message = delete_topic_admin(topic_id, user_id, reason)
        
        if success:
            if report_id:
                update_report_status(report_id, 'resolved', user_id, f'Тема удалена: {reason}')
            
            text_response = f"""✅ <b>ТЕМА УДАЛЕНА</b>

Тема #{topic_id} удалена администратором.

<b>Причина удаления:</b>
{reason}

<b>Администратор:</b> {user_id}
<b>Время:</b> {datetime.now().strftime('%d.%m.%Y %H:%M')}

<i>Тема и все ответы удалены безвозвратно.</i>"""
        else:
            text_response = f"❌ <b>ОШИБКА</b>\n\n{message}"
        
        markup = telebot.types.InlineKeyboardMarkup()
        if report_id:
            markup.add(
                telebot.types.InlineKeyboardButton("🔙 К ЖАЛОБЕ", callback_data=f"view_report_{report_id}")
            )
        else:
            markup.add(
                telebot.types.InlineKeyboardButton("🔙 К СПИСКУ ЖАЛОБ", callback_data="admin_reports_1")
            )
        
        send_message_with_delete(chat_id, user_id, 'admin', text_response, markup)
        del user_states[user_id]
    
    elif state['state'] == 'report_topic':
        pass

# ==================== ЗАПУСК БОТА ====================
if __name__ == '__main__':
    logger.info("🗄️ Бот 'Архив мыслей' запущен...")
    logger.info("🐘 Используется PostgreSQL")
    logger.info("🔔 Система управления уведомлениями активирована")
    logger.info("📌 В групповых чатах бот реагирует ТОЛЬКО на команду /top")
    logger.info("💬 В личных чатах работает полный функционал")
    
    cleanup_invalid_reports()
    
    if ADMIN_ID:
        logger.info(f"⚙️ Администратор: {ADMIN_ID}")
    
    PORT = int(os.environ.get('PORT', 8080))
    bot.remove_webhook()
    
    try:
        webhook_url = os.environ.get('WEBHOOK_URL')
        if webhook_url:
            logger.info(f"🚀 Используем вебхук на Railway: {webhook_url}")
            bot.set_webhook(url=f"{webhook_url}/{BOT_TOKEN}")
            
            from flask import Flask, request
            app = Flask(__name__)
            
            @app.route(f'/{BOT_TOKEN}', methods=['POST'])
            def webhook():
                if request.headers.get('content-type') == 'application/json':
                    json_string = request.get_data().decode('utf-8')
                    update = telebot.types.Update.de_json(json_string)
                    bot.process_new_updates([update])
                    return ''
                return 'Bad request', 400
            
            @app.route('/')
            def index():
                return 'Bot is running on Railway!'
            
            logger.info(f"🌐 Запускаем Flask сервер на порту {PORT}")
            app.run(host='0.0.0.0', port=PORT)
        else:
            logger.info("🔄 Используем polling режим")
            bot.remove_webhook()
            bot.polling(none_stop=True, timeout=30, interval=2, skip_pending=True)
    except KeyboardInterrupt:
        logger.info("Бот остановлен пользователем")
    except Exception as e:
        logger.error(f"Критическая ошибка: {e}")
        raise
