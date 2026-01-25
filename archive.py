# ============================================================
# TELEGRAM BOT — FULL VERSION (PostgreSQL, Railway-ready)
# Part 1/6
# ============================================================

import telebot
import psycopg2
import psycopg2.extras
import os
import random
import logging
import time
import html
import re
from datetime import datetime, timedelta
from contextlib import contextmanager

# ==================== ENV ====================
BOT_TOKEN = os.getenv("BOT_TOKEN")
ADMIN_ID = int(os.getenv("ADMIN_ID", "0"))
DATABASE_URL = os.getenv("DATABASE_URL")

if not BOT_TOKEN:
    raise RuntimeError("BOT_TOKEN is not set")
if not DATABASE_URL:
    raise RuntimeError("DATABASE_URL is not set")

DAILY_TOPIC_LIMIT = 5

# ==================== LOGGING ====================
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s"
)
logger = logging.getLogger("thoughts_bot")

# ==================== POSTGRES ====================

@contextmanager
def get_conn():
    conn = psycopg2.connect(
        DATABASE_URL,
        sslmode="require",
        cursor_factory=psycopg2.extras.RealDictCursor
    )
    try:
        yield conn
        conn.commit()
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()

def init_db():
    with get_conn() as conn:
        cur = conn.cursor()

        cur.execute("""
        CREATE TABLE IF NOT EXISTS topics (
            id SERIAL PRIMARY KEY,
            text TEXT NOT NULL,
            user_id BIGINT NOT NULL,
            is_active BOOLEAN DEFAULT TRUE,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );
        """)

        cur.execute("""
        CREATE TABLE IF NOT EXISTS replies (
            id SERIAL PRIMARY KEY,
            topic_id INTEGER REFERENCES topics(id) ON DELETE CASCADE,
            text TEXT NOT NULL,
            user_id BIGINT NOT NULL,
            is_active BOOLEAN DEFAULT TRUE,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );
        """)

        cur.execute("""
        CREATE TABLE IF NOT EXISTS reports (
            id SERIAL PRIMARY KEY,
            topic_id INTEGER REFERENCES topics(id) ON DELETE CASCADE,
            reporter_id BIGINT NOT NULL,
            reason TEXT NOT NULL,
            status TEXT DEFAULT 'pending',
            admin_action TEXT,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            resolved_at TIMESTAMP,
            admin_id BIGINT
        );
        """)

        cur.execute("""
        CREATE TABLE IF NOT EXISTS bans (
            user_id BIGINT PRIMARY KEY,
            reason TEXT NOT NULL,
            admin_id BIGINT NOT NULL,
            banned_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            unbanned_at TIMESTAMP,
            is_active BOOLEAN DEFAULT TRUE
        );
        """)

        cur.execute("""
        CREATE TABLE IF NOT EXISTS user_stats (
            user_id BIGINT PRIMARY KEY,
            topics_created INTEGER DEFAULT 0,
            replies_written INTEGER DEFAULT 0,
            replies_received INTEGER DEFAULT 0,
            last_active TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );
        """)

        cur.execute("""
        CREATE TABLE IF NOT EXISTS user_names (
            user_id BIGINT PRIMARY KEY,
            username TEXT UNIQUE NOT NULL,
            updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );
        """)

        cur.execute("""
        CREATE TABLE IF NOT EXISTS daily_limits (
            user_id BIGINT,
            date DATE,
            topics_created INTEGER DEFAULT 0,
            PRIMARY KEY (user_id, date)
        );
        """)

        cur.execute("""
        CREATE TABLE IF NOT EXISTS user_notifications (
            user_id BIGINT PRIMARY KEY,
            reply_notifications BOOLEAN DEFAULT TRUE,
            updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );
        """)

        logger.info("✅ PostgreSQL initialized")

init_db()

# ==================== BOT ====================
bot = telebot.TeleBot(BOT_TOKEN, parse_mode="HTML")

# ==================== UTILS ====================

def sanitize(text: str) -> str:
    return html.escape(" ".join(text.strip().split()))

def format_dt(dt):
    if not dt:
        return "неизвестно"
    return dt.strftime("%d.%m.%Y %H:%M")

def generate_username():
    while True:
        name = f"аноним_{random.randint(1000,9999)}"
        with get_conn() as conn:
            cur = conn.cursor()
            cur.execute("SELECT 1 FROM user_names WHERE username=%s", (name,))
            if not cur.fetchone():
                return name
# ============================================================
# Part 2/6 — Users, Names, Notifications, Bans, Stats, Ranks
# ============================================================

# ==================== USERS & NAMES ====================

def get_username(user_id: int) -> str:
    with get_conn() as conn:
        cur = conn.cursor()
        cur.execute("SELECT username FROM user_names WHERE user_id=%s", (user_id,))
        row = cur.fetchone()
        if row:
            return row["username"]

        username = generate_username()
        cur.execute(
            "INSERT INTO user_names (user_id, username) VALUES (%s,%s)",
            (user_id, username)
        )
        return username


def validate_username(username: str):
    if not username:
        return False, "Имя не может быть пустым"
    if len(username) < 3:
        return False, "Минимум 3 символа"
    if len(username) > 12:
        return False, "Максимум 12 символов"
    if not re.match(r'^[a-zA-Zа-яА-ЯёЁ0-9_]+$', username):
        return False, "Допустимы только буквы, цифры и _"
    return True, "OK"


def set_username(user_id: int, username: str):
    ok, msg = validate_username(username)
    if not ok:
        return False, msg

    with get_conn() as conn:
        cur = conn.cursor()
        cur.execute(
            "SELECT 1 FROM user_names WHERE username=%s AND user_id!=%s",
            (username, user_id)
        )
        if cur.fetchone():
            return False, "Имя уже занято"

        cur.execute("""
            INSERT INTO user_names (user_id, username, updated_at)
            VALUES (%s,%s,NOW())
            ON CONFLICT (user_id)
            DO UPDATE SET username=EXCLUDED.username, updated_at=NOW()
        """, (user_id, username))

    return True, "Имя обновлено"


# ==================== NOTIFICATIONS ====================

def get_notifications(user_id: int) -> bool:
    with get_conn() as conn:
        cur = conn.cursor()
        cur.execute(
            "SELECT reply_notifications FROM user_notifications WHERE user_id=%s",
            (user_id,)
        )
        row = cur.fetchone()
        if row is not None:
            return row["reply_notifications"]

        cur.execute(
            "INSERT INTO user_notifications (user_id, reply_notifications) VALUES (%s,TRUE)",
            (user_id,)
        )
        return True


def toggle_notifications(user_id: int):
    current = get_notifications(user_id)
    new = not current
    with get_conn() as conn:
        cur = conn.cursor()
        cur.execute("""
            INSERT INTO user_notifications (user_id, reply_notifications, updated_at)
            VALUES (%s,%s,NOW())
            ON CONFLICT (user_id)
            DO UPDATE SET reply_notifications=EXCLUDED.reply_notifications, updated_at=NOW()
        """, (user_id, new))
    return new


# ==================== BANS ====================

def check_ban(user_id: int):
    with get_conn() as conn:
        cur = conn.cursor()
        cur.execute("""
            SELECT * FROM bans
            WHERE user_id=%s AND is_active=TRUE AND unbanned_at > NOW()
        """, (user_id,))
        return cur.fetchone()


def is_banned(user_id: int) -> bool:
    return check_ban(user_id) is not None


def ban_user(user_id: int, reason: str, admin_id: int, days: int):
    until = datetime.utcnow() + timedelta(days=days)
    with get_conn() as conn:
        cur = conn.cursor()
        cur.execute("""
            INSERT INTO bans (user_id, reason, admin_id, unbanned_at)
            VALUES (%s,%s,%s,%s)
            ON CONFLICT (user_id)
            DO UPDATE SET
                reason=EXCLUDED.reason,
                admin_id=EXCLUDED.admin_id,
                banned_at=NOW(),
                unbanned_at=EXCLUDED.unbanned_at,
                is_active=TRUE
        """, (user_id, reason, admin_id, until))
    return until


def unban_user(user_id: int):
    with get_conn() as conn:
        cur = conn.cursor()
        cur.execute("UPDATE bans SET is_active=FALSE WHERE user_id=%s", (user_id,))


# ==================== DAILY LIMITS ====================

def daily_limit(user_id: int):
    today = datetime.utcnow().date()
    with get_conn() as conn:
        cur = conn.cursor()
        cur.execute("""
            SELECT topics_created FROM daily_limits
            WHERE user_id=%s AND date=%s
        """, (user_id, today))
        row = cur.fetchone()
        if not row:
            return DAILY_TOPIC_LIMIT, 0
        return max(0, DAILY_TOPIC_LIMIT - row["topics_created"]), row["topics_created"]


def inc_daily_limit(user_id: int):
    today = datetime.utcnow().date()
    with get_conn() as conn:
        cur = conn.cursor()
        cur.execute("""
            INSERT INTO daily_limits (user_id, date, topics_created)
            VALUES (%s,%s,1)
            ON CONFLICT (user_id, date)
            DO UPDATE SET topics_created=daily_limits.topics_created+1
        """, (user_id, today))


# ==================== STATS ====================

def ensure_stats(user_id: int):
    with get_conn() as conn:
        cur = conn.cursor()
        cur.execute("""
            INSERT INTO user_stats (user_id)
            VALUES (%s)
            ON CONFLICT DO NOTHING
        """, (user_id,))


def get_stats(user_id: int):
    ensure_stats(user_id)
    with get_conn() as conn:
        cur = conn.cursor()
        cur.execute("SELECT * FROM user_stats WHERE user_id=%s", (user_id,))
        return cur.fetchone()


def inc_stat(user_id: int, field: str):
    with get_conn() as conn:
        cur = conn.cursor()
        cur.execute(f"""
            UPDATE user_stats
            SET {field} = {field} + 1,
                last_active = NOW()
            WHERE user_id=%s
        """, (user_id,))


# ==================== RANKS ====================

RANKS = [
    ("👶 НОВИЧОК", 4, 9),
    ("🧒 ПОСЕТИТЕЛЬ", 9, 24),
    ("👨 УЧАСТНИК", 19, 49),
    ("👨‍💼 АКТИВИСТ", 34, 99),
    ("👨‍🔬 АВТОР", 54, 199),
    ("👨‍🎓 МЫСЛИТЕЛЬ", 84, 399),
    ("👨‍🚀 ДИСКУТАНТ", 129, 699),
    ("👨‍✈️ ФИЛОСОФ", 199, 1199),
    ("👑 МАСТЕР", 299, 1999),
    ("⚡ ЛЕГЕНДА", 999999, 999999),
]


def get_rank(stats):
    for name, max_topics, max_replies in RANKS:
        if stats["topics_created"] <= max_topics and stats["replies_written"] <= max_replies:
            return name
    return RANKS[-1][0]
# ============================================================
# Part 3/6 — Topics, Replies, Feeds, Pagination, Notifications
# ============================================================

# ==================== TOPICS ====================

def create_topic(user_id: int, text: str):
    if is_banned(user_id):
        return "banned"

    remaining, _ = daily_limit(user_id)
    if remaining <= 0:
        return "limit"

    text = sanitize(text)
    if len(text) < 5:
        return "short"

    with get_conn() as conn:
        cur = conn.cursor()
        cur.execute("""
            INSERT INTO topics (text, user_id)
            VALUES (%s,%s)
            RETURNING id
        """, (text, user_id))
        topic_id = cur.fetchone()["id"]

    inc_daily_limit(user_id)
    ensure_stats(user_id)
    inc_stat(user_id, "topics_created")
    return topic_id


def get_topic(topic_id: int):
    with get_conn() as conn:
        cur = conn.cursor()
        cur.execute("""
            SELECT t.*, u.username
            FROM topics t
            JOIN user_names u ON u.user_id = t.user_id
            WHERE t.id=%s AND t.is_active=TRUE
        """, (topic_id,))
        return cur.fetchone()


def delete_topic(topic_id: int, admin=False):
    with get_conn() as conn:
        cur = conn.cursor()
        if admin:
            cur.execute("DELETE FROM topics WHERE id=%s", (topic_id,))
        else:
            cur.execute("UPDATE topics SET is_active=FALSE WHERE id=%s", (topic_id,))


# ==================== REPLIES ====================

def add_reply(topic_id: int, user_id: int, text: str):
    if is_banned(user_id):
        return "banned"

    text = sanitize(text)
    if len(text) < 2:
        return "short"

    with get_conn() as conn:
        cur = conn.cursor()
        cur.execute("""
            SELECT user_id FROM topics
            WHERE id=%s AND is_active=TRUE
        """, (topic_id,))
        row = cur.fetchone()
        if not row:
            return "not_found"

        author_id = row["user_id"]

        cur.execute("""
            INSERT INTO replies (topic_id, text, user_id)
            VALUES (%s,%s,%s)
        """, (topic_id, text, user_id))

    ensure_stats(user_id)
    inc_stat(user_id, "replies_written")
    ensure_stats(author_id)
    inc_stat(author_id, "replies_received")

    # уведомление автору
    if author_id != user_id and get_notifications(author_id):
        try:
            bot.send_message(
                author_id,
                f"💬 На вашу тему ответили:\n\n<i>{text[:200]}</i>"
            )
        except:
            pass

    return True


def get_replies(topic_id: int, offset=0, limit=5):
    with get_conn() as conn:
        cur = conn.cursor()
        cur.execute("""
            SELECT r.*, u.username
            FROM replies r
            JOIN user_names u ON u.user_id=r.user_id
            WHERE r.topic_id=%s AND r.is_active=TRUE
            ORDER BY r.created_at ASC
            OFFSET %s LIMIT %s
        """, (topic_id, offset, limit))
        return cur.fetchall()


# ==================== FEEDS ====================

def get_latest_topics(offset=0, limit=5):
    with get_conn() as conn:
        cur = conn.cursor()
        cur.execute("""
            SELECT t.id, t.text, t.created_at, u.username
            FROM topics t
            JOIN user_names u ON u.user_id=t.user_id
            WHERE t.is_active=TRUE
            ORDER BY t.created_at DESC
            OFFSET %s LIMIT %s
        """, (offset, limit))
        return cur.fetchall()


def get_random_topic():
    with get_conn() as conn:
        cur = conn.cursor()
        cur.execute("""
            SELECT t.id FROM topics t
            WHERE t.is_active=TRUE
            ORDER BY RANDOM()
            LIMIT 1
        """)
        row = cur.fetchone()
        return row["id"] if row else None


def get_popular_topics(limit=5):
    with get_conn() as conn:
        cur = conn.cursor()
        cur.execute("""
            SELECT t.id, t.text, COUNT(r.id) AS replies, u.username
            FROM topics t
            LEFT JOIN replies r ON r.topic_id=t.id AND r.is_active=TRUE
            JOIN user_names u ON u.user_id=t.user_id
            WHERE t.is_active=TRUE
            GROUP BY t.id, u.username
            ORDER BY replies DESC, t.created_at DESC
            LIMIT %s
        """, (limit,))
        return cur.fetchall()


# ==================== PAGINATION ====================

def format_topic(topic):
    return (
        f"📝 <b>Тема #{topic['id']}</b>\n"
        f"👤 {topic['username']}\n"
        f"🕒 {format_dt(topic['created_at'])}\n\n"
        f"{topic['text']}"
    )


def format_reply(reply):
    return (
        f"💬 <b>{reply['username']}</b> "
        f"<i>{format_dt(reply['created_at'])}</i>\n"
        f"{reply['text']}"
    )
# ============================================================
# Part 4/6 — Telegram UI, Commands, Inline Buttons, States
# ============================================================

from telebot.types import InlineKeyboardMarkup, InlineKeyboardButton

# ==================== USER STATES ====================

USER_STATES = {}  # user_id -> dict

def set_state(user_id, state, data=None):
    USER_STATES[user_id] = {"state": state, "data": data or {}}

def clear_state(user_id):
    USER_STATES.pop(user_id, None)

def get_state(user_id):
    return USER_STATES.get(user_id)

# ==================== KEYBOARDS ====================

def main_menu():
    kb = InlineKeyboardMarkup()
    kb.add(
        InlineKeyboardButton("📰 Лента", callback_data="feed_0"),
        InlineKeyboardButton("🎲 Случайная", callback_data="random")
    )
    kb.add(
        InlineKeyboardButton("🔥 Популярные", callback_data="top"),
        InlineKeyboardButton("👤 Профиль", callback_data="profile")
    )
    return kb


def topic_keyboard(topic_id):
    kb = InlineKeyboardMarkup()
    kb.add(
        InlineKeyboardButton("💬 Ответить", callback_data=f"reply_{topic_id}"),
        InlineKeyboardButton("🚩 Пожаловаться", callback_data=f"report_{topic_id}")
    )
    return kb


def replies_keyboard(topic_id, offset):
    kb = InlineKeyboardMarkup()
    if offset > 0:
        kb.add(InlineKeyboardButton("⬅️ Назад", callback_data=f"replies_{topic_id}_{offset-5}"))
    kb.add(InlineKeyboardButton("➡️ Далее", callback_data=f"replies_{topic_id}_{offset+5}"))
    return kb

# ==================== COMMANDS ====================

@bot.message_handler(commands=["start"])
def cmd_start(message):
    username = get_username(message.from_user.id)
    bot.send_message(
        message.chat.id,
        f"👋 Привет, <b>{username}</b>!\n\n"
        "Напиши мысль — она станет темой.\n"
        "Или используй меню ниже 👇",
        reply_markup=main_menu()
    )


@bot.message_handler(commands=["profile"])
def cmd_profile(message):
    stats = get_stats(message.from_user.id)
    rank = get_rank(stats)
    username = get_username(message.from_user.id)

    bot.send_message(
        message.chat.id,
        f"👤 <b>Профиль</b>\n\n"
        f"Имя: <b>{username}</b>\n"
        f"Ранг: {rank}\n\n"
        f"📝 Темы: {stats['topics_created']}\n"
        f"💬 Ответы: {stats['replies_written']}\n"
        f"📥 Получено ответов: {stats['replies_received']}",
        reply_markup=InlineKeyboardMarkup().add(
            InlineKeyboardButton("✏️ Сменить имя", callback_data="change_name"),
            InlineKeyboardButton("🔔 Уведомления", callback_data="toggle_notify")
        )
    )


# ==================== TEXT HANDLER ====================

@bot.message_handler(func=lambda m: True)
def text_handler(message):
    state = get_state(message.from_user.id)

    if state:
        if state["state"] == "reply":
            topic_id = state["data"]["topic_id"]
            res = add_reply(topic_id, message.from_user.id, message.text)
            clear_state(message.from_user.id)

            if res == "banned":
                bot.send_message(message.chat.id, "🚫 Вы заблокированы")
            elif res == "short":
                bot.send_message(message.chat.id, "⚠️ Слишком короткий ответ")
            elif res == "not_found":
                bot.send_message(message.chat.id, "❌ Тема не найдена")
            else:
                bot.send_message(message.chat.id, "✅ Ответ добавлен")
            return

        if state["state"] == "change_name":
            ok, msg = set_username(message.from_user.id, message.text)
            clear_state(message.from_user.id)
            bot.send_message(message.chat.id, "✅ "+msg if ok else "❌ "+msg)
            return

    # если нет состояния — создаём тему
    res = create_topic(message.from_user.id, message.text)

    if res == "banned":
        bot.send_message(message.chat.id, "🚫 Вы заблокированы")
    elif res == "limit":
        bot.send_message(message.chat.id, "🚫 Лимит тем на сегодня исчерпан")
    elif res == "short":
        bot.send_message(message.chat.id, "⚠️ Тема слишком короткая")
    else:
        topic = get_topic(res)
        bot.send_message(
            message.chat.id,
            "✅ Тема создана:\n\n" + format_topic(topic),
            reply_markup=topic_keyboard(res)
        )

# ==================== CALLBACKS ====================

@bot.callback_query_handler(func=lambda c: True)
def callbacks(call):
    data = call.data
    user_id = call.from_user.id

    if data.startswith("feed_"):
        offset = int(data.split("_")[1])
        topics = get_latest_topics(offset)
        if not topics:
            bot.answer_callback_query(call.id, "Пусто")
            return

        for t in topics:
            bot.send_message(
                call.message.chat.id,
                format_topic(t),
                reply_markup=topic_keyboard(t["id"])
            )
        return

    if data == "random":
        tid = get_random_topic()
        if not tid:
            bot.answer_callback_query(call.id, "Нет тем")
            return
        topic = get_topic(tid)
        bot.send_message(
            call.message.chat.id,
            format_topic(topic),
            reply_markup=topic_keyboard(tid)
        )
        return

    if data == "top":
        topics = get_popular_topics()
        if not topics:
            bot.answer_callback_query(call.id, "Нет данных")
            return
        for t in topics:
            bot.send_message(
                call.message.chat.id,
                f"🔥 <b>{t['username']}</b>\n💬 {t['replies']} ответов\n\n{t['text']}",
                reply_markup=topic_keyboard(t["id"])
            )
        return

    if data.startswith("reply_"):
        topic_id = int(data.split("_")[1])
        set_state(user_id, "reply", {"topic_id": topic_id})
        bot.send_message(call.message.chat.id, "✍️ Напишите ответ")
        return

    if data.startswith("replies_"):
        _, topic_id, offset = data.split("_")
        replies = get_replies(int(topic_id), int(offset))
        if not replies:
            bot.answer_callback_query(call.id, "Ответов нет")
            return
        for r in replies:
            bot.send_message(
                call.message.chat.id,
                format_reply(r)
            )
        bot.send_message(
            call.message.chat.id,
            "Навигация:",
            reply_markup=replies_keyboard(int(topic_id), int(offset))
        )
        return

    if data == "profile":
        cmd_profile(call.message)
        return

    if data == "change_name":
        set_state(user_id, "change_name")
        bot.send_message(call.message.chat.id, "✏️ Введите новое имя")
        return

    if data == "toggle_notify":
        new = toggle_notifications(user_id)
        bot.send_message(
            call.message.chat.id,
            "🔔 Уведомления включены" if new else "🔕 Уведомления выключены"
        )
        return
# ============================================================
# Part 5/6 — Reports, Admin Panel, Moderation, Bans
# ============================================================

# ==================== REPORTS ====================

def create_report(topic_id: int, reporter_id: int, reason: str):
    with get_conn() as conn:
        cur = conn.cursor()
        cur.execute("""
            INSERT INTO reports (topic_id, reporter_id, reason)
            VALUES (%s,%s,%s)
        """, (topic_id, reporter_id, sanitize(reason)))


def get_pending_reports():
    with get_conn() as conn:
        cur = conn.cursor()
        cur.execute("""
            SELECT r.*, t.text, u.username
            FROM reports r
            JOIN topics t ON t.id = r.topic_id
            JOIN user_names u ON u.user_id = t.user_id
            WHERE r.status='pending'
            ORDER BY r.created_at ASC
        """)
        return cur.fetchall()


def resolve_report(report_id: int, action: str, admin_id: int):
    with get_conn() as conn:
        cur = conn.cursor()
        cur.execute("""
            UPDATE reports
            SET status='resolved',
                admin_action=%s,
                admin_id=%s,
                resolved_at=NOW()
            WHERE id=%s
        """, (action, admin_id, report_id))


# ==================== ADMIN CHECK ====================

def is_admin(user_id: int) -> bool:
    return user_id == ADMIN_ID


# ==================== ADMIN COMMANDS ====================

@bot.message_handler(commands=["admin"])
def admin_panel(message):
    if not is_admin(message.from_user.id):
        return

    kb = InlineKeyboardMarkup()
    kb.add(
        InlineKeyboardButton("🚩 Жалобы", callback_data="admin_reports"),
        InlineKeyboardButton("⛔ Активные баны", callback_data="admin_bans")
    )
    bot.send_message(
        message.chat.id,
        "🛠 <b>Админ-панель</b>",
        reply_markup=kb
    )


# ==================== ADMIN CALLBACKS ====================

@bot.callback_query_handler(func=lambda c: c.data.startswith("admin_"))
def admin_callbacks(call):
    if not is_admin(call.from_user.id):
        bot.answer_callback_query(call.id, "Нет доступа")
        return

    if call.data == "admin_reports":
        reports = get_pending_reports()
        if not reports:
            bot.send_message(call.message.chat.id, "Жалоб нет")
            return

        for r in reports:
            kb = InlineKeyboardMarkup()
            kb.add(
                InlineKeyboardButton("❌ Удалить тему", callback_data=f"admin_del_{r['topic_id']}_{r['id']}"),
                InlineKeyboardButton("⛔ Бан 7д", callback_data=f"admin_ban_{r['topic_id']}_{r['id']}_7"),
                InlineKeyboardButton("⛔ Бан 30д", callback_data=f"admin_ban_{r['topic_id']}_{r['id']}_30")
            )
            bot.send_message(
                call.message.chat.id,
                f"🚩 <b>Жалоба #{r['id']}</b>\n"
                f"Автор: {r['username']}\n"
                f"Причина: {r['reason']}\n\n"
                f"{r['text']}",
                reply_markup=kb
            )
        return

    if call.data == "admin_bans":
        with get_conn() as conn:
            cur = conn.cursor()
            cur.execute("""
                SELECT b.*, u.username
                FROM bans b
                JOIN user_names u ON u.user_id=b.user_id
                WHERE b.is_active=TRUE
            """)
            bans = cur.fetchall()

        if not bans:
            bot.send_message(call.message.chat.id, "Активных банов нет")
            return

        for b in bans:
            bot.send_message(
                call.message.chat.id,
                f"⛔ <b>{b['username']}</b>\n"
                f"Причина: {b['reason']}\n"
                f"До: {format_dt(b['unbanned_at'])}"
            )


# ==================== ADMIN ACTIONS ====================

@bot.callback_query_handler(func=lambda c: c.data.startswith("admin_del_"))
def admin_delete(call):
    if not is_admin(call.from_user.id):
        return

    _, _, topic_id, report_id = call.data.split("_")
    delete_topic(int(topic_id), admin=True)
    resolve_report(int(report_id), "deleted", call.from_user.id)
    bot.answer_callback_query(call.id, "Тема удалена")


@bot.callback_query_handler(func=lambda c: c.data.startswith("admin_ban_"))
def admin_ban(call):
    if not is_admin(call.from_user.id):
        return

    _, _, topic_id, report_id, days = call.data.split("_")
    topic = get_topic(int(topic_id))
    if topic:
        ban_user(topic["user_id"], "Нарушение правил", call.from_user.id, int(days))
    resolve_report(int(report_id), f"ban_{days}", call.from_user.id)
    bot.answer_callback_query(call.id, f"Бан на {days} дней")
# ============================================================
# Part 6/6 — Safety, Anti-crash, Railway-safe Run
# ============================================================

import sys
import traceback

# ==================== GLOBAL ERROR HANDLER ====================

def safe_handler(func):
    def wrapper(*args, **kwargs):
        try:
            return func(*args, **kwargs)
        except Exception as e:
            logger.error("❌ ERROR in handler")
            logger.error(traceback.format_exc())
    return wrapper


# ==================== APPLY SAFE HANDLER ====================

bot._notify_command_handlers = safe_handler(bot._notify_command_handlers)
bot._notify_message_handlers = safe_handler(bot._notify_message_handlers)
bot._notify_callback_query_handlers = safe_handler(bot._notify_callback_query_handlers)


# ==================== HEALTH CHECK ====================

def self_check():
    try:
        with get_conn() as conn:
            cur = conn.cursor()
            cur.execute("SELECT 1")
        bot.get_me()
        logger.info("✅ Self-check OK")
    except Exception as e:
        logger.critical("❌ Self-check failed")
        logger.critical(e)
        sys.exit(1)


# ==================== STARTUP ====================

if __name__ == "__main__":
    logger.info("🚀 Starting Telegram bot (PostgreSQL / Railway)")
    self_check()

    while True:
        try:
            bot.infinity_polling(
                timeout=60,
                long_polling_timeout=60,
                skip_pending=True
            )
        except KeyboardInterrupt:
            logger.info("🛑 Bot stopped manually")
            break
        except Exception as e:
            logger.error("🔥 Bot crashed, restarting in 5s")
            logger.error(e)
            time.sleep(5)
