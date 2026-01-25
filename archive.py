# ============================================================
# Telegram Anonymous Thoughts Bot
# PostgreSQL version (Railway compatible)
# Block 1/8 — Config, ENV, Logging, DB connection
# ============================================================

import os
import sys
import time
import logging
import traceback
from contextlib import contextmanager
from datetime import datetime, timedelta

import telebot
import psycopg2
import psycopg2.extras

# ===================== ENV =====================

BOT_TOKEN = os.getenv("BOT_TOKEN")
DATABASE_URL = os.getenv("DATABASE_URL")
ADMIN_ID = int(os.getenv("ADMIN_ID", "0"))

if not BOT_TOKEN:
    raise RuntimeError("BOT_TOKEN is not set")
if not DATABASE_URL:
    raise RuntimeError("DATABASE_URL is not set")

# ===================== CONSTANTS =====================

DAILY_TOPIC_LIMIT = 5
REPLIES_PAGE_SIZE = 5
TOPICS_PAGE_SIZE = 5
RECONNECT_DELAY = 5  # seconds

# ===================== LOGGING =====================

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s",
    stream=sys.stdout
)

logger = logging.getLogger("thoughts_bot")

logger.info("Starting bot process...")

# ===================== POSTGRES =====================

@contextmanager
def get_conn():
    """
    Safe PostgreSQL connection context manager.
    Auto-commit / rollback.
    """
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


def db_ping():
    with get_conn() as conn:
        cur = conn.cursor()
        cur.execute("SELECT 1")


# ===================== BOT INIT =====================

bot = telebot.TeleBot(
    BOT_TOKEN,
    parse_mode="HTML",
    disable_web_page_preview=True
)

# ===================== GLOBAL ERROR GUARD =====================

def safe_call(func):
    def wrapper(*args, **kwargs):
        try:
            return func(*args, **kwargs)
        except Exception:
            logger.error("Unhandled error:")
            logger.error(traceback.format_exc())
    return wrapper


bot._notify_command_handlers = safe_call(bot._notify_command_handlers)
bot._notify_message_handlers = safe_call(bot._notify_message_handlers)
bot._notify_callback_query_handlers = safe_call(bot._notify_callback_query_handlers)

# ============================================================
# Block 2/8 — Database schema (PostgreSQL)
# ============================================================

def init_db():
    """
    Create all tables and indexes if they do not exist.
    Fully PostgreSQL compatible.
    """
    with get_conn() as conn:
        cur = conn.cursor()

        # -------- USERS --------
        cur.execute("""
        CREATE TABLE IF NOT EXISTS users (
            user_id BIGINT PRIMARY KEY,
            created_at TIMESTAMP DEFAULT NOW(),
            last_active TIMESTAMP DEFAULT NOW(),
            is_admin BOOLEAN DEFAULT FALSE
        );
        """)

        # -------- USER NAMES --------
        cur.execute("""
        CREATE TABLE IF NOT EXISTS user_names (
            user_id BIGINT PRIMARY KEY REFERENCES users(user_id) ON DELETE CASCADE,
            username TEXT UNIQUE NOT NULL,
            updated_at TIMESTAMP DEFAULT NOW()
        );
        """)

        # -------- USER SETTINGS --------
        cur.execute("""
        CREATE TABLE IF NOT EXISTS user_settings (
            user_id BIGINT PRIMARY KEY REFERENCES users(user_id) ON DELETE CASCADE,
            notify_replies BOOLEAN DEFAULT TRUE,
            notify_system BOOLEAN DEFAULT TRUE,
            updated_at TIMESTAMP DEFAULT NOW()
        );
        """)

        # -------- USER STATS --------
        cur.execute("""
        CREATE TABLE IF NOT EXISTS user_stats (
            user_id BIGINT PRIMARY KEY REFERENCES users(user_id) ON DELETE CASCADE,
            topics_created INTEGER DEFAULT 0,
            replies_written INTEGER DEFAULT 0,
            replies_received INTEGER DEFAULT 0
        );
        """)

        # -------- DAILY LIMITS --------
        cur.execute("""
        CREATE TABLE IF NOT EXISTS daily_limits (
            user_id BIGINT REFERENCES users(user_id) ON DELETE CASCADE,
            date DATE NOT NULL,
            topics_created INTEGER DEFAULT 0,
            PRIMARY KEY (user_id, date)
        );
        """)

        # -------- TOPICS --------
        cur.execute("""
        CREATE TABLE IF NOT EXISTS topics (
            id SERIAL PRIMARY KEY,
            user_id BIGINT REFERENCES users(user_id) ON DELETE CASCADE,
            text TEXT NOT NULL,
            is_active BOOLEAN DEFAULT TRUE,
            created_at TIMESTAMP DEFAULT NOW()
        );
        """)

        # -------- REPLIES --------
        cur.execute("""
        CREATE TABLE IF NOT EXISTS replies (
            id SERIAL PRIMARY KEY,
            topic_id INTEGER REFERENCES topics(id) ON DELETE CASCADE,
            user_id BIGINT REFERENCES users(user_id) ON DELETE CASCADE,
            text TEXT NOT NULL,
            is_active BOOLEAN DEFAULT TRUE,
            created_at TIMESTAMP DEFAULT NOW()
        );
        """)

        # -------- REPORTS --------
        cur.execute("""
        CREATE TABLE IF NOT EXISTS reports (
            id SERIAL PRIMARY KEY,
            topic_id INTEGER REFERENCES topics(id) ON DELETE CASCADE,
            reporter_id BIGINT REFERENCES users(user_id) ON DELETE CASCADE,
            reason TEXT NOT NULL,
            status TEXT DEFAULT 'pending',
            admin_id BIGINT,
            created_at TIMESTAMP DEFAULT NOW(),
            resolved_at TIMESTAMP
        );
        """)

        # -------- BANS --------
        cur.execute("""
        CREATE TABLE IF NOT EXISTS bans (
            user_id BIGINT PRIMARY KEY REFERENCES users(user_id) ON DELETE CASCADE,
            reason TEXT NOT NULL,
            banned_at TIMESTAMP DEFAULT NOW(),
            unban_at TIMESTAMP,
            is_active BOOLEAN DEFAULT TRUE
        );
        """)

        # -------- INDEXES --------
        cur.execute("CREATE INDEX IF NOT EXISTS idx_topics_active ON topics(is_active);")
        cur.execute("CREATE INDEX IF NOT EXISTS idx_replies_topic ON replies(topic_id);")
        cur.execute("CREATE INDEX IF NOT EXISTS idx_reports_status ON reports(status);")

        logger.info("Database schema initialized")


# Initialize DB on startup
init_db()
# ============================================================
# Block 3/8 — Users, Names, Settings, Stats, Ranks, Bans
# ============================================================

import random
import re
import html

# ===================== HELPERS =====================

def sanitize(text: str) -> str:
    return html.escape(" ".join(text.strip().split()))


# ===================== USERS =====================

def ensure_user(user_id: int):
    with get_conn() as conn:
        cur = conn.cursor()
        cur.execute("""
            INSERT INTO users (user_id, is_admin)
            VALUES (%s, %s)
            ON CONFLICT (user_id) DO NOTHING
        """, (user_id, user_id == ADMIN_ID))

        cur.execute("""
            INSERT INTO user_stats (user_id)
            VALUES (%s)
            ON CONFLICT (user_id) DO NOTHING
        """)

        cur.execute("""
            INSERT INTO user_settings (user_id)
            VALUES (%s)
            ON CONFLICT (user_id) DO NOTHING
        """)


# ===================== USERNAMES =====================

def generate_username() -> str:
    while True:
        name = f"аноним_{random.randint(1000, 9999)}"
        with get_conn() as conn:
            cur = conn.cursor()
            cur.execute("SELECT 1 FROM user_names WHERE username=%s", (name,))
            if not cur.fetchone():
                return name


def get_username(user_id: int) -> str:
    ensure_user(user_id)
    with get_conn() as conn:
        cur = conn.cursor()
        cur.execute("SELECT username FROM user_names WHERE user_id=%s", (user_id,))
        row = cur.fetchone()
        if row:
            return row["username"]

        name = generate_username()
        cur.execute("""
            INSERT INTO user_names (user_id, username)
            VALUES (%s, %s)
        """, (user_id, name))
        return name


def validate_username(username: str):
    if not (3 <= len(username) <= 15):
        return False, "Имя должно быть от 3 до 15 символов"
    if not re.match(r'^[a-zA-Zа-яА-ЯёЁ0-9_]+$', username):
        return False, "Допустимы только буквы, цифры и _"
    return True, None


def set_username(user_id: int, username: str):
    ok, err = validate_username(username)
    if not ok:
        return False, err

    with get_conn() as conn:
        cur = conn.cursor()
        cur.execute("""
            SELECT 1 FROM user_names
            WHERE username=%s AND user_id!=%s
        """, (username, user_id))
        if cur.fetchone():
            return False, "Имя уже занято"

        cur.execute("""
            INSERT INTO user_names (user_id, username, updated_at)
            VALUES (%s,%s,NOW())
            ON CONFLICT (user_id)
            DO UPDATE SET username=EXCLUDED.username, updated_at=NOW()
        """, (user_id, username))
    return True, "Имя обновлено"


# ===================== SETTINGS =====================

def toggle_notify_replies(user_id: int) -> bool:
    with get_conn() as conn:
        cur = conn.cursor()
        cur.execute("""
            UPDATE user_settings
            SET notify_replies = NOT notify_replies,
                updated_at = NOW()
            WHERE user_id=%s
            RETURNING notify_replies
        """, (user_id,))
        return cur.fetchone()["notify_replies"]


def notify_replies_enabled(user_id: int) -> bool:
    with get_conn() as conn:
        cur = conn.cursor()
        cur.execute(
            "SELECT notify_replies FROM user_settings WHERE user_id=%s",
            (user_id,)
        )
        row = cur.fetchone()
        return row["notify_replies"] if row else True


# ===================== STATS =====================

def inc_stat(user_id: int, field: str):
    with get_conn() as conn:
        cur = conn.cursor()
        cur.execute(f"""
            UPDATE user_stats
            SET {field} = {field} + 1
            WHERE user_id=%s
        """, (user_id,))


def get_stats(user_id: int):
    ensure_user(user_id)
    with get_conn() as conn:
        cur = conn.cursor()
        cur.execute("SELECT * FROM user_stats WHERE user_id=%s", (user_id,))
        return cur.fetchone()


# ===================== RANKS =====================

RANKS = [
    ("👶 Новичок", 0),
    ("🧒 Посетитель", 5),
    ("👨 Участник", 15),
    ("👨‍💼 Активист", 30),
    ("👨‍🎓 Мыслитель", 60),
    ("👑 Легенда", 120),
]


def get_rank(stats) -> str:
    total = stats["topics_created"] + stats["replies_written"]
    current = RANKS[0][0]
    for name, limit in RANKS:
        if total >= limit:
            current = name
    return current


# ===================== BANS =====================

def is_banned(user_id: int) -> bool:
    with get_conn() as conn:
        cur = conn.cursor()
        cur.execute("""
            SELECT 1 FROM bans
            WHERE user_id=%s
              AND is_active=TRUE
              AND (unban_at IS NULL OR unban_at > NOW())
        """, (user_id,))
        return cur.fetchone() is not None


def ban_user(user_id: int, reason: str, days: int = None):
    until = datetime.utcnow() + timedelta(days=days) if days else None
    with get_conn() as conn:
        cur = conn.cursor()
        cur.execute("""
            INSERT INTO bans (user_id, reason, unban_at, is_active)
            VALUES (%s,%s,%s,TRUE)
            ON CONFLICT (user_id)
            DO UPDATE SET
                reason=EXCLUDED.reason,
                unban_at=EXCLUDED.unban_at,
                banned_at=NOW(),
                is_active=TRUE
        """, (user_id, reason, until))


def unban_user(user_id: int):
    with get_conn() as conn:
        cur = conn.cursor()
        cur.execute(
            "UPDATE bans SET is_active=FALSE WHERE user_id=%s",
            (user_id,)
        )
# ============================================================
# Block 4/8 — Daily limits, Topics, Replies, Notifications
# ============================================================

# ===================== DAILY LIMITS =====================

def get_daily_limit(user_id: int):
    today = datetime.utcnow().date()
    with get_conn() as conn:
        cur = conn.cursor()
        cur.execute("""
            SELECT topics_created
            FROM daily_limits
            WHERE user_id=%s AND date=%s
        """, (user_id, today))
        row = cur.fetchone()
        if not row:
            return DAILY_TOPIC_LIMIT, 0
        used = row["topics_created"]
        return max(0, DAILY_TOPIC_LIMIT - used), used


def inc_daily_limit(user_id: int):
    today = datetime.utcnow().date()
    with get_conn() as conn:
        cur = conn.cursor()
        cur.execute("""
            INSERT INTO daily_limits (user_id, date, topics_created)
            VALUES (%s,%s,1)
            ON CONFLICT (user_id, date)
            DO UPDATE SET topics_created = daily_limits.topics_created + 1
        """, (user_id, today))


# ===================== TOPICS =====================

def create_topic(user_id: int, text: str):
    ensure_user(user_id)

    if is_banned(user_id):
        return "banned"

    remaining, _ = get_daily_limit(user_id)
    if remaining <= 0:
        return "limit"

    text = sanitize(text)
    if len(text) < 5:
        return "short"

    with get_conn() as conn:
        cur = conn.cursor()
        cur.execute("""
            INSERT INTO topics (user_id, text)
            VALUES (%s,%s)
            RETURNING id
        """, (user_id, text))
        topic_id = cur.fetchone()["id"]

    inc_daily_limit(user_id)
    inc_stat(user_id, "topics_created")
    return topic_id


def get_topic(topic_id: int):
    with get_conn() as conn:
        cur = conn.cursor()
        cur.execute("""
            SELECT t.*, u.username
            FROM topics t
            JOIN user_names u ON u.user_id=t.user_id
            WHERE t.id=%s AND t.is_active=TRUE
        """, (topic_id,))
        return cur.fetchone()


def delete_topic(topic_id: int):
    with get_conn() as conn:
        cur = conn.cursor()
        cur.execute(
            "UPDATE topics SET is_active=FALSE WHERE id=%s",
            (topic_id,)
        )


# ===================== REPLIES =====================

def add_reply(topic_id: int, user_id: int, text: str):
    ensure_user(user_id)

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

        topic_author = row["user_id"]

        cur.execute("""
            INSERT INTO replies (topic_id, user_id, text)
            VALUES (%s,%s,%s)
        """, (topic_id, user_id, text))

    inc_stat(user_id, "replies_written")
    inc_stat(topic_author, "replies_received")

    # notify author
    if topic_author != user_id and notify_replies_enabled(topic_author):
        try:
            bot.send_message(
                topic_author,
                "💬 На вашу тему пришёл новый ответ"
            )
        except Exception:
            pass

    return True


def get_replies(topic_id: int, offset: int = 0, limit: int = REPLIES_PAGE_SIZE):
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
# ============================================================
# Block 5/8 — Feeds, Popular, Random, Pagination, Formatting
# ============================================================

from telebot.types import InlineKeyboardMarkup, InlineKeyboardButton

# ===================== FORMATTING =====================

def fmt_dt(dt):
    return dt.strftime("%d.%m.%Y %H:%M") if dt else "-"


def format_topic(topic):
    return (
        f"📝 <b>Тема #{topic['id']}</b>\n"
        f"👤 {topic['username']}\n"
        f"🕒 {fmt_dt(topic['created_at'])}\n\n"
        f"{topic['text']}"
    )


def format_reply(reply):
    return (
        f"💬 <b>{reply['username']}</b> "
        f"<i>{fmt_dt(reply['created_at'])}</i>\n"
        f"{reply['text']}"
    )


# ===================== FEEDS =====================

def get_feed(offset: int = 0, limit: int = TOPICS_PAGE_SIZE):
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


def get_popular(limit: int = 5):
    with get_conn() as conn:
        cur = conn.cursor()
        cur.execute("""
            SELECT t.id, t.text, u.username, COUNT(r.id) AS replies
            FROM topics t
            LEFT JOIN replies r
              ON r.topic_id=t.id AND r.is_active=TRUE
            JOIN user_names u ON u.user_id=t.user_id
            WHERE t.is_active=TRUE
            GROUP BY t.id, u.username
            ORDER BY replies DESC, t.created_at DESC
            LIMIT %s
        """, (limit,))
        return cur.fetchall()


def get_random_topic():
    with get_conn() as conn:
        cur = conn.cursor()
        cur.execute("""
            SELECT id FROM topics
            WHERE is_active=TRUE
            ORDER BY RANDOM()
            LIMIT 1
        """)
        row = cur.fetchone()
        return row["id"] if row else None


# ===================== KEYBOARDS =====================

def kb_topic(topic_id: int):
    kb = InlineKeyboardMarkup()
    kb.add(
        InlineKeyboardButton("💬 Ответить", callback_data=f"reply:{topic_id}"),
        InlineKeyboardButton("📖 Ответы", callback_data=f"replies:{topic_id}:0")
    )
    kb.add(
        InlineKeyboardButton("🚩 Пожаловаться", callback_data=f"report:{topic_id}")
    )
    return kb


def kb_feed(offset: int):
    kb = InlineKeyboardMarkup()
    if offset > 0:
        kb.add(InlineKeyboardButton("⬅️ Назад", callback_data=f"feed:{offset-TOPICS_PAGE_SIZE}"))
    kb.add(InlineKeyboardButton("➡️ Далее", callback_data=f"feed:{offset+TOPICS_PAGE_SIZE}"))
    return kb
# ============================================================
# Block 6/8 — Commands, States, Text Handling
# ============================================================

# ===================== USER STATES =====================

USER_STATE = {}  # user_id -> dict


def set_state(user_id: int, state: str, data: dict | None = None):
    USER_STATE[user_id] = {"state": state, "data": data or {}}


def clear_state(user_id: int):
    USER_STATE.pop(user_id, None)


def get_state(user_id: int):
    return USER_STATE.get(user_id)


# ===================== MAIN MENU =====================

def kb_main():
    kb = InlineKeyboardMarkup()
    kb.add(
        InlineKeyboardButton("📰 Лента", callback_data="feed:0"),
        InlineKeyboardButton("🎲 Случайная", callback_data="random")
    )
    kb.add(
        InlineKeyboardButton("🔥 Популярные", callback_data="popular"),
        InlineKeyboardButton("👤 Профиль", callback_data="profile")
    )
    return kb


# ===================== COMMANDS =====================

@bot.message_handler(commands=["start"])
def cmd_start(message):
    user_id = message.from_user.id
    ensure_user(user_id)
    username = get_username(user_id)

    bot.send_message(
        message.chat.id,
        f"👋 Привет, <b>{username}</b>!\n\n"
        "Напиши мысль — она станет темой.\n"
        "Или выбери действие 👇",
        reply_markup=kb_main()
    )


@bot.message_handler(commands=["profile"])
def cmd_profile(message):
    user_id = message.from_user.id
    stats = get_stats(user_id)
    rank = get_rank(stats)
    username = get_username(user_id)

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


# ===================== TEXT HANDLER =====================

@bot.message_handler(func=lambda m: True)
def on_text(message):
    user_id = message.from_user.id
    ensure_user(user_id)

    state = get_state(user_id)

    # ---- reply to topic ----
    if state and state["state"] == "reply":
        topic_id = state["data"]["topic_id"]
        clear_state(user_id)
        res = add_reply(topic_id, user_id, message.text)

        if res == "banned":
            bot.send_message(message.chat.id, "🚫 Вы заблокированы")
        elif res == "short":
            bot.send_message(message.chat.id, "⚠️ Ответ слишком короткий")
        elif res == "not_found":
            bot.send_message(message.chat.id, "❌ Тема не найдена")
        else:
            bot.send_message(message.chat.id, "✅ Ответ добавлен")
        return

    # ---- change username ----
    if state and state["state"] == "change_name":
        clear_state(user_id)
        ok, msg = set_username(user_id, message.text)
        bot.send_message(
            message.chat.id,
            ("✅ " if ok else "❌ ") + msg
        )
        return

    # ---- create topic ----
    res = create_topic(user_id, message.text)

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
            reply_markup=kb_topic(res)
        )
# ============================================================
# Block 7/8 — Callback queries, Feeds, Replies, Reports
# ============================================================

@bot.callback_query_handler(func=lambda c: True)
def on_callback(call):
    user_id = call.from_user.id
    ensure_user(user_id)

    data = call.data.split(":")
    action = data[0]

    # ===================== FEED =====================
    if action == "feed":
        offset = int(data[1])
        topics = get_feed(offset)

        if not topics:
            bot.answer_callback_query(call.id, "Больше тем нет")
            return

        for t in topics:
            bot.send_message(
                call.message.chat.id,
                format_topic(t),
                reply_markup=kb_topic(t["id"])
            )

        bot.send_message(
            call.message.chat.id,
            "⬇️ Навигация",
            reply_markup=kb_feed(offset)
        )

    # ===================== RANDOM =====================
    elif action == "random":
        topic_id = get_random_topic()
        if not topic_id:
            bot.answer_callback_query(call.id, "Тем пока нет")
            return

        topic = get_topic(topic_id)
        bot.send_message(
            call.message.chat.id,
            format_topic(topic),
            reply_markup=kb_topic(topic_id)
        )

    # ===================== POPULAR =====================
    elif action == "popular":
        topics = get_popular()
        if not topics:
            bot.answer_callback_query(call.id, "Пока пусто")
            return

        for t in topics:
            bot.send_message(
                call.message.chat.id,
                f"🔥 <b>{t['username']}</b>\n"
                f"💬 Ответов: {t['replies']}\n\n"
                f"{t['text']}",
                reply_markup=kb_topic(t["id"])
            )

    # ===================== REPLIES =====================
    elif action == "replies":
        topic_id = int(data[1])
        offset = int(data[2])
        replies = get_replies(topic_id, offset)

        if not replies:
            bot.answer_callback_query(call.id, "Ответов нет")
            return

        for r in replies:
            bot.send_message(
                call.message.chat.id,
                format_reply(r)
            )

    # ===================== REPLY =====================
    elif action == "reply":
        topic_id = int(data[1])
        set_state(user_id, "reply", {"topic_id": topic_id})
        bot.send_message(
            call.message.chat.id,
            "✍️ Напишите ответ:"
        )

    # ===================== PROFILE =====================
    elif action == "profile":
        cmd_profile(call.message)

    elif action == "change_name":
        set_state(user_id, "change_name")
        bot.send_message(
            call.message.chat.id,
            "✏️ Введите новое имя:"
        )

    elif action == "toggle_notify":
        state = toggle_notify_replies(user_id)
        bot.send_message(
            call.message.chat.id,
            "🔔 Уведомления: " + ("включены" if state else "выключены")
        )

    # ===================== REPORT =====================
    elif action == "report":
        topic_id = int(data[1])
        set_state(user_id, "report", {"topic_id": topic_id})
        bot.send_message(
            call.message.chat.id,
            "🚩 Укажите причину жалобы:"
        )

    bot.answer_callback_query(call.id)
# ============================================================
# Block 8/8 — Admin, Reports, Safe Polling, Railway
# ============================================================

# ===================== REPORT HANDLER =====================

@bot.message_handler(func=lambda m: get_state(m.from_user.id) and get_state(m.from_user.id)["state"] == "report")
def handle_report(message):
    user_id = message.from_user.id
    state = get_state(user_id)
    topic_id = state["data"]["topic_id"]
    clear_state(user_id)

    reason = sanitize(message.text)
    if len(reason) < 3:
        bot.send_message(message.chat.id, "⚠️ Причина слишком короткая")
        return

    with get_conn() as conn:
        cur = conn.cursor()
        cur.execute("""
            INSERT INTO reports (topic_id, reporter_id, reason)
            VALUES (%s,%s,%s)
        """, (topic_id, user_id, reason))

    bot.send_message(message.chat.id, "🚩 Жалоба отправлена")

    # notify admin
    if ADMIN_ID:
        try:
            bot.send_message(
                ADMIN_ID,
                f"🚨 <b>Новая жалоба</b>\n"
                f"Тема #{topic_id}\n"
                f"От: {get_username(user_id)}\n"
                f"Причина: {reason}"
            )
        except Exception:
            pass


# ===================== ADMIN COMMANDS =====================

def is_admin(user_id: int) -> bool:
    return user_id == ADMIN_ID


@bot.message_handler(commands=["ban"])
def cmd_ban(message):
    if not is_admin(message.from_user.id):
        return

    try:
        _, uid, days, *reason = message.text.split()
        ban_user(int(uid), " ".join(reason) or "ban", int(days))
        bot.send_message(message.chat.id, "⛔ Пользователь забанен")
    except Exception:
        bot.send_message(message.chat.id, "❌ Использование: /ban user_id days reason")


@bot.message_handler(commands=["unban"])
def cmd_unban(message):
    if not is_admin(message.from_user.id):
        return

    try:
        _, uid = message.text.split()
        unban_user(int(uid))
        bot.send_message(message.chat.id, "✅ Пользователь разбанен")
    except Exception:
        bot.send_message(message.chat.id, "❌ Использование: /unban user_id")


@bot.message_handler(commands=["stats"])
def cmd_stats(message):
    if not is_admin(message.from_user.id):
        return

    with get_conn() as conn:
        cur = conn.cursor()
        cur.execute("SELECT COUNT(*) AS c FROM users")
        users = cur.fetchone()["c"]
        cur.execute("SELECT COUNT(*) AS c FROM topics")
        topics = cur.fetchone()["c"]
        cur.execute("SELECT COUNT(*) AS c FROM replies")
        replies = cur.fetchone()["c"]

    bot.send_message(
        message.chat.id,
        f"📊 <b>Статистика</b>\n\n"
        f"👤 Пользователей: {users}\n"
        f"📝 Тем: {topics}\n"
        f"💬 Ответов: {replies}"
    )


# ===================== SAFE POLLING =====================

def run_bot():
    logger.info("Bot started polling")
    while True:
        try:
            bot.infinity_polling(
                timeout=30,
                long_polling_timeout=30
            )
        except Exception:
            logger.error("Polling crashed, restarting...")
            logger.error(traceback.format_exc())
            time.sleep(RECONNECT_DELAY)


if __name__ == "__main__":
    db_ping()
    run_bot()
