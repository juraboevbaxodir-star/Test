# ===============================================================
# 1. IMPORTS
# ===============================================================
import asyncio
import io
import json
import os
import random
import re
import time
import logging
import sqlite3
import dotenv
import aiosqlite
import qrcode
from aiohttp import web
from PIL import Image
from logging.handlers import RotatingFileHandler
from datetime import datetime, timezone
from typing import Dict, List, Optional, Any, Tuple

# ===============================================================
# 🤖 AIOGRAM KUTUBXONASI (v3.x / Bot API 9.4+)
# ===============================================================
from aiogram import Bot, Dispatcher, types, F
from aiogram.client.default import DefaultBotProperties
from aiogram.filters import Command, StateFilter
from aiogram.fsm.context import FSMContext
from aiogram.fsm.state import State, StatesGroup
from aiogram.fsm.storage.memory import MemoryStorage
from aiogram.types import (
    InlineKeyboardButton, InlineKeyboardMarkup,
    Message, CallbackQuery, ReplyKeyboardRemove,
    KeyboardButton, KeyboardButtonRequestChat, BufferedInputFile, InputMediaPhoto,
    FSInputFile
)
from aiogram.utils.keyboard import InlineKeyboardBuilder, ReplyKeyboardBuilder

# ===============================================================
# 📞 TELETHON USERBOT KUTUBXONASI
# ===============================================================
from telethon import TelegramClient, events, errors
from telethon.sessions import StringSession

# Monkey-patch Telethon TelegramClient _self_id property for compatibility
for cls in TelegramClient.__mro__:
    try:
        if hasattr(cls, '_self_id'):
            delattr(cls, '_self_id')
    except Exception:
        pass

def _get_self_id(self):
    return getattr(self, '_self_id_val', None)

def _set_self_id(self, val):
    self._self_id_val = val

setattr(TelegramClient, '_self_id', property(_get_self_id, _set_self_id))

from telethon.errors import FloodWaitError
from telethon.tl.types import (
    MessageEntityCustomEmoji, MessageEntityMentionName,
    InputMessageEntityMentionName, InputUser, ChannelParticipantsAdmins
)
from telethon.tl.functions.messages import SetTypingRequest, HideAllChatJoinRequestsRequest, GetChatInviteImportersRequest
from telethon.tl.types import SendMessageTypingAction, InputUserEmpty
from telethon.utils import get_display_name

# ===============================================================
# 2. CONFIG & ENVIRONMENT
# ===============================================================
dotenv.load_dotenv()

API_ID_RAW = os.getenv("API_ID")
API_HASH = os.getenv("API_HASH")
BOT_TOKEN = os.getenv("BOT_TOKEN")
OWNER_ID_RAW = os.getenv("OWNER_ID")

if not all([API_ID_RAW, API_HASH, BOT_TOKEN, OWNER_ID_RAW]):
    raise ValueError("Xatolik: .env faylida API_ID, API_HASH, BOT_TOKEN, OWNER_ID yetishmayapti!")

API_ID = int(API_ID_RAW)
OWNER_ID = int(OWNER_ID_RAW)
DB_FILE = os.getenv("DB_FILE", "database.db")
MAINTENANCE_MODE = False

# ===============================================================
# 3. LOGGING SETUP
# ===============================================================
os.makedirs("logs", exist_ok=True)
log = logging.getLogger("alpha_userbot")
log.setLevel(logging.INFO)

file_handler = RotatingFileHandler("logs/bot.log", maxBytes=5 * 1024 * 1024, backupCount=3, encoding="utf-8")
file_handler.setLevel(logging.INFO)
file_formatter = logging.Formatter("[%(asctime)s] %(levelname)s - %(name)s - %(message)s")
file_handler.setFormatter(file_formatter)

console_handler = logging.StreamHandler()
console_handler.setLevel(logging.INFO)
console_formatter = logging.Formatter("[%(asctime)s] %(levelname)s - %(message)s")
console_handler.setFormatter(console_formatter)

if not log.handlers:
    log.addHandler(file_handler)
    log.addHandler(console_handler)

logging.getLogger("aiogram").setLevel(logging.WARNING)
logging.getLogger("telethon").setLevel(logging.WARNING)

# ===============================================================
# 4. DATABASE LAYER
# ===============================================================
def check_db_health() -> bool:
    if not os.path.exists(DB_FILE):
        return False
    try:
        conn = sqlite3.connect(DB_FILE)
        cursor = conn.cursor()
        cursor.execute("PRAGMA quick_check;")
        res = cursor.fetchone()
        conn.close()
        return bool(res and res[0] == "ok")
    except Exception as e:
        log.error(f"DB health check error: {e}")
        return False

class SQLiteRecord:
    def __init__(self, row, columns):
        self._row = row
        self._columns = columns
        self._col_map = {col: i for i, col in enumerate(columns)} if columns else {}

    def __getitem__(self, key):
        if isinstance(key, int):
            return self._row[key]
        if key in self._col_map:
            return self._row[self._col_map[key]]
        raise KeyError(key)

    def __len__(self):
        return len(self._row)

    def __iter__(self):
        return iter(self._row)

class SQLiteResult:
    def __init__(self, rows, columns):
        self.rows = [SQLiteRecord(r, columns) for r in rows] if rows else []
        self.columns = columns or []

class SQLiteClient:
    def __init__(self, conn: aiosqlite.Connection):
        self._conn = conn

    async def execute(self, query: str, *args):
        if len(args) == 1 and isinstance(args[0], (tuple, list)):
            params = list(args[0])
        else:
            params = list(args)

        async with self._conn.execute(query, params) as cursor:
            rows = await cursor.fetchall()
            columns = [desc[0] for desc in cursor.description] if cursor.description else []
            await self._conn.commit()
            return SQLiteResult(rows, columns)

GLOBAL_DB_CONN: Optional[aiosqlite.Connection] = None

async def init_global_db_conn():
    global GLOBAL_DB_CONN
    if GLOBAL_DB_CONN is None:
        GLOBAL_DB_CONN = await aiosqlite.connect(DB_FILE)
        await GLOBAL_DB_CONN.execute("PRAGMA journal_mode=WAL;")
        await GLOBAL_DB_CONN.execute("PRAGMA synchronous=NORMAL;")
        await GLOBAL_DB_CONN.execute("PRAGMA busy_timeout=10000;")

async def close_global_db_conn():
    global GLOBAL_DB_CONN
    if GLOBAL_DB_CONN is not None:
        try:
            await GLOBAL_DB_CONN.close()
        except Exception:
            pass
        GLOBAL_DB_CONN = None

class SQLiteClientManager:
    async def __aenter__(self):
        if GLOBAL_DB_CONN is None:
            await init_global_db_conn()
        return SQLiteClient(GLOBAL_DB_CONN)

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        pass

def get_db_client():
    return SQLiteClientManager()

def row_to_dict(res, row_idx=0) -> dict:
    if not res or not res.rows or row_idx >= len(res.rows):
        return {}
    row = res.rows[row_idx]
    return {col: row[col] for col in res.columns}

async def init_db():
    async with get_db_client() as db:
        await db.execute("""
        CREATE TABLE IF NOT EXISTS users (
            id TEXT PRIMARY KEY,
            first_seen TEXT,
            last_seen TEXT,
            username TEXT,
            first_name TEXT,
            lang TEXT DEFAULT 'uz',
            terms_accepted INTEGER DEFAULT 0,
            exceptions TEXT DEFAULT '[]'
        )
        """)
        try:
            await db.execute("ALTER TABLE users ADD COLUMN lang TEXT DEFAULT 'uz'")
        except Exception:
            pass
        try:
            await db.execute("ALTER TABLE users ADD COLUMN terms_accepted INTEGER DEFAULT 0")
        except Exception:
            pass
        await db.execute("""
        CREATE TABLE IF NOT EXISTS user_sessions (
            user_id TEXT,
            account_id TEXT PRIMARY KEY,
            name TEXT,
            phone TEXT,
            session TEXT,
            status TEXT DEFAULT 'active',
            added_at TEXT
        )
        """)
        await db.execute("""
        CREATE TABLE IF NOT EXISTS user_random_words (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            user_id TEXT,
            word TEXT
        )
        """)
        await db.execute("""
        CREATE TABLE IF NOT EXISTS force_sub_channels (
            channel_id TEXT PRIMARY KEY,
            title TEXT,
            invite_link TEXT
        )
        """)
        await db.execute("""
        CREATE TABLE IF NOT EXISTS usage_stats (
            key TEXT PRIMARY KEY,
            value TEXT
        )
        """)
        await db.execute("""
        CREATE TABLE IF NOT EXISTS system_config (
            key TEXT PRIMARY KEY,
            value TEXT
        )
        """)
        await db.execute("""
        CREATE TABLE IF NOT EXISTS vip_users (
            user_id TEXT PRIMARY KEY,
            custom_name TEXT,
            is_skipped INTEGER DEFAULT 0,
            updated_at TEXT
        )
        """)
        await db.execute("""
        CREATE TABLE IF NOT EXISTS support_messages (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            user_id TEXT,
            message TEXT,
            status TEXT DEFAULT 'pending',
            created_at TEXT
        )
        """)

async def load_vip_users_cache():
    global VIP_USERS_CACHE
    async with get_db_client() as db:
        cursor = await db.execute("SELECT user_id, custom_name, is_skipped, updated_at FROM vip_users")
        VIP_USERS_CACHE = {
            str(row[0]): {
                "custom_name": row[1],
                "is_skipped": int(row[2]) if row[2] else 0,
                "updated_at": row[3]
            }
            for row in cursor.rows
        }

async def init_sqlite_db():
    await init_global_db_conn()
    await init_db()
    await load_vip_users_cache()

async def get_config(key: str, default_val: Any) -> Any:
    async with get_db_client() as db:
        cursor = await db.execute("SELECT value FROM system_config WHERE key = ?", (key,))
        row = (cursor.rows[0] if cursor.rows else None)
        if row:
            try:
                return json.loads(row[0])
            except Exception:
                return default_val
    return default_val

async def set_config(key: str, value: Any):
    async with get_db_client() as db:
        await db.execute("INSERT OR REPLACE INTO system_config (key, value) VALUES (?, ?)", (key, json.dumps(value)))

async def init_configs_if_none():
    if await get_config("global_random_words", None) is None:
        await set_config("global_random_words", ["Salom!", "Hormang!", "Qandaysiz?"])

async def inc_usage_stat(feature: str, count: int = 1):
    today_str = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    async with get_db_client() as db:
        cursor = await db.execute("SELECT value FROM usage_stats WHERE key = ?", (f"{feature}_total",))
        row = (cursor.rows[0] if cursor.rows else None)
        curr_total = int(row[0]) if row and str(row[0]).isdigit() else 0
        new_total = curr_total + count
        await db.execute("INSERT OR REPLACE INTO usage_stats (key, value) VALUES (?, ?)", (f"{feature}_total", str(new_total)))

        cursor = await db.execute("SELECT value FROM usage_stats WHERE key = ?", (f"{feature}_date",))
        date_row = (cursor.rows[0] if cursor.rows else None)
        last_date = date_row[0] if date_row else ""

        if last_date != today_str:
            curr_today = 0
            await db.execute("INSERT OR REPLACE INTO usage_stats (key, value) VALUES (?, ?)", (f"{feature}_date", today_str))
        else:
            cursor = await db.execute("SELECT value FROM usage_stats WHERE key = ?", (f"{feature}_today",))
            today_row = (cursor.rows[0] if cursor.rows else None)
            curr_today = int(today_row[0]) if today_row and str(today_row[0]).isdigit() else 0

        new_today = curr_today + count
        await db.execute("INSERT OR REPLACE INTO usage_stats (key, value) VALUES (?, ?)", (f"{feature}_today", str(new_today)))

async def get_usage_stats() -> dict:
    today_str = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    res = {"utag_total": 0, "utag_today": 0, "scraper_total": 0, "scraper_today": 0}
    async with get_db_client() as db:
        cursor = await db.execute("SELECT key, value FROM usage_stats")
        data = {row[0]: row[1] for row in cursor.rows} if cursor.rows else {}

    for feature in ["utag", "scraper"]:
        tot_val = data.get(f"{feature}_total", "0")
        res[f"{feature}_total"] = int(tot_val) if str(tot_val).isdigit() else 0

        last_date = data.get(f"{feature}_date", "")
        if last_date == today_str:
            tod_val = data.get(f"{feature}_today", "0")
            res[f"{feature}_today"] = int(tod_val) if str(tod_val).isdigit() else 0
        else:
            res[f"{feature}_today"] = 0

    return res

# ===============================================================
# 5. CACHES & GLOBAL STATE
# ===============================================================
account_flood_until = {}
_running_tasks = set()
_tasks = {}
active_utag_sessions = {}
active_user_scrappers = {}
userbot_clients = {}
account_user_cache = {}
qr_login_tasks = {}
USER_RAM_CACHE: Dict[int, tuple[dict, float]] = {}
VIP_USERS_CACHE: Dict[str, dict] = {}

bot_username = "AlphaUserBot"

# ===============================================================
# 6. UTILITY FUNCTIONS & HELPERS
# ===============================================================
PREMIUM_EMOJIS = {
    "💳": "5936017305585586269",
    "📝": "5886330010054168711",
    "📋": "5956561916573782596",
    "🛠": "5988023995125993550",
    "🆔": "5936170807716745162",
    "🔰": "5884366771913233289",
    "🛑": "5258362429389152256",
    "📱": "5897567714674741148",
    "⚡": "5877434570143436450",
    "📩": "5967280668885913944",
    "📲": "6005775159384870794",
    "📌": "5085066469000086268",
    "🌟": "5323509305083706755",
    "🛒": "5258024802010026053",
    "🔢": "5226513232549664618",
    "⏱": "5258258882022612173",
    "🎲": "5454419255430767770",
    "🕒": "5778605968208170641",
    "⚔️": "5454172148782359440",
    "💡": "5258466217273871977",
    "🔙": "5352759161945867747",
    "🔄": "5877410604225924969",
    "⚙️": "4904936030232117798",
    "💾": "5235476906605826199",
    "👤": "5316992572680320646",
    "🛡": "5776159202649051327",
    "🚀": "5372917041193828849",
    "📥": "5877307202888273539",
    "➕": "5258108352008823107",
    "📂": "5341492148468465410",
    "📣": "5377842294940522088",
    "✈️": "5875465628285931233",
    "✅": "5985596818912712352",
    "◀️": "5440542387896070668",
    "🤖": "5931415565955503486",
    "👥": "5258486128742244085",
    "📢": "5771695636411847302",
    "🗡️": "5454172148782359440",
    "💎": "6006074913742396956",
    "🎁": "5985472565508838112",
    "🎞": "5775981206319402773",
    "🎞️": "5996789164383669947",
    "🗑": "5258130763148172425",
    "❌": "5985346521103604145",
    "👑": "5229027828527309057",
    "📊": "5875291072225087249",
    "💬": "5260535596941582167",
    "🔒": "5879895758202735862",
    "🆕": "5084601586034934481",
    "🟢": "5339112148175959615",
    "🔴": "5337017423906226569",
    "🚫": "5877413297170419326",
    "⚠️": "4915853119839011973",
    "🚷": "5872829476143894491",
    "🎉": "5994502837327892086",
    "✨": "5084990757316592457",
    "⌚️": "6237618005124064591",
    "💵": "5974217466270716579",
    "✉️": "5967280668885913944",
    "⏳": "5359523305940344104",
    "📖": "5363839159662306163",
    "🎯": "6032949275732742941",
    "📛": "5472267631979405211",
    "🔗": "5316612764427367709",
    "🕓": "5776213190387961618",
    "💰": "5897958754267174109",
    "🔑": "6005570495603282482",
    "☎️": "5967591100532134862",
    "📷": "6050592962730005028",
    "🔔": "4915820259044230152",
    "🏴‍☠️": "5116309444090661129",
    "📦": "5258134813302332906",
    "📨": "5258241045023463141",
    "🛍": "5348181001686235664",
    "🔥": "5116414868357907335",
    "⚡️": "5085022089103016925",
    "✏️": "5879841310902324730",
    "⏲": "5258258882022612173",
    "👇": "5834634435059782866",
    "1️⃣": "5798659067433980717",
    "2⃣": "5794303034292968945",
    "🏷": "5854776233950188167",
    "⭐️": "5087210087177519843",
    "🖼": "5931629923478278721",
    "🌐": "5879585266426973039",
    "❓": "5375505995940256340",
    "☑️": "5805532930662996322",
}
def f_t(text: str) -> str:
    if not text:
        return ""
    text = re.sub(r'"""(.*?)"""', r'<blockquote>\1</blockquote>', text, flags=re.DOTALL)
    text = re.sub(r'\*\*(.*?)\*\*', r'<b>\1</b>', text, flags=re.DOTALL)
    text = re.sub(r'`([^`]+)`', r'<code>\1</code>', text)
    text = re.sub(r'_(.*?)_', r'<i>\1</i>', text, flags=re.DOTALL)
    text = re.sub(r'\[premium:(\d+)\]', r'<tg-emoji emoji-id="\1">✨</tg-emoji>', text)
    return text

def clean_emojis(text: str) -> str:
    def is_emoji(c):
        o = ord(c)
        return (
            0x1F000 <= o <= 0x1FFFF or
            0x2600 <= o <= 0x27BF or
            0x2B00 <= o <= 0x2BFF or
            0xFE00 <= o <= 0xFE0F or
            0x200D == o or 0x200C == o or
            0x2300 <= o <= 0x23FF or
            0x1F300 <= o <= 0x1F9FF or
            0x1FA00 <= o <= 0x1FAFF
        )
    return "".join(c for c in text if not is_emoji(c)).strip()

def colored_btn(
    text: str,
    callback_data: str = None,
    style: str = "success",
    emoji: Optional[str] = None,
    url: Optional[str] = None
) -> InlineKeyboardButton:
    icon_id = None
    if emoji and emoji in PREMIUM_EMOJIS:
        icon_id = PREMIUM_EMOJIS[emoji]
    else:
        for em, eid in PREMIUM_EMOJIS.items():
            if em in text or (emoji and em in emoji):
                icon_id = eid
                break

    label = clean_emojis(text) if icon_id else text
    if not label:
        label = text

    kwargs = {"text": label}
    if style in ["success", "danger", "primary"]:
        kwargs["style"] = style
    if icon_id:
        kwargs["icon_custom_emoji_id"] = icon_id
    if url:
        kwargs["url"] = url
    elif callback_data:
        kwargs["callback_data"] = callback_data
    else:
        kwargs["callback_data"] = "noop"

    return InlineKeyboardButton(**kwargs)

def format_user_error(e: Exception) -> str:
    err_str = str(e).lower()
    if isinstance(e, (errors.PhoneCodeInvalidError, errors.PhoneCodeExpiredError, errors.PhoneCodeEmptyError)):
        return "Kod noto'g'ri yoki eskirgan. Qayta urinib ko'ring."
    elif isinstance(e, (errors.PasswordHashInvalidError, errors.SessionPasswordNeededError)):
        return "Parol noto'g'ri. Qayta urinib ko'ring."
    elif isinstance(e, (errors.FloodWaitError, errors.FloodError, errors.PhoneNumberBannedError)):
        return "⚠️ Juda ko'p urinish qilindi. Biroz kutib qayta urinib ko'ring."
    else:
        return f"❌ Xatolik yuz berdi: {e}"

def make_text_unique(text: str) -> str:
    if not text:
        return ""
    invisible_chars = ["\u200c", "\u200d", "\u200b"]
    return f"{text}{''.join(random.choices(invisible_chars, k=random.randint(2, 6)))}"

async def send_typing_action(client, chat_id):
    try:
        await client(SetTypingRequest(peer=chat_id, action=SendMessageTypingAction()))
    except Exception:
        pass

def parse_telegram_link(link_or_input: str):
    if not link_or_input:
        return None
    s = str(link_or_input).strip()
    c_match = re.search(r't\.me/c/(\d+)', s)
    if c_match:
        return int(f"-100{c_match.group(1)}")
    u_match = re.search(r't\.me/([a-zA-Z0-9_]{5,32})', s)
    if u_match:
        uname = u_match.group(1)
        if uname.lower() not in ['joinchat', 'addlist', 'c']:
            return f"@{uname}"
    if s.startswith("@"):
        return s
    if s.lstrip("-").isdigit():
        return int(s)
    return s

async def safe_get_entity(client: TelegramClient, target: Any):
    parsed = parse_telegram_link(target)
    try:
        await client.get_dialogs(limit=100)
    except Exception:
        pass
    return await client.get_entity(parsed)

def get_account_flood_remaining(aid: str) -> float:
    until = account_flood_until.get(str(aid), 0)
    now = time.time()
    if until > now:
        return until - now
    return 0.0

# ===============================================================
# 7. USER & SESSION MANAGEMENT HELPERS
# ===============================================================
async def ensure_user(user_id: int, username: str = "", first_name: str = "") -> dict:
    uid = str(user_id)
    int_uid = int(user_id)
    now_ts = time.time()

    if int_uid in USER_RAM_CACHE:
        cached_data, exp_ts = USER_RAM_CACHE[int_uid]
        if now_ts < exp_ts:
            return cached_data

    async with get_db_client() as db:
        cursor = await db.execute("SELECT * FROM users WHERE id = ?", (uid,))
        row = (cursor.rows[0] if cursor.rows else None)
        now = datetime.now(timezone.utc).isoformat()

        if not row:
            await db.execute("""
            INSERT INTO users (id, first_seen, last_seen, username, first_name, lang, terms_accepted)
            VALUES (?, ?, ?, ?, ?, 'uz', 0)
            """, (uid, now, now, username or "", first_name or ""))
            cursor = await db.execute("SELECT * FROM users WHERE id = ?", (uid,))
            row = (cursor.rows[0] if cursor.rows else None)

        user_data = row_to_dict(cursor, 0)
        user_data["exceptions"] = json.loads(user_data["exceptions"]) if isinstance(user_data.get("exceptions"), str) else (user_data.get("exceptions") or [])

        await db.execute("UPDATE users SET last_seen = ?, username = ?, first_name = ? WHERE id = ?", (now, username or "", first_name or "", uid))

    USER_RAM_CACHE[int_uid] = (user_data, now_ts + 60.0)
    return user_data

def invalidate_user_cache(user_id: int | str):
    try:
        int_uid = int(user_id)
        USER_RAM_CACHE.pop(int_uid, None)
    except (ValueError, TypeError):
        pass

async def update_user_db(uid: str | int, updates: dict) -> bool:
    if not updates:
        return True
    str_uid = str(uid)
    try:
        async with get_db_client() as db:
            set_clause = []
            params = []
            for k, v in updates.items():
                if isinstance(v, (dict, list)):
                    v_str = json.dumps(v)
                else:
                    v_str = v
                set_clause.append(f"{k} = ?")
                params.append(v_str)
            params.append(str_uid)
            query = f"UPDATE users SET {', '.join(set_clause)} WHERE id = ?"
            await db.execute(query, tuple(params))

        invalidate_user_cache(str_uid)
        return True
    except Exception as e:
        log.error(f"DB update error for user {str_uid}: {e}")
        return False

async def normalize_phone_number(phone: str) -> str:
    if not phone:
        return ""
    digits = re.sub(r'\D', '', str(phone))
    return f"+{digits}" if digits else ""

async def is_phone_already_connected(phone: str) -> bool:
    norm_phone = await normalize_phone_number(phone)
    if not norm_phone:
        return False
    async with get_db_client() as db:
        cursor = await db.execute("SELECT phone FROM user_sessions WHERE status = 'active'")
        for row in cursor.rows:
            db_phone = row[0] if row and row[0] else ""
            if await normalize_phone_number(db_phone) == norm_phone:
                return True
    return False

async def has_active_account(user_id: int | str) -> bool:
    str_uid = str(user_id)
    async with get_db_client() as db:
        cursor = await db.execute("SELECT COUNT(*) FROM user_sessions WHERE user_id = ? AND status = 'active'", (str_uid,))
        cnt = (cursor.rows[0][0] if cursor.rows else 0)
    return cnt > 0

async def handle_session_revocation(user_id: int | str, account_id: str, phone: str, name: str, reason: str):
    """
    Seans bekor bo'lganda (AuthKeyUnregistered, SessionRevoked, UserDeactivated)
    seansni MB va keshdan darhol tozalaydi va loglanishiga yo'l qo'ymaydi.
    """
    str_uid = str(user_id)
    log.warning(f"⚠️ [Session Revocation] Deleting revoked session for user {str_uid}, acc {account_id}. Reason: {reason}")
    async with get_db_client() as db:
        await db.execute("DELETE FROM user_sessions WHERE account_id = ?", (account_id,))

    account_user_cache.pop(account_id, None)

    if account_id in userbot_clients:
        cl = userbot_clients.pop(account_id, None)
        if cl:
            try:
                await cl.disconnect()
            except Exception:
                pass

    try:
        await bot.send_message(
            int(user_id),
            f_t("⚠️ <b>Userbot seansingiz uzildi!</b>\n\n"
                "Sizning userbot seansingiz bekor qilindi va tizimdan o'chirildi.\n"
                "Qayta ulash uchun bot menyusidan foydalaning.")
        )
    except Exception:
        pass

def is_revoked_session_error(e: Exception) -> bool:
    if isinstance(e, (errors.AuthKeyUnregisteredError, errors.UserDeactivatedError, errors.SessionRevokedError)):
        return True
    if hasattr(errors, "AuthKeyDuplicatedError") and isinstance(e, getattr(errors, "AuthKeyDuplicatedError")):
        return True
    if hasattr(errors, "AuthKeyInvalidError") and isinstance(e, getattr(errors, "AuthKeyInvalidError")):
        return True
    err_str = str(e).lower()
    if "authorization key was used" in err_str or "auth_key_duplicated" in err_str or "session_revoked" in err_str or "user_deactivated" in err_str:
        return True
    return False

async def health_check_session(session_str: str, account_id: str) -> str:
    client = TelegramClient(StringSession(session_str), API_ID, API_HASH)
    try:
        await asyncio.wait_for(client.connect(), timeout=8.0)
        if not await asyncio.wait_for(client.is_user_authorized(), timeout=8.0):
            return "revoked"
        me = await asyncio.wait_for(client.get_me(), timeout=8.0)
        if not me:
            return "revoked"
        return "ok"
    except Exception as e:
        if is_revoked_session_error(e):
            return "revoked"
        return "network_error"
    finally:
        try:
            await client.disconnect()
        except Exception:
            pass

# ===============================================================
# 8. TELETHON USERBOT ENGINE
# ===============================================================
async def start_userbot(acc_id: str, session_str: str, user_id: int):
    if acc_id in userbot_clients:
        existing_client = userbot_clients.pop(acc_id, None)
        if existing_client:
            try:
                await existing_client.disconnect()
            except Exception:
                pass

    client = TelegramClient(StringSession(session_str), API_ID, API_HASH)
    try:
        await asyncio.wait_for(client.connect(), timeout=15.0)
        is_auth = await asyncio.wait_for(client.is_user_authorized(), timeout=15.0)

        if not is_auth:
            await handle_session_revocation(user_id, acc_id, "", "", "SessionNotAuthorized")
            try:
                await client.disconnect()
            except Exception:
                pass
            return None

        me = await asyncio.wait_for(client.get_me(), timeout=15.0)
        if not me:
            await handle_session_revocation(user_id, acc_id, "", "", "GetMeEmpty")
            try:
                await client.disconnect()
            except Exception:
                pass
            return None

        client._self_id = me.id
        userbot_clients[acc_id] = client
        account_user_cache[acc_id] = (user_id, time.time() + 300.0)

        await register_userbot_handlers(client)
        return client

    except Exception as e:
        try:
            await client.disconnect()
        except Exception:
            pass

        if is_revoked_session_error(e):
            await handle_session_revocation(user_id, acc_id, "", "", f"{type(e).__name__}: {e}")
            raise e
        log.error(f"Userbot startup error for acc {acc_id}: {e}")
        raise e

async def auto_activate_all_userbots():
    try:
        async with get_db_client() as db:
            cursor = await db.execute("SELECT account_id, session, user_id FROM user_sessions WHERE status = 'active'")
            rows = cursor.rows
    except Exception as e:
        log.error(f"Error fetching active userbot sessions: {e}")
        return

    if not rows:
        return

    log.info(f"🩺 [Phase 1: Pre-check] {len(rows)} ta seans tekshirilmoqda...")
    passed_sessions = []

    for row in rows:
        acc_id, sess_str, user_id = row[0], row[1], int(row[2])
        await asyncio.sleep(0.5)
        hc_status = await health_check_session(sess_str, acc_id)

        if hc_status == "revoked":
            await handle_session_revocation(user_id, acc_id, "", "", "HealthCheckRevoked")
        else:
            passed_sessions.append((acc_id, sess_str, user_id))

    for acc_id, sess_str, user_id in passed_sessions:
        try:
            await start_userbot(acc_id, sess_str, user_id)
            await asyncio.sleep(0.8)
        except Exception as e:
            log.error(f"Auto-activate error for account {acc_id}: {e}")

async def userbot_watchdog_loop():
    semaphore = asyncio.Semaphore(1)
    while True:
        try:
            async with get_db_client() as db:
                cursor = await db.execute("SELECT account_id, session, user_id FROM user_sessions WHERE status = 'active'")
                rows = cursor.rows

            disconnected_items = []
            for acc_id, session_str, user_id_str in rows:
                if not session_str:
                    continue
                acc_id_str = str(acc_id)
                user_id = int(user_id_str)
                client = userbot_clients.get(acc_id_str)

                needs_reconnect = False
                if client is None or not client.is_connected():
                    needs_reconnect = True
                else:
                    try:
                        me = await asyncio.wait_for(client.get_me(), timeout=8.0)
                        if not me:
                            needs_reconnect = True
                    except Exception as rev_err:
                        if is_revoked_session_error(rev_err):
                            userbot_clients.pop(acc_id_str, None)
                            try:
                                await client.disconnect()
                            except Exception:
                                pass
                            await handle_session_revocation(user_id, acc_id_str, "", "", f"{type(rev_err).__name__}: {rev_err}")
                            continue
                        needs_reconnect = True

                if needs_reconnect:
                    if client:
                        userbot_clients.pop(acc_id_str, None)
                        try:
                            await client.disconnect()
                        except Exception:
                            pass
                    disconnected_items.append((acc_id_str, session_str, user_id))

            for acc_id_str, session_str, user_id in disconnected_items:
                async with semaphore:
                    try:
                        await start_userbot(acc_id_str, session_str, user_id)
                    except Exception as e:
                        log.error(f"🐶 [Watchdog] Failed reconnecting {acc_id_str}: {e}")

        except asyncio.CancelledError:
            raise
        except Exception as e:
            log.error(f"🐶 [Watchdog] Watchdog loop error: {e}")

        await asyncio.sleep(120)

async def auto_save_task():
    while True:
        await asyncio.sleep(300)
        now = time.time()
        try:
            expired_keys = [uid for uid, (_, exp_ts) in list(USER_RAM_CACHE.items()) if now >= exp_ts]
            for uid in expired_keys:
                USER_RAM_CACHE.pop(uid, None)
        except Exception as e:
            log.error(f"USER_RAM_CACHE janitor error: {e}")

        try:
            exp_flood = [aid for aid, until_ts in list(account_flood_until.items()) if now >= until_ts]
            for aid in exp_flood:
                account_flood_until.pop(aid, None)
        except Exception as e:
            log.error(f"account_flood_until janitor error: {e}")

# ===============================================================
# 9. TELETHON USERBOT HANDLERS (.u, .ru, .su, .blok, .unblok)
# ===============================================================
async def register_userbot_handlers(client: TelegramClient):

    @client.on(events.NewMessage(pattern=r"^\.u(?:tag)?\b.*$", outgoing=True))
    async def userbot_u(event):
        me_obj = await client.get_me()
        sender_id = me_obj.id
        cid = event.chat_id

        session_key = (cid, sender_id, "u")
        existing_task = _tasks.get(session_key)
        if existing_task and not existing_task.done():
            return

        try:
            await event.delete()
        except Exception:
            pass

        raw_text = event.message.message or ""
        entities = event.message.entities or []
        encoded_text = telethon_to_premium_encoded(raw_text, entities)

        m = re.match(r"^(\.u(tag)?\b)\s*", encoded_text, re.IGNORECASE)
        if not m: return
        suffix = encoded_text[m.end():]

        task = asyncio.create_task(utag_loop_telethon(cid, sender_id, suffix, client))
        _running_tasks.add(task)
        task.add_done_callback(_running_tasks.discard)
        _tasks[session_key] = task

    @client.on(events.NewMessage(pattern=r"^\.ru\b.*$", outgoing=True))
    async def userbot_ru(event):
        me_obj = await client.get_me()
        sender_id = me_obj.id
        cid = event.chat_id

        session_key = (cid, sender_id, "ru")
        existing_task = _tasks.get(session_key)
        if existing_task and not existing_task.done():
            return

        try:
            await event.delete()
        except Exception:
            pass

        raw_text = event.message.message or ""
        entities = event.message.entities or []
        encoded_text = telethon_to_premium_encoded(raw_text, entities)

        m = re.match(r"^(\.ru\b)\s*", encoded_text, re.IGNORECASE)
        if not m: return
        suffix = encoded_text[m.end():]

        task = asyncio.create_task(rutag_loop_telethon(cid, sender_id, suffix, client))
        _running_tasks.add(task)
        task.add_done_callback(_running_tasks.discard)
        _tasks[session_key] = task

    @client.on(events.NewMessage(pattern=r"^/zayaf(?:\s+|$)"))
    async def userbot_zayaf(event):
        me_obj = await client.get_me()
        sender_id = me_obj.id
        if event.sender_id != sender_id and not event.outgoing:
            return

        try:
            peer = await event.get_input_chat()
            count = 0
            try:
                res_importers = await client(GetChatInviteImportersRequest(
                    peer=peer,
                    requested=True,
                    limit=100,
                    offset_user=InputUserEmpty(),
                    offset_date=None
                ))
                count = getattr(res_importers, 'count', len(getattr(res_importers, 'importers', [])))
            except Exception:
                count = 0

            await client(HideAllChatJoinRequestsRequest(
                peer=peer,
                approved=True
            ))
            if count > 0:
                await event.reply(f"✅ {count} ta kutilayotgan a'zolik so'rovlari (zayafkalar) qabul qilindi.")
            else:
                await event.reply("✅ Barcha kutilayotgan a'zolik so'rovlari (zayafkalar) qabul qilindi.")
        except Exception as e:
            try:
                await event.reply(f"❌ Zayafkalarni tasdiqlashda xatolik: {e}")
            except Exception:
                pass

    @client.on(events.NewMessage(pattern=r"^\.(?:stop|su)$"))
    async def userbot_stop_process(event):
        me_obj = await client.get_me()
        sender_id = me_obj.id
        if event.sender_id != sender_id and not event.outgoing:
            return

        try:
            await event.delete()
        except Exception:
            pass

        cid = event.chat_id
        session_key_u = (cid, sender_id, "u")
        session_key_ru = (cid, sender_id, "ru")

        t1 = _tasks.pop(session_key_u, None)
        if t1 and not t1.done(): t1.cancel()
        t2 = _tasks.pop(session_key_ru, None)
        if t2 and not t2.done(): t2.cancel()

        async with get_db_client() as db:
            cursor = await db.execute("SELECT user_id FROM user_sessions WHERE account_id = ?", (str(sender_id),))
            row = (cursor.rows[0] if cursor.rows else None)
        if row:
            master_uid = int(row[0])
            session = active_utag_sessions.get(master_uid)
            if session:
                session["status"] = "stopped"
                for task in session.get("workers", []):
                    if not task.done(): task.cancel()
                active_utag_sessions.pop(master_uid, None)

    @client.on(events.NewMessage(pattern=r"^\.blok\b(.*)?$", outgoing=True))
    async def userbot_blok(event):
        me_obj = await client.get_me()
        acc_id = str(me_obj.id)
        owner_id = None

        async with get_db_client() as db:
            cursor = await db.execute("SELECT user_id FROM user_sessions WHERE account_id = ?", (acc_id,))
            row = (cursor.rows[0] if cursor.rows else None)
        if row:
            owner_id = row[0]
        if not owner_id: return

        target_id = None
        reply = await event.get_reply_message()
        if reply:
            target_id = reply.sender_id
        else:
            args = (event.pattern_match.group(1) or "").strip()
            if args:
                try:
                    if args.isdigit():
                        target_id = int(args)
                    else:
                        entity = await client.get_entity(args)
                        target_id = entity.id
                except Exception:
                    await event.edit("❌ Foydalanuvchi topilmadi.")
                    return
        if not target_id:
            await event.edit("❌ Reply yuboring yoki ID yozing.")
            return

        u = await ensure_user(int(owner_id))
        exceptions = u.get("exceptions", [])
        if target_id not in exceptions:
            exceptions.append(target_id)
            await update_user_db(owner_id, {"exceptions": exceptions})
            await event.edit(f"✅ ID: `{target_id}` istisnolarga qo'shildi.")
        else:
            await event.edit("⚠️ Foydalanuvchi allaqachon istisnolarda mavjud.")

    @client.on(events.NewMessage(pattern=r"^\.unblok\b(.*)?$", outgoing=True))
    async def userbot_unblok(event):
        me_obj = await client.get_me()
        acc_id = str(me_obj.id)
        owner_id = None

        async with get_db_client() as db:
            cursor = await db.execute("SELECT user_id FROM user_sessions WHERE account_id = ?", (acc_id,))
            row = (cursor.rows[0] if cursor.rows else None)
        if row:
            owner_id = row[0]
        if not owner_id: return

        target_id = None
        reply = await event.get_reply_message()
        if reply:
            target_id = reply.sender_id
        else:
            args = (event.pattern_match.group(1) or "").strip()
            if args:
                try:
                    if args.isdigit():
                        target_id = int(args)
                    else:
                        entity = await client.get_entity(args)
                        target_id = entity.id
                except Exception:
                    await event.edit("❌ Foydalanuvchi topilmadi.")
                    return
        if not target_id:
            await event.edit("❌ Reply yuboring yoki ID yozing.")
            return

        u = await ensure_user(int(owner_id))
        exceptions = u.get("exceptions", [])
        if target_id in exceptions:
            exceptions.remove(target_id)
            await update_user_db(owner_id, {"exceptions": exceptions})
            await event.edit(f"✅ ID: `{target_id}` istisnolardan olib tashlandi.")
        else:
            await event.edit("⚠️ Foydalanuvchi istisnolar ro'yxatida yo'q edi.")

# ===============================================================
# AUTO UTAG ENGINE & PREMIUM EMOJI ENCODERS
# ===============================================================
def utf16_offset_to_char_idx(s: str, utf16_offset: int) -> int:
    utf16_len = 0
    for idx, char in enumerate(s):
        if utf16_len >= utf16_offset:
            return idx
        char_utf16_len = len(char.encode('utf-16-le')) // 2
        utf16_len += char_utf16_len
    return len(s)

def telethon_to_premium_encoded(text: str, entities: list) -> str:
    if not text or not entities:
        return text or ""
    sorted_ents = sorted(entities, key=lambda e: e.offset, reverse=True)
    for ent in sorted_ents:
        if isinstance(ent, MessageEntityCustomEmoji):
            start_char = utf16_offset_to_char_idx(text, ent.offset)
            end_char = utf16_offset_to_char_idx(text, ent.offset + ent.length)
            emoji_code = f"[premium:{ent.document_id}]"
            text = text[:start_char] + emoji_code + text[end_char:]
    return text

def decode_premium_emojis(encoded_text: str):
    pattern = r"\[premium:(\d+)\]"
    entities = []
    while True:
        match = re.search(pattern, encoded_text)
        if not match:
            break
        doc_id = int(match.group(1))
        start_idx = len(encoded_text[:match.start()].encode('utf-16-le')) // 2
        encoded_text = encoded_text[:match.start()] + "✨" + encoded_text[match.end():]
        entities.append(MessageEntityCustomEmoji(
            offset=start_idx,
            length=1,
            document_id=doc_id
        ))
    return encoded_text, entities

def prepare_ultimate_tag_message(user, suffix: str):
    name_raw = getattr(user, 'first_name', None) or "Foydalanuvchi"
    user_id = user.id
    access_hash = getattr(user, 'access_hash', 0)
    username = getattr(user, 'username', None)

    vip_info = VIP_USERS_CACHE.get(str(user_id))
    custom_name = vip_info.get("custom_name") if vip_info else None

    mention_entity = None
    if custom_name:
        mention_text = custom_name
        name_utf16_len = len(mention_text.encode('utf-16-le')) // 2
        mention_entity = InputMessageEntityMentionName(
            offset=0,
            length=name_utf16_len,
            user_id=InputUser(user_id=user_id, access_hash=access_hash)
        )
    elif user_id == 7309051105:
        mention_text = "Owner"
        name_utf16_len = len(mention_text.encode('utf-16-le')) // 2
        mention_entity = InputMessageEntityMentionName(
            offset=0,
            length=name_utf16_len,
            user_id=InputUser(user_id=user_id, access_hash=access_hash)
        )
    elif username:
        mention_text = f"@{username}"
    else:
        mention_text = f"{name_raw}"
        name_utf16_len = len(mention_text.encode('utf-16-le')) // 2
        mention_entity = InputMessageEntityMentionName(
            offset=0,
            length=name_utf16_len,
            user_id=InputUser(user_id=user_id, access_hash=access_hash)
        )

    full_text = f"{mention_text} {suffix}".strip()
    pattern = r'\[premium:(\d+)\]'

    final_entities = []
    if mention_entity:
        final_entities.append(mention_entity)

    cleaned_text = ""
    last_idx = 0
    utf16_current_offset = 0

    for match in re.finditer(pattern, full_text):
        prefix_part = full_text[last_idx:match.start()]
        cleaned_text += prefix_part
        utf16_current_offset += len(prefix_part.encode('utf-16-le')) // 2

        doc_id = int(match.group(1))
        cleaned_text += "✨"

        final_entities.append(MessageEntityCustomEmoji(
            offset=utf16_current_offset,
            length=1,
            document_id=doc_id
        ))

        utf16_current_offset += 1
        last_idx = match.end()

    suffix_part = full_text[last_idx:]
    cleaned_text += suffix_part

    resolved_entities = list(final_entities)
    resolved_entities.sort(key=lambda e: getattr(e, 'offset', 0))

    return cleaned_text, resolved_entities

def aiogram_to_premium_encoded(msg: Message) -> str:
    text = msg.text or msg.caption or ""
    if not text:
        return ""
    entities = msg.entities or msg.caption_entities or []
    sorted_ents = sorted(entities, key=lambda e: e.offset, reverse=True)
    for ent in sorted_ents:
        if ent.type == "custom_emoji" and getattr(ent, "custom_emoji_id", None):
            start_char = utf16_offset_to_char_idx(text, ent.offset)
            end_char = utf16_offset_to_char_idx(text, ent.offset + ent.length)
            emoji_code = f"[premium:{ent.custom_emoji_id}]"
            text = text[:start_char] + emoji_code + text[end_char:]
    return text

async def utag_loop_telethon(cid, sender_id, suffix, client):
    session_key = (cid, sender_id, "u")
    acc_id = str(sender_id)

    async with get_db_client() as db:
        cursor = await db.execute("SELECT user_id FROM user_sessions WHERE account_id = ?", (acc_id,))
        row = (cursor.rows[0] if cursor.rows else None)
    if not row: return
    owner_id = row[0]

    u = await ensure_user(int(owner_id))
    delay = 1.7
    exceptions = u.get("exceptions", [])
    tagged_count = 0

    try:
        async for user in client.iter_participants(cid):
            if user.bot or user.deleted or user.id in exceptions:
                continue

            vip_info = VIP_USERS_CACHE.get(str(user.id))
            if vip_info and vip_info.get("is_skipped"):
                continue

            rem_flood = get_account_flood_remaining(acc_id)
            if rem_flood > 0:
                await asyncio.sleep(rem_flood + 1)

            body_text, telethon_ents = prepare_ultimate_tag_message(user, suffix)
            body_text = make_text_unique(body_text)

            try:
                await send_typing_action(client, cid)
                await client.send_message(cid, body_text, formatting_entities=telethon_ents if telethon_ents else None)
                tagged_count += 1
            except FloodWaitError as fwe:
                account_flood_until[acc_id] = time.time() + fwe.seconds
                await asyncio.sleep(fwe.seconds + 2)
                continue
            except Exception:
                continue
            await asyncio.sleep(delay)
    except asyncio.CancelledError:
        pass
    finally:
        _tasks.pop(session_key, None)
        try:
            await client.send_message(cid, f"uTag @{bot_username}  orqali  yakunlandi🛑")
        except Exception:
            pass
        if tagged_count > 0:
            await inc_usage_stat("utag", tagged_count)

async def rutag_loop_telethon(cid, sender_id, suffix, client):
    session_key = (cid, sender_id, "ru")
    acc_id = str(sender_id)

    async with get_db_client() as db:
        cursor = await db.execute("SELECT user_id FROM user_sessions WHERE account_id = ?", (acc_id,))
        row = (cursor.rows[0] if cursor.rows else None)
    if not row: return
    owner_id = row[0]

    u = await ensure_user(int(owner_id))
    delay = 1.7
    exceptions = u.get("exceptions", [])
    tagged_count = 0

    async with get_db_client() as db:
        cursor = await db.execute("SELECT word FROM user_random_words WHERE user_id = ?", (str(owner_id),))
        words_source = [r[0] for r in cursor.rows]

    try:
        async for user in client.iter_participants(cid):
            if user.bot or user.deleted or user.id in exceptions:
                continue

            vip_info = VIP_USERS_CACHE.get(str(user.id))
            if vip_info and vip_info.get("is_skipped"):
                continue

            rem_flood = get_account_flood_remaining(acc_id)
            if rem_flood > 0:
                await asyncio.sleep(rem_flood + 1)

            raw_word = random.choice(words_source) if words_source else ""

            suffix_with_word = f"{raw_word} {suffix}".strip()
            body_text, telethon_ents = prepare_ultimate_tag_message(user, suffix_with_word)
            body_text = make_text_unique(body_text)

            try:
                await send_typing_action(client, cid)
                await client.send_message(cid, body_text, formatting_entities=telethon_ents if telethon_ents else None)
                tagged_count += 1
            except FloodWaitError as fwe:
                account_flood_until[acc_id] = time.time() + fwe.seconds
                await asyncio.sleep(fwe.seconds + 2)
                continue
            except Exception:
                continue
            await asyncio.sleep(delay)
    except asyncio.CancelledError:
        pass
    finally:
        _tasks.pop(session_key, None)
        try:
            await client.send_message(cid, f"uTag @{bot_username}  orqali  yakunlandi🛑")
        except Exception:
            pass
        if tagged_count > 0:
            await inc_usage_stat("utag", tagged_count)

# ===============================================================
# 10. AIOGRAM BOT SETUP, FSM & KEYBOARDS
# ===============================================================
from aiogram import BaseMiddleware
from aiogram.types import TelegramObject

class ForceSubMiddleware(BaseMiddleware):
    async def __call__(
        self,
        handler,
        event: TelegramObject,
        data: dict
    ):
        user = data.get("event_from_user")
        if not user or user.is_bot:
            return await handler(event, data)

        user_id = user.id
        if user_id == OWNER_ID:
            return await handler(event, data)

        if isinstance(event, CallbackQuery) and event.data == "check_force_sub":
            return await handler(event, data)

        if isinstance(event, Message):
            if event.text and (event.text.startswith("/start") or event.text.startswith("/lang")):
                return await handler(event, data)
        elif isinstance(event, CallbackQuery):
            if event.data and (event.data.startswith("set_lang_") or event.data == "accept_terms_of_service"):
                return await handler(event, data)

        async with get_db_client() as db:
            cursor = await db.execute("SELECT channel_id, title, invite_link FROM force_sub_channels")
            channels = cursor.rows

        if not channels:
            return await handler(event, data)

        unsub_channels = []
        for ch_id, title, invite_link in channels:
            is_sub = False
            try:
                member = await bot.get_chat_member(chat_id=ch_id, user_id=user_id)
                if member.status in ["creator", "administrator", "member", "restricted"]:
                    is_sub = True
            except Exception:
                pass

            if not is_sub:
                unsub_channels.append((ch_id, title, invite_link))

        if not unsub_channels:
            return await handler(event, data)

        kb = InlineKeyboardBuilder()
        for idx, (ch_id, title, invite_link) in enumerate(unsub_channels, 1):
            btn_title = title or f"Kanal {idx}"
            target_url = invite_link if invite_link else f"https://t.me/{str(ch_id).lstrip('@')}"
            kb.row(colored_btn(f"📢 {btn_title}", url=target_url))

        kb.row(colored_btn("🔄 Obunani tekshirish", "check_force_sub", "success"))

        sub_msg_text = f_t(
            "⚠️ <b>Botdan foydalanish uchun quyidagi kanallarga obuna bo'ling:</b>"
        )

        if isinstance(event, CallbackQuery):
            await event.answer("⚠️ Kanallarga obuna bo'lishingiz shart!", show_alert=True)
            try:
                await event.message.answer(sub_msg_text, reply_markup=kb.as_markup())
            except Exception:
                pass
        elif isinstance(event, Message):
            await event.answer(sub_msg_text, reply_markup=kb.as_markup())

        return

storage = MemoryStorage()
bot = Bot(token=BOT_TOKEN, default=DefaultBotProperties(parse_mode="HTML"))
dp = Dispatcher(storage=storage)

dp.message.outer_middleware(ForceSubMiddleware())
dp.callback_query.outer_middleware(ForceSubMiddleware())

class UserStatesGroup(StatesGroup):
    utag_group_link = State()
    utag_custom_suffix = State()
    scrapper_group_link = State()
    scrapper_limit = State()
    login_phone = State()
    login_code = State()
    login_2fa = State()
    add_custom_word = State()
    support_message = State()

class AdminStatesGroup(StatesGroup):
    reply_support = State()
    broadcast_type = State()
    broadcast_content = State()
    broadcast_buttons = State()
    broadcast_confirm = State()
    add_force_sub = State()
    search_user_id = State()

def get_language_keyboard():
    kb = InlineKeyboardBuilder()
    kb.row(colored_btn("🇺🇿 O'zbekcha", "set_lang_uz", "primary"))
    kb.row(colored_btn("🇺🇿 Ўзбекча", "set_lang_uz_cyrl", "primary"))
    kb.row(colored_btn("🇷🇺 Русский", "set_lang_ru", "primary"))
    kb.row(colored_btn("🇬🇧 English", "set_lang_en", "primary"))
    return kb.as_markup()

def get_userbot_keyboard(user_id: int):
    kb = InlineKeyboardBuilder()
    kb.row(
        colored_btn("🏷 Avto uTag", "menu_avto_utag", "primary"),
        colored_btn("👥 Avto User", "menu_avto_user", "primary")
    )
    kb.row(
        colored_btn("👤 Profil", "menu_profil_panel", "success"),
        colored_btn("❓ Yordam", "menu_yordam", "primary")
    )
    return kb.as_markup()

async def show_main_menu(target: Any, user_id: int):
    u = await ensure_user(user_id)
    name = u.get("first_name")
    main_text = (
        f"Assalomu aleykum, {name}\n"
        '"""AlphaUserBot """'
    )
    formatted = f_t(main_text)
    markup = get_userbot_keyboard(user_id)

    if isinstance(target, CallbackQuery):
        try:
            await target.message.edit_text(formatted, reply_markup=markup)
        except Exception:
            await target.message.answer(formatted, reply_markup=markup)
    elif isinstance(target, Message):
        await target.answer(formatted, reply_markup=markup)
    else:
        await bot.send_message(user_id, formatted, reply_markup=markup)

def get_group_selection_keyboard():
    builder = ReplyKeyboardBuilder()
    builder.add(KeyboardButton(
        text="🟢 Guruh tanlash",
        request_chat=KeyboardButtonRequestChat(
            request_id=1001,
            chat_is_channel=False,
            chat_is_forum=False
        )
    ))
    return builder.as_markup(resize_keyboard=True, one_time_keyboard=True)

# ===============================================================
# ADMIN PANEL HANDLERS (/panel)
# ===============================================================
def get_admin_panel_keyboard():
    kb = InlineKeyboardBuilder()
    kb.row(
        colored_btn("📊 Statistika", "admin_stats", "primary"),
        colored_btn("📢 Xabar tarqatish", "admin_broadcast", "primary")
    )
    kb.row(
        colored_btn("📢 Majburiy obuna", "admin_force_sub", "primary"),
        colored_btn("👤 Foydalanuvchilar", "admin_users_search", "primary")
    )
    kb.row(colored_btn("Orqaga", "main_menu", "danger"))
    return kb.as_markup()

async def set_vip_custom_name(user_id: str, custom_name: str):
    now_str = datetime.now(timezone.utc).isoformat()
    str_uid = str(user_id)
    async with get_db_client() as db:
        cursor = await db.execute("SELECT is_skipped FROM vip_users WHERE user_id = ?", (str_uid,))
        row = (cursor.rows[0] if cursor.rows else None)
        is_skipped = int(row[0]) if row and row[0] is not None else 0
        await db.execute("""
        INSERT OR REPLACE INTO vip_users (user_id, custom_name, is_skipped, updated_at)
        VALUES (?, ?, ?, ?)
        """, (str_uid, custom_name, is_skipped, now_str))

    if str_uid not in VIP_USERS_CACHE:
        VIP_USERS_CACHE[str_uid] = {"custom_name": custom_name, "is_skipped": is_skipped, "updated_at": now_str}
    else:
        VIP_USERS_CACHE[str_uid]["custom_name"] = custom_name
        VIP_USERS_CACHE[str_uid]["updated_at"] = now_str

async def set_vip_skip(user_id: str, is_skipped: int):
    now_str = datetime.now(timezone.utc).isoformat()
    str_uid = str(user_id)
    async with get_db_client() as db:
        cursor = await db.execute("SELECT custom_name FROM vip_users WHERE user_id = ?", (str_uid,))
        row = (cursor.rows[0] if cursor.rows else None)
        custom_name = row[0] if row else None
        await db.execute("""
        INSERT OR REPLACE INTO vip_users (user_id, custom_name, is_skipped, updated_at)
        VALUES (?, ?, ?, ?)
        """, (str_uid, custom_name, is_skipped, now_str))

    if str_uid not in VIP_USERS_CACHE:
        VIP_USERS_CACHE[str_uid] = {"custom_name": custom_name, "is_skipped": is_skipped, "updated_at": now_str}
    else:
        VIP_USERS_CACHE[str_uid]["is_skipped"] = is_skipped
        VIP_USERS_CACHE[str_uid]["updated_at"] = now_str

async def reset_vip(user_id: str):
    str_uid = str(user_id)
    async with get_db_client() as db:
        await db.execute("DELETE FROM vip_users WHERE user_id = ?", (str_uid,))
    VIP_USERS_CACHE.pop(str_uid, None)

@dp.message(Command("vip"))
async def cmd_vip_management(message: Message):
    if message.from_user.id != OWNER_ID:
        return

    text_raw = message.text or ""
    parts = text_raw.split(maxsplit=3)
    if len(parts) < 2:
        help_text = (
            "👑 <b>VIP Boshqaruv Paneli (Faqat Owner)</b>\n\n"
            "• <code>/vip set &lt;id&gt; &lt;nom&gt;</code> — mention nomini o'zgartirish\n"
            "• <code>/vip skip &lt;id&gt;</code> — uTaglardan o'tkazib yuborish\n"
            "• <code>/vip unskip &lt;id&gt;</code> — skipni bekor qilish\n"
            "• <code>/vip reset &lt;id&gt;</code> — asliga qaytarish (nom + skip)\n"
            "• <code>/vip info &lt;id&gt;</code> — hozirgi holatni ko'rish\n"
            "• <code>/vip list</code> — barcha VIP userlar ro'yxati"
        )
        await message.answer(f_t(help_text))
        return

    subcmd = parts[1].lower()

    if subcmd == "list":
        if not VIP_USERS_CACHE:
            await message.answer(f_t("ℹ️ VIP ro'yxatida hech qanday foydalanuvchi mavjud emas."))
            return

        lines = ["📋 <b>VIP Foydalanuvchilar Ro'yxati:</b>\n"]
        for idx, (uid, info) in enumerate(VIP_USERS_CACHE.items(), 1):
            cname = info.get("custom_name") or "O'rnatilmagan"
            skip_str = "Ha 🔴" if info.get("is_skipped") else "Yo'q 🟢"
            lines.append(f"{idx}. ID: <code>{uid}</code> | Nom: <b>{cname}</b> | Skip: {skip_str}")

        await message.answer(f_t("\n".join(lines)))
        return

    if subcmd == "set":
        if len(parts) < 4:
            await message.answer(f_t("⚠️ Format: <code>/vip set &lt;id&gt; &lt;nom&gt;</code>"))
            return
        target_id = parts[2].strip()
        custom_name = parts[3].strip()
        await set_vip_custom_name(target_id, custom_name)
        await message.answer(f_t(f"✅ ID: <code>{target_id}</code> uchun mention nomi <b>{custom_name}</b> ga o'zgartirildi."))
        return

    if subcmd == "skip":
        if len(parts) < 3:
            await message.answer(f_t("⚠️ Format: <code>/vip skip &lt;id&gt;</code>"))
            return
        target_id = parts[2].strip()
        await set_vip_skip(target_id, 1)
        await message.answer(f_t(f"✅ ID: <code>{target_id}</code> barcha uTaglardan o'tkazib yuboriladi (skip qilindi)."))
        return

    if subcmd == "unskip":
        if len(parts) < 3:
            await message.answer(f_t("⚠️ Format: <code>/vip unskip &lt;id&gt;</code>"))
            return
        target_id = parts[2].strip()
        await set_vip_skip(target_id, 0)
        await message.answer(f_t(f"✅ ID: <code>{target_id}</code> uchun skip holati bekor qilindi."))
        return

    if subcmd == "reset":
        if len(parts) < 3:
            await message.answer(f_t("⚠️ Format: <code>/vip reset &lt;id&gt;</code>"))
            return
        target_id = parts[2].strip()
        await reset_vip(target_id)
        await message.answer(f_t(f"✅ ID: <code>{target_id}</code> VIP holati va sozlamalari asliga qaytarildi."))
        return

    if subcmd == "info":
        if len(parts) < 3:
            await message.answer(f_t("⚠️ Format: <code>/vip info &lt;id&gt;</code>"))
            return
        target_id = parts[2].strip()
        info = VIP_USERS_CACHE.get(str(target_id))
        if not info or (not info.get("custom_name") and not info.get("is_skipped")):
            await message.answer(f_t(f"ℹ️ ID: <code>{target_id}</code> uchun VIP sozlamalari mavjud emas (Oddiy holatda)."))
            return

        cname = info.get("custom_name") or "O'rnatilmagan"
        skip_str = "Ha 🔴" if info.get("is_skipped") else "Yo'q 🟢"
        updated_at = info.get("updated_at") or "Noma'lum"

        info_text = (
            f"👤 <b>VIP Ma'lumot:</b>\n\n"
            f"🆔 <b>ID:</b> <code>{target_id}</code>\n"
            f"🏷 <b>Custom Nom:</b> {cname}\n"
            f"⏭ <b>Skip qilingan:</b> {skip_str}\n"
            f"⏱ <b>Yangilangan:</b> <code>{updated_at}</code>"
        )
        await message.answer(f_t(info_text))
        return

    await message.answer(f_t("⚠️ Noto'g'ri buyruq. Format: <code>/vip set|skip|unskip|reset|info|list</code>"))

# ===============================================================
# DATABASE BACKUP & RESTORE HANDLERS
# ===============================================================
@dp.message(Command("getdb"), F.chat.type == "private")
async def cmd_getdb_backup(message: Message):
    if message.from_user.id != OWNER_ID:
        return

    msg = await message.answer(f_t("⏳ <b>Database zaxira nusxasi olinmoqda...</b>"))
    backup_file = f"database_backup_{int(time.time())}.db"
    try:
        if GLOBAL_DB_CONN:
            await GLOBAL_DB_CONN.execute("PRAGMA wal_checkpoint(TRUNCATE);")

        if os.path.exists(DB_FILE):
            import shutil
            shutil.copy2(DB_FILE, backup_file)
            await message.answer_document(
                document=FSInputFile(backup_file),
                caption=f_t(f"📦 <b>Database Zaxira Nusxasi</b>\n\n⏱ Vaqt: <code>{datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M:%S')} UTC</code>")
            )
            await msg.delete()
        else:
            await msg.edit_text(f_t("❌ <b>Database fayli topilmadi.</b>"))
    except Exception as e:
        log.error(f"Error creating DB backup: {e}")
        await msg.edit_text(f_t(f"❌ <b>Zaxiralashda xatolik yuz berdi:</b> {e}"))
    finally:
        if os.path.exists(backup_file):
            try:
                os.remove(backup_file)
            except Exception:
                pass

@dp.message(F.document, F.chat.type == "private")
async def msg_handle_db_restore(message: Message, state: FSMContext):
    if message.from_user.id != OWNER_ID:
        return

    doc = message.document
    if not doc.file_name or not doc.file_name.endswith(".db"):
        return

    status_msg = await message.answer(f_t("⏳ <b>.db fayl qabul qilindi. Tekshirilmoqda...</b>"))
    temp_path = f"restore_temp_{int(time.time())}.db"

    try:
        await bot.download(doc, destination=temp_path)

        # 1. SQLite ekanligi va quick_check orqali tekshirish
        conn = None
        try:
            conn = sqlite3.connect(temp_path)
            cur = conn.cursor()
            cur.execute("PRAGMA quick_check;")
            res = cur.fetchone()
            if not res or res[0] != "ok":
                raise ValueError("PRAGMA quick_check muvaffaqiyatsiz tugadi!")
        except Exception as check_err:
            if conn:
                conn.close()
            await status_msg.edit_text(f_t(f"❌ <b>Fayl yaroqsiz SQLite bazasi:</b> {check_err}"))
            if os.path.exists(temp_path):
                os.remove(temp_path)
            return
        finally:
            if conn:
                conn.close()

        await status_msg.edit_text(f_t("🔄 <b>Restore jarayoni boshlandi. Tizim to'xtatilmoqda...</b>"))

        # 2. Barcha background tasklarni to'xtatish
        for task in list(_running_tasks):
            if not task.done():
                task.cancel()
        if _running_tasks:
            await asyncio.gather(*_running_tasks, return_exceptions=True)
        _running_tasks.clear()

        # 3. Barcha faol userbotlarni uzish
        for acc_id, cl in list(userbot_clients.items()):
            try:
                await cl.disconnect()
            except Exception:
                pass
        userbot_clients.clear()

        # 4. Barcha kesh ko'rsatkichlarini tozalash
        USER_RAM_CACHE.clear()
        VIP_USERS_CACHE.clear()
        account_user_cache.clear()
        account_flood_until.clear()

        # 5. Global DB connection ni yopish
        await close_global_db_conn()

        # 6. Eski database.db -> database_old.db qilib zaxiralash
        if os.path.exists(DB_FILE):
            old_backup = "database_old.db"
            if os.path.exists(old_backup):
                os.remove(old_backup)
            os.rename(DB_FILE, old_backup)

        # 7. WAL va SHM fayllarini tozalash
        for ext in ["-wal", "-shm"]:
            f_path = DB_FILE + ext
            if os.path.exists(f_path):
                try:
                    os.remove(f_path)
                except Exception:
                    pass

        # 8. Yangi faylni xavfsiz o'rnatish (os.replace)
        os.replace(temp_path, DB_FILE)

        # 9. Database qayta init qilinishi
        await init_sqlite_db()

        # 10. Background tasklarni qayta ishga tushirish
        task_save = asyncio.create_task(auto_save_task())
        task_watchdog = asyncio.create_task(userbot_watchdog_loop())
        _running_tasks.update({task_save, task_watchdog})

        # 11. Userbotlarni qayta aktivlashtirish
        await auto_activate_all_userbots()

        await status_msg.edit_text(f_t("✅ <b>Database muvaffaqiyatli tiklandi (Restore qilindi)!</b>\n\nBarcha seanslar va background tasklar qayta ishga tushirildi."))

    except Exception as e:
        log.error(f"Error during DB restore: {e}")
        await status_msg.edit_text(f_t(f"❌ <b>Database tiklashda xatolik yuz berdi:</b> {e}"))
        if os.path.exists(temp_path):
            try:
                os.remove(temp_path)
            except Exception:
                pass

@dp.message(Command("panel"), F.chat.type == "private")
async def cmd_admin_panel(message: Message, state: FSMContext):
    if message.from_user.id != OWNER_ID:
        return
    await state.clear()
    await message.answer(
        f_t("⚙️ <b>Admin Panel</b>\n\nKerakli bo'limni tanlang:"),
        reply_markup=get_admin_panel_keyboard()
    )

@dp.callback_query(F.data == "admin_panel")
async def cb_admin_panel(callback: CallbackQuery, state: FSMContext):
    if callback.from_user.id != OWNER_ID:
        await callback.answer("Ruxsat berilmagan!", show_alert=True)
        return
    await state.clear()
    await callback.message.edit_text(
        f_t("⚙️ <b>Admin Panel</b>\n\nKerakli bo'limni tanlang:"),
        reply_markup=get_admin_panel_keyboard()
    )

@dp.callback_query(F.data == "admin_stats")
async def cb_admin_stats(callback: CallbackQuery):
    if callback.from_user.id != OWNER_ID:
        await callback.answer("Ruxsat berilmagan!", show_alert=True)
        return

    async with get_db_client() as db:
        c1 = await db.execute("SELECT COUNT(*) FROM users")
        total_users = c1.rows[0][0] if c1.rows else 0

        c2 = await db.execute("SELECT COUNT(*) FROM user_sessions WHERE status = 'active'")
        active_userbots = c2.rows[0][0] if c2.rows else 0

    stats = await get_usage_stats()

    stats_text = (
        "📊 <b>Real vaqtdagi Statistika:</b>\n\n"
        f"👥 <b>Jami foydalanuvchilar:</b> {total_users} ta\n"
        f"📱 <b>Faol userbotlar soni:</b> {active_userbots} ta\n\n"
        f"🏷 <b>uTag soni:</b> Bugun {stats['utag_today']} | Jami {stats['utag_total']}\n"
        f"👥 <b>Scraper soni:</b> Bugun {stats['scraper_today']} | Jami {stats['scraper_total']}"
    )

    kb = InlineKeyboardBuilder()
    kb.row(colored_btn("🔄 Yangilash", "admin_stats", "primary"))
    kb.row(colored_btn("Orqaga", "admin_panel", "danger"))

    await callback.message.edit_text(f_t(stats_text), reply_markup=kb.as_markup())

# BROADCAST SYSTEM MODULE
@dp.callback_query(F.data == "admin_broadcast")
async def cb_admin_broadcast(callback: CallbackQuery, state: FSMContext):
    if callback.from_user.id != OWNER_ID:
        await callback.answer("Ruxsat berilmagan!", show_alert=True)
        return

    await state.clear()
    kb = InlineKeyboardBuilder()
    kb.row(
        colored_btn("Oddiy xabar", "bcast_mode_plain", "primary"),
        colored_btn("Uzatilgan (forward)", "bcast_mode_forward", "primary")
    )
    kb.row(colored_btn("Orqaga", "admin_panel", "danger"))

    await callback.message.edit_text(f_t("📢 <b>Xabar turini tanlang:</b>"), reply_markup=kb.as_markup())

@dp.callback_query(F.data.startswith("bcast_mode_"))
async def cb_bcast_mode_select(callback: CallbackQuery, state: FSMContext):
    mode = callback.data.replace("bcast_mode_", "")
    await state.update_data(bcast_mode=mode)
    await state.set_state(AdminStatesGroup.broadcast_content)

    if mode == "forward":
        await callback.message.edit_text(f_t("📢 <b>Tarqatmoqchi bo'lgan xabarni ushbu chatga forward (uzatib) qiling:</b>"))
    else:
        await callback.message.edit_text(f_t("📢 <b>Tarqatiladigan xabarni (matn, rasm, video yoki fayl) yuboring:</b>"))

@dp.message(StateFilter(AdminStatesGroup.broadcast_content))
async def msg_admin_bcast_content_receive(message: Message, state: FSMContext):
    if message.from_user.id != OWNER_ID:
        return

    data = await state.get_data()
    mode = data.get("bcast_mode", "plain")

    if mode == "forward":
        await state.update_data(bcast_chat_id=message.chat.id, bcast_msg_id=message.message_id)
        kb = InlineKeyboardBuilder()
        kb.row(colored_btn("✅ Tasdiqlash va yuborish", "start_broadcast_now", "success"))
        kb.row(colored_btn("❌ Bekor qilish", "admin_panel", "danger"))

        await message.answer(f_t("🔍 <b>Forward xabar ko'rinishi (preview):</b>"))
        await message.forward(message.chat.id)
        await message.answer(f_t("Xabarni tarqatishni tasdiqlaysizmi?"), reply_markup=kb.as_markup())
    else:
        await state.update_data(bcast_chat_id=message.chat.id, bcast_msg_id=message.message_id)
        kb = InlineKeyboardBuilder()
        kb.row(colored_btn("➕ Tugma qo'shish", "add_bcast_btn_prompt", "primary"))
        kb.row(colored_btn("⏩ O'tkazib yuborish", "skip_bcast_btn", "success"))
        kb.row(colored_btn("❌ Bekor qilish", "admin_panel", "danger"))

        await message.answer(
            f_t("🔘 <b>Xabarga inline tugmalar qo'shasizmi?</b>\n\nFormat: <code>Tugma matni - https://link.com</code>"),
            reply_markup=kb.as_markup()
        )

@dp.callback_query(F.data == "add_bcast_btn_prompt")
async def cb_add_bcast_btn_prompt(callback: CallbackQuery, state: FSMContext):
    await state.set_state(AdminStatesGroup.broadcast_buttons)
    await callback.message.edit_text(
        f_t("✍️ <b>Tugmalarni quyidagi formatda yuboring:</b>\n\n"
            "<code>Tugma 1 - https://t.me/link1\nTugma 2 - https://t.me/link2</code>")
    )

@dp.message(StateFilter(AdminStatesGroup.broadcast_buttons), F.text)
async def msg_admin_bcast_buttons_receive(message: Message, state: FSMContext):
    if message.from_user.id != OWNER_ID:
        return

    raw_btn_text = message.text.strip()
    btn_rows = []
    for line in raw_btn_text.split("\n"):
        if "-" in line:
            parts = line.split("-", 1)
            btn_label = parts[0].strip()
            btn_url = parts[1].strip()
            if btn_label and btn_url.startswith("http"):
                btn_rows.append({"label": btn_label, "url": btn_url})

    await state.update_data(bcast_buttons=btn_rows)
    await show_bcast_preview(message, state)

@dp.callback_query(F.data == "skip_bcast_btn")
async def cb_skip_bcast_btn(callback: CallbackQuery, state: FSMContext):
    await state.update_data(bcast_buttons=[])
    await show_bcast_preview(callback, state)

async def show_bcast_preview(target: Any, state: FSMContext):
    data = await state.get_data()
    chat_id = data.get("bcast_chat_id")
    msg_id = data.get("bcast_msg_id")
    btn_rows = data.get("bcast_buttons", [])

    kb = InlineKeyboardBuilder()
    for btn in btn_rows:
        kb.row(colored_btn(btn["label"], url=btn["url"]))

    action_kb = InlineKeyboardBuilder()
    action_kb.row(colored_btn("✅ Tasdiqlash va yuborish", "start_broadcast_now", "success"))
    action_kb.row(colored_btn("❌ Bekor qilish", "admin_panel", "danger"))

    if isinstance(target, CallbackQuery):
        msg = target.message
    else:
        msg = target

    await msg.answer(f_t("🔍 <b>Xabar ko'rinishi (preview):</b>"))
    await bot.copy_message(chat_id, chat_id, msg_id, reply_markup=kb.as_markup() if btn_rows else None)
    await msg.answer(f_t("Xabarni tarqatishni tasdiqlaysizmi?"), reply_markup=action_kb.as_markup())

@dp.callback_query(F.data == "start_broadcast_now")
async def cb_start_broadcast_now(callback: CallbackQuery, state: FSMContext):
    if callback.from_user.id != OWNER_ID:
        return

    data = await state.get_data()
    mode = data.get("bcast_mode", "plain")
    chat_id = data.get("bcast_chat_id")
    msg_id = data.get("bcast_msg_id")
    btn_rows = data.get("bcast_buttons", [])
    await state.clear()

    async with get_db_client() as db:
        cursor = await db.execute("SELECT id FROM users")
        user_rows = cursor.rows

    all_user_ids = [int(r[0]) for r in user_rows if r[0] and str(r[0]).isdigit()]
    total_users = len(all_user_ids)

    status_msg = await callback.message.edit_text(
        f_t(f"🚀 <b>Xabar tarqatilmoqda...</b>\n\nYuborildi: 0 / {total_users}\nXato: 0")
    )

    reply_markup = None
    if mode == "plain" and btn_rows:
        kb = InlineKeyboardBuilder()
        for btn in btn_rows:
            kb.row(colored_btn(btn["label"], url=btn["url"]))
        reply_markup = kb.as_markup()

    success_cnt = 0
    fail_cnt = 0

    for idx, uid in enumerate(all_user_ids, 1):
        try:
            if mode == "forward":
                await bot.forward_message(uid, chat_id, msg_id)
            else:
                await bot.copy_message(uid, chat_id, msg_id, reply_markup=reply_markup)
            success_cnt += 1
        except Exception:
            fail_cnt += 1

        if idx % 25 == 0 or idx == total_users:
            try:
                await status_msg.edit_text(
                    f_t(f"🚀 <b>Xabar tarqatilmoqda...</b>\n\nYuborildi: {success_cnt} / {total_users}\nXato: {fail_cnt}")
                )
            except Exception:
                pass
        await asyncio.sleep(0.04)

    await status_msg.edit_text(
        f_t(f"✅ <b>Xabar tarqatish yakunlandi!</b>\n\nYuborildi: {success_cnt} / {total_users}\nXato: {fail_cnt}"),
        reply_markup=InlineKeyboardMarkup(inline_keyboard=[[colored_btn("Orqaga", "admin_panel", "primary")]])
    )

# FORCE SUBSCRIBE CHANNEL MANAGEMENT MODULE
@dp.callback_query(F.data == "admin_force_sub")
async def cb_admin_force_sub(callback: CallbackQuery, state: FSMContext):
    if callback.from_user.id != OWNER_ID:
        await callback.answer("Ruxsat berilmagan!", show_alert=True)
        return

    await state.clear()
    async with get_db_client() as db:
        cursor = await db.execute("SELECT channel_id, title, invite_link FROM force_sub_channels")
        channels = cursor.rows

    text = "📢 <b>Majburiy Obuna Kanallari:</b>\n\n"
    kb = InlineKeyboardBuilder()

    if not channels:
        text += "<i>Hozircha hech qanday kanal qo'shilmagan.</i>"
    else:
        for ch_id, title, invite_link in channels:
            ch_title = title or ch_id
            kb.row(colored_btn(f"❌ {ch_title}", f"del_force_sub_{ch_id}", "primary"))

    kb.row(colored_btn("➕ Kanal qo'shish", "add_force_sub_prompt", "success"))
    kb.row(colored_btn("Orqaga", "admin_panel", "danger"))

    await callback.message.edit_text(f_t(text), reply_markup=kb.as_markup())

@dp.callback_query(F.data == "add_force_sub_prompt")
async def cb_add_force_sub_prompt(callback: CallbackQuery, state: FSMContext):
    if callback.from_user.id != OWNER_ID:
        return
    await state.set_state(AdminStatesGroup.add_force_sub)
    await callback.message.edit_text(
        f_t("✍️ <b>Kanal ID yoki usernamesini yuboring:</b>\n\nMasalan: <code>@kanal_username</code> yoki <code>-100123456789</code>")
    )

@dp.message(StateFilter(AdminStatesGroup.add_force_sub), F.text)
async def msg_admin_add_force_sub(message: Message, state: FSMContext):
    if message.from_user.id != OWNER_ID:
        return

    channel_input = message.text.strip()
    await state.clear()

    try:
        chat = await bot.get_chat(channel_input)
        ch_id = str(chat.id)
        title = chat.title or chat.username or ch_id
        invite_link = chat.invite_link or (f"https://t.me/{chat.username}" if chat.username else "")

        async with get_db_client() as db:
            await db.execute(
                "INSERT OR REPLACE INTO force_sub_channels (channel_id, title, invite_link) VALUES (?, ?, ?)",
                (ch_id, title, invite_link)
            )

        kb = InlineKeyboardBuilder()
        kb.row(colored_btn("Kanallar ro'yxati", "admin_force_sub", "primary"))
        await message.answer(f_t(f"✅ <b>{title}</b> muvaffaqiyatli qo'shildi!"), reply_markup=kb.as_markup())
    except Exception as e:
        kb = InlineKeyboardBuilder()
        kb.row(colored_btn("Orqaga", "admin_force_sub", "danger"))
        await message.answer(f_t(f"❌ Kanalni aniqlashda xatolik: {e}\n\nBot ushbu kanalda admin ekanligiga ishonch hosil qiling."), reply_markup=kb.as_markup())

@dp.callback_query(F.data.startswith("del_force_sub_"))
async def cb_del_force_sub(callback: CallbackQuery, state: FSMContext):
    if callback.from_user.id != OWNER_ID:
        return

    ch_id = callback.data.replace("del_force_sub_", "")
    async with get_db_client() as db:
        await db.execute("DELETE FROM force_sub_channels WHERE channel_id = ?", (ch_id,))

    await callback.answer("✅ Kanal o'chirildi!", show_alert=True)
    await cb_admin_force_sub(callback, state)

# USER SEARCH BY ID MODULE
@dp.callback_query(F.data == "admin_users_search")
async def cb_admin_users_search_prompt(callback: CallbackQuery, state: FSMContext):
    if callback.from_user.id != OWNER_ID:
        await callback.answer("Ruxsat berilmagan!", show_alert=True)
        return

    await state.set_state(AdminStatesGroup.search_user_id)
    await callback.message.edit_text(f_t("🔎 <b>Qidirilayotgan foydalanuvchi ID sini kiriting:</b>"))

@dp.message(StateFilter(AdminStatesGroup.search_user_id), F.text)
async def msg_admin_search_user_by_id(message: Message, state: FSMContext):
    if message.from_user.id != OWNER_ID:
        return

    search_id_str = message.text.strip()
    await state.clear()

    async with get_db_client() as db:
        cursor = await db.execute("SELECT * FROM users WHERE id = ?", (search_id_str,))
        row = (cursor.rows[0] if cursor.rows else None)

    if not row:
        kb = InlineKeyboardBuilder()
        kb.row(colored_btn("Qayta qidirish", "admin_users_search", "primary"))
        kb.row(colored_btn("Orqaga", "admin_panel", "danger"))
        await message.answer(f_t("❌ Foydalanuvchi topilmadi."), reply_markup=kb.as_markup())
        return

    u_dict = row_to_dict(cursor, 0)
    str_uid = str(u_dict.get("id"))

    async with get_db_client() as db:
        c_sess = await db.execute("SELECT account_id, phone, name FROM user_sessions WHERE user_id = ? AND status = 'active'", (str_uid,))
        sess_row = (c_sess.rows[0] if c_sess.rows else None)

        c_words = await db.execute("SELECT COUNT(*) FROM user_random_words WHERE user_id = ?", (str_uid,))
        words_count = c_words.rows[0][0] if c_words.rows else 0

    acc_info = "Nofaol / Ulangan emas 🔴"
    if sess_row:
        acc_info = f"Ulangan 🟢 (Tel: {sess_row[1] or 'Yo\'q'}, Name: {sess_row[2] or 'Yo\'q'})"

    terms_str = "Qabul qilingan ✅" if u_dict.get("terms_accepted") else "Qabul qilinmagan ❌"

    info_text = (
        f"👤 <b>Foydalanuvchi Ma'lumotlari:</b>\n\n"
        f"🆔 <b>ID:</b> <code>{u_dict.get('id')}</code>\n"
        f"📛 <b>Ism:</b> {u_dict.get('first_name') or 'Noma\'lum'}\n"
        f"🔗 <b>Username:</b> @{u_dict.get('username') or 'yo\'q'}\n"
        f"🌐 <b>Til:</b> <code>{u_dict.get('lang', 'uz')}</code>\n"
        f"📜 <b>Shartlar:</b> {terms_str}\n"
        f"📱 <b>Akkaunt:</b> {acc_info}\n"
        f"🎲 <b>Random so'zlar:</b> {words_count} ta\n"
        f"⏱ <b>Oxirgi faollik:</b> <code>{u_dict.get('last_seen') or 'Noma\'lum'}</code>"
    )

    kb = InlineKeyboardBuilder()
    kb.row(colored_btn("Qayta qidirish", "admin_users_search", "primary"))
    kb.row(colored_btn("Orqaga", "admin_panel", "danger"))

    await message.answer(f_t(info_text), reply_markup=kb.as_markup())

# ===============================================================
# 11. AIOGRAM HANDLERS (COMMANDS, LOGIN, PROFILE, UTAG, SCRAPER)
# ===============================================================
# ---------------------------------------------------------------
# SUPPORT SYSTEM HANDLERS
# ---------------------------------------------------------------
@dp.callback_query(F.data == "menu_yordam")
async def cb_menu_yordam(callback: CallbackQuery, state: FSMContext):
    await state.set_state(UserStatesGroup.support_message)
    kb = InlineKeyboardBuilder()
    kb.row(colored_btn("Orqaga", "main_menu", "danger"))
    await callback.message.edit_text(
        f_t("❓ <b>Savol yoki murojaatingizni yuboring:</b>"),
        reply_markup=kb.as_markup()
    )

@dp.message(StateFilter(UserStatesGroup.support_message), F.text | F.photo | F.video | F.document)
async def msg_user_support_send(message: Message, state: FSMContext):
    user_id = message.from_user.id
    await state.clear()

    u = await ensure_user(user_id, message.from_user.username, message.from_user.first_name)
    has_acc = await has_active_account(user_id)
    lang = u.get("lang", "uz")
    uname = f"@{message.from_user.username}" if message.from_user.username else "mavjud emas"
    fname = message.from_user.first_name or "Foydalanuvchi"

    header = (
        f"📩 <b>Yangi Yordam So'rovi!</b>\n\n"
        f"👤 <b>Foydalanuvchi:</b> {fname} ({uname})\n"
        f"🆔 <b>ID:</b> <code>{user_id}</code>\n"
        f"🌐 <b>Til:</b> <code>{lang}</code>\n"
        f"📱 <b>Akkaunt holati:</b> {'Ulangan 🟢' if has_acc else 'Ulanmagan 🔴'}\n\n"
        f"💬 <b>Murojaat:</b>\n"
    )

    kb = InlineKeyboardBuilder()
    kb.row(colored_btn("💬 Javob berish", f"reply_support_{user_id}", "primary"))

    try:
        if message.text:
            await bot.send_message(OWNER_ID, f_t(header + message.text), reply_markup=kb.as_markup())
        else:
            await message.copy_to(OWNER_ID, caption=f_t(header + (message.caption or "")), reply_markup=kb.as_markup())
    except Exception as e:
        log.error(f"Support forward to owner error: {e}")

    back_kb = InlineKeyboardBuilder()
    back_kb.row(colored_btn("Orqaga", "main_menu", "danger"))
    await message.answer(
        f_t("✅ <b>Murojaatingiz adminga yuborildi. Tez orada javob beriladi!</b>"),
        reply_markup=back_kb.as_markup()
    )

@dp.callback_query(F.data.startswith("reply_support_"))
async def cb_reply_support_start(callback: CallbackQuery, state: FSMContext):
    if callback.from_user.id != OWNER_ID:
        await callback.answer("Ruxsat berilmagan!", show_alert=True)
        return

    target_uid = callback.data.replace("reply_support_", "")
    await state.update_data(target_support_uid=target_uid)
    await state.set_state(AdminStatesGroup.reply_support)
    await callback.message.answer(f_t(f"✍️ <b>ID <code>{target_uid}</code> bo'lgan foydalanuvchiga javob matnini yuboring:</b>"))
    await callback.answer()

@dp.message(StateFilter(AdminStatesGroup.reply_support), F.text)
async def msg_admin_reply_support_send(message: Message, state: FSMContext):
    if message.from_user.id != OWNER_ID:
        return

    data = await state.get_data()
    target_uid = int(data.get("target_support_uid", 0))
    await state.clear()

    reply_text = message.text.strip()
    formatted_reply = f"Admin javobi:\n\n{reply_text}"

    kb = InlineKeyboardBuilder()
    kb.row(colored_btn("💬 Yana savol bor", "menu_yordam", "primary"))
    kb.row(colored_btn("Orqaga", "main_menu", "danger"))

    try:
        await bot.send_message(target_uid, f_t(formatted_reply), reply_markup=kb.as_markup())
        await message.answer(f_t(f"✅ <b>Javob ID <code>{target_uid}</code> bo'lgan foydalanuvchiga yuborildi!</b>"))
    except Exception as e:
        await message.answer(f"❌ Javob yuborishda xatolik: {e}")

@dp.callback_query(F.data == "check_force_sub")
async def cb_check_force_sub(callback: CallbackQuery, state: FSMContext):
    user_id = callback.from_user.id
    async with get_db_client() as db:
        cursor = await db.execute("SELECT channel_id, title, invite_link FROM force_sub_channels")
        channels = cursor.rows

    unsub_channels = []
    for ch_id, title, invite_link in channels:
        is_sub = False
        try:
            member = await bot.get_chat_member(chat_id=ch_id, user_id=user_id)
            if member.status in ["creator", "administrator", "member", "restricted"]:
                is_sub = True
        except Exception:
            pass

        if not is_sub:
            unsub_channels.append((ch_id, title, invite_link))

    if unsub_channels:
        await callback.answer("❌ Hali barcha kanallarga obuna bo'lmadingiz!", show_alert=True)
    else:
        await callback.answer("✅ Rahmat! Obuna tasdiqlandi.", show_alert=True)
        try:
            await callback.message.delete()
        except Exception:
            pass
        await show_main_menu(callback, user_id)

bot_active_utags = {}

@dp.message(Command("u"), F.chat.type.in_({"group", "supergroup"}))
async def cmd_bot_utag(message: Message):
    chat_id = message.chat.id
    user_id = message.from_user.id

    if chat_id in bot_active_utags and not bot_active_utags[chat_id].done():
        await message.reply("⚠️ Ushbu guruhda uTag allaqachon bajarilmoqda.")
        return

    str_uid = str(user_id)
    async with get_db_client() as db:
        cursor = await db.execute("SELECT account_id FROM user_sessions WHERE user_id = ? AND status = 'active'", (str_uid,))
        row = (cursor.rows[0] if cursor.rows else None)

    client = None
    if row and row[0] in userbot_clients:
        client = userbot_clients[row[0]]

    full_encoded = aiogram_to_premium_encoded(message)
    suffix = full_encoded.split(" ", 1)[1] if " " in full_encoded else ""

    task = asyncio.create_task(run_bot_api_utag(chat_id, user_id, client, suffix))
    bot_active_utags[chat_id] = task

@dp.message(Command("su"), F.chat.type.in_({"group", "supergroup"}))
async def cmd_bot_su(message: Message):
    chat_id = message.chat.id
    task = bot_active_utags.pop(chat_id, None)
    if task and not task.done():
        task.cancel()
        await message.reply("🛑 uTag to'xtatildi.")
    else:
        await message.reply("⚠️ Faol uTag topilmadi.")

@dp.message(Command("zayaf"))
async def cmd_bot_zayaf(message: Message):
    task = asyncio.create_task(process_zayaf_async(message))
    _running_tasks.add(task)
    task.add_done_callback(_running_tasks.discard)

async def process_zayaf_async(message: Message):
    chat_id = message.chat.id
    user_id = message.from_user.id
    str_uid = str(user_id)

    client = None
    async with get_db_client() as db:
        cursor = await db.execute("SELECT account_id FROM user_sessions WHERE user_id = ? AND status = 'active'", (str_uid,))
        row = (cursor.rows[0] if cursor.rows else None)

    if row and row[0] in userbot_clients:
        client = userbot_clients[row[0]]
    else:
        for acc_id, cl in userbot_clients.items():
            if cl and cl.is_connected():
                client = cl
                break

    if not client:
        await message.reply("❌ Zayafkalarni tasdiqlash uchun faol userbot akkaunti topilmadi.")
        return

    try:
        peer = await safe_get_entity(client, chat_id)
        count = 0
        try:
            res_importers = await client(GetChatInviteImportersRequest(
                peer=peer,
                requested=True,
                limit=100,
                offset_user=InputUserEmpty(),
                offset_date=None
            ))
            count = getattr(res_importers, 'count', len(getattr(res_importers, 'importers', [])))
        except Exception:
            count = 0

        await client(HideAllChatJoinRequestsRequest(
            peer=peer,
            approved=True
        ))

        if count > 0:
            await message.reply(f"✅ {count} ta kutilayotgan a'zolik so'rovlari (zayafkalar) qabul qilindi.")
        else:
            await message.reply("✅ Barcha kutilayotgan a'zolik so'rovlari (zayafkalar) qabul qilindi.")
    except Exception as e:
        log.error(f"Error in zayaf command: {e}")
        try:
            await message.reply(f"❌ Zayafkalarni tasdiqlashda xatolik: {e}")
        except Exception:
            pass

async def run_bot_api_utag(chat_id: int, user_id: int, client, suffix: str):
    delay = 1.7
    tagged_count = 0
    try:
        users_to_tag = []
        if client:
            async for user in client.iter_participants(chat_id):
                if not user.bot and not getattr(user, 'deleted', False):
                    users_to_tag.append(user)
        else:
            try:
                admins = await bot.get_chat_administrators(chat_id)
                for adm in admins:
                    if not adm.user.is_bot:
                        users_to_tag.append(adm.user)
            except Exception:
                pass

        if not users_to_tag:
            await bot.send_message(chat_id, "⚠️ Guruhda tag qilinadigan foydalanuvchilar topilmadi.")
            return

        for user in users_to_tag:
            target_user = getattr(user, 'user', user)
            uid_str = str(target_user.id)
            vip_info = VIP_USERS_CACHE.get(uid_str)
            if vip_info and vip_info.get("is_skipped"):
                continue

            custom_name = vip_info.get("custom_name") if vip_info else None
            first_name = custom_name or getattr(target_user, 'first_name', None) or 'Foydalanuvchi'
            first_name_clean = first_name.replace("&", "&amp;").replace("<", "&lt;").replace(">", "&gt;")

            username = getattr(target_user, 'username', None)
            if custom_name:
                mention_text = f"<a href='tg://user?id={target_user.id}'>{first_name_clean}</a>"
            elif username:
                mention_text = f"@{username}"
            else:
                mention_text = f"<a href='tg://user?id={target_user.id}'>{first_name_clean}</a>"

            tag_msg = f"{mention_text} {suffix}".strip()
            tag_msg = make_text_unique(tag_msg)

            try:
                await bot.send_message(chat_id, f_t(tag_msg))
                tagged_count += 1
            except Exception as e:
                log.error(f"Error in run_bot_api_utag send_message: {e}")
                continue
            await asyncio.sleep(delay)
    except asyncio.CancelledError:
        pass
    finally:
        bot_active_utags.pop(chat_id, None)
        try:
            await bot.send_message(chat_id, f"uTag @{bot_username}  orqali  yakunlandi🛑")
        except Exception:
            pass
        if tagged_count > 0:
            await inc_usage_stat("utag", tagged_count)

@dp.message(Command("lang"), F.chat.type == "private")
async def cmd_lang(message: Message, state: FSMContext):
    await state.clear()
    await message.answer(
        f_t("🌐 <b>Tilni tanlang / Choose language:</b>"),
        reply_markup=get_language_keyboard()
    )

TERMS_OF_SERVICE_TEXT = (
    "<b>AlphaUserBot — Foydalanuvchi shartlari</b>\n\n"
    "Botdan foydalanish orqali siz quyidagi shartlarga to‘liq va shartsiz rozilik bildirasiz:\n\n"
    "1. Siz Telegram’ning barcha qoidalariga (Terms of Service va Privacy Policy) qat’iy rioya qilishga majbursiz.\n"
    "2. Bot orqali spam, ommaviy taglash, reyd, ruxsatsiz ma’lumot yig‘ish yoki boshqa foydalanuvchilarni bezovta qilish qat’iyan taqiqlanadi.\n"
    "3. Siz o‘z Telegram akkauntingizni ulash orqali barcha xavf-xatarlarni (band qilish, cheklash, o‘chirish, ma’lumot yo‘qolishi) to‘liq o‘z zimmangizga olasiz.\n"
    "4. Ma’lumotlar maxfiyligi: Sizning shaxsiy ma’lumotlaringiz, session ma’lumotlaringiz va botdagi faoliyatingiz hech qachon uchinchi shaxslarga berilmaydi, sotilmaydi yoki ulashilmaydi.\n"
    "5. Javobgarlikni cheklash: Bot egasi hech qanday holda javobgar emas (akkaunt ban, zarar, nizolar, xizmat to‘xtashi va h.k.).\n"
    "6. Siz botdan foydalanish natijasida yuzaga kelgan barcha da’vo va shikoyatlardan bot egasini ozod qilasiz.\n"
    "7. Taqiqlangan harakatlar aniqlansa, foydalanuvchi ogohlantirishsiz butunlay bloklanadi.\n"
    "8. Xizmat istalgan vaqtda ogohlantirishsiz o‘zgartirilishi yoki to‘xtatilishi mumkin.\n"
    "9. Ushbu shartlarga rozi bo‘lmasangiz — botdan darhol foydalanishni to‘xtating.\n\n"
    "<i>Botdan foydalanishni davom ettirish ushbu shartlarga to‘liq rozilik bildirish deb hisoblanadi.</i>"
)

async def show_terms_of_service(target: Any, user_id: int):
    kb = InlineKeyboardBuilder()
    kb.row(colored_btn("«Roziman»", "accept_terms_of_service", "success"))

    formatted_text = f_t(TERMS_OF_SERVICE_TEXT)
    markup = kb.as_markup()

    if isinstance(target, CallbackQuery):
        try:
            await target.message.edit_text(formatted_text, reply_markup=markup)
        except Exception:
            await target.message.answer(formatted_text, reply_markup=markup)
    elif isinstance(target, Message):
        await target.answer(formatted_text, reply_markup=markup)
    else:
        await bot.send_message(user_id, formatted_text, reply_markup=markup)

@dp.callback_query(F.data == "accept_terms_of_service")
async def cb_accept_terms_of_service(callback: CallbackQuery, state: FSMContext):
    user_id = callback.from_user.id
    await update_user_db(user_id, {"terms_accepted": 1})
    await callback.answer("✅ Shartlar qabul qilindi!")
    await show_main_menu(callback, user_id)

@dp.callback_query(F.data.startswith("set_lang_"))
async def cb_set_lang(callback: CallbackQuery, state: FSMContext):
    lang_code = callback.data.replace("set_lang_", "")
    user_id = callback.from_user.id
    await update_user_db(user_id, {"lang": lang_code})

    u = await ensure_user(user_id)
    if not u.get("terms_accepted"):
        await show_terms_of_service(callback, user_id)
    else:
        await callback.answer("✅ Til o'zgartirildi!")
        await show_main_menu(callback, user_id)

@dp.message(Command("start"), F.chat.type == "private")
async def cmd_start(message: Message, state: FSMContext):
    await state.clear()
    user_id = message.from_user.id
    u = await ensure_user(user_id, message.from_user.username, message.from_user.first_name)

    if MAINTENANCE_MODE and user_id != OWNER_ID:
        await message.answer("🔧 Texnik ishlar olib borilmoqda. Qisqa vaqtda tuzatiladi.")
        return

    if not u.get("terms_accepted"):
        await message.answer(
            f_t("🌐 <b>Tilni tanlang / Choose language:</b>"),
            reply_markup=get_language_keyboard()
        )
        return

    await show_main_menu(message, user_id)

@dp.callback_query(F.data == "btn_userbot")
@dp.callback_query(F.data == "main_menu")
async def cb_main_menu(callback: CallbackQuery, state: FSMContext):
    await state.clear()
    await show_main_menu(callback, callback.from_user.id)

@dp.callback_query(F.data == "btn_qollanma")
async def cb_btn_qollanma(callback: CallbackQuery):
    text = (
        "🛡 Qoidalar va Foydalanish Qo'llanmasi\n\n"
        '"""Xavfsizlik eslatmalari:\n'
        "1. Bir vaqtda faqat 1 ta akkaunt ulashingiz mumkin.\n"
        "2. uTag tezligini juda tez qilmaslik tavsiya etiladi.\n"
        '3. Telethon seansingiz bekor qilinganda u avtomatik tozalanadi."""'
    )
    kb = InlineKeyboardBuilder()
    kb.row(colored_btn("Orqaga", "btn_userbot", "danger"))
    await callback.message.edit_text(f_t(text), reply_markup=kb.as_markup())

# ---------------------------------------------------------------
# LOGIN SYSTEM (1 USER = 1 ACCOUNT LIMIT, SMS & QR CODE)
# ---------------------------------------------------------------
async def show_add_account_screen(event: Any, state: Optional[FSMContext] = None):
    user_id = getattr(event, "from_user", None).id if getattr(event, "from_user", None) else None
    if user_id and await has_active_account(user_id):
        alert_text = "⚠️ Siz allaqachon 1 ta akkaunt ulangansiz! Har bir foydalanuvchi faqat 1 ta akkaunt ulay oladi."
        if isinstance(event, CallbackQuery):
            await event.answer(alert_text, show_alert=True)
            return
        elif isinstance(event, Message):
            await event.answer(f_t(alert_text))
            return

    text = (
        "👤 <b>Akkaunt qo‘shish</b>\n\n"
        "<blockquote>🔒 Akkauntingiz FAQAT userbotingiz ishlashi uchun kerak. Shaxsiy yozishmalaringiz o‘qilmaydi va saqlanmaydi.</blockquote>\n\n"
        "Login usulini tanlang:"
    )
    kb = InlineKeyboardBuilder()
    kb.row(colored_btn("SMS kod orqali", "select_login_sms", style="primary", emoji="📱"))
    kb.row(colored_btn("QR kod orqali", "select_login_qr", style="primary"))
    kb.row(colored_btn("Orqaga", "btn_userbot", style="danger"))

    msg_text = f_t(text)
    markup = kb.as_markup()

    if isinstance(event, CallbackQuery):
        try:
            await event.message.edit_text(msg_text, reply_markup=markup)
        except Exception:
            await event.message.answer(msg_text, reply_markup=markup)
    else:
        await event.answer(msg_text, reply_markup=markup)

@dp.callback_query(F.data == "add_main_account")
async def cb_add_main_account(callback: CallbackQuery, state: FSMContext):
    await show_add_account_screen(callback, state)

async def cleanup_user_temp_login(user_id: int, state: FSMContext):
    if user_id in qr_login_tasks:
        t = qr_login_tasks.pop(user_id, None)
        if t and not t.done():
            t.cancel()
    data = await state.get_data()
    cl = data.get("temp_client")
    if cl:
        try:
            await cl.disconnect()
        except Exception:
            pass

def build_sms_keypad(current_code: str) -> InlineKeyboardMarkup:
    kb = InlineKeyboardBuilder()
    kb.row(
        colored_btn("1", "keypad_1", "primary"),
        colored_btn("2", "keypad_2", "primary"),
        colored_btn("3", "keypad_3", "primary")
    )
    kb.row(
        colored_btn("4", "keypad_4", "primary"),
        colored_btn("5", "keypad_5", "primary"),
        colored_btn("6", "keypad_6", "primary")
    )
    kb.row(
        colored_btn("7", "keypad_7", "primary"),
        colored_btn("8", "keypad_8", "primary"),
        colored_btn("9", "keypad_9", "primary")
    )
    kb.row(
        colored_btn("🗑 Tozalash", "keypad_clear", "danger"),
        colored_btn("0", "keypad_0", "primary"),
        colored_btn("⌫ O'chirish", "keypad_backspace", "danger")
    )

    if len(current_code) == 5:
        kb.row(colored_btn("✅ Tasdiqlash", "keypad_submit", "success"))

    kb.row(colored_btn("Orqaga", "btn_userbot", "danger"))
    return kb.as_markup()

@dp.callback_query(F.data == "select_login_sms")
async def cb_select_login_sms(callback: CallbackQuery, state: FSMContext):
    user_id = callback.from_user.id
    if await has_active_account(user_id):
        await callback.answer("Siz allaqachon 1 ta akkaunt ulangansiz!", show_alert=True)
        return

    await cleanup_user_temp_login(user_id, state)
    await state.clear()

    await callback.message.edit_text(
        f_t("📱 <b>Telefon raqamingizni kiriting:</b>\n\nMasalan: <code>+998901234567</code>"),
        reply_markup=InlineKeyboardMarkup(inline_keyboard=[[colored_btn("Orqaga", "btn_userbot", "danger")]])
    )
    await state.set_state(UserStatesGroup.login_phone)

@dp.message(StateFilter(UserStatesGroup.login_phone), F.contact | F.text)
async def phone_input_handler(message: Message, state: FSMContext):
    if message.contact:
        phone = message.contact.phone_number
    else:
        phone = message.text.strip().replace(" ", "")

    if not phone.startswith("+"):
        phone = "+" + phone

    if await is_phone_already_connected(phone):
        await message.answer(f_t("⚠️ <b>Ushbu telefon raqami allaqachon botga ulangan!</b>"))
        return

    await state.update_data(phone=phone, login_code_digits="")
    rm_msg = await message.answer(f_t("⏳ Telegram kod yubormoqda..."), reply_markup=ReplyKeyboardRemove())

    client = TelegramClient(StringSession(), API_ID, API_HASH)
    try:
        await client.connect()
        sent_code = await client.send_code_request(phone)
        await state.update_data(temp_client=client, phone_code_hash=sent_code.phone_code_hash)
        await state.set_state(UserStatesGroup.login_code)

        try:
            await rm_msg.delete()
        except Exception:
            pass

        code_text = "Telegram'dan kelgan kodni kiriting\nKod: _"
        await message.answer(code_text, reply_markup=build_sms_keypad(""))
    except Exception as e:
        try:
            await rm_msg.delete()
        except Exception:
            pass
        await message.answer(format_user_error(e))

@dp.callback_query(StateFilter(UserStatesGroup.login_code), F.data.startswith("keypad_"))
async def cb_keypad_press(callback: CallbackQuery, state: FSMContext):
    data = await state.get_data()
    action = callback.data.replace("keypad_", "")
    digits = data.get("login_code_digits", "")

    if action in ["0", "1", "2", "3", "4", "5", "6", "7", "8", "9"]:
        if len(digits) < 5:
            digits += action
    elif action == "clear":
        digits = ""
    elif action == "backspace":
        digits = digits[:-1]
    elif action == "submit":
        if len(digits) == 5:
            await process_sms_code_verification(callback.from_user.id, callback.message, state, digits)
            await callback.answer()
            return
        else:
            await callback.answer("⚠️ Kod 5 xonali bo'lishi kerak!", show_alert=True)
            return

    await state.update_data(login_code_digits=digits)
    display_code = (digits + "_") if len(digits) < 5 else digits
    text = f"Telegram'dan kelgan kodni kiriting\nKod: {display_code}"

    try:
        await callback.message.edit_text(text, reply_markup=build_sms_keypad(digits))
    except Exception:
        pass
    await callback.answer()

async def finalize_login(user_id: int, client: TelegramClient, phone: str):
    str_uid = str(user_id)
    session_str = client.session.save()
    me = await client.get_me()
    acc_id = str(me.id)
    name = get_display_name(me)
    phone_to_check = phone or getattr(me, 'phone', '') or ''

    if await is_phone_already_connected(phone_to_check):
        async with get_db_client() as db:
            cursor = await db.execute("SELECT account_id, user_id FROM user_sessions WHERE account_id = ?", (acc_id,))
            existing = (cursor.rows[0] if cursor.rows else None)
        if not existing or str(existing[1]) != str_uid:
            try:
                await client.disconnect()
            except Exception:
                pass
            raise ValueError("Ushbu telefon raqami allaqachon faol ulangan!")

    async with get_db_client() as db:
        await db.execute("""
        INSERT OR REPLACE INTO user_sessions (user_id, account_id, name, phone, session, status, added_at)
        VALUES (?, ?, ?, ?, ?, 'active', ?)
        """, (str_uid, acc_id, name, phone, session_str, datetime.now(timezone.utc).isoformat()))

    userbot_clients[acc_id] = client
    await register_userbot_handlers(client)

async def process_sms_code_verification(user_id: int, message_or_msg, state: FSMContext, code: str):
    data = await state.get_data()
    client = data.get("temp_client")
    phone = data.get("phone")
    phone_code_hash = data.get("phone_code_hash")

    if not client:
        await bot.send_message(user_id, "⚠️ Sessiya topilmadi. Qaytadan boshlang.")
        await state.clear()
        return

    try:
        await client.sign_in(phone, code, phone_code_hash=phone_code_hash)
        await finalize_login(user_id, client, phone)
        try:
            if isinstance(message_or_msg, CallbackQuery):
                await message_or_msg.message.delete()
            elif hasattr(message_or_msg, "delete") and callable(message_or_msg.delete):
                await message_or_msg.delete()
        except Exception:
            pass
        await bot.send_message(user_id, f_t("✅ Akkaunt muvaffaqiyatli ulandi!"), reply_markup=get_userbot_keyboard(user_id))
        await state.clear()
    except errors.SessionPasswordNeededError:
        await state.set_state(UserStatesGroup.login_2fa)
        try:
            if isinstance(message_or_msg, CallbackQuery):
                await message_or_msg.message.delete()
            elif hasattr(message_or_msg, "delete") and callable(message_or_msg.delete):
                await message_or_msg.delete()
        except Exception:
            pass
        await bot.send_message(user_id, f_t("🔒 <b>2FA Paroli aniqlandi.</b>\nTasdiqlash uchun parolingizni yuboring:"))
    except Exception as e:
        await state.update_data(login_code_digits="")
        err_msg = format_user_error(e)
        code_text = f"{err_msg}\n\nTelegram'dan kelgan kodni kiriting\nKod: _"
        try:
            target_msg = message_or_msg.message if isinstance(message_or_msg, CallbackQuery) else message_or_msg
            await target_msg.edit_text(code_text, reply_markup=build_sms_keypad(""))
        except Exception:
            await bot.send_message(user_id, code_text, reply_markup=build_sms_keypad(""))

@dp.message(StateFilter(UserStatesGroup.login_code), F.text)
async def login_code_verify(message: Message, state: FSMContext):
    code = message.text.strip().replace(" ", "").replace("-", "")
    if code.isdigit():
        code_digits = code[:5]
        await state.update_data(login_code_digits=code_digits)
        if len(code_digits) == 5:
            try:
                await message.delete()
            except Exception:
                pass
            await process_sms_code_verification(message.from_user.id, message, state, code_digits)
        else:
            display_code = (code_digits + "_") if len(code_digits) < 5 else code_digits
            await message.answer(f"Telegram'dan kelgan kodni kiriting\nKod: {display_code}", reply_markup=build_sms_keypad(code_digits))

@dp.message(StateFilter(UserStatesGroup.login_2fa), F.text)
async def login_2fa_verify(message: Message, state: FSMContext):
    password = message.text.strip()
    data = await state.get_data()
    client = data.get("temp_client")
    phone = data.get("phone")

    try:
        await client.sign_in(password=password)
        await finalize_login(message.from_user.id, client, phone)
        await message.answer(f_t("✅ Akkaunt muvaffaqiyatli ulandi!"), reply_markup=get_userbot_keyboard(message.from_user.id))
        await state.clear()
    except Exception as e:
        await message.answer(format_user_error(e))

# QR CODE LOGIN
def make_qr_input_file(url: str) -> BufferedInputFile:
    img = qrcode.make(url)
    img_byte_arr = io.BytesIO()
    img.save(img_byte_arr, format='PNG')
    img_byte_arr.seek(0)
    return BufferedInputFile(img_byte_arr.getvalue(), filename="qr.png")

@dp.callback_query(F.data == "select_login_qr")
async def cb_select_login_qr(callback: CallbackQuery, state: FSMContext):
    user_id = callback.from_user.id
    if await has_active_account(user_id):
        await callback.answer("Siz allaqachon 1 ta akkaunt ulangansiz!", show_alert=True)
        return

    await cleanup_user_temp_login(user_id, state)
    await state.clear()

    client = TelegramClient(StringSession(), API_ID, API_HASH)
    await client.connect()

    try:
        qr = await client.qr_login()
    except Exception as e:
        await client.disconnect()
        await callback.message.edit_text(f"❌ QR login xatoligi: {e}")
        return

    await state.update_data(temp_client=client)

    qr_text = (
        "📸 <b>QR kod orqali kirish</b>\n\n"
        "1. Telegram ilovasini oching.\n"
        "2. Sozlamalar -> Qurilmalar -> Qurilmani ulash tugmasini bosing.\n"
        "3. Ushbu QR kodni skanerlang."
    )
    kb = InlineKeyboardMarkup(inline_keyboard=[
        [colored_btn("🔄 Kodni yangilash", "refresh_qr_code", "primary")],
        [colored_btn("Orqaga", "btn_userbot", "danger")]
    ])

    sent_qr_msg = await callback.message.answer_photo(
        photo=make_qr_input_file(qr.url),
        caption=f_t(qr_text),
        reply_markup=kb
    )
    try:
        await callback.message.delete()
    except Exception:
        pass

    async def qr_poll_worker():
        try:
            start_ts = time.time()
            while time.time() - start_ts < 120:
                try:
                    res = await qr.wait(timeout=5)
                    if res:
                        await finalize_login(user_id, client, getattr(res, 'phone', '') or '')
                        try:
                            await sent_qr_msg.delete()
                        except Exception:
                            pass
                        await bot.send_message(user_id, f_t("✅ QR kod orqali akkaunt muvaffaqiyatli ulandi!"), reply_markup=get_userbot_keyboard(user_id))
                        await state.clear()
                        return
                except asyncio.TimeoutError:
                    continue
                except errors.SessionPasswordNeededError:
                    try:
                        await sent_qr_msg.delete()
                    except Exception:
                        pass
                    await state.set_state(UserStatesGroup.login_2fa)
                    await bot.send_message(user_id, f_t("🔒 <b>2FA Paroli aniqlandi.</b>\nTasdiqlash uchun parolingizni yuboring:"))
                    return
                except (errors.AuthTokenExpiredError, errors.AuthTokenInvalidError, errors.AuthTokenAlreadyAcceptedError):
                    if time.time() - start_ts < 110:
                        try:
                            await qr.recreate()
                            await sent_qr_msg.edit_media(
                                media=InputMediaPhoto(media=make_qr_input_file(qr.url), caption=f_t(qr_text)),
                                reply_markup=kb
                            )
                        except Exception as rec_err:
                            log.error(f"QR recreate error: {rec_err}")
                            break
                    else:
                        break
                except Exception as poll_err:
                    log.error(f"QR Login poll error: {poll_err}")
                    await asyncio.sleep(2)
        except asyncio.CancelledError:
            pass
        except Exception as outer_e:
            log.error(f"QR worker error: {outer_e}")

    task = asyncio.create_task(qr_poll_worker())
    qr_login_tasks[user_id] = task

@dp.callback_query(F.data == "refresh_qr_code")
async def cb_refresh_qr_code(callback: CallbackQuery, state: FSMContext):
    await callback.answer("🔄 QR Kod yangilanmoqda...")
    await cb_select_login_qr(callback, state)

# ---------------------------------------------------------------
# PROFIL PANEL (VIEW & DELETE CONNECTED ACCOUNT)
# ---------------------------------------------------------------
@dp.callback_query(F.data == "menu_profil_panel")
async def cb_menu_profil_panel(callback: CallbackQuery, state: FSMContext):
    if state:
        await state.clear()
    user_id = callback.from_user.id
    str_uid = str(user_id)
    await ensure_user(user_id, callback.from_user.username, callback.from_user.first_name)

    async with get_db_client() as db:
        cursor = await db.execute("SELECT account_id, name, phone FROM user_sessions WHERE user_id = ?", (str_uid,))
        sessions = cursor.rows

    ulangan_soni = len(sessions)
    text = f"Profillar ({ulangan_soni}/1)\n\nAkkaunt tanlash yoki boshqarish:"

    kb = InlineKeyboardBuilder()
    for aid, name, phone in sessions:
        acc_label = name or phone or f"Akkaunt {aid}"
        kb.row(
            colored_btn(f"📱 {acc_label}", f"profile_acc_info_{aid}", "primary"),
            colored_btn("🗑", f"profile_del_acc_{aid}", "danger")
        )

    if ulangan_soni == 0:
        kb.row(colored_btn("Akkaunt qo‘shish", "add_main_account", "success"))

    kb.row(colored_btn("Orqaga", "btn_userbot", "danger"))

    msg_text = f_t(text)
    markup = kb.as_markup()

    try:
        await callback.message.edit_text(msg_text, reply_markup=markup)
    except Exception:
        await callback.message.answer(msg_text, reply_markup=markup)

@dp.callback_query(F.data.startswith("profile_del_acc_"))
async def cb_profile_del_acc(callback: CallbackQuery, state: FSMContext):
    aid = callback.data.replace("profile_del_acc_", "")
    user_id = callback.from_user.id
    str_uid = str(user_id)

    async with get_db_client() as db:
        await db.execute("DELETE FROM user_sessions WHERE account_id = ? AND user_id = ?", (aid, str_uid))

    if aid in userbot_clients:
        cl = userbot_clients.pop(aid, None)
        if cl:
            try:
                await cl.disconnect()
            except Exception:
                pass

    account_user_cache.pop(aid, None)
    await callback.answer("✅ Akkaunt muvaffaqiyatli o'chirildi!", show_alert=True)
    await cb_menu_profil_panel(callback, state)

@dp.callback_query(F.data.startswith("profile_acc_info_"))
async def cb_profile_acc_info(callback: CallbackQuery):
    aid = callback.data.replace("profile_acc_info_", "")
    async with get_db_client() as db:
        cursor = await db.execute("SELECT name, phone, status FROM user_sessions WHERE account_id = ?", (aid,))
        row = (cursor.rows[0] if cursor.rows else None)

    if row:
        name, phone, status = row
        status_str = "🟢 Faol" if status == "active" else "🔴 Nofaol"
        await callback.answer(f"📱 {name or 'Akkaunt'}\n☎️ {phone or 'Noma\'lum'}\n🚦 Status: {status_str}", show_alert=True)
    else:
        await callback.answer("Akkaunt topilmadi", show_alert=True)

# ---------------------------------------------------------------
# AVTO UTAG SYSTEM
# ---------------------------------------------------------------
@dp.callback_query(F.data == "menu_avto_utag")
async def cb_menu_avto_utag(callback: CallbackQuery, state: FSMContext):
    user_id = callback.from_user.id
    if not await has_active_account(user_id):
        await callback.answer("Akkaunt ulanmagan", show_alert=True)
        await show_add_account_screen(callback, state)
        return

    await state.clear()

    text = (
        "<b>Avto uTag</b>\n\n"
        "Guruhlarda quyidagi usullar bilan ishlatishingiz mumkin:\n\n"
        "1. Userbot orqali:\n"
        "• .u     — oddiy uTag\n"
        "• .ru    — random so‘zlar bilan\n"
        "• .su    — to‘xtatish\n\n"
        "2. Botni guruhga qo‘shib:\n"
        "• /u     — oddiy uTag\n"
        "• /su    — to‘xtatish"
    )

    kb = InlineKeyboardBuilder()
    kb.row(colored_btn("🎲 Random so'zlar", "utag_random_sozlar_0", "primary"))
    kb.row(colored_btn("Orqaga", "main_menu", "danger"))

    msg_text = f_t(text)
    markup = kb.as_markup()
    try:
        await callback.message.edit_text(msg_text, reply_markup=markup)
    except Exception:
        await callback.message.answer(msg_text, reply_markup=markup)

@dp.callback_query(F.data == "utag_tezlik_menu")
async def cb_utag_tezlik_menu(callback: CallbackQuery):
    user_id = callback.from_user.id
    u = await ensure_user(user_id)
    cur_speed = max(2.0, u.get("userbot_speed", 2.0))

    kb = InlineKeyboardBuilder()
    speed_list = [2.0, 2.5, 3.0, 3.5, 4.0, 5.0]
    row_btns = []
    for sp in speed_list:
        label = f"• {sp} •" if sp == cur_speed else str(sp)
        row_btns.append(colored_btn(label, f"set_utag_speed_{sp}", "primary"))

    kb.row(*row_btns[:4])
    kb.row(*row_btns[4:])
    kb.row(colored_btn("Orqaga", "menu_avto_utag", "danger"))

    await callback.message.edit_text(f_t("⏱ <b>Utag tezligini tanlang:</b>"), reply_markup=kb.as_markup())

@dp.callback_query(F.data.startswith("set_utag_speed_"))
async def cb_set_utag_speed(callback: CallbackQuery, state: FSMContext):
    user_id = callback.from_user.id
    sp_val = float(callback.data.replace("set_utag_speed_", ""))
    await update_user_db(user_id, {"userbot_speed": sp_val})
    await callback.answer(f"✅ Tezlik {sp_val}s ga o'zgartirildi!")
    await cb_menu_avto_utag(callback, state)

SPEED_OPTIONS = [2.0, 2.5, 3.0, 3.5, 4.0, 5.0]

@dp.callback_query(F.data == "toggle_utag_speed")
async def cb_toggle_utag_speed(callback: CallbackQuery, state: FSMContext):
    user_id = callback.from_user.id
    u = await ensure_user(user_id)
    current_speed = max(2.0, u.get("userbot_speed", 2.0))
    if current_speed in SPEED_OPTIONS:
        curr_idx = SPEED_OPTIONS.index(current_speed)
        new_speed = SPEED_OPTIONS[(curr_idx + 1) % len(SPEED_OPTIONS)]
    else:
        new_speed = 2.0
    await update_user_db(user_id, {"userbot_speed": new_speed})
    await callback.answer(f"⚡️ Tezlik {new_speed}s ga o'zgartirildi!")
    await cb_menu_avto_utag(callback, state)

@dp.callback_query(F.data == "utag_tarix_menu")
async def cb_utag_tarix_menu(callback: CallbackQuery):
    user_id = callback.from_user.id
    u = await ensure_user(user_id)
    raw_hist = u.get("utag_history", "[]")
    try:
        history = json.loads(raw_hist) if isinstance(raw_hist, str) else (raw_hist or [])
    except Exception:
        history = []

    text = "📜 <b>So'nggi 5 ta uTag qilingan guruhlar:</b>\n\n"
    kb = InlineKeyboardBuilder()

    if not history:
        text += "<i>Hali hech qanday guruh uTag qilinmagan.</i>"
    else:
        for idx, item in enumerate(history[:5]):
            chat_title = item.get("title", f"Guruh {idx+1}")
            kb.row(colored_btn(f"👥 {chat_title[:30]}", f"utag_hist_pick_{idx}", "primary"))

    kb.row(colored_btn("Orqaga", "menu_avto_utag", "danger"))
    await callback.message.edit_text(f_t(text), reply_markup=kb.as_markup())

@dp.callback_query(F.data.startswith("utag_hist_pick_"))
async def cb_utag_hist_pick(callback: CallbackQuery, state: FSMContext):
    idx = int(callback.data.replace("utag_hist_pick_", ""))
    user_id = callback.from_user.id
    u = await ensure_user(user_id)
    raw_hist = u.get("utag_history", "[]")
    try:
        history = json.loads(raw_hist) if isinstance(raw_hist, str) else (raw_hist or [])
    except Exception:
        history = []

    if 0 <= idx < len(history):
        target_chat = history[idx].get("target")
        await state.update_data(target_chat_id=target_chat)
        await cb_start_utag_now(callback, state)
    else:
        await callback.answer("⚠️ Guruh topilmadi.", show_alert=True)

@dp.callback_query(F.data == "utag_random_menu")
@dp.callback_query(F.data.startswith("utag_random_sozlar_"))
async def cb_utag_random_sozlar(callback: CallbackQuery, state: FSMContext):
    if state: await state.clear()
    user_id = callback.from_user.id
    str_uid = str(user_id)
    try:
        page = int(callback.data.split("_")[3])
    except Exception:
        page = 0

    async with get_db_client() as db:
        cursor = await db.execute("SELECT id, word FROM user_random_words WHERE user_id = ?", (str_uid,))
        user_words = cursor.rows

    kb = InlineKeyboardBuilder()
    per_page = 20
    total_pages = max(1, (len(user_words) + per_page - 1) // per_page)
    if page >= total_pages: page = total_pages - 1
    if page < 0: page = 0

    start = page * per_page
    end = start + per_page
    page_items = user_words[start:end]

    text = f"🎲 <b>Mening Random So'zlarim (Sahifa {page+1}/{total_pages}):</b>\n"
    if not user_words:
        text += "\n⚠️ Hozircha hech qanday so'z mavjud emas."
    else:
        text += "O'chirish uchun kerakli so'z ustiga bosing:\n\n"
        for idx, (word_id, word) in enumerate(page_items, start + 1):
            word_clean, _ = decode_premium_emojis(word)
            clean_w = re.sub('<[^<]+?>', '', word_clean)
            kb.row(colored_btn(f"❌ {clean_w[:28]}", f"utag_del_word_{word_id}_{page}", "primary"))

    nav_buttons = []
    if page > 0:
        nav_buttons.append(colored_btn("◀️ Oldingi", f"utag_random_sozlar_{page-1}", "primary"))
    if end < len(user_words):
        nav_buttons.append(colored_btn("Keyingi ▶️", f"utag_random_sozlar_{page+1}", "primary"))
    if nav_buttons:
        kb.row(*nav_buttons)

    kb.row(colored_btn("➕ So'z qo'shish", "utag_add_custom_word_prompt", "success"))
    kb.row(colored_btn("Orqaga", "menu_avto_utag", "danger"))

    try:
        await callback.message.edit_text(f_t(text), reply_markup=kb.as_markup())
    except Exception:
        pass

@dp.callback_query(F.data.startswith("utag_del_word_"))
async def cb_utag_del_word(callback: CallbackQuery, state: FSMContext):
    user_id = callback.from_user.id
    str_uid = str(user_id)
    parts = callback.data.split("_")
    try:
        word_id = int(parts[3])
        page = int(parts[4]) if len(parts) > 4 else 0
    except Exception:
        word_id = 0
        page = 0

    async with get_db_client() as db:
        await db.execute("DELETE FROM user_random_words WHERE id = ? AND user_id = ?", (word_id, str_uid))
    await callback.answer("✅ So'z muvaffaqiyatli o'chirildi!", show_alert=True)

    callback.data = f"utag_random_sozlar_{page}"
    await cb_utag_random_sozlar(callback, state)

@dp.callback_query(F.data == "utag_add_custom_word_prompt")
async def cb_utag_add_custom_word_prompt(callback: CallbackQuery, state: FSMContext):
    await callback.message.edit_text(f_t("✍️ <b>Yangi uTag random so'zini yuboring:</b>"))
    await state.set_state(UserStatesGroup.add_custom_word)

@dp.message(StateFilter(UserStatesGroup.add_custom_word), F.text)
async def msg_utag_add_custom_word(message: Message, state: FSMContext):
    user_id = message.from_user.id
    str_uid = str(user_id)
    await state.clear()

    word = aiogram_to_premium_encoded(message).strip()
    if word:
        async with get_db_client() as db:
            await db.execute("INSERT INTO user_random_words (user_id, word) VALUES (?, ?)", (str_uid, word))

    kb = InlineKeyboardBuilder()
    kb.row(
        colored_btn("Ha", "add_word_yes", "success"),
        colored_btn("Yo‘q", "add_word_no", "danger")
    )
    await message.answer(
        f_t("✅ Qabul qilindi. Yana qo‘shamizmi?"),
        reply_markup=kb.as_markup()
    )

@dp.callback_query(F.data == "add_word_yes")
async def cb_add_word_yes(callback: CallbackQuery, state: FSMContext):
    await state.set_state(UserStatesGroup.add_custom_word)
    await callback.message.edit_text(f_t("✍️ <b>Yangi uTag random so'zini yuboring:</b>"))

@dp.callback_query(F.data == "add_word_no")
async def cb_add_word_no(callback: CallbackQuery, state: FSMContext):
    await state.clear()
    callback.data = "utag_random_sozlar_0"
    await cb_utag_random_sozlar(callback, state)

@dp.callback_query(F.data == "utag_yangi_boshlash")
async def cb_utag_yangi_boshlash(callback: CallbackQuery, state: FSMContext):
    await callback.message.delete()
    await bot.send_message(
        callback.from_user.id,
        f_t("🔗 Qaysi guruhda uTag qilmoqchisiz? Guruh tanlang yoki havolasini yuboring:"),
        reply_markup=get_group_selection_keyboard()
    )
    await state.set_state(UserStatesGroup.utag_group_link)

@dp.message(StateFilter(UserStatesGroup.utag_group_link), F.text)
async def utag_group_link_manual_text(message: Message, state: FSMContext):
    link_input = message.text.strip()
    target_chat = parse_telegram_link(link_input)
    await state.update_data(target_chat_id=target_chat)
    try:
        rem_msg = await message.answer("...", reply_markup=ReplyKeyboardRemove())
        await rem_msg.delete()
    except Exception:
        pass
    await message.answer(
        f_t(f"ℹ️ <b>Guruh Havolasi/ID Qabul Qilindi:</b>\n<code>{target_chat}</code>\n\nBoshlashni istaysizmi?"),
        reply_markup=InlineKeyboardMarkup(inline_keyboard=[
            [colored_btn("Boshlash", "start_utag_now", "success"), colored_btn("Bekor qilish", "menu_avto_utag", "danger")]
        ])
    )

@dp.callback_query(F.data == "start_utag_now")
async def cb_start_utag_now(callback: CallbackQuery, state: FSMContext):
    kb = InlineKeyboardBuilder()
    kb.row(colored_btn("@foydalanuvchi ozi", "utag_regime_self", "primary"))
    kb.row(colored_btn("Tasodifiy sozlar (bot)", "utag_regime_random", "primary"))
    kb.row(colored_btn("Oz matnim b-n", "utag_regime_custom", "primary"))
    await callback.message.edit_text(f_t("🛠 <b>Tag rejimini tanlang:</b>"), reply_markup=kb.as_markup())

@dp.callback_query(F.data.startswith("utag_regime_"))
async def cb_utag_regime_select(callback: CallbackQuery, state: FSMContext):
    regime = callback.data.split("_")[2]
    await state.update_data(regime=regime)

    if regime == "custom":
        await callback.message.edit_text(f_t("✍️ <b>Tag qilinganda foydalanuvchi ismi yonidan chiqadigan matnni yuboring:</b>"))
        await state.set_state(UserStatesGroup.utag_custom_suffix)
    else:
        await run_utag_process(callback, state)

@dp.message(StateFilter(UserStatesGroup.utag_custom_suffix), F.text)
async def utag_custom_suffix_input(message: Message, state: FSMContext):
    suffix = aiogram_to_premium_encoded(message).strip()
    if len(suffix) > 400:
        await message.answer("⚠️ Matn juda uzun. 400 belgidan oshmasin.")
        return
    await state.update_data(suffix=suffix)
    await run_utag_process(CallbackQuery(id="0", from_user=message.from_user, chat_instance="0", message=message, data="run_utag"), state)

async def add_to_utag_history(user_id: int, target_chat: Any, chat_title: str):
    u = await ensure_user(user_id)
    raw_hist = u.get("utag_history", "[]")
    try:
        history = json.loads(raw_hist) if isinstance(raw_hist, str) else (raw_hist or [])
    except Exception:
        history = []

    history = [h for h in history if h.get("target") != target_chat]
    history.insert(0, {"target": target_chat, "title": chat_title})
    history = history[:5]
    await update_user_db(user_id, {"utag_history": history})

async def run_utag_process(callback: CallbackQuery, state: FSMContext):
    data = await state.get_data()
    target_chat_id = data.get("target_chat_id")
    regime = data.get("regime", "self")
    suffix = data.get("suffix", "")

    user_id = callback.from_user.id
    str_uid = str(user_id)

    try:
        chat = await bot.get_chat(target_chat_id)
        chat_title = chat.title
    except Exception:
        chat_title = str(target_chat_id)

    await add_to_utag_history(user_id, target_chat_id, chat_title)

    async with get_db_client() as db:
        cursor = await db.execute("SELECT account_id FROM user_sessions WHERE user_id = ? AND status = 'active'", (str_uid,))
        work_rows = cursor.rows

    work_clients = []
    for aid_row in work_rows:
        aid = aid_row[0]
        if aid in userbot_clients:
            work_clients.append((aid, userbot_clients[aid]))

    if not work_clients:
        await callback.message.answer("⚠️ Hech qanday faol ulanish topilmadi!")
        return

    active_utag_sessions[user_id] = {
        "status": "running",
        "chat_id": target_chat_id,
        "chat_title": chat_title,
        "regime": regime,
        "suffix": suffix,
        "workers": []
    }

    await callback.message.answer(
        f_t("🚀 <b>uTag jarayoni boshlandi!</b>"),
        reply_markup=InlineKeyboardMarkup(inline_keyboard=[
            [colored_btn("⏸ Pauza / davom etish", "utag_pause_toggle", "success"),
             colored_btn("To'xtatish", "utag_stop_process", "danger")]
        ])
    )

    for aid, client in work_clients:
        target_fn = rutag_loop_telethon if regime == "random" else utag_loop_telethon
        task = asyncio.create_task(target_fn(target_chat_id, aid, suffix, client))
        _running_tasks.add(task)
        task.add_done_callback(_running_tasks.discard)
        active_utag_sessions[user_id]["workers"].append(task)

@dp.callback_query(F.data == "utag_pause_toggle")
async def cb_utag_pause_toggle(callback: CallbackQuery):
    user_id = callback.from_user.id
    session = active_utag_sessions.get(user_id)
    if session:
        if session["status"] == "running":
            session["status"] = "paused"
            await callback.answer("⏸ Jarayon vaqtincha to'xtatildi.")
        else:
            session["status"] = "running"
            await callback.answer("▶️ Davom ettirilmoqda...")
    else:
        await callback.answer("⚠️ Faol jarayon topilmadi.")

@dp.callback_query(F.data == "utag_stop_process")
async def cb_utag_stop_process(callback: CallbackQuery):
    user_id = callback.from_user.id
    session = active_utag_sessions.get(user_id)
    if session:
        session["status"] = "stopped"
        for task in session["workers"]:
            if not task.done(): task.cancel()
        active_utag_sessions.pop(user_id, None)
        await callback.message.edit_text(f_t("🛑 <b>uTag jarayoni butunlay to'xtatildi!</b>"))
    await callback.answer()

# ---------------------------------------------------------------
# AVTO USER (SCRAPER) SYSTEM
# ---------------------------------------------------------------
@dp.message(F.chat_shared)
async def handle_chat_shared(message: Message, state: FSMContext):
    chat_id = message.chat_shared.chat_id
    current_state = await state.get_state()

    if current_state == UserStatesGroup.utag_group_link:
        await state.update_data(target_chat_id=chat_id)
        try:
            chat_obj = await bot.get_chat(chat_id)
            members_count = await bot.get_chat_member_count(chat_id)
            title = chat_obj.title
        except Exception:
            title = "Nomsiz Guruh"
            members_count = "Noma'lum"

        try:
            rem_msg = await message.answer("...", reply_markup=ReplyKeyboardRemove())
            await rem_msg.delete()
        except Exception:
            pass
        await message.answer(
            f_t(f"ℹ️ <b>Guruh Tanlandi:</b>\n📛 Nomi: {title}\n👥 A'zolar soni: {members_count}\n\n"
                f"Boshlashni istaysizmi?"),
            reply_markup=InlineKeyboardMarkup(inline_keyboard=[
                [colored_btn("Boshlash", "start_utag_now", "success"), colored_btn("Bekor qilish", "menu_avto_utag", "danger")]
            ])
        )
    elif current_state == UserStatesGroup.scrapper_group_link:
        await state.update_data(target_chat_id=chat_id)
        await message.answer(
            f_t(f"✅ Guruh tanlandi. ID: `{chat_id}`\n\n🔢 <b>Nechta User yig'moqchisiz?</b> (Maksimal 2000):"),
            reply_markup=ReplyKeyboardRemove()
        )
        await state.set_state(UserStatesGroup.scrapper_limit)

@dp.callback_query(F.data == "menu_avto_user")
async def cb_menu_avto_user(callback: CallbackQuery, state: FSMContext):
    user_id = callback.from_user.id
    if not await has_active_account(user_id):
        await callback.answer("Akkaunt ulanmagan", show_alert=True)
        await show_add_account_screen(callback, state)
        return
    await state.clear()

    await callback.message.delete()
    await bot.send_message(
        callback.from_user.id,
        f_t("🔗 Guruh linkini yuboring yoki tanlang:"),
        reply_markup=get_group_selection_keyboard()
    )
    await state.set_state(UserStatesGroup.scrapper_group_link)

@dp.message(StateFilter(UserStatesGroup.scrapper_group_link), F.text)
async def scrapper_group_link_input(message: Message, state: FSMContext):
    link_input = message.text.strip()
    target_chat = parse_telegram_link(link_input)
    await state.update_data(target_chat_id=target_chat)
    try:
        rem_msg = await message.answer("...", reply_markup=ReplyKeyboardRemove())
        await rem_msg.delete()
    except Exception:
        pass
    await message.answer(
        f_t(f"✅ Guruh qabul qilindi: `{target_chat}`\n\n🔢 <b>Nechta User yig'moqchisiz?</b> (Maksimal 2000):")
    )
    await state.set_state(UserStatesGroup.scrapper_limit)

@dp.message(StateFilter(UserStatesGroup.scrapper_limit), F.text)
async def scrapper_limit_input(message: Message, state: FSMContext):
    try:
        limit = int(message.text.strip())
        if limit <= 0 or limit > 2000: raise ValueError
    except ValueError:
        await message.answer("❌ Miqdor noto'g'ri. 1 dan 2000 gacha raqam kiriting:")
        return

    data = await state.get_data()
    target_chat_id = data.get("target_chat_id")
    user_id = message.from_user.id
    str_uid = str(user_id)

    async with get_db_client() as db:
        cursor = await db.execute("SELECT account_id FROM user_sessions WHERE user_id = ? AND status = 'active'", (str_uid,))
        row = (cursor.rows[0] if cursor.rows else None)

    if not row:
        await message.answer("⚠️ Akkaunt ulanmagan.")
        return
    aid = row[0]
    client = userbot_clients.get(aid)

    if not client:
        await message.answer("⚠️ Foydalanuvchi akkaunti faol emas.")
        return

    monitor = await message.answer(
        f_t(f"⚡️ <b>Skanerlash Ishga Tushdi</b>\n\n"
            f"📍 Guruh: Skanerlanmoqda...\n"
            f"🎯 Status: faol\n\n"
            f"👥 Topildi: 0 / {limit} ta\n"
            f"📜 Skan qilindi: 0 ta xabar"),
        reply_markup=InlineKeyboardMarkup(inline_keyboard=[
            [colored_btn("⏸ Pauza / Davom etish", "scrapper_pause", "success"),
             colored_btn("🛑 To'xtatish va yuborish", "scrapper_stop", "danger")]
        ])
    )

    active_user_scrappers[user_id] = {
        "status": "running",
        "chat_id": target_chat_id,
        "limit": limit,
        "found": [],
        "scanned_msgs": 0
    }

    task = asyncio.create_task(run_lightning_scraper(user_id, aid, client, target_chat_id, limit, monitor))
    _running_tasks.add(task)
    task.add_done_callback(_running_tasks.discard)

async def run_lightning_scraper(user_id, aid, client, chat_id, limit, monitor_msg: Message):
    session = active_user_scrappers.get(user_id)
    if not session: return

    try:
        admins_user_list = []
        admins_id_list = []
        admins_set = set()

        entity = await safe_get_entity(client, chat_id)
        chat_title = getattr(entity, 'title', 'Guruh')
        target_peer = entity.id if hasattr(entity, 'id') else parse_telegram_link(chat_id)

        async for member in client.iter_participants(target_peer, filter=ChannelParticipantsAdmins):
            if member.bot: continue
            is_owner = getattr(member, 'participant', None) and hasattr(member.participant, "admin_rights") and not member.participant.admin_rights
            crown = "👑 " if is_owner else ""

            admins_id_list.append(f"{crown}`{member.id}`")
            if member.username:
                u = f"@{member.username}".lower()
                admins_set.add(u)
                admins_user_list.append(f"{crown}{u}")

        if admins_id_list:
            id_parts = [admins_id_list[i:i + 30] for i in range(0, len(admins_id_list), 30)]
            for idx, part in enumerate(id_parts, 1):
                await bot.send_message(user_id, f_t(f"🛡 <b>Adminlar ID (Qism {idx}/{len(id_parts)}):\n</b>" + "\n".join(part)))
                await asyncio.sleep(0.2)

        if admins_user_list:
            user_parts = [admins_user_list[i:i + 30] for i in range(0, len(admins_user_list), 30)]
            for idx, part in enumerate(user_parts, 1):
                await bot.send_message(user_id, f_t(f"🔗 <b>Adminlar Username (Qism {idx}/{len(user_parts)}):\n</b>" + "\n".join(part)))
                await asyncio.sleep(0.2)

        max_id = 0
        async for m in client.iter_messages(target_peer, limit=1):
            max_id = m.id
        if max_id == 0: max_id = 1000000

        thread_count = 10
        segment_size = max_id // thread_count if max_id > thread_count else 100

        found_users = session["found"]
        seen_users = set()
        total_messages = 0
        sem = asyncio.Semaphore(8)
        sender_cache = {}
        sent_chunks_count = 0

        async def fetch_lightning_segment(start_id, end_id):
            nonlocal total_messages, sent_chunks_count
            offset_id = start_id
            async with sem:
                while session["status"] in ["running", "paused"] and offset_id > end_id and len(found_users) < limit:
                    if session["status"] == "paused":
                        await asyncio.sleep(0.5)
                        continue
                    try:
                        chunk = []
                        async for msg in client.iter_messages(target_peer, limit=200, offset_id=offset_id):
                            chunk.append(msg)

                        if not chunk: break

                        for msg in chunk:
                            if session["status"] == "stopped" or len(found_users) >= limit:
                                break

                            total_messages += 1
                            offset_id = msg.id

                            if msg.sender_id and not getattr(msg, 'out', False):
                                s_id = msg.sender_id
                                if s_id in sender_cache:
                                    sender = sender_cache[s_id]
                                else:
                                    try:
                                        sender = await client.get_entity(s_id)
                                        sender_cache[s_id] = sender
                                    except Exception:
                                        sender = None

                                if sender and not getattr(sender, 'bot', False) and not getattr(sender, 'megagroup', False) and not getattr(sender, 'channel', False):
                                    if sender.username:
                                        uname = f"@{sender.username}".lower()
                                        if uname not in admins_set and uname not in seen_users:
                                            seen_users.add(uname)
                                            found_users.append(uname)

                            current_full_chunks = len(found_users) // 100
                            if current_full_chunks > sent_chunks_count:
                                start_idx = sent_chunks_count * 100
                                end_idx = current_full_chunks * 100
                                batch = found_users[start_idx:end_idx]
                                sent_chunks_count = current_full_chunks
                                if batch:
                                    await bot.send_message(user_id, f_t(f"A’zolar ({len(batch)}/{sent_chunks_count}-qism)\n\n" + "\n".join(batch)))
                                    await asyncio.sleep(0.4)

                    except FloodWaitError as fwe:
                        await asyncio.sleep(fwe.seconds + 1)
                    except Exception:
                        break

        tasks = []
        for i in range(thread_count):
            s_bound = max_id - (i * segment_size)
            e_bound = max_id - ((i + 1) * segment_size) if i != thread_count - 1 else 0
            tasks.append(fetch_lightning_segment(s_bound, e_bound))

        async def ui_loop():
            while session["status"] in ["running", "paused"] and len(found_users) < limit:
                await asyncio.sleep(2.0)
                try:
                    await monitor_msg.edit_text(
                        f_t(f"⚡️ <b>Turbo Skanerlash</b>\n\n"
                            f"📍 Guruh: <b>{chat_title}</b>\n"
                            f"🎯 Status: {'faol 🟢' if session['status']=='running' else 'pauza ⏸'}\n\n"
                            f"👥 Topildi: {len(found_users)} / {limit} ta\n"
                            f"📜 Skan qilindi: {total_messages} ta xabar"),
                        reply_markup=InlineKeyboardMarkup(inline_keyboard=[
                            [colored_btn("⏸ Pauza / Davom etish", "scrapper_pause", "success"),
                             colored_btn("🛑 To'xtatish va yuborish", "scrapper_stop", "danger")]
                        ])
                    )
                except Exception: pass

        ui_task = asyncio.create_task(ui_loop())
        _running_tasks.add(ui_task)
        ui_task.add_done_callback(_running_tasks.discard)

        await asyncio.gather(*tasks, return_exceptions=True)
        ui_task.cancel()

        try:
            await monitor_msg.delete()
        except Exception:
            pass

        sent_total_from_chunks = sent_chunks_count * 100
        remainder_users = found_users[sent_total_from_chunks:]

        await bot.send_message(
            user_id,
            f_t(f"🏁 <b>SKANERLASH YAKUNLANDI!</b>\n\n"
                f"📍 Guruh: <b>{chat_title}</b>\n"
                f"👑 Adminlar: {len(admins_id_list)} ta\n"
                f"👥 Topilgan userlar: {len(found_users)} ta\n"
                f"📜 Tahlil qilindi: {total_messages} ta xabar")
        )

        if remainder_users:
            await asyncio.sleep(0.3)
            sent_chunks_count += 1
            await bot.send_message(user_id, f_t(f"A’zolar ({len(remainder_users)}/{sent_chunks_count}-qism)\n\n" + "\n".join(remainder_users)))

        if found_users:
            await inc_usage_stat("scraper", len(found_users))

    except Exception as e:
        await bot.send_message(user_id, f"❌ Skanerlashda xatolik yuz berdi: {e}")
    finally:
        active_user_scrappers.pop(user_id, None)

@dp.callback_query(F.data == "scrapper_pause")
async def cb_scrapper_pause(callback: CallbackQuery):
    user_id = callback.from_user.id
    session = active_user_scrappers.get(user_id)
    if session:
        if session["status"] == "running":
            session["status"] = "paused"
            await callback.answer("⏸ Skanerlash vaqtincha to'xtatildi.")
        else:
            session["status"] = "running"
            await callback.answer("▶️ Skanerlash davom etmoqda.")

@dp.callback_query(F.data == "scrapper_stop")
async def cb_scrapper_stop(callback: CallbackQuery):
    user_id = callback.from_user.id
    session = active_user_scrappers.get(user_id)
    if session:
        session["status"] = "stopped"
        await callback.answer("🛑 To'xtatildi, qoldiqlar tashlab berilmoqda...")

# ===============================================================
# 12. HEALTH CHECK WEB SERVER & MAIN ENTRYPOINT
# ===============================================================
async def handle_health_check(request: web.Request) -> web.Response:
    return web.Response(text="200 OK", status=200)

async def start_health_server(port: int) -> web.AppRunner:
    app = web.Application()
    app.router.add_get("/", handle_health_check)
    app.router.add_get("/health", handle_health_check)
    runner = web.AppRunner(app)
    await runner.setup()
    site = web.TCPSite(runner, "0.0.0.0", port)
    await site.start()
    log.info(f"🌐 Health check web server running on port {port}")
    return runner

async def main():
    global bot_username, MAINTENANCE_MODE

    port = int(os.getenv("PORT", 8080))
    runner = await start_health_server(port)

    if not check_db_health():
        MAINTENANCE_MODE = True
        log.error("⚠️ Database missing or quick_check failed! Entering MAINTENANCE_MODE.")
    else:
        MAINTENANCE_MODE = False
        await init_sqlite_db()
        await init_configs_if_none()

    await bot.delete_webhook(drop_pending_updates=True)
    me = await bot.get_me()
    bot_username = me.username
    print(f"🤖 Bot muvaffaqiyatli ishga tushdi: @{bot_username}")

    if not MAINTENANCE_MODE:
        print("📂 SQLite Ma'lumotlar bazasi faollashtirilmoqda. Userbotlar tarmoqqa ulanmoqda...")
        await auto_activate_all_userbots()

        task_save = asyncio.create_task(auto_save_task())
        task_watchdog = asyncio.create_task(userbot_watchdog_loop())

        _running_tasks.update({task_save, task_watchdog})

    try:
        await dp.start_polling(bot, allowed_updates=dp.resolve_used_update_types())
    finally:
        log.info("🛑 Bot to'xtatilmoqda. Resurslar tozalanmoqda...")
        try:
            await runner.cleanup()
        except Exception:
            pass

        for task in list(_running_tasks):
            if not task.done():
                task.cancel()
        if _running_tasks:
            await asyncio.gather(*_running_tasks, return_exceptions=True)
        _running_tasks.clear()

        for acc_id, cl in list(userbot_clients.items()):
            try:
                await cl.disconnect()
            except Exception:
                pass
        userbot_clients.clear()

        await close_global_db_conn()

        try:
            await bot.session.close()
        except Exception:
            pass

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except (KeyboardInterrupt, SystemExit):
        print("Dastur to'xtatildi.")
