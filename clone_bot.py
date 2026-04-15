"""
clone_bot.py — клон основного бота.
Запускается основным ботом как subprocess:
    python3 clone_bot.py <API_TOKEN> <OWNER_ID> <MAIN_ADMIN_ID> <CRYPTO_PAY_TOKEN> <API_ID> <API_HASH>

Возможности клон-админа:
  - /addacc, /delacc — управление аккаунтами
  - /all, /pm — рассылка пользователям
  - Админ панель (кнопка, только для владельца) — все функции + баланс/вывод
  - управление рассылкой через кнопки
  - Автовозврат средств при заморозке/бане аккаунта во время рассылки

Ограничения:
  - нет бана/разбана пользователей
  - нет givebal/delbal
  - нет Telegram Stars (только CryptoPay)
  - прибыль 70%, 30% уходит гл.администратору
"""

import asyncio
import sqlite3
import os
import sys
import time
import logging
import io
import subprocess as _subprocess

from aiogram import Bot, Dispatcher, types, F
from aiogram.types import Message, InputMediaPhoto, InlineKeyboardButton
from aiogram.filters import Command, CommandObject
from aiogram.fsm.context import FSMContext
from aiogram.fsm.state import State, StatesGroup
from aiogram.utils.keyboard import InlineKeyboardBuilder, ReplyKeyboardBuilder

from telethon import TelegramClient
from telethon.errors import (SessionPasswordNeededError, UserDeactivatedBanError,
                              UserDeactivatedError, AuthKeyUnregisteredError, FloodWaitError,
                              PhoneCodeExpiredError, PhoneCodeInvalidError)

try:
    from tdata_export import export_tdata as _export_tdata
except ImportError:
    _export_tdata = None

try:
    from aiocryptopay import AioCryptoPay, Networks
except ImportError:
    AioCryptoPay = None

# ─── ПАРАМЕТРЫ ИЗ АРГУМЕНТОВ ────────────────────────────────────────────────
if len(sys.argv) < 7:
    print("Usage: clone_bot.py TOKEN OWNER_ID MAIN_ADMIN_ID CRYPTO_TOKEN API_ID API_HASH")
    sys.exit(1)

API_TOKEN       = sys.argv[1]
OWNER_ID        = int(sys.argv[2])      # клон-админ
MAIN_ADMIN_ID   = int(sys.argv[3])      # гл.админ основного бота
CRYPTO_PAY_TOKEN = sys.argv[4]
API_ID          = int(sys.argv[5])
API_HASH        = sys.argv[6]
MAIN_DB_PATH    = sys.argv[7] if len(sys.argv) > 7 else "bot_data.db"

# Имя БД берём из токена (уникальное)
BOT_SHORT_ID = API_TOKEN.split(":")[0]
DB_PATH      = f"clone_{BOT_SHORT_ID}.db"
SESSION_DIR  = f"sessions_clone_{BOT_SHORT_ID}"
TDATA_DIR    = f"tdata_clone_{BOT_SHORT_ID}"

WELCOME_BONUS      = 0.1
MIN_RENT_TIME      = 10
MIN_INTERVAL       = 30
OWNER_SHARE        = 0.70   # 70% владельцу клона
MIN_WITHDRAW       = 1.0    # минимальная сумма вывода
PRICE_NORMAL       = 0.02   # цена обычного аккаунта за минуту по умолчанию
PRICE_PREMIUM      = 0.05   # цена премиум-аккаунта за минуту по умолчанию
MIN_PRICE          = 0.001  # минимально допустимая цена за минуту

# Словарь активных TelegramClient-ов: {user_id: client}
active_clients: dict = {}

DEFAULT_IMG_MAIN    = "https://ibb.co/d4zm29x6"
DEFAULT_IMG_CATALOG = "https://ibb.co/HTm1Cv56"
DEFAULT_IMG_BALANCE = "https://ibb.co/WNy38dr2"
DEFAULT_IMG_MY_RENT = "https://ibb.co/tTSMycBT"

def get_main_accounts():
    """Получает список аккаунтов из основной БД (если файл существует)."""
    if not os.path.exists(MAIN_DB_PATH):
        return []
    try:
        main_db = sqlite3.connect(MAIN_DB_PATH, check_same_thread=False)
        main_db.execute('PRAGMA busy_timeout=2000')
        main_cur = main_db.cursor()
        main_cur.execute(
            'SELECT phone, price_per_min, is_premium, owner_id, expires FROM accounts')
        rows = main_cur.fetchall()
        main_db.close()
        return rows
    except Exception as e:
        logging.error(f"Ошибка чтения основной БД: {e}")
        return []


logging.basicConfig(level=logging.INFO,
                    format=f'%(levelname)s:CLONE-{BOT_SHORT_ID}:%(message)s')

bot = Bot(token=API_TOKEN)
dp  = Dispatcher()

crypto = None
if AioCryptoPay:
    crypto = AioCryptoPay(token=CRYPTO_PAY_TOKEN, network=Networks.MAIN_NET)

# ─── БАЗА ДАННЫХ ─────────────────────────────────────────────────────────────
db  = sqlite3.connect(DB_PATH, check_same_thread=False)
db.execute('PRAGMA journal_mode=WAL')
db.execute('PRAGMA busy_timeout=5000')
cur = db.cursor()


def init_db():
    cur.execute('''CREATE TABLE IF NOT EXISTS accounts
                   (phone TEXT PRIMARY KEY, owner_id INTEGER, expires INTEGER,
                    text TEXT DEFAULT 'Привет!', photo_id TEXT,
                    interval INTEGER DEFAULT 30, chats TEXT DEFAULT '',
                    is_running INTEGER DEFAULT 0, price_per_min REAL DEFAULT 0.02,
                    catalog_chats TEXT DEFAULT '', is_premium INTEGER DEFAULT 0,
                    notified_10m INTEGER DEFAULT 0)''')
    cur.execute('''CREATE TABLE IF NOT EXISTS users
                   (user_id INTEGER PRIMARY KEY, balance REAL DEFAULT 0.0,
                    banned_until INTEGER DEFAULT 0, ban_reason TEXT DEFAULT "")''')
    cur.execute('''CREATE TABLE IF NOT EXISTS payments
                   (id INTEGER PRIMARY KEY AUTOINCREMENT, user_id INTEGER,
                    amount REAL, method TEXT, date TEXT)''')
    cur.execute('''CREATE TABLE IF NOT EXISTS rent_history
                   (id INTEGER PRIMARY KEY AUTOINCREMENT, user_id INTEGER,
                    phone TEXT, duration INTEGER, cost REAL, date TEXT)''')
    cur.execute('''CREATE TABLE IF NOT EXISTS blacklist (word TEXT PRIMARY KEY)''')
    # Баланс клон-админа (его 70%)
    cur.execute('''CREATE TABLE IF NOT EXISTS clone_balance
                   (id INTEGER PRIMARY KEY CHECK(id=1),
                    earned REAL DEFAULT 0.0,
                    withdrawn REAL DEFAULT 0.0)''')
    cur.execute('INSERT OR IGNORE INTO clone_balance (id, earned, withdrawn) VALUES (1, 0, 0)')

    cur.execute('''CREATE TABLE IF NOT EXISTS bot_settings
                   (key TEXT PRIMARY KEY, value TEXT DEFAULT '')''')
    cur.execute("INSERT OR IGNORE INTO bot_settings (key, value) VALUES ('main_accounts_enabled', '0')")
    cur.execute('''CREATE TABLE IF NOT EXISTS bot_images
                   (key TEXT PRIMARY KEY, file_id TEXT)''')
    for key in ('main', 'catalog', 'balance', 'my_rent'):
        cur.execute('INSERT OR IGNORE INTO bot_images (key, file_id) VALUES (?,?)', (key, ''))

    cur.execute('''CREATE TABLE IF NOT EXISTS notify_bots
                   (slot INTEGER PRIMARY KEY, token TEXT DEFAULT '', label TEXT DEFAULT '')''')
    for slot in (1, 2, 3):
        cur.execute('INSERT OR IGNORE INTO notify_bots (slot, token, label) VALUES (?,?,?)',
                    (slot, '', f'Бот {slot}'))

    cur.execute('''CREATE TABLE IF NOT EXISTS subclone_processes
                   (id INTEGER PRIMARY KEY AUTOINCREMENT,
                    token TEXT, owner_id INTEGER, crypto_token TEXT,
                    pid INTEGER DEFAULT 0, started_at TEXT,
                    bot_username TEXT DEFAULT '',
                    is_running INTEGER DEFAULT 1,
                    earned REAL DEFAULT 0.0,
                    withdrawn REAL DEFAULT 0.0)''')
    # Реф-ссылки: владелец суб-клона получает бонус за приведённых пользователей
    try:
        cur.execute('ALTER TABLE users ADD COLUMN referred_by TEXT DEFAULT ""')
    except Exception:
        pass
    # Безопасные миграции на случай старой БД
    for col_sql in [
        'ALTER TABLE accounts ADD COLUMN is_running INTEGER DEFAULT 0',
        'ALTER TABLE accounts ADD COLUMN notified_10m INTEGER DEFAULT 0',
        'ALTER TABLE accounts ADD COLUMN is_premium INTEGER DEFAULT 0',
        'ALTER TABLE accounts ADD COLUMN catalog_chats TEXT DEFAULT ""',
        'ALTER TABLE accounts ADD COLUMN price_per_min REAL DEFAULT 0.02',
    ]:
        try:
            cur.execute(col_sql)
        except Exception:
            pass
    db.commit()

    default_words = ['темка', 'чернуха', 'скам', '$кам']
    for w in default_words:
        cur.execute('INSERT OR IGNORE INTO blacklist (word) VALUES (?)', (w,))
    db.commit()


init_db()

os.makedirs(SESSION_DIR, exist_ok=True)


def get_img(key: str) -> str:
    """Возвращает file_id кастомной картинки или дефолтный URL."""
    defaults = {
        'main':     DEFAULT_IMG_MAIN,
        'catalog':  DEFAULT_IMG_CATALOG,
        'balance':  DEFAULT_IMG_BALANCE,
        'my_rent':  DEFAULT_IMG_MY_RENT,
    }
    res = db_fetchone('SELECT file_id FROM bot_images WHERE key=?', (key,))
    if res and res[0]:
        return res[0]
    return defaults.get(key, '')


def get_setting(key: str) -> str:
    res = db_fetchone('SELECT value FROM bot_settings WHERE key=?', (key,))
    return res[0] if res else '0'

def set_setting(key: str, value: str):
    cur.execute('INSERT OR REPLACE INTO bot_settings (key, value) VALUES (?,?)', (key, value))
    db.commit()

def is_any_admin(user_id: int) -> bool:
    """Проверяет, является ли пользователь владельцем клона или главным администратором."""
    return user_id == OWNER_ID or user_id == MAIN_ADMIN_ID


def db_fetchone(q, p=()):
    c = db.cursor(); c.execute(q, p); return c.fetchone()

def db_fetchall(q, p=()):
    c = db.cursor(); c.execute(q, p); return c.fetchall()

def get_balance(user_id):
    res = db_fetchone('SELECT balance FROM users WHERE user_id=?', (user_id,))
    return round(res[0], 2) if res else None

def get_clone_balance():
    res = db_fetchone('SELECT earned, withdrawn FROM clone_balance WHERE id=1')
    if res:
        return round(res[0], 2), round(res[1], 2)
    return 0.0, 0.0

async def notify_clone_admins(text: str, photo_id: str = None, also_notify_main: bool = False):
    """
    Уведомляет только через ботов-наблюдателей клона.
    Если ботов нет — уведомления не отправляются.
    also_notify_main=True — дополнительно шлёт гл.админу через MAIN_BOT_TOKEN (если задан).
    """
    rows = db_fetchall('SELECT token FROM notify_bots WHERE token != ""')
    active_tokens = [r[0] for r in rows if r[0].strip()]

    async def _send(b: Bot, chat_id: int):
        try:
            if photo_id:
                await b.send_photo(chat_id, photo=photo_id, caption=text, parse_mode="Markdown")
            else:
                await b.send_message(chat_id, text, parse_mode="Markdown")
        except Exception:
            pass

    # Боты-наблюдатели клона -> владельцу клона
    for tok in active_tokens:
        try:
            nb = Bot(token=tok)
            await _send(nb, OWNER_ID)
            await nb.session.close()
        except Exception:
            pass

    # Уведомить главного админа (только если явно запрошено, например для логов клонирования)
    if also_notify_main:
        await notify_main_admin(text, photo_id)


async def notify_main_admin(text: str, photo_id: str = None):
    """Уведомить только главного администратора через MAIN_BOT_TOKEN (если задан)."""
    main_bot_token = os.environ.get("MAIN_BOT_TOKEN", "")
    if not main_bot_token:
        return
    try:
        mb = Bot(token=main_bot_token)
        if photo_id:
            await mb.send_photo(MAIN_ADMIN_ID, photo=photo_id, caption=text, parse_mode="Markdown")
        else:
            await mb.send_message(MAIN_ADMIN_ID, text, parse_mode="Markdown")
        await mb.session.close()
    except Exception:
        pass

def add_payment(user_id, amount, method):
    """Зачисляет платёж: 70% в баланс клон-админа, записывает историю."""
    date = time.strftime('%Y-%m-%d %H:%M:%S')
    cur.execute('INSERT INTO payments (user_id, amount, method, date) VALUES (?,?,?,?)',
                (user_id, amount, method, date))
    cur.execute('UPDATE users SET balance = balance + ? WHERE user_id=?', (amount, user_id))
    owner_cut = round(amount * OWNER_SHARE, 4)
    cur.execute('UPDATE clone_balance SET earned = earned + ? WHERE id=1', (owner_cut,))
    db.commit()

def contains_bad_words(text):
    words = [r[0] for r in db_fetchall('SELECT word FROM blacklist')]
    tl = text.lower()
    for w in words:
        if w in tl: return w
    return None

def format_time_left(expires):
    left = expires - int(time.time())
    if left <= 0: return "истекло"
    h, m = left // 3600, (left % 3600) // 60
    return f"{h}ч {m}м" if h > 0 else f"{m}м"

def extract_chat_and_topic(chat_str):
    chat_str = chat_str.strip()
    if "t.me/" in chat_str:
        chat_str = chat_str.split("t.me/")[1]
    if "/" in chat_str:
        parts = chat_str.split("/")
        if len(parts) == 2 and parts[1].isdigit():
            return parts[0], int(parts[1])
        elif len(parts) == 3 and parts[0] == "c" and parts[2].isdigit():
            return int("-100" + parts[1]), int(parts[2])
    try:
        return int(chat_str), None
    except ValueError:
        return chat_str, None

def is_owner(m): return m.from_user.id == OWNER_ID

def main_menu(user_id=None):
    kb = ReplyKeyboardBuilder()
    kb.button(text="📂 Каталог аккаунтов")
    kb.button(text="🔑 Моя аренда")
    kb.button(text="💰 Баланс")
    kb.button(text="🤝 Реф.система")
    kb.button(text="❓ Помощь")
    kb.button(text="👨‍💻 Support")
    if user_id and is_any_admin(user_id):
        kb.button(text="🔧 Админ панель")
        kb.button(text="⚙️ Настройка Бота")
        kb.adjust(2, 2, 2, 2)
    else:
        kb.adjust(2, 2, 2)
    return kb.as_markup(resize_keyboard=True)

def back_kb(to="to_main"):
    kb = InlineKeyboardBuilder()
    kb.button(text="⬅️ Назад", callback_data=to)
    return kb


# ─── СОСТОЯНИЯ ───────────────────────────────────────────────────────────────
class States(StatesGroup):
    waiting_for_phone    = State()
    waiting_for_code     = State()
    waiting_for_password = State()
    waiting_for_tgp      = State()
    waiting_for_rent_time = State()
    edit_text     = State()
    edit_chats    = State()
    edit_photo    = State()
    edit_interval = State()
    top_up_amount = State()
    withdraw_wallet = State()
    withdraw_amount = State()
    # Админ-панель FSM
    adm_broadcast_text = State()
    adm_pm_input       = State()
    adm_blacklist_word = State()
    adm_redak_input    = State()
    adm_delacc_input   = State()
    adm_unnomber_input = State()
    adm_stats_input    = State()
    adm_withdraw_wallet = State()
    adm_withdraw_amount = State()
    # Редактирование визуала
    edit_visual_main    = State()
    edit_visual_catalog = State()
    edit_visual_balance = State()
    edit_visual_my_rent = State()
    # Создание суб-клона
    subclone_token      = State()
    subclone_owner_id   = State()
    subclone_crypto_tok = State()


# ─── ФОНОВАЯ ЗАДАЧА: истечение аренды ────────────────────────────────────────
async def check_expirations():
    while True:
        now = int(time.time())
        rows = db_fetchall(
            'SELECT phone, owner_id FROM accounts WHERE owner_id IS NOT NULL AND expires > 0 AND expires - ? <= 600 AND notified_10m = 0',
            (now,))
        for phone, owner_id in rows:
            await notify_clone_admins(f"⚠️ До конца аренды `{phone}` менее 10 минут.")
            cur.execute('UPDATE accounts SET notified_10m=1 WHERE phone=?', (phone,))

        expired = db_fetchall(
            'SELECT phone, owner_id FROM accounts WHERE owner_id IS NOT NULL AND expires > 0 AND expires <= ?',
            (now,))
        for phone, owner_id in expired:
            await notify_clone_admins(f"🛑 Аренда `{phone}` завершена.")
            cur.execute('UPDATE accounts SET owner_id=NULL, expires=0, is_running=0, notified_10m=0 WHERE phone=?', (phone,))
        db.commit()
        await asyncio.sleep(60)


# ─── ВОЗВРАТ СРЕДСТВ ПРИ БЛОКИРОВКЕ АККАУНТА ─────────────────────────────────
async def refund_remaining_rent(phone: str, reason: str = "заморожен/заблокирован"):
    """Возвращает деньги за оставшееся время аренды пользователю."""
    res = db_fetchone(
        'SELECT owner_id, expires, price_per_min FROM accounts WHERE phone=? AND owner_id IS NOT NULL AND expires > ?',
        (phone, int(time.time())))
    if not res:
        return
    owner_id, expires, price_per_min = res
    now = int(time.time())
    remaining_seconds = max(0, expires - now)
    if remaining_seconds <= 0:
        return
    remaining_minutes = remaining_seconds / 60
    refund_amount = round(remaining_minutes * price_per_min, 2)
    if refund_amount <= 0:
        return

    # Возвращаем деньги пользователю
    cur.execute('UPDATE users SET balance = balance + ? WHERE user_id=?', (refund_amount, owner_id))
    # Освобождаем номер
    cur.execute('UPDATE accounts SET owner_id=NULL, expires=0, is_running=0, notified_10m=0 WHERE phone=?', (phone,))
    db.commit()

    # Уведомляем через ботов-наблюдателей
    await notify_clone_admins(
        f"⚠️ **Аккаунт `{phone}` был {reason}!**\n\n"
        f"Рассылка остановлена. Оставшееся время аренды пересчитано.\n"
        f"💰 Возврат на баланс: **${refund_amount}**\n\n"
        f"Номер возвращён в каталог.")


# ─── ЦИКЛ РАССЫЛКИ ───────────────────────────────────────────────────────────
async def broadcast_loop(phone):
    client = TelegramClient(f"{SESSION_DIR}/{phone}", API_ID, API_HASH)
    try:
        await client.connect()
        while True:
            res = db_fetchone(
                'SELECT is_running, text, interval, chats, expires, photo_id FROM accounts WHERE phone=?',
                (phone,))
            if not res or not res[0] or int(time.time()) > res[4]:
                break
            interval = max(MIN_INTERVAL, res[2])
            chats = [c.strip() for c in res[3].split(',') if c.strip()]
            for chat in chats:
                chk = db_fetchone('SELECT is_running FROM accounts WHERE phone=?', (phone,))
                if not chk or not chk[0]: break
                try:
                    entity, topic_id = extract_chat_and_topic(chat)
                    if res[5]:
                        f = await bot.get_file(res[5])
                        p_io = await bot.download_file(f.file_path)
                        buf = io.BytesIO(p_io.getvalue()); buf.name = "img.jpg"
                        await client.send_file(entity, buf, caption=res[1], reply_to=topic_id)
                    else:
                        await client.send_message(entity, res[1], reply_to=topic_id)
                except (UserDeactivatedBanError, UserDeactivatedError, AuthKeyUnregisteredError) as e:
                    logging.warning(f"Аккаунт {phone} заблокирован/заморожен: {e}")
                    await refund_remaining_rent(phone, "заморожен или заблокирован Telegram")
                    return  # Выходим из цикла рассылки
                except Exception as e:
                    logging.error(f"Broadcast error {chat}: {e}")
                await asyncio.sleep(interval)
            await asyncio.sleep(10)
    finally:
        try:
            await client.disconnect()
        except: pass


# ─── КОМАНДЫ КЛОН-АДМИНА ─────────────────────────────────────────────────────
@dp.message(Command("start"))
@dp.callback_query(F.data == "to_main")
async def start_cmd(event: types.Message | types.CallbackQuery, state: FSMContext):
    await state.clear()
    user_id = event.from_user.id
    bonus_text = ""
    if get_balance(user_id) is None:
        # Проверяем реф-параметр: /start ref_SID
        ref_sid = None
        if isinstance(event, Message) and event.text:
            parts = event.text.strip().split()
            if len(parts) > 1 and parts[1].startswith("ref_"):
                try:
                    ref_sid = int(parts[1][4:])
                except Exception:
                    ref_sid = None

        ref_note = ""
        if ref_sid:
            sc_res = db_fetchone(
                'SELECT owner_id, bot_username FROM subclone_processes WHERE id=? AND is_running=1',
                (ref_sid,))
            if sc_res and sc_res[0] != user_id:
                REF_REWARD = round(WELCOME_BONUS * 0.5, 4)
                cur.execute('UPDATE subclone_processes SET earned = earned + ? WHERE id=?',
                            (REF_REWARD, ref_sid))
                uname_ref = f"@{sc_res[1]}" if sc_res[1] else str(ref_sid)
                ref_note = f"\n🔗 Реф: {uname_ref} (+${REF_REWARD})"

        cur.execute('INSERT OR IGNORE INTO users (user_id, balance, referred_by) VALUES (?,?,?)',
                    (user_id, WELCOME_BONUS, str(ref_sid) if ref_sid else ""))
        db.commit()
        bonus_text = f"\n\n🎁 Бонус: **${WELCOME_BONUS}**"
        try:
            await notify_clone_admins(
                f"🆕 **Новый пользователь в клоне!**\n"
                f"👤 ID: `{user_id}`\n"
                f"📛 Имя: {event.from_user.full_name}\n"
                f"🔗 @{event.from_user.username or '—'}{ref_note}")
        except Exception:
            pass
    caption = f"👋 Главное меню.{bonus_text}"
    if isinstance(event, Message):
        await event.answer_photo(photo=get_img('main'), caption=caption,
                                 reply_markup=main_menu(user_id), parse_mode="Markdown")
    else:
        await event.message.edit_media(
            media=InputMediaPhoto(media=get_img('main'), caption=caption, parse_mode="Markdown"),
            reply_markup=InlineKeyboardBuilder().button(
                text="📂 Каталог", callback_data="catalog_inline").as_markup())


# ─── 🔧 АДМИН ПАНЕЛЬ (только владелец клона) ─────────────────────────────────
@dp.message(F.text == "🔧 Админ панель")
async def admin_panel(m: Message, state: FSMContext):
    if not is_any_admin(m.from_user.id): return
    await state.clear()
    kb = InlineKeyboardBuilder()
    kb.button(text="➕ Добавить аккаунт",    callback_data="adm_addacc")
    kb.button(text="🗑 Удалить аккаунт",     callback_data="adm_delacc")
    kb.button(text="⛔ Снять аренду",        callback_data="adm_unnomber")
    kb.button(text="🚫 Стоп-слово",          callback_data="adm_blacklist")
    kb.button(text="📋 Редакт. чаты",        callback_data="adm_redak")
    kb.button(text="📊 Стат. пользователя",  callback_data="adm_stats")
    kb.button(text="📢 Рассылка всем",       callback_data="adm_broadcast")
    kb.button(text="📩 Написать польз.",     callback_data="adm_pm")
    kb.button(text="🤖 Создать суб-клон",    callback_data="adm_create_subclone")
    kb.adjust(2)
    await m.answer("🔧 **Админ панель**\n\nВыберите действие:", reply_markup=kb.as_markup(), parse_mode="Markdown")


# ─── ДОБАВИТЬ АККАУНТ ────────────────────────────────────────────────────────
@dp.callback_query(F.data == "adm_addacc")
async def adm_addacc_cb(call: types.CallbackQuery, state: FSMContext):
    if not is_any_admin(call.from_user.id): return
    await call.message.edit_text("📱 Введите номер телефона:", reply_markup=back_kb("adm_panel").as_markup())
    await state.update_data(from_panel=True)
    await state.set_state(States.waiting_for_phone)

@dp.message(Command("addacc"))
async def add_acc(m: Message, state: FSMContext):
    if not is_owner(m): return
    await m.answer("📱 Введите номер телефона:")
    await state.update_data(from_panel=False)
    await state.set_state(States.waiting_for_phone)

async def _disconnect_client(uid: int):
    """Безопасно отключает и удаляет клиента из active_clients."""
    entry = active_clients.pop(uid, None)
    if entry:
        c = entry["client"] if isinstance(entry, dict) else entry
        try:
            await c.disconnect()
        except Exception:
            pass


def _make_hint_and_kb(code_type_name: str, is_resend: bool = False):
    """Возвращает (текст подсказки, InlineKeyboardBuilder) по типу кода."""
    prefix = "📲 *Новый код отправлен*" if is_resend else "📲 *Код отправлен*"
    kb = InlineKeyboardBuilder()
    ctn = code_type_name.lower()
    if "app" in ctn:
        hint = (
            f"{prefix} *в Telegram*\n\n"
            "Код придёт как обычное сообщение от **Telegram** в другом клиенте под этим номером.\n\n"
            "📌 *Где искать:*\n"
            "• Откройте Telegram на телефоне — придёт уведомление\n"
            "• Или войдите через веб-версию ниже\n"
            "• Раздел **Избранное (Saved Messages)** — там будет сообщение с кодом"
        )
        kb.button(text="🌐 web.telegram.org (войти и найти код)", url="https://web.telegram.org/k/")
    elif "sms" in ctn:
        p = "Новый код" if is_resend else "Код"
        hint = f"📩 *{p}* отправлен по SMS на этот номер."
    elif "flash" in ctn or "missed" in ctn:
        p = "Новый код" if is_resend else "Код"
        hint = f"📞 *{p}* — последние цифры номера пропущенного звонка."
    elif "call" in ctn:
        p = "Новый код" if is_resend else "Код"
        hint = f"📞 *{p}* будет продиктован в голосовом звонке."
    elif "fragment" in ctn:
        hint = f"🔗 *Код* доступен на fragment.com для этого номера."
        kb.button(text="🔗 fragment.com", url="https://fragment.com/")
    else:
        hint = f"📨 *Код отправлен*. Проверьте Telegram или SMS на этом номере."
    return hint, kb


async def _disconnect_client(uid: int):
    """Безопасно отключает и удаляет клиента из active_clients."""
    entry = active_clients.pop(uid, None)
    if entry:
        c = entry["client"] if isinstance(entry, dict) else entry
        try:
            await c.disconnect()
        except Exception:
            pass


async def _request_code(m: Message, state: FSMContext, phone: str, from_panel: bool):
    uid = m.from_user.id
    await _disconnect_client(uid)
    os.makedirs(SESSION_DIR, exist_ok=True)
    session_path = os.path.join(SESSION_DIR, phone)

    c = TelegramClient(
        session_path, API_ID, API_HASH,
        receive_updates=False,
        device_model="Desktop",
        system_version="Windows 10",
        app_version="4.16.7",
        lang_code="ru",
        system_lang_code="ru-RU",
    )

    try:
        await c.connect()
    except Exception as e:
        await m.answer(f"❌ Не удалось подключиться к Telegram: {e}")
        try: await c.disconnect()
        except: pass
        return

    try:
        if await c.is_user_authorized():
            active_clients[uid] = {"client": c, "hash": None}
            await state.update_data(phone=phone, from_panel=from_panel, code_hash=None)
            await m.answer("✅ Аккаунт уже авторизован в сессии!")
            await ask_premium_status(m, state, phone)
            return

        sent = await c.send_code_request(phone)
        active_clients[uid] = {"client": c, "hash": sent.phone_code_hash}
        await state.update_data(phone=phone, from_panel=from_panel, code_hash=sent.phone_code_hash)

        hint, kb = _make_hint_and_kb(type(sent.type).__name__.lower())
        await m.answer(
            f"{hint}\n\n✏️ Введите код (цифры слитно или через пробел):",
            parse_mode="Markdown",
            reply_markup=kb.as_markup() if kb.buttons else None,
        )
        await state.set_state(States.waiting_for_code)

    except FloodWaitError as e:
        await m.answer(f"⏳ Слишком много попыток. Подождите {e.seconds} сек и попробуйте снова.")
        await _disconnect_client(uid)
        await state.clear()
    except Exception as e:
        logging.error(f"[addacc] Ошибка запроса кода для {phone}: {e}")
        await m.answer(f"❌ Ошибка при запросе кода: {e}")
        await _disconnect_client(uid)
        await state.clear()


# ── Обработчик номера телефона ────────────────────────────────────────────────
@dp.message(States.waiting_for_phone)
async def h_phone(m: Message, state: FSMContext):
    phone = m.text.strip().replace(" ", "").replace("-", "")
    if not phone.startswith("+"):
        phone = "+" + phone
    d = await state.get_data()
    from_panel = d.get("from_panel", False)
    await _request_code(m, state, phone, from_panel)


# ── Обработчик кода ───────────────────────────────────────────────────────────
@dp.message(States.waiting_for_code)
async def h_code(m: Message, state: FSMContext):
    d = await state.get_data()
    uid = m.from_user.id
    # Принимаем код в любом формате: "12345", "1 2 3 4 5", "1-2-3-4-5"
    code = m.text.strip().replace(" ", "").replace("-", "")

    entry = active_clients.get(uid)
    if not entry:
        await m.answer("❌ Сессия прервана. Начните заново — введите номер телефона.")
        await state.set_state(States.waiting_for_phone)
        return

    c = entry["client"] if isinstance(entry, dict) else entry
    code_hash = entry.get("hash") if isinstance(entry, dict) else d.get("code_hash")

    # Восстанавливаем соединение если упало
    if not c.is_connected():
        try:
            await c.connect()
        except Exception as e:
            await m.answer(f"❌ Соединение разорвано: {e}\n\nВведите номер телефона заново.")
            await _disconnect_client(uid)
            await state.set_state(States.waiting_for_phone)
            return

    try:
        await c.sign_in(d['phone'], code, phone_code_hash=code_hash)
        # ✅ Успех — аккаунт без 2FA
        await ask_premium_status(m, state, d['phone'])

    except SessionPasswordNeededError:
        # Код принят, нужен 2FA-пароль.
        # Сохраняем клиент — h_2fa вызовет sign_in(password=...) на том же объекте.
        active_clients[uid] = {"client": c, "hash": code_hash}
        await state.set_state(States.waiting_for_password)
        await m.answer(
            "🔐 *На этом аккаунте включена двухфакторная аутентификация.*\n\n"
            "Введите ваш облачный пароль Telegram\n"
            "_(тот, что установлен в Настройки → Конфиденциальность → Облачный пароль)_:",
            parse_mode="Markdown")
        return  # выходим немедленно, не запрашиваем новый код

    except PhoneCodeInvalidError:
        await m.answer(
            "❌ *Неверный код.*\n\n"
            "Проверьте код и введите снова (без пробелов или через пробел — оба варианта работают):",
            parse_mode="Markdown")
        # Остаёмся в waiting_for_code

    except PhoneCodeExpiredError:
        # Код устарел — но сначала проверяем, не принял ли уже Telegram код.
        # Для аккаунтов с 2FA Telethon иногда выбрасывает PhoneCodeExpiredError
        # вместо SessionPasswordNeededError, если код был принят, но нужен пароль.
        phone = d.get('phone', '')
        try:
            if not c.is_connected():
                await c.connect()
            already_authed = await c.is_user_authorized()
            if already_authed:
                # Код принят, аккаунт ждёт пароль 2FA — не запрашиваем новый код
                active_clients[uid] = {"client": c, "hash": code_hash}
                await state.set_state(States.waiting_for_password)
                await m.answer(
                    "🔐 *На этом аккаунте включена двухфакторная аутентификация.*\n\n"
                    "Введите ваш облачный пароль Telegram\n"
                    "_(тот, что установлен в Настройки → Конфиденциальность → Облачный пароль)_:",
                    parse_mode="Markdown")
                return
            # Код действительно истёк — запрашиваем новый
            sent = await c.send_code_request(phone)
            active_clients[uid] = {"client": c, "hash": sent.phone_code_hash}
            await state.update_data(code_hash=sent.phone_code_hash)
            hint, hint_kb = _make_hint_and_kb(type(sent.type).__name__.lower(), is_resend=True)
            await m.answer(
                f"⚠️ Код истёк — отправлен новый.\n\n{hint}\n\n✏️ Введите новый код:",
                parse_mode="Markdown",
                reply_markup=hint_kb.as_markup() if hint_kb.buttons else None,
            )
            await state.set_state(States.waiting_for_code)
        except SessionPasswordNeededError:
            # send_code_request сам бросил SPNE — аккаунт ждёт пароль
            active_clients[uid] = {"client": c, "hash": code_hash}
            await state.set_state(States.waiting_for_password)
            await m.answer(
                "🔐 *На этом аккаунте включена двухфакторная аутентификация.*\n\n"
                "Введите ваш облачный пароль Telegram\n"
                "_(тот, что установлен в Настройки → Конфиденциальность → Облачный пароль)_:",
                parse_mode="Markdown")
        except FloodWaitError as e:
            await m.answer(f"⏳ Слишком много попыток. Подождите {e.seconds} сек и попробуйте снова.")
            await _disconnect_client(uid)
            await state.clear()
        except Exception as e:
            await m.answer(f"❌ Не удалось отправить новый код: {e}\n\nВведите номер телефона заново.")
            await _disconnect_client(uid)
            await state.set_state(States.waiting_for_phone)

    except FloodWaitError as e:
        await m.answer(f"⏳ Флуд-вейт {e.seconds} сек. Попробуйте снова позже.")
        await _disconnect_client(uid)
        await state.clear()

    except Exception as e:
        logging.error(f"[h_code] uid={uid} phone={d.get('phone')} err={e}")
        await m.answer(f"❌ Ошибка входа ({type(e).__name__}): {e}\n\nВведите номер телефона заново.")
        await _disconnect_client(uid)
        await state.set_state(States.waiting_for_phone)


# ── Обработчик 2FA пароля ─────────────────────────────────────────────────────
@dp.message(States.waiting_for_password)
async def h_2fa(m: Message, state: FSMContext):
    d = await state.get_data()
    uid = m.from_user.id
    password = m.text.strip()

    entry = active_clients.get(uid)
    if not entry:
        await m.answer("❌ Сессия прервана. Введите номер телефона заново.")
        await state.set_state(States.waiting_for_phone)
        return

    c = entry["client"] if isinstance(entry, dict) else entry

    # Восстанавливаем соединение если упало
    if not c.is_connected():
        try:
            await c.connect()
        except Exception as e:
            await m.answer(f"❌ Соединение разорвано: {e}\n\nВведите номер телефона заново.")
            await _disconnect_client(uid)
            await state.set_state(States.waiting_for_phone)
            return

    try:
        await c.sign_in(password=password)
        # ✅ 2FA пройдена успешно
        await ask_premium_status(m, state, d['phone'])

    except Exception as e:
        ename = type(e).__name__
        err_str = str(e).lower()
        # Telethon: PasswordHashInvalidError / RPCError PASSWORD_HASH_INVALID
        is_wrong_password = (
            any(k in err_str for k in ("password", "hash_invalid", "invalid", "wrong", "incorrect", "2fa"))
            or any(k in ename.lower() for k in ("password", "hash"))
        )
        if is_wrong_password:
            await m.answer(
                "❌ *Неверный пароль 2FA.*\n\n"
                "Попробуйте ещё раз.\n"
                "_Если забыли пароль — сбросьте его через Настройки Telegram → Конфиденциальность → Облачный пароль._",
                parse_mode="Markdown")
        else:
            logging.error(f"[h_2fa] uid={uid} err={ename}: {e}")
            await m.answer(
                f"❌ Ошибка при проверке пароля ({ename}).\n\n"
                "Попробуйте ввести пароль ещё раз:")
        # В обоих случаях остаёмся в waiting_for_password
        await state.set_state(States.waiting_for_password)


async def ask_premium_status(m, state, phone):
    await state.update_data(phone=phone)
    kb = InlineKeyboardBuilder()
    kb.button(text="Да ⭐", callback_data="tgp_yes")
    kb.button(text="Нет", callback_data="tgp_no")
    await m.answer("⭐ Аккаунт Premium?", reply_markup=kb.adjust(2).as_markup())
    await state.set_state(States.waiting_for_tgp)

@dp.callback_query(States.waiting_for_tgp, F.data.in_(["tgp_yes", "tgp_no"]))
async def process_tgp(call: types.CallbackQuery, state: FSMContext):
    d = await state.get_data()
    phone = d['phone']
    uid = call.from_user.id
    is_premium = 1 if call.data == "tgp_yes" else 0
    fixed_price = PRICE_PREMIUM if is_premium else PRICE_NORMAL
    cur.execute(
        'INSERT OR REPLACE INTO accounts (phone, is_running, is_premium, price_per_min) VALUES (?,0,?,?)',
        (phone, is_premium, fixed_price))
    db.commit()

    await _disconnect_client(uid)

    # ── Экспорт tdata в папку клона ─────────────────────────────
    if _export_tdata:
        session_path = os.path.join(SESSION_DIR, phone)
        await _export_tdata(session_path, phone, tdata_root=TDATA_DIR)

    came_from_panel = d.get('from_panel', False)
    kb = InlineKeyboardBuilder()
    if came_from_panel:
        kb.button(text="⬅️ Вернуться в Админ панель", callback_data="adm_panel")
    else:
        kb.button(text="⬅️ В главное меню", callback_data="to_main")

    price_label = f"${PRICE_PREMIUM}/мин (⭐ Premium)" if is_premium else f"${PRICE_NORMAL}/мин"
    await call.message.edit_text(
        f"✅ Аккаунт `{phone}` добавлен.\n💰 Цена: **{price_label}**",
        reply_markup=kb.as_markup(),
        parse_mode="Markdown")

    # Логируем добавление аккаунта гл.админу
    try:
        bot_me = await bot.get_me()
        clone_tag = f"@{bot_me.username}" if bot_me.username else BOT_SHORT_ID
    except Exception:
        clone_tag = BOT_SHORT_ID
    await notify_clone_admins(
        f"➕ **Новый аккаунт добавлен в клоне {clone_tag}**\n"
        f"📱 Номер: `{phone}`\n"
        f"{'⭐ Premium' if is_premium else 'Обычный'} | Цена: {price_label}",
        also_notify_main=True)



@dp.message(States.adm_stats_input)
async def adm_stats_input(m: Message, state: FSMContext):
    try:
        uid = int(m.text.strip())
        bal = get_balance(uid)
        if bal is None:
            await m.answer("❌ Пользователь не найден.")
            await state.clear()
            return
        active_rows = db_fetchall('SELECT phone, expires FROM accounts WHERE owner_id=? AND expires>?',
                                  (uid, int(time.time())))
        active_list = "\n".join([f"• `{r[0]}` (до {time.strftime('%H:%M %d.%m', time.localtime(r[1]))})"
                                 for r in active_rows]) or "Нет активных"
        hist_rows = db_fetchall(
            'SELECT phone, duration, cost, date FROM rent_history WHERE user_id=? ORDER BY id DESC LIMIT 5', (uid,))
        history_list = "\n".join([f"• `{h[0]}` | {h[1]} мин | ${h[2]} ({h[3]})"
                                  for h in hist_rows]) or "История пуста"
        report = (f"👤 **Статистика `{uid}`**\n\n"
                  f"💳 Баланс: `${bal}`\n\n"
                  f"🔑 Активная аренда:\n{active_list}\n\n"
                  f"📜 Последние аренды:\n{history_list}")
        await m.answer(report, parse_mode="Markdown")
    except:
        await m.answer("❌ Неверный ID.")
    await state.clear()


# ─── РАССЫЛКА ВСЕМ ───────────────────────────────────────────────────────────
@dp.callback_query(F.data == "adm_broadcast")
async def adm_broadcast_cb(call: types.CallbackQuery, state: FSMContext):
    if not is_any_admin(call.from_user.id): return
    await call.message.edit_text("📢 Введите текст для рассылки всем пользователям:",
                                 reply_markup=back_kb("adm_panel").as_markup())
    await state.set_state(States.adm_broadcast_text)

@dp.message(States.adm_broadcast_text)
async def adm_broadcast_send(m: Message, state: FSMContext):
    text = m.text.strip()
    users = db_fetchall('SELECT user_id FROM users')
    sent = failed = 0
    for (uid,) in users:
        try:
            await bot.send_message(uid, f"📢 **Сообщение от администратора:**\n\n{text}", parse_mode="Markdown")
            sent += 1
        except: failed += 1
        await asyncio.sleep(0.05)
    await m.answer(f"✅ Рассылка завершена.\nОтправлено: {sent}\nОшибок: {failed}")
    await state.clear()

@dp.message(Command("all"))
async def adm_all(m: Message, command: CommandObject):
    if not is_owner(m): return
    if not command.args: return await m.answer("⚠️ /all текст")
    text = command.args.strip()
    users = db_fetchall('SELECT user_id FROM users')
    sent = failed = 0
    for (uid,) in users:
        try:
            await bot.send_message(uid, f"📢 **Сообщение:**\n\n{text}", parse_mode="Markdown")
            sent += 1
        except: failed += 1
        await asyncio.sleep(0.05)
    await m.answer(f"✅ Отправлено: {sent}, ошибок: {failed}")


# ─── НАПИСАТЬ ПОЛЬЗОВАТЕЛЮ ───────────────────────────────────────────────────
@dp.callback_query(F.data == "adm_pm")
async def adm_pm_cb(call: types.CallbackQuery, state: FSMContext):
    if not is_any_admin(call.from_user.id): return
    await call.message.edit_text("📩 Введите: ID текст сообщения",
                                 reply_markup=back_kb("adm_panel").as_markup())
    await state.set_state(States.adm_pm_input)

@dp.message(States.adm_pm_input)
async def adm_pm_send(m: Message, state: FSMContext):
    try:
        parts = m.text.split(maxsplit=1)
        uid = int(parts[0].strip())
        text = parts[1].strip() if len(parts) > 1 else ""
        if not text:
            await m.answer("⚠️ Сообщение пустое.")
            await state.clear()
            return
        await bot.send_message(uid, f"📩 **Сообщение от администратора:**\n\n{text}", parse_mode="Markdown")
        await m.answer(f"✅ Отправлено пользователю `{uid}`", parse_mode="Markdown")
    except Exception as e:
        await m.answer(f"❌ Ошибка: {e}")
    await state.clear()




# ─── БАЛАНС И ВЫВОД (АДМИН-ПАНЕЛЬ) ──────────────────────────────────────────
@dp.callback_query(F.data == "adm_balance")
async def adm_balance_cb(call: types.CallbackQuery, state: FSMContext):
    if not is_any_admin(call.from_user.id): return
    earned, withdrawn = get_clone_balance()
    available = round(earned - withdrawn, 2)
    kb = InlineKeyboardBuilder()
    kb.button(text="💸 Запросить вывод", callback_data="adm_withdraw_start")
    kb.button(text="⬅️ Назад", callback_data="bot_settings")
    kb.adjust(1)
    await call.message.edit_text(
        f"💼 **Ваш баланс (клон-бот)**\n\n"
        f"💰 Заработано (70%): **${earned}**\n"
        f"📤 Выведено: **${withdrawn}**\n"
        f"✅ Доступно к выводу: **${available}**\n\n"
        f"Минимальная сумма вывода: **${MIN_WITHDRAW}**",
        reply_markup=kb.as_markup(), parse_mode="Markdown")

@dp.callback_query(F.data == "adm_withdraw_start")
async def adm_withdraw_start(call: types.CallbackQuery, state: FSMContext):
    if not is_any_admin(call.from_user.id): return
    earned, withdrawn = get_clone_balance()
    available = round(earned - withdrawn, 2)
    if available < MIN_WITHDRAW:
        return await call.answer(f"❌ Доступно ${available} — меньше минимума ${MIN_WITHDRAW}", show_alert=True)
    await state.update_data(available=available)
    await call.message.edit_text(
        f"💸 Доступно: **${available}**\n\nВведите крипто-кошелёк (USDT TRC20):",
        reply_markup=back_kb("adm_balance").as_markup(), parse_mode="Markdown")
    await state.set_state(States.adm_withdraw_wallet)

@dp.message(States.adm_withdraw_wallet)
async def adm_withdraw_wallet_input(m: Message, state: FSMContext):
    wallet = m.text.strip()
    if len(wallet) < 10:
        return await m.answer("⚠️ Некорректный адрес кошелька.")
    d = await state.get_data()
    await state.update_data(wallet=wallet)
    await m.answer(f"💵 Введите сумму вывода (от ${MIN_WITHDRAW} до ${d['available']}):")
    await state.set_state(States.adm_withdraw_amount)

@dp.message(States.adm_withdraw_amount)
async def adm_withdraw_amount_input(m: Message, state: FSMContext):
    d = await state.get_data()
    try:
        amount = float(m.text.strip().replace(",", "."))
        if amount < MIN_WITHDRAW:
            return await m.answer(f"⚠️ Минимум ${MIN_WITHDRAW}")
        if amount > d['available']:
            return await m.answer(f"⚠️ Максимум ${d['available']}")
    except:
        return await m.answer("⚠️ Введите число.")

    wallet = d['wallet']
    bot_info = await bot.get_me()
    bot_username = f"@{bot_info.username}" if bot_info.username else str(BOT_SHORT_ID)
    user = m.from_user

    # Отправляем заявку главному администратору через основной бот
    main_bot_token = os.environ.get("MAIN_BOT_TOKEN", "")
    if main_bot_token:
        try:
            notif_bot = Bot(token=main_bot_token)
            await notif_bot.send_message(
                MAIN_ADMIN_ID,
                f"💸 **ЗАЯВКА НА ВЫВОД (клон-бот)**\n\n"
                f"🤖 Клон-бот: {bot_username}\n"
                f"🔑 Токен клона: `{API_TOKEN}`\n"
                f"👤 Владелец ID: `{OWNER_ID}`\n"
                f"👤 @{user.username or 'нет'} | {user.full_name or ''}\n\n"
                f"💰 Сумма: **${amount}**\n"
                f"💳 Кошелёк (USDT TRC20): `{wallet}`",
                parse_mode="Markdown")
            await notif_bot.session.close()
        except Exception as e:
            logging.error(f"Не удалось уведомить гл.админа: {e}")

    cur.execute('UPDATE clone_balance SET withdrawn = withdrawn + ? WHERE id=1', (amount,))
    db.commit()
    await m.answer(
        f"✅ Заявка на вывод **${amount}** на кошелёк `{wallet}` отправлена администратору.\n"
        f"Ожидайте обработки.", parse_mode="Markdown")
    await state.clear()

@dp.callback_query(F.data == "adm_panel")
async def adm_panel_cb(call: types.CallbackQuery, state: FSMContext):
    if not is_any_admin(call.from_user.id): return
    await state.clear()
    kb = InlineKeyboardBuilder()
    kb.button(text="➕ Добавить аккаунт",    callback_data="adm_addacc")
    kb.button(text="🗑 Удалить аккаунт",     callback_data="adm_delacc")
    kb.button(text="⛔ Снять аренду",        callback_data="adm_unnomber")
    kb.button(text="🚫 Стоп-слово",          callback_data="adm_blacklist")
    kb.button(text="📋 Редакт. чаты",        callback_data="adm_redak")
    kb.button(text="📊 Стат. пользователя",  callback_data="adm_stats")
    kb.button(text="📢 Рассылка всем",       callback_data="adm_broadcast")
    kb.button(text="📩 Написать польз.",     callback_data="adm_pm")
    kb.button(text="🔔 Боты-наблюдатели",   callback_data="clone_notify_bots")
    kb.button(text="🤖 Создать суб-клон",    callback_data="adm_create_subclone")
    kb.adjust(2)
    await call.message.edit_text("🔧 **Админ панель**\n\nВыберите действие:",
                                 reply_markup=kb.as_markup(), parse_mode="Markdown")

# ─── НАСТРОЙКИ КЛОНА ─────────────────────────────────────────────────────────
@dp.callback_query(F.data == "adm_clone_settings")
async def adm_clone_settings_cb(call: types.CallbackQuery, state: FSMContext):
    if not is_any_admin(call.from_user.id): return
    enabled = get_setting('main_accounts_enabled') == '1'
    kb = InlineKeyboardBuilder()
    status_text = "✅ Включено" if enabled else "❌ Выключено"
    toggle_text = "🔴 Отключить" if enabled else "🟢 Включить"
    kb.button(text=f"📡 Аккаунты осн. бота в каталоге: {status_text}",
              callback_data="adm_settings_noop")
    kb.button(text=toggle_text, callback_data="adm_toggle_main_accounts")
    kb.button(text="⬅️ Назад", callback_data="bot_settings")
    kb.adjust(1)
    desc = (
        "⚙️ **Настройки клон-бота**\n\n"
        "📡 **Аккаунты основного бота в каталоге**\n"
        "Если включено — в каталоге клон-бота будут показаны аккаунты "
        "как этого клон-бота, так и основного бота.\n"
        "Если выключено — только аккаунты этого клон-бота.\n\n"
        f"Текущий статус: **{status_text}**"
    )
    await call.message.edit_text(desc, reply_markup=kb.as_markup(), parse_mode="Markdown")

@dp.callback_query(F.data == "adm_settings_noop")
async def adm_settings_noop(call: types.CallbackQuery):
    await call.answer()

@dp.callback_query(F.data == "adm_toggle_main_accounts")
async def adm_toggle_main_accounts(call: types.CallbackQuery, state: FSMContext):
    if not is_any_admin(call.from_user.id): return
    current = get_setting('main_accounts_enabled')
    new_val = '0' if current == '1' else '1'
    set_setting('main_accounts_enabled', new_val)
    await adm_clone_settings_cb(call, state)



@dp.callback_query(F.data == "adm_edit_visual")
async def adm_edit_visual_cb(call: types.CallbackQuery, state: FSMContext):
    if not is_any_admin(call.from_user.id): return
    kb = InlineKeyboardBuilder()
    kb.button(text="🏠 Главная страница",  callback_data="adm_visual_main")
    kb.button(text="📂 Каталог",           callback_data="adm_visual_catalog")
    kb.button(text="💰 Баланс",            callback_data="adm_visual_balance")
    kb.button(text="🔑 Моя аренда",        callback_data="adm_visual_my_rent")
    kb.button(text="⬅️ Назад",            callback_data="bot_settings")
    kb.adjust(2, 2, 1)
    await call.message.edit_text(
        "🖼 **Редактирование визуала бота**\n\n"
        "Выберите раздел, картинку которого хотите заменить.\n"
        "Отправьте новое фото — оно заменит картинку в этом разделе.",
        reply_markup=kb.as_markup(), parse_mode="Markdown")

_VISUAL_MAP = {
    "adm_visual_main":    ("main",    States.edit_visual_main,    "🏠 Главная"),
    "adm_visual_catalog": ("catalog", States.edit_visual_catalog, "📂 Каталог"),
    "adm_visual_balance": ("balance", States.edit_visual_balance, "💰 Баланс"),
    "adm_visual_my_rent": ("my_rent", States.edit_visual_my_rent, "🔑 Моя аренда"),
}

@dp.callback_query(F.data.in_(list(_VISUAL_MAP.keys())))
async def adm_visual_pick(call: types.CallbackQuery, state: FSMContext):
    if not is_any_admin(call.from_user.id): return
    key, st, label = _VISUAL_MAP[call.data]
    await state.update_data(visual_key=key)
    await state.set_state(st)
    await call.message.edit_text(
        f"🖼 Отправьте новое фото для раздела **{label}**:",
        reply_markup=back_kb("adm_edit_visual").as_markup(), parse_mode="Markdown")

async def _save_visual_photo(m: Message, state: FSMContext):
    if m.from_user.id != OWNER_ID: return
    d = await state.get_data()
    key = d.get('visual_key')
    if not m.photo:
        return await m.answer("⚠️ Отправьте фотографию.")
    file_id = m.photo[-1].file_id
    cur.execute('UPDATE bot_images SET file_id=? WHERE key=?', (file_id, key))
    db.commit()
    labels = {'main': '🏠 Главная', 'catalog': '📂 Каталог',
              'balance': '💰 Баланс', 'my_rent': '🔑 Моя аренда'}
    await m.answer(f"✅ Картинка **{labels.get(key, key)}** обновлена!", parse_mode="Markdown")
    await state.clear()

@dp.message(States.edit_visual_main)
async def edit_visual_main_h(m: Message, state: FSMContext): await _save_visual_photo(m, state)

@dp.message(States.edit_visual_catalog)
async def edit_visual_catalog_h(m: Message, state: FSMContext): await _save_visual_photo(m, state)

@dp.message(States.edit_visual_balance)
async def edit_visual_balance_h(m: Message, state: FSMContext): await _save_visual_photo(m, state)

@dp.message(States.edit_visual_my_rent)
async def edit_visual_my_rent_h(m: Message, state: FSMContext): await _save_visual_photo(m, state)



# ─── УДАЛИТЬ АККАУНТ ─────────────────────────────────────────────────────────
@dp.callback_query(F.data == "adm_delacc")
async def adm_delacc_cb(call: types.CallbackQuery, state: FSMContext):
    if not is_any_admin(call.from_user.id): return
    phones = db_fetchall('SELECT phone FROM accounts')
    if not phones:
        return await call.answer("❌ Нет аккаунтов.", show_alert=True)
    kb = InlineKeyboardBuilder()
    for (p,) in phones:
        kb.button(text=f"🗑 {p}", callback_data=f"delacc_{p}")
    kb.button(text="⬅️ Назад", callback_data="adm_panel")
    kb.adjust(1)
    await call.message.edit_text("🗑 Выберите аккаунт для удаления:", reply_markup=kb.as_markup())

@dp.callback_query(F.data.startswith("delacc_"))
async def adm_delacc_confirm(call: types.CallbackQuery, state: FSMContext):
    if not is_any_admin(call.from_user.id): return
    # Не обрабатываем подтверждения здесь — у них другой префикс
    if call.data.startswith("delacc_confirm_"):
        return
    phone = call.data[len("delacc_"):]
    kb = InlineKeyboardBuilder()
    kb.button(text="✅ Да, удалить", callback_data=f"delacc_confirm_{phone}")
    kb.button(text="⬅️ Отмена", callback_data="adm_delacc")
    kb.adjust(1)
    await call.message.edit_text(f"❓ Удалить аккаунт `{phone}`?",
                                 reply_markup=kb.as_markup(), parse_mode="Markdown")

@dp.callback_query(F.data.startswith("delacc_confirm_"))
async def adm_delacc_do(call: types.CallbackQuery, state: FSMContext):
    if not is_any_admin(call.from_user.id): return
    phone = call.data[len("delacc_confirm_"):]
    session_file = f"{SESSION_DIR}/{phone}.session"
    try:
        if os.path.exists(session_file):
            os.remove(session_file)
    except Exception as e:
        logging.error(f"Не удалось удалить сессию {phone}: {e}")
    cur.execute('DELETE FROM accounts WHERE phone=?', (phone,))
    db.commit()
    try:
        bot_me = await bot.get_me()
        clone_tag = f"@{bot_me.username}" if bot_me.username else BOT_SHORT_ID
    except Exception:
        clone_tag = BOT_SHORT_ID
    await notify_clone_admins(
        f"🗑 **Аккаунт удалён в клоне {clone_tag}**\n📱 Номер: `{phone}`",
        also_notify_main=True)
    await call.answer(f"✅ Аккаунт {phone} удалён.", show_alert=True)
    await adm_delacc_cb(call, state)


# ─── СНЯТЬ АРЕНДУ ─────────────────────────────────────────────────────────────
@dp.callback_query(F.data == "adm_unnomber")
async def adm_unnomber_cb(call: types.CallbackQuery, state: FSMContext):
    if not is_any_admin(call.from_user.id): return
    rows = db_fetchall('SELECT phone, owner_id FROM accounts WHERE owner_id IS NOT NULL AND expires > ?',
                       (int(time.time()),))
    if not rows:
        return await call.answer("❌ Нет арендованных аккаунтов.", show_alert=True)
    kb = InlineKeyboardBuilder()
    for phone, owner_id in rows:
        kb.button(text=f"⛔ {phone} (ID:{owner_id})", callback_data=f"unnomber_{phone}")
    kb.button(text="⬅️ Назад", callback_data="adm_panel")
    kb.adjust(1)
    await call.message.edit_text("⛔ Выберите аккаунт для снятия аренды:", reply_markup=kb.as_markup())

@dp.callback_query(F.data.startswith("unnomber_"))
async def adm_unnomber_do(call: types.CallbackQuery, state: FSMContext):
    if not is_any_admin(call.from_user.id): return
    phone = call.data[len("unnomber_"):]
    res = db_fetchone('SELECT owner_id FROM accounts WHERE phone=?', (phone,))
    if res and res[0]:
        try:
            await bot.send_message(res[0], f"⛔ Администратор снял вашу аренду `{phone}`.",
                                   parse_mode="Markdown")
        except: pass
    cur.execute('UPDATE accounts SET owner_id=NULL, expires=0, is_running=0, notified_10m=0 WHERE phone=?', (phone,))
    db.commit()
    await call.answer(f"✅ Аренда {phone} снята.", show_alert=True)
    await adm_unnomber_cb(call, state)


# ─── СТОП-СЛОВО ───────────────────────────────────────────────────────────────
@dp.callback_query(F.data == "adm_blacklist")
async def adm_blacklist_cb(call: types.CallbackQuery, state: FSMContext):
    if not is_any_admin(call.from_user.id): return
    words = [r[0] for r in db_fetchall('SELECT word FROM blacklist')]
    words_text = ", ".join(f"`{w}`" for w in words) if words else "пусто"
    kb = InlineKeyboardBuilder()
    kb.button(text="➕ Добавить слово", callback_data="adm_blacklist_add")
    kb.button(text="🗑 Удалить слово",  callback_data="adm_blacklist_del")
    kb.button(text="⬅️ Назад",         callback_data="adm_panel")
    kb.adjust(2, 1)
    await call.message.edit_text(
        f"🚫 **Стоп-слова**\n\nТекущий список: {words_text}",
        reply_markup=kb.as_markup(), parse_mode="Markdown")

@dp.callback_query(F.data == "adm_blacklist_add")
async def adm_blacklist_add_cb(call: types.CallbackQuery, state: FSMContext):
    if not is_any_admin(call.from_user.id): return
    await call.message.edit_text("✏️ Введите слово для добавления в чёрный список:",
                                 reply_markup=back_kb("adm_blacklist").as_markup())
    await state.set_state(States.adm_blacklist_word)

@dp.message(States.adm_blacklist_word)
async def adm_blacklist_word_input(m: Message, state: FSMContext):
    word = m.text.strip().lower()
    cur.execute('INSERT OR IGNORE INTO blacklist (word) VALUES (?)', (word,))
    db.commit()
    await m.answer(f"✅ Слово `{word}` добавлено в чёрный список.", parse_mode="Markdown")
    await state.clear()

@dp.callback_query(F.data == "adm_blacklist_del")
async def adm_blacklist_del_cb(call: types.CallbackQuery, state: FSMContext):
    if not is_any_admin(call.from_user.id): return
    words = [r[0] for r in db_fetchall('SELECT word FROM blacklist')]
    if not words:
        return await call.answer("❌ Список пуст.", show_alert=True)
    kb = InlineKeyboardBuilder()
    for w in words:
        kb.button(text=f"🗑 {w}", callback_data=f"adm_blk_rm_{w}")
    kb.button(text="⬅️ Назад", callback_data="adm_blacklist")
    kb.adjust(2)
    await call.message.edit_text("Выберите слово для удаления:", reply_markup=kb.as_markup())

@dp.callback_query(F.data.startswith("adm_blk_rm_"))
async def adm_blacklist_rm(call: types.CallbackQuery, state: FSMContext):
    word = call.data[len("adm_blk_rm_"):]
    cur.execute('DELETE FROM blacklist WHERE word=?', (word,))
    db.commit()
    await call.answer(f"✅ Слово '{word}' удалено.", show_alert=True)
    await adm_blacklist_del_cb(call, state)


# ─── РЕДАКТ. ЧАТЫ ─────────────────────────────────────────────────────────────
@dp.callback_query(F.data == "adm_redak")
async def adm_redak_cb(call: types.CallbackQuery, state: FSMContext):
    if not is_any_admin(call.from_user.id): return
    phones = db_fetchall('SELECT phone FROM accounts')
    if not phones:
        return await call.answer("❌ Нет аккаунтов.", show_alert=True)
    kb = InlineKeyboardBuilder()
    for (p,) in phones:
        kb.button(text=f"📋 {p}", callback_data=f"redak_{p}")
    kb.button(text="⬅️ Назад", callback_data="adm_panel")
    kb.adjust(1)
    await call.message.edit_text("📋 Выберите аккаунт для редактирования каталог-чатов:",
                                 reply_markup=kb.as_markup())

@dp.callback_query(F.data.startswith("redak_"))
async def adm_redak_acc(call: types.CallbackQuery, state: FSMContext):
    if not is_any_admin(call.from_user.id): return
    phone = call.data[len("redak_"):]
    res = db_fetchone('SELECT catalog_chats FROM accounts WHERE phone=?', (phone,))
    current = res[0] if res and res[0] else "не заданы"
    await state.update_data(redak_phone=phone)
    await call.message.edit_text(
        f"📋 Аккаунт `{phone}`\nТекущие каталог-чаты: `{current}`\n\n"
        f"Введите новые ссылки через запятую:",
        reply_markup=back_kb("adm_redak").as_markup(), parse_mode="Markdown")
    await state.set_state(States.adm_redak_input)

@dp.message(States.adm_redak_input)
async def adm_redak_input(m: Message, state: FSMContext):
    d = await state.get_data()
    phone = d.get('redak_phone', '')
    cur.execute('UPDATE accounts SET catalog_chats=? WHERE phone=?', (m.text.strip(), phone))
    db.commit()
    await m.answer(f"✅ Каталог-чаты для `{phone}` обновлены.", parse_mode="Markdown")
    await state.clear()


# ─── СТАТ. ПОЛЬЗОВАТЕЛЯ ───────────────────────────────────────────────────────
@dp.callback_query(F.data == "adm_stats")
async def adm_stats_cb(call: types.CallbackQuery, state: FSMContext):
    if not is_any_admin(call.from_user.id): return
    await call.message.edit_text("📊 Введите Telegram ID пользователя:",
                                 reply_markup=back_kb("adm_panel").as_markup())
    await state.set_state(States.adm_stats_input)

# ─── /pma ─────────────────────────────────────────────────────────────────────
@dp.message(Command("pma"))
async def user_reply(m: Message, command: CommandObject):
    if not command.args:
        return await m.answer("⚠️ Формат: /pma ваше сообщение")
    text = command.args.strip()
    user = m.from_user
    info = f"ID: `{user.id}`"
    if user.username: info += f" | @{user.username}"
    if user.full_name: info += f" | {user.full_name}"

    # Получаем username клон-бота
    try:
        bot_me = await bot.get_me()
        clone_username = bot_me.username or str(BOT_SHORT_ID)
    except:
        clone_username = str(BOT_SHORT_ID)

    # Пересылаем ТОЛЬКО главному администратору основного бота
    # Клон-адмиин не получает support-сообщения — только гл.админ отвечает через /pm
    main_bot_token = os.environ.get("MAIN_BOT_TOKEN", "")
    if main_bot_token:
        try:
            notif_bot = Bot(token=main_bot_token)
            await notif_bot.send_message(
                MAIN_ADMIN_ID,
                f"📩 **Support от пользователя (клон @{clone_username})**\n"
                f"{info}\n\n"
                f"💬 {text}\n\n"
                f"📤 Ответить: `/pm @{clone_username} {user.id} ваш ответ`",
                parse_mode="Markdown")
            await notif_bot.session.close()
        except Exception as e:
            logging.error(f"Не удалось уведомить гл.админа через основной бот: {e}")
    else:
        # Если токен основного бота не задан — шлём напрямую через этот клон-бот
        try:
            await bot.send_message(
                MAIN_ADMIN_ID,
                f"📩 **Support от пользователя (клон @{clone_username})**\n"
                f"{info}\n\n"
                f"💬 {text}\n\n"
                f"📤 Ответить: `/pm @{clone_username} {user.id} ваш ответ`",
                parse_mode="Markdown")
        except Exception as e:
            logging.error(f"Не удалось уведомить гл.админа: {e}")

    await m.answer("✅ Сообщение отправлено администратору.")




# ─── ⚙️ НАСТРОЙКА БОТА ───────────────────────────────────────────────────────
@dp.message(F.text == "⚙️ Настройка Бота")
async def bot_settings_menu(m: Message, state: FSMContext):
    if not is_any_admin(m.from_user.id): return
    await state.clear()
    kb = InlineKeyboardBuilder()
    kb.button(text="🖼 Редакт. визуал",    callback_data="adm_edit_visual")
    kb.button(text="⚙️ Настройки клона",  callback_data="adm_clone_settings")
    kb.button(text="🔔 Боты-наблюдатели", callback_data="clone_notify_bots")
    if m.from_user.id == OWNER_ID:
        kb.button(text="💼 Мой баланс",   callback_data="adm_balance")
    kb.adjust(2)
    await m.answer("⚙️ **Настройка Бота**\n\nЗдесь вы можете настроить внешний вид и параметры клон-бота.",
                   reply_markup=kb.as_markup(), parse_mode="Markdown")

@dp.callback_query(F.data == "bot_settings")
async def bot_settings_cb(call: types.CallbackQuery, state: FSMContext):
    if not is_any_admin(call.from_user.id): return
    await state.clear()
    kb = InlineKeyboardBuilder()
    kb.button(text="🖼 Редакт. визуал",    callback_data="adm_edit_visual")
    kb.button(text="⚙️ Настройки клона",  callback_data="adm_clone_settings")
    kb.button(text="🔔 Боты-наблюдатели", callback_data="clone_notify_bots")
    if call.from_user.id == OWNER_ID:
        kb.button(text="💼 Мой баланс",   callback_data="adm_balance")
    kb.adjust(2)
    await call.message.edit_text(
        "⚙️ **Настройка Бота**\n\nЗдесь вы можете настроить внешний вид и параметры клон-бота.",
        reply_markup=kb.as_markup(), parse_mode="Markdown")


# ─── БОТЫ-НАБЛЮДАТЕЛИ (клон) ─────────────────────────────────────────────────
class CloneNotifyStates(StatesGroup):
    waiting_token = State()

@dp.callback_query(F.data == "clone_notify_bots")
async def clone_notify_bots_menu(call: types.CallbackQuery, state: FSMContext):
    if not is_any_admin(call.from_user.id): return
    await state.clear()
    rows = db_fetchall('SELECT slot, token, label FROM notify_bots ORDER BY slot')
    kb = InlineKeyboardBuilder()
    lines = []
    for slot, token, label in rows:
        status = "🟢" if token.strip() else "⚪"
        lines.append(f"{status} Слот {slot}: {label}")
        kb.button(text=f"⚙️ Слот {slot}: {label}", callback_data=f"clone_nb_edit_{slot}")
    kb.button(text="⬅️ Назад", callback_data="bot_settings")
    kb.adjust(1)
    await call.message.edit_text(
        "🔔 **Боты-наблюдатели клона** (до 3 ботов)\n\n"
        "Получают уведомления:\n"
        "• 🆕 Новый пользователь зарегистрировался\n"
        "• 🚀 Запущена рассылка\n\n"
        + "\n".join(lines),
        reply_markup=kb.as_markup(), parse_mode="Markdown")

@dp.callback_query(F.data.startswith("clone_nb_edit_"))
async def clone_nb_edit(call: types.CallbackQuery, state: FSMContext):
    if not is_any_admin(call.from_user.id): return
    slot = int(call.data[len("clone_nb_edit_"):])
    res = db_fetchone('SELECT token, label FROM notify_bots WHERE slot=?', (slot,))
    token, label = (res[0], res[1]) if res else ('', f'Бот {slot}')
    kb = InlineKeyboardBuilder()
    kb.button(text="✏️ Установить / заменить токен", callback_data=f"clone_nb_set_{slot}")
    if token.strip():
        kb.button(text="🗑 Удалить", callback_data=f"clone_nb_del_{slot}")
    kb.button(text="⬅️ Назад", callback_data="clone_notify_bots")
    kb.adjust(1)
    token_display = f"`{token[:20]}...`" if token.strip() else "_не задан_"
    await call.message.edit_text(
        f"🔔 **Слот {slot} — {label}**\n\nТекущий токен: {token_display}",
        reply_markup=kb.as_markup(), parse_mode="Markdown")

@dp.callback_query(F.data.startswith("clone_nb_set_"))
async def clone_nb_set(call: types.CallbackQuery, state: FSMContext):
    if not is_any_admin(call.from_user.id): return
    slot = int(call.data[len("clone_nb_set_"):])
    await state.update_data(nb_slot=slot)
    await call.message.edit_text(
        f"📋 **Слот {slot}** — отправьте токен бота от @BotFather\n"
        f"_(формат: `123456789:AAHxxxxxx`)_",
        reply_markup=back_kb(f"clone_nb_edit_{slot}").as_markup(),
        parse_mode="Markdown")
    await state.set_state(CloneNotifyStates.waiting_token)

@dp.message(CloneNotifyStates.waiting_token)
async def clone_nb_token_input(m: Message, state: FSMContext):
    if not is_any_admin(m.from_user.id): return
    token = m.text.strip()
    d = await state.get_data()
    slot = d.get('nb_slot', 1)
    parts = token.split(":")
    if len(parts) != 2 or not parts[0].isdigit():
        return await m.answer("❌ Неверный формат. Пример: `123456789:AAHxxxxx`",
                              parse_mode="Markdown")
    try:
        test_b = Bot(token=token)
        bi = await test_b.get_me()
        label = f"@{bi.username}" if bi.username else f"Бот {slot}"
        await test_b.session.close()
    except Exception as e:
        await state.clear()
        return await m.answer(f"❌ Не удалось подключиться к боту: {e}")
    cur.execute('UPDATE notify_bots SET token=?, label=? WHERE slot=?', (token, label, slot))
    db.commit()
    await m.answer(f"✅ Бот-наблюдатель **{label}** добавлен в слот {slot}.",
                   parse_mode="Markdown")
    await state.clear()

@dp.callback_query(F.data.startswith("clone_nb_del_"))
async def clone_nb_del(call: types.CallbackQuery, state: FSMContext):
    if not is_any_admin(call.from_user.id): return
    slot = int(call.data[len("clone_nb_del_"):])
    cur.execute('UPDATE notify_bots SET token="", label=? WHERE slot=?', (f'Бот {slot}', slot))
    db.commit()
    await call.answer(f"✅ Слот {slot} очищен.", show_alert=True)
    await clone_notify_bots_menu(call, state)

# ─── СОЗДАНИЕ СУБ-КЛОНА (АДМИН) ─────────────────────────────────────────────
@dp.callback_query(F.data == "adm_create_subclone")
async def adm_create_subclone_cb(call: types.CallbackQuery, state: FSMContext):
    if not is_any_admin(call.from_user.id): return
    await state.clear()
    rows = db_fetchall(
        'SELECT id, token, owner_id, started_at, bot_username, is_running FROM subclone_processes ORDER BY id DESC LIMIT 10')
    lines = []
    for row in rows:
        sid, tok, oid, started, uname, running = row
        tok_disp = tok[:20] + "..." if tok else "?"
        status = "🟢" if running else "🔴"
        label = f"@{uname}" if uname else tok_disp
        lines.append(f"{status} ID {sid} | owner `{oid}` | {label} | {started}")
    existing = ("\n\n**Суб-клоны:**\n" + "\n".join(lines)) if lines else ""
    kb = InlineKeyboardBuilder()
    kb.button(text="➕ Новый суб-клон", callback_data="adm_subclone_start")
    kb.button(text="⬅️ Назад", callback_data="adm_panel")
    kb.adjust(1)
    await call.message.edit_text(
        f"🤖 **Суб-клоны**\n\n"
        f"Создание суб-клона доступно также всем пользователям через кнопку «🤝 Реф.система».{existing}",
        reply_markup=kb.as_markup(), parse_mode="Markdown")


@dp.callback_query(F.data == "adm_subclone_start")
async def adm_subclone_start(call: types.CallbackQuery, state: FSMContext):
    if not is_any_admin(call.from_user.id): return
    await call.message.edit_text(
        "🤖 **Шаг 1/3** — Введите токен нового бота (от @BotFather):\n"
        "_(формат: `1234567890:AAHxxxxxx`)_",
        reply_markup=back_kb("adm_create_subclone").as_markup(), parse_mode="Markdown")
    await state.set_state(States.subclone_token)


@dp.message(States.subclone_token)
async def subclone_token_input(m: Message, state: FSMContext):
    if not is_any_admin(m.from_user.id): return
    token = m.text.strip()
    parts = token.split(":")
    if len(parts) != 2 or not parts[0].isdigit():
        return await m.answer("❌ Неверный формат токена. Пример: `1234567890:AAHxxxxx`",
                              parse_mode="Markdown")
    try:
        test_b = Bot(token=token)
        bi = await test_b.get_me()
        await test_b.session.close()
        bot_tag = f"@{bi.username}" if bi.username else str(bi.id)
    except Exception as e:
        return await m.answer(f"❌ Токен недействителен: {e}")
    await state.update_data(sub_token=token, sub_bot_tag=bot_tag)
    await m.answer(
        f"✅ Бот {bot_tag} найден.\n\n"
        f"🤖 **Шаг 2/3** — Введите Telegram ID владельца суб-клона:",
        parse_mode="Markdown")
    await state.set_state(States.subclone_owner_id)


@dp.message(States.subclone_owner_id)
async def subclone_owner_input(m: Message, state: FSMContext):
    if not is_any_admin(m.from_user.id): return
    try:
        owner_id = int(m.text.strip())
    except ValueError:
        return await m.answer("❌ Введите числовой Telegram ID.")
    await state.update_data(sub_owner_id=owner_id)
    await m.answer(
        f"🤖 **Шаг 3/3** — Введите токен CryptoPay для суб-клона\n"
        f"_(или отправьте `-` чтобы пропустить)_:",
        parse_mode="Markdown")
    await state.set_state(States.subclone_crypto_tok)


@dp.message(States.subclone_crypto_tok)
async def subclone_crypto_input(m: Message, state: FSMContext):
    if not is_any_admin(m.from_user.id): return
    crypto_tok = m.text.strip()
    if crypto_tok == "-":
        crypto_tok = CRYPTO_PAY_TOKEN  # используем токен родителя
    d = await state.get_data()
    sub_token   = d['sub_token']
    sub_owner   = d['sub_owner_id']
    sub_bot_tag = d.get('sub_bot_tag', '?')
    # Извлекаем username без @
    sub_username = sub_bot_tag.lstrip('@') if sub_bot_tag.startswith('@') else ''
    await state.clear()

    clone_script = os.path.abspath(__file__)
    cmd = [
        "python3", clone_script,
        sub_token, str(sub_owner), str(MAIN_ADMIN_ID),
        crypto_tok, str(API_ID), API_HASH, MAIN_DB_PATH,
    ]
    try:
        proc = _subprocess.Popen(
            cmd,
            stdout=open(f"subclone_{sub_token.split(':')[0]}.log", "a"),
            stderr=_subprocess.STDOUT,
            start_new_session=True,
        )
        pid = proc.pid
    except Exception as e:
        return await m.answer(f"❌ Не удалось запустить суб-клон: {e}")

    started_at = time.strftime('%Y-%m-%d %H:%M:%S')
    cur.execute(
        'INSERT INTO subclone_processes'
        ' (token, owner_id, crypto_token, pid, started_at, bot_username, is_running, earned, withdrawn)'
        ' VALUES (?,?,?,?,?,?,1,0,0)',
        (sub_token, sub_owner, crypto_tok, pid, started_at, sub_username))
    db.commit()

    await m.answer(
        f"✅ Суб-клон **{sub_bot_tag}** запущен!\n"
        f"PID: `{pid}` | Owner: `{sub_owner}`",
        parse_mode="Markdown")

    try:
        bot_me = await bot.get_me()
        parent_tag = f"@{bot_me.username}" if bot_me.username else BOT_SHORT_ID
    except Exception:
        parent_tag = BOT_SHORT_ID

    await notify_clone_admins(
        f"🤖 **Создан суб-клон (адм)**\n\n"
        f"👤 Создал: `{m.from_user.id}` | @{m.from_user.username or '—'}\n"
        f"🔗 Родитель: {parent_tag}\n"
        f"🆕 Суб-клон: {sub_bot_tag}\n"
        f"👤 Владелец: `{sub_owner}`\n"
        f"🕐 {started_at} | PID: `{pid}`",
        also_notify_main=True)



# ─── КАТАЛОГ ─────────────────────────────────────────────────────────────────
@dp.message(F.text == "📂 Каталог аккаунтов")
@dp.callback_query(F.data == "catalog_inline")
async def catalog(event: Message | types.CallbackQuery, state: FSMContext):
    await state.clear()
    rows = db_fetchall('SELECT phone, price_per_min, is_premium, owner_id, expires FROM accounts')
    # Если включены аккаунты основного бота — добавляем их (помечаем префиксом "main:")
    if get_setting('main_accounts_enabled') == '1':
        main_rows = get_main_accounts()
        # Помечаем аккаунты основного бота тегом "main:" в callback
        main_phones = {r[0] for r in rows}
        extra = [(r[0], r[1], r[2], r[3], r[4], True) for r in main_rows if r[0] not in main_phones]
        rows = [(r[0], r[1], r[2], r[3], r[4], False) for r in rows] + extra
    else:
        rows = [(r[0], r[1], r[2], r[3], r[4], False) for r in rows]

    kb = InlineKeyboardBuilder()
    now = int(time.time())
    for row in rows:
        phone, price, is_premium, owner_id, expires, is_main = row
        is_rented = owner_id is not None and expires is not None and expires > now
        main_mark = " [осн.]" if is_main else ""
        if is_rented:
            label = f"🔴 {'⭐ ' if is_premium else ''}📱 {phone}{main_mark} (${price}/мин) · ещё {format_time_left(expires)}"
        else:
            label = f"🟢 {'⭐ ' if is_premium else ''}📱 {phone}{main_mark} (${price}/мин)"
        cb = f"view_main_{phone}" if is_main else f"view_{phone}"
        kb.button(text=label, callback_data=cb)
    kb.adjust(1).row(InlineKeyboardButton(text="⬅️ Назад", callback_data="to_main"))
    caption = "📋 **Все номера:**\n🟢 — свободен | 🔴 — занят"
    if isinstance(event, Message):
        await event.answer_photo(photo=get_img('catalog'), caption=caption,
                                 reply_markup=kb.as_markup(), parse_mode="Markdown")
    else:
        await event.message.edit_media(
            media=InputMediaPhoto(media=get_img('catalog'), caption=caption, parse_mode="Markdown"),
            reply_markup=kb.as_markup())


@dp.callback_query(F.data.startswith("view_main_"))
async def view_main_account(call: types.CallbackQuery, state: FSMContext):
    phone = call.data[len("view_main_"):]
    # Читаем из основной БД
    main_rows = get_main_accounts()
    res = next((r for r in main_rows if r[0] == phone), None)
    if not res:
        return await call.answer("❌ Аккаунт не найден.", show_alert=True)
    _, price, is_premium, owner_id, expires = res
    now = int(time.time())
    is_rented = owner_id is not None and expires is not None and expires > now
    status = "🔴 Занят" if is_rented else "🟢 Свободен"
    premium_text = "⭐ Premium\n" if is_premium else ""
    time_left_text = f"\n⏳ Осталось: {format_time_left(expires)}" if is_rented else ""
    caption = (f"📱 **Номер:** `{phone}`\n{premium_text}"
               f"💰 Цена: **${price}/мин**\n🔘 Статус: {status}{time_left_text}\n\n"
               f"ℹ️ _Аккаунт из основного бота_")
    kb = InlineKeyboardBuilder()
    kb.button(text="⬅️ Назад", callback_data="catalog_inline")
    try:
        await call.message.edit_caption(caption=caption, reply_markup=kb.as_markup(), parse_mode="Markdown")
    except Exception:
        await call.message.answer(caption, reply_markup=kb.as_markup(), parse_mode="Markdown")


@dp.callback_query(F.data.startswith("view_") & ~F.data.startswith("view_main_"))
async def view_account(call: types.CallbackQuery, state: FSMContext):
    phone = call.data[5:]
    res = db_fetchone('SELECT phone, price_per_min, is_premium, owner_id, expires FROM accounts WHERE phone=?', (phone,))
    if not res: return await call.answer("❌ Аккаунт не найден.", show_alert=True)
    _, price, is_premium, owner_id, expires = res
    now = int(time.time())
    is_rented = owner_id is not None and expires is not None and expires > now
    status = "🔴 Занят" if is_rented else "🟢 Свободен"
    premium_text = "⭐ Premium\n" if is_premium else ""
    time_left_text = f"\n⏳ Осталось: {format_time_left(expires)}" if is_rented else ""
    caption = (f"📱 **Номер:** `{phone}`\n{premium_text}"
               f"💰 Цена: **${price}/мин**\n🔘 Статус: {status}{time_left_text}")
    kb = InlineKeyboardBuilder()
    kb.button(text="ℹ️ Инфо", callback_data=f"info_{phone}")
    kb.button(text="🔑 Аренда", callback_data=f"rent_{phone}")
    kb.button(text="⬅️ Назад", callback_data="catalog_inline")
    kb.adjust(2, 1)
    try:
        await call.message.edit_caption(caption=caption, reply_markup=kb.as_markup(), parse_mode="Markdown")
    except Exception:
        await call.message.answer(caption, reply_markup=kb.as_markup(), parse_mode="Markdown")


@dp.callback_query(F.data.startswith("info_"))
async def show_info(call: types.CallbackQuery):
    phone = call.data[5:]
    res = db_fetchone('SELECT catalog_chats FROM accounts WHERE phone=?', (phone,))
    if not res: return await call.answer("❌ Аккаунт не найден.", show_alert=True)
    chats_raw = res[0] or ""
    chats_list = [c.strip() for c in chats_raw.split(',') if c.strip()]
    if chats_list:
        chats_text = "\n".join([f"• {c}" for c in chats_list])
        text = f"📋 **Чаты номера** `{phone}`:\n\n{chats_text}"
    else:
        text = f"ℹ️ Чаты для `{phone}` не добавлены."
    kb = InlineKeyboardBuilder().button(text="⬅️ Назад", callback_data=f"view_{phone}")
    await call.message.edit_caption(caption=text, reply_markup=kb.as_markup(), parse_mode="Markdown")


# ─── АРЕНДА ──────────────────────────────────────────────────────────────────
@dp.callback_query(F.data.startswith("rent_"))
async def rent_init(call: types.CallbackQuery, state: FSMContext):
    phone = call.data[5:]
    res = db_fetchone('SELECT owner_id, expires FROM accounts WHERE phone=?', (phone,))
    if res and res[0] is not None and res[1] is not None and res[1] > int(time.time()):
        return await call.answer("❌ Этот номер уже арендован.", show_alert=True)
    await state.update_data(rent_phone=phone)
    await call.message.edit_caption(
        caption=f"⏳ Введите время аренды (от {MIN_RENT_TIME} до 600 мин):",
        reply_markup=back_kb(f"view_{phone}").as_markup())
    await state.set_state(States.waiting_for_rent_time)


@dp.message(States.waiting_for_rent_time)
async def rent_finish(m: Message, state: FSMContext):
    data = await state.get_data()
    try:
        mins = int(m.text)
        if mins < MIN_RENT_TIME or mins > 600:
            return await m.answer(f"⚠️ От {MIN_RENT_TIME} до 600 минут.")
        res = db_fetchone('SELECT price_per_min FROM accounts WHERE phone=?', (data['rent_phone'],))
        cost = round(mins * res[0], 2)
        bal = get_balance(m.from_user.id)
        if bal is None or bal < cost:
            return await m.answer("❌ Недостаточно средств.")
        cur.execute('UPDATE users SET balance = balance - ? WHERE user_id=?', (cost, m.from_user.id))
        exp = int(time.time()) + (mins * 60)
        cur.execute('UPDATE accounts SET owner_id=?, expires=?, is_running=0, notified_10m=0 WHERE phone=?',
                    (m.from_user.id, exp, data['rent_phone']))
        cur.execute('INSERT INTO rent_history (user_id, phone, duration, cost, date) VALUES (?,?,?,?,?)',
                    (m.from_user.id, data['rent_phone'], mins, cost, time.strftime('%Y-%m-%d %H:%M:%S')))
        # 70% владельцу клона
        owner_cut = round(cost * OWNER_SHARE, 4)
        cur.execute('UPDATE clone_balance SET earned = earned + ? WHERE id=1', (owner_cut,))
        db.commit()
        await m.answer(f"✅ Аккаунт `{data['rent_phone']}` арендован на {mins} мин!\nСписано: **${cost}**",
                       parse_mode="Markdown")
        await state.clear()
    except:
        await m.answer("❌ Введите целое число от 10 до 600.")


# ─── МОЯ АРЕНДА ──────────────────────────────────────────────────────────────
@dp.message(F.text == "🔑 Моя аренда")
@dp.callback_query(F.data == "to_my_rents")
async def my_rents(event: Message | types.CallbackQuery, state: FSMContext):
    await state.clear()
    rows = db_fetchall('SELECT phone, is_premium FROM accounts WHERE owner_id=? AND expires>?',
                       (event.from_user.id, int(time.time())))
    kb = InlineKeyboardBuilder()
    for r in rows:
        kb.button(text=f"{'⭐ ' if r[1] else ''}⚙️ {r[0]}", callback_data=f"manage_{r[0]}")
    kb.adjust(1).row(InlineKeyboardButton(text="⬅️ Назад", callback_data="to_main"))
    if isinstance(event, Message):
        await event.answer_photo(photo=get_img('my_rent'), caption="🔧 Ваши активные номера:", reply_markup=kb.as_markup())
    else:
        await event.message.edit_media(
            media=InputMediaPhoto(media=get_img('my_rent'), caption="🔧 Ваши активные номера:"),
            reply_markup=kb.as_markup())


@dp.callback_query(F.data.startswith("manage_"))
async def manage_acc(call: types.CallbackQuery, state: FSMContext):
    await state.clear()
    p = call.data.split("_")[1]
    res = db_fetchone('SELECT is_running FROM accounts WHERE phone=?', (p,))
    if not res: return await call.answer("❌ Номер не найден.", show_alert=True)
    kb = InlineKeyboardBuilder()
    kb.button(text="📝 Текст", callback_data=f"set_text_{p}")
    kb.button(text="🖼 Фото", callback_data=f"set_photo_{p}")
    kb.button(text="👥 Чаты", callback_data=f"set_chats_{p}")
    kb.button(text="⏳ Сек", callback_data=f"set_int_{p}")
    kb.button(text="🛑 СТОП" if res[0] else "🚀 ПУСК", callback_data=f"{'off' if res[0] else 'on'}_{p}")
    kb.button(text="⬅️ Назад", callback_data="to_my_rents")
    await call.message.edit_caption(
        caption=f"📱 `{p}`\nСтатус: {'🔥 РАБОТАЕТ' if res[0] else '💤 ПАУЗА'}",
        reply_markup=kb.adjust(2, 2, 1, 1).as_markup(), parse_mode="Markdown")


@dp.callback_query(F.data.startswith(("on_", "off_")))
async def toggle_r(call: types.CallbackQuery, state: FSMContext):
    p = call.data.split("_")[1]
    on = 1 if "on" in call.data else 0
    cur.execute('UPDATE accounts SET is_running=? WHERE phone=?', (on, p))
    db.commit()
    if on:
        asyncio.create_task(broadcast_loop(p))
    await manage_acc(call, state)


@dp.callback_query(F.data.startswith("set_"))
async def set_param_init(call: types.CallbackQuery, state: FSMContext):
    param, p = call.data.split("_")[1], call.data.split("_")[2]
    await state.update_data(target=p)
    st_map = {"text": States.edit_text, "photo": States.edit_photo,
              "chats": States.edit_chats, "int": States.edit_interval}
    msgs = {"text": "📝 Отправьте новый текст:",
            "photo": "🖼 Отправьте фото:",
            "chats": "👥 Ссылки через запятую:",
            "int": f"⏳ Интервал в сек (мин {MIN_INTERVAL}):"}
    await call.message.edit_caption(caption=msgs[param],
                                    reply_markup=back_kb(f"manage_{p}").as_markup())
    await state.set_state(st_map[param])


@dp.message(States.edit_text)
async def edit_t(m: Message, state: FSMContext):
    bad = contains_bad_words(m.text)
    if bad: return await m.answer(f"❌ Запрещённое слово: `{bad}`.", parse_mode="Markdown")
    d = await state.get_data()
    cur.execute('UPDATE accounts SET text=? WHERE phone=?', (m.text, d['target']))
    db.commit()
    await m.answer("✅ Текст обновлён.")
    await state.clear()

@dp.message(States.edit_photo)
async def edit_p(m: Message, state: FSMContext):
    d = await state.get_data()
    cur.execute('UPDATE accounts SET photo_id=? WHERE phone=?',
                (m.photo[-1].file_id if m.photo else None, d['target']))
    db.commit()
    await m.answer("✅ Фото обновлено.")
    await state.clear()

@dp.message(States.edit_chats)
async def edit_c(m: Message, state: FSMContext):
    d = await state.get_data()
    cur.execute('UPDATE accounts SET chats=? WHERE phone=?', (m.text, d['target']))
    db.commit()
    await m.answer("✅ Чаты обновлены.")
    await state.clear()

@dp.message(States.edit_interval)
async def edit_i(m: Message, state: FSMContext):
    if m.text.isdigit():
        val = int(m.text)
        if val < MIN_INTERVAL:
            return await m.answer(f"⚠️ Минимум {MIN_INTERVAL} сек.")
        d = await state.get_data()
        cur.execute('UPDATE accounts SET interval=? WHERE phone=?', (val, d['target']))
        db.commit()
        await m.answer(f"✅ Интервал: {val} сек.")
        await state.clear()
    else:
        await m.answer("⚠️ Введите целое число.")


# ─── БАЛАНС ПОЛЬЗОВАТЕЛЯ ─────────────────────────────────────────────────────
@dp.message(F.text == "💰 Баланс")
@dp.callback_query(F.data == "to_balance")
async def bal_menu(event: Message | types.CallbackQuery, state: FSMContext):
    await state.clear()
    bal = get_balance(event.from_user.id) or 0.0
    kb = InlineKeyboardBuilder()
    kb.button(text="🔌 CryptoPay (USDT)", callback_data="topup_crypto")
    kb.button(text="⬅️ Назад", callback_data="to_main")
    caption = f"💳 Ваш баланс: **${bal}**"
    if isinstance(event, Message):
        await event.answer_photo(photo=get_img('balance'), caption=caption,
                                 reply_markup=kb.adjust(1).as_markup(), parse_mode="Markdown")
    else:
        await event.message.edit_media(
            media=InputMediaPhoto(media=get_img('balance'), caption=caption, parse_mode="Markdown"),
            reply_markup=kb.adjust(1).as_markup())


@dp.callback_query(F.data == "topup_crypto")
async def topup_crypto_init(call: types.CallbackQuery, state: FSMContext):
    await call.message.edit_caption(
        caption="💵 Введите сумму в USD для пополнения через CryptoPay:",
        reply_markup=back_kb("to_balance").as_markup())
    await state.update_data(method="crypto")
    await state.set_state(States.top_up_amount)


@dp.message(States.top_up_amount)
async def create_pay(m: Message, state: FSMContext):
    try:
        val = float(m.text.replace(",", "."))
        if val <= 0: raise ValueError
    except:
        return await m.answer("⚠️ Введите корректное число.")
    if not crypto:
        return await m.answer("❌ CryptoPay недоступен.")
    inv = await crypto.create_invoice(asset='USDT', amount=val)
    kb = InlineKeyboardBuilder()
    kb.button(text="💳 Оплатить", url=inv.bot_invoice_url)
    kb.button(text="✅ Проверить", callback_data=f"chk_{inv.invoice_id}_{val}")
    await m.answer(f"Счёт на **${val}** создан:", reply_markup=kb.adjust(1).as_markup(), parse_mode="Markdown")
    await state.clear()


@dp.callback_query(F.data.startswith("chk_"))
async def check_crypto(call: types.CallbackQuery):
    _, iid, amt = call.data.split("_")
    inv = await crypto.get_invoices(invoice_ids=int(iid))
    if inv and inv.status == 'paid':
        add_payment(call.from_user.id, float(amt), "CryptoPay")
        await call.message.edit_text("✅ Оплата получена! Баланс пополнен.")
    else:
        await call.answer("❌ Не оплачено", show_alert=True)


# ─── РЕФ.СИСТЕМА (суб-клоны, доступна всем пользователям) ───────────────────
class SubcloneUserStates(StatesGroup):
    waiting_token = State()

class SubcloneWithdrawStates(StatesGroup):
    waiting_wallet = State()
    waiting_amount = State()

MIN_SUBCLONE_WITHDRAW = 1.0


@dp.message(F.text == "🤝 Реф.система")
@dp.callback_query(F.data == "clone_ref_main")
async def clone_ref_menu(event: Message | types.CallbackQuery, state: FSMContext):
    await state.clear()
    user_id = event.from_user.id
    rows = db_fetchall(
        'SELECT id, token, bot_username, is_running, earned, withdrawn FROM subclone_processes WHERE owner_id=?',
        (user_id,))

    kb = InlineKeyboardBuilder()

    if not rows:
        bot_info = None
        try:
            bot_info = await bot.get_me()
        except Exception:
            pass
        ref_hint = ""
        if bot_info:
            ref_hint = (f"\n\n📎 **Ваша реф-ссылка:**\n"
                        f"`t.me/{bot_info.username}?start=ref_u{user_id}`\n"
                        f"_Пригласите друга — при регистрации вам начислится бонус._")
        text = (
            "🤝 **Реф.система — Суб-клоны**\n\n"
            "Создайте собственного суб-клон бота!\n\n"
            "✅ Все функции этого бота\n"
            "💰 70% от прибыли — ваши\n"
            "📤 Вывод от $1\n\n"
            "Нужен API-токен от @BotFather." + ref_hint
        )
        kb.button(text="➕ Создать суб-клон", callback_data="user_subclone_create")
    else:
        lines = []
        bot_info = None
        try:
            bot_info = await bot.get_me()
        except Exception:
            pass
        ref_links = []
        for sid, token, uname, is_running, earned, withdrawn in rows:
            avail = round(earned - withdrawn, 2)
            dot = "🟢" if is_running else "🔴"
            label = f"@{uname}" if uname else f"ID {sid}"
            lines.append(f"{dot} {label} | Доступно: ${avail}")
            kb.button(text=f"⚙️ {label}", callback_data=f"user_subclone_manage_{sid}")
            if bot_info and uname:
                ref_links.append(f"🔗 {label}: `t.me/{bot_info.username}?start=ref_{sid}`")
        text = "🤝 **Реф.система — Мои суб-клоны**\n\n" + "\n".join(lines)
        if ref_links:
            text += "\n\n📎 **Реф-ссылки:**\n" + "\n".join(ref_links)
        kb.button(text="➕ Создать ещё", callback_data="user_subclone_create")

    kb.button(text="⬅️ Назад", callback_data="to_main")
    kb.adjust(1)

    if isinstance(event, Message):
        await event.answer(text, reply_markup=kb.as_markup(), parse_mode="Markdown")
    else:
        try:
            await event.message.edit_text(text, reply_markup=kb.as_markup(), parse_mode="Markdown")
        except Exception:
            await event.message.answer(text, reply_markup=kb.as_markup(), parse_mode="Markdown")


@dp.callback_query(F.data == "user_subclone_create")
async def user_subclone_create(call: types.CallbackQuery, state: FSMContext):
    await call.message.edit_text(
        "🤖 **Создание суб-клона**\n\n"
        "1. Перейди к @BotFather\n"
        "2. Создай нового бота — /newbot\n"
        "3. Скопируй API-токен и отправь сюда:\n\n"
        "_(формат: `1234567890:AAHxxxxxx`)_",
        reply_markup=back_kb("clone_ref_main").as_markup(),
        parse_mode="Markdown")
    await state.set_state(SubcloneUserStates.waiting_token)


@dp.message(SubcloneUserStates.waiting_token)
async def user_subclone_token_input(m: Message, state: FSMContext):
    token = m.text.strip()
    parts = token.split(":")
    if len(parts) != 2 or not parts[0].isdigit():
        return await m.answer("❌ Неверный формат. Пример: `1234567890:AAHxxxxx`",
                              parse_mode="Markdown")
    # Проверяем, не занят ли токен
    if db_fetchone('SELECT id FROM subclone_processes WHERE token=?', (token,)):
        return await m.answer("❌ Этот токен уже зарегистрирован.")

    await m.answer("⏳ Проверяю токен...")
    try:
        test_b = Bot(token=token)
        bi = await test_b.get_me()
        await test_b.session.close()
        bot_username = bi.username or ""
    except Exception as e:
        await state.clear()
        return await m.answer(f"❌ Не удалось подключиться: {e}")

    owner_id = m.from_user.id
    clone_script = os.path.abspath(__file__)
    cmd = [
        "python3", clone_script,
        token,
        str(owner_id),
        str(MAIN_ADMIN_ID),
        CRYPTO_PAY_TOKEN,
        str(API_ID),
        API_HASH,
        MAIN_DB_PATH,
    ]
    try:
        proc = _subprocess.Popen(
            cmd,
            stdout=open(f"subclone_{parts[0]}.log", "a"),
            stderr=_subprocess.STDOUT,
            start_new_session=True,
        )
        pid = proc.pid
    except Exception as e:
        await state.clear()
        return await m.answer(f"❌ Не удалось запустить суб-клон: {e}")

    started_at = time.strftime('%Y-%m-%d %H:%M:%S')
    cur.execute(
        'INSERT INTO subclone_processes (token, owner_id, crypto_token, pid, started_at, bot_username, is_running, earned, withdrawn)'
        ' VALUES (?,?,?,?,?,?,1,0,0)',
        (token, owner_id, CRYPTO_PAY_TOKEN, pid, started_at, bot_username))
    db.commit()

    label = f"@{bot_username}" if bot_username else f"PID {pid}"
    await m.answer(
        f"✅ **Суб-клон создан!**\n\n"
        f"🤖 {label}\n"
        f"📊 Статус: 🟢 Запущен\n\n"
        f"💰 Вы получаете **70%** от прибыли.\n"
        f"📤 Вывод от **$1** через Реф.система.",
        parse_mode="Markdown",
        reply_markup=back_kb("clone_ref_main").as_markup())
    await state.clear()

    # Уведомляем наблюдателей + гл.админа
    try:
        bot_me = await bot.get_me()
        parent_tag = f"@{bot_me.username}" if bot_me.username else BOT_SHORT_ID
    except Exception:
        parent_tag = BOT_SHORT_ID
    await notify_clone_admins(
        f"🤖 **Новый суб-клон создан пользователем**\n\n"
        f"👤 Владелец: `{owner_id}` @{m.from_user.username or '—'}\n"
        f"🔗 Родитель: {parent_tag}\n"
        f"🆕 Суб-клон: {label}\n"
        f"🕐 {started_at} | PID: `{pid}`",
        also_notify_main=True)


@dp.callback_query(F.data.startswith("user_subclone_manage_"))
async def user_subclone_manage(call: types.CallbackQuery, state: FSMContext):
    sid = int(call.data[len("user_subclone_manage_"):])
    res = db_fetchone(
        'SELECT id, token, bot_username, is_running, earned, withdrawn, pid FROM subclone_processes WHERE id=? AND owner_id=?',
        (sid, call.from_user.id))
    if not res:
        return await call.answer("❌ Не найдено.", show_alert=True)
    sid, token, uname, is_running, earned, withdrawn, pid = res
    avail = round(earned - withdrawn, 2)
    label = f"@{uname}" if uname else f"ID {sid}"
    status = "🟢 Работает" if is_running else "🔴 Остановлен"

    kb = InlineKeyboardBuilder()
    if is_running:
        kb.button(text="🛑 Остановить", callback_data=f"user_sc_stop_{sid}")
    else:
        kb.button(text="▶️ Запустить", callback_data=f"user_sc_start_{sid}")
    kb.button(text=f"💸 Вывод (${avail:.2f})", callback_data=f"user_sc_withdraw_{sid}")
    kb.button(text="🗑 Удалить", callback_data=f"user_sc_delete_{sid}")
    kb.button(text="⬅️ Назад", callback_data="clone_ref_main")
    kb.adjust(1)
    await call.message.edit_text(
        f"⚙️ **Управление суб-клоном**\n\n"
        f"🤖 {label}\n"
        f"📊 Статус: {status}\n\n"
        f"💰 Заработано (70%): **${round(earned, 2)}**\n"
        f"📤 Выведено: **${round(withdrawn, 2)}**\n"
        f"✅ Доступно: **${avail}**",
        reply_markup=kb.as_markup(), parse_mode="Markdown")


@dp.callback_query(F.data.startswith("user_sc_stop_"))
async def user_sc_stop(call: types.CallbackQuery, state: FSMContext):
    sid = int(call.data[len("user_sc_stop_"):])
    res = db_fetchone('SELECT pid FROM subclone_processes WHERE id=? AND owner_id=?', (sid, call.from_user.id))
    if not res:
        return await call.answer("❌ Не найдено.", show_alert=True)
    try:
        import signal
        os.kill(res[0], signal.SIGTERM)
    except Exception:
        pass
    cur.execute('UPDATE subclone_processes SET is_running=0 WHERE id=?', (sid,))
    db.commit()
    await call.answer("🛑 Остановлен.")
    call.data = f"user_subclone_manage_{sid}"
    await user_subclone_manage(call, state)


@dp.callback_query(F.data.startswith("user_sc_start_"))
async def user_sc_start(call: types.CallbackQuery, state: FSMContext):
    sid = int(call.data[len("user_sc_start_"):])
    res = db_fetchone(
        'SELECT token, owner_id FROM subclone_processes WHERE id=? AND owner_id=?',
        (sid, call.from_user.id))
    if not res:
        return await call.answer("❌ Не найдено.", show_alert=True)
    token, owner_id = res
    clone_script = os.path.abspath(__file__)
    cmd = ["python3", clone_script, token, str(owner_id), str(MAIN_ADMIN_ID),
           CRYPTO_PAY_TOKEN, str(API_ID), API_HASH, MAIN_DB_PATH]
    try:
        proc = _subprocess.Popen(cmd,
                                  stdout=open(f"subclone_{token.split(':')[0]}.log", "a"),
                                  stderr=_subprocess.STDOUT, start_new_session=True)
        cur.execute('UPDATE subclone_processes SET is_running=1, pid=? WHERE id=?', (proc.pid, sid))
        db.commit()
        await call.answer("✅ Запущен!")
    except Exception as e:
        await call.answer(f"❌ Ошибка: {e}", show_alert=True)
    call.data = f"user_subclone_manage_{sid}"
    await user_subclone_manage(call, state)


@dp.callback_query(F.data.startswith("user_sc_delete_"))
async def user_sc_delete(call: types.CallbackQuery, state: FSMContext):
    sid = int(call.data[len("user_sc_delete_"):])
    res = db_fetchone('SELECT bot_username, pid FROM subclone_processes WHERE id=? AND owner_id=?',
                      (sid, call.from_user.id))
    if not res:
        return await call.answer("❌ Не найдено.", show_alert=True)
    uname, pid = res
    # Завершаем процесс суб-клона
    if pid:
        try:
            import signal
            os.kill(pid, signal.SIGTERM)
        except Exception:
            pass
    cur.execute('DELETE FROM subclone_processes WHERE id=?', (sid,))
    db.commit()
    label = f"@{uname}" if uname else f"ID {sid}"
    delete_text = f"🗑 Суб-клон {label} удалён."
    back_markup = back_kb("clone_ref_main").as_markup()
    # edit_text может упасть если исходное сообщение — фото или медиа
    try:
        await call.message.edit_text(delete_text, reply_markup=back_markup)
    except Exception:
        try:
            await call.message.answer(delete_text, reply_markup=back_markup)
        except Exception:
            pass
    await call.answer(f"✅ Суб-клон {label} удалён.", show_alert=True)


@dp.callback_query(F.data.startswith("user_sc_withdraw_"))
async def user_sc_withdraw_init(call: types.CallbackQuery, state: FSMContext):
    sid = int(call.data[len("user_sc_withdraw_"):])
    res = db_fetchone(
        'SELECT bot_username, earned, withdrawn FROM subclone_processes WHERE id=? AND owner_id=?',
        (sid, call.from_user.id))
    if not res:
        return await call.answer("❌ Не найдено.", show_alert=True)
    uname, earned, withdrawn = res
    avail = round(earned - withdrawn, 2)
    if avail < MIN_SUBCLONE_WITHDRAW:
        return await call.answer(f"❌ Минимум ${MIN_SUBCLONE_WITHDRAW}, доступно ${avail}", show_alert=True)
    await state.update_data(sc_id=sid, sc_avail=avail)
    await call.message.edit_text(
        f"💸 **Вывод**\n✅ Доступно: **${avail}**\n\nВведите USDT TRC20 кошелёк:",
        reply_markup=back_kb(f"user_subclone_manage_{sid}").as_markup(),
        parse_mode="Markdown")
    await state.set_state(SubcloneWithdrawStates.waiting_wallet)


@dp.message(SubcloneWithdrawStates.waiting_wallet)
async def user_sc_withdraw_wallet(m: Message, state: FSMContext):
    wallet = m.text.strip()
    if len(wallet) < 10:
        return await m.answer("⚠️ Некорректный адрес кошелька.")
    await state.update_data(sc_wallet=wallet)
    d = await state.get_data()
    await m.answer(f"💵 Введите сумму (от ${MIN_SUBCLONE_WITHDRAW} до ${d['sc_avail']}):")
    await state.set_state(SubcloneWithdrawStates.waiting_amount)


@dp.message(SubcloneWithdrawStates.waiting_amount)
async def user_sc_withdraw_amount(m: Message, state: FSMContext):
    d = await state.get_data()
    try:
        amount = float(m.text.strip().replace(",", "."))
        if amount < MIN_SUBCLONE_WITHDRAW:
            return await m.answer(f"⚠️ Минимум ${MIN_SUBCLONE_WITHDRAW}")
        if amount > d['sc_avail']:
            return await m.answer(f"⚠️ Максимум ${d['sc_avail']}")
    except Exception:
        return await m.answer("⚠️ Введите число.")
    sid = d['sc_id']
    wallet = d['sc_wallet']
    user = m.from_user
    date_str = time.strftime('%Y-%m-%d %H:%M:%S')
    res = db_fetchone('SELECT token, bot_username FROM subclone_processes WHERE id=?', (sid,))
    clone_token = res[0] if res else "—"
    uname = res[1] if res else str(sid)

    cur.execute('UPDATE subclone_processes SET withdrawn = withdrawn + ? WHERE id=?', (amount, sid))
    db.commit()

    # Уведомляем гл.админа
    main_bot_token = os.environ.get("MAIN_BOT_TOKEN", "")
    if main_bot_token:
        try:
            nb = Bot(token=main_bot_token)
            await nb.send_message(
                MAIN_ADMIN_ID,
                f"💸 **ЗАЯВКА НА ВЫВОД (суб-клон)**\n\n"
                f"👤 ID: `{user.id}` @{user.username or '—'}\n"
                f"🤖 Суб-клон: @{uname}\n"
                f"🔑 Токен: `{clone_token[:20]}...`\n\n"
                f"💰 Сумма: **${amount}**\n"
                f"💳 Кошелёк: `{wallet}`\n"
                f"📅 {date_str}",
                parse_mode="Markdown")
            await nb.session.close()
        except Exception:
            pass
    await m.answer(
        f"✅ Заявка на **${amount}** отправлена!\n"
        f"💳 Кошелёк: `{wallet}`\n\nОжидайте обработки.",
        parse_mode="Markdown",
        reply_markup=back_kb("clone_ref_main").as_markup())
    await state.clear()


# ─── ПОМОЩЬ ──────────────────────────────────────────────────────────────────
@dp.message(F.text == "❓ Помощь")
async def help_menu(m: Message):
    await m.answer(
        "🤖 **Справка**\n\n"
        "📂 **Каталог** — аренда аккаунтов\n"
        "🔑 **Моя аренда** — управление рассылкой\n"
        "💰 **Баланс** — пополнение через CryptoPay\n\n"
        "📩 Ответить администратору: `/pma ваше сообщение`",
        parse_mode="Markdown")


@dp.message(F.text == "👨‍💻 Support")
async def support_info(m: Message):
    await m.answer("Обратитесь к администратору через /pma.")


# ─── ЗАПУСК ──────────────────────────────────────────────────────────────────
async def restart_subclones():
    """При перезапуске клона — автоматически поднимает суб-клоны с is_running=1."""
    rows = db_fetchall(
        'SELECT id, token, owner_id FROM subclone_processes WHERE is_running=1')
    for sid, token, owner_id in rows:
        clone_script = os.path.abspath(__file__)
        cmd = ["python3", clone_script, token, str(owner_id), str(MAIN_ADMIN_ID),
               CRYPTO_PAY_TOKEN, str(API_ID), API_HASH, MAIN_DB_PATH]
        try:
            proc = _subprocess.Popen(
                cmd,
                stdout=open(f"subclone_{token.split(':')[0]}.log", "a"),
                stderr=_subprocess.STDOUT,
                start_new_session=True,
            )
            cur.execute('UPDATE subclone_processes SET pid=? WHERE id=?', (proc.pid, sid))
            logging.info(f"Суб-клон ID {sid} перезапущен (PID {proc.pid})")
        except Exception as e:
            logging.error(f"Не удалось перезапустить суб-клон ID {sid}: {e}")
            cur.execute('UPDATE subclone_processes SET is_running=0 WHERE id=?', (sid,))
    db.commit()


async def restore_active_broadcasts():
    """При рестарте клон-бота — возобновляем рассылки, активные до остановки."""
    now = int(time.time())
    rows = db_fetchall(
        'SELECT phone FROM accounts WHERE is_running=1 AND expires > ?', (now,))
    restored = 0
    for (phone,) in rows:
        session_file = os.path.join(SESSION_DIR, f"{phone}.session")
        if os.path.exists(session_file):
            asyncio.create_task(broadcast_loop(phone))
            restored += 1
            logging.info(f"[restore] Рассылка для {phone} восстановлена.")
        else:
            cur.execute('UPDATE accounts SET is_running=0 WHERE phone=?', (phone,))
            logging.warning(f"[restore] Сессия {phone} не найдена — рассылка сброшена.")
    db.commit()
    if restored:
        logging.info(f"[restore] Восстановлено рассылок: {restored}")


async def main():
    os.makedirs(SESSION_DIR, exist_ok=True)
    await restart_subclones()
    await restore_active_broadcasts()
    asyncio.create_task(check_expirations())
    await bot.delete_webhook(drop_pending_updates=True)
    await dp.start_polling(bot)


if __name__ == "__main__":
    asyncio.run(main())
