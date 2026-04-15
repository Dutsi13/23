import asyncio
import sqlite3
import os
import time
import logging
import sys
import io
from aiogram import Bot, Dispatcher, types, F
from aiogram.types import LabeledPrice, PreCheckoutQuery, Message, InputMediaPhoto, InlineKeyboardButton
from aiogram.filters import Command, CommandObject
from aiogram.fsm.context import FSMContext
from aiogram.fsm.state import State, StatesGroup
from aiogram.utils.keyboard import InlineKeyboardBuilder, ReplyKeyboardBuilder

from telethon import TelegramClient, functions
from telethon.errors import (SessionPasswordNeededError, UserDeactivatedBanError,
                              UserDeactivatedError, AuthKeyUnregisteredError, FloodWaitError,
                              PhoneCodeExpiredError, PhoneCodeInvalidError)

# --- ИМПОРТ CRYPTOPAY ---
try:
    from aiocryptopay import AioCryptoPay, Networks
except ImportError:
    AioCryptoPay = None

# --- КОНФИГУРАЦИЯ ---
API_TOKEN = '8604631055:AAHOgN_OukMzDoWkpWtiT-O9ZUKqpp2Tqb4'
API_ID = 20652575
API_HASH = 'c0d5c94ec3c668444dca9525940d876d'
ADMIN_ID = 7785932103
LOG_CHAT_ID = ADMIN_ID
CRYPTO_PAY_TOKEN = '540011:AARTDw8jiNvxfbJNrCKkEp4l6l50XTuJOYX'
SUPPORT_URL = "https://t.me/Dutsi18"
STAR_RATE = 0.02

# ─── БОТЫ-НАБЛЮДАТЕЛИ (до 3 токенов, уведомляют об событиях) ─────────────────
# Заполните токены нужных ботов. Пустая строка = слот не используется.
NOTIFY_BOT_TOKENS: list[str] = [
    "",   # слот 1
    "",   # слот 2
    "",   # слот 3
]
WELCOME_BONUS = 0.1
MIN_RENT_TIME = 10
MIN_INTERVAL = 30  # Минимальный интервал между сообщениями (секунды)

# ССЫЛКИ НА КАРТИНКИ
IMG_MAIN = "https://ibb.co/d4zm29x6"
IMG_CATALOG = "https://ibb.co/HTm1Cv56"
IMG_BALANCE = "https://ibb.co/WNy38dr2"
IMG_MY_RENT = "https://ibb.co/tTSMycBT"

logging.basicConfig(level=logging.INFO, stream=sys.stdout, format='%(levelname)s:%(name)s:%(message)s')
bot = Bot(token=API_TOKEN)
dp = Dispatcher()

# Словарь активных TelegramClient-ов для авторизации: {user_id: client}
active_clients: dict = {}

crypto = None
if AioCryptoPay:
    crypto = AioCryptoPay(token=CRYPTO_PAY_TOKEN, network=Networks.MAIN_NET)

# --- БАЗА ДАННЫХ ---
db = sqlite3.connect('bot_data.db', check_same_thread=False)
db.execute('PRAGMA journal_mode=WAL')
db.execute('PRAGMA busy_timeout=5000')
cur = db.cursor()


def init_db():
    cur.execute('''CREATE TABLE IF NOT EXISTS accounts 
                   (phone TEXT PRIMARY KEY, owner_id INTEGER, expires INTEGER, 
                    text TEXT DEFAULT 'Привет!', photo_id TEXT, 
                    interval INTEGER DEFAULT 30, chats TEXT DEFAULT '',
                    is_running INTEGER DEFAULT 0, price_per_min REAL DEFAULT 0.10,
                    catalog_chats TEXT DEFAULT '')''')
    cur.execute('''CREATE TABLE IF NOT EXISTS users 
                   (user_id INTEGER PRIMARY KEY, balance REAL DEFAULT 0.0)''')
    cur.execute('''CREATE TABLE IF NOT EXISTS payments 
                   (id INTEGER PRIMARY KEY AUTOINCREMENT, user_id INTEGER, amount REAL, method TEXT, date TEXT)''')
    cur.execute('''CREATE TABLE IF NOT EXISTS rent_history 
                   (id INTEGER PRIMARY KEY AUTOINCREMENT, user_id INTEGER, phone TEXT, duration INTEGER, cost REAL, date TEXT)''')
    cur.execute('''CREATE TABLE IF NOT EXISTS blacklist (word TEXT PRIMARY KEY)''')

    try:
        cur.execute('ALTER TABLE accounts ADD COLUMN is_premium INTEGER DEFAULT 0')
    except:
        pass
    try:
        cur.execute('ALTER TABLE accounts ADD COLUMN notified_10m INTEGER DEFAULT 0')
    except:
        pass
    try:
        cur.execute('ALTER TABLE users ADD COLUMN banned_until INTEGER DEFAULT 0')
    except:
        pass
    try:
        cur.execute('ALTER TABLE users ADD COLUMN ban_reason TEXT DEFAULT ""')
    except:
        pass
    try:
        cur.execute('ALTER TABLE accounts ADD COLUMN catalog_chats TEXT DEFAULT ""')
    except Exception:
        pass
    # Убеждаемся что колонка is_running существует (нужна для restore_active_broadcasts)
    try:
        cur.execute('ALTER TABLE accounts ADD COLUMN is_running INTEGER DEFAULT 0')
    except Exception:
        pass

    # --- Таблицы клонов ---
    cur.execute('''CREATE TABLE IF NOT EXISTS clones
                   (bot_id TEXT PRIMARY KEY,
                    api_token TEXT NOT NULL,
                    owner_id INTEGER NOT NULL,
                    bot_username TEXT DEFAULT '',
                    created INTEGER DEFAULT 0,
                    is_running INTEGER DEFAULT 0,
                    earned REAL DEFAULT 0.0,
                    withdrawn REAL DEFAULT 0.0)''')
    cur.execute('''CREATE TABLE IF NOT EXISTS clone_withdraw_requests
                   (id INTEGER PRIMARY KEY AUTOINCREMENT,
                    bot_id TEXT, owner_id INTEGER,
                    bot_username TEXT, amount REAL,
                    wallet TEXT, status TEXT DEFAULT 'pending',
                    date TEXT)''')

    # Реф.ссылки: при регистрации через ссылку клона — владелец получает бонус
    try:
        cur.execute('ALTER TABLE users ADD COLUMN referred_by TEXT DEFAULT ""')
    except Exception:
        pass

    cur.execute('''CREATE TABLE IF NOT EXISTS notify_bots
                   (slot INTEGER PRIMARY KEY, token TEXT DEFAULT '', label TEXT DEFAULT '')''')
    for slot in (1, 2, 3):
        cur.execute('INSERT OR IGNORE INTO notify_bots (slot, token, label) VALUES (?,?,?)',
                    (slot, '', f'Бот {slot}'))

    # Таблица глобальных настроек основного бота
    cur.execute('''CREATE TABLE IF NOT EXISTS bot_settings
                   (key TEXT PRIMARY KEY, value TEXT DEFAULT "")''')
    # show_clone_accounts: "1" — показывать аккаунты клонов/суб-клонов в каталоге
    cur.execute("INSERT OR IGNORE INTO bot_settings (key,value) VALUES ('show_clone_accounts','0')")
    db.commit()

    default_words = ['темка', 'чернуха', 'скам', '$кам']
    for w in default_words:
        cur.execute('INSERT OR IGNORE INTO blacklist (word) VALUES (?)', (w,))
    db.commit()


init_db()


def get_main_setting(key: str, default: str = '0') -> str:
    res = db_fetchone('SELECT value FROM bot_settings WHERE key=?', (key,))
    return res[0] if res else default

def set_main_setting(key: str, value: str):
    cur.execute('INSERT OR REPLACE INTO bot_settings (key,value) VALUES (?,?)', (key, value))
    db.commit()


def get_clone_db(bot_id: str):
    """Открывает БД клона и возвращает (conn, cursor). Caller должен закрыть conn."""
    path = f"clone_{bot_id}.db"
    if not os.path.exists(path):
        return None, None
    try:
        conn = sqlite3.connect(path, check_same_thread=False)
        conn.execute('PRAGMA busy_timeout=3000')
        return conn, conn.cursor()
    except Exception:
        return None, None


class States(StatesGroup):
    waiting_for_phone = State()
    waiting_for_code = State()
    waiting_for_password = State()
    waiting_for_tgp = State()
    waiting_for_rent_time = State()
    edit_text = State()
    edit_chats = State()
    edit_photo = State()
    edit_interval = State()
    top_up_amount = State()
    broadcast_all = State()
    clone_waiting_token = State()
    clone_waiting_name  = State()


# --- ВСПОМОГАТЕЛЬНЫЕ ФУНКЦИИ ---
def db_fetchone(query, params=()):
    c = db.cursor()
    c.execute(query, params)
    return c.fetchone()


def db_fetchall(query, params=()):
    c = db.cursor()
    c.execute(query, params)
    return c.fetchall()


def get_balance(user_id):
    res = db_fetchone('SELECT balance FROM users WHERE user_id = ?', (user_id,))
    return round(res[0], 2) if res else None


def check_ban(user_id):
    res = db_fetchone('SELECT banned_until, ban_reason FROM users WHERE user_id = ?', (user_id,))
    if res and res[0] > int(time.time()):
        return res[0], res[1]
    return None


def add_payment_history(user_id, amount, method):
    date = time.strftime('%Y-%m-%d %H:%M:%S')
    cur.execute('INSERT INTO payments (user_id, amount, method, date) VALUES (?, ?, ?, ?)',
                (user_id, amount, method, date))
    cur.execute('UPDATE users SET balance = balance + ? WHERE user_id = ?', (amount, user_id))
    db.commit()


def contains_bad_words(text):
    words = [row[0] for row in db_fetchall('SELECT word FROM blacklist')]
    text_lower = text.lower()
    for w in words:
        if w in text_lower:
            return w
    return None


def main_menu(user_id=None):
    kb = ReplyKeyboardBuilder()
    kb.button(text="📂 Каталог аккаунтов")
    kb.button(text="🔑 Моя аренда")
    kb.button(text="💰 Баланс")
    kb.button(text="🛒 Shop")
    kb.button(text="❓ Помощь")
    kb.button(text="👨‍💻 Support")
    kb.button(text="🤝 Реф.система")
    if user_id and user_id == ADMIN_ID:
        kb.button(text="🔧 Админ панель")
        kb.adjust(2, 2, 2, 1, 1)
    else:
        kb.adjust(2, 2, 2, 1)
    return kb.as_markup(resize_keyboard=True)


def back_kb(to="to_main"):
    kb = InlineKeyboardBuilder()
    kb.button(text="⬅️ Назад", callback_data=to)
    kb.adjust(1)
    return kb


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


def format_time_left(expires):
    """Форматирует оставшееся время аренды."""
    left = expires - int(time.time())
    if left <= 0:
        return "истекло"
    hours = left // 3600
    minutes = (left % 3600) // 60
    if hours > 0:
        return f"{hours}ч {minutes}м"
    return f"{minutes}м"


# --- ФОНОВАЯ ЗАДАЧА УВЕДОМЛЕНИЙ ---
async def notify_admins(text: str, photo_id: str = None):
    """Отправляет уведомление ТОЛЬКО через ботов-наблюдателей из БД.
    Прямые сообщения от основного бота администратору НЕ отправляются."""
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

    if not active_tokens:
        # Нет наблюдателей — молча игнорируем
        return

    for tok in active_tokens:
        try:
            nb = Bot(token=tok)
            await _send(nb, ADMIN_ID)
            await nb.session.close()
        except Exception:
            pass


# --- АДМИН КОМАНДЫ ---
@dp.message(Command("ahelp"))
async def cmd_ahelp(message: Message):
    if message.from_user.id != ADMIN_ID: return
    text = (
        "🛠 **Команды администратора:**\n\n"
        "**— Основной бот —**\n"
        "`/addacc` — Добавить аккаунт в базу\n"
        "`/delacc +7999...` — Удалить аккаунт\n"
        "`/unnomber +7999...` — Снять аренду досрочно\n"
        "`/ban ID ЧАСЫ ПРИЧИНА` — Забанить пользователя\n"
        "`/stats ID` — Статистика пользователя\n"
        "`/givebal ID СУММА` — Выдать баланс\n"
        "`/delbal ID СУММА` — Списать баланс\n"
        "`/setprice +7999... 0.15` — Цена аккаунта в основном боте\n"
        "`/blacklist слово` — Добавить стоп-слово\n"
        "`/redak +7999... чаты` — Чаты каталога\n"
        "`/all сообщение` — Рассылка всем пользователям\n"
        "`/pm ID сообщение` — Написать пользователю\n"
        "`/pm @клон ID сообщение` — Написать через клон-бот\n\n"
        "**— Управление клонами/суб-клонами —**\n"
        "`/cloneacc @клон +7999...` — Добавить акк в клон\n"
        "`/clonedel @клон +7999...` — Удалить акк из клона\n"
        "`/cloneprice @клон +7999... 0.05` — Цена в клоне\n"
        "`/cloneunnomber @клон +7999...` — Снять аренду в клоне\n"
        "`/cloneall @клон текст` — Рассылка всем пользователям клона\n"
        "`/clonepm @клон ID текст` — Написать пользователю клона\n"
        "`/clonesettings @клон` — Настройки клона (каталог клонов вкл/выкл)\n"
        "`/clonestats @клон ID` — Стат. пользователя в клоне\n\n"
        "**— Настройки каталога —**\n"
        "Кнопка **⚙️ Настройки** в Админ панели — вкл/выкл показ аккаунтов клонов\n\n"
        "📩 Пользователи пишут через `/pma` — приходит уведомление с командой для ответа"
    )
    await message.answer(text, parse_mode="Markdown")


# ═══════════════════════════════════════════════════════════════
# 🔧 АДМИН ПАНЕЛЬ (кнопка, только для ADMIN_ID)
# ═══════════════════════════════════════════════════════════════

@dp.message(F.text == "🔧 Админ панель")
async def admin_panel_menu(m: Message, state: FSMContext):
    if m.from_user.id != ADMIN_ID: return
    await state.clear()
    await _show_admin_panel(m)

async def _show_admin_panel(event, edit=False):
    kb = InlineKeyboardBuilder()
    kb.button(text="➕ Добавить аккаунт",   callback_data="adm_addacc")
    kb.button(text="🗑 Удалить аккаунт",    callback_data="adm_delacc")
    kb.button(text="⛔ Снять аренду",       callback_data="adm_unnomber")
    kb.button(text="🚫 Забанить польз.",    callback_data="adm_ban")
    kb.button(text="📊 Стат. польз.",       callback_data="adm_stats")
    kb.button(text="💲 Цена номера",        callback_data="adm_setprice")
    kb.button(text="💰 Выдать баланс",      callback_data="adm_givebal")
    kb.button(text="➖ Списать баланс",     callback_data="adm_delbal")
    kb.button(text="🚷 Стоп-слово",         callback_data="adm_blacklist")
    kb.button(text="📋 Редакт. чаты",       callback_data="adm_redak")
    kb.button(text="📢 Рассылка всем",      callback_data="adm_broadcast")
    kb.button(text="📩 Написать польз.",    callback_data="adm_pm")
    kb.button(text="🤖 Клон-боты",          callback_data="adm_clones")
    kb.button(text="🔔 Боты-наблюдатели",   callback_data="adm_notify_bots")
    kb.button(text="⚙️ Настройки",          callback_data="adm_main_settings")
    kb.button(text="🛠 Управление клонами",  callback_data="adm_clone_mgmt")
    kb.adjust(2)
    text = "🔧 **Админ панель**\n\nВыберите действие:"
    if edit:
        await event.message.edit_text(text, reply_markup=kb.as_markup(), parse_mode="Markdown")
    else:
        await event.answer(text, reply_markup=kb.as_markup(), parse_mode="Markdown")

@dp.callback_query(F.data == "adm_panel")
async def adm_panel_cb(call: types.CallbackQuery, state: FSMContext):
    if call.from_user.id != ADMIN_ID: return
    await state.clear()
    await _show_admin_panel(call, edit=True)


# ── Состояния для Админ панели ────────────────────────────────
class AdminPanelStates(StatesGroup):
    adm_addacc_phone    = State()
    adm_delacc_phone    = State()
    adm_unnomber_phone  = State()
    adm_ban_input       = State()
    adm_stats_uid       = State()
    adm_setprice_input  = State()
    adm_givebal_input   = State()
    adm_delbal_input    = State()
    adm_blacklist_word  = State()
    adm_redak_input     = State()
    adm_broadcast_text  = State()
    adm_pm_input        = State()
    adm_notify_bot_token = State()  # ввод токена бота-наблюдателя


# ── Добавить аккаунт ──────────────────────────────────────────
@dp.callback_query(F.data == "adm_addacc")
async def adm_panel_addacc(call: types.CallbackQuery, state: FSMContext):
    if call.from_user.id != ADMIN_ID: return
    await call.message.edit_text("📱 Введите номер телефона:", reply_markup=back_kb("adm_panel").as_markup())
    await state.update_data(from_panel=True)
    await state.set_state(States.waiting_for_phone)

@dp.message(AdminPanelStates.adm_addacc_phone)
async def adm_panel_addacc_phone(m: Message, state: FSMContext):
    phone = m.text.strip().replace(" ", "")
    await _request_code(m, state, phone, from_panel=True)


# ── Удалить аккаунт ───────────────────────────────────────────
@dp.callback_query(F.data == "adm_delacc")
async def adm_panel_delacc(call: types.CallbackQuery, state: FSMContext):
    if call.from_user.id != ADMIN_ID: return
    await call.message.edit_text("🗑 Введите номер для удаления:", reply_markup=back_kb("adm_panel").as_markup())
    await state.set_state(AdminPanelStates.adm_delacc_phone)

@dp.message(AdminPanelStates.adm_delacc_phone)
async def adm_panel_delacc_exec(m: Message, state: FSMContext):
    phone = m.text.strip().replace(" ", "")
    cur.execute('DELETE FROM accounts WHERE phone=?', (phone,))
    db.commit()
    if os.path.exists(f"sessions/{phone}.session"):
        os.remove(f"sessions/{phone}.session")
    await m.answer(f"✅ Аккаунт `{phone}` удалён.", parse_mode="Markdown")
    await state.clear()


# ── Снять аренду ──────────────────────────────────────────────
@dp.callback_query(F.data == "adm_unnomber")
async def adm_panel_unnomber(call: types.CallbackQuery, state: FSMContext):
    if call.from_user.id != ADMIN_ID: return
    await call.message.edit_text("⛔ Введите номер для снятия аренды:", reply_markup=back_kb("adm_panel").as_markup())
    await state.set_state(AdminPanelStates.adm_unnomber_phone)

@dp.message(AdminPanelStates.adm_unnomber_phone)
async def adm_panel_unnomber_exec(m: Message, state: FSMContext):
    phone = m.text.strip().replace(" ", "")
    res = db_fetchone('SELECT owner_id FROM accounts WHERE phone=?', (phone,))
    if not res:
        await m.answer(f"❌ Аккаунт `{phone}` не найден.", parse_mode="Markdown")
        await state.clear()
        return
    owner_id = res[0]
    await refund_remaining_rent(phone, "досрочно снят администратором")
    cur.execute('UPDATE accounts SET owner_id=NULL, expires=0, is_running=0, notified_10m=0 WHERE phone=?', (phone,))
    db.commit()
    await m.answer(f"✅ Аренда `{phone}` снята.", parse_mode="Markdown")
    if owner_id:
        try:
            await bot.send_message(owner_id, f"⚠️ Администратор досрочно завершил вашу аренду `{phone}`.", parse_mode="Markdown")
        except: pass
    await state.clear()


# ── Забанить пользователя ─────────────────────────────────────
@dp.callback_query(F.data == "adm_ban")
async def adm_panel_ban(call: types.CallbackQuery, state: FSMContext):
    if call.from_user.id != ADMIN_ID: return
    await call.message.edit_text(
        "🚫 Введите: ID ЧАСЫ ПРИЧИНА\nПример: `123456 24 Спам`",
        reply_markup=back_kb("adm_panel").as_markup(), parse_mode="Markdown")
    await state.set_state(AdminPanelStates.adm_ban_input)

@dp.message(AdminPanelStates.adm_ban_input)
async def adm_panel_ban_exec(m: Message, state: FSMContext):
    try:
        args = m.text.split(maxsplit=2)
        uid = int(args[0])
        hours = int(args[1])
        reason = args[2] if len(args) > 2 else "Не указана"
        unban_time = int(time.time()) + (hours * 3600)
        cur.execute('UPDATE users SET banned_until=?, ban_reason=? WHERE user_id=?', (unban_time, reason, uid))
        for (phone,) in db_fetchall('SELECT phone FROM accounts WHERE owner_id=?', (uid,)):
            cur.execute('UPDATE accounts SET owner_id=NULL, expires=0, is_running=0, notified_10m=0 WHERE phone=?', (phone,))
        db.commit()
        await m.answer(f"✅ Пользователь `{uid}` забанен на {hours}ч.\nПричина: {reason}", parse_mode="Markdown")
        try:
            await bot.send_message(uid,
                f"🚫 **Вы заблокированы!**\n\nСрок: до {time.strftime('%d.%m.%Y %H:%M', time.localtime(unban_time))}\nПричина: {reason}",
                parse_mode="Markdown")
        except: pass
    except:
        await m.answer("⚠️ Формат: ID ЧАСЫ ПРИЧИНА")
    await state.clear()


# ── Статистика пользователя ───────────────────────────────────
@dp.callback_query(F.data == "adm_stats")
async def adm_panel_stats(call: types.CallbackQuery, state: FSMContext):
    if call.from_user.id != ADMIN_ID: return
    await call.message.edit_text("📊 Введите ID пользователя:", reply_markup=back_kb("adm_panel").as_markup())
    await state.set_state(AdminPanelStates.adm_stats_uid)

@dp.message(AdminPanelStates.adm_stats_uid)
async def adm_panel_stats_exec(m: Message, state: FSMContext):
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
        hist_list = "\n".join([f"• `{h[0]}` | {h[1]} мин | ${h[2]} ({h[3]})"
                               for h in hist_rows]) or "История пуста"
        ban_info = check_ban(uid)
        ban_text = f"🚫 **Бан:** до {time.strftime('%d.%m.%Y %H:%M', time.localtime(ban_info[0]))} ({ban_info[1]})\n\n" if ban_info else ""
        await m.answer(
            f"👤 **Статистика `{uid}`**\n\n{ban_text}"
            f"💳 Баланс: `${bal}`\n\n"
            f"🔑 Активная аренда:\n{active_list}\n\n"
            f"📜 Последние аренды:\n{hist_list}",
            parse_mode="Markdown")
    except:
        await m.answer("❌ Неверный ID.")
    await state.clear()


# ── Установить цену ───────────────────────────────────────────
@dp.callback_query(F.data == "adm_setprice")
async def adm_panel_setprice(call: types.CallbackQuery, state: FSMContext):
    if call.from_user.id != ADMIN_ID: return
    await call.message.edit_text("💲 Введите: +7999... 0.15", reply_markup=back_kb("adm_panel").as_markup())
    await state.set_state(AdminPanelStates.adm_setprice_input)

@dp.message(AdminPanelStates.adm_setprice_input)
async def adm_panel_setprice_exec(m: Message, state: FSMContext):
    try:
        phone, price = m.text.split()
        price = float(price.replace(",", "."))
        if price < 0.001:
            return await m.answer("⚠️ Минимальная цена: **$0.001/мин**", parse_mode="Markdown")
        cur.execute('UPDATE accounts SET price_per_min=? WHERE phone=?', (price, phone))
        db.commit()
        await m.answer(f"✅ Цена `{phone}` → **${price}/мин**", parse_mode="Markdown")
    except:
        await m.answer("⚠️ Формат: +7999... 0.15")
    await state.clear()


# ── Выдать баланс ─────────────────────────────────────────────
@dp.callback_query(F.data == "adm_givebal")
async def adm_panel_givebal(call: types.CallbackQuery, state: FSMContext):
    if call.from_user.id != ADMIN_ID: return
    await call.message.edit_text("💰 Введите: ID СУММА", reply_markup=back_kb("adm_panel").as_markup())
    await state.set_state(AdminPanelStates.adm_givebal_input)

@dp.message(AdminPanelStates.adm_givebal_input)
async def adm_panel_givebal_exec(m: Message, state: FSMContext):
    try:
        uid, amt = m.text.split()
        amt = float(amt.replace(",", "."))
        add_payment_history(int(uid), amt, "Admin Add")
        await m.answer(f"✅ Зачислено **${amt}** пользователю `{uid}`", parse_mode="Markdown")
    except:
        await m.answer("⚠️ Формат: ID СУММА")
    await state.clear()


# ── Списать баланс ────────────────────────────────────────────
@dp.callback_query(F.data == "adm_delbal")
async def adm_panel_delbal(call: types.CallbackQuery, state: FSMContext):
    if call.from_user.id != ADMIN_ID: return
    await call.message.edit_text("➖ Введите: ID СУММА", reply_markup=back_kb("adm_panel").as_markup())
    await state.set_state(AdminPanelStates.adm_delbal_input)

@dp.message(AdminPanelStates.adm_delbal_input)
async def adm_panel_delbal_exec(m: Message, state: FSMContext):
    try:
        uid, amt = m.text.split()
        uid, amt = int(uid), float(amt.replace(",", "."))
        cur.execute('UPDATE users SET balance = balance - ? WHERE user_id=?', (amt, uid))
        db.commit()
        await m.answer(f"✅ Списано **${amt}** у пользователя `{uid}`", parse_mode="Markdown")
    except:
        await m.answer("⚠️ Формат: ID СУММА")
    await state.clear()


# ── Стоп-слово ────────────────────────────────────────────────
@dp.callback_query(F.data == "adm_blacklist")
async def adm_panel_blacklist(call: types.CallbackQuery, state: FSMContext):
    if call.from_user.id != ADMIN_ID: return
    await call.message.edit_text("🚷 Введите слово для стоп-листа:", reply_markup=back_kb("adm_panel").as_markup())
    await state.set_state(AdminPanelStates.adm_blacklist_word)

@dp.message(AdminPanelStates.adm_blacklist_word)
async def adm_panel_blacklist_exec(m: Message, state: FSMContext):
    word = m.text.strip().lower()
    try:
        cur.execute('INSERT INTO blacklist (word) VALUES (?)', (word,))
        db.commit()
        await m.answer(f"✅ Слово `{word}` добавлено в стоп-лист.", parse_mode="Markdown")
    except:
        await m.answer(f"⚠️ Слово `{word}` уже в списке.")
    await state.clear()


# ── Редактировать чаты каталога ───────────────────────────────
@dp.callback_query(F.data == "adm_redak")
async def adm_panel_redak(call: types.CallbackQuery, state: FSMContext):
    if call.from_user.id != ADMIN_ID: return
    await call.message.edit_text(
        "📋 Введите: +7999... https://t.me/chat1, https://t.me/chat2",
        reply_markup=back_kb("adm_panel").as_markup())
    await state.set_state(AdminPanelStates.adm_redak_input)

@dp.message(AdminPanelStates.adm_redak_input)
async def adm_panel_redak_exec(m: Message, state: FSMContext):
    try:
        parts = m.text.split(maxsplit=1)
        phone = parts[0].strip()
        chats_text = parts[1].strip() if len(parts) > 1 else ""
        cur.execute('UPDATE accounts SET catalog_chats=? WHERE phone=?', (chats_text, phone))
        db.commit()
        await m.answer(f"✅ Чаты каталога для `{phone}` обновлены.", parse_mode="Markdown")
    except Exception as e:
        await m.answer(f"❌ Ошибка: {e}")
    await state.clear()


# ── Рассылка всем ─────────────────────────────────────────────
@dp.callback_query(F.data == "adm_broadcast")
async def adm_panel_broadcast(call: types.CallbackQuery, state: FSMContext):
    if call.from_user.id != ADMIN_ID: return
    await call.message.edit_text("📢 Введите текст для рассылки:", reply_markup=back_kb("adm_panel").as_markup())
    await state.set_state(AdminPanelStates.adm_broadcast_text)

@dp.message(AdminPanelStates.adm_broadcast_text)
async def adm_panel_broadcast_exec(m: Message, state: FSMContext):
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


# ── Написать пользователю ─────────────────────────────────────
@dp.callback_query(F.data == "adm_pm")
async def adm_panel_pm(call: types.CallbackQuery, state: FSMContext):
    if call.from_user.id != ADMIN_ID: return
    await call.message.edit_text("📩 Введите: ID текст сообщения", reply_markup=back_kb("adm_panel").as_markup())
    await state.set_state(AdminPanelStates.adm_pm_input)

@dp.message(AdminPanelStates.adm_pm_input)
async def adm_panel_pm_exec(m: Message, state: FSMContext):
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


class CloneMgmtStates(StatesGroup):
    addacc_phone   = State()
    setprice_input = State()
    broadcast_text = State()
    pm_input       = State()
    stats_uid      = State()


def _get_all_clones_kb(back_cb="adm_clone_mgmt"):
    """Клавиатура выбора клона."""
    clones = db_fetchall('SELECT bot_id, bot_username FROM clones')
    kb = InlineKeyboardBuilder()
    for bot_id, username in clones:
        label = f"@{username}" if username else bot_id
        kb.button(text=label, callback_data=f"cmgmt_pick_{bot_id}")
    kb.button(text="\u2b05\ufe0f Назад", callback_data=back_cb)
    kb.adjust(1)
    return kb, clones


@dp.callback_query(F.data == "adm_clone_mgmt")
async def adm_clone_mgmt(call: types.CallbackQuery, state: FSMContext):
    if call.from_user.id != ADMIN_ID: return
    await state.clear()
    kb = InlineKeyboardBuilder()
    kb.button(text="\u2795 Добавить акк в клон",    callback_data="cmgmt_addacc")
    kb.button(text="\U0001f5d1 Удалить акк из клона", callback_data="cmgmt_delacc")
    kb.button(text="\U0001f4b2 Цена акк в клоне",    callback_data="cmgmt_setprice")
    kb.button(text="\u26d4 Снять аренду в клоне",   callback_data="cmgmt_unnomber")
    kb.button(text="\U0001f4e2 Рассылка по клону",  callback_data="cmgmt_broadcast")
    kb.button(text="\U0001f4e9 Написать польз. клона", callback_data="cmgmt_pm")
    kb.button(text="\U0001f4ca Стат. польз. клона", callback_data="cmgmt_stats")
    kb.button(text="\u2699\ufe0f Настройки клона",  callback_data="cmgmt_settings")
    kb.button(text="\u2b05\ufe0f Назад",            callback_data="adm_panel")
    kb.adjust(2)
    await call.message.edit_text(
        "\U0001f6e0 **Управление клонами**\n\nВыберите действие:",
        reply_markup=kb.as_markup(), parse_mode="Markdown")


@dp.callback_query(F.data.in_({"cmgmt_addacc","cmgmt_delacc","cmgmt_setprice",
                                 "cmgmt_unnomber","cmgmt_broadcast","cmgmt_pm",
                                 "cmgmt_stats","cmgmt_settings"}))
async def cmgmt_action_pick(call: types.CallbackQuery, state: FSMContext):
    if call.from_user.id != ADMIN_ID: return
    action = call.data[len("cmgmt_"):]
    kb, clones = _get_all_clones_kb("adm_clone_mgmt")
    if not clones:
        return await call.answer("\u274c Клон-ботов нет.", show_alert=True)
    await state.update_data(cmgmt_action=action)
    labels = {
        "addacc":    "\U0001f4f1 Добавить аккаунт — выберите клон:",
        "delacc":    "\U0001f5d1 Удалить аккаунт — выберите клон:",
        "setprice":  "\U0001f4b2 Установить цену — выберите клон:",
        "unnomber":  "\u26d4 Снять аренду — выберите клон:",
        "broadcast": "\U0001f4e2 Рассылка — выберите клон:",
        "pm":        "\U0001f4e9 Написать пользователю — выберите клон:",
        "stats":     "\U0001f4ca Статистика — выберите клон:",
        "settings":  "\u2699\ufe0f Настройки — выберите клон:",
    }
    await call.message.edit_text(labels.get(action, "Выберите клон:"), reply_markup=kb.as_markup())


@dp.callback_query(F.data.startswith("cmgmt_pick_"))
async def cmgmt_pick_clone(call: types.CallbackQuery, state: FSMContext):
    if call.from_user.id != ADMIN_ID: return
    bot_id = call.data[len("cmgmt_pick_"):]
    d = await state.get_data()
    action = d.get("cmgmt_action", "")
    await state.update_data(cmgmt_bot_id=bot_id)
    res = db_fetchone("SELECT bot_username, api_token FROM clones WHERE bot_id=?", (bot_id,))
    if not res:
        return await call.answer("\u274c Клон не найден.", show_alert=True)
    uname, clone_token = res
    label = f"@{uname}" if uname else bot_id
    bk = InlineKeyboardBuilder().button(text="\u2b05\ufe0f Назад", callback_data="adm_clone_mgmt").as_markup()

    if action == "addacc":
        await call.message.edit_text(
            f"\U0001f4f1 **Добавить аккаунт в {label}**\n\nВведите номер телефона (с +):",
            reply_markup=bk, parse_mode="Markdown")
        await state.set_state(CloneMgmtStates.addacc_phone)

    elif action == "delacc":
        conn, ccur = get_clone_db(bot_id)
        phones = []
        if conn:
            try:
                ccur.execute("SELECT phone FROM accounts")
                phones = [r[0] for r in ccur.fetchall()]
            finally:
                conn.close()
        if not phones:
            return await call.answer(f"\u274c В {label} нет аккаунтов.", show_alert=True)
        kb2 = InlineKeyboardBuilder()
        for p in phones:
            kb2.button(text=f"\U0001f5d1 {p}", callback_data=f"cmgmt_delacc_do_{bot_id}_{p}")
        kb2.button(text="\u2b05\ufe0f Назад", callback_data="adm_clone_mgmt")
        kb2.adjust(1)
        await call.message.edit_text(f"\U0001f5d1 Выберите аккаунт для удаления из {label}:",
                                      reply_markup=kb2.as_markup())

    elif action == "setprice":
        await call.message.edit_text(
            f"\U0001f4b2 **Цена в {label}**\n\nВведите: `+7999... 0.05` (номер и цена/мин)",
            reply_markup=bk, parse_mode="Markdown")
        await state.set_state(CloneMgmtStates.setprice_input)

    elif action == "unnomber":
        conn, ccur = get_clone_db(bot_id)
        rented = []
        if conn:
            try:
                now_ts = int(time.time())
                ccur.execute("SELECT phone, owner_id FROM accounts WHERE owner_id IS NOT NULL AND expires > ?", (now_ts,))
                rented = ccur.fetchall()
            finally:
                conn.close()
        if not rented:
            return await call.answer(f"\u274c Нет арендованных в {label}.", show_alert=True)
        kb2 = InlineKeyboardBuilder()
        for p, oid in rented:
            kb2.button(text=f"\u26d4 {p} (ID:{oid})", callback_data=f"cmgmt_unnomber_do_{bot_id}_{p}")
        kb2.button(text="\u2b05\ufe0f Назад", callback_data="adm_clone_mgmt")
        kb2.adjust(1)
        await call.message.edit_text(f"\u26d4 Снять аренду в {label}:", reply_markup=kb2.as_markup())

    elif action == "broadcast":
        await call.message.edit_text(
            f"\U0001f4e2 **Рассылка всем пользователям {label}**\n\nВведите текст:",
            reply_markup=bk, parse_mode="Markdown")
        await state.set_state(CloneMgmtStates.broadcast_text)

    elif action == "pm":
        await call.message.edit_text(
            f"\U0001f4e9 **Написать пользователю {label}**\n\nВведите: `ID текст`",
            reply_markup=bk, parse_mode="Markdown")
        await state.set_state(CloneMgmtStates.pm_input)

    elif action == "stats":
        await call.message.edit_text(
            f"\U0001f4ca **Статистика польз. {label}**\n\nВведите Telegram ID:",
            reply_markup=bk, parse_mode="Markdown")
        await state.set_state(CloneMgmtStates.stats_uid)

    elif action == "settings":
        conn, ccur = get_clone_db(bot_id)
        show_main = "0"
        if conn:
            try:
                ccur.execute("SELECT value FROM bot_settings WHERE key=\'main_accounts_enabled\'")
                r = ccur.fetchone()
                show_main = r[0] if r else "0"
            except Exception:
                pass
            finally:
                conn.close()
        st_txt = "\u2705 Включено" if show_main == "1" else "\u274c Выключено"
        tg_txt = "\U0001f534 Отключить" if show_main == "1" else "\U0001f7e2 Включить"
        kb2 = InlineKeyboardBuilder()
        kb2.button(text=f"\U0001f4e1 Акк осн.бота в каталоге: {st_txt}", callback_data="cmgmt_noop")
        kb2.button(text=tg_txt, callback_data=f"cmgmt_toggle_main_{bot_id}")
        kb2.button(text="\u2b05\ufe0f Назад", callback_data="adm_clone_mgmt")
        kb2.adjust(1)
        await call.message.edit_text(
            f"\u2699\ufe0f **Настройки {label}**\n\n\U0001f4e1 Аккаунты осн.бота в каталоге клона: **{st_txt}**",
            reply_markup=kb2.as_markup(), parse_mode="Markdown")


@dp.callback_query(F.data == "cmgmt_noop")
async def cmgmt_noop(call: types.CallbackQuery):
    await call.answer()


@dp.callback_query(F.data.startswith("cmgmt_toggle_main_"))
async def cmgmt_toggle_main(call: types.CallbackQuery, state: FSMContext):
    if call.from_user.id != ADMIN_ID: return
    bot_id = call.data[len("cmgmt_toggle_main_"):]
    conn, ccur = get_clone_db(bot_id)
    if not conn:
        return await call.answer("\u274c БД клона недоступна.", show_alert=True)
    try:
        ccur.execute("SELECT value FROM bot_settings WHERE key=\'main_accounts_enabled\'")
        r = ccur.fetchone()
        new_val = "0" if (r and r[0] == "1") else "1"
        ccur.execute("INSERT OR REPLACE INTO bot_settings (key,value) VALUES (\'main_accounts_enabled\',?)", (new_val,))
        conn.commit()
    finally:
        conn.close()
    await call.answer(f"\u2705 {'Включено' if new_val == '1' else 'Выключено'}")
    await state.update_data(cmgmt_action="settings")
    call.data = f"cmgmt_pick_{bot_id}"
    await cmgmt_pick_clone(call, state)


@dp.callback_query(F.data.startswith("cmgmt_delacc_do_"))
async def cmgmt_delacc_do(call: types.CallbackQuery, state: FSMContext):
    if call.from_user.id != ADMIN_ID: return
    rest = call.data[len("cmgmt_delacc_do_"):]
    # bot_id may contain underscores, phone starts with +
    idx = rest.rfind("_+")
    if idx == -1:
        return await call.answer("\u274c Ошибка парсинга.", show_alert=True)
    bot_id, phone = rest[:idx], rest[idx+1:]
    conn, ccur = get_clone_db(bot_id)
    if not conn:
        return await call.answer("\u274c БД недоступна.", show_alert=True)
    try:
        ccur.execute("DELETE FROM accounts WHERE phone=?", (phone,))
        conn.commit()
    finally:
        conn.close()
    await call.answer(f"\u2705 {phone} удалён.", show_alert=True)
    await state.clear()
    await adm_clone_mgmt(call, state)


@dp.callback_query(F.data.startswith("cmgmt_unnomber_do_"))
async def cmgmt_unnomber_do(call: types.CallbackQuery, state: FSMContext):
    if call.from_user.id != ADMIN_ID: return
    rest = call.data[len("cmgmt_unnomber_do_"):]
    idx = rest.rfind("_+")
    if idx == -1:
        return await call.answer("\u274c Ошибка парсинга.", show_alert=True)
    bot_id, phone = rest[:idx], rest[idx+1:]
    conn, ccur = get_clone_db(bot_id)
    if not conn:
        return await call.answer("\u274c БД недоступна.", show_alert=True)
    try:
        ccur.execute("SELECT owner_id FROM accounts WHERE phone=?", (phone,))
        r = ccur.fetchone()
        owner_id = r[0] if r else None
        ccur.execute("UPDATE accounts SET owner_id=NULL,expires=0,is_running=0,notified_10m=0 WHERE phone=?", (phone,))
        conn.commit()
    finally:
        conn.close()
    if owner_id:
        try:
            cr = db_fetchone("SELECT api_token FROM clones WHERE bot_id=?", (bot_id,))
            if cr:
                cb_bot = Bot(token=cr[0])
                await cb_bot.send_message(owner_id, f"\u26d4 Администратор снял вашу аренду `{phone}`.", parse_mode="Markdown")
                await cb_bot.session.close()
        except Exception:
            pass
    await call.answer(f"\u2705 Аренда {phone} снята.", show_alert=True)
    await state.clear()
    await adm_clone_mgmt(call, state)


@dp.message(CloneMgmtStates.addacc_phone)
async def cmgmt_addacc_phone(m: Message, state: FSMContext):
    if m.from_user.id != ADMIN_ID: return
    d = await state.get_data()
    bot_id = d.get("cmgmt_bot_id", "")
    phone = m.text.strip().replace(" ", "").replace("-", "")
    if not phone.startswith("+"): phone = "+" + phone
    conn, ccur = get_clone_db(bot_id)
    if not conn:
        await state.clear()
        return await m.answer("\u274c БД клона недоступна.")
    try:
        ccur.execute("INSERT OR IGNORE INTO accounts (phone, is_running, is_premium, price_per_min) VALUES (?,0,0,0.02)", (phone,))
        conn.commit()
        uname_r = db_fetchone("SELECT bot_username FROM clones WHERE bot_id=?", (bot_id,))
        label = f"@{uname_r[0]}" if uname_r and uname_r[0] else bot_id
        bot_short = bot_id.split(":")[0] if ":" in bot_id else bot_id
        await m.answer(
            f"\u2705 Аккаунт `{phone}` добавлен в БД клона {label}.\n\n"
            f"\u26a0\ufe0f Сессия должна находиться в папке:\n"
            f"`sessions_clone_{bot_short}/{phone}.session`",
            parse_mode="Markdown")
    except Exception as e:
        await m.answer(f"\u274c Ошибка: {e}")
    finally:
        conn.close()
    await state.clear()


@dp.message(CloneMgmtStates.setprice_input)
async def cmgmt_setprice_input(m: Message, state: FSMContext):
    if m.from_user.id != ADMIN_ID: return
    d = await state.get_data()
    bot_id = d.get("cmgmt_bot_id", "")
    try:
        parts = m.text.strip().split()
        phone, price = parts[0], float(parts[1].replace(",", "."))
        if price < 0.001: raise ValueError("min $0.001")
        conn, ccur = get_clone_db(bot_id)
        if not conn: raise RuntimeError("БД недоступна")
        try:
            ccur.execute("UPDATE accounts SET price_per_min=? WHERE phone=?", (price, phone))
            conn.commit()
            rows = ccur.rowcount
        finally:
            conn.close()
        if rows == 0:
            await m.answer(f"\u274c Номер `{phone}` не найден в клоне.", parse_mode="Markdown")
        else:
            await m.answer(f"\u2705 Цена `{phone}`: **${price}/мин**", parse_mode="Markdown")
    except Exception as e:
        await m.answer(f"\u274c Ошибка: {e}\nФормат: `+7999... 0.05`", parse_mode="Markdown")
    await state.clear()


@dp.message(CloneMgmtStates.broadcast_text)
async def cmgmt_broadcast_text(m: Message, state: FSMContext):
    if m.from_user.id != ADMIN_ID: return
    d = await state.get_data()
    bot_id = d.get("cmgmt_bot_id", "")
    text_msg = m.text.strip()
    cr = db_fetchone("SELECT api_token FROM clones WHERE bot_id=?", (bot_id,))
    if not cr:
        await state.clear()
        return await m.answer("\u274c Клон не найден.")
    conn, ccur = get_clone_db(bot_id)
    users = []
    if conn:
        try:
            ccur.execute("SELECT user_id FROM users")
            users = [r[0] for r in ccur.fetchall()]
        finally:
            conn.close()
    cb_bot = Bot(token=cr[0])
    sent = failed = 0
    for uid in users:
        try:
            await cb_bot.send_message(uid, f"\U0001f4e2 **Сообщение от администратора:**\n\n{text_msg}", parse_mode="Markdown")
            sent += 1
        except Exception:
            failed += 1
        await asyncio.sleep(0.05)
    await cb_bot.session.close()
    await m.answer(f"\u2705 Рассылка завершена.\nОтправлено: {sent} | Ошибок: {failed}")
    await state.clear()


@dp.message(CloneMgmtStates.pm_input)
async def cmgmt_pm_input(m: Message, state: FSMContext):
    if m.from_user.id != ADMIN_ID: return
    d = await state.get_data()
    bot_id = d.get("cmgmt_bot_id", "")
    try:
        parts = m.text.strip().split(maxsplit=1)
        uid = int(parts[0])
        msg_text = parts[1].strip() if len(parts) > 1 else ""
        if not msg_text: raise ValueError("Пустое сообщение")
        cr = db_fetchone("SELECT api_token FROM clones WHERE bot_id=?", (bot_id,))
        if not cr: raise RuntimeError("Клон не найден")
        cb_bot = Bot(token=cr[0])
        await cb_bot.send_message(uid, f"\U0001f4e9 **Сообщение от администратора:**\n\n{msg_text}", parse_mode="Markdown")
        await cb_bot.session.close()
        await m.answer(f"\u2705 Отправлено пользователю `{uid}`", parse_mode="Markdown")
    except Exception as e:
        await m.answer(f"\u274c Ошибка: {e}\nФормат: `ID текст`", parse_mode="Markdown")
    await state.clear()


@dp.message(CloneMgmtStates.stats_uid)
async def cmgmt_stats_uid(m: Message, state: FSMContext):
    if m.from_user.id != ADMIN_ID: return
    d = await state.get_data()
    bot_id = d.get("cmgmt_bot_id", "")
    try:
        uid = int(m.text.strip())
        conn, ccur = get_clone_db(bot_id)
        if not conn: raise RuntimeError("БД недоступна")
        try:
            ccur.execute("SELECT balance FROM users WHERE user_id=?", (uid,))
            bal_r = ccur.fetchone()
            if not bal_r: raise ValueError("Пользователь не найден")
            bal = round(bal_r[0], 2)
            now_ts = int(time.time())
            ccur.execute("SELECT phone, expires FROM accounts WHERE owner_id=? AND expires>?", (uid, now_ts))
            active = ccur.fetchall()
            ccur.execute("SELECT phone, duration, cost, date FROM rent_history WHERE user_id=? ORDER BY id DESC LIMIT 5", (uid,))
            hist = ccur.fetchall()
        finally:
            conn.close()
        uname_r = db_fetchone("SELECT bot_username FROM clones WHERE bot_id=?", (bot_id,))
        label = f"@{uname_r[0]}" if uname_r and uname_r[0] else bot_id
        active_txt = "\n".join([f"\u2022 `{r[0]}` (до {time.strftime('%H:%M %d.%m', time.localtime(r[1]))})" for r in active]) or "Нет активных"
        hist_txt = "\n".join([f"\u2022 `{h[0]}` | {h[1]}мин | ${h[2]} ({h[3]})" for h in hist]) or "История пуста"
        await m.answer(
            f"\U0001f4ca **Статистика `{uid}` в {label}**\n\n"
            f"\U0001f4b3 Баланс: `${bal}`\n\n"
            f"\U0001f511 Активная аренда:\n{active_txt}\n\n"
            f"\U0001f4dc История (5 последних):\n{hist_txt}",
            parse_mode="Markdown")
    except Exception as e:
        await m.answer(f"\u274c Ошибка: {e}")
    await state.clear()


# ── Клон-боты (статистика) ────────────────────────────────────
@dp.callback_query(F.data == "adm_clones")
async def adm_panel_clones(call: types.CallbackQuery, state: FSMContext):
    if call.from_user.id != ADMIN_ID: return
    clones = db_fetchall(
        'SELECT bot_id, bot_username, owner_id, is_running, earned, withdrawn FROM clones', ())
    if not clones:
        kb = InlineKeyboardBuilder().button(text="⬅️ Назад", callback_data="adm_panel")
        return await call.message.edit_text("🤖 Клон-ботов пока нет.", reply_markup=kb.as_markup())
    kb = InlineKeyboardBuilder()
    lines = []
    for bot_id, username, owner_id, is_running, earned, withdrawn in clones:
        dot = "🟢" if is_running else "🔴"
        uname = f"@{username}" if username else bot_id
        avail = round(earned - withdrawn, 2)
        lines.append(f"{dot} {uname} | Владелец: `{owner_id}` | Прибыль: ${round(earned,2)}")
        kb.button(text=f"📋 {uname}", callback_data=f"adm_clone_info_{bot_id}")
    kb.button(text="⬅️ Назад", callback_data="adm_panel")
    kb.adjust(1)
    await call.message.edit_text(
        "🤖 **Клон-боты**\n\n" + "\n".join(lines),
        reply_markup=kb.as_markup(), parse_mode="Markdown")

@dp.callback_query(F.data.startswith("adm_clone_info_"))
async def adm_clone_info(call: types.CallbackQuery, state: FSMContext):
    if call.from_user.id != ADMIN_ID: return
    bot_id = call.data[len("adm_clone_info_"):]
    res = db_fetchone(
        'SELECT bot_id, bot_username, owner_id, is_running, earned, withdrawn FROM clones WHERE bot_id=?',
        (bot_id,))
    if not res:
        return await call.answer("❌ Не найдено.", show_alert=True)
    _, username, owner_id, is_running, earned, withdrawn = res
    avail = round(earned - withdrawn, 2)
    uname = f"@{username}" if username else bot_id
    status = "🟢 Работает" if is_running else "🔴 Остановлен"

    # Аккаунты клона — они в основной БД, принадлежащие пользователям этого клона
    # (у клона своя БД, но покажем общее число из таблицы clones)
    # Число пользователей — через rent_history уникальные user_id с арендами у аккаунтов клона
    text = (
        f"📋 **Статистика клон-бота**\n\n"
        f"🤖 {uname}\n"
        f"📊 Статус: {status}\n"
        f"👤 Владелец: `{owner_id}`\n\n"
        f"💰 Прибыль за всё время: **${round(earned, 2)}**\n"
        f"📤 Выведено: **${round(withdrawn, 2)}**\n"
        f"✅ Доступно: **${avail}**"
    )
    kb = InlineKeyboardBuilder()
    if is_running:
        kb.button(text="🛑 Остановить бота", callback_data=f"adm_clone_stop_{bot_id}")
    else:
        kb.button(text="▶️ Запустить бота",  callback_data=f"adm_clone_start_{bot_id}")
    kb.button(text="🗑 Удалить клон-бот",    callback_data=f"adm_clone_del_{bot_id}")
    kb.button(text="⬅️ Назад",              callback_data="adm_clones")
    kb.adjust(1)
    await call.message.edit_text(text, reply_markup=kb.as_markup(), parse_mode="Markdown")


@dp.callback_query(F.data.startswith("adm_clone_start_"))
async def adm_clone_start_handler(call: types.CallbackQuery, state: FSMContext):
    if call.from_user.id != ADMIN_ID: return
    bot_id = call.data[len("adm_clone_start_"):]
    res = db_fetchone('SELECT api_token, owner_id FROM clones WHERE bot_id=?', (bot_id,))
    if not res: return await call.answer("❌ Не найдено.", show_alert=True)
    ok = launch_clone(res[0], res[1], bot_id)
    if ok:
        cur.execute('UPDATE clones SET is_running=1 WHERE bot_id=?', (bot_id,))
        db.commit()
        await call.answer("✅ Бот запущен!")
    else:
        await call.answer("❌ Ошибка запуска.", show_alert=True)
    # refresh info — patch call.data so adm_clone_info reads correct bot_id
    call.data = f"adm_clone_info_{bot_id}"
    await adm_clone_info(call, state)


@dp.callback_query(F.data.startswith("adm_clone_stop_"))
async def adm_clone_stop_handler(call: types.CallbackQuery, state: FSMContext):
    if call.from_user.id != ADMIN_ID: return
    bot_id = call.data[len("adm_clone_stop_"):]
    stop_clone(bot_id)
    cur.execute('UPDATE clones SET is_running=0 WHERE bot_id=?', (bot_id,))
    db.commit()
    await call.answer("🛑 Бот остановлен.")
    call.data = f"adm_clone_info_{bot_id}"
    await adm_clone_info(call, state)


@dp.callback_query(F.data.startswith("adm_clone_del_"))
async def adm_clone_del_handler(call: types.CallbackQuery, state: FSMContext):
    if call.from_user.id != ADMIN_ID: return
    bot_id = call.data[len("adm_clone_del_"):]
    res = db_fetchone('SELECT bot_username, owner_id FROM clones WHERE bot_id=?', (bot_id,))
    if not res: return await call.answer("❌ Не найдено.", show_alert=True)
    uname_db, owner_id = res
    stop_clone(bot_id)
    cur.execute('DELETE FROM clones WHERE bot_id=?', (bot_id,))
    cur.execute('DELETE FROM clone_withdraw_requests WHERE bot_id=?', (bot_id,))
    db.commit()
    label = f"@{uname_db}" if uname_db else bot_id
    await call.message.edit_text(
        f"🗑 Клон-бот {label} удалён администратором.",
        reply_markup=back_kb("adm_clones").as_markup())
    try:
        await bot.send_message(
            owner_id,
            f"⚠️ Ваш клон-бот {label} был **удалён администратором**.",
            parse_mode="Markdown")
    except Exception:
        pass


# ─── БОТЫ-НАБЛЮДАТЕЛИ (управление) ───────────────────────────────────────────
@dp.callback_query(F.data == "adm_notify_bots")
async def adm_notify_bots_menu(call: types.CallbackQuery, state: FSMContext):
    if call.from_user.id != ADMIN_ID: return
    await state.clear()
    rows = db_fetchall('SELECT slot, token, label FROM notify_bots ORDER BY slot')
    kb = InlineKeyboardBuilder()
    lines = []
    for slot, token, label in rows:
        status = "🟢" if token.strip() else "⚪"
        lines.append(f"{status} Слот {slot}: {label}")
        kb.button(text=f"⚙️ Слот {slot}: {label}", callback_data=f"adm_nb_edit_{slot}")
    kb.button(text="⬅️ Назад", callback_data="adm_panel")
    kb.adjust(1)
    await call.message.edit_text(
        "🔔 **Боты-наблюдатели** (до 3 ботов)\n\n"
        "Получают уведомления о событиях:\n"
        "• 🆕 Новый пользователь зарегистрировался\n"
        "• 🤖 Создан новый клон-бот\n"
        "• 🚀 Запущена рассылка\n"
        "• ✏️ Изменён текст рассылки\n\n"
        + "\n".join(lines),
        reply_markup=kb.as_markup(), parse_mode="Markdown")


@dp.callback_query(F.data.startswith("adm_nb_edit_"))
async def adm_nb_edit(call: types.CallbackQuery, state: FSMContext):
    if call.from_user.id != ADMIN_ID: return
    slot = int(call.data[len("adm_nb_edit_"):])
    res = db_fetchone('SELECT token, label FROM notify_bots WHERE slot=?', (slot,))
    token, label = (res[0], res[1]) if res else ('', f'Бот {slot}')
    kb = InlineKeyboardBuilder()
    kb.button(text="✏️ Установить / заменить токен", callback_data=f"adm_nb_set_{slot}")
    if token.strip():
        kb.button(text="🗑 Удалить этот бот", callback_data=f"adm_nb_del_{slot}")
    kb.button(text="⬅️ Назад", callback_data="adm_notify_bots")
    kb.adjust(1)
    token_display = f"`{token[:20]}...`" if token.strip() else "_не задан_"
    await call.message.edit_text(
        f"🔔 **Слот {slot} — {label}**\n\nТекущий токен: {token_display}",
        reply_markup=kb.as_markup(), parse_mode="Markdown")


@dp.callback_query(F.data.startswith("adm_nb_set_"))
async def adm_nb_set(call: types.CallbackQuery, state: FSMContext):
    if call.from_user.id != ADMIN_ID: return
    slot = int(call.data[len("adm_nb_set_"):])
    await state.update_data(nb_slot=slot)
    await call.message.edit_text(
        f"📋 **Слот {slot}** — введите токен бота от @BotFather\n"
        f"_(формат: `123456789:AAHxxxxxx`)_",
        reply_markup=back_kb(f"adm_nb_edit_{slot}").as_markup(),
        parse_mode="Markdown")
    await state.set_state(AdminPanelStates.adm_notify_bot_token)


@dp.message(AdminPanelStates.adm_notify_bot_token)
async def adm_nb_token_input(m: Message, state: FSMContext):
    if m.from_user.id != ADMIN_ID: return
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


@dp.callback_query(F.data.startswith("adm_nb_del_"))
async def adm_nb_del(call: types.CallbackQuery, state: FSMContext):
    if call.from_user.id != ADMIN_ID: return
    slot = int(call.data[len("adm_nb_del_"):])
    cur.execute('UPDATE notify_bots SET token="", label=? WHERE slot=?', (f'Бот {slot}', slot))
    db.commit()
    await call.answer(f"✅ Слот {slot} очищен.", show_alert=True)
    await adm_notify_bots_menu(call, state)


async def adm_ban(message: Message, command: CommandObject):
    if message.from_user.id != ADMIN_ID: return
    try:
        args = command.args.split(maxsplit=2)
        uid = int(args[0])
        hours = int(args[1])
        reason = args[2] if len(args) > 2 else "Не указана"
        unban_time = int(time.time()) + (hours * 3600)

        cur.execute('UPDATE users SET banned_until = ?, ban_reason = ? WHERE user_id = ?', (unban_time, reason, uid))
        for (phone,) in db_fetchall('SELECT phone FROM accounts WHERE owner_id = ?', (uid,)):
            cur.execute(
                'UPDATE accounts SET owner_id = NULL, expires = 0, is_running = 0, notified_10m = 0 WHERE phone = ?',
                (phone,))
        db.commit()

        await message.answer(
            f"✅ Пользователь {uid} забанен на {hours} ч.\nПричина: {reason}\nВсе его активные номера возвращены в каталог.")
        try:
            await bot.send_message(uid,
                                   f"🚫 **Вы были заблокированы!**\n\nСрок: до {time.strftime('%d.%m.%Y %H:%M', time.localtime(unban_time))}\nПричина: {reason}\nВаши аренды отменены.",
                                   parse_mode="Markdown")
        except:
            pass
    except Exception:
        await message.answer("⚠️ Формат: `/ban ID ЧАСЫ ПРИЧИНА`\nПример: `/ban 123456789 24 Спам`",
                             parse_mode="Markdown")


@dp.message(Command("unnomber"))
async def adm_unnomber(message: Message, command: CommandObject):
    if message.from_user.id != ADMIN_ID: return
    if not command.args: return await message.answer("⚠️ Формат: `/unnomber +79991234567`")

    phone = command.args.strip().replace(" ", "")
    res = db_fetchone('SELECT owner_id FROM accounts WHERE phone = ?', (phone,))
    if not res: return await message.answer(f"❌ Аккаунт `{phone}` не найден в базе.")

    owner_id = res[0]
    await refund_remaining_rent(phone, "досрочно снят администратором")
    cur.execute('UPDATE accounts SET owner_id = NULL, expires = 0, is_running = 0, notified_10m = 0 WHERE phone = ?',
                (phone,))
    db.commit()
    await message.answer(f"✅ Аренда номера `{phone}` досрочно завершена.", parse_mode="Markdown")
    if owner_id:
        try:
            await bot.send_message(owner_id, f"⚠️ Администратор досрочно завершил вашу аренду номера `{phone}`.",
                                   parse_mode="Markdown")
        except:
            pass


@dp.message(Command("blacklist"))
async def adm_blacklist(message: Message, command: CommandObject):
    if message.from_user.id != ADMIN_ID: return
    if not command.args: return await message.answer("⚠️ Формат: `/blacklist слово`")
    word = command.args.strip().lower()
    try:
        cur.execute('INSERT INTO blacklist (word) VALUES (?)', (word,))
        db.commit()
        await message.answer(f"✅ Слово `{word}` успешно добавлено.")
    except sqlite3.IntegrityError:
        await message.answer(f"⚠️ Слово `{word}` уже присутствует.")


@dp.message(Command("stats"))
async def adm_stats(message: Message, command: CommandObject):
    if message.from_user.id != ADMIN_ID: return
    if not command.args: return await message.answer("⚠️ Формат: `/stats ID`")
    try:
        uid = int(command.args.strip())
        bal = get_balance(uid)
        if bal is None: return await message.answer("❌ Пользователь не найден.")

        active_rows = db_fetchall('SELECT phone, expires FROM accounts WHERE owner_id = ? AND expires > ?',
                                  (uid, int(time.time())))
        active_list = "\n".join([f"• `{r[0]}` (до {time.strftime('%H:%M %d.%m', time.localtime(r[1]))})" for r in
                                 active_rows]) or "Нет активных"

        hist_rows = db_fetchall(
            'SELECT phone, duration, cost, date FROM rent_history WHERE user_id = ? ORDER BY id DESC LIMIT 5', (uid,))
        history_rent_list = "\n".join(
            [f"• `{h[0]}` | {h[1]} мин | ${h[2]} ({h[3]})" for h in hist_rows]) or "История пуста"

        ban_info = check_ban(uid)
        ban_text = f"🚫 **Бан:** до {time.strftime('%d.%m.%Y %H:%M', time.localtime(ban_info[0]))} ({ban_info[1]})\n\n" if ban_info else ""

        report = (f"👤 **Статистика пользователя `{uid}`**\n\n{ban_text}💳 **Баланс:** `${bal}`\n\n"
                  f"🔑 **Активная аренда:**\n{active_list}\n\n"
                  f"📜 **Последние аренды:**\n{history_rent_list}")
        await message.answer(report, parse_mode="Markdown")
    except:
        await message.answer("❌ Ошибка в ID.")


@dp.message(Command("givebal"))
async def adm_give(message: Message, command: CommandObject):
    if message.from_user.id != ADMIN_ID: return
    try:
        uid, amt = command.args.split()
        amt = float(amt.replace(",", "."))
        add_payment_history(int(uid), amt, "Admin Add")
        await message.answer(f"✅ Зачислено **${amt}** пользователю `{uid}`", parse_mode="Markdown")
    except:
        await message.answer("Ошибка. Формат: `/givebal ID СУММА`")


@dp.message(Command("delbal"))
async def adm_del_bal(message: Message, command: CommandObject):
    if message.from_user.id != ADMIN_ID: return
    try:
        uid, amt = command.args.split()
        uid, amt = int(uid), float(amt.replace(",", "."))
        cur.execute('UPDATE users SET balance = balance - ? WHERE user_id = ?', (amt, uid))
        db.commit()
        await message.answer(f"✅ Списано **${amt}** у пользователя `{uid}`", parse_mode="Markdown")
    except:
        await message.answer("Ошибка. Формат: `/delbal ID СУММА`")


@dp.message(Command("delacc"))
async def adm_del_acc(message: Message, command: CommandObject):
    if message.from_user.id != ADMIN_ID: return
    if not command.args: return await message.answer("⚠️ Формат: `/delacc +7999...`")
    phone = command.args.strip().replace(" ", "")
    cur.execute('DELETE FROM accounts WHERE phone = ?', (phone,))
    db.commit()
    if os.path.exists(f"sessions/{phone}.session"):
        os.remove(f"sessions/{phone}.session")
    await message.answer(f"✅ Аккаунт `{phone}` удален.")


@dp.message(Command("setprice"))
async def adm_set_price(message: Message, command: CommandObject):
    if message.from_user.id != ADMIN_ID: return
    try:
        phone, price = command.args.split()
        price = float(price.replace(",", "."))
        if price < 0.001:
            return await message.answer("⚠️ Минимальная цена: **$0.001/мин**", parse_mode="Markdown")
        cur.execute('UPDATE accounts SET price_per_min = ? WHERE phone = ?', (price, phone))
        db.commit()
        await message.answer(f"✅ Цена для `{phone}` теперь **${price}/мин**", parse_mode="Markdown")
    except:
        await message.answer("Ошибка. Формат: `/setprice +7... 0.15`")


# --- КОМАНДА: УСТАНОВИТЬ ЦЕНУ В КЛОНЕ ---
# Формат: /setpriceclon @username_клона +7999... 0.05
@dp.message(Command("setpriceclon"))
async def adm_set_price_clon(message: Message, command: CommandObject):
    if message.from_user.id != ADMIN_ID: return
    if not command.args:
        return await message.answer(
            "⚠️ Формат: `/setpriceclon @username_клона +7999... цена`\n"
            "Пример: `/setpriceclon @myclonebot +79991234567 0.05`\n\n"
            "Минимальная цена: **$0.001/мин**",
            parse_mode="Markdown")
    try:
        args = command.args.strip().split()
        if len(args) != 3:
            raise ValueError("Нужно 3 аргумента")
        clone_username = args[0].lstrip("@")
        phone = args[1].strip()
        price = float(args[2].replace(",", "."))

        if price < 0.001:
            return await message.answer(
                "⚠️ Минимальная цена: **$0.001/мин**", parse_mode="Markdown")

        # Ищем токен клона по username
        res = db_fetchone(
            'SELECT api_token, bot_id FROM clones WHERE bot_username=?',
            (clone_username,))
        if not res:
            return await message.answer(
                f"❌ Клон-бот `@{clone_username}` не найден в базе.",
                parse_mode="Markdown")

        clone_token, bot_id = res
        clone_bot_db = f"clone_{bot_id}.db"

        # Обновляем цену напрямую в БД клона
        if not os.path.exists(clone_bot_db):
            return await message.answer(
                f"❌ База данных клона `{clone_bot_db}` не найдена.\n"
                "Убедитесь, что клон-бот хотя бы раз запускался.",
                parse_mode="Markdown")

        clone_db = sqlite3.connect(clone_bot_db, check_same_thread=False)
        clone_db.execute('PRAGMA busy_timeout=3000')
        clone_cur = clone_db.cursor()
        clone_cur.execute(
            'UPDATE accounts SET price_per_min=? WHERE phone=?', (price, phone))
        clone_db.commit()
        rows_affected = clone_cur.rowcount
        clone_db.close()

        if rows_affected == 0:
            return await message.answer(
                f"❌ Номер `{phone}` не найден в клоне `@{clone_username}`.",
                parse_mode="Markdown")

        await message.answer(
            f"✅ Цена для `{phone}` в клоне `@{clone_username}` установлена: **${price}/мин**",
            parse_mode="Markdown")

        # Уведомляем владельца клона
        owner_res = db_fetchone('SELECT owner_id FROM clones WHERE bot_username=?', (clone_username,))
        if owner_res:
            try:
                clone_bot_obj = Bot(token=clone_token)
                await clone_bot_obj.send_message(
                    owner_res[0],
                    f"📢 **Администратор изменил цену аккаунта**\n\n"
                    f"📱 Номер: `{phone}`\n"
                    f"💰 Новая цена: **${price}/мин**",
                    parse_mode="Markdown")
                await clone_bot_obj.session.close()
            except Exception as e:
                logging.error(f"Не удалось уведомить владельца клона: {e}")

    except ValueError as e:
        await message.answer(
            f"⚠️ Ошибка: {e}\n\n"
            "Формат: `/setpriceclon @username +7999... 0.05`",
            parse_mode="Markdown")
    except Exception as e:
        await message.answer(f"❌ Ошибка: {e}")


# --- НОВАЯ КОМАНДА: РЕДАКТИРОВАТЬ ЧАТЫ КАТАЛОГА ---
@dp.message(Command("redak"))
async def adm_redak(message: Message, command: CommandObject):
    """
    /redak +79991234567 https://t.me/chat1, https://t.me/chat2
    Редактирует список чатов, которые отображаются в каталоге при нажатии "Инфо".
    """
    if message.from_user.id != ADMIN_ID: return
    if not command.args:
        return await message.answer(
            "⚠️ Формат: `/redak +79991234567 чаты`\n\nПример:\n`/redak +79991234567 https://t.me/chat1, https://t.me/chat2`",
            parse_mode="Markdown")
    try:
        parts = command.args.split(maxsplit=1)
        phone = parts[0].strip()
        chats_text = parts[1].strip() if len(parts) > 1 else ""

        res = db_fetchone('SELECT phone FROM accounts WHERE phone = ?', (phone,))
        if not res:
            return await message.answer(f"❌ Аккаунт `{phone}` не найден.", parse_mode="Markdown")

        cur.execute('UPDATE accounts SET catalog_chats = ? WHERE phone = ?', (chats_text, phone))
        db.commit()
        await message.answer(
            f"✅ Список чатов для `{phone}` обновлён:\n`{chats_text}`",
            parse_mode="Markdown")
    except Exception as e:
        await message.answer(f"❌ Ошибка: {e}")


# --- НОВАЯ КОМАНДА: НАПИСАТЬ ВСЕМ ПОЛЬЗОВАТЕЛЯМ ---
@dp.message(Command("all"))
async def adm_broadcast_all(message: Message, command: CommandObject):
    if message.from_user.id != ADMIN_ID: return
    if not command.args:
        return await message.answer("⚠️ Формат: `/all ваше сообщение`", parse_mode="Markdown")

    text = command.args.strip()
    users = db_fetchall('SELECT user_id FROM users')
    sent = 0
    failed = 0
    for (uid,) in users:
        try:
            await bot.send_message(uid, f"📢 **Сообщение от администратора:**\n\n{text}", parse_mode="Markdown")
            sent += 1
        except:
            failed += 1
        await asyncio.sleep(0.05)  # Антиспам-пауза

    await message.answer(f"✅ Рассылка завершена.\nОтправлено: {sent}\nОшибок: {failed}")


# --- НОВАЯ КОМАНДА: НАПИСАТЬ КОНКРЕТНОМУ ПОЛЬЗОВАТЕЛЮ ---
# Форматы:
#   /pm ID сообщение                          — отправить в основной бот
#   /pm @username_клона ID сообщение          — отправить через клон-бот
@dp.message(Command("pm"))
async def adm_pm(message: Message, command: CommandObject):
    if message.from_user.id != ADMIN_ID: return
    if not command.args:
        return await message.answer(
            "⚠️ Форматы:\n"
            "`/pm ID сообщение` — ответ через основной бот\n"
            "`/pm @username_клона ID сообщение` — ответ через клон-бот",
            parse_mode="Markdown")
    try:
        args = command.args.strip()

        # Формат: /pm @username_клона ID текст
        if args.startswith("@"):
            parts = args.split(maxsplit=2)
            clone_username = parts[0].lstrip("@")
            uid = int(parts[1])
            text = parts[2] if len(parts) > 2 else ""
            if not text:
                return await message.answer("⚠️ Сообщение не может быть пустым.")

            # Ищем токен клона по username
            res = db_fetchone(
                'SELECT api_token FROM clones WHERE bot_username=?', (clone_username,))
            if not res:
                return await message.answer(
                    f"❌ Клон-бот `@{clone_username}` не найден в базе.",
                    parse_mode="Markdown")

            clone_token = res[0]
            clone_bot_obj = Bot(token=clone_token)
            try:
                await clone_bot_obj.send_message(
                    uid,
                    f"📩 **Сообщение от администратора:**\n\n{text}",
                    parse_mode="Markdown")
                await message.answer(
                    f"✅ Ответ отправлен пользователю `{uid}` через `@{clone_username}`.",
                    parse_mode="Markdown")
            finally:
                await clone_bot_obj.session.close()

        # Формат: /pm ID текст
        else:
            parts = args.split(maxsplit=1)
            uid = int(parts[0])
            text = parts[1].strip() if len(parts) > 1 else ""
            if not text:
                return await message.answer("⚠️ Сообщение не может быть пустым.")
            await bot.send_message(
                uid,
                f"📩 **Сообщение от администратора:**\n\n{text}",
                parse_mode="Markdown")
            await message.answer(
                f"✅ Сообщение успешно отправлено пользователю `{uid}`.",
                parse_mode="Markdown")

    except Exception as e:
        await message.answer(f"❌ Ошибка: {e}")


# --- КОМАНДА ОТВЕТА ПОЛЬЗОВАТЕЛЯ АДМИНУ ---
@dp.message(Command("pma"))
async def user_reply_to_admin(message: Message, command: CommandObject):
    if not command.args:
        return await message.answer("⚠️ Формат: /pma ваше сообщение")
    text = command.args.strip()
    user = message.from_user
    user_info = f"ID: `{user.id}`"
    if user.username:
        user_info += f" | @{user.username}"
    if user.full_name:
        user_info += f" | {user.full_name}"
    try:
        # Отправляем ВСЕМ ADMIN_ID (можно расширить список)
        await bot.send_message(
            ADMIN_ID,
            f"📩 **Сообщение от пользователя (основной бот)**\n"
            f"{user_info}\n\n"
            f"💬 {text}\n\n"
            f"📤 Ответить: `/pm {user.id} ваш ответ`",
            parse_mode="Markdown"
        )
        await message.answer("✅ Ваше сообщение отправлено администратору.")
    except Exception as e:
        await message.answer(f"❌ Не удалось отправить сообщение: {e}")


# --- ОСНОВНЫЕ ОБРАБОТЧИКИ ---
@dp.message(Command("start"))
@dp.callback_query(F.data == "to_main")
async def start_cmd(event: types.Message | types.CallbackQuery, state: FSMContext):
    await state.clear()
    user_id = event.from_user.id
    bonus_text = ""

    if get_balance(user_id) is None:
        # Проверяем реф-параметр: /start ref_BOTID
        ref_bot_id = None
        if isinstance(event, Message) and event.text:
            parts = event.text.strip().split()
            if len(parts) > 1 and parts[1].startswith("ref_"):
                ref_bot_id = parts[1][4:]

        ref_note = ""
        if ref_bot_id:
            clone_res = db_fetchone('SELECT owner_id, bot_username FROM clones WHERE bot_id=?', (ref_bot_id,))
            if clone_res and clone_res[0] != user_id:
                REF_REWARD = round(WELCOME_BONUS * 0.5, 4)
                cur.execute('UPDATE clones SET earned = earned + ? WHERE bot_id=?', (REF_REWARD, ref_bot_id))
                uname_ref = f"@{clone_res[1]}" if clone_res[1] else ref_bot_id
                ref_note = f"\n🔗 Реф: {uname_ref} (+${REF_REWARD})"

        cur.execute('INSERT OR IGNORE INTO users (user_id, balance, referred_by) VALUES (?, ?, ?)',
                    (user_id, WELCOME_BONUS, ref_bot_id or ""))
        db.commit()
        bonus_text = f"\n\n🎁 Вам начислен бонус: **${WELCOME_BONUS}**"
        try:
            await notify_admins(
                f"🆕 **Новый пользователь зарегистрирован!**\n"
                f"👤 ID: `{user_id}`\n"
                f"📛 Имя: {event.from_user.full_name}\n"
                f"🔗 @{event.from_user.username or '—'}{ref_note}")
        except Exception:
            pass

    caption = f"👋 Главное меню. Выберите раздел:{bonus_text}"

    if isinstance(event, Message):
        await event.answer_photo(
            photo=IMG_MAIN, caption=caption,
            reply_markup=main_menu(user_id), parse_mode="Markdown")
    else:
        await event.message.edit_media(
            media=InputMediaPhoto(media=IMG_MAIN, caption=caption, parse_mode="Markdown"),
            reply_markup=InlineKeyboardBuilder().button(
                text="📂 Каталог", callback_data="catalog_inline").as_markup())


@dp.message(F.text == "❓ Помощь")
async def help_menu(message: Message, state: FSMContext):
    await state.clear()
    text = """🤖 **Справка по боту**

1️⃣ **Каталог аккаунтов**
Здесь вы можете выбрать и арендовать номер на нужное время. У каждого номера указана своя цена за минуту аренды.
🔴 - номер занят | 🟢 - номер свободен

2️⃣ **Моя аренда**
В этом разделе происходит всё управление рассылкой:
• **📝 Текст и 🖼 Фото:** Установите сообщение для рассылки.
• **👥 Чаты:** Настройте список ссылок. 
💡 **Важно:** Бот умеет рассылать сразу в несколько тем (топиков) одного чата! Просто укажите ссылки через запятую:
`https://t.me/roblox_basee/16425957, https://t.me/roblox_basee/25539176`
• **⏳ Сек:** Интервал ожидания между отправкой сообщений (минимум 30 сек).
• **🚀 ПУСК / 🛑 СТОП:** Управление процессом рассылки.

3️⃣ **Баланс**
Для аренды необходимо пополнить внутренний счет. Доступно пополнение через Telegram Stars или CryptoPay.

⚠️ **Внимание:** В боте работает система фильтрации слов. Использование запрещенных слов (скам, чернуха и т.д.) приведет к невозможности рассылки или бану.

📩 **Ответить администратору:** если вам написал администратор, вы можете ответить командой `/pma ваше сообщение`."""
    await message.answer(text, parse_mode="Markdown")


@dp.message(F.text == "👨‍💻 Support")
async def support_info(message: Message):
    kb = InlineKeyboardBuilder().button(text="Написать в Поддержку", url=SUPPORT_URL)
    await message.answer("Связь с администрацией и поддержка:", reply_markup=kb.as_markup())


@dp.message(F.text == "💰 Баланс")
@dp.callback_query(F.data == "to_balance")
async def bal_menu(event: Message | types.CallbackQuery, state: FSMContext):
    await state.clear()
    bal = get_balance(event.from_user.id)
    kb = InlineKeyboardBuilder()
    kb.button(text="💎 Stars", callback_data="topup_stars")
    kb.button(text="🔌 CryptoPay", callback_data="topup_crypto")
    kb.button(text="⬅️ Назад", callback_data="to_main")

    if isinstance(event, Message):
        await event.answer_photo(photo=IMG_BALANCE, caption=f"💳 Ваш баланс: **${bal}**",
                                 reply_markup=kb.adjust(2, 1).as_markup())
    else:
        await event.message.edit_media(media=InputMediaPhoto(media=IMG_BALANCE, caption=f"💳 Ваш баланс: **${bal}**"),
                                       reply_markup=kb.adjust(2, 1).as_markup())


# --- МАГАЗИН ---
@dp.message(F.text == "🛒 Shop")
@dp.callback_query(F.data == "shop_main")
async def shop_menu(event: Message | types.CallbackQuery, state: FSMContext):
    await state.clear()
    kb = InlineKeyboardBuilder().button(text="Код Бота - 20$", callback_data="buy_bot_code")
    kb.row(InlineKeyboardButton(text="⬅️ Назад", callback_data="to_main"))
    text = "🛒 **Магазин**\n\nВыберите товар для покупки:"
    if isinstance(event, Message):
        await event.answer(text, reply_markup=kb.as_markup(), parse_mode="Markdown")
    else:
        try:
            await event.message.edit_text(text, reply_markup=kb.as_markup(), parse_mode="Markdown")
        except:
            await event.message.edit_caption(caption=text, reply_markup=kb.as_markup(), parse_mode="Markdown")


@dp.callback_query(F.data == "buy_bot_code")
async def buy_bot_code(call: types.CallbackQuery):
    cost = 20.0
    if get_balance(call.from_user.id) < cost:
        return await call.answer("❌ Недостаточно средств!", show_alert=True)
    cur.execute('UPDATE users SET balance = balance - ? WHERE user_id = ?', (cost, call.from_user.id))
    db.commit()
    msg = f"✅ **Оплата успешна!**\nСписано ${cost}.\n🔗 Ссылка на архив: https://ВАША_ССЫЛКА_НА_АРХИВ.zip"
    await call.message.edit_text(msg, parse_mode="Markdown")


# --- КАТАЛОГ ---
@dp.message(F.text == "📂 Каталог аккаунтов")
@dp.callback_query(F.data == "catalog_inline")
async def catalog(event: Message | types.CallbackQuery, state: FSMContext):
    await state.clear()
    user_id = event.from_user.id
    ban_info = check_ban(user_id)
    if ban_info:
        msg = f"🚫 Вы заблокированы до {time.strftime('%d.%m.%Y %H:%M', time.localtime(ban_info[0]))}.\nПричина: {ban_info[1]}\nДоступ в каталог закрыт."
        if isinstance(event, Message):
            await event.answer(msg)
        else:
            await event.answer(msg, show_alert=True)
        return

    # Показываем ВСЕ аккаунты — и свободные, и занятые
    rows = db_fetchall('SELECT phone, price_per_min, is_premium, owner_id, expires FROM accounts', ())
    all_items = [(phone, price, is_premium, owner_id, expires, '') for phone, price, is_premium, owner_id, expires in rows]

    # Если включён показ аккаунтов клонов — подгружаем из каждой клоновой БД
    if get_main_setting('show_clone_accounts') == '1':
        clone_rows = db_fetchall('SELECT bot_id, bot_username FROM clones WHERE is_running=1')
        for bot_id, bot_username in clone_rows:
            conn, ccur = get_clone_db(bot_id)
            if conn is None:
                continue
            try:
                ccur.execute('SELECT phone, price_per_min, is_premium, owner_id, expires FROM accounts')
                for row in ccur.fetchall():
                    phone, price, is_premium, owner_id, expires = row
                    tag = f"@{bot_username}" if bot_username else bot_id
                    all_items.append((phone, price, is_premium, owner_id, expires, tag))
            except Exception:
                pass
            finally:
                conn.close()

    kb = InlineKeyboardBuilder()
    now = int(time.time())
    for phone, price, is_premium, owner_id, expires, clone_tag in all_items:
        is_rented = owner_id is not None and expires is not None and expires > now
        tag_label = f" [{clone_tag}]" if clone_tag else ""
        if is_rented:
            time_left = format_time_left(expires)
            label = f"🔴 {'⭐ ' if is_premium else ''}📱 {phone}{tag_label} (${price}/мин) · ещё {time_left}"
        else:
            label = f"🟢 {'⭐ ' if is_premium else ''}📱 {phone}{tag_label} (${price}/мин)"
        # Для аккаунтов клонов передаём bot_id через callback
        cb = f"view_clone_{bot_id}_{phone}" if clone_tag else f"view_{phone}"
        kb.button(text=label, callback_data=cb)

    kb.adjust(1).row(InlineKeyboardButton(text="⬅️ Назад", callback_data="to_main"))

    caption = "📋 **Все номера в сервисе:**\n🟢 — свободен | 🔴 — занят"
    if isinstance(event, Message):
        await event.answer_photo(photo=IMG_CATALOG, caption=caption, reply_markup=kb.as_markup(),
                                 parse_mode="Markdown")
    else:
        await event.message.edit_media(
            media=InputMediaPhoto(media=IMG_CATALOG, caption=caption, parse_mode="Markdown"),
            reply_markup=kb.as_markup())


# --- ПРОСМОТР НОМЕРА (Инфо + Аренда) ---
@dp.callback_query(F.data.startswith("view_"))
async def view_account(call: types.CallbackQuery, state: FSMContext):
    phone = call.data[5:]
    res = db_fetchone('SELECT phone, price_per_min, is_premium, owner_id, expires FROM accounts WHERE phone = ?',
                      (phone,))
    if not res:
        return await call.answer("❌ Аккаунт не найден.", show_alert=True)

    _, price, is_premium, owner_id, expires = res
    now = int(time.time())
    is_rented = owner_id is not None and expires is not None and expires > now

    status_icon = "🔴 Занят" if is_rented else "🟢 Свободен"
    premium_text = "⭐ Premium\n" if is_premium else ""
    time_left_text = f"\n⏳ Осталось: {format_time_left(expires)}" if is_rented else ""

    caption = (f"📱 **Номер:** `{phone}`\n"
               f"{premium_text}"
               f"💰 Цена: **${price}/мин**\n"
               f"🔘 Статус: {status_icon}{time_left_text}")

    kb = InlineKeyboardBuilder()
    kb.button(text="ℹ️ Инфо", callback_data=f"info_{phone}")
    kb.button(text="🔑 Аренда", callback_data=f"rent_{phone}")
    kb.button(text="⬅️ Назад", callback_data="catalog_inline")
    kb.adjust(2, 1)

    try:
        await call.message.edit_caption(caption=caption, reply_markup=kb.as_markup(), parse_mode="Markdown")
    except:
        await call.message.answer(caption, reply_markup=kb.as_markup(), parse_mode="Markdown")


# --- КНОПКА "ИНфО" — чаты из каталога ---
@dp.callback_query(F.data.startswith("info_"))
async def show_info(call: types.CallbackQuery):
    phone = call.data[5:]
    res = db_fetchone('SELECT catalog_chats FROM accounts WHERE phone = ?', (phone,))
    if not res:
        return await call.answer("❌ Аккаунт не найден.", show_alert=True)

    chats_raw = res[0] or ""
    chats_list = [c.strip() for c in chats_raw.split(',') if c.strip()]

    if chats_list:
        chats_text = "\n".join([f"• {c}" for c in chats_list])
        text = f"📋 **Чаты для рассылки номера** `{phone}`:\n\n{chats_text}"
    else:
        text = f"ℹ️ Для номера `{phone}` чаты ещё не добавлены.\n\nАдминистратор может добавить их командой:\n`/redak {phone} ссылки`"

    kb = InlineKeyboardBuilder()
    kb.button(text="⬅️ Назад", callback_data=f"view_{phone}")

    await call.message.edit_caption(caption=text, reply_markup=kb.as_markup(), parse_mode="Markdown")


# --- КНОПКА "АРЕНДА" ---
@dp.callback_query(F.data.startswith("rent_"))
async def rent_init(call: types.CallbackQuery, state: FSMContext):
    ban_info = check_ban(call.from_user.id)
    if ban_info:
        return await call.answer("🚫 Вы заблокированы. Аренда недоступна.", show_alert=True)

    phone = call.data[5:]

    # Проверяем что номер свободен
    res = db_fetchone('SELECT owner_id, expires FROM accounts WHERE phone = ?', (phone,))
    if res and res[0] is not None and res[1] is not None and res[1] > int(time.time()):
        return await call.answer("❌ Этот номер уже арендован.", show_alert=True)

    await state.update_data(rent_phone=phone)
    await call.message.edit_caption(
        caption=f"⏳ Введите время аренды в минутах\n(От {MIN_RENT_TIME} до 600):",
        reply_markup=back_kb(f"view_{phone}").as_markup())
    await state.set_state(States.waiting_for_rent_time)


@dp.message(States.waiting_for_rent_time)
async def rent_finish(m: Message, state: FSMContext):
    data = await state.get_data()
    try:
        mins = int(m.text)
        if mins < MIN_RENT_TIME or mins > 600:
            return await m.answer(f"⚠️ Лимит: {MIN_RENT_TIME} - 600 минут.")

        res = db_fetchone('SELECT price_per_min FROM accounts WHERE phone = ?', (data['rent_phone'],))
        cost = round(mins * res[0], 2)
        if get_balance(m.from_user.id) < cost:
            return await m.answer("❌ Недостаточно средств.")

        cur.execute('UPDATE users SET balance = balance - ? WHERE user_id = ?', (cost, m.from_user.id))
        exp = int(time.time()) + (mins * 60)
        cur.execute('UPDATE accounts SET owner_id = ?, expires = ?, is_running = 0, notified_10m = 0 WHERE phone = ?',
                    (m.from_user.id, exp, data['rent_phone']))
        cur.execute('INSERT INTO rent_history (user_id, phone, duration, cost, date) VALUES (?, ?, ?, ?, ?)',
                    (m.from_user.id, data['rent_phone'], mins, cost, time.strftime('%Y-%m-%d %H:%M:%S')))
        db.commit()
        await m.answer(f"✅ Аккаунт `{data['rent_phone']}` арендован на {mins} мин!\nСписано: **${cost}**",
                       parse_mode="Markdown")

        try:
            await notify_admins(
                f"🔔 **Новая аренда**\n"
                f"👤 Пользователь: `{m.from_user.id}`\n"
                f"📱 Номер: `{data['rent_phone']}`\n"
                f"⏱ Время: {mins} мин.\n"
                f"💰 Списано: **${cost}**")
        except Exception:
            pass
        await state.clear()
    except:
        await m.answer("Ошибка ввода. Введите целое число от 10 до 600.")


# --- УПРАВЛЕНИЕ АРЕНДОЙ ---
@dp.message(F.text == "🔑 Моя аренда")
@dp.callback_query(F.data == "to_my_rents")
async def my_rents(event: Message | types.CallbackQuery, state: FSMContext):
    await state.clear()
    rows = db_fetchall('SELECT phone, is_premium FROM accounts WHERE owner_id = ? AND expires > ?',
                       (event.from_user.id, int(time.time())))
    kb = InlineKeyboardBuilder()
    for r in rows:
        kb.button(text=f"{'⭐ ' if r[1] else ''}⚙️ {r[0]}", callback_data=f"manage_{r[0]}")
    kb.adjust(1).row(InlineKeyboardButton(text="⬅️ Назад", callback_data="to_main"))

    if isinstance(event, Message):
        await event.answer_photo(photo=IMG_MY_RENT, caption="🔧 Ваши активные номера:", reply_markup=kb.as_markup())
    else:
        await event.message.edit_media(media=InputMediaPhoto(media=IMG_MY_RENT, caption="🔧 Ваши активные номера:"),
                                       reply_markup=kb.as_markup())


@dp.callback_query(F.data.startswith("manage_"))
async def manage_acc(call: types.CallbackQuery, state: FSMContext):
    await state.clear()
    p = call.data.split("_")[1]
    res = db_fetchone('SELECT is_running FROM accounts WHERE phone = ?', (p,))
    if not res:
        return await call.answer("❌ Номер больше не в вашей аренде", show_alert=True)

    kb = InlineKeyboardBuilder()
    kb.button(text="📝 Текст", callback_data=f"set_text_{p}")
    kb.button(text="🖼 Фото", callback_data=f"set_photo_{p}")
    kb.button(text="👥 Чаты", callback_data=f"set_chats_{p}")
    kb.button(text="⏳ Сек", callback_data=f"set_int_{p}")
    kb.button(text="🛑 СТОП" if res[0] else "🚀 ПУСК", callback_data=f"{'off' if res[0] else 'on'}_{p}")
    kb.button(text="⬅️ Назад", callback_data="to_my_rents")
    await call.message.edit_caption(caption=f"📱 `{p}`\nСтатус: {'🔥 РАБОТАЕТ' if res[0] else '💤 ПАУЗА'}",
                                    reply_markup=kb.adjust(2, 2, 1, 1).as_markup(), parse_mode="Markdown")


# --- ОПЛАТА ---
@dp.callback_query(F.data.startswith("topup_"))
async def topup_init(call: types.CallbackQuery, state: FSMContext):
    method = call.data.split("_")[1]
    await state.update_data(method=method)
    msg = "Введите количество Stars:" if method == 'stars' else "Введите сумму в USD для CryptoPay:"
    await call.message.edit_caption(caption=msg, reply_markup=back_kb("to_balance").as_markup())
    await state.set_state(States.top_up_amount)


@dp.message(States.top_up_amount)
async def create_pay(message: Message, state: FSMContext):
    data = await state.get_data()
    try:
        val = float(message.text.replace(",", "."))
        if val <= 0: raise ValueError
    except:
        return await message.answer("Пожалуйста, введите корректное число.")

    if data['method'] == 'stars':
        stars_count = int(val)
        usd_equiv = round(stars_count * STAR_RATE, 2)
        await message.answer_invoice(
            title="Пополнение баланса",
            description=f"Покупка {stars_count} ⭐ Stars → ${usd_equiv} на баланс",
            payload=f"paystars_{usd_equiv}",
            currency="XTR",
            prices=[LabeledPrice(label="Stars", amount=stars_count)],
        )
    elif crypto:
        inv = await crypto.create_invoice(asset='USDT', amount=val)
        kb = InlineKeyboardBuilder().button(text="Оплатить", url=inv.bot_invoice_url).button(
            text="Проверить", callback_data=f"chk_{inv.invoice_id}_{val}")
        await message.answer(f"Счет на ${val} создан:", reply_markup=kb.adjust(1).as_markup())
    await state.clear()


@dp.callback_query(F.data.startswith("chk_"))
async def check_crypto(call: types.CallbackQuery):
    _, iid, amt = call.data.split("_")
    inv = await crypto.get_invoices(invoice_ids=int(iid))
    if inv and inv.status == 'paid':
        add_payment_history(call.from_user.id, float(amt), "CryptoPay")
        await call.message.edit_text("✅ Оплата получена!")
        try:
            await notify_admins(
                f"💰 **Пополнение баланса**\n"
                f"👤 Пользователь: `{call.from_user.id}`\n"
                f"💵 Сумма: **${amt}**\n"
                f"💳 Метод: CryptoPay")
        except Exception:
            pass
    else:
        await call.answer("Не оплачено", show_alert=True)


@dp.pre_checkout_query()
async def pre_checkout(q: PreCheckoutQuery):
    await bot.answer_pre_checkout_query(q.id, ok=True)


@dp.message(F.successful_payment)
async def success_pay(m: Message):
    usd = float(m.successful_payment.invoice_payload.split("_")[1])
    add_payment_history(m.from_user.id, usd, "Stars")
    await m.answer(f"✅ Зачислено ${usd}")
    try:
        await notify_admins(
            f"💰 **Пополнение баланса**\n"
            f"👤 Пользователь: `{m.from_user.id}`\n"
            f"💵 Сумма: **${usd}**\n"
            f"⭐ Метод: Telegram Stars")
    except Exception:
        pass


# --- ТЕЛЕТОН И РАССЫЛКА ---

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
    """
    Единая функция запроса кода Telegram.
    Создаёт TelegramClient, подключается, отправляет запрос кода.
    При успехе — переводит в waiting_for_code.
    При уже авторизованной сессии — сразу к ask_premium_status.
    """
    uid = m.from_user.id
    await _disconnect_client(uid)

    os.makedirs("sessions", exist_ok=True)

    c = TelegramClient(
        f"sessions/{phone}",
        API_ID,
        API_HASH,
        receive_updates=False,
        device_model="Desktop",
        system_version="Windows 10",
        app_version="4.16.7",
        lang_code="ru",
        system_lang_code="ru-RU",
    )

    try:
        await c.connect()
        logging.info(f"[addacc] Подключились для {phone}, uid={uid}")
    except Exception as e:
        await m.answer(f"❌ Не удалось подключиться к Telegram: {e}")
        try:
            await c.disconnect()
        except Exception:
            pass
        return

    try:
        if await c.is_user_authorized():
            active_clients[uid] = {"client": c, "hash": None}
            await m.answer("✅ Аккаунт уже авторизован в сессии!")
            await state.update_data(phone=phone, from_panel=from_panel)
            await ask_premium_status(m, state, phone)
            return

        sent = await c.send_code_request(phone)
        logging.info(f"[addacc] Код запрошен для {phone}, hash={sent.phone_code_hash[:6]}…")

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

@dp.message(Command("addacc"))
async def add_acc(m: Message, state: FSMContext):
    if m.from_user.id != ADMIN_ID: return
    await m.answer("📱 Введите номер телефона (с кодом страны, например: +79991234567):")
    await state.update_data(from_panel=False)
    await state.set_state(States.waiting_for_phone)


@dp.message(States.waiting_for_phone)
async def h_phone(m: Message, state: FSMContext):
    d = await state.get_data()
    phone = m.text.strip().replace(" ", "").replace("-", "")
    if not phone.startswith("+"):
        phone = "+" + phone
    from_panel = d.get('from_panel', False)
    await _request_code(m, state, phone, from_panel)


@dp.message(States.waiting_for_code)
async def h_code(m: Message, state: FSMContext):
    d = await state.get_data()
    uid = m.from_user.id
    code = m.text.strip().replace(" ", "").replace("-", "")

    entry = active_clients.get(uid)
    if not entry:
        await m.answer("❌ Сессия прервана. Введите номер телефона заново.")
        await state.set_state(States.waiting_for_phone)
        return

    c         = entry["client"] if isinstance(entry, dict) else entry
    code_hash = entry["hash"]   if isinstance(entry, dict) else d.get("code_hash")

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
        # ✅ Успех без 2FA
        await ask_premium_status(m, state, d['phone'])

    except SessionPasswordNeededError:
        # Код принят, аккаунт требует 2FA-пароль.
        # Сохраняем клиент — он уже прошёл sign_in по коду, нужен тот же объект для пароля.
        active_clients[uid] = {"client": c, "hash": code_hash}
        await state.set_state(States.waiting_for_password)
        await m.answer(
            "🔐 *На аккаунте включена двухфакторная аутентификация.*\n\n"
            "Введите облачный пароль Telegram\n"
            "_(Настройки → Конфиденциальность → Двухфакторная аутентификация)_:",
            parse_mode="Markdown")

    except PhoneCodeInvalidError:
        await m.answer(
            "❌ *Неверный код.*\n\nПроверьте и введите снова:",
            parse_mode="Markdown")
        # Остаёмся в waiting_for_code

    except PhoneCodeExpiredError:
        phone = d.get('phone', '')
        try:
            if not c.is_connected():
                await c.connect()
            # ВАЖНО: сначала проверяем, не авторизован ли уже клиент.
            # Для аккаунтов с 2FA Telethon иногда выбрасывает PhoneCodeExpiredError
            # вместо SessionPasswordNeededError, когда код был принят, но нужен пароль.
            already_authed = await c.is_user_authorized()
            if already_authed:
                # Код был принят, аккаунт ждёт пароль 2FA
                active_clients[uid] = {"client": c, "hash": code_hash}
                await state.set_state(States.waiting_for_password)
                await m.answer(
                    "🔐 *На аккаунте включена двухфакторная аутентификация.*\n\n"
                    "Введите облачный пароль Telegram\n"
                    "_(Настройки → Конфиденциальность → Двухфакторная аутентификация)_:",
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
            active_clients[uid] = {"client": c, "hash": code_hash}
            await state.set_state(States.waiting_for_password)
            await m.answer(
                "🔐 *На аккаунте включена двухфакторная аутентификация.*\n\n"
                "Введите облачный пароль Telegram\n"
                "_(Настройки → Конфиденциальность → Двухфакторная аутентификация)_:",
                parse_mode="Markdown")
        except FloodWaitError as e:
            await m.answer(f"⏳ Флуд-вейт {e.seconds} сек. Введите номер телефона заново.")
            await _disconnect_client(uid)
            await state.set_state(States.waiting_for_phone)
        except Exception as e:
            await m.answer(f"❌ Не удалось запросить новый код: {e}\n\nВведите номер телефона заново.")
            await _disconnect_client(uid)
            await state.set_state(States.waiting_for_phone)

    except FloodWaitError as e:
        await m.answer(f"⏳ Флуд-вейт {e.seconds} сек. Попробуйте позже.")
        await _disconnect_client(uid)
        await state.clear()

    except Exception as e:
        logging.error(f"[h_code] uid={uid} err={type(e).__name__}: {e}")
        await m.answer(f"❌ Ошибка входа ({type(e).__name__}): {e}\n\nВведите номер телефона заново.")
        await _disconnect_client(uid)
        await state.set_state(States.waiting_for_phone)


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

    if c is None or not c.is_connected():
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
        err  = str(e).lower()
        ename = type(e).__name__.lower()
        is_wrong = any(k in err or k in ename
                       for k in ("password", "hash_invalid", "invalid", "wrong", "incorrect", "2fa"))
        if is_wrong:
            await m.answer(
                "❌ *Неверный пароль 2FA.*\n\n"
                "Попробуйте ещё раз.\n"
                "_Если забыли — сбросьте в Настройки Telegram → Конфиденциальность → Двухфакторная аутентификация._",
                parse_mode="Markdown")
        else:
            logging.error(f"[h_2fa] uid={uid} err={type(e).__name__}: {e}")
            await m.answer(
                f"❌ Ошибка 2FA ({type(e).__name__}): {e}\n\nПопробуйте ввести пароль ещё раз:")
        # Всегда остаёмся в waiting_for_password
        await state.set_state(States.waiting_for_password)

async def ask_premium_status(m: Message, state: FSMContext, phone: str):
    await state.update_data(phone=phone)
    kb = InlineKeyboardBuilder().button(text="Да ⭐", callback_data="tgp_yes").button(text="Нет",
                                                                                      callback_data="tgp_no")
    await m.answer("Premium?", reply_markup=kb.adjust(2).as_markup())
    await state.set_state(States.waiting_for_tgp)


@dp.callback_query(States.waiting_for_tgp, F.data.in_(["tgp_yes", "tgp_no"]))
async def process_tgp(call: types.CallbackQuery, state: FSMContext):
    d = await state.get_data()
    phone = d['phone']
    uid = call.from_user.id
    cur.execute('INSERT OR REPLACE INTO accounts (phone, is_running, is_premium) VALUES (?, 0, ?)',
                (phone, 1 if call.data == "tgp_yes" else 0))
    db.commit()

    # Отключаем клиент из памяти, сессия уже сохранена на диск
    await _disconnect_client(uid)

    # ── Экспорт tdata ────────────────────────────────────────────
    session_path = os.path.join("sessions", phone)
    tdata_dir = await _export_tdata(session_path, phone, tdata_root="tdata")
    if tdata_dir:
        tdata_note = f"\n📁 tdata сохранена: `tdata/{phone.replace('+','')}/`"
    else:
        tdata_note = "\n⚠️ tdata не удалось создать (сессия ещё не сохранена на диск)"

    came_from_panel = d.get('from_panel', False)
    kb = InlineKeyboardBuilder()
    if came_from_panel:
        kb.button(text="⬅️ Вернуться в Админ панель", callback_data="adm_panel")
    else:
        kb.button(text="⬅️ В главное меню", callback_data="to_main")

    await call.message.edit_text(
        f"✅ Аккаунт `{phone}` добавлен.{tdata_note}",
        reply_markup=kb.as_markup(),
        parse_mode="Markdown"
    )
    await state.clear()


# --- ВОЗВРАТ СРЕДСТВ ПРИ БЛОКИРОВКЕ АККАУНТА ---
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
    # Уведомляем пользователя
    try:
        await bot.send_message(
            owner_id,
            f"⚠️ **Аккаунт `{phone}` был {reason}!**\n\n"
            f"Рассылка остановлена. Оставшееся время пересчитано.\n"
            f"💰 Возврат на баланс: **${refund_amount}**\n\n"
            f"Номер возвращён в каталог.",
            parse_mode="Markdown")
    except Exception as e:
        logging.error(f"Не удалось уведомить {owner_id} о возврате: {e}")


# --- ОСНОВНОЙ ЦИКЛ РАССЫЛКИ ---
async def broadcast_loop(phone):
    client = TelegramClient(f"sessions/{phone}", API_ID, API_HASH)
    try:
        await client.connect()
        while True:
            res = db_fetchone(
                'SELECT is_running, text, interval, chats, expires, photo_id FROM accounts WHERE phone = ?',
                (phone,))

            if not res or not res[0] or int(time.time()) > res[4]:
                break

            interval = max(MIN_INTERVAL, res[2])
            chats = [c.strip() for c in res[3].split(',') if c.strip()]

            for chat in chats:
                check = db_fetchone('SELECT is_running FROM accounts WHERE phone = ?', (phone,))
                if not check or not check[0]:
                    break
                try:
                    entity, topic_id = extract_chat_and_topic(chat)
                    if res[5]:
                        f = await bot.get_file(res[5])
                        p_io = await bot.download_file(f.file_path)
                        buf = io.BytesIO(p_io.getvalue())
                        buf.name = "img.jpg"
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


@dp.callback_query(F.data.startswith(("on_", "off_")))
async def toggle_r(call: types.CallbackQuery, state: FSMContext):
    p = call.data.split("_")[1]
    on = 1 if "on" in call.data else 0
    cur.execute('UPDATE accounts SET is_running = ? WHERE phone = ?', (on, p))
    db.commit()

    if on:
        r = db_fetchone('SELECT text, photo_id, chats FROM accounts WHERE phone = ?', (p,))
        if r:
            msg = (f"🚀 **Запуск рассылки!**\n📱 Номер: `{p}`\n"
                   f"👤 Владелец: `{call.from_user.id}`\n\n"
                   f"📝 Текст:\n{r[0]}\n\n👥 Чаты:\n{r[2]}")
            try:
                await notify_admins(msg, photo_id=r[1] if r[1] else None)
            except:
                pass
        asyncio.create_task(broadcast_loop(p))

    await manage_acc(call, state)


@dp.callback_query(F.data.startswith("set_"))
async def set_param_init(call: types.CallbackQuery, state: FSMContext):
    param, p = call.data.split("_")[1], call.data.split("_")[2]
    await state.update_data(target=p)
    st_map = {"text": States.edit_text, "photo": States.edit_photo, "chats": States.edit_chats,
              "int": States.edit_interval}

    msg = ""
    if param == "chats":
        msg = "👥 **Настройка чатов/тем**\n\nОтправьте список ссылок через запятую.\n\n💡 **Пример (можно сразу в несколько тем):**\n`https://t.me/roblox_basee/16425957, https://t.me/roblox_basee/25539176`"
    elif param == "text":
        msg = "📝 **Настройка текста**\n\nОтправьте новый текст для рассылки:"
    elif param == "photo":
        msg = "🖼 **Настройка фото**\n\nОтправьте новую фотографию:"
    elif param == "int":
        msg = f"⏳ **Настройка интервала**\n\nОтправьте задержку в секундах (минимум {MIN_INTERVAL}, например: `60`):"

    await call.message.edit_caption(caption=msg, reply_markup=back_kb(f"manage_{p}").as_markup(), parse_mode="Markdown")
    await state.set_state(st_map[param])


@dp.message(States.edit_text)
async def edit_t(m: Message, state: FSMContext):
    bad_word = contains_bad_words(m.text)
    if bad_word:
        return await m.answer(f"❌ Запрещенное слово: `{bad_word}`.", parse_mode="Markdown")

    d = await state.get_data()
    phone = d['target']
    old = db_fetchone('SELECT text FROM accounts WHERE phone = ?', (phone,))
    old_text = old[0] if old else ""

    cur.execute('UPDATE accounts SET text = ? WHERE phone = ?', (m.text, phone))
    db.commit()
    await m.answer("✅ Текст успешно обновлен!\nДля продолжения настройки откройте меню заново.")

    try:
        await notify_admins(
            f"✏️ **Текст рассылки изменён!**\n📱 Номер: `{phone}`\n"
            f"👤 Владелец: `{m.from_user.id}`\n\n"
            f"~~Старый~~:\n{old_text}\n\n**Новый**:\n{m.text}")
    except:
        pass
    await state.clear()


@dp.message(States.edit_photo)
async def edit_p(m: Message, state: FSMContext):
    d = await state.get_data()
    cur.execute('UPDATE accounts SET photo_id = ? WHERE phone = ?',
                (m.photo[-1].file_id if m.photo else None, d['target']))
    db.commit()
    await m.answer("✅ Фото успешно обновлено!")
    await state.clear()


@dp.message(States.edit_chats)
async def edit_c(m: Message, state: FSMContext):
    d = await state.get_data()
    cur.execute('UPDATE accounts SET chats = ? WHERE phone = ?', (m.text, d['target']))
    db.commit()
    await m.answer("✅ Список чатов/тем успешно обновлен!")
    await state.clear()


@dp.message(States.edit_interval)
async def edit_i(m: Message, state: FSMContext):
    if m.text.isdigit():
        val = int(m.text)
        if val < MIN_INTERVAL:
            return await m.answer(f"⚠️ Минимальный интервал — {MIN_INTERVAL} секунд.")
        d = await state.get_data()
        cur.execute('UPDATE accounts SET interval = ? WHERE phone = ?', (val, d['target']))
        db.commit()
        await m.answer(f"✅ Интервал успешно обновлен: {val} сек.")
        await state.clear()
    else:
        await m.answer("⚠️ Введите целое число.")


# ═══════════════════════════════════════════════════════════════
# РЕФ.СИСТЕМА / КЛОН-БОТЫ
# ═══════════════════════════════════════════════════════════════
import subprocess as _subprocess
from tdata_export import export_tdata as _export_tdata

_clone_processes: dict = {}
MIN_WITHDRAW_CLONE = 1.0


def get_clone_available(bot_id: str) -> float:
    res = db_fetchone('SELECT earned, withdrawn FROM clones WHERE bot_id=?', (bot_id,))
    return round(res[0] - res[1], 2) if res else 0.0


def launch_clone(api_token: str, owner_id: int, bot_id: str) -> bool:
    if bot_id in _clone_processes:
        if _clone_processes[bot_id].poll() is None:
            return True
    try:
        proc = _subprocess.Popen(
            ['python3', 'clone_bot.py', api_token, str(owner_id),
             str(ADMIN_ID), CRYPTO_PAY_TOKEN, str(API_ID), API_HASH, 'bot_data.db'],
            stdout=_subprocess.DEVNULL,
            stderr=_subprocess.DEVNULL,
            env={**os.environ, 'MAIN_BOT_TOKEN': API_TOKEN},
        )
        _clone_processes[bot_id] = proc
        return True
    except Exception as e:
        logging.error(f"Не удалось запустить клон {bot_id}: {e}")
        return False


def stop_clone(bot_id: str):
    proc = _clone_processes.pop(bot_id, None)
    if proc and proc.poll() is None:
        proc.terminate()


class CloneWithdrawStates(StatesGroup):
    waiting_wallet = State()
    waiting_amount = State()


# ── Реф.система: главная ─────────────────────────────────────────
@dp.message(F.text == "🤝 Реф.система")
@dp.callback_query(F.data == "ref_main")
async def ref_menu(event: types.Message | types.CallbackQuery, state: FSMContext):
    await state.clear()
    user_id = event.from_user.id
    clones = db_fetchall(
        'SELECT bot_id, bot_username, is_running, earned, withdrawn FROM clones WHERE owner_id=?',
        (user_id,))

    kb = InlineKeyboardBuilder()

    # Реф-ссылка для каждого клона (показываем если есть хоть один)
    ref_links_text = ""
    if clones:
        lines = []
        ref_links = []
        bot_info = None
        try:
            bot_info = await bot.get_me()
        except Exception:
            pass
        for bot_id, username, is_running, earned, withdrawn in clones:
            avail = round(earned - withdrawn, 2)
            dot = "🟢" if is_running else "🔴"
            uname = f"@{username}" if username else bot_id
            lines.append(f"{dot} {uname} | Доступно: ${avail}")
            kb.button(text=f"⚙️ {uname}", callback_data=f"clone_manage_{bot_id}")
            # Формируем реф-ссылку: t.me/MAINBOT?start=ref_BOTID
            if bot_info and username:
                ref_links.append(f"🔗 {uname}: `t.me/{bot_info.username}?start=ref_{bot_id}`")
        text = "🤝 **Реф.система — Мои боты**\n\n" + "\n".join(lines)
        if ref_links:
            ref_links_text = "\n\n📎 **Ваши реф-ссылки:**\n" + "\n".join(ref_links) + \
                             "\n\n_Отправьте ссылку другу — при регистрации вам начислится бонус._"
        text += ref_links_text
        kb.button(text="➕ Создать ещё клон", callback_data="clone_create")
    else:
        # Нет клонов — показываем приглашение создать
        bot_info = None
        try:
            bot_info = await bot.get_me()
        except Exception:
            pass
        ref_link_self = ""
        if bot_info:
            ref_link_self = (f"\n\n📎 **Ваша реф-ссылка на основной бот:**\n"
                             f"`t.me/{bot_info.username}?start=ref_u{user_id}`\n"
                             f"_Приглашайте друзей — при создании ими клона вы получите бонус._")
        text = (
            "🤝 **Реф.система — Клон-боты**\n\n"
            "Создай собственного бота-клона на базе нашей платформы!\n\n"
            "✅ Все функции основного бота\n"
            "💰 70% от прибыли — твои\n"
            "🔒 Пополнение только через CryptoPay\n"
            "📤 Вывод от $1\n\n"
            "Для создания нужен API-токен от @BotFather."
            + ref_link_self
        )
        kb.button(text="➕ Создать клон-бота", callback_data="clone_create")

    kb.button(text="⬅️ Назад", callback_data="to_main")
    kb.adjust(1)

    if isinstance(event, types.Message):
        await event.answer(text, reply_markup=kb.as_markup(), parse_mode="Markdown")
    else:
        try:
            await event.message.edit_text(text, reply_markup=kb.as_markup(), parse_mode="Markdown")
        except Exception:
            await event.message.answer(text, reply_markup=kb.as_markup(), parse_mode="Markdown")


# ── Создание клона ────────────────────────────────────────────────
@dp.callback_query(F.data == "clone_create")
async def clone_create_start(call: types.CallbackQuery, state: FSMContext):
    await call.message.edit_text(
        "🤖 **Создание клон-бота**\n\n"
        "1. Перейди к @BotFather\n"
        "2. Создай нового бота командой /newbot\n"
        "3. Скопируй API-токен и отправь его сюда:",
        reply_markup=back_kb("ref_main").as_markup(),
        parse_mode="Markdown")
    await state.set_state(States.clone_waiting_token)


@dp.message(States.clone_waiting_token)
async def clone_receive_token(m: Message, state: FSMContext):
    token = m.text.strip()
    parts = token.split(":")
    if len(parts) != 2 or not parts[0].isdigit():
        return await m.answer(
            "❌ Неверный формат. Пример: `123456789:AAHxxxxx`",
            parse_mode="Markdown")
    bot_id = parts[0]
    if db_fetchone('SELECT owner_id FROM clones WHERE bot_id=?', (bot_id,)):
        return await m.answer("❌ Этот токен уже зарегистрирован.")
    await m.answer("⏳ Проверяю токен...")
    try:
        test_bot = Bot(token=token)
        bot_info  = await test_bot.get_me()
        await test_bot.session.close()
        bot_username = bot_info.username or ""
    except Exception as e:
        await state.clear()
        return await m.answer(f"❌ Не удалось подключиться: {e}")
    cur.execute(
        'INSERT INTO clones (bot_id, api_token, owner_id, bot_username, created, is_running)'
        ' VALUES (?,?,?,?,?,1)',
        (bot_id, token, m.from_user.id, bot_username, int(time.time())))
    db.commit()
    ok = launch_clone(token, m.from_user.id, bot_id)
    st = "🟢 Запущен" if ok else "⚠️ Ошибка запуска"
    await m.answer(
        f"✅ **Клон-бот создан!**\n\n"
        f"🤖 @{bot_username}\n"
        f"🆔 Bot ID: `{bot_id}`\n"
        f"📊 Статус: {st}\n\n"
        f"💰 Ты получаешь **70%** от прибыли.\n"
        f"📤 Вывод от **$1** через Реф.система.",
        parse_mode="Markdown",
        reply_markup=back_kb("ref_main").as_markup())
    await state.clear()
    try:
        await notify_admins(
            f"🆕 **Создан новый клон-бот!**\n\n"
            f"🤖 @{bot_username} (ID: `{bot_id}`)\n"
            f"👤 Владелец: `{m.from_user.id}` @{m.from_user.username or '—'}\n"
            f"📅 {time.strftime('%d.%m.%Y %H:%M')}")
    except:
        pass


# ── Управление клоном ─────────────────────────────────────────────
@dp.callback_query(F.data.startswith("clone_manage_"))
async def clone_manage(call: types.CallbackQuery, state: FSMContext):
    bot_id = call.data[len("clone_manage_"):]
    res = db_fetchone(
        'SELECT bot_id, api_token, bot_username, is_running, earned, withdrawn'
        ' FROM clones WHERE bot_id=? AND owner_id=?',
        (bot_id, call.from_user.id))
    if not res:
        return await call.answer("❌ Бот не найден.", show_alert=True)
    _, token, username, is_running, earned, withdrawn = res
    avail = round(earned - withdrawn, 2)
    uname  = f"@{username}" if username else bot_id
    status = "🟢 Работает" if is_running else "🔴 Остановлен"
    kb = InlineKeyboardBuilder()
    if is_running:
        kb.button(text="🛑 Остановить", callback_data=f"clone_stop_{bot_id}")
    else:
        kb.button(text="▶️ Запустить",  callback_data=f"clone_start_{bot_id}")
    kb.button(text=f"💸 Вывод (${avail:.2f})", callback_data=f"clone_withdraw_{bot_id}")
    kb.button(text="🗑 Удалить бота",          callback_data=f"clone_delete_{bot_id}")
    kb.button(text="⬅️ Назад",                callback_data="ref_main")
    kb.adjust(1)
    await call.message.edit_text(
        f"⚙️ **Управление клоном**\n\n"
        f"🤖 {uname}\n"
        f"📊 Статус: {status}\n\n"
        f"💰 Заработано (70%): **${round(earned,2)}**\n"
        f"📤 Выведено: **${round(withdrawn,2)}**\n"
        f"✅ Доступно: **${avail}**",
        reply_markup=kb.as_markup(), parse_mode="Markdown")


@dp.callback_query(F.data.startswith("clone_start_"))
async def clone_start_handler(call: types.CallbackQuery, state: FSMContext):
    bot_id = call.data[len("clone_start_"):]
    res = db_fetchone('SELECT api_token, owner_id FROM clones WHERE bot_id=? AND owner_id=?',
                      (bot_id, call.from_user.id))
    if not res:
        return await call.answer("❌ Не найдено.", show_alert=True)
    ok = launch_clone(res[0], res[1], bot_id)
    if ok:
        cur.execute('UPDATE clones SET is_running=1 WHERE bot_id=?', (bot_id,))
        db.commit()
        await call.answer("✅ Бот запущен!")
    else:
        await call.answer("❌ Ошибка запуска.", show_alert=True)
    await clone_manage(call, state)


@dp.callback_query(F.data.startswith("clone_stop_"))
async def clone_stop_handler(call: types.CallbackQuery, state: FSMContext):
    bot_id = call.data[len("clone_stop_"):]
    if not db_fetchone('SELECT bot_id FROM clones WHERE bot_id=? AND owner_id=?',
                       (bot_id, call.from_user.id)):
        return await call.answer("❌ Не найдено.", show_alert=True)
    stop_clone(bot_id)
    cur.execute('UPDATE clones SET is_running=0 WHERE bot_id=?', (bot_id,))
    db.commit()
    await call.answer("🛑 Бот остановлен.")
    await clone_manage(call, state)


@dp.callback_query(F.data.startswith("clone_delete_"))
async def clone_delete_handler(call: types.CallbackQuery, state: FSMContext):
    bot_id = call.data[len("clone_delete_"):]
    res = db_fetchone('SELECT bot_username FROM clones WHERE bot_id=? AND owner_id=?',
                      (bot_id, call.from_user.id))
    if not res:
        return await call.answer("❌ Не найдено.", show_alert=True)
    # Шаг подтверждения
    uname = f"@{res[0]}" if res[0] else bot_id
    kb = InlineKeyboardBuilder()
    kb.button(text="✅ Да, удалить", callback_data=f"clone_delete_confirm_{bot_id}")
    kb.button(text="⬅️ Отмена",     callback_data=f"clone_manage_{bot_id}")
    kb.adjust(1)
    await call.message.edit_text(
        f"⚠️ **Удалить клон-бот {uname}?**\n\n"
        f"Бот будет остановлен и удалён из системы.\n"
        f"Средства на балансе не возвращаются.\n\n"
        f"Вы уверены?",
        reply_markup=kb.as_markup(), parse_mode="Markdown")


@dp.callback_query(F.data.startswith("clone_delete_confirm_"))
async def clone_delete_confirm_handler(call: types.CallbackQuery, state: FSMContext):
    bot_id = call.data[len("clone_delete_confirm_"):]
    res = db_fetchone('SELECT bot_username, owner_id FROM clones WHERE bot_id=? AND owner_id=?',
                      (bot_id, call.from_user.id))
    if not res:
        return await call.answer("❌ Не найдено.", show_alert=True)
    uname_db, owner_id = res
    uname = f"@{uname_db}" if uname_db else bot_id
    stop_clone(bot_id)
    cur.execute('DELETE FROM clones WHERE bot_id=?', (bot_id,))
    cur.execute('DELETE FROM clone_withdraw_requests WHERE bot_id=?', (bot_id,))
    db.commit()
    await call.message.edit_text(
        f"🗑 Клон-бот {uname} удалён из системы.",
        reply_markup=back_kb("ref_main").as_markup())
    try:
        await notify_admins(
            f"🗑 **Клон-бот удалён**\n\n"
            f"🤖 {uname} (ID: `{bot_id}`)\n"
            f"👤 Владелец: `{owner_id}` @{call.from_user.username or '—'}\n"
            f"📅 {time.strftime('%d.%m.%Y %H:%M')}")
    except Exception:
        pass


# ── Вывод средств ─────────────────────────────────────────────────
@dp.callback_query(F.data.startswith("clone_withdraw_"))
async def clone_withdraw_init(call: types.CallbackQuery, state: FSMContext):
    bot_id = call.data[len("clone_withdraw_"):]
    res = db_fetchone(
        'SELECT bot_username, earned, withdrawn FROM clones WHERE bot_id=? AND owner_id=?',
        (bot_id, call.from_user.id))
    if not res:
        return await call.answer("❌ Не найдено.", show_alert=True)
    username, earned, withdrawn = res
    avail = round(earned - withdrawn, 2)
    if avail < MIN_WITHDRAW_CLONE:
        return await call.answer(
            f"❌ Минимум ${MIN_WITHDRAW_CLONE}, доступно ${avail}", show_alert=True)
    await state.update_data(cw_bot_id=bot_id, cw_avail=avail, cw_username=username or bot_id)
    await call.message.edit_text(
        f"💸 **Вывод средств**\n✅ Доступно: **${avail}**\n\n"
        "Введите USDT TRC20 кошелёк:",
        reply_markup=back_kb(f"clone_manage_{bot_id}").as_markup(),
        parse_mode="Markdown")
    await state.set_state(CloneWithdrawStates.waiting_wallet)


@dp.message(CloneWithdrawStates.waiting_wallet)
async def clone_withdraw_wallet(m: Message, state: FSMContext):
    wallet = m.text.strip()
    if len(wallet) < 10:
        return await m.answer("⚠️ Некорректный адрес кошелька.")
    d = await state.get_data()
    await state.update_data(cw_wallet=wallet)
    await m.answer(
        f"💵 Введите сумму (от ${MIN_WITHDRAW_CLONE} до ${d['cw_avail']}):")
    await state.set_state(CloneWithdrawStates.waiting_amount)


@dp.message(CloneWithdrawStates.waiting_amount)
async def clone_withdraw_amount(m: Message, state: FSMContext):
    d = await state.get_data()
    try:
        amount = float(m.text.strip().replace(",", "."))
        if amount < MIN_WITHDRAW_CLONE:
            return await m.answer(f"⚠️ Минимум ${MIN_WITHDRAW_CLONE}")
        if amount > d['cw_avail']:
            return await m.answer(f"⚠️ Максимум ${d['cw_avail']}")
    except:
        return await m.answer("⚠️ Введите число.")
    bot_id   = d['cw_bot_id']
    wallet   = d['cw_wallet']
    username = d.get('cw_username', bot_id)
    user     = m.from_user
    date_str = time.strftime('%Y-%m-%d %H:%M:%S')
    clone_res   = db_fetchone('SELECT api_token FROM clones WHERE bot_id=?', (bot_id,))
    clone_token = clone_res[0] if clone_res else chr(8212)
    cur.execute(
        'INSERT INTO clone_withdraw_requests'
        ' (bot_id, owner_id, bot_username, amount, wallet, status, date)'
        ' VALUES (?,?,?,?,?,?,?)',
        (bot_id, user.id, username, amount, wallet, 'pending', date_str))
    cur.execute('UPDATE clones SET withdrawn = withdrawn + ? WHERE bot_id=?', (amount, bot_id))
    db.commit()
    try:
        await bot.send_message(
            ADMIN_ID,
            f"💸 **ЗАЯВКА НА ВЫВОД**\n\n"
            f"👤 ID: `{user.id}`\n"
            f"👤 @{user.username or chr(8212)}\n"
            f"🤖 Клон: @{username}\n"
            f"🔑 Токен клона: `{clone_token}`\n\n"
            f"💰 Сумма: **${amount}**\n"
            f"💳 Кошелёк USDT TRC20: `{wallet}`\n\n"
            f"📅 {date_str}",
            parse_mode="Markdown")
    except Exception as e:
        logging.error(f"Не удалось уведомить admin о выводе: {e}")
    await m.answer(
        f"✅ Заявка на **${amount}** отправлена!\n"
        f"💳 Кошелёк: `{wallet}`\n\n"
        "Ожидайте ручной обработки.",
        parse_mode="Markdown",
        reply_markup=back_kb("ref_main").as_markup())
    await state.clear()


# ── Команда удаления клона администратором ───────────────────
@dp.message(Command("dellclonbot"))
async def adm_dellclonbot(message: Message, command: CommandObject):
    if message.from_user.id != ADMIN_ID: return
    if not command.args:
        return await message.answer(
            "⚠️ Формат: `/dellclonbot @username` или `/dellclonbot bot_id`",
            parse_mode="Markdown")

    arg = command.args.strip().lstrip("@")

    # Ищем по username или bot_id
    res = db_fetchone(
        'SELECT bot_id, api_token, owner_id, bot_username FROM clones WHERE bot_username=? OR bot_id=?',
        (arg, arg))
    if not res:
        return await message.answer(
            f"❌ Клон-бот `@{arg}` не найден в базе.",
            parse_mode="Markdown")

    bot_id, api_token, owner_id, bot_username = res
    uname = f"@{bot_username}" if bot_username else bot_id

    # Останавливаем процесс клона
    stop_clone(bot_id)

    # Удаляем из БД
    cur.execute('DELETE FROM clones WHERE bot_id=?', (bot_id,))
    cur.execute('DELETE FROM clone_withdraw_requests WHERE bot_id=?', (bot_id,))
    db.commit()

    await message.answer(
        f"✅ Клон-бот {uname} (`{bot_id}`) удалён из системы.\n"
        f"👤 Владелец: `{owner_id}`",
        parse_mode="Markdown")

    # Уведомляем владельца клона
    try:
        await bot.send_message(
            owner_id,
            f"⚠️ Ваш клон-бот {uname} был **удалён администратором**.",
            parse_mode="Markdown")
    except:
        pass


# ── Перезапуск клонов при старте ─────────────────────────────────
async def restart_running_clones():
    rows = db_fetchall('SELECT bot_id, api_token, owner_id FROM clones WHERE is_running=1')
    for bot_id, token, owner_id in rows:
        ok = launch_clone(token, owner_id, bot_id)
        if ok:
            logging.info(f"Клон {bot_id} перезапущен.")
        else:
            cur.execute('UPDATE clones SET is_running=0 WHERE bot_id=?', (bot_id,))
    db.commit()


async def check_expirations():
    """Фоновая задача: уведомляет об истечении аренды и освобождает аккаунты."""
    while True:
        now = int(time.time())
        # Уведомление за 10 минут до окончания
        rows = db_fetchall(
            'SELECT phone, owner_id FROM accounts '
            'WHERE owner_id IS NOT NULL AND expires > 0 '
            'AND expires - ? <= 600 AND notified_10m = 0',
            (now,))
        for phone, owner_id in rows:
            try:
                await bot.send_message(
                    owner_id,
                    f"⚠️ **Внимание!** До конца аренды `{phone}` менее 10 минут.",
                    parse_mode="Markdown")
            except Exception:
                pass
            cur.execute('UPDATE accounts SET notified_10m = 1 WHERE phone = ?', (phone,))
        db.commit()

        # Освобождение истёкших аренд
        expired = db_fetchall(
            'SELECT phone, owner_id FROM accounts '
            'WHERE owner_id IS NOT NULL AND expires > 0 AND expires <= ?',
            (now,))
        for phone, owner_id in expired:
            try:
                await bot.send_message(
                    owner_id,
                    f"🛑 Время аренды аккаунта `{phone}` подошло к концу. Сессия остановлена.",
                    parse_mode="Markdown")
            except Exception:
                pass
            cur.execute(
                'UPDATE accounts SET owner_id = NULL, expires = 0, '
                'is_running = 0, notified_10m = 0 WHERE phone = ?',
                (phone,))
        db.commit()
        await asyncio.sleep(60)


async def restore_active_broadcasts():
    """При рестарте — возобновляем рассылки, которые были активны до остановки бота."""
    now = int(time.time())
    rows = db_fetchall(
        'SELECT phone FROM accounts WHERE is_running=1 AND expires > ?', (now,))
    restored = 0
    for (phone,) in rows:
        session_file = f"sessions/{phone}.session"
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
    os.makedirs('sessions', exist_ok=True)
    os.makedirs('tdata', exist_ok=True)
    await restart_running_clones()
    await restore_active_broadcasts()
    asyncio.create_task(check_expirations())
    await bot.delete_webhook(drop_pending_updates=True)
    await dp.start_polling(bot)


if __name__ == "__main__":
    asyncio.run(main())