"""
tdata_export.py — экспорт Telethon-сессии в папку tdata/<phone>/

После авторизации через Telethon вызовите:
    await export_tdata(session_path, phone, tdata_root="tdata")

Что создаётся в tdata/<phone>/:
  session_info.json   — auth_key, dc_id, server_address, port, phone, дата
  auth_key.bin        — сырой 256-байтный ключ авторизации
  README.txt          — инструкция по использованию

Данные читаются напрямую из SQLite-файла сессии Telethon (.session).
"""

import os
import json
import sqlite3
import asyncio
import logging
import time
from datetime import datetime

logger = logging.getLogger(__name__)

# DC → (адрес, порт)
DC_MAP = {
    1: ("149.154.175.53",  443),
    2: ("149.154.167.51",  443),
    3: ("149.154.175.100", 443),
    4: ("149.154.167.91",  443),
    5: ("91.108.56.130",   443),
}


def _read_session_sqlite(session_path: str) -> dict | None:
    """
    Читает данные из .session файла Telethon (SQLite).
    Возвращает словарь с ключами: dc_id, server_address, port, auth_key, phone.
    """
    # Telethon добавляет .session сам; проверим оба варианта
    for path in (session_path, session_path + ".session"):
        if os.path.exists(path):
            break
    else:
        logger.warning(f"[tdata_export] Session file not found: {session_path}")
        return None

    try:
        conn = sqlite3.connect(f"file:{path}?mode=ro", uri=True, check_same_thread=False)
        conn.execute("PRAGMA busy_timeout=3000")
        cur = conn.cursor()

        # Таблица sessions в Telethon SQLite
        cur.execute("SELECT dc_id, server_address, port, auth_key FROM sessions LIMIT 1")
        row = cur.fetchone()
        if not row:
            conn.close()
            return None

        dc_id, server_address, port, auth_key_blob = row

        # Попытка получить телефон из таблицы entities или version
        phone = None
        try:
            cur.execute("SELECT phone FROM version LIMIT 1")
            r = cur.fetchone()
            if r:
                phone = r[0]
        except Exception:
            pass

        if phone is None:
            try:
                cur.execute("SELECT id FROM entities WHERE phone IS NOT NULL LIMIT 1")
                r = cur.fetchone()
                if r:
                    phone = r[0]
            except Exception:
                pass

        conn.close()

        return {
            "dc_id": dc_id,
            "server_address": server_address or DC_MAP.get(dc_id, ("",))[0],
            "port": port or 443,
            "auth_key": auth_key_blob,   # bytes
            "phone": phone,
        }

    except Exception as e:
        logger.error(f"[tdata_export] Cannot read session {path}: {e}")
        return None


async def export_tdata(
    session_path: str,
    phone: str,
    tdata_root: str = "tdata",
) -> str | None:
    """
    Экспортирует Telethon-сессию в папку tdata/<phone_clean>/.

    :param session_path: путь к .session файлу Telethon (без расширения или с ним)
    :param phone:        номер телефона (+7...)
    :param tdata_root:   корневая папка для tdata (по умолчанию "tdata")
    :return:             путь к созданной папке или None при ошибке
    """
    loop = asyncio.get_event_loop()
    data = await loop.run_in_executor(None, _read_session_sqlite, session_path)
    if not data:
        return None

    # Нормализуем телефон для имени папки: убираем +, пробелы, тире
    phone_clean = phone.replace("+", "").replace(" ", "").replace("-", "")
    out_dir = os.path.join(tdata_root, phone_clean)
    os.makedirs(out_dir, exist_ok=True)

    auth_key_bytes: bytes = data["auth_key"] if isinstance(data["auth_key"], bytes) else bytes(data["auth_key"])

    # 1. Сохраняем auth_key как бинарный файл
    key_path = os.path.join(out_dir, "auth_key.bin")
    with open(key_path, "wb") as f:
        f.write(auth_key_bytes)

    # 2. Сохраняем полную информацию о сессии в JSON
    dc_id = data["dc_id"]
    server_address = data["server_address"] or DC_MAP.get(dc_id, ("unknown",))[0]
    port = data["port"] or 443

    session_info = {
        "phone": phone,
        "dc_id": dc_id,
        "server_address": server_address,
        "port": port,
        "auth_key_hex": auth_key_bytes.hex(),
        "auth_key_length": len(auth_key_bytes),
        "exported_at": datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S UTC"),
        "session_file": os.path.abspath(session_path if session_path.endswith(".session") else session_path + ".session"),
    }
    info_path = os.path.join(out_dir, "session_info.json")
    with open(info_path, "w", encoding="utf-8") as f:
        json.dump(session_info, f, ensure_ascii=False, indent=2)

    # 3. README с инструкцией
    readme_path = os.path.join(out_dir, "README.txt")
    with open(readme_path, "w", encoding="utf-8") as f:
        f.write(f"""Данные сессии Telegram-аккаунта
================================
Телефон : {phone}
DC ID   : {dc_id}
Сервер  : {server_address}:{port}
Экспорт : {session_info['exported_at']}

Файлы в этой папке:
  auth_key.bin      — 256-байтный ключ авторизации (бинарный)
  session_info.json — все параметры сессии в JSON-формате
  README.txt        — этот файл

Как использовать session_info.json вручную:
  auth_key_hex — ключ в hex-формате (256 байт = 512 символов)
  dc_id        — номер дата-центра Telegram (1–5)
  server_address / port — адрес и порт сервера

Оригинальный .session файл Telethon:
  {session_info['session_file']}
""")

    logger.info(f"[tdata_export] Exported session for {phone} → {out_dir}")
    return out_dir
