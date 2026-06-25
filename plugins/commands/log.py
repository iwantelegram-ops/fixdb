"""
plugins/commands/log.py
────────────────────────
Logging ke channel owner:
  - Notif saat bot masuk grup baru
  - /list (owner DM) → lihat semua grup aktif
  - Log deteksi alasan pesan dihapus (group=3)

Desain log SERAGAM: semua pakai header ❖ JUDUL ❖ + blockquote isi.
Tiap jenis pelanggaran punya detail alasan spesifik (bukan generic).
"""

import os
import re
import time
import html
import hashlib
import asyncio
from datetime import datetime
from pyrogram import Client, filters
from pyrogram.types import Message
from pyrogram.enums import ParseMode, MessageEntityType
from pyrogram.errors import PeerIdInvalid, ChannelInvalid, ChatIdInvalid, FloodWait

from database import (
    config_db, get_config, is_admin, regex_db, messages_db, db,
    GLOBAL_EXPIRY, TZ_WIB,
    set_global_flood_backoff, wait_global_flood_backoff,
)
from core.regex_utils import remove_mentions_for_regex, match_with_leet
from plugins.nexus.engine import pipeline_pembersihan

OWNER_ID    = int(os.environ.get("OWNER_ID", 0))
LOG_CHANNEL = int(os.environ.get("LOG_CHANNEL", 0))

free_col            = db["free_per_group"]
group_regex_db      = db["regex_per_group"]
_log_local_regex_cache: dict[int, tuple[list, float]] = {}

_log_channel_valid: bool | None = None
_log_channel_fail_ts: float = 0.0
_LOG_CHANNEL_RETRY_INTERVAL = 300  # 5 menit sebelum retry setelah gagal

# ── BATCHING LOG QUEUE ────────────────────────────────────────────────────────
LOG_FLUSH_INTERVAL = int(os.environ.get("LOG_FLUSH_INTERVAL", 8))
LOG_MAX_CHARS       = 3500
LOG_MAX_QUEUE       = 500

_log_queue: list[str] = []
_log_queue_lock = asyncio.Lock()
_log_dropped_count = 0


async def _enqueue_log(text: str) -> None:
    global _log_dropped_count
    if not LOG_CHANNEL:
        return
    async with _log_queue_lock:
        if len(_log_queue) >= LOG_MAX_QUEUE:
            _log_dropped_count += 1
            return
        _log_queue.append(text)


async def _send_log(client: Client, text: str) -> bool:
    await _enqueue_log(text)
    return True


async def _flush_log_queue_once(client: Client) -> None:
    global _log_channel_valid, _log_channel_fail_ts, _log_dropped_count
    if not LOG_CHANNEL:
        return

    await wait_global_flood_backoff()

    async with _log_queue_lock:
        if not _log_queue and _log_dropped_count == 0:
            return
        pending = _log_queue.copy()
        _log_queue.clear()
        dropped = _log_dropped_count
        _log_dropped_count = 0

    if _log_channel_valid is False:
        if time.time() - _log_channel_fail_ts >= _LOG_CHANNEL_RETRY_INTERVAL:
            _log_channel_valid = None
        else:
            async with _log_queue_lock:
                _log_queue[0:0] = pending
                _log_dropped_count += dropped
            return

    if dropped:
        pending.append(
            f"⚠️ <b>{dropped} entri log dibuang</b> (antrian penuh saat flood tinggi)."
        )

    batches: list[str] = []
    current = ""
    sep = "\n\n— — —\n\n"
    for entry in pending:
        candidate = (current + sep + entry) if current else entry
        if len(candidate) > LOG_MAX_CHARS and current:
            batches.append(current)
            current = entry
        else:
            current = candidate
    if current:
        batches.append(current)

    not_sent: list[str] = []
    for i, batch_text in enumerate(batches):
        if i > 0:
            await asyncio.sleep(0.5)
        try:
            await client.send_message(
                LOG_CHANNEL, batch_text,
                parse_mode=ParseMode.HTML,
                disable_web_page_preview=True,
            )
            _log_channel_valid = True
        except (PeerIdInvalid, ChannelInvalid, ChatIdInvalid) as e:
            if _log_channel_valid is None:
                print(f"[LOG] LOG_CHANNEL tidak valid ({LOG_CHANNEL}): {e}. "
                      f"Akan retry dalam {_LOG_CHANNEL_RETRY_INTERVAL//60} menit.")
            _log_channel_valid = False
            _log_channel_fail_ts = time.time()
            not_sent.extend(batches[i:])
            break
        except FloodWait as e:
            print(f"[LOG] FloodWait {e.value}s — batch ditunda, dikembalikan ke antrian.")
            set_global_flood_backoff(e.value)
            not_sent.extend(batches[i:])
            break
        except Exception as e:
            print(f"[LOG ERROR] {e}")
            not_sent.extend(batches[i:])
            break

    if not_sent:
        async with _log_queue_lock:
            _log_queue[0:0] = not_sent


async def log_flush_worker_loop(client: Client) -> None:
    while True:
        try:
            await _flush_log_queue_once(client)
        except Exception as e:
            print(f"[LOG FLUSH WORKER ERROR] {e}")
        await asyncio.sleep(LOG_FLUSH_INTERVAL)


async def _get_local_patterns_log(chat_id: int):
    now = time.monotonic()
    hit = _log_local_regex_cache.get(chat_id)
    if hit and (now - hit[1]) < 300:
        return hit[0]
    patterns = []
    async for doc in group_regex_db.find({"chat_id": chat_id}):
        try:
            patterns.append((re.compile(doc["pattern"], re.IGNORECASE), doc.get("raw", doc["pattern"])))
        except Exception:
            pass
    _log_local_regex_cache[chat_id] = (patterns, now)
    return patterns


# ── Helper: format waktu ──────────────────────────────────────────────────────
def _fmt_waktu() -> str:
    return datetime.now(TZ_WIB).strftime("%d/%m/%Y %H:%M:%S WIB")


# ── Helper: baris user ────────────────────────────────────────────────────────
def _user_line(uid: int, name: str) -> str:
    safe_name = html.escape(name or str(uid))
    return f"<a href='tg://user?id={uid}'>{safe_name}</a> (<code>{uid}</code>)"


# ── LOG 1: Bot masuk grup baru ────────────────────────────────────────────────
@Client.on_message(filters.service, group=10)
async def log_new_group(client: Client, message: Message):
    if not message.new_chat_members or not LOG_CHANNEL:
        return
    me = client.me
    for member in message.new_chat_members:
        if member.id == me.id:
            chat  = message.chat
            text  = (
                "<b>❖ SISTEM — NODE BARU ❖</b>\n"
                "<blockquote>"
                "➕ Bot bergabung ke grup baru\n"
                f"◈ <b>Grup:</b> {html.escape(chat.title)}\n"
                f"◈ <b>ID:</b> <code>{chat.id}</code>\n"
                f"◈ <b>Username:</b> @{chat.username if chat.username else '—'}\n"
                f"◈ <b>Waktu:</b> {_fmt_waktu()}\n"
                "<i>Firewall aktif pada grup ini.</i>"
                "</blockquote>"
            )
            await _send_log(client, text)


# ── LOG 2: /list — daftar semua grup ─────────────────────────────────────────
@Client.on_message(filters.command("list") & filters.private & filters.user(OWNER_ID))
async def list_grup_pengguna(client: Client, message: Message):
    msg = await message.reply("⏳ <i>Menarik data node grup dari server...</i>", parse_mode=ParseMode.HTML)
    grup_list = []
    grup_terhapus_count = 0

    async for doc in config_db.find({}):
        chat_id = doc.get("chat_id")
        if not chat_id:
            continue
        try:
            chat = await client.get_chat(chat_id)
            username = f"@{chat.username}" if chat.username else "—"
            grup_list.append(
                f"◈ <b>{html.escape(chat.title)}</b>\n"
                f"   └ ID: <code>{chat_id}</code> | Link: {html.escape(username)}"
            )
        except Exception:
            await config_db.delete_one({"chat_id": chat_id})
            grup_terhapus_count += 1

    if not grup_list:
        text = "<b>❖ NODE INDEX ❖</b>\n\n📭 <b>Sistem tidak mendeteksi koneksi grup aktif.</b>"
        if grup_terhapus_count:
            text += f"\n\n♻️ <i>Garbage collection: <b>{grup_terhapus_count} node mati</b> dibersihkan.</i>"
        await msg.edit(text, parse_mode=ParseMode.HTML)
        return

    header = (
        "<b>❖ NODE INDEX ❖</b>\n\n"
        f"⚡ <b>Total Grup Dilindungi:</b> <code>{len(grup_list)}</code>\n"
    )
    if grup_terhapus_count:
        header += f"♻️ <i>Garbage collection: <b>{grup_terhapus_count} node mati</b> dibersihkan.</i>\n"
    header += "\n<b>▰▰▰ DAFTAR GRUP AKTIF ▰▰▰</b>\n\n"

    chunks, current_chunk = [], header
    for g in grup_list:
        if len(current_chunk) + len(g) + 2 > 3900:
            chunks.append(current_chunk)
            current_chunk = "<b>📋 LANJUTAN DAFTAR GRUP:</b>\n\n"
        current_chunk += g + "\n\n"
    if current_chunk:
        chunks.append(current_chunk)

    await msg.edit(chunks[0], parse_mode=ParseMode.HTML)
    for extra in chunks[1:]:
        await message.reply(extra, parse_mode=ParseMode.HTML)


# ── LOG 3: Log alasan pesan dihapus (group=3) ────────────────────────────────
@Client.on_message(filters.group & ~filters.service, group=3)
async def log_deletion_trigger(client: Client, message: Message):
    if not message.from_user or not LOG_CHANNEL:
        return

    cid = message.chat.id
    uid = message.from_user.id

    if await is_admin(client, cid, uid):
        return

    if await free_col.find_one({"user_id": uid, "chat_id": cid}):
        return

    content = (message.text or message.caption or "").strip()
    if not content or content.startswith("/"):
        return

    cfg    = await get_config(cid)
    alasan = None
    detail = ""
    now_ts = time.time()
    regex_safe       = remove_mentions_for_regex(message)
    teks_super_clean = pipeline_pembersihan(content)

    # Regex global (Owner Regex)
    async for doc in regex_db.find({}):
        pat_str = doc.get("pattern") or doc.get("pola")
        if not pat_str:
            continue
        try:
            pat = re.compile(pat_str, re.IGNORECASE)
        except Exception:
            continue
        if match_with_leet(pat, regex_safe) or (teks_super_clean and pat.search(teks_super_clean)):
            raw_tag = html.escape(str(doc.get("raw", pat.pattern)))
            alasan  = "Filter Regex Global"
            detail  = (
                f"◈ <b>Pola cocok:</b> <code>{raw_tag}</code>\n"
                f"◈ <b>Keterangan:</b> Kata kunci dalam daftar filter owner"
            )
            break

    # Regex lokal (Group Filter)
    if not alasan:
        for pat, raw_pattern in await _get_local_patterns_log(cid):
            if match_with_leet(pat, regex_safe):
                alasan = "Filter Regex Grup"
                detail = (
                    f"◈ <b>Pola cocok:</b> <code>{html.escape(str(raw_pattern))}</code>\n"
                    f"◈ <b>Keterangan:</b> Kata kunci dalam filter lokal grup ini"
                )
                break

    # Anti-duplikasi lokal
    if not alasan and cfg.get("local") is True:
        lokal_record = await messages_db.find_one({
            "chat_id": cid, "msg_id": message.id, "type": "local_track"
        })
        if lokal_record and lokal_record.get("warned") is True:
            alasan = "Anti-Spam Duplikat Lokal"
            detail = (
                "◈ <b>Keterangan:</b> Pesan identik/mirip dikirim berulang\n"
                f"◈ <b>Interval deteksi:</b> {cfg.get('expiry', 60)} detik"
            )

    # Anti-gcast global
    if not alasan and cfg.get("global") is True:
        content_hash = hashlib.md5(content.encode()).hexdigest()
        global_key   = f"glob_{uid}_{content_hash}"
        existing     = await messages_db.find_one({"_id": global_key})
        if existing and (now_ts - existing.get("time", 0)) < GLOBAL_EXPIRY:
            if len(existing.get("locations", [])) >= 2:
                locs   = existing.get("locations", [])
                alasan = "Anti-Broadcast Gcast Global"
                detail = (
                    f"◈ <b>Keterangan:</b> Pesan disebar serentak ke {len(locs)} grup\n"
                    "◈ <b>Indikator:</b> Konten identik muncul di beberapa grup dalam waktu singkat"
                )

    # Bio link
    if not alasan and cfg.get("bio_check") is True:
        try:
            from plugins.filters.bio import _bio_cache
            hit = _bio_cache.get(uid)
            if hit and hit[0] is True:
                alasan = "Bio Link Detector"
                detail = (
                    "◈ <b>Keterangan:</b> Profil bio user mengandung tautan/link\n"
                    "◈ <b>Kebijakan:</b> Pesan dari user berbio link dihapus otomatis"
                )
        except ImportError:
            pass

    # Link detector
    if not alasan:
        url_types    = {MessageEntityType.URL, MessageEntityType.TEXT_LINK}
        all_entities = list(message.entities or []) + list(message.caption_entities or [])
        if any(e.type in url_types for e in all_entities):
            alasan = "Link Detector"
            detail = (
                "◈ <b>Keterangan:</b> Pesan mengandung tautan/URL aktif\n"
                "◈ <b>Kebijakan:</b> Pengiriman link tidak diizinkan di grup ini"
            )

    # External mention
    if not alasan and cfg.get("anti_mention", True) is True:
        try:
            from plugins.filters.antispam import _is_external_mention
            if await _is_external_mention(client, message):
                alasan = "Mention Pengguna Luar Grup"
                detail = (
                    "◈ <b>Keterangan:</b> Pesan menyebut user yang bukan anggota grup\n"
                    "◈ <b>Indikator:</b> Pola mention spam untuk menarik orang luar"
                )
        except ImportError:
            pass

    # Hapus silent — user masih dalam masa mute aktif
    if not alasan and cfg.get("local") is True:
        try:
            from database import get_local_mute
            mute_rec = await get_local_mute(cid, uid)
            if mute_rec.get("muted_until", 0.0) > now_ts:
                until_dt = datetime.fromtimestamp(mute_rec["muted_until"], tz=TZ_WIB)
                alasan = "Hapus Senyap — Masa Mute Aktif"
                detail = (
                    f"◈ <b>Keterangan:</b> User masih di-mute, pesan otomatis dihapus\n"
                    f"◈ <b>Mute berakhir:</b> {until_dt.strftime('%H:%M:%S WIB')}"
                )
        except Exception:
            pass

    if not alasan:
        return

    # ── Peta ikon per jenis pelanggaran ──────────────────────────────────────
    icon_map = {
        "Filter Regex Global":              "🚫",
        "Filter Regex Grup":               "🔡",
        "Anti-Spam Duplikat Lokal":        "🔁",
        "Anti-Broadcast Gcast Global":     "🌐",
        "Bio Link Detector":               "🔍",
        "Link Detector":                   "🔗",
        "Mention Pengguna Luar Grup":      "👤",
        "Hapus Senyap — Masa Mute Aktif":  "🔇",
    }
    icon         = icon_map.get(alasan, "⚠️")
    user_mention = _user_line(uid, message.from_user.first_name)

    log_text = (
        f"<b>❖ HAPUS OTOMATIS — {alasan.upper()} ❖</b>\n"
        "<blockquote>"
        f"{icon} <b>Tipe:</b> {alasan}\n"
        f"◈ <b>User:</b> {user_mention}\n"
        f"◈ <b>Grup:</b> {html.escape(message.chat.title)} (<code>{cid}</code>)\n"
        f"◈ <b>Waktu:</b> {_fmt_waktu()}\n"
        f"{detail}\n\n"
        f"📨 <b>Konten:</b>\n<code>{html.escape(content[:500])}</code>"
        "</blockquote>"
    )
    await _send_log(client, log_text)


# ── INTEGRASI NEXUS ───────────────────────────────────────────────────────────

async def log_spam_global(client: Client, message: Message, pola: str, indikator: str):
    """Dipanggil oleh Nexus Engine untuk log pelanggaran GLOBAL."""
    uid          = message.from_user.id
    cid          = message.chat.id
    user_mention = _user_line(uid, message.from_user.first_name)

    log_text = (
        "<b>❖ HAPUS OTOMATIS — NEXUS AI GLOBAL ❖</b>\n"
        "<blockquote>"
        f"🌐 <b>Tipe:</b> Deteksi AI Global\n"
        f"◈ <b>User:</b> {user_mention}\n"
        f"◈ <b>Grup:</b> {html.escape(message.chat.title)} (<code>{cid}</code>)\n"
        f"◈ <b>Waktu:</b> {_fmt_waktu()}\n"
        f"◈ <b>Keterangan:</b> Model AI mendeteksi pola spam lintas grup\n"
        f"◈ <b>Indikator AI:</b> <code>{html.escape(str(indikator))}</code>\n"
        f"◈ <b>Pola terdeteksi:</b> <code>{html.escape(str(pola)[:80])}</code>"
        "</blockquote>"
    )
    await _send_log(client, log_text)


async def log_spam_lokal(client: Client, message: Message, pola: str, indikator: str):
    """Dipanggil oleh Nexus Engine untuk log pelanggaran LOKAL (Owner)."""
    uid          = message.from_user.id
    cid          = message.chat.id
    user_mention = _user_line(uid, message.from_user.first_name)

    log_text = (
        "<b>❖ HAPUS OTOMATIS — NEXUS AI OWNER ❖</b>\n"
        "<blockquote>"
        f"⚙️ <b>Tipe:</b> Filter Manual Owner (Nexus AI)\n"
        f"◈ <b>User:</b> {user_mention}\n"
        f"◈ <b>Grup:</b> {html.escape(message.chat.title)} (<code>{cid}</code>)\n"
        f"◈ <b>Waktu:</b> {_fmt_waktu()}\n"
        f"◈ <b>Keterangan:</b> Cocok dengan filter kata yang diset owner\n"
        f"◈ <b>Indikator AI:</b> <code>{html.escape(str(indikator))}</code>\n"
        f"◈ <b>Pola terdeteksi:</b> <code>{html.escape(str(pola)[:80])}</code>"
        "</blockquote>"
    )
    await _send_log(client, log_text)


async def log_sistem(client: Client, judul: str, pesan: str):
    """Log notifikasi sistem ke channel."""
    log_text = (
        f"<b>❖ SISTEM — {judul.upper()} ❖</b>\n"
        "<blockquote>"
        f"⚡ <b>Waktu:</b> {_fmt_waktu()}\n"
        f"{pesan}"
        "</blockquote>"
    )
    await _send_log(client, log_text)
