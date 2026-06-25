"""
plugins/filters/antispam.py
────────────────────────────
Filter utama pesan grup:
  1. Regex global & lokal  (Owner Regex — TANPA pengaruh Whitelist Nexus)
  2. External mention
  3. Link detector
  4. Anti duplikasi lokal (per user per grup) — DIOPTIMALKAN VIA FAST-PATH RAM
  5. Anti duplikasi global (anti-gcast lintas grup) — PROTEKSI MASSAL ANTI-CLONE

SISTEM LOGGING:
  Telah dihubungkan secara penuh dengan plugins.commands.log (log_spam_lokal)
  sehingga setiap tindakan Fast-Path RAM langsung dilaporkan ke log worker/channel.
────────────────────────────
TOGGLE-DRIVEN DETECTION:
  Setiap fitur deteksi (bukan hanya hukuman) dimatikan sepenuhnya saat toggle OFF.
  - global OFF  → PROTEKSI A & B (RAM mass-burst) tidak berjalan sama sekali
  - local OFF   → PROTEKSI C (RAM per-user) tidak berjalan sama sekali
  - Logika detection_queue juga mengikuti toggle masing-masing fitur
"""

import os
import re
import time
import asyncio
import hashlib
from datetime import datetime
from pyrogram import Client, filters
from pyrogram.enums import MessageEntityType, ParseMode
from pyrogram.errors import UserNotParticipant, PeerIdInvalid, RPCError

LOG_CHANNEL = int(os.environ.get("LOG_CHANNEL", 0))

from database import (
    messages_db, regex_db, get_config, is_admin, db,
    delete_queue, GLOBAL_EXPIRY, TZ_WIB, auto_delete_reply,
    mark_message_handled, is_message_handled,
    get_local_mute, reset_local_mute,
    insert_group_action_log,
    check_bot_permissions,
)
from core.regex_utils import simplify, remove_mentions_for_regex, match_with_leet

# ── IMPOR FUNGSI HUKUMAN & LOG BAWAAN ANDA ───────────────────────────────────
from core.punishment import check_and_punish
from plugins.commands.log import log_spam_lokal

group_regex_db = db["regex_per_group"]
free_col       = db["free_per_group"]

# ── 1. Cache Per-User (Bom Spam dari 1 Akun Tunggal) ──────────────────────────
_local_flood_cache: dict[int, dict[int, tuple[str, float, int]]] = {}
_FLOOD_WINDOW   = 5.0  
_MAX_DUPLICATE  = 2    

# ── 2. Cache Lintas-User (Serangan Massal Banyak Akun Kloning / Userbot) ──────
_global_text_tracker: dict[int, dict[str, list[float]]] = {}
_global_text_blacklist: dict[int, dict[str, float]] = {}

_MASS_BURST_WINDOW = 1.5  
_MASS_BURST_LIMIT  = 3    
_LOCK_DURATION     = 10.0 

# ── Cache regex ───────────────────────────────────────────────────────────────
_regex_cache:     list  = []
_regex_cache_ts:  float = 0.0
_local_regex_cache: dict[int, tuple[list, float]] = {}
REGEX_TTL = 300

_URL_ENTITY_TYPES = {MessageEntityType.URL, MessageEntityType.TEXT_LINK}


def _has_url_entity(message) -> bool:
    entities = list(message.entities or []) + list(message.caption_entities or [])
    return any(e.type in _URL_ENTITY_TYPES for e in entities)


async def _get_global_patterns():
    global _regex_cache, _regex_cache_ts
    now = time.monotonic()
    if now - _regex_cache_ts < REGEX_TTL:
        return _regex_cache
    patterns = []
    async for doc in regex_db.find({"pattern": {"$exists": True}}):
        try:
            raw = doc.get("raw") or doc.get("pattern", "")
            patterns.append((re.compile(doc["pattern"], re.IGNORECASE), raw))
        except Exception:
            pass
    _regex_cache = patterns
    _regex_cache_ts = now
    return _regex_cache


async def _get_local_patterns(chat_id: int):
    now = time.monotonic()
    hit = _local_regex_cache.get(chat_id)
    if hit and (now - hit[1]) < REGEX_TTL:
        return hit[0]
    patterns = []
    async for doc in group_regex_db.find({"chat_id": chat_id}):
        try:
            raw = doc.get("raw") or doc.get("pattern", "")
            patterns.append((re.compile(doc["pattern"], re.IGNORECASE), raw))
        except Exception:
            pass
    _local_regex_cache[chat_id] = (patterns, now)
    return patterns


def invalidate_local_regex_cache(chat_id: int) -> None:
    _local_regex_cache.pop(chat_id, None)


async def _is_external_mention(client: Client, message) -> bool:
    """
    Deteksi apakah pesan mengandung mention ke user yang BUKAN member grup.

    Urutan prioritas per mention:
      1. Cek mention_member_cache (DB lokal) → O(1) tanpa Telegram API
      2. Cache hit  → langsung return
      3. Cache miss → delegate ke bot pembantu (MonitorInstance) via check_member_via_monitor
      4. Monitor tidak aktif / error → fallback ke bot utama (get_chat_member langsung)

    TTL cache 1 minggu, diperbarui setiap ada mention yang cache hit.
    username dan user_id keduanya disimpan secara terpisah di cache.
    """
    if not message.entities:
        return False
    content = message.text or message.caption or ""
    cid = message.chat.id

    try:
        from monitor_bot_reference import check_member_via_monitor
        _monitor_available = True
    except Exception:
        _monitor_available = False

    for entity in message.entities:
        target = None
        target_uid: int | None = None
        target_uname: str | None = None

        # Hanya proses @username mention biasa.
        # TEXT_MENTION (tag tanpa @username) di-skip karena hanya bisa
        # dilakukan ke member aktif — Telegram tidak mengizinkan tag jenis
        # ini ke non-member, sehingga pasti bukan external.
        # tg://user?id= juga di-skip karena alasan yang sama.
        if entity.type == MessageEntityType.MENTION:
            uname = content[entity.offset:entity.offset + entity.length].lstrip("@").lower()
            target = uname
            target_uname = uname
        else:
            continue

        if not target:
            continue
        # Skip username sistem Telegram yang pasti bukan non-member
        if target in ("botfather", "telegram", "admin"):
            continue

        # ── 1. Cek cache dulu ───────────────────────────────────────────────
        from database import (
            mention_cache_get_by_uid, mention_cache_get_by_username,
            mention_cache_refresh_ttl,
        )
        cached = None
        if target_uid:
            cached = await mention_cache_get_by_uid(cid, target_uid)
            if cached is not None:
                asyncio.create_task(mention_cache_refresh_ttl(cid, target_uid))
                if not cached:   # is_member=False → external
                    return True
                continue         # is_member=True → lanjut entity berikutnya
        elif target_uname:
            cached = await mention_cache_get_by_username(cid, target_uname)
            if cached is not None:
                # Tidak punya user_id di sini, skip refresh TTL (monitor nanti update)
                if not cached:
                    return True
                continue

        # ── 2. Cache miss → coba bot pembantu ──────────────────────────────
        if _monitor_available:
            try:
                result = await check_member_via_monitor(cid, target)
                if result is not None:
                    # Monitor berhasil → tidak perlu hit bot utama
                    if not result:
                        return True   # external
                    continue          # member, lanjut
                # result=None → monitor tidak aktif / error → fallback ke bot utama
            except Exception:
                pass

        # ── 3. Fallback: bot utama langsung ────────────────────────────────
        try:
            member = await client.get_chat_member(cid, target)
            is_member = member is not None
            # Simpan ke cache untuk mention berikutnya
            uid = member.user.id if (member and member.user) else (target_uid)
            uname = member.user.username if (member and member.user) else target_uname
            if uid:
                from database import mention_cache_set
                asyncio.create_task(mention_cache_set(cid, uid, is_member, username=uname))
            if not is_member:
                return True
        except (UserNotParticipant, PeerIdInvalid, RPCError):
            # Simpan ke cache: bukan member
            if target_uid:
                from database import mention_cache_set
                asyncio.create_task(mention_cache_set(cid, target_uid, False, username=target_uname))
            return True

    return False


def _trigger_passive_learn_spam(text: str, confidence: float = 1.0) -> None:
    try:
        from nexus.ai_core import nexus_ai_passive_observe
        asyncio.create_task(
            nexus_ai_passive_observe(text, True, confidence, force_learn=True)
        )
    except Exception:
        pass


# ─────────────────────────────────────────────────────────────────────────────
#  Main filter (group=2) — FAST-PATH RAM
# ─────────────────────────────────────────────────────────────────────────────
@Client.on_message(filters.group & ~filters.service, group=2)
async def main_antispam_filter(client, message):
    if not message.from_user:
        return
    cid, uid, mid = message.chat.id, message.from_user.id, message.id

    if is_message_handled(cid, mid):
        return

    # ── Cek izin bot: HARUS punya delete_messages DAN restrict_members ───────
    if not await check_bot_permissions(client, cid):
        return

    if await is_admin(client, cid, uid):
        return

    if await free_col.find_one({"user_id": uid, "chat_id": cid}):
        return

    content = (message.text or message.caption or "").strip()
    if not content or content.startswith("/"):
        return

    # ── Ambil config SEKALI di awal — semua fast-path RAM bergantung padanya ──
    cfg    = await get_config(cid)
    global_on = cfg.get("global") is True
    local_on  = cfg.get("local")  is True

    content_hash = hashlib.md5(content.encode("utf-8", errors="ignore")).hexdigest()
    now_ts = time.time()

    # ── PROTEKSI A: Karantina RAM Sementara (Serangan Massal Banyak Akun) ──────
    # Hanya berjalan jika toggle global ON
    if global_on and cid in _global_text_blacklist and content_hash in _global_text_blacklist[cid]:
        if now_ts < _global_text_blacklist[cid][content_hash]:
            mark_message_handled(cid, mid)
            asyncio.create_task(check_and_punish(client, message, "MASS_FLOOD_BURST_RAM", content))
            asyncio.create_task(log_spam_lokal(client, message, pola=content[:80], indikator="MASS_FLOOD_BURST_RAM"))
            asyncio.create_task(message.delete())
            return
        else:
            _global_text_blacklist[cid].pop(content_hash, None)

    # ── PROTEKSI B: Deteksi Serangan Massal Banyak Akun Kloning (Lintas User) ──
    # Tracking & eksekusi hanya jika toggle global ON
    if global_on:
        if cid not in _global_text_tracker:
            _global_text_tracker[cid] = {}

        if content_hash not in _global_text_tracker[cid]:
            _global_text_tracker[cid][content_hash] = []

        _global_text_tracker[cid][content_hash].append(now_ts)

        _global_text_tracker[cid][content_hash] = [
            ts for ts in _global_text_tracker[cid][content_hash]
            if (now_ts - ts) <= _MASS_BURST_WINDOW
        ]

        if len(_global_text_tracker[cid][content_hash]) >= _MASS_BURST_LIMIT:
            if cid not in _global_text_blacklist:
                _global_text_blacklist[cid] = {}

            _global_text_blacklist[cid][content_hash] = now_ts + _LOCK_DURATION

            mark_message_handled(cid, mid)
            asyncio.create_task(check_and_punish(client, message, "MASS_FLOOD_BURST_RAM", content))
            asyncio.create_task(log_spam_lokal(client, message, pola=content[:80], indikator="MASS_FLOOD_BURST_RAM"))
            asyncio.create_task(message.delete())
            return

    # ── PROTEKSI C: Deteksi Duplikasi Tunggal Per-User ────────────────────────
    # Tracking & eksekusi hanya jika toggle local ON
    if local_on:
        if cid not in _local_flood_cache:
            _local_flood_cache[cid] = {}

        user_flood_data = _local_flood_cache[cid].get(uid)

        if user_flood_data:
            last_hash, last_time, duplicate_count = user_flood_data

            if last_hash == content_hash and (now_ts - last_time) < _FLOOD_WINDOW:
                duplicate_count += 1
                _local_flood_cache[cid][uid] = (content_hash, now_ts, duplicate_count)

                if duplicate_count >= _MAX_DUPLICATE:
                    mark_message_handled(cid, mid)
                    asyncio.create_task(check_and_punish(client, message, "LOCAL_FLOOD_RAM", content))
                    asyncio.create_task(log_spam_lokal(client, message, pola=content[:80], indikator="LOCAL_FLOOD_RAM"))
                    asyncio.create_task(message.delete())
                    return
            else:
                _local_flood_cache[cid][uid] = (content_hash, now_ts, 1)
        else:
            _local_flood_cache[cid][uid] = (content_hash, now_ts, 1)

    # ── Enqueue ke detection_queue (Untuk sistem antrean latar belakang bawaan) ──
    from core.antispam_queue import enqueue_for_detection
    await enqueue_for_detection(client, message)


async def _gcast_punish_other_group(
    client,
    chat_id: int,
    user_id: int,
    konten: str,
) -> None:
    from database import (
        get_local_mute, increment_local_spam, apply_local_mute,
        revert_failed_local_mute, insert_group_action_log,
    )
    from core.punishment import SPAM_MUTE_THRESHOLD
    from core.moderation_queue import queue_mute
    import time as _time
    now_ts = _time.time()
    mute_rec = await get_local_mute(chat_id, user_id)
    if mute_rec.get("muted_until", 0.0) > now_ts:
        return
    updated = await increment_local_spam(chat_id, user_id)
    consec  = updated.get("consec_spam", 1)
    if consec < SPAM_MUTE_THRESHOLD:
        return
    duration_secs, level_before = await apply_local_mute(chat_id, user_id)
    duration_min = duration_secs // 60

    async def _on_done(success: bool):
        if not success:
            await revert_failed_local_mute(chat_id, user_id, level_before)
            return
        try:
            await insert_group_action_log(
                chat_id, "MUTE",
                f"Mute {duration_min} mnt — Anti-Broadcast Gcast Global 10×",
                user_id, str(user_id), konten,
            )
        except Exception:
            pass

    await queue_mute(chat_id, user_id, duration_secs, on_done=_on_done)


# ─────────────────────────────────────────────────────────────────────────────
#  group=10 — Tracker pesan bersih
# ─────────────────────────────────────────────────────────────────────────────
@Client.on_message(filters.group & ~filters.service, group=10)
async def _clean_message_tracker(client, message):
    if not message.from_user or message.from_user.is_bot:
        return
    cid = message.chat.id
    mid = message.id
    uid = message.from_user.id

    if not is_message_handled(cid, mid):
        asyncio.create_task(_reset_mute_async(cid, uid))


async def _reset_mute_async(chat_id: int, user_id: int) -> None:
    try:
        await reset_local_mute(chat_id, user_id)
    except Exception:
        pass
