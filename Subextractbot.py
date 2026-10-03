# ============================================================
# Telegram Subtitle Extractor Bot
# Forward / send a video -> every soft subtitle track is extracted
# (in order) and sent back as separate files.
# Credentials come from environment variables (set by Sub Extract Launcher.py).
# ============================================================

import asyncio
import contextlib
import json
import logging
import os
import re
import shutil
import time

from pyrogram import Client, filters, enums, idle
from pyrogram.errors import FloodWait

logging.getLogger("pyrogram").setLevel(logging.ERROR)

API_ID = int(os.environ["API_ID"])
API_HASH = os.environ["API_HASH"]
BOT_TOKEN = os.environ["BOT_TOKEN"]
WORK_DIR = os.environ.get("WORK_DIR", "/content/sub_work")
os.makedirs(WORK_DIR, exist_ok=True)

VIDEO_EXTS = (".mkv", ".mp4", ".avi", ".webm", ".flv", ".mov", ".m4v", ".ts", ".wmv", ".mpg", ".mpeg")
UI_INTERVAL = 2.5

# codec -> (output extension, ffmpeg subtitle codec)
TEXT_SUBS = {
    "subrip": (".srt", "copy"),
    "srt": (".srt", "copy"),
    "ass": (".ass", "copy"),
    "ssa": (".ssa", "copy"),
    "webvtt": (".vtt", "copy"),
    "mov_text": (".srt", "srt"),
    "text": (".srt", "srt"),
}
# Bitmap subtitles can't become text, but PGS can still be dumped as .sup
IMAGE_SUBS = {"hdmv_pgs_subtitle": ".sup"}

app = Client(
    "sub_extract_bot",
    api_id=API_ID,
    api_hash=API_HASH,
    bot_token=BOT_TOKEN,
    workers=16,
    sleep_threshold=60,
    in_memory=True,
)

# Videos are processed one at a time, in the order they arrive (keeps forwarded batches in order).
process_lock = asyncio.Lock()
cancelled = set()


# ------------------------------------------------------------
# Helpers
# ------------------------------------------------------------
def format_bytes(size):
    size = float(size or 0)
    for unit in ("B", "KB", "MB", "GB"):
        if size < 1024 or unit == "GB":
            return f"{size:.2f} {unit}"
        size /= 1024


def format_time(seconds):
    seconds = max(0, int(seconds))
    m, s = divmod(seconds, 60)
    h, m = divmod(m, 60)
    return f"{h}h {m}m {s}s" if h else f"{m}m {s}s" if m else f"{s}s"


def safe_name(name):
    name = re.sub(r'[\\/:*?"<>|\r\n]+', "_", name).strip(" .")
    return name[:150] or "video"


def get_media(message):
    """Return (media, filename) for a video / video-like document, else (None, None)."""
    if message.video:
        m = message.video
        return m, m.file_name or f"video_{message.id}.mp4"
    if message.document:
        m = message.document
        name = m.file_name or ""
        if (m.mime_type or "").startswith("video/") or name.lower().endswith(VIDEO_EXTS):
            return m, name or f"video_{message.id}.mkv"
    return None, None


async def safe_call(coro_fn, *args, **kwargs):
    """Run a Telegram call, sleeping through FloodWait."""
    for _ in range(5):
        try:
            return await coro_fn(*args, **kwargs)
        except FloodWait as e:
            await asyncio.sleep(int(getattr(e, "value", 0) or 0) + 1)
    return None


async def probe_subtitles(path):
    proc = await asyncio.create_subprocess_exec(
        "ffprobe", "-v", "error", "-print_format", "json", "-show_streams", "-select_streams", "s", path,
        stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.PIPE,
    )
    out, err = await asyncio.wait_for(proc.communicate(), timeout=120)
    if proc.returncode != 0:
        raise RuntimeError(err.decode("utf-8", "ignore")[:500])
    return json.loads(out.decode("utf-8", "ignore")).get("streams", [])


async def extract_track(path, stream_index, codec_args, out_path):
    proc = await asyncio.create_subprocess_exec(
        "ffmpeg", "-hide_banner", "-loglevel", "error", "-y", "-i", path,
        "-map", f"0:{stream_index}", "-c:s", codec_args, out_path,
        stdout=asyncio.subprocess.DEVNULL, stderr=asyncio.subprocess.PIPE,
    )
    _, err = await asyncio.wait_for(proc.communicate(), timeout=600)
    ok = proc.returncode == 0 and os.path.exists(out_path) and os.path.getsize(out_path) > 0
    return ok, err.decode("utf-8", "ignore")[-300:]


# ------------------------------------------------------------
# Commands
# ------------------------------------------------------------
@app.on_message(filters.command("start") & filters.private)
async def start_cmd(client, message):
    await message.reply_text(
        "👋 <b>Subtitle Extractor Bot</b>\n\n"
        "වීඩියෝ එකක් (MKV / MP4 ...) මට <b>Forward</b> කරන්න හෝ එවන්න.\n"
        "ඒකේ තියෙන <b>Soft Subtitles</b> ඔක්කොම පිළිවෙළට වෙන වෙනම Extract කරලා එවන්නම්.\n\n"
        "ℹ️ SRT / ASS / SSA / VTT / mov_text (→ SRT) සහ PGS (.sup) support කරනවා.",
        parse_mode=enums.ParseMode.HTML,
    )


@app.on_callback_query(filters.regex(r"^cancel_"))
async def cancel_cb(client, cb):
    cancelled.add(cb.data.split("_", 1)[1])
    await cb.answer("Cancelling...")


# ------------------------------------------------------------
# Video handler
# ------------------------------------------------------------
@app.on_message((filters.video | filters.document) & filters.private)
async def handle_video(client, message):
    media, filename = get_media(message)
    if not media:
        return

    task_id = f"{message.chat.id}_{message.id}"
    from pyrogram.types import InlineKeyboardButton, InlineKeyboardMarkup
    cancel_markup = InlineKeyboardMarkup([[InlineKeyboardButton("❌ Cancel", callback_data=f"cancel_{task_id}")]])

    status = await safe_call(
        message.reply_text,
        f"⏳ <b>Queue එකේ...</b>\n<code>{filename}</code>",
        parse_mode=enums.ParseMode.HTML, reply_markup=cancel_markup, quote=True,
    )

    async with process_lock:
        if task_id in cancelled:
            cancelled.discard(task_id)
            with contextlib.suppress(Exception):
                await status.edit_text("🚫 <b>Cancelled.</b>", parse_mode=enums.ParseMode.HTML)
            return

        work = os.path.join(WORK_DIR, task_id)
        os.makedirs(work, exist_ok=True)
        try:
            await process_video(client, message, status, media, filename, task_id, work, cancel_markup)
        except asyncio.CancelledError:
            raise
        except Exception as e:
            with contextlib.suppress(Exception):
                await status.edit_text(f"❌ <b>Error:</b>\n<code>{str(e)[:3000]}</code>", parse_mode=enums.ParseMode.HTML)
        finally:
            cancelled.discard(task_id)
            shutil.rmtree(work, ignore_errors=True)


async def process_video(client, message, status, media, filename, task_id, work, cancel_markup):
    # ---- 1. download ----
    start = time.time()
    last_edit = [0.0]

    async def progress(current, total):
        if task_id in cancelled:
            client.stop_transmission()
        now = time.time()
        if now - last_edit[0] < UI_INTERVAL and current < total:
            return
        last_edit[0] = now
        pct = current * 100 / total if total else 0
        speed = current / max(now - start, 0.001)
        eta = (total - current) / speed if speed else 0
        bar = "█" * int(pct / 100 * 18) + "░" * (18 - int(pct / 100 * 18))
        with contextlib.suppress(Exception):
            await status.edit_text(
                f"📥 <b>Downloading</b>\n<code>{filename}</code>\n\n"
                f"<code>[{bar}] {pct:.1f}%</code>\n"
                f"{format_bytes(current)} / {format_bytes(total)}\n"
                f"<b>Speed:</b> {format_bytes(speed)}/s • <b>ETA:</b> {format_time(eta)}",
                parse_mode=enums.ParseMode.HTML, reply_markup=cancel_markup,
            )

    video_path = os.path.join(work, safe_name(filename))
    path = await client.download_media(message, file_name=video_path, progress=progress)
    if task_id in cancelled or not path:
        await status.edit_text("🚫 <b>Cancelled.</b>", parse_mode=enums.ParseMode.HTML)
        return

    # ---- 2. probe ----
    await status.edit_text("🔍 <b>Subtitle tracks සොයමින්...</b>", parse_mode=enums.ParseMode.HTML)
    tracks = await probe_subtitles(path)
    if not tracks:
        await status.edit_text(
            f"❌ <b>Soft subtitle එකක් හමු වුණේ නැහැ.</b>\n<code>{filename}</code>",
            parse_mode=enums.ParseMode.HTML,
        )
        return

    # ---- 3. extract + send, in track order ----
    base = os.path.splitext(safe_name(filename))[0]
    sent, skipped = 0, []

    for n, st in enumerate(tracks, start=1):
        if task_id in cancelled:
            await status.edit_text("🚫 <b>Cancelled.</b>", parse_mode=enums.ParseMode.HTML)
            return

        codec = (st.get("codec_name") or "unknown").lower()
        tags = st.get("tags") or {}
        lang = (tags.get("language") or "und").lower()
        title = tags.get("title") or ""

        if codec in TEXT_SUBS:
            ext, ffcodec = TEXT_SUBS[codec]
        elif codec in IMAGE_SUBS:
            ext, ffcodec = IMAGE_SUBS[codec], "copy"
        else:
            skipped.append(f"#{n} ({codec})")
            continue

        await status.edit_text(
            f"⚙️ <b>Extracting subtitle {n}/{len(tracks)}</b>\n<code>{lang} • {codec}</code>",
            parse_mode=enums.ParseMode.HTML, reply_markup=cancel_markup,
        )
        out_name = f"{base}.{n:02d}.{lang}{ext}" if len(tracks) > 1 else f"{base}.{lang}{ext}"
        out_path = os.path.join(work, out_name)
        ok, err = await extract_track(path, st["index"], ffcodec, out_path)
        if not ok:
            skipped.append(f"#{n} ({codec})")
            print(f"[ERROR] track {n} failed: {err}")
            continue

        caption = f"<b>Track {n}/{len(tracks)}</b> • <code>{lang}</code> • {codec.upper()}" + (f"\n<i>{title}</i>" if title else "")
        await safe_call(
            client.send_document, message.chat.id, out_path,
            caption=caption, parse_mode=enums.ParseMode.HTML, reply_to_message_id=message.id,
        )
        sent += 1
        with contextlib.suppress(Exception):
            os.remove(out_path)

    summary = f"✅ <b>Done!</b> Subtitles {sent}/{len(tracks)} එවා ඇත.\n<code>{filename}</code>"
    if skipped:
        summary += "\n\n⚠️ Extract කළ නොහැකි tracks: " + ", ".join(skipped)
    await status.edit_text(summary, parse_mode=enums.ParseMode.HTML)


# ------------------------------------------------------------
# Run
# ------------------------------------------------------------
async def main():
    await app.start()
    me = await app.get_me()
    print(f"✅ Subtitle Extractor Bot running as @{me.username}")
    await idle()
    await app.stop()


if __name__ == "__main__":
    asyncio.get_event_loop().run_until_complete(main())
