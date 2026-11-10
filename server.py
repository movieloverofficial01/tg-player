"""Telegram -> browser video streamer (Starlette, no pydantic, Termux friendly)."""
import os, re, json
from contextlib import asynccontextmanager
from starlette.applications import Starlette
from starlette.exceptions import HTTPException
from starlette.responses import FileResponse, JSONResponse, Response, StreamingResponse
from starlette.routing import Route, Mount
from starlette.staticfiles import StaticFiles
from pyrogram import Client
from pyrogram.handlers import MessageHandler
from pyrogram import filters

CHUNK = 1024 * 1024  # Pyrogram streams in 1 MiB chunks
tg = Client("streamer", api_id=int(os.environ["API_ID"]), api_hash=os.environ["API_HASH"],
            bot_token=os.environ["BOT_TOKEN"], in_memory=True)
_cache = {}
os.makedirs("subs", exist_ok=True)

def load_lib():
    try:
        with open("library.json", encoding="utf-8") as f:
            return json.load(f)
    except FileNotFoundError:
        return {}

def videos():
    try:
        with open("videos.json", encoding="utf-8") as f:
            manual = json.load(f)
    except FileNotFoundError:
        manual = {}
    return {**load_lib(), **manual}

def save_lib(lib):
    with open("library.json", "w", encoding="utf-8") as f:
        json.dump(lib, f, ensure_ascii=False, indent=1)

def bucket(h):
    return "1080" if h >= 1000 else "720" if h >= 600 else "480"

async def on_post(client, msg):
    """New channel posts -> library. Same title in different qualities = one video.
    A .srt/.vtt file posted as a reply to a video becomes its subtitle."""
    chat, d = msg.chat.id, msg.document
    if d and (d.file_name or "").lower().endswith((".srt", ".vtt")) and msg.reply_to_message_id:
        lib = load_lib()
        for e in lib.values():
            if any(s["chat"] == chat and s["msg"] == msg.reply_to_message_id for s in e["sources"].values()):
                label = (msg.caption or d.file_name.rsplit(".", 1)[0])[:40]
                e["subs"].append({"label": label, "url": f"/sub/{chat}/{msg.id}.vtt"})
                save_lib(lib)
        return
    media = msg.video or (d if d and (d.mime_type or "").startswith("video/") else None)
    if not media:
        return
    text = (msg.caption or getattr(media, "file_name", None) or "").split("\n")[0]
    m = re.search(r"(?i)\b(\d{3,4})p\b", text)
    q = m.group(1) if m else bucket(getattr(media, "height", 0) or 0)
    title = re.sub(r"\s+", " ", re.sub(r"(?i)\b\d{3,4}p\b", "", text)).strip(" |-_[]()") or f"Video {msg.id}"
    key = re.sub(r"\W+", "-", title.lower()).strip("-") or f"c{abs(chat)}m{msg.id}"
    lib = load_lib()
    e = lib.setdefault(key, {"title": title[:80], "added": msg.date.isoformat() if msg.date else "",
                             "sources": {}, "subs": []})
    e["sources"][q] = {"chat": chat, "msg": msg.id}
    save_lib(lib)

_subs = {}

async def sub(request):
    chat, mid = int(request.path_params["chat"]), int(request.path_params["mid"])
    if not any(s["url"] == f"/sub/{chat}/{mid}.vtt" for e in videos().values() for s in e.get("subs", [])):
        raise HTTPException(404, "Unknown subtitle")
    if (chat, mid) not in _subs:
        m = await tg.get_messages(chat, mid)
        if not m or m.empty or not m.document:
            raise HTTPException(404, "No subtitle file")
        buf = await tg.download_media(m, in_memory=True)
        txt = bytes(buf.getbuffer()).decode("utf-8-sig", "replace")
        if not txt.lstrip().startswith("WEBVTT"):
            txt = "WEBVTT\n\n" + re.sub(r"(\d\d:\d\d:\d\d),(\d\d\d)", r"\1.\2", txt.replace("\r", ""))
        _subs[(chat, mid)] = txt
    return Response(_subs[(chat, mid)], media_type="text/vtt", headers={"Cache-Control": "no-store"})

tg.add_handler(MessageHandler(on_post, filters.channel))

async def get_msg(chat, mid):
    key = (chat, mid)
    if key not in _cache:
        m = await tg.get_messages(chat, mid)
        if not m or m.empty or not (m.video or m.document):
            raise HTTPException(404, "Telegram message has no video")
        _cache[key] = m
    return _cache[key]

async def index(request):
    return FileResponse("index.html")

async def library_page(request):
    return FileResponse("library.html")

async def library_api(request):
    items = [{"id": k, "title": v.get("title", k), "added": v.get("added", ""),
              "q": sorted(v["sources"], key=int, reverse=True)}
             for k, v in videos().items()]
    items.sort(key=lambda x: x["added"], reverse=True)
    return JSONResponse(items)

async def info(request):
    v = videos().get(request.path_params["vid"])
    if not v:
        raise HTTPException(404, "Unknown video")
    return JSONResponse({"title": v.get("title", ""), "qualities": sorted(v["sources"], key=int),
                         "subs": v.get("subs", [])})

async def stream(request):
    vid, q = request.path_params["vid"], request.path_params["q"]
    src = videos().get(vid, {}).get("sources", {}).get(q)
    if not src:
        raise HTTPException(404, "Unknown source")
    msg = await get_msg(src["chat"], src["msg"])
    media = msg.video or msg.document
    size, mime = media.file_size, media.mime_type or "video/mp4"

    start, end, partial = 0, size - 1, False
    rng = request.headers.get("range")
    m = re.match(r"bytes=(\d*)-(\d*)", rng) if rng else None
    if m:
        partial = True
        if m.group(1):
            start = int(m.group(1))
            if m.group(2):
                end = min(int(m.group(2)), size - 1)
        elif m.group(2):
            start = max(size - int(m.group(2)), 0)
    if start > end or start >= size:
        return Response(status_code=416, headers={"Content-Range": f"bytes */{size}"})

    first, last = start // CHUNK, end // CHUNK

    async def body():
        skip, left = start % CHUNK, end - start + 1
        async for chunk in tg.stream_media(msg, offset=first, limit=last - first + 1):
            if skip:
                chunk, skip = chunk[skip:], 0
            chunk = chunk[:left]
            left -= len(chunk)
            yield chunk
            if left <= 0:
                break

    headers = {"Accept-Ranges": "bytes", "Content-Length": str(end - start + 1),
               "Cache-Control": "no-store"}
    if partial:
        headers["Content-Range"] = f"bytes {start}-{end}/{size}"
    return StreamingResponse(body(), status_code=206 if partial else 200,
                             media_type=mime, headers=headers)

@asynccontextmanager
async def lifespan(app):
    await tg.start()
    yield
    await tg.stop()

app = Starlette(lifespan=lifespan, routes=[
    Route("/", index),
    Route("/sub/{chat}/{mid}.vtt", sub),
    Route("/library", library_page),
    Route("/api/library", library_api),
    Route("/api/video/{vid}", info),
    Route("/stream/{vid}/{q}", stream),
    Mount("/subs", StaticFiles(directory="subs")),
])
