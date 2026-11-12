"""Telegram -> browser video streamer with an admin panel (Starlette, Render friendly)."""
import os, re, json, base64, hmac, asyncio
from datetime import datetime, timezone
from contextlib import asynccontextmanager
from urllib.parse import quote
import httpx
from starlette.applications import Starlette
from starlette.middleware import Middleware
from starlette.middleware.cors import CORSMiddleware
from starlette.exceptions import HTTPException
from starlette.responses import FileResponse, JSONResponse, Response, StreamingResponse
from starlette.routing import Route, Mount
from starlette.staticfiles import StaticFiles
from pyrogram import Client, filters
from pyrogram.handlers import MessageHandler

CHUNK = 1024 * 1024  # Pyrogram streams in 1 MiB chunks
tg = Client("streamer", api_id=int(os.environ["API_ID"]), api_hash=os.environ["API_HASH"],
            bot_token=os.environ["BOT_TOKEN"], in_memory=True)
os.makedirs("subs", exist_ok=True)

ADMIN = os.environ.get("ADMIN_PASSWORD", "")
GH_TOKEN, GH_REPO = os.environ.get("GITHUB_TOKEN", ""), os.environ.get("GITHUB_REPO", "")
GH_ON = bool(GH_TOKEN and GH_REPO)
GH_BRANCH, GH_FILE = "data", "library.json"
API = f"https://api.github.com/repos/{GH_REPO}"
_cache, _titles, _subs = {}, {}, {}
_lib, _sha = {}, None
_lock = asyncio.Lock()
# Signed links: /v/<chat>/<msg>?s=<signature> work without any stored library, so they never break.
SECRET = (os.environ.get("STREAM_SECRET") or os.environ["BOT_TOKEN"]).encode()
ORIGINS = [o.strip() for o in os.environ.get("ALLOWED_ORIGINS", "*").split(",") if o.strip()]

def sign(chat, mid):
    return hmac.new(SECRET, f"{chat}:{mid}".encode(), "sha256").hexdigest()[:24]

# ---------- library storage (GitHub "data" branch keeps it across restarts) ----------
def gh():
    return httpx.AsyncClient(timeout=25, headers={"Authorization": f"Bearer {GH_TOKEN}",
                                                  "Accept": "application/vnd.github+json"})

async def init_lib():
    global _lib, _sha
    if GH_ON:
        try:
            async with gh() as c:
                r = await c.get(f"{API}/contents/{GH_FILE}", params={"ref": GH_BRANCH})
            if r.status_code == 200:
                _sha = r.json()["sha"]
                _lib = json.loads(base64.b64decode(r.json()["content"]))
                return
            print("GitHub library not loaded:", r.status_code)
        except Exception as e:
            print("GitHub load failed:", e)
    try:
        with open("library.json", encoding="utf-8") as f:
            _lib = json.load(f)
    except Exception:
        _lib = {}

async def ensure_branch(c):
    if (await c.get(f"{API}/branches/{GH_BRANCH}")).status_code == 200:
        return
    d = (await c.get(API)).json()["default_branch"]
    sha = (await c.get(f"{API}/git/ref/heads/{d}")).json()["object"]["sha"]
    (await c.post(f"{API}/git/refs", json={"ref": f"refs/heads/{GH_BRANCH}", "sha": sha})).raise_for_status()

async def save_lib():
    global _sha
    async with _lock:
        try:
            with open("library.json", "w", encoding="utf-8") as f:
                json.dump(_lib, f, ensure_ascii=False, indent=1)
        except Exception:
            pass
        if not GH_ON:
            return
        body = {"message": "update library", "branch": GH_BRANCH,
                "content": base64.b64encode(json.dumps(_lib, ensure_ascii=False, indent=1).encode()).decode()}
        async with gh() as c:
            if _sha is None:
                await ensure_branch(c)
                g = await c.get(f"{API}/contents/{GH_FILE}", params={"ref": GH_BRANCH})
                if g.status_code == 200:
                    _sha = g.json()["sha"]
            if _sha:
                body["sha"] = _sha
            r = await c.put(f"{API}/contents/{GH_FILE}", json=body)
            if r.status_code in (409, 422):
                g = await c.get(f"{API}/contents/{GH_FILE}", params={"ref": GH_BRANCH})
                if g.status_code == 200:
                    body["sha"] = g.json()["sha"]
                    r = await c.put(f"{API}/contents/{GH_FILE}", json=body)
            r.raise_for_status()
            _sha = r.json()["content"]["sha"]

def videos():
    try:
        with open("videos.json", encoding="utf-8") as f:
            manual = json.load(f)
    except FileNotFoundError:
        manual = {}
    return {**_lib, **manual}

# ---------- helpers ----------
def bucket(h):
    return "1080" if h >= 1000 else "720" if h >= 600 else "480"

def clean_title(t):
    return re.sub(r"\s+", " ", re.sub(r"(?i)\b\d{3,4}p\b", "", t)).strip(" |-_[]()")

def slug(t):
    return re.sub(r"\W+", "-", t.lower()).strip("-")

def now():
    return datetime.now(timezone.utc).isoformat()

def media_of(m):
    if not m or m.empty:
        return None
    d = m.document
    return m.video or (d if d and (d.mime_type or "").startswith("video/") else None)

def name_from(m, media):
    cap = (m.caption or "").split("\n")[0].strip()
    return cap or os.path.splitext(getattr(media, "file_name", "") or "")[0].strip()

def entry_for(text, media, chat, mid, q=None, target=None, date=None):
    m = re.search(r"(?i)\b(\d{3,4})p\b", text)
    q = q or (m.group(1) if m else bucket(getattr(media, "height", 0) or 0))
    title = clean_title(text) or f"Video {mid}"
    key = target or slug(title) or f"m{mid}"
    e = _lib.setdefault(key, {"title": title[:150], "added": date or now(), "sources": {}, "subs": []})
    e["sources"][q] = {"chat": chat, "msg": mid}
    return key, e, q

async def get_msg(chat, mid):
    key = (chat, mid)
    if key not in _cache:
        m = await tg.get_messages(chat, mid)
        if not m or m.empty or not (m.video or m.document):
            raise HTTPException(404, "Telegram message has no video")
        _cache[key] = m
    return _cache[key]

async def title_of(vid, e):
    """Title set by the admin, otherwise the original caption / file name from Telegram."""
    if e.get("title"):
        return e["title"]
    if vid not in _titles:
        try:
            src = next(iter(e["sources"].values()))
            m = await get_msg(src["chat"], src["msg"])
            name = name_from(m, m.video or m.document)
            if not name:
                return f"Video {src['msg']}"
            _titles[vid] = name[:150]
        except Exception:
            return vid
    return _titles[vid]

# ---------- new posts in the channel ----------
async def on_post(client, msg):
    """New channel posts -> library. Same title in different qualities = one video.
    A .srt/.vtt file posted as a reply to a video becomes its subtitle."""
    chat, d = msg.chat.id, msg.document
    if d and (d.file_name or "").lower().endswith((".srt", ".vtt")) and msg.reply_to_message_id:
        for e in _lib.values():
            if any(s["chat"] == chat and s["msg"] == msg.reply_to_message_id for s in e["sources"].values()):
                label = (msg.caption or d.file_name.rsplit(".", 1)[0])[:40]
                e["subs"].append({"label": label, "url": f"/sub/{chat}/{msg.id}.vtt"})
                await save_lib()
        return
    media = media_of(msg)
    if not media:
        return
    entry_for(name_from(msg, media), media, chat, msg.id, date=msg.date.isoformat() if msg.date else None)
    await save_lib()

tg.add_handler(MessageHandler(on_post, filters.channel))

# ---------- pages and public api ----------
async def index(request):
    return FileResponse("index.html" if request.query_params.get("v") else "library.html")

async def library_page(request):
    return FileResponse("library.html")

async def admin_page(request):
    return FileResponse("admin.html")

async def library_api(request):
    vs = videos()
    titles = await asyncio.gather(*[title_of(k, v) for k, v in vs.items()])
    items = [{"id": k, "title": ti, "added": v.get("added", ""),
              "q": sorted(v["sources"], key=int, reverse=True)}
             for (k, v), ti in zip(vs.items(), titles)]
    items.sort(key=lambda x: x["added"], reverse=True)
    return JSONResponse(items)

async def info(request):
    vid = request.path_params["vid"]
    v = videos().get(vid)
    if not v:
        raise HTTPException(404, "Unknown video")
    return JSONResponse({"title": await title_of(vid, v), "qualities": sorted(v["sources"], key=int),
                         "subs": v.get("subs", [])})

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

async def respond(request, msg, name):
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
    if request.query_params.get("dl"):
        ext = os.path.splitext(getattr(media, "file_name", "") or "")[1] or ".mp4"
        base = re.sub(r'[\\/:*?"<>|]+', "", name).strip() or "video"
        headers["Content-Disposition"] = "attachment; filename*=UTF-8''" + quote(base + ext)
    if partial:
        headers["Content-Range"] = f"bytes {start}-{end}/{size}"
    return StreamingResponse(body(), status_code=206 if partial else 200,
                             media_type=mime, headers=headers)

async def stream(request):
    vid, q = request.path_params["vid"], request.path_params["q"]
    src = videos().get(vid, {}).get("sources", {}).get(q)
    if not src:
        raise HTTPException(404, "Unknown source")
    msg = await get_msg(src["chat"], src["msg"])
    return await respond(request, msg, f"{await title_of(vid, videos().get(vid, {}))} {q}p")

async def vstream(request):
    raw, mid = request.path_params["chat"], request.path_params["mid"]
    chat = int(raw) if re.fullmatch(r"-?\d+", raw) else raw
    if not hmac.compare_digest(request.query_params.get("s", "").encode(), sign(chat, mid).encode()):
        raise HTTPException(403, "Bad link")
    msg = await get_msg(chat, mid)
    return await respond(request, msg, name_from(msg, msg.video or msg.document) or f"video {mid}")

async def health(request):
    return Response("ok", headers={"Cache-Control": "no-store"})

def parse_link(link):
    m = re.match(r"https?://(?:www\.)?t\.me/(c/)?([^/?#]+)/(?:\d+/)?(\d+)", str(link).strip())
    if not m:
        return None
    if m.group(1):
        return (int("-100" + m.group(2)), int(m.group(3))) if m.group(2).isdigit() else None
    return m.group(2), int(m.group(3))

async def admin_sign(request):
    """Used by your own admin panel: Telegram message link -> signed stream path."""
    await guard(request)
    b = await request.json()
    p = parse_link(b.get("link", ""))
    if not p:
        return JSONResponse({"ok": False, "error": "Not a Telegram message link"})
    chat, mid = p
    try:
        msg = await tg.get_messages(chat, mid)
    except Exception as ex:
        return JSONResponse({"ok": False, "error": f"Telegram: {ex}"[:200]})
    media = media_of(msg)
    if not media:
        return JSONResponse({"ok": False, "error": "That message has no video (is the bot an admin of the channel?)"})
    _cache[(chat, mid)] = msg
    return JSONResponse({"ok": True, "path": f"/v/{chat}/{mid}?s={sign(chat, mid)}",
                         "title": clean_title(name_from(msg, media)),
                         "q": bucket(getattr(media, "height", 0) or 0)})

# ---------- admin panel api (needs ADMIN_PASSWORD) ----------
async def guard(request):
    if not ADMIN:
        raise HTTPException(503, "ADMIN_PASSWORD is not set on the server")
    if not hmac.compare_digest(request.headers.get("x-admin", "").encode(), ADMIN.encode()):
        await asyncio.sleep(0.6)
        raise HTTPException(401, "Wrong password")

async def admin_list(request):
    await guard(request)
    vs = videos()
    titles = await asyncio.gather(*[title_of(k, v) for k, v in vs.items()])
    items = [{"id": k, "title": ti, "added": v.get("added", ""), "manual": k not in _lib or k in _manual(),
              "q": sorted(v["sources"], key=int, reverse=True)}
             for (k, v), ti in zip(vs.items(), titles)]
    items.sort(key=lambda x: x["added"], reverse=True)
    return JSONResponse({"persist": "github" if GH_ON else "local", "items": items})

def _manual():
    try:
        with open("videos.json", encoding="utf-8") as f:
            return json.load(f)
    except FileNotFoundError:
        return {}

async def admin_add(request):
    await guard(request)
    b = await request.json()
    m = re.match(r"https?://(?:www\.)?t\.me/(c/)?([^/?#]+)/(?:\d+/)?(\d+)", str(b.get("link", "")).strip())
    if not m:
        return JSONResponse({"ok": False, "error": "Not a Telegram message link"})
    if m.group(1):
        if not m.group(2).isdigit():
            return JSONResponse({"ok": False, "error": "Bad channel number in link"})
        chat = int("-100" + m.group(2))
    else:
        chat = m.group(2)
    mid = int(m.group(3))
    try:
        msg = await tg.get_messages(chat, mid)
    except Exception as ex:
        return JSONResponse({"ok": False, "error": f"Telegram: {ex}"[:200]})
    media = media_of(msg)
    if not media:
        return JSONResponse({"ok": False, "error": "That message has no video (is the bot an admin of the channel?)"})
    _cache[(chat, mid)] = msg
    q = str(b.get("quality") or "")
    q = q if q.isdigit() else None
    target = b.get("target") or None
    if target not in _lib:
        target = None
    key = target or slug(clean_title(name_from(msg, media))) or f"m{mid}"
    merged = key in _lib
    key, e, q = entry_for(name_from(msg, media), media, chat, mid, q=q, target=target,
                          date=msg.date.isoformat() if msg.date else None)
    out = {"ok": True, "id": key, "title": e["title"], "q": q, "merged": merged}
    try:
        await save_lib()
    except Exception as ex:
        out["warn"] = f"saved in memory only, GitHub sync failed: {ex}"[:200]
    return JSONResponse(out)

async def admin_delete(request):
    await guard(request)
    b = await request.json()
    if b.get("id") not in _lib:
        return JSONResponse({"ok": False, "error": "Not found (videos from videos.json cannot be deleted here)"})
    _lib.pop(b["id"])
    try:
        await save_lib()
    except Exception as ex:
        return JSONResponse({"ok": True, "warn": f"GitHub sync failed: {ex}"[:200]})
    return JSONResponse({"ok": True})

async def admin_rename(request):
    await guard(request)
    b = await request.json()
    title = str(b.get("title", "")).strip()[:150]
    if b.get("id") not in _lib or not title:
        return JSONResponse({"ok": False, "error": "Nothing to rename"})
    _lib[b["id"]]["title"] = title
    try:
        await save_lib()
    except Exception as ex:
        return JSONResponse({"ok": True, "warn": f"GitHub sync failed: {ex}"[:200]})
    return JSONResponse({"ok": True})

@asynccontextmanager
async def lifespan(app):
    await tg.start()
    await init_lib()
    yield
    await tg.stop()

app = Starlette(lifespan=lifespan, middleware=[
    Middleware(CORSMiddleware, allow_origins=ORIGINS, allow_methods=["GET", "POST", "OPTIONS"],
               allow_headers=["x-admin", "content-type"])], routes=[
    Route("/", index),
    Route("/library", library_page),
    Route("/admin", admin_page),
    Route("/api/library", library_api),
    Route("/api/video/{vid}", info),
    Route("/api/admin/list", admin_list),
    Route("/api/admin/add", admin_add, methods=["POST"]),
    Route("/api/admin/delete", admin_delete, methods=["POST"]),
    Route("/api/admin/rename", admin_rename, methods=["POST"]),
    Route("/sub/{chat}/{mid}.vtt", sub),
    Route("/stream/{vid}/{q}", stream),
    Route("/v/{chat}/{mid:int}", vstream),
    Route("/health", health),
    Route("/api/admin/sign", admin_sign, methods=["POST"]),
    Mount("/subs", StaticFiles(directory="subs")),
])
