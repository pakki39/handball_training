import os
import posixpath
import shutil
import sqlite3
import mimetypes
import re
import time
import threading
import subprocess
import math
from datetime import datetime, timezone

from flask import Flask, jsonify, render_template, request, send_file, g, Response

import config

app = Flask(__name__)
app.config.from_object(config)


def _ensure_dirs():
    os.makedirs(app.config["VIDEO_ROOT"], exist_ok=True)
    os.makedirs(app.config["TARGET_ROOT"], exist_ok=True)
    os.makedirs(app.config["EXPORT_ROOT"], exist_ok=True)
    os.makedirs(os.path.dirname(app.config["DB_PATH"]), exist_ok=True)


def _utc_now_iso():
    return datetime.now(timezone.utc).isoformat()


def _is_allowed_video_filename(name: str) -> bool:
    _, ext = os.path.splitext(name)
    return ext.lower() in app.config["ALLOWED_VIDEO_EXTENSIONS"]


def _normalize_relpath(relpath: str | None) -> str:
    if relpath is None:
        return ""
    relpath = relpath.strip()
    relpath = relpath.lstrip("/\\")
    if relpath == "":
        return ""
    relpath = relpath.replace("\\", "/")
    norm = posixpath.normpath(relpath)
    if norm in (".", ""):
        return ""
    if norm.startswith("../") or norm == ".." or "/../" in f"/{norm}/":
        raise ValueError("invalid_path")
    return norm


def _safe_abs_path(root: str, relpath: str | None) -> tuple[str, str]:
    root_abs = os.path.abspath(root)
    norm = _normalize_relpath(relpath)

    abs_path = os.path.abspath(os.path.join(root_abs, norm))
    if abs_path == root_abs:
        return abs_path, norm

    root_prefix = root_abs + os.sep
    if not abs_path.startswith(root_prefix):
        raise ValueError("invalid_path")

    return abs_path, norm


def _get_db():
    db = getattr(g, "_db", None)
    if db is None:
        db = sqlite3.connect(app.config["DB_PATH"])
        db.row_factory = sqlite3.Row
        g._db = db
    return db


@app.teardown_appcontext
def _close_db(_exc):
    db = getattr(g, "_db", None)
    if db is not None:
        db.close()


def _init_db():
    db = _get_db()
    db.execute(
        """
        CREATE TABLE IF NOT EXISTS queue_items (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            target_relpath TEXT UNIQUE NOT NULL,
            source_relpath TEXT,
            filename TEXT NOT NULL,
            position INTEGER NOT NULL,
            added_at TEXT NOT NULL
        )
        """
    )
    cols = [r[1] for r in db.execute("PRAGMA table_info(queue_items)").fetchall()]
    if "source_relpath" not in cols:
        try:
            db.execute("ALTER TABLE queue_items ADD COLUMN source_relpath TEXT")
        except Exception:
            pass
    db.commit()


def _queue_get_items():
    db = _get_db()
    rows = db.execute(
        """
        SELECT id, filename, target_relpath, added_at, position
        FROM queue_items
        ORDER BY position ASC
        """
    ).fetchall()
    return [dict(r) for r in rows]


def _queue_next_position():
    db = _get_db()
    row = db.execute("SELECT COALESCE(MAX(position), -1) AS maxpos FROM queue_items").fetchone()
    return int(row["maxpos"]) + 1


def _queue_get_by_target_relpath(target_relpath: str):
    db = _get_db()
    row = db.execute(
        """
        SELECT id, filename, target_relpath, added_at, position
        FROM queue_items
        WHERE target_relpath = ?
        """,
        (target_relpath,),
    ).fetchone()
    return dict(row) if row else None


def _queue_get_by_source_relpath(source_relpath: str):
    db = _get_db()
    row = db.execute(
        """
        SELECT id, filename, target_relpath, source_relpath, added_at, position
        FROM queue_items
        WHERE source_relpath = ?
        """,
        (source_relpath,),
    ).fetchone()
    return dict(row) if row else None


def _queue_get_by_id(item_id: int):
    db = _get_db()
    row = db.execute(
        """
        SELECT id, filename, target_relpath, added_at, position
        FROM queue_items
        WHERE id = ?
        """,
        (item_id,),
    ).fetchone()
    return dict(row) if row else None


def _queue_add_item(target_relpath: str, source_relpath: str | None = None):
    target_relpath = _normalize_relpath(target_relpath)

    existing = _queue_get_by_target_relpath(target_relpath)
    if existing:
        return existing, False

    filename = os.path.basename(target_relpath)
    if not _is_allowed_video_filename(filename):
        raise ValueError("invalid_video_extension")

    target_abs, _ = _safe_abs_path(app.config["TARGET_ROOT"], target_relpath)
    if not os.path.isfile(target_abs):
        raise FileNotFoundError("target_not_found")

    db = _get_db()
    pos = _queue_next_position()
    added_at = _utc_now_iso()
    db.execute(
        """
        INSERT INTO queue_items (target_relpath, source_relpath, filename, position, added_at)
        VALUES (?, ?, ?, ?, ?)
        """,
        (target_relpath, _normalize_relpath(source_relpath) if source_relpath else None, filename, pos, added_at),
    )
    db.commit()
    item = _queue_get_by_target_relpath(target_relpath)
    return item, True


def _clear_directory_contents(dir_abs: str):
    with os.scandir(dir_abs) as it:
        for entry in it:
            p = os.path.join(dir_abs, entry.name)
            if entry.is_dir(follow_symlinks=False):
                shutil.rmtree(p)
            else:
                os.remove(p)


_TAG_INDEX_CACHE = {}
_TAG_INDEX_LOCK = threading.Lock()
_TAG_INDEX_BUILDING = False
_TAG_INDEX_LAST_ERROR = None


def _extract_tags_from_filename(filename: str) -> list[str]:
    matches = re.findall(r"\[(.*?)\]", filename)
    out = []
    seen = set()
    for m in matches:
        for t in re.split(r"[\s,]+", m.strip()):
            if not t:
                continue
            key = t.lower()
            if key in seen:
                continue
            seen.add(key)
            out.append(t)
    return out


def _parse_filename_for_tag_block(filename: str):
    base, ext = os.path.splitext(filename)
    m = re.search(r"(?:\s*\[[^\]]*\])+\s*$", base)
    if not m:
        return base, ext, []
    tag_part = base[m.start():]
    base_stem = base[: m.start()].rstrip()
    tags = _extract_tags_from_filename(tag_part)
    return base_stem, ext, tags


def _build_filename_with_tags(base_stem: str, ext: str, tags: list[str]):
    base_stem = base_stem.strip()
    if tags:
        return f"{base_stem} [{ ' '.join(tags) }]{ext}"
    return f"{base_stem}{ext}"


def _validate_tag_token(tag: str) -> str:
    if not isinstance(tag, str):
        raise ValueError("bad_tag")
    t = tag.strip()
    if t == "":
        raise ValueError("bad_tag")
    if any(ch in t for ch in ("[", "]", "/", "\\")):
        raise ValueError("bad_tag")
    if re.search(r"\s", t):
        raise ValueError("bad_tag")
    if len(t) > 48:
        raise ValueError("bad_tag")
    return t


def _posix_relpath(path: str) -> str:
    return os.path.relpath(path).replace("\\", "/")


def _build_tag_index(tag_root_abs: str, video_root_abs: str):
    entries = []
    for dirpath, dirnames, filenames in os.walk(tag_root_abs, followlinks=False):
        dirnames[:] = [d for d in dirnames if not d.startswith(".")]
        for fn in filenames:
            if fn.startswith("."):
                continue
            if not _is_allowed_video_filename(fn):
                continue
            tags = _extract_tags_from_filename(fn)
            abs_path = os.path.join(dirpath, fn)
            try:
                rel_to_video = os.path.relpath(abs_path, video_root_abs)
            except ValueError:
                continue
            rel_to_video = _normalize_relpath(rel_to_video)
            entries.append(
                {
                    "relpath": rel_to_video,
                    "name": fn,
                    "name_lower": fn.lower(),
                    "tags": tags,
                    "tags_lower": {t.lower() for t in tags},
                }
            )
    entries.sort(key=lambda x: x["relpath"].lower())
    return entries


def _get_tag_index(refresh: bool = False):
    tag_root_abs = os.path.abspath(app.config["TAG_SCAN_ROOT"])
    video_root_abs = os.path.abspath(app.config["VIDEO_ROOT"])

    video_prefix = video_root_abs + os.sep
    if tag_root_abs != video_root_abs and not tag_root_abs.startswith(video_prefix):
        raise ValueError("tag_root_outside_video_root")

    if not os.path.isdir(tag_root_abs):
        raise FileNotFoundError("tag_root_missing")

    cache_key = tag_root_abs
    now = time.time()
    with _TAG_INDEX_LOCK:
        cached = _TAG_INDEX_CACHE.get(cache_key)
        if cached and not refresh:
            if now - cached.get("built_at", 0) < 30:
                return cached

    entries = _build_tag_index(tag_root_abs, video_root_abs)
    cached = {"root_abs": tag_root_abs, "built_at": now, "entries": entries}
    with _TAG_INDEX_LOCK:
        _TAG_INDEX_CACHE[cache_key] = cached
    return cached


def _start_tag_index_build():
    def _worker():
        global _TAG_INDEX_BUILDING, _TAG_INDEX_LAST_ERROR
        with app.app_context():
            with _TAG_INDEX_LOCK:
                if _TAG_INDEX_BUILDING:
                    return
                _TAG_INDEX_BUILDING = True
                _TAG_INDEX_LAST_ERROR = None
            try:
                _get_tag_index(refresh=True)
            except Exception as e:
                with _TAG_INDEX_LOCK:
                    _TAG_INDEX_LAST_ERROR = str(e)
            finally:
                with _TAG_INDEX_LOCK:
                    _TAG_INDEX_BUILDING = False

    t = threading.Thread(target=_worker, daemon=True)
    t.start()


def _json_error(message: str, status: int = 400, code: str | None = None, details=None):
    payload = {"ok": False, "error": message}
    if code is not None:
        payload["code"] = code
    if details is not None:
        payload["details"] = details
    return jsonify(payload), status


def _send_file_with_range(abs_path: str, mimetype: str):
    def _resp_range_not_satisfiable(size: int):
        resp = Response(status=416, mimetype=mimetype or "application/octet-stream")
        resp.headers["Content-Range"] = f"bytes */{size}"
        resp.headers["Accept-Ranges"] = "bytes"
        resp.headers["Cache-Control"] = "no-store"
        resp.headers["Pragma"] = "no-cache"
        resp.headers["Expires"] = "0"
        return resp

    range_header = request.headers.get("Range")
    if not range_header:
        resp = send_file(abs_path, conditional=False, mimetype=mimetype or "application/octet-stream")
        try:
            resp.headers.setdefault("Accept-Ranges", "bytes")
            resp.headers.setdefault("Cache-Control", "no-store")
            resp.headers.setdefault("Pragma", "no-cache")
            resp.headers.setdefault("Expires", "0")
        except Exception:
            pass
        return resp

    m = re.match(r"^bytes=(\d*)-(\d*)$", range_header.strip())
    if not m:
        resp = send_file(abs_path, conditional=False, mimetype=mimetype or "application/octet-stream")
        try:
            resp.headers.setdefault("Accept-Ranges", "bytes")
            resp.headers.setdefault("Cache-Control", "no-store")
            resp.headers.setdefault("Pragma", "no-cache")
            resp.headers.setdefault("Expires", "0")
        except Exception:
            pass
        return resp

    size = os.path.getsize(abs_path)
    start_s, end_s = m.groups()

    if start_s == "" and end_s == "":
        return _resp_range_not_satisfiable(size)

    try:
        if start_s == "":
            length = int(end_s)
            if length <= 0:
                return _resp_range_not_satisfiable(size)
            start = max(0, size - length)
            end = size - 1
        else:
            start = int(start_s)
            end = int(end_s) if end_s != "" else size - 1
    except Exception:
        return _resp_range_not_satisfiable(size)

    if start < 0 or end < 0 or start >= size or start > end:
        return _resp_range_not_satisfiable(size)
    end = min(end, size - 1)

    length = end - start + 1

    def _gen():
        with open(abs_path, "rb") as f:
            f.seek(start)
            remaining = length
            while remaining > 0:
                chunk = f.read(min(1024 * 1024, remaining))
                if not chunk:
                    break
                remaining -= len(chunk)
                yield chunk

    resp = Response(_gen(), status=206, mimetype=mimetype or "application/octet-stream")
    resp.headers["Content-Range"] = f"bytes {start}-{end}/{size}"
    resp.headers["Accept-Ranges"] = "bytes"
    resp.headers["Content-Length"] = str(length)
    resp.headers["Cache-Control"] = "no-store"
    resp.headers["Pragma"] = "no-cache"
    resp.headers["Expires"] = "0"
    return resp


def _guess_video_mimetype(path: str) -> str:
    _, ext = os.path.splitext(path)
    ext = ext.lower()
    if ext == ".mp4":
        return "video/mp4"
    if ext == ".mkv":
        return "video/x-matroska"
    if ext == ".webm":
        return "video/webm"
    if ext == ".mov":
        return "video/quicktime"
    mime, _ = mimetypes.guess_type(path)
    return mime or "application/octet-stream"


def _list_dir(root: str, relpath: str | None):
    abs_dir, norm = _safe_abs_path(root, relpath)
    if not os.path.isdir(abs_dir):
        raise FileNotFoundError("not_a_directory")

    folders = []
    videos = []

    with os.scandir(abs_dir) as it:
        for entry in it:
            if entry.name.startswith("."):
                continue
            if entry.is_dir(follow_symlinks=False):
                child_rel = entry.name if norm == "" else f"{norm}/{entry.name}"
                folders.append({"name": entry.name, "relpath": child_rel})
            elif entry.is_file(follow_symlinks=False):
                if _is_allowed_video_filename(entry.name):
                    child_rel = entry.name if norm == "" else f"{norm}/{entry.name}"
                    videos.append({"name": entry.name, "relpath": child_rel})

    folders.sort(key=lambda x: x["name"].lower())
    videos.sort(key=lambda x: x["name"].lower())

    if norm == "":
        parent_path = None
    else:
        parent_norm = posixpath.dirname(norm)
        parent_path = "" if parent_norm in ("", ".") else parent_norm

    return {
        "current_path": norm,
        "parent_path": parent_path,
        "folders": folders,
        "videos": videos,
    }


def _list_dirs_only(root: str, relpath: str | None):
    data = _list_dir(root, relpath)
    return {
        "current_path": data["current_path"],
        "parent_path": data["parent_path"],
        "folders": data["folders"],
    }


def _unique_destination_filename(dest_dir_abs: str, filename: str):
    base, ext = os.path.splitext(filename)
    candidate = filename
    i = 1
    while os.path.exists(os.path.join(dest_dir_abs, candidate)):
        candidate = f"{base}_{i}{ext}"
        i += 1
    return candidate


def _unique_export_name(dest_dir_abs: str, prefixed_filename: str):
    base, ext = os.path.splitext(prefixed_filename)
    candidate = prefixed_filename
    i = 1
    while os.path.exists(os.path.join(dest_dir_abs, candidate)):
        candidate = f"{base}_{i}{ext}"
        i += 1
    return candidate


@app.route("/")
def index():
    return render_template("index.html")


@app.route("/api/list", methods=["GET"])
def api_list():
    rel = request.args.get("path", "")
    try:
        data = _list_dir(app.config["VIDEO_ROOT"], rel)
        return jsonify(data)
    except ValueError:
        return _json_error("Ungültiger Pfad.", 400, code="invalid_path")
    except FileNotFoundError:
        return _json_error("Ordner nicht gefunden.", 404, code="not_found")


@app.route("/api/transfer", methods=["POST"])
def api_transfer():
    body = request.get_json(silent=True) or {}
    source_path = body.get("source_path")
    target_subdir = body.get("target_subdir", "")
    mode = body.get("mode", "copy")

    if not source_path or not isinstance(source_path, str):
        return _json_error("source_path fehlt.", 400, code="bad_request")
    if mode not in ("copy", "move"):
        return _json_error("Ungültiger mode.", 400, code="bad_request")

    try:
        src_abs, src_norm = _safe_abs_path(app.config["VIDEO_ROOT"], source_path)
        if not os.path.isfile(src_abs):
            return _json_error("Quelldatei nicht gefunden.", 404, code="not_found")
        if not _is_allowed_video_filename(os.path.basename(src_norm)):
            return _json_error("Nicht erlaubte Video-Endung.", 400, code="invalid_video_extension")

        existing = _queue_get_by_source_relpath(src_norm)
        if existing:
            return _json_error("Dieses Video ist bereits in der Queue.", 409, code="duplicate")

        tgt_dir_abs, tgt_subdir_norm = _safe_abs_path(app.config["TARGET_ROOT"], target_subdir)
        os.makedirs(tgt_dir_abs, exist_ok=True)

        src_filename = os.path.basename(src_norm)
        dest_filename = _unique_destination_filename(tgt_dir_abs, src_filename)

        target_relpath = dest_filename if tgt_subdir_norm == "" else f"{tgt_subdir_norm}/{dest_filename}"
        target_abs, _ = _safe_abs_path(app.config["TARGET_ROOT"], target_relpath)

        if mode == "copy":
            shutil.copy2(src_abs, target_abs)
        else:
            shutil.move(src_abs, target_abs)

        item, _created = _queue_add_item(target_relpath, source_relpath=src_norm)
        return jsonify(
            {
                "ok": True,
                "queue_item": {
                    "id": item["id"],
                    "relpath": src_norm,
                    "filename": item["filename"],
                    "target_relpath": item["target_relpath"],
                },
            }
        )
    except ValueError as e:
        if str(e) == "invalid_video_extension":
            return _json_error("Nicht erlaubte Video-Endung.", 400, code="invalid_video_extension")
        return _json_error("Ungültiger Pfad.", 400, code="invalid_path")
    except FileNotFoundError:
        return _json_error("Datei/Ordner nicht gefunden.", 404, code="not_found")
    except sqlite3.IntegrityError:
        return _json_error("Dieses Video ist bereits in der Queue.", 409, code="duplicate")
    except Exception:
        return _json_error("Transfer fehlgeschlagen.", 500, code="server_error")


@app.route("/api/queue", methods=["GET"])
def api_queue():
    items = _queue_get_items()
    return jsonify({"items": items})


@app.route("/api/queue/add", methods=["POST"])
def api_queue_add():
    body = request.get_json(silent=True) or {}
    target_relpath = body.get("target_relpath")
    if not target_relpath or not isinstance(target_relpath, str):
        return _json_error("target_relpath fehlt.", 400, code="bad_request")

    try:
        item, _created = _queue_add_item(target_relpath)
        return jsonify({"ok": True, "item": item})
    except ValueError as e:
        if str(e) == "invalid_video_extension":
            return _json_error("Nicht erlaubte Video-Endung.", 400, code="invalid_video_extension")
        return _json_error("Ungültiger Pfad.", 400, code="invalid_path")
    except FileNotFoundError:
        return _json_error("Zieldatei nicht gefunden.", 404, code="not_found")
    except sqlite3.IntegrityError:
        return _json_error("Dieses Video ist bereits in der Queue.", 409, code="duplicate")
    except Exception:
        return _json_error("Queue add fehlgeschlagen.", 500, code="server_error")


@app.route("/api/queue/reorder", methods=["POST"])
def api_queue_reorder():
    body = request.get_json(silent=True) or {}
    ordered_ids = body.get("ordered_ids")
    ordered_paths = body.get("ordered_target_relpaths")

    if ordered_ids is None and ordered_paths is None:
        return _json_error("ordered_ids oder ordered_target_relpaths fehlt.", 400, code="bad_request")

    items = _queue_get_items()

    try:
        if ordered_ids is not None:
            if not isinstance(ordered_ids, list) or not all(isinstance(x, int) for x in ordered_ids):
                return _json_error("ordered_ids muss Liste von ints sein.", 400, code="bad_request")
            current_ids = [it["id"] for it in items]
            if sorted(current_ids) != sorted(ordered_ids):
                return _json_error("ordered_ids muss exakt alle aktuellen IDs enthalten.", 400, code="bad_request")
            order = ordered_ids
            id_to_pos = {item_id: idx for idx, item_id in enumerate(order)}
            db = _get_db()
            with db:
                for item_id, pos in id_to_pos.items():
                    db.execute("UPDATE queue_items SET position = ? WHERE id = ?", (pos, item_id))
        else:
            if not isinstance(ordered_paths, list) or not all(isinstance(x, str) for x in ordered_paths):
                return _json_error("ordered_target_relpaths muss Liste von strings sein.", 400, code="bad_request")
            current_paths = [it["target_relpath"] for it in items]
            normalized = [_normalize_relpath(p) for p in ordered_paths]
            if sorted(current_paths) != sorted(normalized):
                return _json_error(
                    "ordered_target_relpaths muss exakt alle aktuellen target_relpaths enthalten.",
                    400,
                    code="bad_request",
                )
            order = normalized
            path_to_pos = {p: idx for idx, p in enumerate(order)}
            db = _get_db()
            with db:
                for p, pos in path_to_pos.items():
                    db.execute("UPDATE queue_items SET position = ? WHERE target_relpath = ?", (pos, p))

        return jsonify({"ok": True})
    except ValueError:
        return _json_error("Ungültiger Pfad.", 400, code="invalid_path")
    except Exception:
        return _json_error("Reorder fehlgeschlagen.", 500, code="server_error")


@app.route("/api/queue/item", methods=["DELETE"])
def api_queue_delete_item():
    item_id = request.args.get("id")
    if not item_id:
        return _json_error("id fehlt.", 400, code="bad_request")
    try:
        item_id_int = int(item_id)
    except ValueError:
        return _json_error("id muss integer sein.", 400, code="bad_request")

    existing = _queue_get_by_id(item_id_int)
    if not existing:
        return _json_error("Item nicht gefunden.", 404, code="not_found")

    db = _get_db()
    with db:
        db.execute("DELETE FROM queue_items WHERE id = ?", (item_id_int,))
    return jsonify({"ok": True})


@app.route("/api/queue/clear", methods=["POST"])
def api_queue_clear():
    db = _get_db()
    with db:
        row = db.execute("SELECT COUNT(1) AS cnt FROM queue_items").fetchone()
        count = int(row["cnt"]) if row else 0
        db.execute("DELETE FROM queue_items")
    return jsonify({"ok": True, "deleted_count": count})


@app.route("/api/export/list", methods=["GET"])
def api_export_list():
    rel = request.args.get("path", "")
    try:
        data = _list_dirs_only(app.config["EXPORT_ROOT"], rel)
        return jsonify(data)
    except ValueError:
        return _json_error("Ungültiger Pfad.", 400, code="invalid_path")
    except FileNotFoundError:
        return _json_error("Ordner nicht gefunden.", 404, code="not_found")


@app.route("/api/export/mkdir", methods=["POST"])
def api_export_mkdir():
    body = request.get_json(silent=True) or {}
    parent_path = body.get("parent_path", "")
    folder_name = body.get("folder_name")

    if folder_name is None or not isinstance(folder_name, str) or folder_name.strip() == "":
        return _json_error("folder_name fehlt.", 400, code="bad_request")

    folder_name = folder_name.strip()
    if "/" in folder_name or "\\" in folder_name:
        return _json_error("folder_name darf keine Pfadtrenner enthalten.", 400, code="bad_request")

    if folder_name in (".", ".."):
        return _json_error("Ungültiger Ordnername.", 400, code="bad_request")

    try:
        parent_abs, parent_norm = _safe_abs_path(app.config["EXPORT_ROOT"], parent_path)
        if not os.path.isdir(parent_abs):
            return _json_error("Parent-Ordner nicht gefunden.", 404, code="not_found")

        new_rel = folder_name if parent_norm == "" else f"{parent_norm}/{folder_name}"
        new_abs, _ = _safe_abs_path(app.config["EXPORT_ROOT"], new_rel)
        os.makedirs(new_abs, exist_ok=False)
        return jsonify({"ok": True, "created": new_rel})
    except FileExistsError:
        return _json_error("Ordner existiert bereits.", 409, code="already_exists")
    except ValueError:
        return _json_error("Ungültiger Pfad.", 400, code="invalid_path")
    except Exception:
        return _json_error("Ordner konnte nicht erstellt werden.", 500, code="server_error")


@app.route("/api/export/run", methods=["POST"])
def api_export_run():
    body = request.get_json(silent=True) or {}
    destination_subdir = body.get("destination_subdir", "")
    clear_destination = bool(body.get("clear_destination", False))

    try:
        dest_abs, dest_norm = _safe_abs_path(app.config["EXPORT_ROOT"], destination_subdir)
        os.makedirs(dest_abs, exist_ok=True)

        has_any = any(True for _ in os.scandir(dest_abs))
        if has_any and not clear_destination:
            return _json_error(
                "Zielordner ist nicht leer.",
                409,
                code="destination_not_empty",
                details={"destination_relpath": dest_norm},
            )
        if has_any and clear_destination:
            _clear_directory_contents(dest_abs)

        items = _queue_get_items()
        if not items:
            return _json_error("Queue ist leer.", 400, code="empty_queue")

        pad = max(3, len(str(len(items))))

        exported = []
        skipped = []

        for idx, it in enumerate(items, start=1):
            src_abs, _ = _safe_abs_path(app.config["TARGET_ROOT"], it["target_relpath"])
            if not os.path.isfile(src_abs):
                skipped.append({"id": it["id"], "target_relpath": it["target_relpath"], "reason": "missing_source"})
                continue

            base, ext = os.path.splitext(it["filename"])
            prefixed = f"{idx:0{pad}d}_{base}{ext}"
            prefixed = _unique_export_name(dest_abs, prefixed)
            dest_file_abs = os.path.join(dest_abs, prefixed)

            shutil.copy2(src_abs, dest_file_abs)

            dest_rel = prefixed if dest_norm == "" else f"{dest_norm}/{prefixed}"
            exported.append({"id": it["id"], "dest_relpath": dest_rel, "dest_filename": prefixed})

        return jsonify(
            {
                "ok": True,
                "destination_relpath": dest_norm,
                "exported_count": len(exported),
                "skipped_count": len(skipped),
                "exported": exported,
                "skipped": skipped,
            }
        )
    except ValueError:
        return _json_error("Ungültiger Pfad.", 400, code="invalid_path")
    except Exception:
        return _json_error("Export fehlgeschlagen.", 500, code="server_error")


@app.route("/api/tags/search", methods=["POST"])
def api_tags_search():
    body = request.get_json(silent=True) or {}
    query = body.get("query", "")
    mode = body.get("mode", "and")
    refresh = bool(body.get("refresh", False))
    limit = body.get("limit", 200)

    if not isinstance(query, str):
        return _json_error("query muss string sein.", 400, code="bad_request")
    if mode not in ("and", "or"):
        return _json_error("mode muss 'and' oder 'or' sein.", 400, code="bad_request")
    try:
        limit = int(limit)
    except Exception:
        return _json_error("limit muss int sein.", 400, code="bad_request")
    limit = max(1, min(2000, limit))

    tokens = [t for t in re.split(r"[\s,]+", query.strip()) if t]
    if not tokens:
        return jsonify({"ok": True, "query": "", "mode": mode, "results": [], "count": 0})
    want = [t.lower() for t in tokens]

    try:
        idx = _get_tag_index(refresh=refresh)
        entries = idx["entries"]

        results = []
        for e in entries:
            tags_lower = e["tags_lower"]
            if mode == "and":
                ok = all(t in tags_lower for t in want)
            else:
                ok = any(t in tags_lower for t in want)
            if not ok:
                continue
            results.append({"relpath": e["relpath"], "name": e["name"], "tags": e["tags"]})
            if len(results) >= limit:
                break

        return jsonify(
            {
                "ok": True,
                "query": query,
                "mode": mode,
                "count": len(results),
                "results": results,
            }
        )
    except ValueError as e:
        if str(e) == "tag_root_outside_video_root":
            return _json_error("TAG_SCAN_ROOT muss innerhalb von VIDEO_ROOT liegen.", 400, code="bad_request")
        return _json_error("Ungültiger Pfad.", 400, code="invalid_path")
    except FileNotFoundError:
        return _json_error("TAG_SCAN_ROOT nicht gefunden.", 404, code="not_found")
    except Exception:
        return _json_error("Tagsuche fehlgeschlagen.", 500, code="server_error")


@app.route("/api/tags/edit", methods=["POST"])
def api_tags_edit():
    body = request.get_json(silent=True) or {}
    relpath = body.get("relpath")
    action = body.get("action")
    tag = body.get("tag")

    if not relpath or not isinstance(relpath, str):
        return _json_error("relpath fehlt.", 400, code="bad_request")
    if action not in ("add", "remove"):
        return _json_error("action muss 'add' oder 'remove' sein.", 400, code="bad_request")
    try:
        tag_tok = _validate_tag_token(tag)
    except ValueError:
        return _json_error("Ungültiger Tag.", 400, code="bad_request")

    try:
        src_abs, src_norm = _safe_abs_path(app.config["VIDEO_ROOT"], relpath)
        if not os.path.isfile(src_abs):
            return _json_error("Datei nicht gefunden.", 404, code="not_found")

        old_filename = os.path.basename(src_norm)
        base_stem, ext, tags = _parse_filename_for_tag_block(old_filename)
        tags_norm = []
        seen = set()
        for t in tags:
            k = t.lower()
            if k in seen:
                continue
            seen.add(k)
            tags_norm.append(t)

        tag_lower = tag_tok.lower()

        if action == "add":
            if tag_lower not in {t.lower() for t in tags_norm}:
                tags_norm.append(tag_tok)
        else:
            tags_norm = [t for t in tags_norm if t.lower() != tag_lower]

        new_filename = _build_filename_with_tags(base_stem, ext, tags_norm)
        if new_filename == old_filename:
            return jsonify({"ok": True, "changed": False, "relpath": src_norm, "name": old_filename, "tags": tags_norm})

        dir_norm = posixpath.dirname(src_norm)
        new_relpath = new_filename if dir_norm in ("", ".") else f"{dir_norm}/{new_filename}"
        dst_abs, dst_norm = _safe_abs_path(app.config["VIDEO_ROOT"], new_relpath)

        if os.path.exists(dst_abs):
            return _json_error("Zieldatei existiert bereits.", 409, code="conflict")

        os.rename(src_abs, dst_abs)
        _start_tag_index_build()
        return jsonify({"ok": True, "changed": True, "relpath": dst_norm, "name": new_filename, "tags": tags_norm})
    except ValueError:
        return _json_error("Ungültiger Pfad.", 400, code="invalid_path")
    except Exception:
        return _json_error("Tag-Update fehlgeschlagen.", 500, code="server_error")


def _next_clip_filename(dir_abs: str, base_stem: str, ext: str, tags: list[str]) -> tuple[str, int]:
    i = 1
    while True:
        candidate_stem = f"{base_stem}_{i:02d}"
        candidate = _build_filename_with_tags(candidate_stem, ext, tags)
        if not os.path.exists(os.path.join(dir_abs, candidate)):
            return candidate, i
        i += 1


@app.route("/api/clips/create", methods=["POST"])
def api_clips_create():
    body = request.get_json(silent=True) or {}
    relpath = body.get("relpath")
    segments = body.get("segments")

    if not relpath or not isinstance(relpath, str):
        return _json_error("relpath fehlt.", 400, code="bad_request")
    if not isinstance(segments, list):
        return _json_error("segments muss eine Liste sein.", 400, code="bad_request")
    if len(segments) == 0:
        return _json_error("Keine Segmente angegeben.", 400, code="bad_request")
    if len(segments) > 100:
        return _json_error("Zu viele Segmente.", 400, code="bad_request")

    try:
        src_abs, src_norm = _safe_abs_path(app.config["VIDEO_ROOT"], relpath)
        if not os.path.isfile(src_abs):
            return _json_error("Datei nicht gefunden.", 404, code="not_found")
        if not _is_allowed_video_filename(os.path.basename(src_norm)):
            return _json_error("Nicht erlaubte Video-Endung.", 400, code="invalid_video_extension")

        old_filename = os.path.basename(src_norm)
        base_stem, ext, tags = _parse_filename_for_tag_block(old_filename)

        dir_norm = posixpath.dirname(src_norm)
        dir_abs = os.path.dirname(src_abs)

        created = []
        for seg in segments:
            if not isinstance(seg, dict):
                return _json_error("Segment hat falsches Format.", 400, code="bad_request")
            start = seg.get("start")
            end = seg.get("end")
            try:
                start_f = float(start)
                end_f = float(end)
            except Exception:
                return _json_error("start/end müssen Zahlen sein.", 400, code="bad_request")
            if not (math.isfinite(start_f) and math.isfinite(end_f)):
                return _json_error("start/end müssen endlich sein.", 400, code="bad_request")
            if start_f < 0:
                return _json_error("start muss >= 0 sein.", 400, code="bad_request")
            if end_f <= start_f + 0.05:
                return _json_error("Segment ist zu kurz.", 400, code="bad_request")

            duration = end_f - start_f

            new_filename, used_idx = _next_clip_filename(dir_abs, base_stem, ext, tags)
            new_relpath = new_filename if dir_norm in ("", ".") else f"{dir_norm}/{new_filename}"
            dst_abs, dst_norm = _safe_abs_path(app.config["VIDEO_ROOT"], new_relpath)

            cmd = [
                "ffmpeg",
                "-hide_banner",
                "-loglevel",
                "error",
                "-ss",
                str(start_f),
                "-t",
                str(duration),
                "-i",
                src_abs,
                "-c",
                "copy",
                "-movflags",
                "+faststart",
                dst_abs,
            ]

            try:
                subprocess.run(cmd, check=True, capture_output=True, text=True)
            except FileNotFoundError:
                return _json_error("ffmpeg nicht gefunden. Bitte ffmpeg installieren.", 500, code="ffmpeg_missing")
            except subprocess.CalledProcessError as e:
                msg = (e.stderr or "").strip() or "ffmpeg Fehler"
                return _json_error("Clip-Erstellung fehlgeschlagen.", 500, code="ffmpeg_error", details={"stderr": msg})

            if not os.path.isfile(dst_abs):
                return _json_error("Clip-Datei wurde nicht erstellt.", 500, code="server_error")

            created.append(
                {
                    "relpath": dst_norm,
                    "name": new_filename,
                    "start": start_f,
                    "end": end_f,
                    "index": used_idx,
                }
            )

        _start_tag_index_build()
        return jsonify({"ok": True, "source_relpath": src_norm, "created": created})
    except ValueError:
        return _json_error("Ungültiger Pfad.", 400, code="invalid_path")
    except Exception:
        return _json_error("Clip-Erstellung fehlgeschlagen.", 500, code="server_error")


@app.route("/api/name/search", methods=["POST"])
def api_name_search():
    body = request.get_json(silent=True) or {}
    query = body.get("query", "")
    limit = body.get("limit", 200)

    if not isinstance(query, str):
        return _json_error("query muss string sein.", 400, code="bad_request")
    try:
        limit = int(limit)
    except Exception:
        return _json_error("limit muss int sein.", 400, code="bad_request")
    limit = max(1, min(2000, limit))

    tokens = [t for t in re.split(r"[\s,]+", query.strip()) if t]
    if not tokens:
        return jsonify({"ok": True, "query": "", "results": [], "count": 0})
    want = [t.lower() for t in tokens]

    try:
        idx = _get_tag_index(refresh=False)
        entries = idx["entries"]

        results = []
        for e in entries:
            name_lower = e.get("name_lower", "")
            if not all(t in name_lower for t in want):
                continue
            results.append({"relpath": e["relpath"], "name": e["name"], "tags": e["tags"]})
            if len(results) >= limit:
                break

        return jsonify({"ok": True, "query": query, "count": len(results), "results": results})
    except ValueError as e:
        if str(e) == "tag_root_outside_video_root":
            return _json_error("TAG_SCAN_ROOT muss innerhalb von VIDEO_ROOT liegen.", 400, code="bad_request")
        return _json_error("Ungültiger Pfad.", 400, code="invalid_path")
    except FileNotFoundError:
        return _json_error("TAG_SCAN_ROOT nicht gefunden.", 404, code="not_found")
    except Exception:
        return _json_error("Dateiname-Suche fehlgeschlagen.", 500, code="server_error")


@app.route("/api/tags/list", methods=["GET"])
def api_tags_list():
    refresh = request.args.get("refresh", "").lower() in ("1", "true", "yes")
    try:
        if refresh:
            _start_tag_index_build()

        tag_root_abs = os.path.abspath(app.config["TAG_SCAN_ROOT"])
        with _TAG_INDEX_LOCK:
            cached = _TAG_INDEX_CACHE.get(tag_root_abs)
            building = bool(_TAG_INDEX_BUILDING)
            last_error = _TAG_INDEX_LAST_ERROR

        if not cached:
            return jsonify(
                {
                    "ok": True,
                    "building": building,
                    "error": last_error,
                    "tags": [],
                    "count": 0,
                }
            )

        counts = {}
        for e in cached.get("entries", []):
            for t in e.get("tags_lower", set()):
                counts[t] = counts.get(t, 0) + 1

        tags = [{"tag": k, "count": v} for k, v in counts.items()]
        tags.sort(key=lambda x: (-x["count"], x["tag"]))

        return jsonify(
            {
                "ok": True,
                "building": building,
                "error": last_error,
                "tags": tags,
                "count": len(tags),
            }
        )
    except ValueError as e:
        if str(e) == "tag_root_outside_video_root":
            return _json_error("TAG_SCAN_ROOT muss innerhalb von VIDEO_ROOT liegen.", 400, code="bad_request")
        return _json_error("Ungültiger Pfad.", 400, code="invalid_path")
    except FileNotFoundError:
        return _json_error("TAG_SCAN_ROOT nicht gefunden.", 404, code="not_found")
    except Exception:
        return _json_error("Tag-Liste konnte nicht geladen werden.", 500, code="server_error")


@app.route("/media/source/<path:relpath>", methods=["GET"])
def media_source(relpath):
    try:
        abs_path, norm = _safe_abs_path(app.config["VIDEO_ROOT"], relpath)
        if not _is_allowed_video_filename(os.path.basename(norm)):
            return _json_error("Nicht erlaubte Video-Endung.", 400, code="invalid_video_extension")
        if not os.path.isfile(abs_path):
            return _json_error("Datei nicht gefunden.", 404, code="not_found")
        mime = _guess_video_mimetype(abs_path)
        return _send_file_with_range(abs_path, mime)
    except ValueError:
        return _json_error("Ungültiger Pfad.", 400, code="invalid_path")


@app.route("/media/target/<path:relpath>", methods=["GET"])
def media_target(relpath):
    try:
        abs_path, norm = _safe_abs_path(app.config["TARGET_ROOT"], relpath)
        if not _is_allowed_video_filename(os.path.basename(norm)):
            return _json_error("Nicht erlaubte Video-Endung.", 400, code="invalid_video_extension")
        if not os.path.isfile(abs_path):
            return _json_error("Datei nicht gefunden.", 404, code="not_found")
        mime = _guess_video_mimetype(abs_path)
        return _send_file_with_range(abs_path, mime)
    except ValueError:
        return _json_error("Ungültiger Pfad.", 400, code="invalid_path")


with app.app_context():
    _ensure_dirs()
    _init_db()
    _start_tag_index_build()
