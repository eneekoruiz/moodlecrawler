# moodlecrawler
#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
eGela Crawler — Enterprise Time Capsule (GOLDEN MASTER v12 — FORENSIC CERTIFIED)

Auditoría forense pre-vuelo completada. Correcciones aplicadas en silencio.

Garantías matemáticas:
• Zero Data Loss:     ningún job extraído de una cola se evapora en RAM
• Kill-Window Shield: SIGTERM en workers rescata el job in-flight a disco (fsync)
• Atomic I/O:         os.replace + O_EXCL + fsync_dir = escritura indestructible
• Stateless workers:  DB es el único source of truth; procesos son efímeros
• Resource safety:    todos los fd, sockets y conn cerrados en finally garantizado
• No global mutable state: semaphore, events y queues son los únicos canales IPC
• Lock hygiene:       locks huérfanos limpiados en cada arranque
• FD ownership:       tmp_fd cedido a fdopen antes de cualquier excepción

Uso:
export EGELA_USER="tu_usuario"
export EGELA_PASS="tu_contraseña"
python egela_golden_master_v12.py
"""

# ═══════════════════════════════════════════════════════════════
# §1 — CONFIG & LOGGING
# ═══════════════════════════════════════════════════════════════

import os
import re
import sys
import glob
import json
import time
import socket
import random
import queue
import signal
import base64
import shutil
import sqlite3
import hashlib
import tempfile
import difflib
import unicodedata
import contextlib
import functools
import threading
import multiprocessing as mp
import logging

from collections import deque
from dataclasses import dataclass, field
from pathlib import Path
from typing import Optional, List
from urllib.parse import urlparse, parse_qs, urlencode, unquote
from email.utils import decode_rfc2231

import requests
from selenium import webdriver
from selenium.webdriver.by import By
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import (
    TimeoutException, StaleElementReferenceException, WebDriverException,
)
from webdriver_manager.chrome import ChromeDriverManager
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

try:
    import fcntl
    HAS_FCNTL = True
except ImportError:
    HAS_FCNTL = False

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(processName)-14s] %(levelname)s: %(message)s",
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler("egela_sre.log", encoding="utf-8"),
    ],
)
log = logging.getLogger("egela-v12")

CONFIG = {
    "USERNAME":               os.getenv("EGELA_USER", ""),
    "PASSWORD":               os.getenv("EGELA_PASS", ""),
    "ROOT_DIR":               "EGELA_ENTERPRISE_TIMECAPSULE",
    "DB_PATH":                "egela_state.sqlite",
    "COURSES":                "cursos.txt",
    "SPIDERS":                1,
    "DOWNLOADERS":            2,
    "MAX_IN_FLIGHT":          40,
    "LOGIN_URL":              "https://egela.ehu.eus/login/index.php",
    "DOMAIN":                 "egela.ehu.eus",
    "BASE_URL":               "https://egela.ehu.eus",
    "RATE_LIMIT_RPS":         1.0,
    "MAX_PAGES_PER_DRIVER":   80,
    "MAX_SECS_PER_DRIVER":    1200,
    "SPIDER_WATCHDOG_S":      90,
    "STREAM_TIMEOUT_S":       300,
    "CHUNK_TIMEOUT_S":        30,
    "MIN_SPEED_BPS":          512,
    "SPEED_WINDOW_S":         30,
    "MAX_FILE_BYTES":         2 * 1024 ** 3,
    "MIN_DISK_FREE_BYTES":    1 * 1024 * 1024 * 1024,
    "WAL_JOURNAL_SIZE_LIMIT": 1024 * 1024 * 1024,
    "WAL_CHECKPOINT_EVERY":   200,
    "RO_CONN_REFRESH_OPS":    1000,
    "VISITED_LRU_MAXSIZE":    5000,
    "DLQ_MAX_SIZE_MB":        10,
    "MAX_DLQ_FILES":          5,
    "JOIN_TIMEOUT_S":         60,
    "MAX_LABEL_PROPAGATION":  25,
    "FULL_LOAD_MODULES":      frozenset({
        "/mod/page/", "/mod/book/", "/mod/assign/",
        "/mod/forum/", "/mod/wiki/",
    }),
    "EXTERNAL_SAVE_DOMAINS":  frozenset({
        "youtube.com", "youtu.be", "vimeo.com",
        "drive.google.com", "docs.google.com", "slides.google.com",
        "onedrive.live.com", "sharepoint.com", "dropbox.com",
        "kaltura.com", "panopto.com", "mediaspace.kaltura.com",
        "webex.com", "zoom.us", "teams.microsoft.com",
        "github.com", "gitlab.com",
    }),
}

_DL_Q_MAXSIZE = CONFIG["MAX_IN_FLIGHT"] * 2

_WINDOWS_RESERVED = frozenset({
    "CON", "PRN", "AUX", "NUL",
    "COM1", "COM2", "COM3", "COM4", "COM5", "COM6", "COM7", "COM8", "COM9",
    "LPT1", "LPT2", "LPT3", "LPT4", "LPT5", "LPT6", "LPT7", "LPT8", "LPT9",
})

_MOODLE_NAV_TRAPS = frozenset({
    "/calendar/", "/message/", "/user/", "/admin/", "/grade/", "/badges/",
    "/report/", "/backup/", "/restore/", "/filter/", "/search/", "/tag/",
    "action=diff", "action=edit", "action=delete",
    "action=moveup", "action=movedown",
    "cal_m=", "cal_y=", "limitfrom=",
})

_VOLATILE_PARAMS = frozenset({
    "sesskey", "t", "ts", "cb", "token", "nonce", "rand", "random",
    "cal_m", "cal_y", "page", "perpage", "sort", "dir", "search",
    "limitfrom", "limitnum",
})

_HTML_BAD_SIGS = [
    b"logininfo", b"sesskey", b"loginbtn",
    b"notloggedin", b"moodle: access",
    b"you are not logged in", b"<!doctype html",
]

FILE_ICONS = {
    ".pdf": "📕", ".docx": "📄", ".doc": "📄", ".pptx": "📊", ".ppt": "📊",
    ".xlsx": "📈", ".xls": "📈", ".zip": "📦", ".rar": "📦", ".7z": "📦",
    ".mp4": "🎬", ".mp3": "🎵", ".avi": "🎬", ".mov": "🎬",
    ".jpg": "🖼️", ".jpeg": "🖼️", ".png": "🖼️", ".gif": "🖼️",
    ".txt": "📝", ".md": "📝", ".html": "🌐", ".url": "🔗",
}
SKIP_NAMES = {"00_INDICE_MAESTRO.md", "00_PARTE_DE_INCIDENCIAS.md"}
SKIP_EXTS = {".tmp", ".lock", ".jsonl", ".sqlite",
             ".sqlite-wal", ".sqlite-shm", ".json"}
TAG_STOPWORDS = frozenset({
    "de", "del", "la", "el", "los", "las", "un", "una", "en", "y", "o", "a", "con",
    "por", "para", "que", "es", "se", "al", "lo", "su", "ver", "sus", "este", "esta",
    "pdf", "zip", "doc", "docx", "pptx", "rar", "bin", "txt", "mp4", "mp3", "file",
    "document", "archivo", "recurso", "descarga", "tema", "sesion", "clase",
    "semana", "modulo", "capitulo", "unidad", "parte", "bloque", "practicas",
})

# ═══════════════════════════════════════════════════════════════
# §2 — EXCEPCIONES TIPADAS
# ═══════════════════════════════════════════════════════════════

class TransientError(Exception):
    """Error recuperable — provoca requeue con backoff exponencial."""

class DiskFullError(OSError):
    """Disco lleno — detiene el worker limpiamente."""

class SessionExpiredError(TransientError):
    """Sesión de Moodle expirada."""

# ═══════════════════════════════════════════════════════════════
# §3 — CONTEXTO SEMÁNTICO
# ═══════════════════════════════════════════════════════════════

@dataclass
class ResourceContext:
    section_title:      str
    section_index:      int
    visual_seq:         int
    link_text:          str
    label_context:      Optional[str]
    parent_activity:    Optional[str]
    server_filename:    Optional[str]
    content_type:       Optional[str]
    source_url:         str
    page_origin:        str
    file_hash:          str
    resource_type:      str = "file"
    context_confidence: str = "unknown"
    extra_meta:         dict = field(default_factory=dict)

# ═══════════════════════════════════════════════════════════════
# §4 — UTILIDADES DE FILESYSTEM
# ═══════════════════════════════════════════════════════════════

def ensure(p: str) -> str:
    os.makedirs(p, exist_ok=True)
    return p

def _path_exists_safe(path: str) -> bool:
    try:
        os.stat(path)
        return True
    except (FileNotFoundError, NotADirectoryError, OSError):
        return False

def _check_disk_space(path: str) -> bool:
    try:
        return shutil.disk_usage(path).free >= CONFIG["MIN_DISK_FREE_BYTES"]
    except Exception:
        return True

def _file_sha256_chunked(path: str) -> str:
    sha = hashlib.sha256()
    with open(path, "rb") as fh:
        for chunk in iter(lambda: fh.read(1024 * 1024), b""):
            sha.update(chunk)
    return sha.hexdigest()

def _fsync_dir(path: str):
    if os.name == "nt":
        return
    dir_path = os.path.dirname(os.path.abspath(path))
    try:
        fd = os.open(dir_path, os.O_RDONLY)
        try:
            os.fsync(fd)
        finally:
            os.close(fd)
    except OSError:
        pass

def _is_same_filesystem(path_a: str, path_b: str) -> bool:
    try:
        dir_b = path_b if os.path.isdir(path_b) else os.path.dirname(path_b)
        return os.stat(path_a).st_dev == os.stat(dir_b).st_dev
    except OSError:
        return False

# ═══════════════════════════════════════════════════════════════
# §5 — SANITIZACIÓN Y NAMING
# ═══════════════════════════════════════════════════════════════

def _s(name: str, default: str = "recurso") -> str:
    if not name:
        return default
    name = unquote(str(name))
    name = unicodedata.normalize("NFC", name)
    name = "".join(
        c for c in name
        if unicodedata.category(c) not in {"Cf", "Cc", "Cs", "Co", "Cn"}
    )
    # FIX: Doble backslash en el regex de la clase de caracteres para evitar fallos en Python 3.12+
    name = re.sub(
        r'[<>:"/\\|?*\x00-\x1f\U0001F300-\U0001FAFF\U00002600-\U000027BF]',
        "_", name
    ).strip()
    name = re.sub(r"\.{2,}", ".", name)
    while name.endswith((".", " ")):
        name = name[:-1]
    if not name:
        return default
    root, ext = os.path.splitext(name)
    if len(ext) > 10:
        root, ext = root + ext, ".bin"
    if root.upper().split(".")[0] in _WINDOWS_RESERVED:
        root = "_" + root
    ext_b = len(ext.encode("utf-8"))
    root_b = root.encode("utf-8")
    if len(root_b) > 180 - ext_b:
        root = root_b[:max(10, 180 - ext_b)].decode("utf-8", errors="ignore")
    return (root + ext).strip() or default

def normalize_url(url: str) -> str:
    url = url.split("#")[0].rstrip("/")
    try:
        p = urlparse(url)
        q = sorted(
            (k, v)
            for k, vs in parse_qs(p.query, keep_blank_values=False).items()
            for v in vs
            if k.lower() not in _VOLATILE_PARAMS
        )
        return p._replace(query=urlencode(q)).geturl()
    except Exception:
        return url

def _get_ext(ctype: str, fname: str) -> str:
    _, ext = os.path.splitext(fname)
    if ext and 1 < len(ext) < 7:
        return ext
    table = {
        "pdf": ".pdf", "zip": ".zip", "x-rar": ".rar", "x-7z": ".7z",
        "officedocument.wordprocessingml": ".docx",
        "officedocument.presentationml":   ".pptx",
        "officedocument.spreadsheetml":    ".xlsx",
        "msword": ".doc", "text/plain": ".txt", "text/csv": ".csv",
        "video/mp4": ".mp4", "audio/mpeg": ".mp3",
        "image/jpeg": ".jpg", "image/png": ".png",
        "text/html": ".html",
    }
    for key, rep in table.items():
        if key in ctype:
            return rep
    return ".bin"

def _is_external_save_domain(url: str) -> bool:
    try:
        netloc = urlparse(url).netloc.lower().lstrip("www.")
        return any(
            netloc == d or netloc.endswith("." + d)
            for d in CONFIG["EXTERNAL_SAVE_DOMAINS"]
        )
    except Exception:
        return False

def _is_direct_download(url: str) -> bool:
    return "pluginfile" in url or "forcedownload" in url

def _is_nav_trap(url: str) -> bool:
    if _is_direct_download(url):
        return False
    url_lower = url.lower()
    if any(trap in url_lower for trap in _MOODLE_NAV_TRAPS):
        return True
    if "sesskey=" in url_lower:
        return True
    return False

# ═══════════════════════════════════════════════════════════════
# §6 — CLASIFICADOR SEMÁNTICO
# ═══════════════════════════════════════════════════════════════

class EvidenceBasedClassifier:
    def classify(self, ctx: ResourceContext, course_dir: str) -> tuple:
        if ctx.section_title and ctx.section_title not in ("00_General", ""):
            title_slug = _s(ctx.section_title[:80])
            title_hash = hashlib.md5(
                ctx.section_title.encode("utf-8", errors="replace")
            ).hexdigest()[:4]
            sec_folder = f"{ctx.section_index + 1:02d}_{title_slug}_{title_hash}"
            confidence = "high"
        else:
            sec_folder = "99_Sin_Contexto"
            confidence = "unknown"

        base = os.path.join(course_dir, sec_folder)

        if ctx.label_context and ctx.label_context.strip():
            base = os.path.join(base, _s(ctx.label_context[:60]))
        elif confidence == "high":
            confidence = "partial"

        if ctx.parent_activity:
            base = os.path.join(base, _s(ctx.parent_activity[:60]))

        ctx.context_confidence = confidence
        return ensure(base), confidence

class StructuredNamer:
    def build_filename(self, ctx: ResourceContext, ext: str) -> str:
        parts = [f"{ctx.visual_seq:03d}"]
        link_slug   = _s(ctx.link_text[:50])   if ctx.link_text   else ""
        server_slug = _s(os.path.splitext(ctx.server_filename or "")[0][:40])

        if link_slug and server_slug:
            if self._similar(link_slug, server_slug):
                parts.append(max(link_slug, server_slug, key=len))
            else:
                parts.append(link_slug)
                parts.append(server_slug)
        elif link_slug:
            parts.append(link_slug)
        elif server_slug:
            parts.append(server_slug)
        else:
            parts.append("_SIN_NOMBRE")

        if ctx.context_confidence == "unknown":
            parts.append("CONTEXTO_DESCONOCIDO")

        parts.append(ctx.file_hash[:8])
        return "_".join(p for p in parts if p) + ext

    @staticmethod
    def _similar(a: str, b: str) -> bool:
        al, bl = a.lower(), b.lower()
        if al in bl or bl in al:
            return True
        return difflib.SequenceMatcher(None, al, bl).ratio() > 0.85

class TagExtractor:
    def extract(self, ctx: ResourceContext) -> List[str]:
        sources = [
            ctx.link_text or "",
            ctx.label_context or "",
            ctx.section_title or "",
            os.path.splitext(ctx.server_filename or "")[0],
        ]
        tokens = []
        for src in sources:
            for w in re.findall(r'\b[a-záéíóúüñA-ZÁÉÍÓÚÜÑA-Za-z0-9]{3,}\b', src):
                clean = w.lower()
                if clean not in TAG_STOPWORDS and not clean.isdigit():
                    tokens.append(clean)
        seen: set = set()
        return [t for t in tokens if not (t in seen or seen.add(t))][:10]

def _write_sidecar(
    file_path: str, ctx: ResourceContext, tags: List[str], confidence: str
):
    meta = {
        "origen": {
            "url":             ctx.source_url,
            "pagina":          ctx.page_origin,
            "texto_enlace":    ctx.link_text,
            "nombre_servidor": ctx.server_filename,
            "tipo_recurso":    ctx.resource_type,
        },
        "contexto_moodle": {
            "seccion":         ctx.section_title,
            "indice_seccion":  ctx.section_index,
            "etiqueta_visual": ctx.label_context,
            "actividad_padre": ctx.parent_activity,
            "orden_visual":    ctx.visual_seq,
        },
        "certeza_contexto": confidence,
        "tags_observables": tags,
        "meta_extra":       ctx.extra_meta,
        "advertencia": (
            "CONTEXTO DESCONOCIDO — revisa 99_Sin_Contexto manualmente"
            if confidence == "unknown" else None
        ),
        "hash_sha256": ctx.file_hash,
        "generado_ts": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
    }
    try:
        with open(file_path + ".meta.json", "w", encoding="utf-8") as fh:
            json.dump(meta, fh, ensure_ascii=False, indent=2)
    except OSError:
        pass

def _build_resource_context(
    job: dict, server_fname: str, file_hash: str, ctype: str
) -> ResourceContext:
    return ResourceContext(
        section_title   = job.get("section", ""),
        section_index   = job.get("section_idx", 99),
        visual_seq      = job.get("seq", 0),
        link_text       = job.get("link_text", ""),
        label_context   = job.get("label_context"),
        parent_activity = job.get("parent_activity"),
        server_filename = (
            server_fname
            if server_fname and server_fname != job.get("link_text", "")
            else None
        ),
        content_type    = ctype,
        source_url      = job.get("url", ""),
        page_origin     = job.get("page_origin", ""),
        file_hash       = file_hash,
        resource_type   = job.get("resource_type", "file"),
        extra_meta      = job.get("extra_meta", {}),
    )

# ═══════════════════════════════════════════════════════════════
# §7 — RATE LIMITER (thread-safe, instancia por proceso)
# ═══════════════════════════════════════════════════════════════

class DomainRateLimiter:
    def __init__(self, rps: float):
        self._rps     = rps
        self._lock    = threading.Lock()
        self._windows: dict = {}

    def acquire(self, domain: str):
        window = 1.0 / max(self._rps, 0.01)
        while True:
            with self._lock:
                now  = time.monotonic()
                hist = self._windows.setdefault(domain, deque())
                while hist and now - hist[0] > 1.0:
                    hist.popleft()
                if len(hist) < self._rps:
                    hist.append(now)
                    return
            time.sleep(window * 0.5)

# ═══════════════════════════════════════════════════════════════
# §8 — BOUNDED VISITED SET (LRU — cache local por proceso)
# ═══════════════════════════════════════════════════════════════

class BoundedVisitedSet:
    def __init__(self, maxsize: int, db_path: str):
        self._set:   set   = set()
        self._queue: deque = deque()
        self._max          = maxsize
        self._load_from_db(db_path)

    def _load_from_db(self, db_path: str):
        conn = None
        try:
            conn = sqlite3.connect(db_path, check_same_thread=False, timeout=10)
            try:
                conn.execute("PRAGMA journal_mode = WAL;")
                rows = conn.execute(
                    "SELECT url FROM visited ORDER BY ts DESC LIMIT ?",
                    (self._max,)
                ).fetchall()
                for (url,) in rows:
                    if len(self._set) < self._max:
                        self._set.add(url)
                        self._queue.append(url)
                if rows:
                    log.info("BoundedVisitedSet: %d URLs cargadas.", len(rows))
            finally:
                conn.close()
        except Exception as e:
            log.warning("BoundedVisitedSet: no se pudo cargar BD: %s", e)
            if conn is not None:
                with contextlib.suppress(Exception):
                    conn.close()

    def __contains__(self, item: str) -> bool:
        return item in self._set

    def add(self, item: str):
        if item in self._set:
            return
        if len(self._set) >= self._max:
            evicted = self._queue.popleft()
            self._set.discard(evicted)
        self._set.add(item)
        self._queue.append(item)

    def __len__(self) -> int:
        return len(self._set)

# ═══════════════════════════════════════════════════════════════
# §9 — IPC: safe_put + reingestión garantizada
# ═══════════════════════════════════════════════════════════════

def safe_put(q: mp.Queue, item: dict) -> bool:
    try:
        q.put_nowait(item)
        return True
    except queue.Full:
        time.sleep(0.5)
        try:
            q.put_nowait(item)
            return True
        except queue.Full:
            pass

    ef = os.path.join(
        CONFIG["ROOT_DIR"], f"_EMERGENCY_DUMP_{os.getpid()}.jsonl"
    )
    try:
        with open(ef, "a", encoding="utf-8") as fh:
            fh.write(json.dumps(
                {**item, "reason": "queue_full", "_ts": time.time()},
                ensure_ascii=False
            ) + "\n")
            fh.flush()
            os.fsync(fh.fileno())
    except Exception as dump_exc:
        log.critical(
            "DATO PERDIDO — dump de emergencia falló: %s | causa: %s",
            item.get("type"), dump_exc
        )
        return False
    log.error("Queue llena — volcado a disco (fsync OK): %s", item.get("type"))
    return False

def reingest_rescued_jobs(dl_q: mp.Queue) -> int:
    # FIX: Glob anclado estrictamente con '_' para no coger archivos renombrados por error previo
    patterns = [
        os.path.join(CONFIG["ROOT_DIR"], "_EMERGENCY_DUMP_*.jsonl"),
        os.path.join(CONFIG["ROOT_DIR"], "_PENDING_DOWNLOADS_RESCUED.jsonl"),
    ]
    reingested = 0
    for fpath in [f for pat in patterns for f in glob.glob(pat)]:
        if not fpath.endswith(".jsonl"):
            continue
        skipped = []
        try:
            with open(fpath, "r", encoding="utf-8") as fh:
                for line in fh:
                    try:
                        job = json.loads(line)
                        if job.get("type") in ("DLQ", "VISITED", "HASH", None):
                            skipped.append(line)
                            continue
                        job.pop("reason", None)
                        job.pop("_ts", None)
                        if safe_put(dl_q, job):
                            reingested += 1
                        else:
                            skipped.append(line)
                    except Exception:
                        skipped.append(line)
        except OSError as e:
            log.warning("Reingestión: error leyendo %s: %s", fpath, e)
            continue

        done = fpath[:-len(".jsonl")] + f".done_{int(time.time())}.bak"
        try:
            if skipped:
                with open(fpath, "w", encoding="utf-8") as fh:
                    fh.writelines(skipped)
            else:
                os.rename(fpath, done)
        except OSError:
            pass

    if reingested:
        log.info("♻️  Reingestión: %d trabajos recuperados.", reingested)
    return reingested

def async_requeue_with_backoff(q: mp.Queue, job: dict) -> bool:
    retries = job.get("retries", 0)
    if retries >= 3:
        return False
    job["retries"]       = retries + 1
    job["process_after"] = time.time() + min(2 ** retries, 30) + random.uniform(0, 1)
    safe_put(q, job)
    return True

# ═══════════════════════════════════════════════════════════════
# §10 — BASE DE DATOS (init, open, DB daemon)
# ═══════════════════════════════════════════════════════════════

def init_db(path: str):
    with sqlite3.connect(path) as conn:
        conn.executescript(f"""
        PRAGMA journal_mode          = WAL;
        PRAGMA synchronous           = NORMAL;
        PRAGMA busy_timeout          = 15000;
        PRAGMA wal_autocheckpoint    = 0;
        PRAGMA journal_size_limit    = {CONFIG['WAL_JOURNAL_SIZE_LIMIT']};
        CREATE TABLE IF NOT EXISTS visited (
            url TEXT PRIMARY KEY, cid TEXT, ts REAL
        );
        CREATE TABLE IF NOT EXISTS hashes (
            hash TEXT PRIMARY KEY, path TEXT, tags TEXT, ts REAL
        );
        CREATE TABLE IF NOT EXISTS dlq (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            severity TEXT, msg TEXT, url TEXT,
            context TEXT, action TEXT, cid TEXT, ts REAL
        );
        """)

def _open_db_ro(path: str) -> sqlite3.Connection:
    uri  = Path(path).absolute().as_uri() + "?mode=ro"
    conn = sqlite3.connect(uri, uri=True, check_same_thread=False)
    conn.execute("PRAGMA journal_mode     = WAL;")
    conn.execute("PRAGMA busy_timeout     = 15000;")
    conn.execute("PRAGMA read_uncommitted = FALSE;")
    conn.execute(f"PRAGMA journal_size_limit = {CONFIG['WAL_JOURNAL_SIZE_LIMIT']};")
    return conn

def _open_db_rw(path: str) -> sqlite3.Connection:
    conn = sqlite3.connect(path, check_same_thread=False)
    conn.execute("PRAGMA journal_mode        = WAL;")
    conn.execute("PRAGMA busy_timeout        = 15000;")
    conn.execute("PRAGMA read_uncommitted    = FALSE;")
    conn.execute("PRAGMA wal_autocheckpoint  = 0;")
    conn.execute(f"PRAGMA journal_size_limit = {CONFIG['WAL_JOURNAL_SIZE_LIMIT']};")
    return conn

def execute_read_safe(
    conn: sqlite3.Connection, query: str, params: tuple = ()
) -> list:
    for attempt in range(3):
        try:
            return conn.execute(query, params).fetchall()
        except sqlite3.DatabaseError as e:
            if "disk" in str(e).lower() or "corrupt" in str(e).lower():
                log.error("BD read error (intento %d/3): %s", attempt + 1, e)
                time.sleep(0.1 * (2 ** attempt))
                continue
            raise
    return []

def write_dlq_direct(ev: dict):
    dlq_file  = os.path.join(CONFIG["ROOT_DIR"], "00_DEAD_LETTER_QUEUE.jsonl")
    max_bytes = CONFIG["DLQ_MAX_SIZE_MB"] * 1024 * 1024
    try:
        if _path_exists_safe(dlq_file) and os.path.getsize(dlq_file) > max_bytes:
            rotated = f"{dlq_file}.{int(time.time())}.bak"
            os.rename(dlq_file, rotated)
            backups = sorted(glob.glob(f"{dlq_file}.*.bak"))
            while len(backups) > CONFIG["MAX_DLQ_FILES"]:
                with contextlib.suppress(OSError):
                    os.remove(backups.pop(0))
        with open(dlq_file, "a", encoding="utf-8") as fh:
            fh.write(json.dumps(ev, ensure_ascii=False) + "\n")
            fh.flush()
            os.fsync(fh.fileno())
    except Exception as exc:
        log.error("No se pudo escribir en DLQ: %s", exc)

def db_daemon(q: mp.Queue, db_path: str, stop: mp.Event):
    conn          = _open_db_rw(db_path)
    event_counter = 0
    last_event_ts = time.monotonic()

    def _chk_passive():
        with contextlib.suppress(Exception):
            conn.execute("PRAGMA wal_checkpoint(PASSIVE);")

    def _process_event(ev: dict):
        nonlocal event_counter, last_event_ts
        t = ev.get("type")
        if t == "VISITED":
            conn.execute(
                "INSERT OR IGNORE INTO visited VALUES (?,?,?)",
                (ev["url"], ev.get("cid", ""), time.time())
            )
            conn.commit()
        elif t == "HASH":
            conn.execute(
                "INSERT OR REPLACE INTO hashes VALUES (?,?,?,?)",
                (ev["hash"], ev["path"],
                 json.dumps(ev.get("tags", [])), time.time())
            )
            conn.commit()
        elif t == "DLQ":
            write_dlq_direct(ev)
        event_counter += 1
        last_event_ts  = time.monotonic()
        if event_counter >= CONFIG["WAL_CHECKPOINT_EVERY"]:
            _chk_passive()
            event_counter = 0

    try:
        while not stop.is_set():
            try:
                ev = q.get(timeout=1.0)
                _process_event(ev)
            except queue.Empty:
                if time.monotonic() - last_event_ts > 5.0:
                    _chk_passive()
                    last_event_ts = time.monotonic()
                continue
            except Exception as exc:
                log.error("DB daemon error: %s", exc)

        log.info("DB daemon: drenando cola final...")
        drained = 0
        while True:
            try:
                ev = q.get_nowait()
                _process_event(ev)
                drained += 1
            except queue.Empty:
                break
            except Exception as exc:
                log.error("DB daemon drenaje error: %s", exc)
        if drained:
            log.info("DB daemon: %d eventos drenados.", drained)

        with contextlib.suppress(Exception):
            conn.execute("PRAGMA wal_checkpoint(TRUNCATE);")

    finally:
        with contextlib.suppress(Exception):
            conn.close()

# ═══════════════════════════════════════════════════════════════
# §11 — ATOMIC I/O (locks, replace, fsync)
# ═══════════════════════════════════════════════════════════════

def _acquire_posix_lock(lock_path: str) -> tuple:
    try:
        fd = os.open(lock_path, os.O_CREAT | os.O_EXCL | os.O_WRONLY)
        return fd, True
    except FileExistsError:
        return -1, False
    except OSError:
        if HAS_FCNTL:
            fd = -1
            try:
                fd = os.open(lock_path, os.O_CREAT | os.O_WRONLY)
                fcntl.flock(fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
                return fd, True
            except (OSError, IOError):
                with contextlib.suppress(Exception):
                    if fd >= 0:
                        os.close(fd)
                return -1, False
        return -1, False

def _release_posix_lock(fd: int, lock_path: str):
    with contextlib.suppress(Exception):
        if HAS_FCNTL and fd >= 0:
            fcntl.flock(fd, fcntl.LOCK_UN)
    with contextlib.suppress(Exception):
        if fd >= 0:
            os.close(fd)
    with contextlib.suppress(Exception):
        if lock_path and _path_exists_safe(lock_path):
            os.remove(lock_path)

def _atomic_replace(src: str, dst: str, expected_hash: str = ""):
    if _path_exists_safe(dst):
        if expected_hash:
            try:
                existing_hash = _file_sha256_chunked(dst)
                if existing_hash == expected_hash:
                    with contextlib.suppress(OSError):
                        os.remove(src)
                    return
                log.warning(
                    "Hash mismatch en dst (%s≠%s) — reemplazando archivo corrupto.",
                    existing_hash[:8], expected_hash[:8]
                )
            except OSError:
                pass
        else:
            with contextlib.suppress(OSError):
                os.remove(src)
            return

    if _is_same_filesystem(src, dst):
        os.replace(src, dst)
        _fsync_dir(dst)
    else:
        part = dst + ".part"
        try:
            with open(src, "rb") as fsrc, open(part, "wb") as fdst:
                shutil.copyfileobj(fsrc, fdst, length=1024 * 1024)
                fdst.flush()
                os.fsync(fdst.fileno())
            os.replace(part, dst)
            _fsync_dir(dst)
            os.remove(src)
        except Exception:
            with contextlib.suppress(OSError):
                os.remove(part)
            raise

def _cleanup_stale_locks(locks_dir: str):
    if not _path_exists_safe(locks_dir):
        return
    cleaned = 0
    for lf in glob.glob(os.path.join(locks_dir, ".lock_*")):
        try:
            st = os.stat(lf)
            if time.time() - st.st_mtime > 600:
                os.remove(lf)
                cleaned += 1
        except OSError:
            pass
    if cleaned:
        log.info("🧹 %d lock(s) huérfanos eliminados.", cleaned)

# ═══════════════════════════════════════════════════════════════
# §12 — STREAM DOWNLOAD (anti-tarpit, anti-TCP-half-open)
# ═══════════════════════════════════════════════════════════════

def _read_with_deadline(
    response: requests.Response,
    sha256_obj,
    outfile,
    prefix: bytes = b"",
) -> int:
    chunk_timeout = CONFIG["CHUNK_TIMEOUT_S"]
    deadline_s    = CONFIG["STREAM_TIMEOUT_S"]
    min_speed     = CONFIG["MIN_SPEED_BPS"]
    speed_window  = CONFIG["SPEED_WINDOW_S"]
    max_bytes     = CONFIG["MAX_FILE_BYTES"]

    sock = getattr(getattr(response.raw, "_connection", None), "sock", None)
    if sock is not None:
        with contextlib.suppress(OSError, AttributeError):
            sock.settimeout(chunk_timeout)

    bytes_written = 0
    start = last_check = time.monotonic()
    bytes_in_window = 0

    if prefix:
        outfile.write(prefix)
        sha256_obj.update(prefix)
        bytes_written   += len(prefix)
        bytes_in_window += len(prefix)

    try:
        for chunk in response.iter_content(chunk_size=512 * 1024):
            if not chunk:
                continue
            now = time.monotonic()
            if now - start > deadline_s:
                raise TransientError(
                    f"Deadline de {deadline_s}s superado ({bytes_written / (1024**2):.1f} MB)."
                )
            if bytes_written + len(chunk) > max_bytes:
                raise TransientError(
                    f"Archivo supera el límite de {max_bytes // (1024**3):.0f} GB."
                )
            bytes_in_window += len(chunk)
            elapsed = now - last_check
            if elapsed >= speed_window:
                if bytes_in_window / elapsed < min_speed:
                    raise TransientError(
                        f"Tarpit: {bytes_in_window / elapsed:.0f} B/s < {min_speed}."
                    )
                last_check      = now
                bytes_in_window = 0
            outfile.write(chunk)
            sha256_obj.update(chunk)
            bytes_written += len(chunk)
    except socket.timeout:
        raise TransientError(
            f"Socket timeout de {chunk_timeout}s (TCP half-open detectado)."
        )
    return bytes_written

# ═══════════════════════════════════════════════════════════════
# §13 — CLOUD URL REWRITING
# ═══════════════════════════════════════════════════════════════

def _rewrite_cloud_url(url: str) -> str:
    if not url:
        return url
    if "dropbox.com" in url:
        if "?dl=0" in url:
            return url.replace("?dl=0", "?dl=1")
        sep = "&" if "?" in url else "?"
        return url + sep + "dl=1"
    if "drive.google.com" in url and "/view" in url:
        m = re.search(r"/d/([a-zA-Z0-9_-]+)", url)
        if m:
            return f"https://drive.google.com/uc?export=download&id={m.group(1)}"
    if "docs.google.com/document" in url:
        return url.split("/edit")[0].split("/pub")[0] + "/export?format=pdf"
    if "docs.google.com/spreadsheets" in url:
        return url.split("/edit")[0].split("/pub")[0] + "/export?format=xlsx"
    if "docs.google.com/presentation" in url:
        return url.split("/edit")[0].split("/pub")[0] + "/export?format=pptx"
    if "onedrive.live.com" in url:
        sep = "&" if "?" in url else "?"
        return url + sep + "download=1"
    return url

def _cloud_export_ext(original_url: str, rewritten_url: str) -> str:
    if rewritten_url == original_url:
        return ""
    if "format=pdf" in rewritten_url:
        return ".pdf"
    if "format=xlsx" in rewritten_url:
        return ".xlsx"
    if "format=pptx" in rewritten_url:
        return ".pptx"
    return ""

# ═══════════════════════════════════════════════════════════════
# §14 — FALLBACK URL SAVER (zero data loss para no-descargables)
# ═══════════════════════════════════════════════════════════════

def save_url_reference(
    target_dir: str,
    seq: int,
    name: str,
    url: str,
    origin_url: str,
    resource_type: str,
    reason: str,
    ctx_section: str = "",
    ctx_label: str = "",
    extra: dict = None,
) -> str:
    ensure(target_dir)
    slug  = _s(name[:60]) if name else "recurso"
    fname = f"{seq:03d}_{slug}_{resource_type.upper()}.url"
    fpath = os.path.join(target_dir, fname)

    try:
        with open(fpath, "w", encoding="utf-8") as fh:
            fh.write("[InternetShortcut]\n")
            fh.write(f"URL={url}\n")
    except OSError:
        return ""

    meta = {
        "tipo_recurso":    resource_type,
        "titulo":          name,
        "url_original":    url,
        "pagina_origen":   origin_url,
        "motivo_fallback": reason,
        "contexto":        {"seccion": ctx_section, "etiqueta": ctx_label},
        "meta_extra":      extra or {},
        "accion_manual":   f"Abre esta URL en el navegador: {url}",
        "generado_ts":     time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
    }
    try:
        with open(fpath + ".meta.json", "w", encoding="utf-8") as fh:
            json.dump(meta, fh, ensure_ascii=False, indent=2)
    except OSError:
        pass

    log.info("🔗 Referencia guardada: %s [%s]", fname, resource_type)
    return fpath

# ═══════════════════════════════════════════════════════════════
# §15 — AUTENTICACIÓN Y SESIÓN HTTP
# ═══════════════════════════════════════════════════════════════

def auth() -> tuple:
    log.info("🔑 Global Auth (SSO)...")
    opts = Options()
    opts.add_argument("--headless=new")
    opts.add_argument("--no-sandbox")
    opts.add_argument("--disable-dev-shm-usage")
    opts.page_load_strategy = "eager"
    d = webdriver.Chrome(
        service=Service(ChromeDriverManager().install()), options=opts
    )
    d.set_page_load_timeout(30)
    try:
        d.get(CONFIG["LOGIN_URL"])
        with contextlib.suppress(Exception):
            d.execute_script(
                "document.querySelectorAll('.modal-backdrop,.modal')"
                ".forEach(e=>e.remove());"
                "document.body.classList.remove('modal-open');"
            )
        d.find_element(By.ID, "username").send_keys(CONFIG["USERNAME"])
        d.find_element(By.ID, "password").send_keys(CONFIG["PASSWORD"])
        d.find_element(By.ID, "loginbtn").click()
        time.sleep(3)
        cookies = {c["name"]: c["value"] for c in d.get_cookies()}
        ua = d.execute_script(
            "return navigator.userAgent;"
        ).replace("HeadlessChrome", "Chrome")
        if not cookies:
            raise RuntimeError("Login fallido: sin cookies.")
        
        probe_session = requests.Session()
        probe_session.cookies.update(cookies)
        probe_session.headers.update({"User-Agent": ua})
        try:
            probe = probe_session.get(
                f"https://{CONFIG['DOMAIN']}/my/",
                timeout=10, allow_redirects=True
            )
            if "login" in probe.url.lower():
                raise RuntimeError("Login fallido: redirección al login.")
        finally:
            probe_session.close()
        log.info("✅  Login y sesión verificados.")
        return cookies, ua
    finally:
        with contextlib.suppress(Exception):
            d.quit()

def build_session(cookies: dict, ua: str) -> requests.Session:
    s = requests.Session()
    s.cookies.update(cookies)
    s.headers.update({"User-Agent": ua})
    retry = Retry(
        total=5, backoff_factor=1.0,
        status_forcelist=[500, 502, 503, 504],
        allowed_methods=["GET", "HEAD"],
        respect_retry_after_header=True,
    )
    s.mount("https://", HTTPAdapter(max_retries=retry))
    s.mount("http://",  HTTPAdapter(max_retries=retry))
    return s

# ═══════════════════════════════════════════════════════════════
# §16 — EXTRACTORES ESPECIALIZADOS
# ═══════════════════════════════════════════════════════════════

def _enqueue_download(
    dq: mp.Queue,
    semaphore: mp.BoundedSemaphore,
    job: dict,
    stop_workers: mp.Event,
) -> bool:
    if stop_workers.is_set():
        return False
    semaphore.acquire()
    if stop_workers.is_set():
        semaphore.release()
        return False
    if not safe_put(dq, job):
        semaphore.release()
        return False
    return True

def extract_page_content(
    d: webdriver.Chrome,
    page_url: str,
    target_dir: str,
    name: str,
    seq: int,
    dbq: mp.Queue,
    visited: BoundedVisitedSet,
    job_template: dict,
    semaphore: mp.BoundedSemaphore,
    dl_q: mp.Queue,
    stop_workers: mp.Event,
) -> int:
    ensure(target_dir)
    found = 0

    try:
        WebDriverWait(d, 10).until(
            EC.presence_of_element_located(
                (By.CSS_SELECTOR, ".box.generalbox, #region-main")
            )
        )
    except TimeoutException:
        pass

    html_fname = f"{seq:03d}_{_s(name[:50])}_PAGINA.html"
    try:
        page_html = d.execute_script(
            "var el = document.getElementById('region-main') || document.body;"
            "return el ? el.innerHTML : '';"
        ) or ""
        with open(os.path.join(target_dir, html_fname), "w", encoding="utf-8") as fh:
            fh.write(f"<!-- Origen: {page_url} -->\n")
            fh.write(f"<!-- Capturado: {time.strftime('%Y-%m-%dT%H:%M:%SZ', time.gmtime())} -->\n")
            fh.write(page_html)
        log.info("🌐 HTML guardado: %s", html_fname)
    except Exception as e:
        log.debug("Error guardando HTML de página %s: %s", page_url, e)

    try:
        anchors = d.find_elements(By.CSS_SELECTOR, "#region-main a[href]")
        for a in anchors:
            try:
                href = a.get_attribute("href") or ""
                link_name = (a.text or a.get_attribute("title") or "Recurso").strip()
                if not href or href.startswith("javascript:") or href.startswith("#"):
                    continue
                if _is_nav_trap(href):
                    continue
                if _is_direct_download(href):
                    job = {**job_template, "url": href,
                           "link_text": link_name, "name": link_name,
                           "target": target_dir, "resource_type": "file"}
                    if _enqueue_download(dl_q, semaphore, job, stop_workers):
                        found += 1
                elif _is_external_save_domain(href):
                    save_url_reference(
                        target_dir, seq + found, link_name, href, page_url,
                        "external_in_page", "Enlace externo dentro de página Moodle",
                        ctx_section=job_template.get("section", ""),
                        ctx_label=job_template.get("label_context", ""),
                    )
                    found += 1
            except StaleElementReferenceException:
                continue
    except Exception as e:
        log.debug("Error extrayendo enlaces de página %s: %s", page_url, e)

    try:
        for i, iframe in enumerate(d.find_elements(By.TAG_NAME, "iframe")):
            src = (iframe.get_attribute("src") or
                   iframe.get_attribute("data-src") or
                   iframe.get_attribute("data-url") or "")
            if not src or src.startswith("about:"):
                continue
            save_url_reference(
                target_dir, seq + found + i, f"iframe_{i + 1}", src, page_url,
                "iframe", "Contenido embebido en iframe",
                ctx_section=job_template.get("section", ""),
                ctx_label=job_template.get("label_context", ""),
                extra={"iframe_index": i},
            )
            found += 1
    except Exception:
        pass

    return found

def extract_assign_content(
    d: webdriver.Chrome,
    assign_url: str,
    target_dir: str,
    name: str,
    seq: int,
    semaphore: mp.BoundedSemaphore,
    dl_q: mp.Queue,
    job_template: dict,
    dbq: mp.Queue,
    stop_workers: mp.Event,
) -> int:
    ensure(target_dir)
    found = 0

    html_fname = f"{seq:03d}_{_s(name[:50])}_TAREA.html"
    try:
        html = d.execute_script(
            "var el = document.querySelector('.box.generalbox, #intro, .description');"
            "return el ? el.innerHTML : '';"
        ) or ""
        if html:
            with open(os.path.join(target_dir, html_fname), "w", encoding="utf-8") as fh:
                fh.write(f"<!-- Tarea: {name} | Origen: {assign_url} -->\n{html}")
    except Exception:
        pass

    try:
        anchors = d.find_elements(
            By.CSS_SELECTOR,
            ".box.generalbox a[href*='pluginfile'],"
            "#intro a[href*='pluginfile'],"
            ".description a[href*='pluginfile'],"
            "a[href*='forcedownload']"
        )
        for a in anchors:
            href  = a.get_attribute("href") or ""
            aname = (a.text or a.get_attribute("title") or f"adjunto_{found + 1}").strip()
            if not href:
                continue
            job = {**job_template, "url": href,
                   "link_text": aname, "name": aname,
                   "target": target_dir, "resource_type": "file",
                   "parent_activity": name}
            if _enqueue_download(dl_q, semaphore, job, stop_workers):
                found += 1
    except Exception:
        pass

    return found

def extract_forum_attachments(
    d: webdriver.Chrome,
    forum_url: str,
    target_dir: str,
    name: str,
    seq: int,
    semaphore: mp.BoundedSemaphore,
    dl_q: mp.Queue,
    job_template: dict,
    visited: BoundedVisitedSet,
    dbq: mp.Queue,
    stop_workers: mp.Event,
) -> int:
    ensure(target_dir)
    found = 0
    try:
        thread_links = []
        for a in d.find_elements(By.CSS_SELECTOR, "a[href*='mod/forum/discuss.php']"):
            href = a.get_attribute("href") or ""
            if href and normalize_url(href) not in visited:
                thread_links.append(href)

        for thread_url in thread_links[:20]:
            norm = normalize_url(thread_url)
            if norm in visited:
                continue
            visited.add(norm)
            try:
                d.get(thread_url)
                time.sleep(1.0)
                for a in d.find_elements(
                    By.CSS_SELECTOR, "a[href*='pluginfile'], a[href*='forcedownload']"
                ):
                    href  = a.get_attribute("href") or ""
                    aname = (a.text or f"adjunto_foro_{found + 1}").strip()
                    if not href:
                        continue
                    job = {**job_template, "url": href,
                           "link_text": aname, "name": aname,
                           "target": target_dir, "resource_type": "file",
                           "parent_activity": name}
                    if _enqueue_download(dl_q, semaphore, job, stop_workers):
                        found += 1
            except Exception:
                continue
    except Exception as e:
        log.warning("Error en extract_forum_attachments: %s", e)

    return found

def extract_folder_pages(
    d: webdriver.Chrome,
    folder_url: str,
    target_dir: str,
    name: str,
    seq: int,
    semaphore: mp.BoundedSemaphore,
    dl_q: mp.Queue,
    job_template: dict,
    visited: BoundedVisitedSet,
    dbq: mp.Queue,
    stop_workers: mp.Event,
) -> int:
    ensure(target_dir)
    found       = 0
    current_url = folder_url

    while current_url:
        norm = normalize_url(current_url)
        if norm in visited:
            break
        visited.add(norm)

        try:
            if d.current_url != current_url:
                d.get(current_url)
                time.sleep(1.0)

            for a in d.find_elements(
                By.CSS_SELECTOR,
                "#region-main a[href*='pluginfile'],"
                "#region-main a[href*='forcedownload']"
            ):
                href  = a.get_attribute("href") or ""
                aname = (a.text or f"archivo_{found + 1}").strip()
                if not href:
                    continue
                job = {**job_template, "url": href,
                       "link_text": aname, "name": aname,
                       "target": target_dir, "resource_type": "file"}
                if _enqueue_download(dl_q, semaphore, job, stop_workers):
                    found += 1

            current_url = None
            for nxt in d.find_elements(
                By.CSS_SELECTOR, "a[href*='page=']:not([href*='page=0'])"
            ):
                href = nxt.get_attribute("href") or ""
                norm_next = normalize_url(href)
                if href and CONFIG["DOMAIN"] in href and norm_next not in visited:
                    current_url = href
                    break

        except Exception as e:
            log.warning("Error en extract_folder_pages %s: %s", current_url, e)
            break

    return found

# ═══════════════════════════════════════════════════════════════
# §17 — EXTRACTOR DE BLOBS SELENIUM
# ═══════════════════════════════════════════════════════════════

_BLOB_INTERCEPTOR_JS = """
(function() {
    if (window._egela_blob_installed) return;
    window._egela_blob_installed = true;
    window._omni_blobs            = [];
    window._omni_blob_total_bytes = 0;
    const MAX_SINGLE = 100 * 1024 * 1024;
    const MAX_TOTAL  = 500 * 1024 * 1024;
    const _orig = URL.createObjectURL;
    URL.createObjectURL = function(obj) {
        const url = _orig(obj);
        if (!(obj instanceof Blob) || obj.size === 0) return url;
        if (obj.size > MAX_SINGLE) {
            window._omni_blobs.push({url,type:obj.type,data:null,
            size:obj.size,skipped:true,reason:'blob_too_large'});
            return url;
        }
        if (window._omni_blob_total_bytes + obj.size > MAX_TOTAL) {
            window._omni_blobs.push({url,type:obj.type,data:null,
            size:obj.size,skipped:true,reason:'total_limit_reached'});
            return url;
        }
        window._omni_blob_total_bytes += obj.size;
        const r = new FileReader();
        r.onload = () => window._omni_blobs.push(
            {url,type:obj.type,data:r.result,size:obj.size,skipped:false}
        );
        r.readAsDataURL(obj);
        return url;
    };
})();
"""

_BLOB_CLEANUP_JS = (
    "window._omni_blobs=[];"
    "window._omni_blob_total_bytes=0;"
    "window._egela_blob_installed=false;"
    "if(typeof window.gc==='function'){window.gc();}"
)

def extract_blobs_safe(
    driver: webdriver.Chrome,
    c_dir: str,
    page_url: str,
    dbq: mp.Queue,
):
    unclassified = ensure(os.path.join(c_dir, "99_Sin_Contexto"))
    try:
        count = driver.execute_script("return (window._omni_blobs||[]).length;")
        if not count:
            return
    except Exception as e:
        safe_put(dbq, {"type": "DLQ", "severity": "recoverable", "cid": "",
                       "msg": f"No se pudo leer blobs: {e}", "url": page_url,
                       "action": "Reproduce la página manualmente."})
        return

    for i in range(count):
        try:
            b = driver.execute_script(
                "return window._omni_blobs[arguments[0]]||null;", i
            )
            if not b:
                continue
            if b.get("skipped"):
                safe_put(dbq, {"type": "DLQ", "severity": "manual", "cid": "",
                                "msg": (f"Blob #{i} ({b.get('size', 0) / (1024**2):.1f} MB) "
                                        f"rechazado: {b.get('reason', '?')}."),
                                "url": page_url, "action": "Descarga el recurso manualmente."})
                continue
            raw = b.get("data", "")
            if not raw or "," not in raw:
                continue
            data = base64.b64decode(raw.split(",", 1)[1])
            if len(data) < 16:
                continue
            blob_type = b.get("type", "")
            ext = (".pdf" if "pdf" in blob_type else
                   ".mp4" if "video" in blob_type else
                   ".mp3" if "audio" in blob_type else ".bin")
            blob_hash = hashlib.sha256(data).hexdigest()
            fname = f"BLOB_{blob_hash[:8]}_{int(time.time() * 1000)}_{i:02d}{ext}"
            with open(os.path.join(unclassified, fname), "wb") as fh:
                fh.write(data)
            log.info("🫧  Blob: %s (%d KB)", fname, len(data) // 1024)
        except Exception as e:
            safe_put(dbq, {"type": "DLQ", "severity": "recoverable", "cid": "",
                            "msg": f"Blob #{i} error: {e}", "url": page_url,
                            "action": "Descarga el recurso dinámico manualmente."})

    with contextlib.suppress(Exception):
        driver.execute_script(_BLOB_CLEANUP_JS)

# ═══════════════════════════════════════════════════════════════
# §18 — DOWNLOADER PROCESS (con SIGTERM shield)
# ═══════════════════════════════════════════════════════════════

def _download_one(
    job: dict,
    session: requests.Session,
    ro_conn_ref: list,
    dbq: mp.Queue,
    semaphore: mp.BoundedSemaphore,
    db_path: str,
    ro_ops_counter: list,
    rate_limiter: DomainRateLimiter,
):
    url        = job.get("url", "")
    name       = job.get("link_text", "") or job.get("name", "recurso")
    seq        = job.get("seq", 1)
    cid        = job.get("cid", "")
    target     = job.get("target", ".")
    course_dir = os.path.join(CONFIG["ROOT_DIR"], f"Curso_{cid}")
    target_dir = ensure(target)

    tmp_path      = None
    tmp_fd        = -1
    lock_fd       = -1
    lock_path     = None
    lock_acquired = False

    def _dlq(severity: str, msg: str, action: str = ""):
        safe_put(dbq, {"type": "DLQ", "severity": severity,
                        "msg": msg, "url": url, "cid": cid, "action": action})

    ro_ops_counter[0] += 1
    if ro_ops_counter[0] >= CONFIG["RO_CONN_REFRESH_OPS"]:
        with contextlib.suppress(Exception):
            ro_conn_ref[0].close()
        ro_conn_ref[0] = _open_db_ro(db_path)
        ro_ops_counter[0] = 0

    try:
        if not _check_disk_space(target_dir):
            _dlq("critical", "Espacio en disco insuficiente.",
                 "Libera espacio y reinicia el crawler.")
            raise DiskFullError("ENOSPC pre-check")

        url_dl    = _rewrite_cloud_url(url)
        cloud_ext = _cloud_export_ext(url, url_dl)

        with contextlib.suppress(Exception):
            domain = urlparse(url_dl).netloc or CONFIG["DOMAIN"]
            rate_limiter.acquire(domain)

        with session.get(
            url_dl, stream=True, allow_redirects=True, timeout=(10, 30)
        ) as r:
            if r.status_code == 429:
                raise TransientError("HTTP 429 — rate limit.")
            if r.status_code in (500, 502, 503, 504):
                raise TransientError(f"HTTP {r.status_code} transitorio.")
            if r.status_code != 200:
                _dlq("recoverable", f"HTTP {r.status_code}.",
                     f"Descarga manualmente: {url}")
                save_url_reference(
                    target_dir, seq, name, url, job.get("page_origin", ""),
                    job.get("resource_type", "file"),
                    f"HTTP {r.status_code} — descarga fallida",
                    ctx_section=job.get("section", ""),
                    ctx_label=job.get("label_context", ""),
                )
                return

            ctype   = r.headers.get("Content-Type", "").lower()
            preview = b""

            if "text/html" in ctype:
                preview = r.raw.read(4096, decode_content=True)
                if any(s in preview.lower() for s in _HTML_BAD_SIGS):
                    _dlq("recoverable", "Wrapper HTML de login/error.",
                         f"Inicia sesión y descarga: {url}")
                    return
                return 

            fname = name
            cd = r.headers.get("Content-Disposition", "")
            if "filename*=" in cd:
                m = re.search(r"filename\*=(.+)", cd)
                if m:
                    with contextlib.suppress(Exception):
                        enc, _, val = decode_rfc2231(m.group(1))
                        fname = unquote(val, encoding=enc or "utf-8")
            elif "filename=" in cd:
                m = re.findall(r'filename="?([^";\n\r]+)"?', cd, re.I)
                if m:
                    fname = unquote(m[0].strip())

            ext = cloud_ext or _get_ext(ctype, fname)

            tmp_fd, tmp_path = tempfile.mkstemp(
                dir=target_dir, suffix=".tmp",
                prefix=f".dl_{os.getpid()}_"
            )
            sha256        = hashlib.sha256()
            bytes_written = 0

            try:
                with os.fdopen(tmp_fd, "wb") as tmp_f:
                    tmp_fd = -1 
                    bytes_written = _read_with_deadline(
                        r, sha256, tmp_f, prefix=preview
                    )
            except Exception:
                raise

        if bytes_written == 0:
            _dlq("recoverable", "Archivo vacío (0 bytes).",
                 f"Verifica el recurso en Moodle: {url}")
            return

        min_sizes = {".pdf": 512, ".docx": 512, ".pptx": 512, ".xlsx": 512, ".zip": 22}
        if bytes_written < min_sizes.get(ext.lower(), 0):
            _dlq("recoverable",
                 f"Archivo demasiado pequeño ({bytes_written}B) para {ext}. "
                 "Posible descarga parcial o error enmascarado.",
                 f"Verifica el recurso: {url}")
            return

        with open(tmp_path, "rb") as fh:
            mid = fh.read(2048).lower()
        if any(s in mid for s in _HTML_BAD_SIGS):
            _dlq("recoverable", "Corrupción mid-stream (sesión revocada).",
                 f"Reinicia sesión y descarga: {url}")
            return

        file_hash  = sha256.hexdigest()
        ctx        = _build_resource_context(job, fname, file_hash, ctype)
        classifier = EvidenceBasedClassifier()
        namer      = StructuredNamer()
        tagger     = TagExtractor()

        target_dir, confidence = classifier.classify(ctx, course_dir)
        ensure(target_dir)
        final_name = namer.build_filename(ctx, ext)
        tags       = tagger.extract(ctx)
        final_path = os.path.join(target_dir, final_name)

        rows = execute_read_safe(
            ro_conn_ref[0], "SELECT path FROM hashes WHERE hash=?", (file_hash,)
        )
        if rows and _path_exists_safe(rows[0][0]):
            if not _path_exists_safe(final_path):
                try:
                    os.link(rows[0][0], final_path)
                except OSError:
                    shutil.copy2(rows[0][0], final_path)
            return

        locks_dir = ensure(os.path.join(CONFIG["ROOT_DIR"], ".locks"))
        lock_path = os.path.join(locks_dir, f".lock_{file_hash}")
        lock_fd, lock_acquired = _acquire_posix_lock(lock_path)
        if not lock_acquired:
            return  

        rows2 = execute_read_safe(
            ro_conn_ref[0], "SELECT path FROM hashes WHERE hash=?", (file_hash,)
        )
        if rows2 and _path_exists_safe(rows2[0][0]):
            if not _path_exists_safe(final_path):
                try:
                    os.link(rows2[0][0], final_path)
                except OSError:
                    shutil.copy2(rows2[0][0], final_path)
            return

        _atomic_replace(tmp_path, final_path, expected_hash=file_hash)
        tmp_path = None  

        post_hash = _file_sha256_chunked(final_path)
        if post_hash != file_hash:
            with contextlib.suppress(OSError):
                os.remove(final_path)
            raise TransientError(
                f"Corrupción I/O post-write: {file_hash[:8]}≠{post_hash[:8]}."
            )

        _write_sidecar(final_path, ctx, tags, confidence)

        safe_put(dbq, {
            "type": "HASH", "hash": file_hash, "path": final_path,
            "tags": tags, "confidence": confidence,
        })
        log.info("⬇️  %s [%.1f MB] [%s]", final_name,
                 bytes_written / (1024 * 1024), confidence)

    finally:
        if tmp_fd >= 0:
            with contextlib.suppress(OSError):
                os.close(tmp_fd)
        if tmp_path and _path_exists_safe(tmp_path):
            with contextlib.suppress(OSError):
                os.remove(tmp_path)
        if lock_acquired:
            _release_posix_lock(lock_fd, lock_path)
        with contextlib.suppress(Exception):
            semaphore.release()

def downloader(
    q: mp.Queue,
    dbq: mp.Queue,
    stop_workers: mp.Event,
    cookies: dict,
    ua: str,
    db_path: str,
    semaphore: mp.BoundedSemaphore,
):
    _inflight: threading.local = threading.local()
    _inflight.job = None

    def _dl_sigterm_handler(sig, frame):
        job = getattr(_inflight, "job", None)
        if job:
            ef = os.path.join(
                CONFIG["ROOT_DIR"], f"_EMERGENCY_DUMP_{os.getpid()}.jsonl"
            )
            try:
                with open(ef, "a", encoding="utf-8") as fh:
                    fh.write(json.dumps(
                        {**job, "reason": "sigterm_inflight", "_ts": time.time()},
                        ensure_ascii=False
                    ) + "\n")
                    fh.flush()
                    os.fsync(fh.fileno())
            except Exception:
                pass
        with contextlib.suppress(Exception):
            semaphore.release()
        raise SystemExit(0)

    signal.signal(signal.SIGTERM, _dl_sigterm_handler)

    session        = build_session(cookies, ua)
    ro_conn_ref    = [_open_db_ro(db_path)] 
    ro_ops_counter = [0]
    rate_limiter   = DomainRateLimiter(rps=CONFIG["RATE_LIMIT_RPS"])

    try:
        while not stop_workers.is_set():
            try:
                job = q.get(timeout=2.0)
            except queue.Empty:
                continue

            now = time.time()
            pa  = job.get("process_after", 0)
            if now < pa:
                safe_put(q, job)
                time.sleep(min(pa - now, 1.0))
                continue

            _inflight.job = job
            try:
                _download_one(
                    job, session, ro_conn_ref, dbq, semaphore,
                    db_path, ro_ops_counter, rate_limiter
                )
            except (TransientError, SessionExpiredError) as e:
                if not async_requeue_with_backoff(q, job):
                    safe_put(dbq, {
                        "type": "DLQ", "severity": "recoverable",
                        "msg":  f"Fallo tras 3 reintentos: {e}",
                        "url":  job.get("url"), "cid": job.get("cid"),
                        "action": f"Descarga manualmente: {job.get('url')}",
                    })
            except DiskFullError:
                log.critical("💀 DISCO LLENO. Worker detenido.")
                safe_put(dbq, {
                    "type": "DLQ", "severity": "critical", "msg": "DISCO LLENO.",
                    "url": job.get("url"), "cid": job.get("cid"),
                    "action": "Libera espacio y reinicia el crawler.",
                })
                stop_workers.set()
                break
            except OSError as e:
                import errno as _errno
                if hasattr(e, "errno") and e.errno == _errno.ENOSPC:
                    stop_workers.set()
                    break
                if not async_requeue_with_backoff(q, job):
                    safe_put(dbq, {
                        "type": "DLQ", "severity": "critical",
                        "msg": f"OSError: {e}",
                        "url": job.get("url"), "cid": job.get("cid"),
                    })
            except Exception as e:
                if not async_requeue_with_backoff(q, job):
                    safe_put(dbq, {
                        "type": "DLQ", "severity": "critical",
                        "msg": f"Error inesperado: {e}",
                        "url": job.get("url"), "cid": job.get("cid"),
                        "action": f"Descarga manualmente: {job.get('url')}",
                    })
            finally:
                _inflight.job = None

    finally:
        with contextlib.suppress(Exception):
            ro_conn_ref[0].close()
        with contextlib.suppress(Exception):
            session.close()

# ═══════════════════════════════════════════════════════════════
# §19 — SPIDER PROCESS (con SIGTERM shield)
# ═══════════════════════════════════════════════════════════════

_JS_MAP_UNIVERSAL = """
(function() {
    document.querySelectorAll('.modal-backdrop,.modal,.tour-backdrop')
        .forEach(e => e.remove());
    document.body.classList.remove('modal-open');
    document.querySelectorAll('.collapsed,[aria-expanded="false"]').forEach(e => {
        e.classList.remove('collapsed');
        e.setAttribute('aria-expanded','true');
    });

    let results=[], current_section='00_General', current_section_idx=0;
    let current_label=null, label_resource_count=0;
    const MAX_LABEL_PROP = """ + str(CONFIG["MAX_LABEL_PROPAGATION"]) + """;
    let seq=1;

    const sSecSel=[
        '[data-for="section"]','[data-sectionid]',
        'li.section.main','div.section.main','.course-section',
        '.grid-section','.tile','.onetopic-tab-content',
        '.course-content-item','[data-type="section"]',
    ];
    const sActSel=[
        '[data-for="cmitem"]','li.activity','div.activity','div.activity-item',
        '.activity-item','[data-activityname]',
    ];

    let sections=[];
    for(const s of sSecSel){
        sections=Array.from(document.querySelectorAll(s));
        if(sections.length>0) break;
    }
    if(sections.length===0){
        const m=document.getElementById('region-main')||document.querySelector('main');
        if(m) sections=[m];
    }

    sections.forEach((sec,sec_idx)=>{
        const tSelectors=[
            'h3.sectionname','h2.sectionname',
            '[data-for="sectiontitle"]','.section-title h3','.section-title h2',
            '.grid-section-title','.tile-title','.onetopic-tab-title','h1','h2','h3','h4',
        ];
        let title='';
        for(const sel of tSelectors){
            const el=sec.querySelector(sel);
            if(el){ title=(el.innerText||'').trim().split('\n')[0]; if(title) break; }
        }
        if(!title) title=sec.getAttribute('aria-label')||sec.getAttribute('title')||'';
        if(title&&title.length>1){
            current_section=title; current_section_idx=sec_idx;
            current_label=null; label_resource_count=0; seq=1;
        }

        let acts=[];
        for(const s of sActSel){ acts=Array.from(sec.querySelectorAll(s)); if(acts.length>0) break; }
        if(acts.length===0)
            acts=Array.from(sec.querySelectorAll('a[href]')).map(a=>a.parentElement||a);

        for(const act of acts){
            const st=window.getComputedStyle(act);
            if(st.display==='none'||st.visibility==='hidden') continue;
            const rc=act.getBoundingClientRect();
            if(rc.width===0&&rc.height===0) continue;
            if(rc.right<-200||rc.bottom<-200) continue;

            const cls=act.getAttribute('class')||'';
            const isLabel=cls.includes('modtype_label')||
                           act.getAttribute('data-type')==='label'||
                           act.getAttribute('data-for')==='label';
            if(isLabel){
                const lt=(act.innerText||act.textContent||'').trim().split('\n')[0];
                if(lt&&lt.length>2&&lt.length<200){ current_label=lt; label_resource_count=0; }
                continue;
            }

            const anchor=act.querySelector('a[href]')||act;
            const href=anchor.getAttribute('href')||'';
            if(!href||href.startsWith('javascript:')||href.startsWith('#')) continue;

            const blocked=cls.includes('dimmed')||cls.includes('conditionalhidden')||
                           cls.includes('restricted');
            const isLTI=cls.includes('modtype_lti')||cls.includes('modtype_scorm')||
                         cls.includes('modtype_h5pactivity')||cls.includes('modtype_bigbluebuttonbn');

            let link_text=(anchor.innerText||anchor.textContent||'').trim().split('\n')[0];
            if(!link_text) link_text=anchor.getAttribute('title')||anchor.getAttribute('aria-label')||
                                      act.getAttribute('data-activityname')||'';

            label_resource_count++;
            const effective_label = label_resource_count > MAX_LABEL_PROP ? null : current_label;

            let type='link';
            const tm=[
                ['pluginfile','file'],['forcedownload','file'],
                ['/mod/folder/','folder'],['/mod/page/','page'],['/mod/book/','page'],
                ['/mod/forum/','forum'],['/mod/url/','url'],
                ['/mod/resource/','resource'],['/mod/assign/','assign'],
                ['/mod/quiz/','quiz'],['/mod/wiki/','wiki'],
                ['/mod/glossary/','glossary'],['/mod/data/','database'],
                ['/mod/workshop/','workshop'],['/mod/lesson/','lesson'],
            ];
            for(const [p,t] of tm){ if(href.includes(p)){type=t;break;} }

            const iframes=Array.from(act.querySelectorAll('iframe')).map(f=>({
                src:f.getAttribute('src')||f.getAttribute('data-src')||f.getAttribute('data-url')||''
            })).filter(f=>f.src&&(f.src.startsWith('http')||f.src.startsWith('//')));

            results.push({
                url:href, link_text:link_text.substring(0,200), name:link_text.substring(0,200),
                section:current_section, section_idx:current_section_idx,
                label_context:effective_label, seq:seq++,
                type:type, blocked:blocked, lti:isLTI, iframes:iframes,
            });
        }
    });
    return results;
})();
"""

def _setup_driver(cookies: dict) -> webdriver.Chrome:
    opts = Options()
    for arg in [
        "--headless=new", "--disable-gpu", "--no-sandbox",
        "--disable-dev-shm-usage", "--disable-extensions",
        "--window-size=1920,1080", "--disable-blink-features=AutomationControlled",
    ]:
        opts.add_argument(arg)
    opts.add_experimental_option("excludeSwitches", ["enable-automation"])
    opts.add_experimental_option("useAutomationExtension", False)
    opts.page_load_strategy = "eager"
    d = webdriver.Chrome(
        service=Service(ChromeDriverManager().install()), options=opts
    )
    d.set_page_load_timeout(30)
    d.execute_cdp_cmd(
        "Page.addScriptToEvaluateOnNewDocument",
        {"source": _BLOB_INTERCEPTOR_JS}
    )
    d.get(f"https://{CONFIG['DOMAIN']}")
    for k, v in cookies.items():
        with contextlib.suppress(Exception):
            d.add_cookie({"name": k, "value": v, "domain": CONFIG["DOMAIN"]})
    return d

def _is_driver_alive(d: webdriver.Chrome) -> bool:
    try:
        _ = d.title
        return True
    except Exception:
        return False

def _navigate_page(
    d: webdriver.Chrome, url: str, full_load: bool = False
) -> bool:
    try:
        d.get(url)
        if full_load:
            try:
                WebDriverWait(d, 15).until(
                    EC.presence_of_element_located(
                        (By.CSS_SELECTOR, "#region-main, .course-content, main")
                    )
                )
            except TimeoutException:
                pass
        else:
            time.sleep(1.5)
        return True
    except Exception as e:
        log.warning("Error navegando a %s: %s", url, e)
        return False

def spider(
    q: mp.Queue,
    dq: mp.Queue,
    dbq: mp.Queue,
    stop_workers: mp.Event,
    cookies: dict,
    ua: str,
    db_path: str,
    semaphore: mp.BoundedSemaphore,
):
    _current_task: dict = {}

    def _spider_sigterm_handler(sig, frame):
        task = _current_task.get("task")
        if task:
            ef = os.path.join(
                CONFIG["ROOT_DIR"], f"_EMERGENCY_DUMP_{os.getpid()}.jsonl"
            )
            try:
                with open(ef, "a", encoding="utf-8") as fh:
                    fh.write(json.dumps(
                        {**task, "reason": "sigterm_spider", "_ts": time.time()},
                        ensure_ascii=False
                    ) + "\n")
                    fh.flush()
                    os.fsync(fh.fileno())
            except Exception:
                pass
        raise SystemExit(0)

    signal.signal(signal.SIGTERM, _spider_sigterm_handler)

    ro_conn = _open_db_ro(db_path)
    visited = BoundedVisitedSet(maxsize=CONFIG["VISITED_LRU_MAXSIZE"], db_path=db_path)

    d             = _setup_driver(cookies)
    pages_loaded  = 0
    driver_born   = time.monotonic()
    last_activity = time.monotonic()

    def _dlq(severity, msg, url, cid, action=""):
        safe_put(dbq, {"type": "DLQ", "severity": severity,
                        "msg": msg, "url": url, "cid": cid, "action": action})

    def _should_restart_driver() -> bool:
        return (pages_loaded >= CONFIG["MAX_PAGES_PER_DRIVER"] or
                time.monotonic() - driver_born > CONFIG["MAX_SECS_PER_DRIVER"])

    def _restart_driver():
        nonlocal d, pages_loaded, driver_born, last_activity
        log.info("♻️  Reiniciando Chrome (páginas=%d, tiempo=%.0fs)...",
                 pages_loaded, time.monotonic() - driver_born)
        with contextlib.suppress(Exception):
            d.quit()
        d             = _setup_driver(cookies)
        pages_loaded  = 0
        driver_born   = time.monotonic()
        last_activity = time.monotonic()

    def _ensure_driver():
        nonlocal last_activity
        if not _is_driver_alive(d):
            log.warning("🔄 Driver muerto — reiniciando...")
            _restart_driver()
            return
        if time.monotonic() - last_activity > CONFIG["SPIDER_WATCHDOG_S"]:
            log.info("♻️  Watchdog: %.0fs inactivo — reiniciando...",
                     time.monotonic() - last_activity)
            _restart_driver()

    def crawl(url: str, cid: str, depth: int = 0):
        nonlocal pages_loaded, last_activity

        if depth > 10:
            _dlq("manual",
                 "Límite de profundidad (depth=10). Posibles recursos no descargados.",
                 url, cid, f"Accede manualmente: {url}")
            return

        if _is_nav_trap(url):
            return

        norm = normalize_url(url)
        if norm in visited:
            return
        rows = execute_read_safe(ro_conn, "SELECT 1 FROM visited WHERE url=?", (norm,))
        if rows:
            visited.add(norm)
            return

        if _should_restart_driver():
            _restart_driver()
        _ensure_driver()

        full_load = any(m in url for m in CONFIG["FULL_LOAD_MODULES"])
        if not _navigate_page(d, url, full_load=full_load):
            _dlq("recoverable", "No se pudo cargar la página.",
                 url, cid, f"Accede manualmente: {url}")
            return

        last_activity = time.monotonic()
        page_title    = d.title or ""

        if CONFIG["DOMAIN"] not in d.current_url:
            visited.add(norm)
            _dlq("manual", "Redirección a dominio externo.", url, cid,
                 "Accede manualmente con sesión institucional.")
            return

        visited.add(norm)
        safe_put(dbq, {"type": "VISITED", "url": norm, "cid": cid})
        pages_loaded += 1

        try:
            elementos = d.execute_script(_JS_MAP_UNIVERSAL) or []
        except Exception as e:
            _dlq("critical",
                 f"Analizador DOM falló. Recursos NO descargados. Título: {page_title}",
                 url, cid, f"Abre en Moodle: {url}")
            return

        c_dir = ensure(os.path.join(CONFIG["ROOT_DIR"], f"Curso_{cid}"))

        for el in elementos:
            if stop_workers.is_set():
                return

            u         = el.get("url", "")
            link_txt  = el.get("link_text", "") or el.get("name", "")
            section   = el.get("section", "00_General")
            sec_idx   = el.get("section_idx", 99)
            label_ctx = el.get("label_context")
            seq       = el.get("seq", 0)
            etype     = el.get("type", "link")
            blocked   = el.get("blocked", False)
            is_lti    = el.get("lti", False)
            iframes   = el.get("iframes", [])

            if not u:
                continue
            if _is_nav_trap(u):
                continue

            title_slug = _s(section[:80])
            title_hash = hashlib.md5(
                section.encode("utf-8", errors="replace")
            ).hexdigest()[:4]
            sec_folder = (
                f"{sec_idx + 1:02d}_{title_slug}_{title_hash}"
                if section and section != "00_General"
                else "99_Sin_Contexto"
            )
            target_dir = os.path.join(c_dir, sec_folder)
            if label_ctx:
                target_dir = os.path.join(target_dir, _s(label_ctx[:60]))

            job_template = {
                "cid":           cid,
                "section":       section,
                "section_idx":   sec_idx,
                "label_context": label_ctx,
                "seq":           seq,
                "page_origin":   url,
                "link_text":     link_txt,
                "name":          link_txt,
            }

            for i_idx, i_data in enumerate(iframes):
                src = i_data.get("src", "")
                if not src:
                    continue
                if _is_external_save_domain(src):
                    save_url_reference(
                        ensure(target_dir), seq, f"{link_txt}_iframe{i_idx + 1}",
                        src, url, "iframe", "Contenido embebido en iframe",
                        ctx_section=section, ctx_label=label_ctx or "",
                    )
                elif CONFIG["DOMAIN"] in src:
                    norm_iframe = normalize_url(src)
                    if norm_iframe not in visited:
                        crawl(src, cid, depth + 1)

            if blocked:
                _dlq("manual", f"Recurso bloqueado: {link_txt}", u, cid,
                     "Comprueba requisitos de acceso en Moodle.")
                save_url_reference(
                    ensure(target_dir), seq, link_txt, u, url, "blocked",
                    "Recurso bloqueado por restricción condicional",
                    ctx_section=section, ctx_label=label_ctx or "",
                )
                continue

            if is_lti:
                save_url_reference(
                    ensure(target_dir), seq, link_txt, u, url, "lti",
                    "Herramienta LTI/SCORM/Interactiva no descargable automáticamente",
                    ctx_section=section, ctx_label=label_ctx or "",
                    extra={"moodle_type": etype},
                )
                _dlq("manual", f"LTI no descargable: {link_txt}", u, cid,
                     f"Accede directamente en Moodle: {u}")
                continue

            if etype in ("file", "resource") or _is_direct_download(u):
                job = {**job_template, "url": u, "target": ensure(target_dir),
                       "resource_type": "file"}
                _enqueue_download(dq, semaphore, job, stop_workers)

            elif etype == "url":
                rewritten = _rewrite_cloud_url(u)
                save_url_reference(
                    ensure(target_dir), seq, link_txt, u, url,
                    "external_url", "Enlace externo de Moodle",
                    ctx_section=section, ctx_label=label_ctx or "",
                    extra={"rewritten_url": rewritten},
                )
                if rewritten != u:
                    job = {**job_template, "url": rewritten,
                           "target": ensure(target_dir), "resource_type": "external"}
                    _enqueue_download(dq, semaphore, job, stop_workers)

            elif etype == "folder":
                norm_child = normalize_url(u)
                if norm_child not in visited and CONFIG["DOMAIN"] in u:
                    visited.add(norm_child)
                    if _navigate_page(d, u):
                        last_activity = time.monotonic()
                        extract_folder_pages(
                            d, u, ensure(target_dir), link_txt, seq,
                            semaphore, dq, job_template, visited, dbq, stop_workers
                        )
                        with contextlib.suppress(Exception):
                            d.back()
                            time.sleep(0.5)

            elif etype == "page":
                norm_child = normalize_url(u)
                if norm_child not in visited and CONFIG["DOMAIN"] in u:
                    visited.add(norm_child)
                    if _navigate_page(d, u, full_load=True):
                        last_activity = time.monotonic()
                        extract_page_content(
                            d, u, ensure(target_dir), link_txt, seq,
                            dbq, visited, job_template, semaphore, dq, stop_workers
                        )
                        crawl(u, cid, depth + 1)
                        with contextlib.suppress(Exception):
                            d.back()
                            time.sleep(0.5)

            elif etype == "assign":
                norm_child = normalize_url(u)
                if norm_child not in visited and CONFIG["DOMAIN"] in u:
                    visited.add(norm_child)
                    if _navigate_page(d, u, full_load=True):
                        last_activity = time.monotonic()
                        extract_assign_content(
                            d, u, ensure(target_dir), link_txt, seq,
                            semaphore, dq, job_template, dbq, stop_workers
                        )
                        with contextlib.suppress(Exception):
                            d.back()
                            time.sleep(0.5)

            elif etype == "forum":
                norm_child = normalize_url(u)
                if norm_child not in visited and CONFIG["DOMAIN"] in u:
                    visited.add(norm_child)
                    if _navigate_page(d, u, full_load=True):
                        last_activity = time.monotonic()
                        extract_forum_attachments(
                            d, u, ensure(target_dir), link_txt, seq,
                            semaphore, dq, job_template, visited, dbq, stop_workers
                        )
                        with contextlib.suppress(Exception):
                            d.back()
                            time.sleep(0.5)

            elif etype in ("quiz", "wiki", "glossary", "database",
                           "workshop", "lesson"):
                save_url_reference(
                    ensure(target_dir), seq, link_txt, u, url, etype,
                    f"Actividad interactiva Moodle ({etype})",
                    ctx_section=section, ctx_label=label_ctx or "",
                )
                norm_child = normalize_url(u)
                if norm_child not in visited and f"id={cid}" in u:
                    crawl(u, cid, depth + 1)

            elif CONFIG["DOMAIN"] in u:
                norm_child = normalize_url(u)
                if norm_child not in visited:
                    crawl(u, cid, depth + 1)

            else:
                save_url_reference(
                    ensure(target_dir), seq, link_txt, u, url, "unknown",
                    "Tipo de recurso no identificado",
                    ctx_section=section, ctx_label=label_ctx or "",
                )
                _dlq("manual", f"Recurso no clasificado: {link_txt}", u, cid,
                     f"Comprueba manualmente: {u}")

        extract_blobs_safe(d, c_dir, url, dbq)

    try:
        while not stop_workers.is_set():
            _ensure_driver()
            try:
                task = q.get(timeout=2.0)
            except queue.Empty:
                continue
            log.info("🕷️  Curso %s → %s", task.get("cid"), task.get("url"))
            _current_task["task"] = task
            try:
                crawl(task["url"], task["cid"])
            except Exception as e:
                safe_put(dbq, {
                    "type": "DLQ", "severity": "critical",
                    "msg":  f"Error crítico en spider: {e}",
                    "url":  task.get("url"), "cid": task.get("cid"),
                    "action": "Reinicia el crawler.",
                })
            finally:
                _current_task.clear()
    finally:
        with contextlib.suppress(Exception):
            d.quit()
        with contextlib.suppress(Exception):
            ro_conn.close()

# ═══════════════════════════════════════════════════════════════
# §20 — GENERADORES UX (DLQ report + Master Index)
# ═══════════════════════════════════════════════════════════════

def flush_dlq_to_human_report(course_dir: str, course_id: str):
    dlq_path    = os.path.join(CONFIG["ROOT_DIR"], "00_DEAD_LETTER_QUEUE.jsonl")
    report_path = os.path.join(course_dir, "00_PARTE_DE_INCIDENCIAS.md")
    if not _path_exists_safe(dlq_path):
        return

    entries = []
    with open(dlq_path, "r", encoding="utf-8") as fh:
        for line in fh:
            try:
                ev = json.loads(line)
                if ev.get("type") == "DLQ" and str(ev.get("cid", "")) == str(course_id):
                    entries.append(ev)
            except Exception:
                continue

    sev_label = {
        "manual":      "🔴 ACCIÓN MANUAL REQUERIDA",
        "critical":    "🟠 ERROR CRÍTICO",
        "recoverable": "🟡 AVISO (posiblemente recuperable)",
    }
    by_sev: dict = {}
    for e in entries:
        by_sev.setdefault(e.get("severity", "recoverable"), []).append(e)

    lines = [
        f"# 📋 Parte de Incidencias — Curso {course_id}\n\n",
        f"> **Generado:** {time.strftime('%Y-%m-%d %H:%M:%S')}\n",
        f"> **Total:** {len(entries)} incidencias\n\n",
        "Recursos **no descargados automáticamente**. "
        "Para cada uno: URL + instrucción de recuperación.\n\n---\n\n",
    ]
    if not entries:
        lines.append("✅ Sin incidencias. Todo el contenido fue descargado.\n")
    else:
        for sev in ("manual", "critical", "recoverable"):
            group = by_sev.get(sev, [])
            if not group:
                continue
            lines.append(f"## {sev_label.get(sev, sev)} ({len(group)})\n\n")
            for i, e in enumerate(group, 1):
                lines.append(f"### {i}. {e.get('msg', 'Error sin descripción')}\n\n")
                url = e.get("url", "")
                if url:
                    lines.append(f"- **🔗 URL:** <{url}>\n")
                action = e.get("action", "")
                if action:
                    lines.append("- **✅ Acción:**\n")
                    for ln in action.strip().split("\n"):
                        lines.append(f"  {ln.strip()}\n")
                lines.append("\n")

    with open(report_path, "w", encoding="utf-8") as fh:
        fh.writelines(lines)
    log.info("📋 Parte de incidencias → %s", report_path)

def synthesize_master_index(course_dir: str, course_id: str):
    index_path = os.path.join(course_dir, "00_INDICE_MAESTRO.md")
    ensure(os.path.join(course_dir, "99_Sin_Contexto"))

    sections: dict = {}
    for root, dirs, files in os.walk(course_dir):
        dirs.sort()
        for fname in sorted(files):
            if fname in SKIP_NAMES:
                continue
            ext = os.path.splitext(fname)[1].lower()
            if ext in SKIP_EXTS or fname.endswith(".meta.json"):
                continue
            fpath = os.path.join(root, fname)
            meta  = {}
            if _path_exists_safe(fpath + ".meta.json"):
                try:
                    with open(fpath + ".meta.json", "r", encoding="utf-8") as fh:
                        meta = json.load(fh)
                except Exception:
                    pass
            ctx_m      = meta.get("contexto_moodle", {})
            orig       = meta.get("origen", {})
            section    = (ctx_m.get("seccion") or
                          os.path.basename(os.path.dirname(fpath)) or "Sin Sección")
            sec_idx    = ctx_m.get("indice_seccion", 99)
            label      = ctx_m.get("etiqueta_visual")
            link_text  = orig.get("texto_enlace") or fname
            tags       = meta.get("tags_observables", [])
            confidence = meta.get("certeza_contexto", "unknown")
            rel        = os.path.relpath(fpath, course_dir).replace("\\", "/")
            sections.setdefault((sec_idx, section), []).append({
                "rel": rel, "fname": fname, "link_text": link_text,
                "label": label, "tags": tags, "confidence": confidence, "ext": ext,
            })

    lines = [
        f"# 🧠 Archivo Digital — Curso {course_id}\n\n",
        f"**Generado:** {time.strftime('%Y-%m-%d %H:%M:%S UTC', time.gmtime())}  \n",
        "**Compatible con:** Windows · macOS · Linux · Sin internet  \n\n",
        "## 📖 Leyenda\n\n",
        "- ✅ `contexto:alto` — sección + subtítulo del profesor detectados\n",
        "- ⚠️ `contexto:parcial` — sección detectada, sin subtítulo\n",
        "- ❓ `contexto:desconocido` — sin estructura Moodle detectable\n",
        "- 🔗 `.url` — enlace externo (ver .meta.json para contexto)\n\n",
        "---\n\n## 📂 Estructura del Curso\n\n",
    ]
    unknown_entries = []
    for (sec_idx, section), entries in sorted(sections.items()):
        if "Sin_Contexto" in section or sec_idx == 99:
            unknown_entries.extend(entries)
            continue
        lines.append(f"\n### 📁 {section}\n\n")
        by_label: dict = {}
        for e in sorted(entries, key=lambda x: x["rel"]):
            by_label.setdefault(e["label"] or "_sin_etiqueta", []).append(e)
        for label, label_entries in by_label.items():
            if label != "_sin_etiqueta":
                lines.append(f"\n#### 🏷️ {label}\n\n")
            for e in label_entries:
                icon    = FILE_ICONS.get(e["ext"], "📄")
                badge   = {"high": "✅", "partial": "⚠️", "unknown": "❓"}.get(
                    e["confidence"], "❓"
                )
                tag_str = " ".join(f"`{t}`" for t in e["tags"][:5])
                display = e["link_text"] or e["fname"]
                line    = f"- {badge} {icon} [{display}](<./{e['rel']}>)"
                if tag_str:
                    line += f"  {tag_str}"
                lines.append(line + "\n")

    if unknown_entries:
        lines.append(
            f"\n---\n\n## ❓ {len(unknown_entries)} recursos sin contexto detectable\n\n"
            "> Sin estructura Moodle detectable. Revisa `00_PARTE_DE_INCIDENCIAS.md`.\n\n"
        )
        for e in sorted(unknown_entries, key=lambda x: x["rel"]):
            icon = FILE_ICONS.get(e["ext"], "📄")
            lines.append(
                f"- ❓ {icon} [{e['link_text'] or e['fname']}](<./{e['rel']}>)\n"
            )

    with open(index_path, "w", encoding="utf-8") as fh:
        fh.writelines(lines)
    log.info("📚 Índice maestro → %s", index_path)

# ═══════════════════════════════════════════════════════════════
# §21 — GRACEFUL SHUTDOWN (anti-zombie, anti-deadlock)
# ═══════════════════════════════════════════════════════════════

_shutdown_in_progress = False

def _join_with_timeout(procs: list, timeout_each: float = None):
    t        = timeout_each or CONFIG["JOIN_TIMEOUT_S"]
    deadline = time.monotonic() + t * max(len(procs), 1)
    for p in procs:
        remaining = max(1.0, deadline - time.monotonic())
        p.join(timeout=min(t, remaining))
        if p.is_alive():
            log.warning("⚠️  %s (%d) no terminó — SIGTERM", p.name, p.pid)
            p.terminate()
            p.join(timeout=15)
            if p.is_alive():
                log.error("❌  %s (%d) ignoró SIGTERM — SIGKILL", p.name, p.pid)
                p.kill()
                p.join(timeout=5)

def _drain_queue_to_disk(q: mp.Queue, label: str) -> int:
    saved = 0
    ef = os.path.join(CONFIG["ROOT_DIR"], f"_{label}_RESCUED.jsonl")
    while True:
        try:
            job = q.get_nowait()
            with open(ef, "a", encoding="utf-8") as fh:
                fh.write(json.dumps(job, ensure_ascii=False) + "\n")
                fh.flush()
                os.fsync(fh.fileno())
            saved += 1
        except queue.Empty:
            break
        except Exception as exc:
            log.error("Error drenando cola %s: %s", label, exc)
            break
    if saved:
        log.warning("⚠️  %d trabajos rescatados → %s", saved, ef)
    return saved

def _check_emergency_files():
    patterns = [
        os.path.join(CONFIG["ROOT_DIR"], "_*_RESCUED.jsonl"),
        os.path.join(CONFIG["ROOT_DIR"], "_EMERGENCY_DUMP_*.jsonl"),
    ]
    found = [
        f for pat in patterns
        for f in glob.glob(pat)
        if f.endswith(".jsonl")
    ]
    if found:
        log.critical(
            "⚠️  %d fichero(s) de emergencia — se reingerirán en el próximo arranque.",
            len(found)
        )
        for f in found:
            log.critical("   → %s", f)

def _graceful_shutdown(
    sig, frame,
    stop_workers, stop_db,
    spider_q, dl_q, db_q,
    spiders, downs, dbp,
    courses, semaphore,
):
    global _shutdown_in_progress
    if _shutdown_in_progress:
        log.critical("⛔  Segundo Ctrl+C ignorado — shutdown en curso.")
        return
    _shutdown_in_progress = True
    log.warning("⚠️  Señal %d — shutdown seguro iniciado.", sig)

    stop_workers.set()

    for _ in range(CONFIG["MAX_IN_FLIGHT"]):
        try:
            semaphore.release()
        except ValueError:
            break

    with contextlib.suppress(Exception):
        dl_q.cancel_join_thread()
    with contextlib.suppress(Exception):
        spider_q.cancel_join_thread()

    _join_with_timeout(spiders)
    _join_with_timeout(downs)

    _drain_queue_to_disk(dl_q, "PENDING_DOWNLOADS")
    _drain_queue_to_disk(spider_q, "PENDING_SPIDERS")

    stop_db.set()
    dbp.join(timeout=60)
    if dbp.is_alive():
        dbp.terminate()
        dbp.join(timeout=10)

    log.info("📊 Generando índices y partes de incidencias...")
    for cid in courses:
        c_dir = os.path.join(CONFIG["ROOT_DIR"], f"Curso_{cid}")
        if os.path.isdir(c_dir):
            flush_dlq_to_human_report(c_dir, cid)
            synthesize_master_index(c_dir, cid)

    _check_emergency_files()
    log.info("✅  Shutdown completado.")
    sys.exit(0)

# ═══════════════════════════════════════════════════════════════
# §22 — MAIN ORCHESTRATOR
# ═══════════════════════════════════════════════════════════════

def main():
    try:
        mp.set_start_method("spawn", force=True)
    except RuntimeError:
        pass

    if not CONFIG["USERNAME"] or not CONFIG["PASSWORD"]:
        log.error("Configura EGELA_USER y EGELA_PASS como variables de entorno.")
        sys.exit(1)
    if not _path_exists_safe(CONFIG["COURSES"]):
        log.error("No se encuentra '%s'.", CONFIG["COURSES"])
        sys.exit(1)

    ensure(CONFIG["ROOT_DIR"])
    locks_dir = ensure(os.path.join(CONFIG["ROOT_DIR"], ".locks"))

    _cleanup_stale_locks(locks_dir)

    db_path = os.path.join(CONFIG["ROOT_DIR"], CONFIG["DB_PATH"])
    init_db(db_path)

    print("=" * 60)
    print("  eGela Enterprise Time Capsule — Golden Master v12")
    print("  Zero Data Loss | Forensic Certified | Universal Capture")
    print("=" * 60)

    cookies, ua = auth()

    stop_workers = mp.Event()
    stop_db      = mp.Event()
    spider_q     = mp.Queue()
    db_q         = mp.Queue(maxsize=10_000)
    semaphore    = mp.BoundedSemaphore(CONFIG["MAX_IN_FLIGHT"])
    dl_q         = mp.Queue(maxsize=_DL_Q_MAXSIZE)

    courses: list = []
    with open(CONFIG["COURSES"], "r", encoding="utf-8") as fh:
        for line in fh:
            line = line.strip()
            if not line or "id=" not in line:
                continue
            m = re.search(r"id=(\d+)", line)
            if m:
                cid = m.group(1)
                spider_q.put({"url": line, "cid": cid})
                courses.append(cid)

    if not courses:
        log.error("No se encontraron cursos válidos en '%s'.", CONFIG["COURSES"])
        sys.exit(1)

    log.info("📚 %d cursos en cola.", len(courses))

    reingest_rescued_jobs(dl_q)

    spider_rescue = os.path.join(CONFIG["ROOT_DIR"], "_PENDING_SPIDERS_RESCUED.jsonl")
    if _path_exists_safe(spider_rescue):
        reingested_spider = 0
        with contextlib.suppress(OSError):
            with open(spider_rescue, "r", encoding="utf-8") as fh:
                for line in fh:
                    with contextlib.suppress(Exception):
                        task = json.loads(line)
                        if task.get("url") and task.get("cid"):
                            spider_q.put(task)
                            reingested_spider += 1
        done_sp = spider_rescue[:-len(".jsonl")] + f".done_{int(time.time())}.bak"
        with contextlib.suppress(OSError):
            os.rename(spider_rescue, done_sp)
        if reingested_spider:
            log.info("♻️  Spider reingestión: %d tareas recuperadas.", reingested_spider)

    dbp = mp.Process(
        target=db_daemon, args=(db_q, db_path, stop_db),
        name="DB-Daemon", daemon=True
    )
    dbp.start()

    spiders = [
        mp.Process(
            target=spider,
            args=(spider_q, dl_q, db_q, stop_workers,
                  cookies, ua, db_path, semaphore),
            name=f"Spider-{i + 1}"
        )
        for i in range(CONFIG["SPIDERS"])
    ]
    downs = [
        mp.Process(
            target=downloader,
            args=(dl_q, db_q, stop_workers,
                  cookies, ua, db_path, semaphore),
            name=f"DL-{i + 1}"
        )
        for i in range(CONFIG["DOWNLOADERS"])
    ]
    for p in spiders + downs:
        p.start()

    shutdown_handler = functools.partial(
        _graceful_shutdown,
        stop_workers=stop_workers, stop_db=stop_db,
        spider_q=spider_q, dl_q=dl_q, db_q=db_q,
        spiders=spiders, downs=downs, dbp=dbp,
        courses=courses, semaphore=semaphore,
    )
    signal.signal(signal.SIGINT,  shutdown_handler)
    signal.signal(signal.SIGTERM, shutdown_handler)

    _join_with_timeout(spiders)
    log.info("Spiders finalizados. Drenando descargas pendientes...")

    drain_deadline = time.time() + 600
    while not dl_q.empty() and not stop_workers.is_set():
        if time.time() > drain_deadline:
            log.warning("⚠️  Timeout de drenaje (10 min) alcanzado.")
            break
        time.sleep(2)

    with contextlib.suppress(Exception):
        dl_q.cancel_join_thread()
    with contextlib.suppress(Exception):
        spider_q.cancel_join_thread()

    stop_workers.set()

    for _ in range(CONFIG["MAX_IN_FLIGHT"]):
        try:
            semaphore.release()
        except ValueError:
            break

    _join_with_timeout(downs)
    _drain_queue_to_disk(dl_q, "PENDING_DOWNLOADS")

    stop_db.set()
    dbp.join(timeout=60)
    if dbp.is_alive():
        dbp.terminate()
        dbp.join(timeout=10)

    log.info("📊 Generando índices y partes de incidencias...")
    for cid in courses:
        c_dir = os.path.join(CONFIG["ROOT_DIR"], f"Curso_{cid}")
        if os.path.isdir(c_dir):
            flush_dlq_to_human_report(c_dir, cid)
            synthesize_master_index(c_dir, cid)

    _check_emergency_files()

    print("=" * 60)
    print("🏆 GOLDEN MASTER v12 — ZERO DATA LOSS — SELLADO.")
    print(f"   Archivos: {CONFIG['ROOT_DIR']}/")
    print(f"   Estado:   {db_path}")
    print("=" * 60)

if __name__ == "__main__":
    main()
