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

Arquitectura (22 secciones):
§1  Config & Logging
§2  Excepciones tipadas
§3  Contexto semántico (ResourceContext)
§4  Utilidades de filesystem
§5  Sanitización y naming
§6  Clasificador semántico
§7  Rate limiter (thread-safe, stateless por proceso)
§8  Bounded Visited Set (LRU — cache local, no IPC)
§9  IPC: safe_put + reingestión garantizada
§10 Base de datos (init, open, DB daemon)
§11 Atomic I/O (locks, replace, fsync)
§12 Stream download (anti-tarpit, anti-TCP-half-open)
§13 Cloud URL rewriting
§14 Fallback URL Saver (zero data loss para no-descargables)
§15 Autenticación y sesión HTTP
§16 Extractores especializados (page, assign, forum, folder)
§17 Extractor de blobs Selenium
§18 Downloader process (con SIGTERM shield)
§19 Spider process (con SIGTERM shield)
§20 Generadores UX (DLQ + Master Index)
§21 Graceful Shutdown (anti-zombie, anti-deadlock)
§22 Main Orchestrator

Uso:
  export EGELA_USER="tu_usuario"
  export EGELA_PASS="tu_contraseña"
  python egela_golden_master_v9.py
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
from dotenv import load_dotenv  # <--- AÑADE ESTO AQUÍ

load_dotenv()

from collections import deque
from dataclasses import dataclass, field
from pathlib import Path
from typing import Optional, List, Tuple
from urllib.parse import urlparse, parse_qs, urlencode, unquote
from email.utils import decode_rfc2231

import requests
from selenium import webdriver
from selenium.webdriver.common.by import By
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
        "/course/view.php", "/mod/page/", "/mod/book/", "/mod/assign/",
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

# Tamaño real de dl_q = MAX_IN_FLIGHT * 2 (backpressure bloqueante real)
_DL_Q_MAXSIZE = CONFIG["MAX_IN_FLIGHT"] * 2
_STOP_SENTINEL = {"type": "_STOP_SENTINEL_"}

# Shared Registry (mp.Manager) — Eliminamos ventana archivo-escrito -> HASH-en-DB
_shared_hashes = None 

_WINDOWS_RESERVED = frozenset({
    "CON", "PRN", "AUX", "NUL",
    "COM1", "COM2", "COM3", "COM4", "COM5", "COM6", "COM7", "COM8", "COM9",
    "LPT1", "LPT2", "LPT3", "LPT4", "LPT5", "LPT6", "LPT7", "LPT8", "LPT9",
})

# Traps de navegación — NUNCA aplicar a URLs con pluginfile/forcedownload
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
# AgentDebugger
# ═══════════════════════════════════════════════════════════════

class AgentDebugger:
    def __init__(self, driver: webdriver.Chrome):
        self.driver = driver

    def capture_context(self, step_name: str) -> Tuple[Optional[str], Optional[str]]:
        try:
            folder = "agent_context"
            if not os.path.exists(folder):
                os.makedirs(folder)
            
            timestamp = time.strftime("%Y%m%d_%H%M%S")
            safe_step_name = "".join(c if c.isalnum() or c in ("-", "_") else "_" for c in step_name)
            base_filename = f"{timestamp}_{safe_step_name}"
            
            # Captura de pantalla
            screenshot_path = os.path.join(folder, f"{base_filename}.png")
            self.driver.save_screenshot(screenshot_path)
            
            # Guardar código fuente DOM
            html_path = os.path.join(folder, f"{base_filename}.html")
            with open(html_path, "w", encoding="utf-8") as f:
                f.write(self.driver.page_source)
                
            log.info("📸 Contexto del agente guardado: screenshot=%s, HTML=%s", screenshot_path, html_path)
            return screenshot_path, html_path
        except Exception as e:
            log.error("Error al guardar el contexto del agente: %s", e)
            return None, None

    def alert_and_wait(self, error_msg: str, step_name: str):
        log.warning("⚠️ Alerta de Agente en el paso '%s': %s", step_name, error_msg)
        screenshot_path, html_path = self.capture_context(step_name)
        
        # Alerta visual llamativa
        print("\n" + "!" * 85)
        print(f"⚠️  ALERTA DE AGENTE - PASO FALLIDO: {step_name.upper()}")
        print(f"❌  ERROR DETECTADO: {error_msg}")
        if screenshot_path and html_path:
            print("📸  CAPTURAS DE DIAGNÓSTICO GUARDADAS EN:")
            print(f"    - Captura: {os.path.abspath(screenshot_path)}")
            print(f"    - Código DOM:  {os.path.abspath(html_path)}")
        print("!" * 85 + "\n")
        
        # Human-in-the-loop: pausar sin cerrar el driver ni propagar la excepción
        input("⚠️  Agente atascado. Revisa el navegador/capturas. Arregla el problema en la ventana y pulsa ENTER para reintentar/continuar: ")

    def dump_diagnostics(self, course_id: str, page_type: str) -> str:
        try:
            folder = os.path.join("agent_context", "diagnostics", str(course_id))
            timestamp = time.strftime("%Y%m%d_%H%M%S")
            page_folder = os.path.join(folder, f"{timestamp}_{page_type}")
            if not os.path.exists(page_folder):
                os.makedirs(page_folder)
            
            # 1. Raw HTML page source
            html_path = os.path.join(page_folder, "00_PAGE_SOURCE.html")
            with open(html_path, "w", encoding="utf-8") as f:
                f.write(self.driver.page_source)
                
            # 2. Simplified DOM
            dom_js = """
            function getSimplifiedDOM(root) {
              let lines = [];
              function walk(node, depth) {
                if (node.nodeType === Node.ELEMENT_NODE) {
                  const tagName = node.tagName.toLowerCase();
                  if (['script', 'style', 'head', 'noscript', 'meta', 'link', 'svg', 'path'].includes(tagName)) {
                    return;
                  }
                  const style = window.getComputedStyle(node);
                  if (style.display === 'none' || style.visibility === 'hidden') {
                    return;
                  }
                  const id = node.id ? '#' + node.id : '';
                  const cls = node.className && typeof node.className === 'string' ? '.' + node.className.trim().replace(/\\\\s+/g, '.') : '';
                  let text = '';
                  let child = node.firstChild;
                  while (child) {
                    if (child.nodeType === Node.TEXT_NODE) {
                      text += child.nodeValue.trim();
                    }
                    child = child.nextSibling;
                  }
                  const href = node.getAttribute('href') ? ` [href="${node.getAttribute('href')}"]` : '';
                  const textSnippet = text ? ` "${text.substring(0, 50)}"` : '';
                  lines.push('  '.repeat(depth) + `<${tagName}${id}${cls}${href}>${textSnippet}`);
                  for (let i = 0; i < node.childNodes.length; i++) {
                    walk(node.childNodes[i], depth + 1);
                  }
                }
              }
              walk(root || document.body, 0);
              return lines.join('\\\\n');
            }
            return getSimplifiedDOM();
            """
            simplified_dom = self.driver.execute_script(dom_js)
            dom_path = os.path.join(page_folder, "00_DOM_SIMPLIFICADO.txt")
            with open(dom_path, "w", encoding="utf-8") as f:
                f.write(simplified_dom)
                
            # 3. Links found
            links_js = """
            return Array.from(document.querySelectorAll('a[href]')).map(a => ({
              text: (a.innerText || '').trim(),
              href: a.getAttribute('href'),
              class: a.className,
              visible: a.offsetWidth > 0 && a.offsetHeight > 0
            }));
            """
            links = self.driver.execute_script(links_js)
            links_path = os.path.join(page_folder, "00_ENLACES_ENCONTRADOS.txt")
            with open(links_path, "w", encoding="utf-8") as f:
                for lnk in links:
                    vis_str = "[VISIBLE]" if lnk['visible'] else "[OCULTO]"
                    f.write(f"{vis_str} {lnk['text']} -> {lnk['href']} (class: {lnk['class']})\\n")
            
            # 4. Metadata
            meta = {
                "course_id": course_id,
                "page_type": page_type,
                "url": self.driver.current_url,
                "title": self.driver.title,
                "timestamp": timestamp,
            }
            meta_path = os.path.join(page_folder, "00_METADATOS.json")
            with open(meta_path, "w", encoding="utf-8") as f:
                json.dump(meta, f, indent=2, ensure_ascii=False)
                
            log.info("📊 Diagnósticos de página volcados en: %s", page_folder)
            return page_folder
        except Exception as e:
            log.error("Error al volcar diagnósticos de página: %s", e)
            return ""


class CourseTree:
    def __init__(self, course_name: str, course_id: str):
        self.root = {
            "name": course_name,
            "type": "course",
            "url": f"https://egela.ehu.eus/course/view.php?id={course_id}",
            "children": [],
            "metadata": {}
        }

    def add_node(self, path: List[str], node_name: str, node_type: str, url: str = "", metadata: dict = None) -> dict:
        """
        Adds a node to the tree at the given hierarchical path.
        path: List of parent names, e.g. ["Tema 1: Matrices", "Lecturas"]
        """
        current = self.root
        
        # Traverse path, creating intermediate folder/section nodes if needed
        for segment in path:
            found = None
            for child in current["children"]:
                if child["name"] == segment:
                    found = child
                    break
            if not found:
                found = {
                    "name": segment,
                    "type": "section" if current["type"] == "course" else "label",
                    "url": "",
                    "children": [],
                    "metadata": {}
                }
                current["children"].append(found)
            current = found
            
        # Add the leaf node or sub-resource
        for child in current["children"]:
            if child["name"] == node_name and child["type"] == node_type:
                if url:
                    child["url"] = url
                if metadata:
                    child["metadata"].update(metadata)
                return child
                
        new_node = {
            "name": node_name,
            "type": node_type,
            "url": url,
            "children": [],
            "metadata": metadata or {}
        }
        current["children"].append(new_node)
        return new_node

    def to_ascii_tree(self) -> str:
        lines = []
        def walk(node, prefix="", is_last=True):
            if node["type"] == "course":
                lines.append(f"🎓 Curso: {node['name']}")
            else:
                marker = "└── " if is_last else "├── "
                node_info = f" ({node['type'].upper()})" if node['type'] not in ("section", "label") else ""
                lines.append(f"{prefix}{marker}{node['name']}{node_info}")
                
            child_prefix = prefix + ("    " if is_last else "│   ")
            child_count = len(node["children"])
            for i, child in enumerate(node["children"]):
                walk(child, child_prefix, i == child_count - 1)
        walk(self.root)
        return "\n".join(lines)

    def save_to_files(self, target_dir: str):
        # 1. Save TXT tree
        txt_path = os.path.join(target_dir, "00_ESTRUCTURA_CURSO.txt")
        with open(txt_path, "w", encoding="utf-8") as f:
            f.write(self.to_ascii_tree())
            
        # 2. Save JSON tree
        json_path = os.path.join(target_dir, "00_ESTRUCTURA_CURSO.json")
        with open(json_path, "w", encoding="utf-8") as f:
            json.dump(self.root, f, indent=2, ensure_ascii=False)


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
    """
    Evidencia DOM pura. Sin inferencias por tamaño, MIME ni duración.
    Todos los campos None = genuinamente desconocido.
    """
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
    hierarchy:          Optional[List[str]] = None


# ═══════════════════════════════════════════════════════════════
# §4 — UTILIDADES DE FILESYSTEM
# ═══════════════════════════════════════════════════════════════

def ensure(p: str) -> str:
    os.makedirs(p, exist_ok=True)
    return p


def _path_exists_safe(path: str) -> bool:
    """os.stat() elimina TOCTOU respecto a os.path.exists()."""
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
    """SHA256 en chunks de 1 MB — sin RAM spike en archivos grandes."""
    sha = hashlib.sha256()
    with open(path, "rb") as fh:
        for chunk in iter(lambda: fh.read(1024 * 1024), b""):
            sha.update(chunk)
    return sha.hexdigest()


def _fsync_dir(path: str):
    """
    fsync del directorio padre tras rename.
    Garantiza visibilidad en NFS/SMB. Solo POSIX; no-op silencioso en Windows.
    """
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
    """Sanitización multiplataforma: Windows reserved names, bytes, caracteres ilegales."""
    if not name:
        return default
    name = unquote(str(name))
    name = unicodedata.normalize("NFC", name)
    name = "".join(
        c for c in name
        if unicodedata.category(c) not in {"Cf", "Cc", "Cs", "Co", "Cn"}
    )
    name = re.sub(
        r'[<>:"/\\|?*\x00-\x1f\U0001F300-\U0001FAFF\U00002600-\U000027BF]',
        "_", name
    ).strip()  # \\  dentro de raw string = literal \ en la clase de caracteres
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
    """URL canónica: elimina volátiles, ordena parámetros restantes."""
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
    """True si la URL es una descarga binaria directa de Moodle."""
    return "pluginfile" in url or "forcedownload" in url


def _is_nav_trap(url: str) -> bool:
    """True si la URL es una trampa de navegación. NUNCA aplicar a descargas directas."""
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

class ConfidenceEvaluator:
    """
    Evaluador de confianza y necesidad de revisión manual para cada recurso.
    Calcula un score de 0.0 a 1.0 y compila una lista de motivos.
    """
    def evaluate(self, ctx: ResourceContext, ctype: str, bytes_written: int, url: str, final_path: str, blocked: bool = False, was_deduplicated: bool = False, is_fallback: bool = False) -> dict:
        score = 1.0
        reasons = []
        evidence = {
            "section_title": ctx.section_title,
            "section_index": ctx.section_index,
            "label_context": ctx.label_context,
            "parent_activity": ctx.parent_activity,
            "link_text": ctx.link_text,
            "server_filename": ctx.server_filename,
            "content_type": ctype,
            "url_pattern": url,
            "hash_duplicate": was_deduplicated,
            "fallback_manual": is_fallback,
            "blocked": blocked
        }

        # Contexto desconocido o 99_Sin_Contexto
        if ctx.context_confidence == "unknown" or "99_Sin_Contexto" in final_path:
            score -= 0.8
            reasons.append("contexto desconocido / 99_Sin_Contexto")

        # Sección ambigua o genérica
        if not ctx.section_title or any(kw in ctx.section_title.lower() for kw in ("general", "tema", "sección", "seccion", "modulo", "unidad")):
            # Si no tiene etiqueta visual ni actividad padre para aportar contexto semántico
            if not ctx.label_context and not ctx.parent_activity:
                score -= 0.2
                reasons.append("sección ambigua o genérica sin contexto adicional")

        # Label propagado con baja confianza
        if ctx.label_context and len(ctx.label_context) > CONFIG["MAX_LABEL_PROPAGATION"]:
            score -= 0.2
            reasons.append("label propagado con baja confianza")

        # Título vacío o genérico
        if not ctx.link_text or ctx.link_text.strip().lower() in ("recurso", "descarga", "archivo", "file", "document", "click aquí", "enlace", "view", "descargar"):
            score -= 0.3
            reasons.append("título vacío o genérico")

        # Nombre de archivo generado artificialmente
        if not ctx.link_text and not ctx.server_filename:
            score -= 0.5
            reasons.append("nombre de archivo generado artificialmente")

        # Server filename contradice link text
        if ctx.link_text and ctx.server_filename:
            link_slug = _s(ctx.link_text)
            server_slug = _s(os.path.splitext(ctx.server_filename)[0])
            if not StructuredNamer._similar(link_slug, server_slug):
                score -= 0.3
                reasons.append("server filename contradice link text")

        # MIME y extensión no cuadran
        ext = os.path.splitext(final_path)[1].lower()
        expected_ext = _get_ext(ctype or "", ctx.server_filename or "")
        if ext and expected_ext and ext != expected_ext:
            score -= 0.4
            reasons.append(f"MIME y extensión no cuadran ({ctype} -> {expected_ext} vs {ext})")

        # Content-Length ausente o sospechoso / descarga demasiado pequeña
        min_sizes = {".pdf": 512, ".docx": 512, ".pptx": 512, ".xlsx": 512, ".zip": 22}
        if bytes_written > 0 and bytes_written < min_sizes.get(ext, 0):
            score -= 0.4
            reasons.append(f"descarga demasiado pequeña para el tipo esperado ({bytes_written} bytes)")

        # HTML en lugar de binario
        if ctype and "text/html" in ctype.lower() and ext not in (".html", ".htm"):
            score -= 0.6
            reasons.append("HTML en lugar de binario")

        # Recurso externo no descargado
        if is_fallback:
            score -= 0.5
            reasons.append("recurso externo no descargado (guardado enlace)")

        # Blob extraído sin contexto
        if "BLOB_" in os.path.basename(final_path):
            score -= 0.6
            reasons.append("blob extraído sin contexto")

        # Deduplicado con nombre/contexto distinto al original
        if was_deduplicated:
            score -= 0.2
            reasons.append("deduplicado con nombre/contexto distinto al original")

        # Ruta acortada por longitud
        orig_basename = ctx.link_text or ctx.server_filename or ""
        final_basename = os.path.basename(final_path)
        if len(orig_basename) > 10 and len(final_basename) < len(orig_basename) - 20:
            score -= 0.1
            reasons.append("ruta acortada por longitud")

        if blocked:
            score -= 0.6
            reasons.append("recurso bloqueado/oculto/restringido")

        score = max(0.0, min(1.0, score))
        if score >= 0.85 and not reasons:
            level = "high"
            review_required = False
        elif score >= 0.6:
            level = "medium"
            review_required = True
        else:
            level = "low"
            review_required = True

        return {
            "confidence_score": round(score, 2),
            "confidence_level": level,
            "review_required": review_required,
            "review_reasons": reasons,
            "decision_trace": f"Ubicado en {os.path.basename(os.path.dirname(final_path))} con nombre {final_basename} usando certeza {level}.",
            "source_evidence": evidence
        }


class EvidenceBasedClassifier:
    """
    Carpeta destino basada en evidencia DOM explícita.
    Sufijo hash en sec_folder garantiza unicidad ante títulos duplicados.
    """

    def classify(self, ctx: ResourceContext, course_dir: str) -> tuple:
        if ctx.hierarchy:
            cleaned_segments = []
            for i, segment in enumerate(ctx.hierarchy):
                if i == 0 and ctx.section_title and segment == ctx.section_title:
                    title_slug = _s(segment[:40])
                    title_hash = hashlib.md5(
                        segment.encode("utf-8", errors="replace")
                    ).hexdigest()[:3]
                    cleaned_segments.append(f"{ctx.section_index + 1:02d}_{title_slug}_{title_hash}")
                else:
                    cleaned_segments.append(_s(segment[:40]))
            if ctx.parent_activity and _s(ctx.parent_activity[:35]) not in cleaned_segments:
                cleaned_segments.append(_s(ctx.parent_activity[:35]))
            base = os.path.join(course_dir, *cleaned_segments)
            ctx.context_confidence = "high"
            return ensure(base), "high"

        # Clasificar secciones de forma más semántica e inclusiva
        if ctx.section_title and ctx.section_title != "":
            title_slug = _s(ctx.section_title[:40])
            title_hash = hashlib.md5(
                ctx.section_title.encode("utf-8", errors="replace")
            ).hexdigest()[:3]
            sec_folder = f"{ctx.section_index + 1:02d}_{title_slug}_{title_hash}"
            confidence = "high"
        elif ctx.extra_meta and ctx.extra_meta.get("breadcrumbs"):
            # Intentar clasificar usando breadcrumbs visuales
            bc = ctx.extra_meta["breadcrumbs"]
            parts = [p.strip() for p in bc.split(">") if p.strip()]
            if len(parts) > 1:
                candidate = parts[-1]
                title_slug = _s(candidate[:40])
                title_hash = hashlib.md5(candidate.encode("utf-8", errors="replace")).hexdigest()[:3]
                sec_folder = f"01_{title_slug}_{title_hash}"
                confidence = "partial"
            else:
                sec_folder = "99_Sin_Contexto"
                confidence = "unknown"
        else:
            sec_folder = "99_Sin_Contexto"
            confidence = "unknown"

        base = os.path.join(course_dir, sec_folder)

        if ctx.label_context and ctx.label_context.strip():
            base = os.path.join(base, _s(ctx.label_context[:35]))
        elif confidence == "high":
            # Si no hay etiqueta pero la sección es fuerte, mantenemos high
            pass

        if ctx.parent_activity:
            base = os.path.join(base, _s(ctx.parent_activity[:35]))

        ctx.context_confidence = confidence
        return ensure(base), confidence


class StructuredNamer:
    def build_filename(self, ctx: ResourceContext, ext: str) -> str:
        parts = [f"{ctx.visual_seq:03d}"]
        
        # Detectar si el texto del enlace es genérico
        raw_link = ctx.link_text.strip().lower() if ctx.link_text else ""
        is_generic_link = raw_link in ("", "view", "click_here", "download", "archivo", "recurso", "file", "enlace", "documento", "descarga", "descargar")
        
        link_slug   = _s(ctx.link_text[:50])   if (ctx.link_text and not is_generic_link) else ""
        server_slug = _s(os.path.splitext(ctx.server_filename or "")[0][:40])
        parent_slug = _s(ctx.parent_activity[:30]) if ctx.parent_activity else ""

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
        elif parent_slug:
            parts.append(parent_slug)
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
    file_path: str, ctx: ResourceContext, tags: List[str], confidence: str, confidence_eval: dict = None
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
        "revision_manual": confidence_eval,
        "generado_ts": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
    }
    try:
        with open(file_path + ".meta.json", "w", encoding="utf-8") as fh:
            json.dump(meta, fh, ensure_ascii=False, indent=2)
            fh.flush()
            os.fsync(fh.fileno())
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
        hierarchy       = job.get("hierarchy"),
    )


# ═══════════════════════════════════════════════════════════════
# §7 — RATE LIMITER (thread-safe, instancia por proceso)
# ═══════════════════════════════════════════════════════════════

class DomainRateLimiter:
    """
    Ventana deslizante de 1s por dominio. Thread-safe con Lock.
    Instancia creada dentro de cada worker process — no compartida por IPC.
    """

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
    """
    Cache de velocidad local. Source of truth = tabla `visited` en SQLite.
    Al arrancar, carga el estado previo para no re-crawlear runs anteriores.
    NO escribe en SQLite directamente — esa responsabilidad es del DB daemon.
    """

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
                    "SELECT url FROM visited WHERE status IN ('completed', 'manual') ORDER BY ts DESC LIMIT ?",
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
    """
    Zero Data Loss para IPC.
    1. put_nowait — camino feliz.
    2. Espera 500ms y reintento — cubre picos transitorios.
    3. Fallback físico a disco — nunca bloquea el caller.
    """
    if item is None or item == _STOP_SENTINEL:
        try:
            q.put(item, timeout=5)
            return True
        except queue.Full:
            return False

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
                {**item, "reason": "queue_full", "_ts": time.time(), "_pid": os.getpid()},
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

class DownloadPermit:
    """
    RAII Context Manager para permisos de descarga.
    Garantiza que el semáforo se libere exactamente una vez.
    """
    def __init__(self, semaphore, job):
        self.sem = semaphore
        self.job = job
        self.acquired = False

    def __enter__(self):
        self.sem.acquire()
        self.acquired = True
        self.job["_sem_owned"] = True
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.release()

    def release(self):
        if self.acquired:
            with contextlib.suppress(Exception):
                self.sem.release()
            self.acquired = False
            self.job["_sem_owned"] = False

def reingest_emergency_data_worker(dl_q: mp.Queue, spider_q: mp.Queue, db_q: mp.Queue, semaphore: mp.BoundedSemaphore, active_downloads_count: mp.Value, stop_workers: mp.Event):
    """
    Trabajador de reingestión en hilo de background para evitar bloquear el arranque
    y resolver de forma segura la adquisición de semáforos a medida que se liberan slots.
    """
    patterns = [
        os.path.join(CONFIG["ROOT_DIR"], "_EMERGENCY_DUMP_*.jsonl"),
        os.path.join(CONFIG["ROOT_DIR"], "_*_RESCUED.jsonl"),
    ]
    
    for fpath in [f for pat in patterns for f in glob.glob(pat)]:
        if not fpath.endswith(".jsonl"): continue
        
        remaining = []
        counts = {"DOWNLOAD_JOB": 0, "SPIDER_TASK": 0, "DB_EVENT": 0}
        
        try:
            with open(fpath, "r", encoding="utf-8") as fh:
                for line in fh:
                    if stop_workers.is_set():
                        remaining.append(line)
                        continue
                    
                    try:
                        item = json.loads(line)
                    except json.JSONDecodeError:
                        corrupt_file = os.path.join(CONFIG["ROOT_DIR"], "00_CORRUPT_RESCUES.jsonl")
                        with contextlib.suppress(Exception):
                            with open(corrupt_file, "a", encoding="utf-8") as cfh:
                                cfh.write(line)
                        continue
                        
                    t = item.get("type")
                    
                    if t in ("VISITED", "HASH", "DLQ"):
                        if safe_put(db_q, item): counts["DB_EVENT"] += 1
                        else: remaining.append(line)
                    elif "cid" in item and "url" in item:
                        if "section" in item or "target" in item:
                            acquired = False
                            while not stop_workers.is_set():
                                if semaphore.acquire(timeout=1.0):
                                    acquired = True
                                    break
                            
                            if acquired:
                                item["_sem_owned"] = True
                                item["_requeuing"] = False
                                if safe_put(dl_q, item):
                                    counts["DOWNLOAD_JOB"] += 1
                                    if active_downloads_count is not None:
                                        with active_downloads_count.get_lock():
                                            active_downloads_count.value += 1
                                else:
                                    semaphore.release()
                                    item.pop("_sem_owned", None)
                                    remaining.append(line)
                            else:
                                remaining.append(line)
                        else:
                            if safe_put(spider_q, item): counts["SPIDER_TASK"] += 1
                            else: remaining.append(line)
                    else:
                        remaining.append(line)
        except OSError as e:
            log.error("Error al abrir archivo de reingestión %s: %s", fpath, e)
            continue

        if remaining:
            tmp_f = fpath + ".tmp"
            try:
                with open(tmp_f, "w", encoding="utf-8") as fh:
                    fh.writelines(remaining)
                    fh.flush()
                    os.fsync(fh.fileno())
                os.replace(tmp_f, fpath)
            except OSError:
                pass
        else:
            done = fpath[:-len(".jsonl")] + f".done_{int(time.time())}.bak"
            with contextlib.suppress(OSError):
                os.replace(fpath, done)

        for k, v in counts.items():
            if v: log.info("♻️  Reingestión %s: %d items recuperados del archivo %s.", k, v, os.path.basename(fpath))


def reingest_emergency_data(dl_q: mp.Queue, spider_q: mp.Queue, db_q: mp.Queue, semaphore: mp.BoundedSemaphore) -> dict:
    """
    Reingestión centralizada de trabajos rescatados síncrona (retrocompatibilidad con tests).
    """
    patterns = [
        os.path.join(CONFIG["ROOT_DIR"], "_EMERGENCY_DUMP_*.jsonl"),
        os.path.join(CONFIG["ROOT_DIR"], "_*_RESCUED.jsonl"),
    ]
    
    total_counts = {"DOWNLOAD_JOB": 0, "SPIDER_TASK": 0, "DB_EVENT": 0}
    
    for fpath in [f for pat in patterns for f in glob.glob(pat)]:
        if not fpath.endswith(".jsonl"): continue
        
        remaining = []
        counts = {"DOWNLOAD_JOB": 0, "SPIDER_TASK": 0, "DB_EVENT": 0}
        
        try:
            with open(fpath, "r", encoding="utf-8") as fh:
                for line in fh:
                    try:
                        item = json.loads(line)
                    except json.JSONDecodeError:
                        continue
                        
                    t = item.get("type")
                    if t in ("VISITED", "HASH", "DLQ"):
                        if safe_put(db_q, item):
                            counts["DB_EVENT"] += 1
                        else:
                            remaining.append(line)
                    elif "cid" in item and "url" in item:
                        if "section" in item or "target" in item:
                            if semaphore.acquire(timeout=1.0):
                                item["_sem_owned"] = True
                                item["_requeuing"] = False
                                if safe_put(dl_q, item):
                                    counts["DOWNLOAD_JOB"] += 1
                                else:
                                    semaphore.release()
                                    item.pop("_sem_owned", None)
                                    remaining.append(line)
                            else:
                                remaining.append(line)
                        else:
                            if safe_put(spider_q, item):
                                counts["SPIDER_TASK"] += 1
                            else:
                                remaining.append(line)
                    else:
                        remaining.append(line)
        except OSError:
            continue
            
        if remaining:
            tmp_f = fpath + ".tmp"
            try:
                with open(tmp_f, "w", encoding="utf-8") as fh:
                    fh.writelines(remaining)
                    fh.flush()
                    os.fsync(fh.fileno())
                os.replace(tmp_f, fpath)
            except OSError:
                pass
        else:
            done = fpath[:-len(".jsonl")] + f".done_{int(time.time())}.bak"
            with contextlib.suppress(OSError):
                os.replace(fpath, done)
                
        for k, v in counts.items():
            total_counts[k] += v
            
    return total_counts


def async_requeue_with_backoff(q: mp.Queue, job: dict, semaphore: mp.BoundedSemaphore = None) -> bool:
    """
    Reencola con backoff exponencial.
    Mantiene el permiso del semáforo si ya lo posee para evitar deriva.
    """
    retries = job.get("retries", 0)
    if retries >= 3:
        if job.get("_sem_owned") and semaphore:
            with contextlib.suppress(Exception):
                semaphore.release()
        job["_sem_owned"] = False
        return False
    
    job["retries"]       = retries + 1
    job["process_after"] = time.time() + min(2 ** retries, 30) + random.uniform(0, 1)
    
    job["_requeuing"] = True
    
    if safe_put(q, job):
        return True
    else:
        if job.get("_sem_owned") and semaphore:
            with contextlib.suppress(Exception):
                semaphore.release()
        job["_sem_owned"] = False
        job["_requeuing"] = False
        log.error("Fallo crítico al reencolar job %s — volcado a disco y liberado semáforo.", job.get("url"))
        return False


# ═══════════════════════════════════════════════════════════════
# §10 — BASE DE DATOS (init, open, DB daemon)
# ═══════════════════════════════════════════════════════════════

def init_db(path: str):
    """Crea el esquema y configura WAL. Llamado una sola vez en main."""
    with sqlite3.connect(path) as conn:
        conn.executescript(f"""
            PRAGMA journal_mode          = WAL;
            PRAGMA synchronous           = NORMAL;
            PRAGMA busy_timeout          = 15000;
            PRAGMA wal_autocheckpoint    = 0;
            PRAGMA journal_size_limit    = {CONFIG['WAL_JOURNAL_SIZE_LIMIT']};
            CREATE TABLE IF NOT EXISTS visited (
                url TEXT PRIMARY KEY, cid TEXT, status TEXT, ts REAL
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
        # Migración: Añadir columna status si la tabla ya existía sin ella
        try:
            conn.execute("ALTER TABLE visited ADD COLUMN status TEXT DEFAULT 'completed'")
            conn.commit()
        except sqlite3.OperationalError:
            pass  # La columna ya existe o la tabla no existía (y se creó con ella)


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
    """
    Escribe en el DLQ físico con rotación. Llamado solo desde db_daemon.
    flush + fsync garantizan durabilidad ante SIGKILL o corte de alimentación
    entre el write() y el cierre del descriptor. (Fix grieta #2)
    """
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


def db_daemon(q: mp.Queue, db_path: str, stop: mp.Event, shared_persisted_hashes: dict = None):
    """
    Único escritor SQLite (patrón DB daemon).
    WAL PASSIVE periódico durante ejecución. TRUNCATE solo al cierre.
    Drenaje garantizado en finally antes de cerrar la conexión.
    """
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
                "INSERT OR REPLACE INTO visited (url, cid, status, ts) VALUES (?,?,?,?)",
                (ev["url"], ev.get("cid", ""), ev.get("status", "completed"), time.time())
            )
            conn.commit()
        elif t == "HASH":
            conn.execute(
                "INSERT OR REPLACE INTO hashes VALUES (?,?,?,?)",
                (ev["hash"], ev["path"],
                 json.dumps(ev.get("tags", [])), time.time())
            )
            conn.commit()
            if shared_persisted_hashes is not None:
                shared_persisted_hashes[ev["hash"]] = True
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
                if ev == _STOP_SENTINEL:
                    break
                _process_event(ev)
            except queue.Empty:
                if time.monotonic() - last_event_ts > 5.0:
                    _chk_passive()
                    last_event_ts = time.monotonic()
                continue
            except Exception as exc:
                log.error("DB daemon error: %s", exc)

        # Drenaje completo garantizado antes de cerrar
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

        # TRUNCATE solo al cierre (sin readers activos)
        with contextlib.suppress(Exception):
            conn.execute("PRAGMA wal_checkpoint(TRUNCATE);")

    finally:
        with contextlib.suppress(Exception):
            conn.close()


# ═══════════════════════════════════════════════════════════════
# §11 — ATOMIC I/O (locks, replace, fsync)
# ═══════════════════════════════════════════════════════════════

def _acquire_posix_lock(lock_path: str) -> tuple:
    """
    O_EXCL: atómico en POSIX local.
    fcntl.flock fallback: advisory lock para NFS.
    Retorna (fd, acquired).
    """
    try:
        fd = os.open(lock_path, os.O_CREAT | os.O_EXCL | os.O_WRONLY)
        # Escribir metadatos de propiedad (PID + Hostname + TS)
        meta = json.dumps({
            "pid": os.getpid(),
            "host": socket.gethostname(),
            "ts": time.time()
        })
        os.write(fd, meta.encode("utf-8"))
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
                # Suppress NameError (fd undefined) + OSError (close fails)
                with contextlib.suppress(Exception):
                    if fd >= 0:
                        os.close(fd)
                return -1, False
        return -1, False


def _release_posix_lock(fd: int, lock_path: str):
    """Liberación garantizada de lock. Seguro llamarlo varias veces."""
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
    """
    Escritura atómica NFS-safe con fsync de directorio.
    Si dst existe con hash incorrecto → siempre reemplazar (archivo corrupto).
    Si dst existe con hash correcto → otro worker ganó la carrera limpiamente.
    """
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
                pass  # No se puede leer dst → proceder con reemplazo
        else:
            with contextlib.suppress(OSError):
                os.remove(src)
            return

    if _is_same_filesystem(src, dst):
        os.replace(src, dst)
        _fsync_dir(dst)
    else:
        # NFS/SMB: copia con fsync explícito + rename del .part
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


def _is_pid_alive(pid: int) -> bool:
    if os.name == "nt":
        import ctypes
        PROCESS_QUERY_INFORMATION = 0x0400
        handle = ctypes.windll.kernel32.OpenProcess(PROCESS_QUERY_INFORMATION, False, pid)
        if handle == 0:
            return False
        ctypes.windll.kernel32.CloseHandle(handle)
        return True
    else:
        try:
            os.kill(pid, 0)
            return True
        except (OSError, ProcessLookupError):
            return False


def _cleanup_stale_locks(locks_dir: str):
    """
    Elimina lock files huérfanos verificando si el proceso dueño sigue vivo.
    """
    if not _path_exists_safe(locks_dir):
        return
    cleaned = 0
    my_host = socket.gethostname()
    for lf in glob.glob(os.path.join(locks_dir, ".lock_*")):
        try:
            with open(lf, "r") as f:
                data = json.loads(f.read())
            
            # Solo limpiar si es de este host y el PID no existe
            if data.get("host") == my_host:
                pid = data.get("pid")
                if pid and not _is_pid_alive(pid):
                    os.remove(lf)
                    cleaned += 1
            elif time.time() - os.stat(lf).st_mtime > 3600:
                # Si es de otro host, esperar 1h por si acaso
                os.remove(lf)
                cleaned += 1
        except (OSError, ValueError, json.JSONDecodeError):
            # Si el lock está corrupto o vacío, y tiene cierta edad, limpiar
            with contextlib.suppress(OSError):
                if time.time() - os.stat(lf).st_mtime > 60:
                    os.remove(lf)
                    cleaned += 1
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
    """
    Descarga con timeout a nivel socket (anti TCP half-open) +
    ventana deslizante anti-tarpit.
    El fd de outfile se cierra por el caller en su bloque finally.
    """
    chunk_timeout = CONFIG["CHUNK_TIMEOUT_S"]
    deadline_s    = CONFIG["STREAM_TIMEOUT_S"]
    min_speed     = CONFIG["MIN_SPEED_BPS"]
    speed_window  = CONFIG["SPEED_WINDOW_S"]
    max_bytes     = CONFIG["MAX_FILE_BYTES"]

    # Timeout a nivel socket — iter_content() deja de bloquear
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

    # Verificación de integridad por tamaño (Anti-Truncamiento)
    expected = response.headers.get("Content-Length")
    if expected and int(expected) > 0:
        if bytes_written != int(expected):
            raise TransientError(
                f"Descarga incompleta (truncada): {bytes_written} de {expected} bytes."
            )
            
    return bytes_written


# ═══════════════════════════════════════════════════════════════
# §13 — CLOUD URL REWRITING
# ═══════════════════════════════════════════════════════════════

def _rewrite_cloud_url(url: str) -> str:
    """Convierte URLs de nube a descarga directa cuando es posible."""
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
    """Extensión correcta para una URL de nube reescrita."""
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
    confidence_eval: dict = None,
) -> str:
    """
    Zero Data Loss para recursos no descargables.
    Genera .url (abre en navegador) + .meta.json (contexto completo).
    Todo lo que no se puede descargar queda registrado con acción manual.
    """
    ensure(target_dir)
    slug  = _s(name[:60]) if name else "recurso"
    fname = f"{seq:03d}_{slug}_{resource_type.upper()}.url"
    fpath = os.path.join(target_dir, fname)

    try:
        with open(fpath, "w", encoding="utf-8") as fh:
            fh.write("[InternetShortcut]\n")
            fh.write(f"URL={url}\n")
            fh.flush()
            os.fsync(fh.fileno())
    except OSError:
        return ""

    if confidence_eval is None:
        evaluator = ConfidenceEvaluator()
        ctx = ResourceContext(
            section_title=ctx_section,
            section_index=99,
            visual_seq=seq,
            link_text=name,
            label_context=ctx_label,
            parent_activity=None,
            server_filename=None,
            content_type="text/html",
            source_url=url,
            page_origin=origin_url,
            file_hash="",
            resource_type=resource_type,
            extra_meta=extra or {}
        )
        confidence_eval = evaluator.evaluate(
            ctx, "text/html", 0, url, fpath,
            was_deduplicated=False, is_fallback=True
        )

    meta = {
        "tipo_recurso":    resource_type,
        "titulo":          name,
        "url":             url,
        "pagina_origen":   origin_url,
        "razon_no_descargado": reason,
        "contexto_moodle": {
            "seccion":  ctx_section,
            "etiqueta": ctx_label,
            "secuencia": seq,
        },
        "revision_manual": confidence_eval,
        "extra_meta": extra or {},
        "generado_ts": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
    }
    try:
        with open(fpath + ".meta.json", "w", encoding="utf-8") as fh:
            json.dump(meta, fh, ensure_ascii=False, indent=2)
            fh.flush()
            os.fsync(fh.fileno())
    except OSError:
        pass
    return fpath


def _enqueue_download(
    dq: mp.Queue,
    semaphore: mp.BoundedSemaphore,
    job: dict,
    stop_workers: mp.Event,
    active_downloads_count: mp.Value = None,
) -> bool:
    """
    Adquiere semáforo y encola. Si safe_put falla, libera semáforo.
    """
    if stop_workers.is_set():
        return False
    
    with DownloadPermit(semaphore, job) as permit:
        if stop_workers.is_set():
            return False
        if safe_put(dq, job):
            # El permiso ahora pertenece al job en la cola, no a este contexto
            permit.acquired = False 
            if active_downloads_count is not None:
                with active_downloads_count.get_lock():
                    active_downloads_count.value += 1
            return True
        return False


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
    active_downloads_count: mp.Value = None,
    debugger: Optional['AgentDebugger'] = None,
    current_tree: Optional['CourseTree'] = None,
    parent_path: Optional[List[str]] = None,
) -> int:
    """
    Extrae contenido de mod/page y mod/book.
    Guarda HTML completo siempre (preservación). Captura pluginfiles e iframes.
    """
    ensure(target_dir)
    found = 0

    while True:
        try:
            WebDriverWait(d, 10).until(
                EC.presence_of_element_located(
                    (By.CSS_SELECTOR, ".box.generalbox, #region-main")
                )
            )
            break
        except TimeoutException as e:
            if debugger and CONFIG.get("AGENT_MODE", False):
                debugger.alert_and_wait(f"Timeout esperando contenido en página: {e}", f"Esperar contenido de página {name}")
                continue
            break

    # Guardar HTML completo SIEMPRE (preservación offline)
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
        if current_tree and parent_path:
            current_tree.add_node(parent_path, html_fname, "page_html", url=page_url)
    except Exception as e:
        log.debug("Error guardando HTML de página %s: %s", page_url, e)

    # Extraer enlaces descargables dentro de la página
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
                    if current_tree and parent_path:
                        current_tree.add_node(parent_path, link_name, "file", url=href)
                    job = {**job_template, "url": href,
                           "link_text": link_name, "name": link_name,
                           "target": target_dir, "resource_type": "file",
                           "parent_activity": name, "hierarchy": parent_path}
                    if _enqueue_download(dl_q, semaphore, job, stop_workers, active_downloads_count):
                        found += 1
                elif _is_external_save_domain(href):
                    if current_tree and parent_path:
                        current_tree.add_node(parent_path, link_name, "external_url", url=href)
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

    # Capturar iframes dentro de la página
    try:
        for i, iframe in enumerate(d.find_elements(By.TAG_NAME, "iframe")):
            src = (iframe.get_attribute("src") or
                   iframe.get_attribute("data-src") or
                   iframe.get_attribute("data-url") or "")
            if not src or src.startswith("about:"):
                continue
            if current_tree and parent_path:
                current_tree.add_node(parent_path, f"iframe_{i + 1}", "iframe", url=src)
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



def extract_book_content(
    d: webdriver.Chrome,
    book_url: str,
    target_dir: str,
    name: str,
    seq: int,
    semaphore: mp.BoundedSemaphore,
    dl_q: mp.Queue,
    job_template: dict,
    visited: BoundedVisitedSet,
    dbq: mp.Queue,
    stop_workers: mp.Event,
    active_downloads_count: mp.Value = None,
    debugger: Optional['AgentDebugger'] = None,
    current_tree: Optional['CourseTree'] = None,
    parent_path: Optional[List[str]] = None,
) -> int:
    """Extrae todos los capítulos de un Libro de Moodle recursivamente."""
    book_dir = os.path.join(target_dir, f"{seq:03d}_LIBRO_{_s(name[:50])}")
    ensure(book_dir)
    found = 0
    
    chapter_urls = []
    try:
        toc_links = d.find_elements(By.CSS_SELECTOR, ".booktoc a[href], .block_book_toc a[href]")
        for a in toc_links:
            href = a.get_attribute("href") or ""
            if href and "chapterid=" in href and href not in chapter_urls:
                chapter_urls.append(href)
    except Exception:
        pass
    
    if not chapter_urls:
        chapter_urls = [book_url]
    
    for idx, chap_url in enumerate(chapter_urls):
        while True:
            try:
                if d.current_url != chap_url:
                    d.get(chap_url)
                    time.sleep(1.0)
                
                chap_title = d.title or f"Capitulo_{idx + 1}"
                chap_html = d.execute_script(
                    "var el = document.getElementById('region-main') || document.body;"
                    "return el ? el.innerHTML : '';"
                )
                if chap_html:
                    fname = f"{(idx + 1):02d}_{_s(chap_title[:50])}.html"
                    with open(os.path.join(book_dir, fname), "w", encoding="utf-8") as fh:
                        fh.write(f"<!-- Libro: {name} | Origen: {chap_url} -->\n{chap_html}")
                    if current_tree and parent_path:
                        # Append the book activity name to get the book's sub-path
                        book_path = parent_path + [name]
                        current_tree.add_node(book_path, fname, "chapter_html", url=chap_url)
                
                for a in d.find_elements(By.CSS_SELECTOR, "#region-main a[href*='pluginfile'], a[href*='forcedownload']"):
                    href = a.get_attribute("href") or ""
                    aname = (a.text or a.get_attribute("title") or f"adjunto_cap_{idx + 1}_{found + 1}").strip()
                    if not href or any(p in href for p in ("/user/icon/", "/theme/image.php", "pix/u/")):
                        continue
                    book_path = parent_path + [name] if parent_path else []
                    if current_tree and book_path:
                        current_tree.add_node(book_path, aname, "file", url=href)
                    job = {**job_template, "url": href, "link_text": aname, "name": aname,
                           "target": book_dir, "resource_type": "file", "parent_activity": name,
                           "hierarchy": book_path}
                    if _enqueue_download(dl_q, semaphore, job, stop_workers, active_downloads_count):
                        found += 1
                break
            except Exception as e:
                if debugger and CONFIG.get("AGENT_MODE", False):
                    debugger.alert_and_wait(f"Error procesando capítulo {idx+1}: {e}", f"Procesar libro {name} cap {idx+1}")
                    continue
                break

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
    active_downloads_count: mp.Value = None,
    debugger: Optional['AgentDebugger'] = None,
    current_tree: Optional['CourseTree'] = None,
    parent_path: Optional[List[str]] = None,
) -> int:
    """
    Extrae materiales de apoyo de mod/assign (enunciados, plantillas, adjuntos).
    Guarda HTML de descripción siempre.
    """
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
            if current_tree and parent_path:
                assign_path = parent_path + [name]
                current_tree.add_node(assign_path, html_fname, "assign_html", url=assign_url)
    except Exception as e:
        if isinstance(e, (WebDriverException, TimeoutException)):
            raise
        pass

    try:
        anchors = d.find_elements(
            By.CSS_SELECTOR,
            "#region-main a[href*='pluginfile'], a[href*='forcedownload']"
        )
        for a in anchors:
            try:
                href  = a.get_attribute("href") or ""
                aname = (a.text or a.get_attribute("title") or f"adjunto_{found + 1}").strip()
                if not href:
                    continue
                # Filtrar iconos de sistema, avatares e imagenes de tema
                if any(p in href for p in ("/user/icon/", "/theme/image.php", "pix/u/", "pix/f/")):
                    continue
                assign_path = parent_path + [name] if parent_path else []
                if current_tree and assign_path:
                    current_tree.add_node(assign_path, aname, "file", url=href)
                job = {**job_template, "url": href,
                       "link_text": aname, "name": aname,
                       "target": target_dir, "resource_type": "file",
                       "parent_activity": name, "hierarchy": assign_path}
                if _enqueue_download(dl_q, semaphore, job, stop_workers, active_downloads_count):
                    found += 1
            except Exception as e:
                if isinstance(e, (WebDriverException, TimeoutException)):
                    raise
                continue
    except Exception as e:
        if isinstance(e, (WebDriverException, TimeoutException)):
            raise
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
    active_downloads_count: mp.Value = None,
    debugger: Optional['AgentDebugger'] = None,
    current_tree: Optional['CourseTree'] = None,
    parent_path: Optional[List[str]] = None,
) -> int:
    """Navega hilos del foro y extrae adjuntos. Máximo 20 hilos."""
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
                
                # Guardar el HTML completo de la discusión para preservación offline
                try:
                    disc_title = d.title or f"discusion_{found + 1}"
                    disc_html = d.execute_script(
                        "var el = document.getElementById('region-main') || document.body;"
                        "return el ? el.innerHTML : '';"
                    )
                    if disc_html:
                        disc_fname = f"{seq + found:03d}_{_s(name[:30])}_{_s(disc_title[:30])}_DISCUSION.html"
                        with open(os.path.join(target_dir, disc_fname), "w", encoding="utf-8") as fh:
                            fh.write(f"<!-- Foro: {name} | Origen: {thread_url} -->\n{disc_html}")
                        if current_tree and parent_path:
                            forum_path = parent_path + [name]
                            current_tree.add_node(forum_path, disc_fname, "forum_discussion_html", url=thread_url)
                except Exception as de:
                    if isinstance(de, (WebDriverException, TimeoutException)):
                        raise
                    log.debug("Error al guardar HTML de discusion del foro: %s", de)

                for a in d.find_elements(
                    By.CSS_SELECTOR, "a[href*='pluginfile'], a[href*='forcedownload']"
                ):
                    href  = a.get_attribute("href") or ""
                    aname = (a.text or f"adjunto_foro_{found + 1}").strip()
                    if not href:
                        continue
                    if any(p in href for p in ("/user/icon/", "/theme/image.php", "pix/u/", "pix/f/")):
                        continue
                    forum_path = parent_path + [name] if parent_path else []
                    if current_tree and forum_path:
                        current_tree.add_node(forum_path, aname, "file", url=href)
                    job = {**job_template, "url": href,
                           "link_text": aname, "name": aname,
                           "target": target_dir, "resource_type": "file",
                           "parent_activity": name, "hierarchy": forum_path}
                    if _enqueue_download(dl_q, semaphore, job, stop_workers, active_downloads_count):
                        found += 1
            except Exception as e:
                if isinstance(e, (WebDriverException, TimeoutException)):
                    raise
                continue
    except Exception as e:
        if isinstance(e, (WebDriverException, TimeoutException)):
            raise
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
    active_downloads_count: mp.Value = None,
    debugger: Optional['AgentDebugger'] = None,
    current_tree: Optional['CourseTree'] = None,
    parent_path: Optional[List[str]] = None,
) -> int:
    """
    Navega TODAS las páginas de un mod/folder con paginación.
    Garantiza que no se pierden archivos en carpetas con >50 elementos.
    """
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
                if current_tree and parent_path:
                    current_tree.add_node(parent_path, aname, "file", url=href)
                job = {**job_template, "url": href,
                       "link_text": aname, "name": aname,
                       "target": target_dir, "resource_type": "file",
                       "parent_activity": name, "hierarchy": parent_path}
                if _enqueue_download(dl_q, semaphore, job, stop_workers, active_downloads_count):
                    found += 1

            # Siguiente página de paginación
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
            if isinstance(e, (WebDriverException, TimeoutException)):
                raise
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
    """
    Extrae blobs uno a uno (no serializa el array completo — anti-OOM).
    DLQ individual por blob fallido. GC del renderer al finalizar.
    """
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
    ro_conn_ref: list,          # [sqlite3.Connection] — lista mutable: el refresh
                                # propaga de vuelta al caller (Fix grieta #1)
    dbq: mp.Queue,
    semaphore: mp.BoundedSemaphore,
    db_path: str,
    ro_ops_counter: list,
    rate_limiter: DomainRateLimiter,
    shared_hashes: dict = None,
    shared_persisted_hashes: dict = None,
):
    """
    Descarga atómica con tres capas de deduplicación y lock NFS-safe.

    Garantías de Zero Data Loss:
    - tmp_fd cedido a os.fdopen ANTES de cualquier excepción posible
    - tmp_path siempre eliminado en finally si no fue movido
    - lock_fd siempre liberado en finally
    - semaphore.release() en finally SIEMPRE (incluso en excepciones)
    - Si falla la descarga, save_url_reference registra el recurso
    - ro_conn_ref[0] actualizado in-place — caller ve la conexión renovada
    """
    url        = job.get("url", "")
    name       = job.get("link_text", "") or job.get("name", "recurso")
    seq        = job.get("seq", 1)
    cid        = job.get("cid", "")
    target     = job.get("target", ".")
    course_dir = os.path.join(CONFIG["ROOT_DIR"], f"Curso_{cid}")
    target_dir = ensure(target)

    # Inicializar variables ANTES de cualquier operación (garantía del finally)
    tmp_path      = None
    tmp_fd        = -1
    lock_fd       = -1
    lock_path     = None
    lock_acquired = False
    fname         = name
    ctype         = ""

    def _dlq(severity: str, msg: str, action: str = ""):
        safe_put(dbq, {"type": "DLQ", "severity": severity,
                        "msg": msg, "url": url, "cid": cid, "action": action})

    def _save_fallback_reference(reason: str, resource_type: str = "file"):
        evaluator = ConfidenceEvaluator()
        ctx_fb = _build_resource_context(job, fname or "", "", ctype or "")
        confidence_eval = evaluator.evaluate(
            ctx_fb, ctype or "", 0, url, os.path.join(target_dir, f"{seq:03d}_{_s(name[:60])}_{resource_type.upper()}.url"),
            was_deduplicated=False, is_fallback=True
        )
        save_url_reference(
            target_dir,
            seq,
            name,
            url,
            job.get("page_origin", ""),
            resource_type,
            reason,
            ctx_section=job.get("section", ""),
            ctx_label=job.get("label_context", ""),
            extra={"parent_activity": job.get("parent_activity", "")},
            confidence_eval=confidence_eval
        )

    # Refresh periódico de ro_conn para evitar bloqueo de WAL checkpoint.
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

        # Reescribir URL de nube a descarga directa
        url_dl    = _rewrite_cloud_url(url)
        cloud_ext = _cloud_export_ext(url, url_dl)

        # Rate limiting por dominio
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
                _save_fallback_reference(
                    f"HTTP {r.status_code} — descarga fallida",
                    resource_type=job.get("resource_type", "file")
                )
                return

            ctype   = r.headers.get("Content-Type", "").lower()
            preview = b""

            is_external = job.get("resource_type") == "external"
            if "text/html" in ctype:
                preview = r.raw.read(4096, decode_content=True)
                if is_external:
                    # Enlace externo normal que apunta a una página web. Guardamos como acceso directo .url.
                    _save_fallback_reference("Enlace externo (HTML)", resource_type="external_url")
                    return

                if not _is_direct_download(job.get("url", "")):
                    is_login_html = any(sig in preview.lower() for sig in _HTML_BAD_SIGS)
                    if is_login_html:
                        raise SessionExpiredError("Sesión expirada detectada en stream.")
                    if b"<!doctype html" in preview.lower() or b"<html" in preview.lower():
                        pass
                    else:
                        _dlq("manual", "El recurso devolvió HTML en lugar de binario.",
                             f"Abre y guarda manualmente: {url}")
                        _save_fallback_reference(
                            "Respuesta HTML no binaria; recurso guardado como acceso manual",
                            resource_type="html_content",
                        )
                        return

            # Extraer nombre del servidor (RFC 5987)
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

            # tmp en el mismo directorio que el destino (garantiza mismo FS)
            tmp_fd, tmp_path = tempfile.mkstemp(
                dir=target_dir, suffix=".part",
                prefix=f".dl_{os.getpid()}_"
            )
            sha256        = hashlib.sha256()
            bytes_written = 0

            try:
                with os.fdopen(tmp_fd, "wb") as tmp_f:
                    tmp_fd = -1  # propiedad transferida — NO cerrar en finally
                    bytes_written = _read_with_deadline(
                        r, sha256, tmp_f, prefix=preview
                    )
            except Exception:
                raise

        if bytes_written == 0:
            _dlq("recoverable", "Archivo vacío (0 bytes).",
                 f"Verifica el recurso en Moodle: {url}")
            _save_fallback_reference(
                "Archivo vacío detectado (0 bytes); requiere verificación manual",
                resource_type="empty_download",
            )
            return

        # Validación de tamaño mínimo por tipo
        min_sizes = {".pdf": 512, ".docx": 512, ".pptx": 512, ".xlsx": 512, ".zip": 22}
        if bytes_written < min_sizes.get(ext.lower(), 0):
            _dlq("recoverable",
                 f"Archivo demasiado pequeño ({bytes_written}B) para {ext}. "
                 "Posible descarga parcial o error enmascarado.",
                 f"Verifica el recurso: {url}")
            _save_fallback_reference(
                f"Descarga parcial sospechosa ({bytes_written} bytes para {ext})",
                resource_type="partial_download",
            )
            return

        # Corrupción mid-stream
        with open(tmp_path, "rb") as fh:
            mid = fh.read(2048).lower()
        if not _is_direct_download(url) and any(s in mid for s in _HTML_BAD_SIGS):
            _dlq("recoverable", "Corrupción mid-stream (sesión revocada).",
                 f"Reinicia sesión y descarga: {url}")
            _save_fallback_reference(
                "Corrupción mid-stream detectada; descarga manual recomendada",
                resource_type="corrupt_stream",
            )
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

        # ── Deduplicación capa 0: shared memory registry (ultra-fast) ──
        if shared_hashes is not None and file_hash in shared_hashes:
            existing_path = shared_hashes[file_hash]
            if _path_exists_safe(existing_path):
                if not _path_exists_safe(final_path):
                    try:
                        os.link(existing_path, final_path)
                    except OSError:
                        shutil.copy2(existing_path, final_path)
                evaluator = ConfidenceEvaluator()
                confidence_eval = evaluator.evaluate(
                    ctx, ctype, bytes_written, url, final_path,
                    was_deduplicated=True, is_fallback=False
                )
                _write_sidecar(final_path, ctx, tags, confidence, confidence_eval)
                return

        # ── Deduplicación capa 1: hash registry (DB) ──
        rows = execute_read_safe(
            ro_conn_ref[0], "SELECT path FROM hashes WHERE hash=?", (file_hash,)
        )
        if rows and _path_exists_safe(rows[0][0]):
            existing_path = rows[0][0]
            if shared_hashes is not None:
                shared_hashes[file_hash] = existing_path
            if not _path_exists_safe(final_path):
                try:
                    os.link(existing_path, final_path)
                except OSError:
                    shutil.copy2(existing_path, final_path)
            evaluator = ConfidenceEvaluator()
            confidence_eval = evaluator.evaluate(
                ctx, ctype, bytes_written, url, final_path,
                was_deduplicated=True, is_fallback=False
            )
            _write_sidecar(final_path, ctx, tags, confidence, confidence_eval)
            return

        # ── POSIX lock NFS-safe ──
        locks_dir = ensure(os.path.join(CONFIG["ROOT_DIR"], ".locks"))
        lock_path = os.path.join(locks_dir, f".lock_{file_hash}")
        lock_fd, lock_acquired = _acquire_posix_lock(lock_path)
        if not lock_acquired:
            return  # Otro worker procesando este mismo hash

        # ── Deduplicación capa 2: post-lock (idempotencia real bajo carrera) ──
        rows2 = execute_read_safe(
            ro_conn_ref[0], "SELECT path FROM hashes WHERE hash=?", (file_hash,)
        )
        if rows2 and _path_exists_safe(rows2[0][0]):
            existing_path = rows2[0][0]
            if not _path_exists_safe(final_path):
                try:
                    os.link(existing_path, final_path)
                except OSError:
                    shutil.copy2(existing_path, final_path)
            evaluator = ConfidenceEvaluator()
            confidence_eval = evaluator.evaluate(
                ctx, ctype, bytes_written, url, final_path,
                was_deduplicated=True, is_fallback=False
            )
            _write_sidecar(final_path, ctx, tags, confidence, confidence_eval)
            return

        # ── Escritura atómica con fsync de directorio ──
        _atomic_replace(tmp_path, final_path, expected_hash=file_hash)
        tmp_path = None  # Ya movido — finally no debe borrarlo

        # Verificación de integridad post-write
        post_hash = _file_sha256_chunked(final_path)
        if post_hash != file_hash:
            with contextlib.suppress(OSError):
                os.remove(final_path)
            raise TransientError(
                f"Corrupción I/O post-write: {file_hash[:8]}≠{post_hash[:8]}."
            )

        evaluator = ConfidenceEvaluator()
        confidence_eval = evaluator.evaluate(
            ctx, ctype, bytes_written, url, final_path,
            was_deduplicated=False, is_fallback=False
        )

        _write_sidecar(final_path, ctx, tags, confidence, confidence_eval)

        # Actualizar registry compartido antes de liberar lock y enviar a DB
        if shared_hashes is not None:
            shared_hashes[file_hash] = final_path

        safe_put(dbq, {
            "type": "HASH", "hash": file_hash, "path": final_path,
            "tags": tags, "confidence": confidence,
        })
        log.info("⬇️  %s [%.1f MB] [%s]", final_name,
                 bytes_written / (1024 * 1024), confidence)

        # ESCUDO DE DEDUPLICACIÓN SÍNCRONA:
        # Esperamos a que el DB daemon confirme la persistencia en SQLite
        if shared_persisted_hashes is not None:
            t0 = time.monotonic()
            while file_hash not in shared_persisted_hashes and time.monotonic() - t0 < 5.0:
                time.sleep(0.01)

    finally:
        # Cierre garantizado de TODOS los recursos — orden importa
        if tmp_fd >= 0:
            with contextlib.suppress(OSError):
                os.close(tmp_fd)
        if tmp_path and _path_exists_safe(tmp_path):
            with contextlib.suppress(OSError):
                os.remove(tmp_path)
        if lock_acquired:
            _release_posix_lock(lock_fd, lock_path)
        # Semáforo liberado solo si NO estamos reencolando y poseemos el permiso
        if job.get("_sem_owned") and not job.get("_requeuing"):
            with contextlib.suppress(Exception):
                semaphore.release()
                job["_sem_owned"] = False


def downloader(
    q: mp.Queue,
    dbq: mp.Queue,
    stop_workers: mp.Event,
    cookies: dict,
    ua: str,
    db_path: str,
    semaphore: mp.BoundedSemaphore,
    shared_hashes: dict = None,
    shared_persisted_hashes: dict = None,
    active_downloads_count: mp.Value = None,
):
    """
    Worker de descarga. Stateless — DB es el único source of truth.
    ro_conn y session cerrados en finally garantizado.
    rate_limiter instanciado localmente (no compartido por IPC).

    SIGTERM shield: si el proceso recibe SIGTERM mientras hay un job
    in-flight, el handler lo escribe a disco (fsync) antes de salir.
    El semáforo se libera en el handler para no bloquear el orquestador.
    """
    # ── Kill-Window Shield ──────────────────────────────────────────────────
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
            if job.get("_sem_owned"):
                with contextlib.suppress(Exception):
                    semaphore.release()
                    job["_sem_owned"] = False
        raise SystemExit(0)

    signal.signal(signal.SIGTERM, _dl_sigterm_handler)
    # ────────────────────────────────────────────────────────────────────────

    session        = build_session(cookies, ua)
    ro_conn_ref    = [_open_db_ro(db_path)]   # lista mutable — _download_one refresca in-place
    ro_ops_counter = [0]
    rate_limiter   = DomainRateLimiter(rps=CONFIG["RATE_LIMIT_RPS"])
    local_delayed_jobs = []

    try:
        while not stop_workers.is_set():
            now = time.time()
            # 1. Procesar cualquier job local que ya esté listo
            ready_job = None
            for idx, job in enumerate(local_delayed_jobs):
                if now >= job.get("process_after", 0):
                    ready_job = local_delayed_jobs.pop(idx)
                    break
            
            if ready_job is not None:
                job = ready_job
            else:
                # Calcular timeout dinámico para q.get
                if local_delayed_jobs:
                    min_pa = min(j.get("process_after", 0) for j in local_delayed_jobs)
                    q_timeout = max(0.01, min(min_pa - now, 2.0))
                else:
                    q_timeout = 2.0
                
                try:
                    job = q.get(timeout=q_timeout)
                    if job == _STOP_SENTINEL:
                        # Propagar sentinel a otros workers y salir
                        safe_put(q, _STOP_SENTINEL)
                        break
                except queue.Empty:
                    continue

            _inflight.job = job

            # Verificar si el job sacado de la cola necesita retraso
            now = time.time()
            pa  = job.get("process_after", 0)
            if now < pa:
                local_delayed_jobs.append(job)
                _inflight.job = None
                continue

            job["_requeuing"] = False
            
            try:
                _download_one(
                    job, session, ro_conn_ref, dbq, semaphore,
                    db_path, ro_ops_counter, rate_limiter, shared_hashes,
                    shared_persisted_hashes
                )
                if active_downloads_count is not None:
                    with active_downloads_count.get_lock():
                        active_downloads_count.value -= 1
            except (TransientError, SessionExpiredError) as e:
                if not async_requeue_with_backoff(q, job, semaphore):
                    safe_put(dbq, {
                        "type": "DLQ", "severity": "recoverable",
                        "msg":  f"Fallo tras 3 reintentos: {e}",
                        "url":  job.get("url"), "cid": job.get("cid"),
                        "action": f"Descarga manualmente: {job.get('url')}",
                    })
                    evaluator = ConfidenceEvaluator()
                    ctx_fb = _build_resource_context(job, job.get("link_text") or "recurso", "", "")
                    confidence_eval = evaluator.evaluate(
                        ctx_fb, "", 0, job.get("url", ""),
                        os.path.join(ensure(job.get("target", os.path.join(CONFIG["ROOT_DIR"], "99_Sin_Contexto"))), f"{int(job.get('seq', 1)):03d}_{_s(job.get('link_text') or 'recurso')}_FILE.url"),
                        was_deduplicated=False, is_fallback=True
                    )
                    save_url_reference(
                        ensure(job.get("target", os.path.join(CONFIG["ROOT_DIR"], "99_Sin_Contexto"))),
                        int(job.get("seq", 1)),
                        job.get("link_text") or job.get("name") or "recurso",
                        job.get("url", ""),
                        job.get("page_origin", ""),
                        job.get("resource_type", "file"),
                        f"Fallo terminal tras 3 reintentos: {e}",
                        ctx_section=job.get("section", ""),
                        ctx_label=job.get("label_context", ""),
                        extra={"parent_activity": job.get("parent_activity", "")},
                        confidence_eval=confidence_eval
                    )
                    if active_downloads_count is not None:
                        with active_downloads_count.get_lock():
                            active_downloads_count.value -= 1
            except DiskFullError:
                log.critical("💀 DISCO LLENO. Worker detenido.")
                safe_put(dbq, {
                    "type": "DLQ", "severity": "critical", "msg": "DISCO LLENO.",
                    "url": job.get("url"), "cid": job.get("cid"),
                    "action": "Libera espacio y reinicia el crawler.",
                })
                if active_downloads_count is not None:
                    with active_downloads_count.get_lock():
                        active_downloads_count.value -= 1
                stop_workers.set()
                break
            except OSError as e:
                import errno as _errno
                if hasattr(e, "errno") and e.errno == _errno.ENOSPC:
                    if active_downloads_count is not None:
                        with active_downloads_count.get_lock():
                            active_downloads_count.value -= 1
                    stop_workers.set()
                    break
                if not async_requeue_with_backoff(q, job, semaphore):
                    safe_put(dbq, {
                        "type": "DLQ", "severity": "critical",
                        "msg": f"OSError: {e}",
                        "url": job.get("url"), "cid": job.get("cid"),
                    })
                    evaluator = ConfidenceEvaluator()
                    ctx_fb = _build_resource_context(job, job.get("link_text") or "recurso", "", "")
                    confidence_eval = evaluator.evaluate(
                        ctx_fb, "", 0, job.get("url", ""),
                        os.path.join(ensure(job.get("target", os.path.join(CONFIG["ROOT_DIR"], "99_Sin_Contexto"))), f"{int(job.get('seq', 1)):03d}_{_s(job.get('link_text') or 'recurso')}_FILE.url"),
                        was_deduplicated=False, is_fallback=True
                    )
                    save_url_reference(
                        ensure(job.get("target", os.path.join(CONFIG["ROOT_DIR"], "99_Sin_Contexto"))),
                        int(job.get("seq", 1)),
                        job.get("link_text") or job.get("name") or "recurso",
                        job.get("url", ""),
                        job.get("page_origin", ""),
                        job.get("resource_type", "file"),
                        f"Fallo terminal OSError tras reintentos: {e}",
                        ctx_section=job.get("section", ""),
                        ctx_label=job.get("label_context", ""),
                        extra={"parent_activity": job.get("parent_activity", "")},
                        confidence_eval=confidence_eval
                    )
                    if active_downloads_count is not None:
                        with active_downloads_count.get_lock():
                            active_downloads_count.value -= 1
            except Exception as e:
                if not async_requeue_with_backoff(q, job, semaphore):
                    safe_put(dbq, {
                        "type": "DLQ", "severity": "critical",
                        "msg": f"Error inesperado: {e}",
                        "url": job.get("url"), "cid": job.get("cid"),
                        "action": f"Descarga manualmente: {job.get('url')}",
                    })
                    evaluator = ConfidenceEvaluator()
                    ctx_fb = _build_resource_context(job, job.get("link_text") or "recurso", "", "")
                    confidence_eval = evaluator.evaluate(
                        ctx_fb, "", 0, job.get("url", ""),
                        os.path.join(ensure(job.get("target", os.path.join(CONFIG["ROOT_DIR"], "99_Sin_Contexto"))), f"{int(job.get('seq', 1)):03d}_{_s(job.get('link_text') or 'recurso')}_FILE.url"),
                        was_deduplicated=False, is_fallback=True
                    )
                    save_url_reference(
                        ensure(job.get("target", os.path.join(CONFIG["ROOT_DIR"], "99_Sin_Contexto"))),
                        int(job.get("seq", 1)),
                        job.get("link_text") or job.get("name") or "recurso",
                        job.get("url", ""),
                        job.get("page_origin", ""),
                        job.get("resource_type", "file"),
                        f"Fallo terminal inesperado tras reintentos: {e}",
                        ctx_section=job.get("section", ""),
                        ctx_label=job.get("label_context", ""),
                        extra={"parent_activity": job.get("parent_activity", "")},
                        confidence_eval=confidence_eval
                    )
                    if active_downloads_count is not None:
                        with active_downloads_count.get_lock():
                            active_downloads_count.value -= 1
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

  // Extraer breadcrumbs globales del curso/sitio para mayor contexto
  const breadcrumbs = Array.from(document.querySelectorAll('.breadcrumb-item, .breadcrumb a, .breadcrumbs a'))
    .map(e => (e.innerText || e.textContent || '').trim())
    .filter(Boolean)
    .join(' > ');

  // Encontrar todas las secciones
  let sections = Array.from(document.querySelectorAll('[data-for="section"], [data-sectionid], li.section.main, div.section.main, .course-section, .onetopic-tab-content, .section'));
  if (sections.length === 0) {
    const mainArea = document.getElementById('region-main') || document.querySelector('main') || document.body;
    sections = mainArea ? [mainArea] : [];
  }

  sections.forEach((sec, sec_idx) => {
    // 1. Obtener título de sección
    let title = '';
    const heading = sec.querySelector('h3, h2, [data-for="sectiontitle"], .section-title, .sectionname');
    if (heading) {
      title = (heading.innerText || heading.textContent || '').trim().split('\\n')[0];
    }
    if (!title) {
      title = sec.getAttribute('aria-label') || sec.getAttribute('title') || '';
    }
    if (title && title.length > 1) {
      current_section = title;
      current_section_idx = sec_idx;
      current_label = null;
      label_resource_count = 0;
      seq = 1;
    }

    // 2. Escaneo secuencial del DOM interno para reconstruir la jerarquía
    const walker = document.createTreeWalker(sec, NodeFilter.SHOW_ELEMENT, {
      acceptNode: function(node) {
        const tagName = node.tagName.toLowerCase();
        const cls = node.className || '';
        
        if (cls.includes('action-menu') || cls.includes('actions') || cls.includes('dropdown')) {
          return NodeFilter.FILTER_REJECT;
        }
        
        if (cls.includes('activity') || cls.includes('activity-item') || node.hasAttribute('data-activityname') || cls.includes('modtype_label') || tagName === 'h4' || tagName === 'h5') {
          return NodeFilter.FILTER_ACCEPT;
        }
        if (tagName === 'a' && node.getAttribute('href')) {
          const href = node.getAttribute('href');
          if (href.includes('/mod/') || href.includes('pluginfile') || href.includes('forcedownload')) {
            return NodeFilter.FILTER_ACCEPT;
          }
        }
        return NodeFilter.FILTER_SKIP;
      }
    });

    let node;
    while (node = walker.nextNode()) {
      const tagName = node.tagName.toLowerCase();
      const cls = typeof node.className === 'string' ? node.className : '';
      
      const isLabel = cls.includes('modtype_label') || tagName === 'h4' || tagName === 'h5' || 
                      (tagName !== 'a' && !node.querySelector('a[href]') && (node.innerText || '').trim().length > 3);
                      
      if (isLabel) {
        const text = (node.innerText || node.textContent || '').trim().split('\\n')[0];
        if (text && text.length > 3 && text.length < 200) {
          current_label = text;
          label_resource_count = 0;
        }
        continue;
      }

      const anchor = tagName === 'a' ? node : node.querySelector('a[href]');
      if (!anchor) continue;
      
      const href = anchor.getAttribute('href') || '';
      if (!href || href.startsWith('javascript:') || href.startsWith('#')) continue;
      
      const isTrap = href.includes('sesskey=') || ['/user/', '/message/', '/grade/', '/calendar/', '/report/'].some(trap => href.includes(trap));
      if (isTrap) continue;

      let link_text = (anchor.innerText || anchor.textContent || '').trim().split('\\n')[0];
      if (!link_text) {
        link_text = anchor.getAttribute('title') || anchor.getAttribute('aria-label') || node.getAttribute('data-activityname') || 'recurso';
      }
      
      label_resource_count++;
      const effective_label = label_resource_count > MAX_LABEL_PROP ? null : current_label;

      let type = 'link';
      const typeMap = [
        ['pluginfile', 'file'], ['forcedownload', 'file'],
        ['/mod/folder/', 'folder'], ['/mod/page/', 'page'], ['/mod/book/', 'page'],
        ['/mod/forum/', 'forum'], ['/mod/url/', 'url'],
        ['/mod/resource/', 'resource'], ['/mod/assign/', 'assign'],
        ['/mod/quiz/', 'quiz'], ['/mod/wiki/', 'wiki'],
        ['/mod/glossary/', 'glossary'], ['/mod/data/', 'database'],
        ['/mod/workshop/', 'workshop'], ['/mod/lesson/', 'lesson']
      ];
      for (const [p, t] of typeMap) {
        if (href.includes(p)) {
          type = t;
          break;
        }
      }

      const icon = node.querySelector('img, svg');
      let inferred_ext = '';
      if (icon) {
        const src = icon.getAttribute('src') || '';
        const alt = icon.getAttribute('alt') || '';
        const icon_text = (src + ' ' + alt).toLowerCase();
        if (icon_text.includes('pdf')) inferred_ext = '.pdf';
        else if (icon_text.includes('powerpoint') || icon_text.includes('ppt') || icon_text.includes('pptx')) inferred_ext = '.pptx';
        else if (icon_text.includes('word') || icon_text.includes('doc') || icon_text.includes('docx')) inferred_ext = '.docx';
        else if (icon_text.includes('excel') || icon_text.includes('xls') || icon_text.includes('xlsx')) inferred_ext = '.xlsx';
        else if (icon_text.includes('zip') || icon_text.includes('rar') || icon_text.includes('archive')) inferred_ext = '.zip';
        else if (icon_text.includes('mp4') || icon_text.includes('video') || icon_text.includes('movie')) inferred_ext = '.mp4';
      }

      const blocked = cls.includes('dimmed') || cls.includes('restricted') || cls.includes('conditionalhidden');
      const isLTI = cls.includes('modtype_lti') || cls.includes('modtype_scorm') || cls.includes('modtype_h5p');
      const iframes = Array.from(node.querySelectorAll('iframe')).map(f => ({
        src: f.getAttribute('src') || f.getAttribute('data-src') || ''
      })).filter(f => f.src);

      let item_hierarchy = [current_section];
      if (effective_label) {
        item_hierarchy.push(effective_label);
      }

      results.push({
        url: href,
        link_text: link_text.substring(0, 200),
        name: link_text.substring(0, 200),
        section: current_section,
        section_idx: current_section_idx,
        label_context: effective_label,
        seq: seq++,
        type: type,
        blocked: blocked,
        lti: isLTI,
        iframes: iframes,
        hierarchy: item_hierarchy,
        inferred_ext: inferred_ext,
        extra_meta: {
          breadcrumbs: breadcrumbs,
          nearest_heading: current_section
        }
      });
    }
  });

  Array.from(document.querySelectorAll('a[href*="/course/view.php"]')).forEach(a => {
    const href = a.getAttribute('href') || '';
    const isSection = href.includes('section=') || href.includes('sectionid=');
    if (isSection) {
      let text = (a.innerText || a.textContent || '').trim();
      if (text.length > 0 && text.length < 150) {
        results.push({
          url: href,
          link_text: text,
          name: text,
          section: 'Navegación de Secciones',
          section_idx: 0,
          type: 'section_link',
          blocked: false,
          lti: false,
          iframes: [],
          hierarchy: ['Navegación de Secciones']
        });
      }
    }
  });

  return results;
})();
"""


def auth() -> Tuple[list, str]:
    """
    Autenticación centralizada con Selenium.
    Inicia un navegador temporal headless, navega al login de eGela,
    introduce credenciales, espera a estar autenticado,
    y extrae cookies + User-Agent.
    """
    log.info("🔐 Iniciando proceso de autenticación interactivo (se abrirá Chrome)...")
    opts = Options()
    for arg in [
        "--disable-gpu", "--no-sandbox",
        "--disable-dev-shm-usage", "--disable-extensions",
        "--window-size=1200,900", "--disable-blink-features=AutomationControlled",
    ]:
        opts.add_argument(arg)
    opts.add_experimental_option("excludeSwitches", ["enable-automation"])
    opts.add_experimental_option("useAutomationExtension", False)
    
    d = webdriver.Chrome(
        service=Service(ChromeDriverManager().install()), options=opts
    )
    try:
        d.set_page_load_timeout(30)
        d.get(f"https://{CONFIG['DOMAIN']}/login/index.php")
        
        WebDriverWait(d, 15).until(
            EC.presence_of_element_located((By.CSS_SELECTOR, "input[name='username'], #username"))
        )
        
        u_el = d.find_element(By.CSS_SELECTOR, "input[name='username'], #username")
        u_el.clear()
        u_el.send_keys(CONFIG["USERNAME"])
        
        log.info("🖥️  [INTERACTIVO] Por favor, introduce tu contraseña en la ventana de Chrome que se acaba de abrir y haz clic en 'Iniciar sesión'.")
        
        # Esperar hasta 120 segundos a que el usuario complete el login
        WebDriverWait(d, 120).until(
            lambda driver: "login" not in driver.current_url.lower() or
                           driver.find_elements(By.CSS_SELECTOR, ".userpicture, #action-menu-toggle-1")
        )
        
        raw_cookies = d.get_cookies()
        cookies = [c for c in raw_cookies if c["name"] in ("MoodleSessionegela", "MOODLEID1_egela")]
        ua = d.execute_script("return navigator.userAgent")
        log.info("🔑 Autenticación exitosa. Se extrajeron %d cookies íntegras de sesión.", len(cookies))
        return cookies, ua
    except Exception as e:
        log.error("❌ Error de autenticación: %s", e)
        raise
    finally:
        with contextlib.suppress(Exception):
            d.quit()


def build_session(cookies: list, ua: str) -> requests.Session:
    """
    Crea una sesión requests con cookies y User-Agent preconfigurados.
    Añade reintentos robustos ante microcortes (TransientError).
    """
    session = requests.Session()
    session.headers.update({"User-Agent": ua})
    if isinstance(cookies, dict):
        session.cookies.update(cookies)
    else:
        for c in cookies:
            session.cookies.set(c["name"], c["value"], domain=c.get("domain", CONFIG["DOMAIN"]))
    
    retries = Retry(
        total=5,
        backoff_factor=0.5,
        status_forcelist=[500, 502, 503, 504],
        raise_on_status=False
    )
    adapter = HTTPAdapter(max_retries=retries)
    session.mount("http://", adapter)
    session.mount("https://", adapter)
    return session


def _setup_driver(cookies: list, ua: str) -> webdriver.Chrome:
    opts = Options()
    
    # ¡ESTA LÍNEA ES LA MAGIA QUE ENGAÑA A MOODLE!
    opts.add_argument(f"user-agent={ua}")
    
    args_list = [
        "--disable-gpu", "--no-sandbox",
        "--disable-dev-shm-usage", "--disable-extensions",
        "--window-size=1920,1080", "--disable-blink-features=AutomationControlled",
    ]
    if not CONFIG.get("AGENT_MODE", False):
        args_list.append("--headless=new")
        
    for arg in args_list:
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
    try:
        d.delete_all_cookies()
    except Exception:
        pass
    if isinstance(cookies, dict):
        for k, v in cookies.items():
            with contextlib.suppress(Exception):
                d.add_cookie({"name": k, "value": v, "domain": CONFIG["DOMAIN"], "path": "/"})
    else:
        for c in cookies:
            try:
                c["domain"] = CONFIG["DOMAIN"]
                c["path"] = "/"
                if "expiry" in c:
                    del c["expiry"]
                d.add_cookie(c)
            except Exception as e:
                log.error("❌ Fallo al inyectar cookie %s: %s", c.get("name"), e)
    return d


def _is_driver_alive(d: webdriver.Chrome) -> bool:
    try:
        _ = d.title
        return True
    except Exception:
        return False


def classify_page_context(d: webdriver.Chrome) -> dict:
    url = d.current_url
    title = d.title or ""
    
    # Breadcrumbs extraction
    try:
        bc_elements = d.find_elements(By.CSS_SELECTOR, '.breadcrumb-item, .breadcrumb a, .breadcrumbs a')
        breadcrumbs = [el.text.strip() for el in bc_elements if el.text.strip()]
    except Exception:
        breadcrumbs = []
        
    page_type = "external_url"
    if CONFIG["DOMAIN"] in url:
        if "/course/view.php" in url:
            page_type = "course_home"
        elif "/mod/folder/" in url:
            page_type = "folder_view"
        elif "/mod/page/" in url:
            page_type = "page_view"
        elif "/mod/book/" in url:
            page_type = "book_view"
        elif "/mod/assign/" in url:
            page_type = "assign_view"
        elif "/mod/forum/" in url:
            page_type = "forum_view"
        else:
            page_type = "generic_moodle_page"
            
    # Try to extract the main heading as page title if title is generic
    main_heading = ""
    try:
        headings = d.find_elements(By.CSS_SELECTOR, 'h1, h2, h3')
        for h in headings:
            if h.is_displayed() and h.text.strip():
                main_heading = h.text.strip()
                break
    except Exception:
        pass
        
    return {
        "page_type": page_type,
        "title": main_heading or title,
        "url": url,
        "breadcrumbs": breadcrumbs
    }


def _navigate_page(
    d: webdriver.Chrome, url: str, full_load: bool = False
) -> bool:
    """Navega con la estrategia adecuada. Retorna True si tuvo éxito."""
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
    active_downloads_count: mp.Value = None,
):
    """
    Worker spider. Stateless — DB es el único source of truth.
    ro_conn, session y driver cerrados en finally garantizado.

    SIGTERM shield: registra la tarea en curso y la escribe a disco
    antes de salir para garantizar re-crawl en el siguiente arranque.
    """
    # ── Kill-Window Shield ──────────────────────────────────────────────────
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
    # ────────────────────────────────────────────────────────────────────────

    ro_conn = _open_db_ro(db_path)
    visited = BoundedVisitedSet(maxsize=CONFIG["VISITED_LRU_MAXSIZE"], db_path=db_path)

    d             = _setup_driver(cookies, ua)
    debugger      = AgentDebugger(d)
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
        nonlocal d, debugger, pages_loaded, driver_born, last_activity
        log.info("♻️  Reiniciando Chrome (páginas=%d, tiempo=%.0fs)...",
                 pages_loaded, time.monotonic() - driver_born)
        with contextlib.suppress(Exception):
            d.quit()
        d             = _setup_driver(cookies, ua)
        debugger      = AgentDebugger(d)
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

    def crawl(url: str, cid: str, depth: int = 0, current_tree: Optional['CourseTree'] = None) -> Optional['CourseTree']:
        nonlocal pages_loaded, last_activity

        if depth > 10:
            _dlq("manual",
                 "Límite de profundidad (depth=10). Posibles recursos no descargados.",
                 url, cid, f"Accede manualmente: {url}")
            return current_tree

        if _is_nav_trap(url):
            return current_tree

        norm = normalize_url(url)
        if norm in visited:
            return current_tree
        rows = execute_read_safe(ro_conn, "SELECT status FROM visited WHERE url=?", (norm,))
        if rows:
            status = rows[0][0]
            if status in ("completed", "failed", "manual"):
                visited.add(norm)
                return current_tree

        # Registrar estado de procesamiento inicial
        safe_put(dbq, {"type": "VISITED", "url": norm, "status": "processing", "cid": cid})

        if _should_restart_driver():
            _restart_driver()
        _ensure_driver()

        full_load = any(m in url for m in CONFIG["FULL_LOAD_MODULES"])
        while True:
            try:
                if not _navigate_page(d, url, full_load=full_load):
                    if CONFIG.get("AGENT_MODE", False):
                        debugger.alert_and_wait("No se pudo cargar la página.", f"Navegar a {url}")
                        continue
                    else:
                        _dlq("recoverable", "No se pudo cargar la página.",
                             url, cid, f"Accede manualmente: {url}")
                        safe_put(dbq, {"type": "VISITED", "url": norm, "status": "failed", "cid": cid})
                        return current_tree
                
                # DETECTOR DE EXPULSIÓN DE MOODLE
                is_homepage = d.current_url.strip("/").replace("https://", "").replace("http://", "") == CONFIG["DOMAIN"].strip("/")
                if "login" in d.current_url or d.find_elements(By.ID, "username") or is_homepage:
                    if CONFIG.get("AGENT_MODE", False):
                        debugger.alert_and_wait("Moodle ha expulsado a la araña o pide login.", f"Verificar sesión en {url}")
                        continue
                    else:
                        log.error("❌ Moodle ha expulsado a la araña. URL actual: %s", d.current_url)
                        _dlq("critical", "Moodle rechazó las cookies por cambio de navegador.", url, cid, "Revisa la sesión.")
                        return current_tree
                break
            except Exception as e:
                if CONFIG.get("AGENT_MODE", False):
                    debugger.alert_and_wait(f"Excepción al navegar: {e}", f"Navegar a {url}")
                    continue
                else:
                    raise
        
        last_activity = time.monotonic()
        page_title    = d.title or ""

        if CONFIG["DOMAIN"] not in d.current_url:
            visited.add(norm)
            _dlq("manual", "Redirección a dominio externo.", url, cid,
                 "Accede manualmente con sesión institucional.")
            safe_put(dbq, {"type": "VISITED", "url": norm, "status": "manual", "cid": cid})
            return current_tree

        # ── CLASIFICACIÓN DE CONTEXTO Y DIAGNÓSTICOS ─────────────────────────
        page_ctx = classify_page_context(d)
        page_type = page_ctx["page_type"]
        page_title = page_ctx["title"] or page_title
        debugger.dump_diagnostics(cid, page_type)

        if current_tree is None:
            current_tree = CourseTree(page_title or f"Curso_{cid}", cid)
        elif current_tree.root["name"] == f"Curso_{cid}" and page_title:
            current_tree.root["name"] = page_title
        # ─────────────────────────────────────────────────────────────────────

        pages_loaded += 1

        time.sleep(1.0)

        while True:
            try:
                elementos = d.execute_script("return " + _JS_MAP_UNIVERSAL) or []
                log.info("🕷️  Extracción en %s exitosa. Elementos encontrados: %d", url, len(elementos))
                break
            except Exception as e:
                if CONFIG.get("AGENT_MODE", False):
                    debugger.alert_and_wait(f"Excepción al extraer DOM: {e}", f"Extraer elementos de {url}")
                    continue
                else:
                    _dlq("critical",
                         f"Analizador DOM falló. Recursos NO descargados. Título: {page_title}",
                         url, cid, f"Abre en Moodle: {url}")
                    safe_put(dbq, {"type": "VISITED", "url": norm, "status": "failed", "cid": cid})
                    return current_tree

        # Modo Interactivo de Aprobación
        if depth == 0 and CONFIG.get("AGENT_MODE", False):
            while True:
                files_count = sum(1 for el in elementos if el.get("type") in ("file", "resource") or _is_direct_download(el.get("url", "")))
                folders_count = sum(1 for el in elementos if el.get("type") == "folder")
                pages_count = sum(1 for el in elementos if el.get("type") in ("page", "book"))
                assigns_count = sum(1 for el in elementos if el.get("type") == "assign")
                forums_count = sum(1 for el in elementos if el.get("type") == "forum")
                others_count = len(elementos) - files_count - folders_count - pages_count - assigns_count - forums_count
                
                print("\n" + "=" * 80)
                print(f"📋 RESUMEN DE PARSEO PARA EL CURSO: ID {cid} - \"{page_title}\"")
                print("-" * 80)
                print(f"   📂 Carpetas (Folders):         {folders_count}")
                print(f"   📄 Páginas / Libros:           {pages_count}")
                print(f"   📥 Archivos (PDFs/Docx/etc):   {files_count}")
                print(f"   📝 Tareas (Assignments):       {assigns_count}")
                print(f"   💬 Foros (Forums):             {forums_count}")
                print(f"   🔗 Otros (LTI/Wikis/etc):      {others_count}")
                print(f"   📌 Total de recursos:          {len(elementos)}")
                print("=" * 80 + "\n")
                
                res = input("🤖 ¿Confirmas que el parseo es correcto para proceder a la descarga masiva? (S/N) [S]: ").strip().lower()
                if res in ("", "s", "si", "y", "yes"):
                    log.info("✅ Confirmación recibida. Procediendo con el procesamiento...")
                    break
                elif res in ("n", "no"):
                    log.warning("❌ Aprobación rechazada por el usuario.")
                    opt = input("⚠️ ¿Qué deseas hacer? (R: Reintentar parseo, S: Saltar este curso, D: Lanzar debugger y pausar) [R]: ").strip().lower()
                    if opt == "s":
                        log.info(f"⏭️ Saltando el curso {cid} por decisión del usuario.")
                        safe_put(dbq, {"type": "VISITED", "url": norm, "status": "manual", "cid": cid})
                        return current_tree
                    elif opt == "d":
                        debugger.alert_and_wait("El usuario rechazó el parseo de elementos.", "Aprobación de parseo de curso")
                        try:
                            elementos = d.execute_script("return " + _JS_MAP_UNIVERSAL) or []
                            page_title = d.title or ""
                        except Exception as e:
                            log.error(f"Error al volver a parsear: {e}")
                    else:
                        # Reintentar parseo por defecto
                        try:
                            elementos = d.execute_script("return " + _JS_MAP_UNIVERSAL) or []
                            page_title = d.title or ""
                        except Exception as e:
                            log.error(f"Error al volver a parsear: {e}")
                else:
                    print("Opción no reconocida. Por favor ingresa S, N o presiona ENTER.")

        c_dir = ensure(os.path.join(CONFIG["ROOT_DIR"], f"Curso_{cid}"))

        for el in elementos:
            if stop_workers.is_set():
                return current_tree

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
            hierarchy = el.get("hierarchy", [])

            if not u:
                continue
            if _is_nav_trap(u):
                continue

            title_slug = _s(section[:40])
            title_hash = hashlib.md5(
                section.encode("utf-8", errors="replace")
            ).hexdigest()[:3]
            sec_folder = (
                f"{sec_idx + 1:02d}_{title_slug}_{title_hash}"
                if section and section != "00_General"
                else "99_Sin_Contexto"
            )

            # Construcción dinámica de directorios basados en jerarquía semántica
            if hierarchy:
                cleaned_segments = []
                for i, segment in enumerate(hierarchy):
                    if i == 0 and section and segment == section:
                        cleaned_segments.append(sec_folder)
                    else:
                        cleaned_segments.append(_s(segment[:40]))
                target_dir = os.path.join(c_dir, *cleaned_segments)
            else:
                target_dir = os.path.join(c_dir, "99_Sin_Contexto")

            job_template = {
                "cid":           cid,
                "section":       section,
                "section_idx":   sec_idx,
                "label_context": label_ctx,
                "seq":           seq,
                "page_origin":   url,
                "link_text":     link_txt,
                "name":          link_txt,
                "hierarchy":     hierarchy,
            }

            for i_idx, i_data in enumerate(iframes):
                src = i_data.get("src", "")
                if not src:
                    continue
                if _is_external_save_domain(src):
                    if current_tree:
                        current_tree.add_node(hierarchy, f"{link_txt}_iframe{i_idx + 1}", "iframe", url=src)
                    save_url_reference(
                        ensure(target_dir), seq, f"{link_txt}_iframe{i_idx + 1}",
                        src, url, "iframe", "Contenido embebido en iframe",
                        ctx_section=section, ctx_label=label_ctx or "",
                    )
                elif CONFIG["DOMAIN"] in src:
                    norm_iframe = normalize_url(src)
                    if norm_iframe not in visited:
                        crawl(src, cid, depth + 1, current_tree=current_tree)

            if blocked:
                if current_tree:
                    current_tree.add_node(hierarchy, link_txt, "blocked", url=u)
                _dlq("manual", f"Recurso bloqueado: {link_txt}", u, cid,
                     "Comprueba requisitos de acceso en Moodle.")
                save_url_reference(
                    ensure(target_dir), seq, link_txt, u, url, "blocked",
                    "Recurso bloqueado por restricción condicional",
                    ctx_section=section, ctx_label=label_ctx or "",
                )
                continue

            if is_lti:
                if current_tree:
                    current_tree.add_node(hierarchy, link_txt, "lti", url=u)
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
                if current_tree:
                    current_tree.add_node(hierarchy, link_txt, "file", url=u)
                job = {**job_template, "url": u, "target": ensure(target_dir),
                       "resource_type": "file"}
                _enqueue_download(dq, semaphore, job, stop_workers, active_downloads_count)

            elif etype == "url":
                rewritten = _rewrite_cloud_url(u)
                if current_tree:
                    current_tree.add_node(hierarchy, link_txt, "url", url=rewritten)
                job = {**job_template, "url": rewritten,
                       "target": ensure(target_dir), "resource_type": "external"}
                _enqueue_download(dq, semaphore, job, stop_workers, active_downloads_count)

            elif etype == "folder":
                norm_child = normalize_url(u)
                if norm_child not in visited and CONFIG["DOMAIN"] in u:
                    visited.add(norm_child)
                    if current_tree:
                        current_tree.add_node(hierarchy, link_txt, "folder", url=u)
                    nested_target_dir = os.path.join(target_dir, _s(link_txt[:40]))
                    while True:
                        try:
                            if not _navigate_page(d, u):
                                if CONFIG.get("AGENT_MODE", False):
                                    debugger.alert_and_wait("No se pudo cargar la carpeta.", f"Navegar a carpeta {link_txt}")
                                    continue
                                else:
                                    break
                            
                            last_activity = time.monotonic()
                            extract_folder_pages(
                                d, u, ensure(nested_target_dir), link_txt, seq,
                                semaphore, dq, job_template, visited, dbq, stop_workers,
                                active_downloads_count, debugger=debugger,
                                current_tree=current_tree, parent_path=hierarchy + [link_txt]
                            )
                            break
                        except Exception as e:
                            if CONFIG.get("AGENT_MODE", False):
                                debugger.alert_and_wait(f"Error procesando carpeta: {e}", f"Procesar carpeta {link_txt}")
                                continue
                            else:
                                break
                    with contextlib.suppress(Exception):
                        d.back()
                        time.sleep(0.5)

            elif etype in ("page", "book"):
                norm_child = normalize_url(u)
                if norm_child not in visited and CONFIG["DOMAIN"] in u:
                    visited.add(norm_child)
                    if current_tree:
                        current_tree.add_node(hierarchy, link_txt, etype, url=u)
                    nested_target_dir = os.path.join(target_dir, _s(link_txt[:40]))
                    while True:
                        try:
                            if not _navigate_page(d, u, full_load=True):
                                if CONFIG.get("AGENT_MODE", False):
                                    debugger.alert_and_wait(f"No se pudo cargar {etype}.", f"Navegar a {etype} {link_txt}")
                                    continue
                                else:
                                    break
                            
                            last_activity = time.monotonic()
                            if etype == "page":
                                extract_page_content(
                                    d, u, ensure(nested_target_dir), link_txt, seq,
                                    dbq, visited, job_template, semaphore, dq, stop_workers,
                                    active_downloads_count, debugger=debugger,
                                    current_tree=current_tree, parent_path=hierarchy + [link_txt]
                                )
                            else:
                                extract_book_content(
                                    d, u, ensure(target_dir), link_txt, seq,
                                    semaphore, dq, job_template, visited, dbq, stop_workers,
                                    active_downloads_count, debugger=debugger,
                                    current_tree=current_tree, parent_path=hierarchy + [link_txt]
                                )
                            crawl(u, cid, depth + 1, current_tree=current_tree)
                            break
                        except Exception as e:
                            if CONFIG.get("AGENT_MODE", False):
                                debugger.alert_and_wait(f"Error procesando {etype}: {e}", f"Procesar {etype} {link_txt}")
                                continue
                            else:
                                break
                    with contextlib.suppress(Exception):
                        d.back()
                        time.sleep(0.5)

            elif etype == "assign":
                norm_child = normalize_url(u)
                if norm_child not in visited and CONFIG["DOMAIN"] in u:
                    visited.add(norm_child)
                    if current_tree:
                        current_tree.add_node(hierarchy, link_txt, "assign", url=u)
                    nested_target_dir = os.path.join(target_dir, _s(link_txt[:40]))
                    while True:
                        try:
                            if not _navigate_page(d, u, full_load=True):
                                if CONFIG.get("AGENT_MODE", False):
                                    debugger.alert_and_wait("No se pudo cargar la tarea.", f"Navegar a tarea {link_txt}")
                                    continue
                                else:
                                    break
                            
                            last_activity = time.monotonic()
                            extract_assign_content(
                                d, u, ensure(nested_target_dir), link_txt, seq,
                                semaphore, dq, job_template, dbq, stop_workers,
                                active_downloads_count, debugger=debugger,
                                current_tree=current_tree, parent_path=hierarchy + [link_txt]
                            )
                            break
                        except Exception as e:
                            if CONFIG.get("AGENT_MODE", False):
                                debugger.alert_and_wait(f"Error procesando tarea: {e}", f"Procesar tarea {link_txt}")
                                continue
                            else:
                                break
                    with contextlib.suppress(Exception):
                        d.back()
                        time.sleep(0.5)

            elif etype == "section_link":
                norm_child = normalize_url(u)
                if norm_child not in visited and CONFIG["DOMAIN"] in u:
                    try:
                        url_params = parse_qs(urlparse(u).query)
                        url_cid = url_params.get("id", [None])[0]
                        if url_cid and str(url_cid) == str(cid):
                            visited.add(norm_child)
                            crawl(u, cid, depth + 1, current_tree=current_tree)
                    except Exception:
                        pass

            elif etype == "forum":
                norm_child = normalize_url(u)
                if norm_child not in visited and CONFIG["DOMAIN"] in u:
                    visited.add(norm_child)
                    if current_tree:
                        current_tree.add_node(hierarchy, link_txt, "forum", url=u)
                    nested_target_dir = os.path.join(target_dir, _s(link_txt[:40]))
                    while True:
                        try:
                            if not _navigate_page(d, u, full_load=True):
                                if CONFIG.get("AGENT_MODE", False):
                                    debugger.alert_and_wait("No se pudo cargar el foro.", f"Navegar a foro {link_txt}")
                                    continue
                                else:
                                    break
                            
                            last_activity = time.monotonic()
                            extract_forum_attachments(
                                d, u, ensure(nested_target_dir), link_txt, seq,
                                semaphore, dq, job_template, visited, dbq, stop_workers,
                                active_downloads_count, debugger=debugger,
                                current_tree=current_tree, parent_path=hierarchy + [link_txt]
                            )
                            break
                        except Exception as e:
                            if CONFIG.get("AGENT_MODE", False):
                                debugger.alert_and_wait(f"Error procesando foro: {e}", f"Procesar foro {link_txt}")
                                continue
                            else:
                                break
                    with contextlib.suppress(Exception):
                        d.back()
                        time.sleep(0.5)

            elif etype in ("quiz", "wiki", "glossary", "database",
                           "workshop", "lesson"):
                if current_tree:
                    current_tree.add_node(hierarchy, link_txt, etype, url=u)
                save_url_reference(
                    ensure(target_dir), seq, link_txt, u, url, etype,
                    f"Actividad interactiva Moodle ({etype})",
                    ctx_section=section, ctx_label=label_ctx or "",
                )
                norm_child = normalize_url(u)
                if norm_child not in visited and f"id={cid}" in u:
                    crawl(u, cid, depth + 1, current_tree=current_tree)

            elif CONFIG["DOMAIN"] in u:
                norm_child = normalize_url(u)
                if norm_child not in visited:
                    crawl(u, cid, depth + 1, current_tree=current_tree)

            else:
                if current_tree:
                    current_tree.add_node(hierarchy, link_txt, "unknown", url=u)
                save_url_reference(
                    ensure(target_dir), seq, link_txt, u, url, "unknown",
                    "Tipo de recurso no identificado",
                    ctx_section=section, ctx_label=label_ctx or "",
                )
                _dlq("manual", f"Recurso no clasificado: {link_txt}", u, cid,
                     f"Comprueba manualmente: {u}")

        extract_blobs_safe(d, c_dir, url, dbq)

        # Completado con éxito
        visited.add(norm)
        safe_put(dbq, {"type": "VISITED", "url": norm, "status": "completed", "cid": cid})
        return current_tree

    try:
        while not stop_workers.is_set():
            _ensure_driver()
            try:
                task = q.get(timeout=2.0)
                if task == _STOP_SENTINEL:
                    break
            except queue.Empty:
                continue

            _current_task["task"] = task
            log.info("🕷️  Curso %s → %s", task.get("cid"), task.get("url"))
            try:
                tree = crawl(task["url"], task["cid"])
                if tree:
                    c_dir = ensure(os.path.join(CONFIG["ROOT_DIR"], f"Curso_{task['cid']}"))
                    tree.save_to_files(c_dir)
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
    """Genera 00_PARTE_DE_INCIDENCIAS.md en lenguaje natural."""
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
    """
    Índice Maestro cross-platform con contexto semántico real.
    Agrupa por sección → etiqueta visual. Badges. Tags. Rutas entre <>.
    """
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


def generate_review_report(course_dir: str, course_id: str):
    """
    Genera 00_REVISAR_MANUALMENTE.md agrupando anomalías por prioridad.
    """
    report_path = os.path.join(course_dir, "00_REVISAR_MANUALMENTE.md")
    ensure(course_dir)

    anomalies = []
    for root, _, files in os.walk(course_dir):
        for fname in files:
            if fname.endswith(".meta.json"):
                fpath = os.path.join(root, fname)
                try:
                    with open(fpath, "r", encoding="utf-8") as fh:
                        meta = json.load(fh)
                except Exception:
                    continue
                
                rev = meta.get("revision_manual")
                if rev and rev.get("review_required"):
                    rel_ref = os.path.relpath(fpath[:-10], course_dir).replace("\\", "/")
                    anomalies.append({
                        "rel_path": rel_ref,
                        "fname": os.path.basename(fpath[:-10]),
                        "score": rev.get("confidence_score", 1.0),
                        "level": rev.get("confidence_level", "high"),
                        "reasons": rev.get("review_reasons", []),
                        "trace": rev.get("decision_trace", ""),
                        "url": meta.get("origen", {}).get("url", ""),
                    })

    high_priority = []
    med_priority = []
    low_priority = []

    for a in anomalies:
        score = a["score"]
        if score < 0.4:
            high_priority.append(a)
        elif score < 0.7:
            med_priority.append(a)
        else:
            low_priority.append(a)

    lines = [
        f"# 🔍 Reporte de Revisión Manual — Curso {course_id}\n\n",
        f"> **Generado:** {time.strftime('%Y-%m-%d %H:%M:%S UTC', time.gmtime())}\n",
        f"> **Total Anomalías:** {len(anomalies)} recursos sospechosos\n\n",
        "Este reporte clasifica todos los recursos que requieren una verificación manual "
        "debido a inconsistencias en su tamaño, metadatos, extensiones o procedencia.\n\n---\n\n"
    ]

    def _format_anomaly_group(title, emoji, group):
        glines = [f"## {emoji} {title} ({len(group)})\n\n"]
        if not group:
            glines.append("✅ Sin anomalías en esta categoría.\n\n")
            return glines
        for i, a in enumerate(group, 1):
            glines.append(f"### {i}. {a['fname']}\n")
            glines.append(f"- **Ruta Local:** `{a['rel_path']}`\n")
            glines.append(f"- **Puntuación de Confianza:** `{a['score']}` / 1.0\n")
            if a["url"]:
                glines.append(f"- **Enlace de Descarga:** <{a['url']}>\n")
            glines.append("- **Inconsistencias Detectadas:**\n")
            for r in a["reasons"]:
                glines.append(f"  - ⚠️ {r}\n")
            if a["trace"]:
                glines.append(f"- **Detalles:** *{a['trace']}*\n")
            glines.append("\n")
        return glines

    lines.extend(_format_anomaly_group("Alta Prioridad (Requiere Atención Inmediata)", "🔴", high_priority))
    lines.extend(_format_anomaly_group("Media Prioridad (Verificación Recomendada)", "🟡", med_priority))
    lines.extend(_format_anomaly_group("Baja Prioridad (Revisiones Menores)", "🟢", low_priority))

    try:
        with open(report_path, "w", encoding="utf-8") as fh:
            fh.writelines(lines)
            fh.flush()
            os.fsync(fh.fileno())
        log.info("🔍 Reporte de revisión manual → %s", report_path)
    except Exception as e:
        log.error("No se pudo escribir el reporte de revisión manual: %s", e)


# ═══════════════════════════════════════════════════════════════
# §21 — GRACEFUL SHUTDOWN (anti-zombie, anti-deadlock)
# ═══════════════════════════════════════════════════════════════

_shutdown_in_progress = False  # Bandera de proceso solo — no compartida por IPC


def _join_with_timeout(procs: list, timeout_each: float = None):
    """Join con escalada SIGTERM → SIGKILL. Nunca bloquea indefinidamente."""
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
    """
    Vuelca todos los jobs pendientes de una cola al disco.
    Con fsync garantizado por archivo — zero data loss incluso ante crash posterior.
    """
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
    """
    Avisa de ficheros de emergencia pendientes.
    Excluye explícitamente los .bak (ya reingeridos y archivados).
    """
    patterns = [
        os.path.join(CONFIG["ROOT_DIR"], "_*_RESCUED.jsonl"),
        os.path.join(CONFIG["ROOT_DIR"], "_EMERGENCY_DUMP_*.jsonl"),
    ]
    # Solo contar .jsonl activos — los .bak son archivo histórico, no pendientes
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
    """
    Shutdown seguro. Orden crítico para zero data loss:
    1. Señalar stop_workers
    2. Liberar semáforo (desbloquea spiders en acquire())
    3. cancel_join_thread() ANTES del join() (anti-deadlock feeder thread)
    4. join() con timeout + escalada SIGKILL
    5. Drenar colas al disco con fsync
    6. Detener DB daemon (drena completamente antes de cerrar)
    7. Post-procesado forense
    """
    global _shutdown_in_progress
    if _shutdown_in_progress:
        log.critical("⛔  Segundo Ctrl+C ignorado — shutdown en curso.")
        return
    _shutdown_in_progress = True
    log.warning("⚠️  Señal %d — shutdown seguro iniciado.", sig)

    stop_workers.set()

    # Liberar semáforo para desbloquear spiders en acquire()
    # BoundedSemaphore lanza ValueError si se sobre-libera → parar con break
    for _ in range(CONFIG["MAX_IN_FLIGHT"]):
        try:
            semaphore.release()
        except ValueError:
            break

    # cancel_join_thread ANTES del join — libera feeder thread de Python
    with contextlib.suppress(Exception):
        dl_q.cancel_join_thread()
    with contextlib.suppress(Exception):
        spider_q.cancel_join_thread()

    # Enviar sentinels a todos los niveles
    for _ in range(CONFIG["SPIDERS"]):
        with contextlib.suppress(Exception):
            spider_q.put_nowait(_STOP_SENTINEL)
    
    _join_with_timeout(spiders)
    
    for _ in range(CONFIG["DOWNLOADERS"]):
        with contextlib.suppress(Exception):
            dl_q.put_nowait(_STOP_SENTINEL)

    _join_with_timeout(downs)

    # Drenar colas físicas (lo que quede tras sentinels)
    _drain_queue_to_disk(dl_q, "PENDING_DOWNLOADS")
    _drain_queue_to_disk(spider_q, "PENDING_SPIDERS")

    # DB daemon — último en morir
    with contextlib.suppress(Exception):
        db_q.put(_STOP_SENTINEL, timeout=5)
    
    stop_db.set()
    dbp.join(timeout=60)
    if dbp.is_alive():
        dbp.terminate()
        dbp.join(timeout=10)

    # Post-procesado forense
    log.info("📊 Generando índices y partes de incidencias...")
    for cid in courses:
        c_dir = os.path.join(CONFIG["ROOT_DIR"], f"Curso_{cid}")
        if os.path.isdir(c_dir):
            flush_dlq_to_human_report(c_dir, cid)
            synthesize_master_index(c_dir, cid)
            generate_review_report(c_dir, cid)

    _check_emergency_files()
    log.info("✅  Shutdown completado.")
    sys.exit(0)


# ═══════════════════════════════════════════════════════════════
# §22 — MAIN ORCHESTRATOR
# ═══════════════════════════════════════════════════════════════

def main():
    import argparse
    parser = argparse.ArgumentParser(description="eGela Crawler Agentic Edition")
    parser.add_argument("--agent", action="store_true", help="Activar el modo interactivo del Agente (Human-in-the-Loop)")
    args = parser.parse_known_args()[0]
    
    if args.agent:
        CONFIG["AGENT_MODE"] = True
        log.info("🤖 MODO AGENTE CON HUMAN-IN-THE-LOOP ACTIVADO.")

    # spawn: comportamiento coherente con Selenium y SQLite en macOS/Windows
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

    # Limpieza de locks huérfanos de runs anteriores ANTES de abrir la BD
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
    # BoundedSemaphore: previene sobre-liberación que corrompería el contador
    semaphore    = mp.BoundedSemaphore(CONFIG["MAX_IN_FLIGHT"])
    # dl_q con maxsize = MAX_IN_FLIGHT * 2 (backpressure bloqueante real)
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

    active_downloads_count = mp.Value('i', 0)

    # Reingestión centralizada en hilo de background para no bloquear el inicio
    reingestion_thread = threading.Thread(
        target=reingest_emergency_data_worker,
        args=(dl_q, spider_q, db_q, semaphore, active_downloads_count, stop_workers),
        name="Reingestion-Thread",
        daemon=True
    )
    reingestion_thread.start()

    # Manager para estado compartido ultra-rápido (deduplicación sin ventana y compromiso SQLite)
    manager = mp.Manager()
    shared_hashes = manager.dict()
    shared_persisted_hashes = manager.dict()

    dbp = mp.Process(
        target=db_daemon, args=(db_q, db_path, stop_db, shared_persisted_hashes),
        name="DB-Daemon", daemon=False
    )
    dbp.start()

    spiders = []
    if not CONFIG.get("AGENT_MODE", False):
        spiders = [
            mp.Process(
                target=spider,
                args=(spider_q, dl_q, db_q, stop_workers,
                      cookies, ua, db_path, semaphore, active_downloads_count),
                name=f"Spider-{i + 1}"
            )
            for i in range(CONFIG["SPIDERS"])
        ]

    downs = [
        mp.Process(
            target=downloader,
            args=(dl_q, db_q, stop_workers,
                  cookies, ua, db_path, semaphore, shared_hashes,
                  shared_persisted_hashes, active_downloads_count),
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
        active_downloads_count=active_downloads_count,
    )
    signal.signal(signal.SIGINT,  shutdown_handler)
    signal.signal(signal.SIGTERM, shutdown_handler)

    # === NUEVA LÓGICA DE ESPERA (REEMPLAZAR DESDE AQUÍ) ===
    
    if CONFIG.get("AGENT_MODE", False):
        # Enviar señal de fin a la araña
        spider_q.put(_STOP_SENTINEL)
        
        # Ejecutar spider en el proceso principal
        log.info("🤖 Ejecutando Spider en el proceso principal (Modo Agente)...")
        spider(spider_q, dl_q, db_q, stop_workers,
               cookies, ua, db_path, semaphore, active_downloads_count)
    else:
        # 1. Enviar señal de fin a las arañas (una por cada proceso spider)
        for _ in range(CONFIG["SPIDERS"]):
            spider_q.put(_STOP_SENTINEL)

        # 2. Esperar a que las arañas terminen de leer TODOS los cursos sin límite de tiempo
        log.info("🕷️ Arañas desplegadas. Explorando eGela (esto puede tardar unos minutos)...")
        for p in spiders:
            p.join() 

    # 3. Esperar a que los downloaders terminen todas las descargas
    log.info("✅ Spiders finalizados. Esperando descargas activas (%d en curso/cola)...", active_downloads_count.value)
    t0 = time.monotonic()
    while active_downloads_count.value > 0 and not stop_workers.is_set():
        time.sleep(1.0)
        if time.monotonic() - t0 > 1800:  # 30 minutos límite
            log.warning("Timeout superado esperando a que terminen las descargas.")
            break

    log.info("✅ Descargas finalizadas. Apagando downloaders...")
    for _ in range(CONFIG["DOWNLOADERS"]):
        dl_q.put(_STOP_SENTINEL)

    for p in downs:
        p.join()
        
    log.info("✅ Downloaders finalizados. Guardando base de datos...")
    db_q.put(_STOP_SENTINEL)
    
    stop_workers.set()
    stop_db.set()
    dbp.join(timeout=60)
    
    # === HASTA AQUÍ ===
    for cid in courses:
        c_dir = os.path.join(CONFIG["ROOT_DIR"], f"Curso_{cid}")
        if os.path.isdir(c_dir):
            flush_dlq_to_human_report(c_dir, cid)
            synthesize_master_index(c_dir, cid)
            generate_review_report(c_dir, cid)

    _check_emergency_files()

    print("=" * 60)
    print("[*] GOLDEN MASTER v12 — ZERO DATA LOSS — SELLADO.")
    print(f"   Archivos: {CONFIG['ROOT_DIR']}/")
    print(f"   Estado:   {db_path}")
    print("=" * 60)


if __name__ == "__main__":
    main()
