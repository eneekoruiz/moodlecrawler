import os
import re
import time
import requests
import unicodedata
from urllib.parse import urljoin, unquote
from bs4 import BeautifulSoup

from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

# --- Constants & Config ---
LOGIN_URL = "https://egela.ehu.eus/login/index.php"
USERNAME = os.getenv("EGELA_USER")
PASSWORD = os.getenv("EGELA_PASS")
OUTPUT_DIR = os.path.abspath("EGELA_DOWNLOADS")

WINDOWS_RESERVED = {
    "CON", "PRN", "AUX", "NUL",
    "COM1", "COM2", "COM3", "COM4", "COM5", "COM6", "COM7", "COM8", "COM9",
    "LPT1", "LPT2", "LPT3", "LPT4", "LPT5", "LPT6", "LPT7", "LPT8", "LPT9"
}

# --- Resilience & Windows Helpers ---
def make_windows_safe_path(abs_path):
    """
    Desbloquea el límite estricto de 260 caracteres de MAX_PATH en Windows.
    Añadiendo el prefijo '\\?\' a una ruta absoluta, se permite hasta 32,767 caracteres.
    """
    if os.name == 'nt' and not abs_path.startswith('\\\\?\\'):
        return '\\\\?\\' + os.path.normpath(abs_path)
    return abs_path

def clean_name(name, default="Recurso"):
    if not name:
        return default
    
    # 1. Limpieza de basura (ocultos, retornos)
    name = str(name).replace('\n', ' ').replace('\r', '').replace('\t', ' ')
    name = unquote(name)
    name = unicodedata.normalize("NFC", name)
    
    # 2. Quitar caracteres de control
    name = "".join(c for c in name if unicodedata.category(c) not in {"Cf", "Cc", "Cs", "Co", "Cn"})
    
    # 3. Remover caracteres ilegales en Windows
    name = re.sub(r'[<>:"/\\|?*\x00-\x1f]', '_', name).strip()
    name = re.sub(r'\.{2,}', '.', name)
    while name.endswith(('.', ' ')):
        name = name[:-1]
    
    if not name:
        return default

    # 4. Palabras reservadas de Windows
    base_name = name.split('.')[0].upper()
    if base_name in WINDOWS_RESERVED:
        name = "_" + name
    
    # 5. Acortar el componente individual para evitar excesos estéticos,
    # aunque MAX_PATH ya esté solucionado con \\?\
    name = name[:120].strip()
    return name or default

def get_safe_filepath(target_dir, file_name, remote_size=None):
    """
    Evita la pérdida de datos (Data Loss) por colisión de nombres.
    Si el archivo existe y el tamaño es diferente, crea una versión con sufijo (1), (2).
    """
    base_name, ext = os.path.splitext(file_name)
    counter = 1
    
    final_path = os.path.join(target_dir, file_name)
    final_path = make_windows_safe_path(final_path)
    
    while os.path.exists(final_path):
        local_size = os.path.getsize(final_path)
        # Si el tamaño es idéntico, asumimos que es el mismo archivo descargado previamente
        if remote_size is not None and local_size == remote_size:
            return final_path, True # exists_and_matches
            
        # Si el tamaño difiere, es un duplicado con otro contenido. Resolvemos el nombre.
        new_name = f"{base_name} ({counter}){ext}"
        final_path = os.path.join(target_dir, new_name)
        final_path = make_windows_safe_path(final_path)
        counter += 1
        
    return final_path, False

# --- Network & Session Management ---
def create_resilient_session():
    """Crea una sesión blindada contra microcortes y fallos de red"""
    session = requests.Session()
    
    # Política de reintentos agresiva
    retry_strategy = Retry(
        total=5,  # 5 reintentos en total
        backoff_factor=1,  # 1s, 2s, 4s, 8s, 16s
        status_forcelist=[429, 500, 502, 503, 504],
        allowed_methods=["HEAD", "GET", "OPTIONS"]
    )
    
    adapter = HTTPAdapter(max_retries=retry_strategy)
    session.mount("http://", adapter)
    session.mount("https://", adapter)
    return session

def init_driver():
    options = webdriver.ChromeOptions()
    options.add_argument("--disable-gpu")
    options.add_argument("--no-sandbox")
    options.add_argument("--headless") # Lo mantenemos invisible para operaciones en background
    return webdriver.Chrome(options=options)

def login_and_get_cookies():
    """Ejecuta un ciclo limpio de Selenium para robar cookies frescas"""
    print("  [Auth] Solicitando nuevas credenciales de sesión...")
    if not USERNAME or not PASSWORD:
        raise ValueError("EGELA_USER o EGELA_PASS no están configurados.")
    
    driver = init_driver()
    try:
        driver.get(LOGIN_URL)
        WebDriverWait(driver, 15).until(EC.presence_of_element_located((By.ID, "username")))
        
        driver.find_element(By.ID, "username").send_keys(USERNAME)
        driver.find_element(By.ID, "password").send_keys(PASSWORD)
        driver.find_element(By.ID, "loginbtn").click()
        
        WebDriverWait(driver, 20).until(EC.url_contains("my/"))
        print("  [Auth] Login exitoso. Extrayendo cookies.")
        return driver.get_cookies()
    finally:
        driver.quit()

def smart_get(session, url, **kwargs):
    """
    Envoltura para detectar caídas de sesión a mitad del scraping y auto-recuperarse.
    """
    try:
        response = session.get(url, **kwargs)
        
        # Moodle redirige al login si la sesión expira
        if "login/index.php" in response.url or response.status_code == 403:
            print("\n[ALERTA] Sesión caducada o acceso denegado. Iniciando re-autenticación al vuelo...")
            new_cookies = login_and_get_cookies()
            for cookie in new_cookies:
                session.cookies.set(cookie['name'], cookie['value'], domain=cookie['domain'])
            print("[ALERTA] Sesión recuperada. Reintentando descarga...")
            # Reintento directo
            response = session.get(url, **kwargs)
            response.raise_for_status()
            
        response.raise_for_status()
        return response
    except requests.exceptions.RequestException as e:
        print(f"  [Network Error Fatal] Falla irrecuperable en {url}: {e}")
        return None

# --- Scraping Logic ---

def expand_all_sections(driver):
    try:
        js_script = """
        document.querySelectorAll('.collapsed').forEach(el => el.classList.remove('collapsed'));
        document.querySelectorAll('.collapse').forEach(el => el.classList.add('show'));
        """
        driver.execute_script(js_script)
        time.sleep(2)
    except Exception:
        pass

def parse_course_html(page_source, course_name):
    soup = BeautifulSoup(page_source, 'html.parser')
    for hidden in soup.find_all(class_=['sr-only', 'accesshide']):
        hidden.decompose()

    course_structure = []
    sections = soup.find_all('li', class_='section')
    if not sections:
        sections = soup.find_all('h3', class_='sectionname')
    
    for sec_idx, section in enumerate(sections, 1):
        title_elem = section.find('h3', class_='sectionname') or section.find(class_='sectionname')
        sec_title = title_elem.get_text(strip=True) if title_elem else f"Seccion_{sec_idx}"
        sec_title_clean = f"{sec_idx:02d}_{clean_name(sec_title)}"
        
        current_label = ""
        activities = section.find_all(['li', 'div'], class_='activity')
        
        for act_idx, activity in enumerate(activities, 1):
            if 'modtype_label' in activity.get('class', []):
                current_label = clean_name(activity.get_text(separator=' ', strip=True))[:50]
                continue
            
            link_elem = activity.find('a', class_='aalink') or activity.find('a')
            if not link_elem:
                continue
                
            url = link_elem.get('href')
            if not url:
                continue
                
            name_elem = activity.find(class_='instancename') or link_elem
            for span in name_elem.find_all('span', class_='accesshide'):
                span.decompose()
            res_name = clean_name(name_elem.get_text(strip=True))
            
            mod_classes = activity.get('class', [])
            res_type = "file"
            
            if 'modtype_folder' in mod_classes or 'folder/view.php' in url:
                res_type = "folder"
            elif 'modtype_assign' in mod_classes or 'assign/view.php' in url:
                res_type = "assign"
            elif 'modtype_page' in mod_classes or 'page/view.php' in url:
                res_type = "page"
            elif 'modtype_forum' in mod_classes or 'forum/view.php' in url:
                res_type = "forum"
            elif 'modtype_url' in mod_classes or 'url/view.php' in url:
                res_type = "url"
                
            path_parts = [clean_name(course_name), sec_title_clean]
            if current_label:
                path_parts.append(current_label)
                
            target_path = os.path.join(*path_parts)
            
            course_structure.append({
                'path': target_path,
                'name': res_name,
                'url': url,
                'type': res_type
            })
            
    return course_structure

def create_url_shortcut(target_dir, name, url):
    """Guarda enlaces externos y foros como atajos de Windows (.url)"""
    safe_dir = make_windows_safe_path(target_dir)
    os.makedirs(safe_dir, exist_ok=True)
    link_path = os.path.join(safe_dir, f"{name}.url")
    link_path = make_windows_safe_path(link_path)
    try:
        with open(link_path, "w", encoding="utf-8") as f:
            f.write(f"[InternetShortcut]\nURL={url}\n")
        print(f"  [Shortcut] Guardado acceso directo: {name}")
    except Exception as e:
        print(f"  [Error] No se pudo crear acceso directo {name}: {e}")

def save_html_content(target_dir, name, html_content):
    """Guarda contenido textual de páginas mod_page para no perder contexto explicativo."""
    safe_dir = make_windows_safe_path(target_dir)
    os.makedirs(safe_dir, exist_ok=True)
    html_path = os.path.join(safe_dir, f"{name}.html")
    html_path = make_windows_safe_path(html_path)
    try:
        with open(html_path, "w", encoding="utf-8") as f:
            f.write(html_content)
        print(f"  [Página] Guardado texto HTML de la página: {name}")
    except Exception as e:
        print(f"  [Error] Guardando página HTML {name}: {e}")

def download_file(session, url, target_dir, file_name_base):
    try:
        response = smart_get(session, url, stream=True, timeout=30)
        if not response: return
        
        # Extracción de extensión correcta
        cd = response.headers.get('content-disposition')
        ext = ""
        if cd:
            filenames = re.findall('filename="?([^";]+)"?', cd)
            if filenames:
                _, ext = os.path.splitext(filenames[0])
        
        if not ext:
            ct = response.headers.get('content-type', '')
            if 'application/pdf' in ct: ext = '.pdf'
            elif 'application/zip' in ct: ext = '.zip'
            elif 'officedocument.wordprocessingml' in ct: ext = '.docx'
            elif 'officedocument.spreadsheetml' in ct: ext = '.xlsx'
            elif 'officedocument.presentationml' in ct: ext = '.pptx'
            elif 'text/html' in ct: ext = '.html'
            else:
                _, url_ext = os.path.splitext(url.split('?')[0])
                ext = url_ext if url_ext else '.bin'
                
        final_name = f"{file_name_base}{ext}"
        
        # Crear ruta segura
        safe_dir = make_windows_safe_path(target_dir)
        os.makedirs(safe_dir, exist_ok=True)
        
        # Algoritmo Anti-Data-Loss
        remote_size = int(response.headers.get('content-length', 0))
        final_path, matches = get_safe_filepath(safe_dir, clean_name(final_name), remote_size if remote_size > 0 else None)
        
        if matches:
            print(f"  [Omitido] Ya existe de igual tamaño: {os.path.basename(final_path)}")
            return
            
        with open(final_path, 'wb') as f:
            for chunk in response.iter_content(chunk_size=8192):
                f.write(chunk)
                
        print(f"  [Descargado OK] {os.path.basename(final_path)}")
    except Exception as e:
        print(f"  [Error] Fallo descargando {url} -> {e}")

def process_moodle_container(session, url, target_dir, container_type, container_name):
    try:
        response = smart_get(session, url, timeout=30)
        if not response: return
        soup = BeautifulSoup(response.text, 'html.parser')
        
        links = []
        
        if container_type == 'folder':
            for a in soup.find_all('a'):
                href = a.get('href')
                if href and ('pluginfile.php' in href or 'forcedownload' in href):
                    name = a.get_text(strip=True) or "Archivo_Carpeta"
                    links.append((href, name))
                    
        elif container_type == 'assign':
            for div in soup.find_all('div', class_='fileuploadsubmission'):
                for a in div.find_all('a'):
                    href = a.get('href')
                    if href and 'pluginfile.php' in href:
                        name = a.get_text(strip=True) or "Archivo_Tarea"
                        links.append((href, name))
                        
            for div in soup.find_all('div', class_='assignattachment'):
                for a in div.find_all('a'):
                    href = a.get('href')
                    if href and 'pluginfile.php' in href:
                        name = a.get_text(strip=True) or "Archivo_Adjunto_Tarea"
                        links.append((href, name))

        elif container_type == 'page':
            # Pages can contain files to download OR text instructions
            content_div = soup.find('div', role='main') or soup.body
            if content_div:
                save_html_content(target_dir, container_name, str(content_div))
                
            for a in soup.find_all('a'):
                href = a.get('href')
                if href and ('pluginfile.php' in href or 'forcedownload' in href):
                    name = a.get_text(strip=True) or "Archivo_Pagina"
                    links.append((href, name))

        if not links and container_type != 'page':
            print(f"  [Info] Contenedor vacío o sin descargables directos en {container_type}")
            return
            
        for file_url, file_name in set(links): 
            download_file(session, file_url, target_dir, clean_name(file_name))
            
    except Exception as e:
        print(f"  [Error] Analizando contenedor {container_type} en {url} -> {e}")

def main():
    if not os.path.exists("cursos.txt"):
        print("Error: No se encontró 'cursos.txt'.")
        return

    with open("cursos.txt", "r", encoding="utf-8") as f:
        course_urls = [line.strip() for line in f if line.strip() and not line.startswith("#")]
        
    if not course_urls:
        print("No hay URLs en cursos.txt")
        return

    print("=== FASE 1: MAPEO Y ANÁLISIS ESTRUCTURAL ESTÁTICO ===")
    all_courses_structure = []
    
    # Login Inicial
    driver = init_driver()
    try:
        driver.get(LOGIN_URL)
        WebDriverWait(driver, 15).until(EC.presence_of_element_located((By.ID, "username")))
        driver.find_element(By.ID, "username").send_keys(USERNAME)
        driver.find_element(By.ID, "password").send_keys(PASSWORD)
        driver.find_element(By.ID, "loginbtn").click()
        WebDriverWait(driver, 20).until(EC.url_contains("my/"))
        
        for url in course_urls:
            print(f"\n=> Analizando Árbol del Curso: {url}")
            driver.get(url)
            course_title = driver.title.split('|')[0].strip() or "Curso_Desconocido"
            expand_all_sections(driver)
            
            source = driver.page_source
            structure = parse_course_html(source, course_title)
            all_courses_structure.extend(structure)
            print(f"   [+] {len(structure)} recursos mapeados posicionalmente.")
            
        selenium_cookies = driver.get_cookies()
    finally:
        driver.quit()

    if not all_courses_structure:
        print("\nEl mapeo ha devuelto 0 recursos.")
        return

    print("\n=== FASE 2: MOTOR DE DESCARGA BLINDADO ===")
    # Sesión resiliente con HTTPAdapter
    session = create_resilient_session()
    for cookie in selenium_cookies:
        session.cookies.set(cookie['name'], cookie['value'], domain=cookie['domain'])

    for resource in all_courses_structure:
        raw_target_dir = os.path.join(OUTPUT_DIR, resource['path'])
        
        url = resource['url']
        res_type = resource['type']
        name = resource['name']
        
        print(f"\n-> [{res_type.upper()}] {name}")
        
        if res_type == 'file':
            download_file(session, url, raw_target_dir, name)
        elif res_type in ('folder', 'assign', 'page'):
            container_dir = os.path.join(raw_target_dir, clean_name(name))
            process_moodle_container(session, url, container_dir, res_type, name)
        elif res_type in ('forum', 'url'):
            create_url_shortcut(raw_target_dir, name, url)
            
    print("\n[ÉXITO TOTAL] Descarga estructurada finalizada. Todos los vectores mitigados.")

if __name__ == "__main__":
    main()
