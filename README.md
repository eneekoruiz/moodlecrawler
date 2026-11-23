# MoodleCrawler · Descargador Incremental de Apuntes eGela

Herramienta de línea de comandos en Python para la descarga automatizada, indexación y sincronización incremental de materiales docentes desde eGela (Moodle de la UPV/EHU).

- **Documentación de arquitectura:** [DeepWiki - moodlecrawler](https://deepwiki.com/eneekoruiz/moodlecrawler)

## Propósito y Valor Técnico

Automatiza la tediosa tarea de revisar asignatura por asignatura en la plataforma universitaria para descargar presentaciones, ejercicios y PDFs, garantizando que el estudiante mantenga una copia local sincronizada y actualizada en todo momento.

- **Autenticación SSO / CAS Automatizada:** Inicia sesión de forma desatendida a través del sistema corporativo de autenticación LDAP / CAS de la universidad mediante un navegador Chromium headless gobernado por Selenium WebDriver.
- **Sincronización Incremental con Hashes SHA-256:** Para cada recurso descubierto en el curso, calcula su huella criptográfica SHA-256 y la contrasta contra una base de datos local SQLite. Si el archivo no ha sufrido modificaciones, se omite su descarga, reduciendo el tráfico y el tiempo de sincronización.
- **Normalización y Estructura Limpia:** Mapea automáticamente la estructura de categorías, temas y carpetas del aula virtual a un árbol de directorios local saneado, eliminando caracteres incompatibles con el sistema de archivos de Windows y Linux.

## Stack Técnico

- **Lenguaje:** Python 3.10+
- **Automatización Web:** Selenium WebDriver, ChromeDriver / Chromium
- **Persistencia de Estado:** SQLite

## Puesta en Marcha Local

### Prerrequisitos

- Python 3.10+
- Navegador Google Chrome o Chromium instalado

### Instalación y Ejecución

```bash
git clone https://github.com/eneekoruiz/moodlecrawler.git
cd moodlecrawler

python -m venv .venv
source .venv/bin/activate  # En Windows: .venv\Scripts\activate
pip install -r requirements.txt

# Configurar credenciales corporativas mediante variables de entorno
export EGELA_USER="tu-usuario-ldap"
export EGELA_PASS="tu-contraseña-ldap"
# En PowerShell:
# $env:EGELA_USER="tu-usuario-ldap"; $env:EGELA_PASS="tu-contraseña-ldap"

# Ejecutar crawler de sincronización
python legacy_crawlers/refactored_crawler.py
```
