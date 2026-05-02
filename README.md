# eGela Downloader & Archiver

Este es un proyecto personal para automatizar la descarga y archivo de los cursos de la plataforma Moodle de la UPV/EHU (eGela). 

Bajar apuntes archivo por archivo a final de cuatrimestre es tedioso, así que este script se encarga de recorrer la estructura del curso, clasificar los recursos y descargarlos de forma concurrente manteniendo la organización por temas. 

Está diseñado para ser muy tolerante a fallos: si se te cae el internet, te quedas sin espacio en disco o cancelas la ejecución a medias, el script guarda su estado y retomará el trabajo exactamente donde lo dejó la próxima vez que lo arranques.

## Características

*   **Descargas concurrentes:** Utiliza `multiprocessing` para aislar el proceso de scraping (Selenium) de los procesos de descarga (`requests`), acelerando el proceso.
*   **Tolerancia a interrupciones (Graceful Shutdown):** Puedes parar el script en cualquier momento con `Ctrl+C`. Los procesos terminarán de descargar el archivo actual, guardarán las tareas pendientes en disco y cerrarán la base de datos limpiamente para evitar corrupciones.
*   **Deduplicación (SQLite WAL):** Mantiene un registro en SQLite de lo que ya se ha visitado y descargado (basado en hashes SHA256). Si un profesor sube el mismo PDF en dos sitios distintos, el script crea un enlace duro (hardlink) en lugar de descargarlo dos veces.
*   **Escrituras atómicas:** Usa bloqueos POSIX y volcados a archivos temporales (`.tmp`) antes de mover el archivo final. Esto evita que te queden PDFs a medio descargar si hay un corte de luz.
*   **Generación de Índices:** Crea automáticamente un archivo `00_INDICE_MAESTRO.md` en cada curso con enlaces locales a todo el material, ideal para navegarlo offline con Obsidian o VS Code.

## Requisitos

Necesitas tener instalado **Python 3.12+** y Google Chrome en tu equipo.

Clona este repositorio e instala las dependencias:
```bash
pip install requests selenium webdriver-manager
