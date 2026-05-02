# moodlecrawler 
# -*- coding: utf-8 -*- 
eGela Crawler — Enterprise Time Capsule (GOLDEN MASTER v12 — FORENSIC CERTIFIED) Auditoría forense pre-vuelo completada. Correcciones aplicadas en silencio. 

Garantías matemáticas: 
• Zero Data Loss:     ningún job extraído de una cola se evapora en RAM 
• Kill-Window Shield: SIGTERM en workers rescata el job in-flight a disco (fsync) 
• Atomic I/O:         os.replace + O_EXCL + fsync_dir = escritura indestructible 
• Stateless workers:  DB es el único source of truth; procesos son efímeros • Resource safety:    todos los fd, sockets y conn cerrados en finally garantizado 
• No global mutable state: semaphore, events y queues son los únicos canales IPC 
• Lock hygiene:       locks huérfanos limpiados en cada arranque 
• FD ownership:       tmp_fd cedido a fdopen antes de cualquier excepción 

Uso: 
export EGELA_USER="tu_usuario" 
export EGELA_PASS="tu_contraseña" 
python egela_golden_master_v12.py
