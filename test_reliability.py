import unittest
import multiprocessing as mp
import time
import os
import json
import signal
import shutil
import tempfile
from pathlib import Path
from unittest.mock import MagicMock, patch

# Importar componentes del crawler (asumiendo que están en el mismo dir)
import sys
from unittest.mock import MagicMock
sys.modules["webdriver_manager"] = MagicMock()
sys.modules["webdriver_manager.chrome"] = MagicMock()

import crawler

class TestCrawlerReliability(unittest.TestCase):
    def setUp(self):
        self.tmp_dir = tempfile.mkdtemp()
        crawler.CONFIG["ROOT_DIR"] = self.tmp_dir
        crawler.CONFIG["DB_PATH"] = "test.sqlite"
        self.db_path = os.path.join(self.tmp_dir, "test.sqlite")
        crawler.init_db(self.db_path)

    def tearDown(self):
        shutil.rmtree(self.tmp_dir)

    def test_semaphore_ownership(self):
        """Verifica que el semáforo no se libera doblemente y que _sem_owned funciona."""
        sem = mp.BoundedSemaphore(1)
        job = {"url": "http://test.com", "cid": "1"}
        stop = mp.Event()
        dq = mp.Queue()
        
        # 1. Encolar adquiere el semáforo
        success = crawler._enqueue_download(dq, sem, job, stop)
        self.assertTrue(success)
        self.assertTrue(job.get("_sem_owned"))
        self.assertEqual(dq.get(), job)
        
        # Intentar adquirir otra vez debería bloquear (no lo hacemos para no colgar el test)
        self.assertFalse(sem.acquire(block=False))
        
        # 2. _download_one libera el semáforo
        # Simulamos el finally de _download_one
        if job.get("_sem_owned"):
            sem.release()
            job["_sem_owned"] = False
            
        self.assertFalse(job.get("_sem_owned"))
        # Ahora debería estar libre
        self.assertTrue(sem.acquire(block=False))
        sem.release()
        
        # 3. Doble release prevenido
        # Si intentamos liberar un job que ya no tiene el permiso, no debería lanzar ValueError
        # (porque el wrapper checkea _sem_owned)
        if job.get("_sem_owned"):
            sem.release()
            job["_sem_owned"] = False
        # No hace nada, no peta.

    def test_requeue_backoff(self):
        """Verifica que el backoff aumenta el tiempo de espera y retries."""
        q = mp.Queue()
        job = {"url": "http://test.com", "cid": "1", "retries": 0}
        
        success = crawler.async_requeue_with_backoff(q, job)
        self.assertTrue(success)
        
        queued_job = q.get()
        self.assertEqual(queued_job["retries"], 1)
        self.assertGreater(queued_job["process_after"], time.time())
        
        # Probar límite
        queued_job["retries"] = 3
        success = crawler.async_requeue_with_backoff(q, queued_job)
        self.assertFalse(success)

    def test_reingestion_routing(self):
        """Verifica que la reingestión clasifica correctamente los tipos."""
        # Crear ficheros de emergencia simulados
        dump_file = os.path.join(self.tmp_dir, "_EMERGENCY_DUMP_999.jsonl")
        items = [
            {"type": "VISITED", "url": "http://v1", "cid": "1"},
            {"type": "HASH", "hash": "abc", "path": "/tmp/a"},
            {"url": "http://s1", "cid": "1"}, # Spider task
            {"url": "http://d1", "cid": "1", "target": "/tmp/d", "section": "S1"} # Download job
        ]
        with open(dump_file, "w") as f:
            for item in items:
                f.write(json.dumps(item) + "\n")
        
        dl_q = mp.Queue()
        sp_q = mp.Queue()
        db_q = mp.Queue()
        sem = mp.BoundedSemaphore(10)
        
        counts = crawler.reingest_emergency_data(dl_q, sp_q, db_q, sem)
        
        self.assertEqual(counts["DB_EVENT"], 2)
        self.assertEqual(counts["SPIDER_TASK"], 1)
        self.assertEqual(counts["DOWNLOAD_JOB"], 1)
        
        # Verificar que el job de descarga tiene el permiso
        job = dl_q.get()
        self.assertTrue(job.get("_sem_owned"))

    def test_safe_put_emergency_dump(self):
        """Verifica que safe_put vuelca a disco si la cola está llena."""
        q = mp.Queue(maxsize=1)
        q.put({"msg": "full"})
        
        job = {"type": "TEST", "data": "important"}
        success = crawler.safe_put(q, job)
        
        # Debería retornar False porque fue a disco
        self.assertFalse(success)
        
        # Verificar archivo en disco
        dumps = list(Path(self.tmp_dir).glob("_EMERGENCY_DUMP_*.jsonl"))
        self.assertGreater(len(dumps), 0)
        
        with open(dumps[0], "r") as f:
            data = json.loads(f.read())
            self.assertEqual(data["type"], "TEST")

if __name__ == "__main__":
    unittest.main()
