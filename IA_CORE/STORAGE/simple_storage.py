import os
import json
import sqlite3
import logging
import hashlib
import struct
import time
import requests
import chromadb
from datetime import datetime
from typing import List, Dict, Optional, Any
from chromadb.utils import embedding_functions

logger = logging.getLogger(__name__)

class RohdenEmbeddingFunction(embedding_functions.EmbeddingFunction):
    """Função de embedding customizada que usa o servidor Rohden AI."""
    
    def __init__(self):
        self.ai_url_internal = os.getenv("ROHDEN_AI_INTERNAL_URL", "http://192.168.1.217:11434")
        self.ai_model = os.getenv("ROHDEN_AI_MODEL", "qwen2.5:3b")
        self._dim = None

    def __call__(self, input: List[str]) -> List[List[float]]:
        embeddings = []
        for text in input:
            vector = self._generate_embedding(text)
            if vector:
                if self._dim is None: self._dim = len(vector)
                embeddings.append(vector)
            else:
                dim = self._dim if self._dim else 2048 # Fallback seguro
                embeddings.append([0.0] * dim)
        return embeddings

    def _generate_embedding(self, text: str) -> Optional[List[float]]:
        clean_text = text.replace("\n", " ").strip()
        url = f"{self.ai_url_internal}/api/embed"
        
        try:
            payload = {"model": self.ai_model, "input": clean_text}
            response = requests.post(url, json=payload, timeout=30)
            
            if response.status_code == 200:
                data = response.json()
                if "embeddings" in data:
                    return data["embeddings"][0]
        except Exception:
            pass
        return None

class SimpleStorage:
    """
    Storage Unificado: SQLite (Logs/Histórico) + ChromaDB (Conhecimento).
    """
    
    def __init__(self):
        # Configuração centralizada para C:\ia\banco_ia
        self.base_dir = r"C:\ia\banco_ia"
        os.makedirs(self.base_dir, exist_ok=True)
        
        self.db_path = os.path.join(self.base_dir, "ai_data.db")
        self.chroma_path = os.path.join(self.base_dir, "chroma_db")
        
        self._init_sqlite()
        self._init_chroma()
        
    def _init_sqlite(self):
        """Inicializa tabelas SQLite."""
        with sqlite3.connect(self.db_path) as conn:
            conn.execute('''
                CREATE TABLE IF NOT EXISTS observer_logs (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    sql_hash TEXT,
                    sql_text TEXT,
                    timestamp DATETIME DEFAULT CURRENT_TIMESTAMP,
                    execution_time REAL,
                    row_count INTEGER,
                    error_message TEXT,
                    context TEXT
                )
            ''')
            conn.execute('''
                CREATE TABLE IF NOT EXISTS patterns_detected (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    pattern_type TEXT,
                    description TEXT,
                    confidence REAL,
                    status TEXT DEFAULT 'PENDING', -- PENDING, VALIDATED, REJECTED
                    created_at DATETIME DEFAULT CURRENT_TIMESTAMP
                )
            ''')
            
    def _init_chroma(self):
        """Inicializa ChromaDB."""
        try:
            self.chroma_client = chromadb.PersistentClient(path=self.chroma_path)
            self.embedding_fn = RohdenEmbeddingFunction()
            self.kb_collection = self.chroma_client.get_or_create_collection(
                name="knowledge_base",
                embedding_function=self.embedding_fn,
                metadata={"hnsw:space": "cosine"}
            )
        except Exception as e:
            logger.error(f"Erro ao iniciar ChromaDB: {e}")
            self.kb_collection = None

    def log_observation(self, sql: str, exec_time: float, rows: int, error: str = None, context: str = ""):
        """Registra observação de execução SQL."""
        sql_hash = hashlib.md5(sql.encode()).hexdigest()
        with sqlite3.connect(self.db_path) as conn:
            conn.execute(
                "INSERT INTO observer_logs (sql_hash, sql_text, execution_time, row_count, error_message, context) VALUES (?, ?, ?, ?, ?, ?)",
                (sql_hash, sql, exec_time, rows, error, context)
            )

    def save_pattern(self, type: str, desc: str, confidence: float):
        """Salva um padrão detectado para validação."""
        with sqlite3.connect(self.db_path) as conn:
            conn.execute(
                "INSERT INTO patterns_detected (pattern_type, description, confidence) VALUES (?, ?, ?)",
                (type, desc, confidence)
            )

    def get_recent_logs(self, limit: int = 100) -> List[Dict]:
        """Recupera logs recentes."""
        with sqlite3.connect(self.db_path) as conn:
            conn.row_factory = sqlite3.Row
            cursor = conn.execute("SELECT * FROM observer_logs ORDER BY timestamp DESC LIMIT ?", (limit,))
            return [dict(row) for row in cursor.fetchall()]

    def save_knowledge(self, title: str, content: str, category: str, tags: str = ""):
        """Salva conhecimento confirmado no ChromaDB."""
        if not self.kb_collection: return
        
        id_val = f"{category}_{hashlib.md5(title.encode()).hexdigest()}"
        try:
            self.kb_collection.upsert(
                ids=[id_val],
                documents=[content],
                metadatas=[{
                    "title": title,
                    "category": category,
                    "tags": tags,
                    "timestamp": datetime.now().isoformat()
                }]
            )
        except Exception as e:
            # Tratamento para erro de dimensão
            if "dimension" in str(e).lower():
                logger.warning("Erro de dimensão no ChromaDB. Recriando coleção...")
                self.chroma_client.delete_collection("knowledge_base")
                self._init_chroma()
                self.save_knowledge(title, content, category, tags)

    def find_knowledge(self, query: str, limit: int = 5) -> List[Dict]:
        """Busca conhecimento similar."""
        if not self.kb_collection: return []
        
        results = self.kb_collection.query(
            query_texts=[query],
            n_results=limit
        )
        
        knowledge = []
        if results["documents"]:
            for i, doc in enumerate(results["documents"][0]):
                meta = results["metadatas"][0][i]
                knowledge.append({
                    "content": doc,
                    "title": meta.get("title"),
                    "category": meta.get("category"),
                    "similarity": 1 - results["distances"][0][i] if "distances" in results else 0
                })
        return knowledge
