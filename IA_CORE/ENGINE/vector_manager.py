import json
import requests
import numpy as np
import struct
import os
import hashlib
import time
from typing import List, Dict, Optional

class VectorManager:
    """
    Gerenciador de Vetores (Embeddings) para busca semântica robusta.
    """
    _embedding_cache = {}  # Cache de embeddings em memória
    _failed_endpoints = {}  # Cache de endpoints que falharam recentemente (url: timestamp)
    
    def __init__(self):
        self.ai_url = os.getenv("ROHDEN_AI_URL")
        self.ai_url_internal = os.getenv("ROHDEN_AI_INTERNAL_URL", "http://192.168.1.217:11434")
        self.api_key = os.getenv("ROHDEN_AI_KEY", "ROHDEN_AI_SECRET_2024")
        self.headers = {
            'X-ROHDEN-AI-KEY': self.api_key,
            'Content-Type': 'application/json'
        }

    def get_embedding_dimension(self) -> int:
        """Retorna a dimensão do embedding gerado pelo modelo atual."""
        # Tenta pegar do cache
        if self._embedding_cache:
            return len(next(iter(self._embedding_cache.values())))
        
        # Tenta gerar um pequeno embedding para teste
        test_vector = self.generate_embedding("test")
        if test_vector:
            return len(test_vector)
        
        # Fallback padrão (geralmente 1024 ou 2048)
        return 2048 # Qwen 2.5 3B costuma usar 2048 ou 1536 dependendo da versão, mas o erro diz que veio 2048

    def generate_embedding(self, text: str) -> Optional[List[float]]:
        """Gera um vetor numérico (embedding) para o text usando o servidor Rohden AI."""
        if not text:
            return None
        
        # 1. Verifica Cache em Memória
        cache_key = hashlib.md5(text.encode('utf-8')).hexdigest()
        if cache_key in self._embedding_cache:
            return self._embedding_cache[cache_key]

        clean_text = text.replace("\n", " ").strip()
        
        # 2. Coleta URLs base disponíveis
        raw_urls = []
        if self.ai_url_internal: raw_urls.append(self.ai_url_internal)
        if self.ai_url: raw_urls.append(self.ai_url)
        
        base_urls = []
        for url in raw_urls:
            base = url.replace('/api/generate', '').replace('/api/chat', '').rstrip('/')
            if base not in base_urls:
                base_urls.append(base)
        
        if not base_urls:
            base_urls = ["http://localhost:11434"]
        
        now = time.time()
        
        # 3. Tenta cada base URL com seus endpoints possíveis
        for base_url in base_urls:
            endpoints = [
                f"{base_url}/api/embed",
                f"{base_url}/api/embeddings",
                f"{base_url}/v1/embeddings"
            ]
            
            for emb_url in endpoints:
                # Pula se falhou recentemente (nos últimos 60 segundos)
                if emb_url in self._failed_endpoints:
                    last_fail = self._failed_endpoints[emb_url]
                    if (now - last_fail) < 60:
                        continue

                try:
                    # Formato Ollama Moderno (/api/embed)
                    if '/api/embed' in emb_url:
                        payload = {
                            "model": os.getenv("ROHDEN_AI_MODEL", "qwen2.5:3b"),
                            "input": clean_text
                        }
                    # Formato Ollama Antigo (/api/embeddings)
                    elif '/api/embeddings' in emb_url:
                        payload = {
                            "model": os.getenv("ROHDEN_AI_MODEL", "qwen2.5:3b"),
                            "prompt": clean_text
                        }
                    # Formato OpenAI/Llama-cpp (/v1/embeddings)
                    else:
                        payload = {
                            "model": os.getenv("ROHDEN_AI_MODEL", "qwen2.5:3b"),
                            "input": clean_text,
                            "encoding_format": "float"
                        }
                        
                    timeout = 60 if '192.168' in emb_url or 'localhost' in emb_url else 70
                    
                    response = requests.post(emb_url, json=payload, headers=self.headers, timeout=timeout)
                    
                    if response.status_code == 200:
                        data = response.json()
                        embedding = data.get("embedding") or data.get("embeddings")
                        
                        if not embedding and "data" in data and isinstance(data["data"], list):
                            embedding = data["data"][0].get("embedding") or data["data"][0].get("embeddings")
                        
                        if embedding and isinstance(embedding, list) and len(embedding) > 0:
                            if isinstance(embedding[0], list):
                                embedding = embedding[0]
                            
                            try:
                                clean_vector = [float(x) for x in embedding if x is not None]
                                if len(clean_vector) > 0:
                                    self._embedding_cache[cache_key] = clean_vector
                                    self._failed_endpoints.pop(emb_url, None)
                                    return clean_vector
                            except Exception:
                                pass
                    
                    self._failed_endpoints[emb_url] = now
                    
                except Exception as e:
                    self._failed_endpoints[emb_url] = now
                    if '192.168' in emb_url or 'localhost' in emb_url:
                        print(f"   -> Endpoint interno falhou: {emb_url} ({str(e)})")
                    continue
        
        return None

    def vector_to_blob(self, vector: List[float]) -> bytes:
        """Converte uma lista de floats para um BLOB binário (compacto)"""
        if not vector: return b''
        return struct.pack(f'{len(vector)}f', *vector)

    def blob_to_vector(self, blob: bytes) -> List[float]:
        """Converte um BLOB binário de volta para uma lista de floats"""
        if not blob: return []
        n = len(blob) // 4
        return list(struct.unpack(f'{n}f', blob))

    def cosine_similarity(self, v1: List[float], v2: List[float]) -> float:
        """Calcula a similaridade de cosseno entre dois vetores"""
        if not v1 or not v2 or len(v1) != len(v2):
            return 0.0
        
        a = np.array(v1)
        b = np.array(v2)
        
        norm_a = np.linalg.norm(a)
        norm_b = np.linalg.norm(b)
        
        if norm_a == 0 or norm_b == 0:
            return 0.0
            
        return np.dot(a, b) / (norm_a * norm_b)

    def find_most_similar(self, query_text: str, items_with_vectors: List[Dict], top_n: int = 3) -> List[Dict]:
        """Encontra os itens mais similares semânticamente à consulta"""
        query_vector = self.generate_embedding(query_text)
        if not query_vector:
            return []
            
        results = []
        for item in items_with_vectors:
            vector_blob = item.get('embedding_vector')
            if not vector_blob: continue
            
            item_vector = self.blob_to_vector(vector_blob)
            similarity = self.cosine_similarity(query_vector, item_vector)
            
            item_copy = item.copy()
            item_copy['similarity'] = float(similarity)
            results.append(item_copy)
            
        results.sort(key=lambda x: x['similarity'], reverse=True)
        return results[:top_n]
