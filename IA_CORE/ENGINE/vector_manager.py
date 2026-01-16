import json
import requests
import numpy as np
import struct
import os
from typing import List, Dict, Optional

class VectorManager:
    """
    Gerenciador de Vetores (Embeddings) para busca semântica robusta.
    """
    _embedding_cache = {}  # Cache de embeddings em memória
    
    def __init__(self):
        self.ai_url = os.getenv("ROHDEN_AI_URL")
        self.ai_url_internal = os.getenv("ROHDEN_AI_INTERNAL_URL")
        self.api_key = os.getenv("ROHDEN_AI_KEY", "ROHDEN_AI_SECRET_2024")
        self.headers = {
            'X-ROHDEN-AI-KEY': self.api_key,
            'Content-Type': 'application/json'
        }

    def generate_embedding(self, text: str) -> Optional[List[float]]:
        """Gera um vetor numérico para o texto usando o servidor Rohden AI com retentativas e tratamento de texto"""
        if not text or not text.strip():
            return None

        # Verificar Cache
        cache_key = text.lower().strip()
        if cache_key in self._embedding_cache:
            return self._embedding_cache[cache_key]

        # Limitar o tamanho do texto para evitar erros de contexto no modelo de embedding
        # Modelos típicos aceitam entre 512 e 2048 tokens. 
        # Vamos ser conservadores e pegar os primeiros 4000 caracteres (~1000 tokens)
        clean_text = text[:4000].replace('\n', ' ').strip()
        
        candidate_urls = [self.ai_url_internal, self.ai_url]
        
        for base_url in candidate_urls:
            if not base_url: continue
            
            # Limpeza da URL base: extrair o host e o prefixo do caminho se houver
            from urllib.parse import urlparse
            parsed = urlparse(base_url)
            
            # Se o caminho contém /api/generate, o prefixo é o que vem antes de /api
            path_prefix = parsed.path.split('/api/')[0].rstrip('/')
            host_url = f"{parsed.scheme}://{parsed.netloc}{path_prefix}"
            
            # Lista de possíveis endpoints de embedding (Ollama, Llama.cpp, OpenAI-style)
            endpoints = [
                f"{host_url}/api/embeddings",
                f"{host_url}/v1/embeddings",
                f"{host_url}/embeddings",
                f"{host_url}/embedding"
            ]
            
            # Adiciona os base_url originais com replace caso não sejam apenas host/porta
            if "/api/generate" in base_url:
                endpoints.insert(0, base_url.replace('/api/generate', '/api/embeddings'))
                endpoints.insert(1, base_url.replace('/api/generate', '/v1/embeddings'))
            
            # Remover duplicatas mantendo a ordem
            endpoints = list(dict.fromkeys(endpoints))
            
            for emb_url in endpoints:
                # Tenta até 2 vezes por endpoint
                for attempt in range(2):
                    try:
                        # Formato Ollama (/api/embeddings)
                        if '/api/embeddings' in emb_url:
                            payload = {
                                "model": os.getenv("ROHDEN_AI_MODEL", "llama3.1-gguf"),
                                "prompt": clean_text
                            }
                        # Formato OpenAI/Llama-cpp (/v1/embeddings)
                        else:
                            payload = {
                                "model": os.getenv("ROHDEN_AI_MODEL", "llama3.1-gguf"),
                                "input": clean_text,
                                "encoding_format": "float"
                            }
                            
                        response = requests.post(emb_url, json=payload, headers=self.headers, timeout=10)
                        
                        if response.status_code == 200:
                            data = response.json()
                            # Extração flexível (pode vir como 'embedding' ou 'data[0].embedding')
                            embedding = data.get("embedding")
                            if not embedding and "data" in data and isinstance(data["data"], list):
                                embedding = data["data"][0].get("embedding")
                                
                            if embedding and isinstance(embedding, list) and len(embedding) > 0:
                                # LIMPEZA E VALIDAÇÃO: Garantir que todos os elementos sejam floats
                                try:
                                    clean_vector = []
                                    for x in embedding:
                                        if x is None: continue
                                        try:
                                            clean_vector.append(float(x))
                                        except (ValueError, TypeError):
                                            continue
                                    
                                    if len(clean_vector) > 0:
                                        # Salvar no Cache
                                        self._embedding_cache[cache_key] = clean_vector
                                        return clean_vector
                                except Exception as e:
                                    print(f"Erro ao processar vetor da API: {str(e)}")
                        else:
                            if attempt == 1:
                                print(f"Erro na API em {emb_url} (Status {response.status_code})")
                        
                    except Exception as e:
                        if attempt == 1 and emb_url == endpoints[-1]: # Loga apenas na última falha total daquela URL base
                            print(f"Aviso: Servidor {base_url} indisponível ou recusou conexão. Verifique se o serviço Rohden AI está rodando.")
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
            
            # Adiciona similaridade ao item e inclui nos resultados
            item_copy = item.copy()
            item_copy['similarity'] = float(similarity)
            results.append(item_copy)
            
        # Ordena por similaridade decrescente
        results.sort(key=lambda x: x['similarity'], reverse=True)
        return results[:top_n]
