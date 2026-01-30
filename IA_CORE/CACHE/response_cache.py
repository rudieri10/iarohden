# -*- coding: utf-8 -*-
import hashlib
import json
import time
from typing import Dict, Optional, Any
from ..DATA.storage import DataStorage

class ResponseCache:
    """
    Cache semântico de respostas para acelerar consultas similares.
    Utiliza embeddings para encontrar perguntas com significado próximo.
    """

    def __init__(self):
        self.storage = DataStorage()
        # Usa a infraestrutura do DataStorage para obter a coleção no ChromaDB
        self.collection = self.storage._get_collection("response_cache_v1")
        
    def get_similar_response(self, user_query: str, threshold: float = 0.85) -> Optional[Dict[str, Any]]:
        """
        Busca uma resposta similar no cache.
        
        Args:
            user_query: A pergunta do usuário
            threshold: Limite de similaridade (0 a 1). 
                       ChromaDB retorna distância (menor é mais similar).
                       Assumindo distância de cosseno: similaridade = 1 - distância.
                       Então se threshold é 0.85, distância deve ser <= 0.15.
        
        Returns:
            Dict com a resposta e metadados ou None se não encontrar
        """
        try:
            results = self.collection.query(
                query_texts=[user_query],
                n_results=1
            )
            
            if not results['ids'] or not results['ids'][0]:
                return None
                
            # Distância (menor é melhor para cosseno/euclidiana)
            distance = results['distances'][0][0]
            
            # Convertendo threshold de similaridade para distância máxima aceitável
            # Se threshold é 0.85 (alta similaridade), aceitamos distância até 0.15
            max_distance = 1.0 - threshold
            
            if distance <= max_distance:
                metadata = results['metadatas'][0][0]
                return {
                    'response': metadata.get('response'),
                    'original_query': metadata.get('original_query'),
                    'similarity': 1.0 - distance,
                    'cached_at': metadata.get('timestamp'),
                    'source': 'semantic_cache'
                }
                
            return None
            
        except Exception as e:
            print(f"⚠️ Erro ao consultar cache semântico: {e}")
            return None

    def cache_response(self, query: str, response: str, metadata: Dict[str, Any] = None):
        """
        Salva uma resposta no cache.
        
        Args:
            query: A pergunta original
            response: A resposta gerada
            metadata: Dados adicionais (opcional)
        """
        try:
            # Gerar ID único baseado no hash da query para evitar duplicatas exatas
            query_hash = hashlib.md5(query.encode('utf-8')).hexdigest()
            
            # Preparar metadados (ChromaDB requer tipos simples)
            meta = {
                'original_query': query,
                'response': response,
                'timestamp': time.time(),
                'date_str': time.strftime('%Y-%m-%d %H:%M:%S')
            }
            
            # Adicionar metadados extras se fornecidos (filtrando tipos complexos)
            if metadata:
                for k, v in metadata.items():
                    if isinstance(v, (str, int, float, bool)):
                        meta[f"meta_{k}"] = v
            
            self.collection.upsert(
                ids=[query_hash],
                documents=[query],
                metadatas=[meta]
            )
            
        except Exception as e:
            print(f"⚠️ Erro ao salvar no cache semântico: {e}")
