"""
STORAGE - Sistema de Armazenamento Local (ChromaDB + SQLite)
Substitui o Oracle para máxima velocidade e eficiência.
"""

import json
import os
import sqlite3
import chromadb
from chromadb.api.types import Documents, Embeddings, EmbeddingFunction
from datetime import datetime
from typing import Dict, List, Optional, Any
from pathlib import Path

class RohdenEmbeddingFunction(EmbeddingFunction):
    """Função de embedding customizada que usa o servidor Rohden AI via VectorManager"""
    def __init__(self):
        from ..ENGINE.vector_manager import VectorManager
        self.vm = VectorManager()
        
    def __call__(self, input: Documents) -> Embeddings:
        embeddings = []
        for text in input:
            vector = self.vm.generate_embedding(text)
            if vector:
                embeddings.append(vector)
            else:
                # Fallback para vetor de zeros se falhar (evita erro no Chroma)
                embeddings.append([0.0] * 1024) # Ajustar dimensão se necessário
        return embeddings

class DataStorage:
    """Sistema profissional de armazenamento local usando ChromaDB (Vetores) e SQLite (Relacional)"""
    
    def __init__(self):
        """Inicializa os bancos de dados locais"""
        # Caminhos para os bancos
        self.base_path = Path(os.path.dirname(__file__))
        self.db_path = self.base_path / "ai_storage.db"
        self.chroma_path = str(self.base_path / "chroma_db")
        
        # Garantir diretório existe
        os.makedirs(self.base_path, exist_ok=True)
        
        # Inicializar ChromaDB com função de embedding customizada
        self.embedding_fn = RohdenEmbeddingFunction()
        self.chroma_client = chromadb.PersistentClient(path=self.chroma_path)
        
        # Inicializar SQLite e criar tabelas se não existirem
        self._init_sqlite()
    
    def _get_collection(self, name: str):
        """Obtém ou cria uma coleção com a função de embedding correta"""
        return self.chroma_client.get_or_create_collection(
            name=name, 
            embedding_function=self.embedding_fn
        )
        
    def _init_sqlite(self):
        """Cria as tabelas do SQLite se não existirem"""
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        
        # Tabela de Configurações
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS tb_ai_configurations (
                key TEXT PRIMARY KEY,
                value TEXT,
                value_type TEXT,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        ''')
        
        # Tabela de Fluxos de Processo
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS tb_ai_process_flows (
                process_name TEXT PRIMARY KEY,
                flow_sequence TEXT,
                timing_insights TEXT,
                description TEXT,
                is_active INTEGER DEFAULT 1,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        ''')
        
        # Tabela de Histórico de Chat
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS tb_ai_chat_history (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                chat_id TEXT,
                role TEXT,
                content TEXT,
                tokens INTEGER,
                model TEXT,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        ''')
        
        # Tabela de Memória Contextual
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS tb_ai_contextual_memory (
                key TEXT PRIMARY KEY,
                value TEXT,
                expiration TIMESTAMP,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        ''')
        
        conn.commit()
        conn.close()

    def _get_sqlite_conn(self):
        """Retorna uma conexão com o SQLite que retorna dicionários"""
        conn = sqlite3.connect(self.db_path)
        conn.row_factory = sqlite3.Row
        return conn

    # --- CONFIGURAÇÕES ---

    def get_config(self, key: str, default: Any = None) -> Any:
        """Obtém uma configuração do SQLite"""
        conn = self._get_sqlite_conn()
        cursor = conn.cursor()
        try:
            cursor.execute('SELECT value, value_type FROM tb_ai_configurations WHERE key = ?', (key,))
            result = cursor.fetchone()
            
            if not result:
                return default
            
            value, value_type = result['value'], result['value_type']
            
            if value_type == 'json':
                return json.loads(value)
            elif value_type == 'int':
                return int(value)
            elif value_type == 'float':
                return float(value)
            elif value_type == 'bool':
                return str(value).lower() == 'true'
            else:
                return value
        finally:
            conn.close()
    
    def save_config(self, key: str, value: Any, value_type: str = 'string'):
        """Salva uma configuração no SQLite"""
        conn = self._get_sqlite_conn()
        cursor = conn.cursor()
        try:
            if value_type == 'json':
                value_str = json.dumps(value)
            else:
                value_str = str(value)
            
            cursor.execute('''
                INSERT INTO tb_ai_configurations (key, value, value_type, updated_at)
                VALUES (?, ?, ?, CURRENT_TIMESTAMP)
                ON CONFLICT(key) DO UPDATE SET 
                    value = excluded.value, 
                    value_type = excluded.value_type, 
                    updated_at = CURRENT_TIMESTAMP
            ''', (key, value_str, value_type))
            conn.commit()
        finally:
            conn.close()

    # --- METADADOS DE TABELAS (CHROMA + SQLITE) ---

    def load_tables(self, limit: int = None) -> List[Dict]:
        """Carrega metadados das tabelas do ChromaDB"""
        collection = self._get_collection("table_metadata")
        results = collection.get()
        
        tables = []
        for i in range(len(results['ids'])):
            metadata = results['metadatas'][i]
            # ChromaDB não suporta tipos complexos em metadados, então decodificamos strings JSON
            tables.append({
                'id': results['ids'][i],
                'table_name': metadata.get('table_name'),
                'table_description': metadata.get('table_description'),
                'schema_info': json.loads(metadata.get('schema_info', '{}')),
                'columns_info': json.loads(metadata.get('columns_info', '[]')),
                'sample_data': json.loads(metadata.get('sample_data', '[]')),
                'record_count': metadata.get('record_count', 0),
                'is_active': metadata.get('is_active', 1),
                'deep_profile': json.loads(metadata.get('deep_profile', '{}')),
                'semantic_context': json.loads(metadata.get('semantic_context', '[]')),
                'export_status': metadata.get('export_status', 'Sucesso')
            })
            
        if limit:
            return tables[:limit]
        return tables

    def find_similar_tables(self, query_text: str, limit: int = 3) -> List[Dict]:
        """Busca tabelas similares usando busca vetorial no ChromaDB"""
        collection = self._get_collection("table_metadata")
        
        results = collection.query(
            query_texts=[query_text],
            n_results=limit
        )
        
        tables = []
        if results['metadatas'] and len(results['metadatas'][0]) > 0:
            for i, meta in enumerate(results['metadatas'][0]):
                # Decodificar campos JSON
                table_data = {
                    'table_name': meta.get('table_name'),
                    'table_description': meta.get('table_description'),
                    'schema_info': json.loads(meta.get('schema_info', '{}')),
                    'columns_info': json.loads(meta.get('columns_info', '[]')),
                    'sample_data': json.loads(meta.get('sample_data', '[]')),
                    'record_count': meta.get('record_count', 0),
                    'is_active': meta.get('is_active', 1),
                    'deep_profile': json.loads(meta.get('deep_profile', '{}')),
                    'semantic_context': json.loads(meta.get('semantic_context', '[]')),
                    'similarity': 1.0 - (results['distances'][0][i] if 'distances' in results else 0.5)
                }
                tables.append(table_data)
        
        return tables

    def save_table_metadata(self, table_name: str, metadata: Dict):
        """Salva metadados no ChromaDB"""
        collection = self._get_collection("table_metadata")
        
        # Preparar metadados para o Chroma (apenas strings, ints, floats, bools)
        chroma_meta = {
            'table_name': table_name,
            'table_description': metadata.get('table_description', ''),
            'schema_info': json.dumps(metadata.get('schema_info', {})),
            'columns_info': json.dumps(metadata.get('columns_info', [])),
            'sample_data': json.dumps(metadata.get('sample_data', [])),
            'record_count': metadata.get('record_count', 0),
            'is_active': 1 if metadata.get('is_active', True) else 0,
            'deep_profile': json.dumps(metadata.get('deep_profile', {})),
            'semantic_context': json.dumps(metadata.get('semantic_context', [])),
            'export_status': metadata.get('export_status', 'Sucesso')
        }
        
        # Usar embedding se fornecido, senão o Chroma gera um básico
        embedding = metadata.get('embedding_vector')
        if embedding:
            # Chroma espera uma lista de floats se passarmos o embedding
            if isinstance(embedding, bytes):
                # Se for binário do Oracle, precisamos converter (isso depende de como foi gerado)
                # Por agora, vamos deixar o Chroma gerar um novo a partir do texto
                pass
        
        text_content = f"{table_name}: {metadata.get('table_description', '')}"
        
        collection.upsert(
            ids=[table_name],
            metadatas=[chroma_meta],
            documents=[text_content]
        )

    def get_table_metadata(self, table_name: str) -> Optional[Dict]:
        """Recupera metadados de uma tabela específica do ChromaDB"""
        collection = self._get_collection("table_metadata")
        result = collection.get(ids=[table_name])
        if result['metadatas']:
            meta = result['metadatas'][0]
            # Decodificar campos JSON
            return {
                'table_name': meta.get('table_name'),
                'table_description': meta.get('table_description'),
                'schema_info': json.loads(meta.get('schema_info', '{}')),
                'columns_info': json.loads(meta.get('columns_info', '[]')),
                'sample_data': json.loads(meta.get('sample_data', '[]')),
                'record_count': meta.get('record_count', 0),
                'is_active': meta.get('is_active', 1),
                'deep_profile': json.loads(meta.get('deep_profile', '{}')),
                'semantic_context': json.loads(meta.get('semantic_context', '[]')),
                'export_status': meta.get('export_status', 'Sucesso')
            }
        return None

    def get_patterns_count(self) -> int:
        """Retorna o total de padrões armazenados no ChromaDB"""
        collection = self._get_collection("behavioral_patterns")
        return collection.count()

    # --- PADRÕES COMPORTAMENTAIS (CHROMA) ---

    def clear_behavioral_patterns(self):
        """Limpa todos os padrões comportamentais do ChromaDB"""
        try:
            self.chroma_client.delete_collection("behavioral_patterns")
            print("Coleção 'behavioral_patterns' removida com sucesso.")
        except Exception as e:
            print(f"Erro ao remover coleção behavioral_patterns: {e}")
        
        # Recriar a coleção vazia
        self._get_collection("behavioral_patterns")

    def clear_table_metadata(self):
        """Limpa todos os metadados de tabelas do ChromaDB"""
        try:
            self.chroma_client.delete_collection("table_metadata")
            print("Coleção 'table_metadata' removida com sucesso.")
        except Exception as e:
            print(f"Erro ao remover coleção table_metadata: {e}")
        
        # Recriar a coleção vazia
        self._get_collection("table_metadata")

    def clear_knowledge_base(self):
        """Limpa toda a base de conhecimento do ChromaDB"""
        try:
            self.chroma_client.delete_collection("knowledge_base")
            print("Coleção 'knowledge_base' removida com sucesso.")
        except Exception as e:
            print(f"Erro ao remover coleção knowledge_base: {e}")
        
        # Recriar a coleção vazia
        self._get_collection("knowledge_base")

    def clear_all_sqlite_data(self):
        """Limpa todas as tabelas do SQLite"""
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        try:
            cursor.execute('DELETE FROM tb_ai_configurations')
            cursor.execute('DELETE FROM tb_ai_process_flows')
            cursor.execute('DELETE FROM tb_ai_chat_history')
            cursor.execute('DELETE FROM tb_ai_contextual_memory')
            conn.commit()
            print("Todas as tabelas SQLite foram limpas.")
        except Exception as e:
            print(f"Erro ao limpar SQLite: {e}")
        finally:
            conn.close()

    def save_behavioral_pattern(self, pattern: Dict):
        """Salva um padrão no ChromaDB para busca semântica instantânea"""
        collection = self._get_collection("behavioral_patterns")
        
        id_val = f"pat_{datetime.now().timestamp()}"
        
        chroma_meta = {
            'situation': pattern['situation'],
            'user_input': pattern['user_input'],
            'ai_action': pattern['ai_action'],
            'ai_response': pattern['ai_response'],
            'success_indicator': pattern.get('success_indicator', 1.0),
            'category': pattern.get('category', 'Geral'),
            'tags': pattern.get('tags', ''),
            'created_at': datetime.now().isoformat()
        }
        
        collection.add(
            ids=[id_val],
            metadatas=[chroma_meta],
            documents=[pattern['user_input']] # Indexamos pelo input do usuário para busca
        )

    def get_behavioral_patterns(self, category: Optional[str] = None, limit: int = None) -> List[Dict]:
        """Recupera padrões do ChromaDB"""
        collection = self._get_collection("behavioral_patterns")
        
        where = None
        if category:
            where = {"category": category}
            
        results = collection.get(where=where, limit=limit)
        
        patterns = []
        for i in range(len(results['ids'])):
            patterns.append(results['metadatas'][i])
        
        return patterns

    def find_similar_patterns(self, user_input: str, limit: int = 3) -> List[Dict]:
        """Busca padrões similares usando busca vetorial (MUITO RÁPIDO)"""
        collection = self._get_collection("behavioral_patterns")
        
        results = collection.query(
            query_texts=[user_input],
            n_results=limit
        )
        
        patterns = []
        if results['metadatas'] and len(results['metadatas'][0]) > 0:
            for i, meta in enumerate(results['metadatas'][0]):
                # Adicionamos o ID para permitir atualizações futuras
                meta['id'] = results['ids'][0][i]
                patterns.append(meta)
        
        return patterns

    def update_pattern_score(self, pattern_id: str, delta: int):
        """Atualiza a pontuação de um padrão no ChromaDB"""
        collection = self._get_collection("behavioral_patterns")
        
        # 1. Buscar o padrão atual
        result = collection.get(ids=[pattern_id])
        if not result['metadatas']:
            return
            
        meta = result['metadatas'][0]
        
        # 2. Atualizar contadores
        if delta > 0:
            meta['success_count'] = meta.get('success_count', 0) + 1
            meta['priority_score'] = meta.get('priority_score', 0.5) + 0.1
        else:
            meta['failure_count'] = meta.get('failure_count', 0) + 1
            meta['priority_score'] = max(0.1, meta.get('priority_score', 0.5) - 0.2)
            
        # 3. Salvar de volta
        collection.update(
            ids=[pattern_id],
            metadatas=[meta]
        )

    # --- BASE DE CONHECIMENTO (CHROMA) ---

    def get_knowledge(self, category: str = None, limit: int = 100) -> List[Dict]:
        """Obtém itens da base de conhecimento do ChromaDB"""
        collection = self._get_collection("knowledge_base")
        
        where = None
        if category:
            where = {"category": category}
            
        results = collection.get(where=where, limit=limit)
        
        knowledge = []
        for i in range(len(results['ids'])):
            item = results['metadatas'][i]
            item['id'] = results['ids'][i]
            knowledge.append(item)
            
        return knowledge
    
    def save_knowledge(self, category: str, title: str, content: str, 
                        tags: str = '', priority: int = 1, embedding_vector: bytes = None):
        """Salva item no ChromaDB"""
        collection = self._get_collection("knowledge_base")
        
        id_val = f"kb_{category}_{title}".replace(" ", "_")
        
        chroma_meta = {
            'category': category,
            'title': title,
            'content': content,
            'tags': tags,
            'priority': priority,
            'updated_at': datetime.now().isoformat()
        }
        
        collection.upsert(
            ids=[id_val],
            metadatas=[chroma_meta],
            documents=[f"{title}: {content}"]
        )

    # --- FLUXOS DE PROCESSO (SQLITE) ---

    def save_process_flow(self, process_name: str, flow_sequence: List[str], timing_insights: List[Dict], description: str = ''):
        """Salva um fluxo no SQLite"""
        conn = self._get_sqlite_conn()
        cursor = conn.cursor()
        try:
            cursor.execute('''
                INSERT INTO tb_ai_process_flows (process_name, flow_sequence, timing_insights, description, updated_at)
                VALUES (?, ?, ?, ?, CURRENT_TIMESTAMP)
                ON CONFLICT(process_name) DO UPDATE SET 
                    flow_sequence = excluded.flow_sequence, 
                    timing_insights = excluded.timing_insights, 
                    description = excluded.description,
                    updated_at = CURRENT_TIMESTAMP
            ''', (process_name, json.dumps(flow_sequence), json.dumps(timing_insights), description))
            conn.commit()
        finally:
            conn.close()

    def get_process_flows(self, limit: int = None) -> List[Dict]:
        """Recupera fluxos do SQLite"""
        conn = self._get_sqlite_conn()
        cursor = conn.cursor()
        try:
            sql = 'SELECT * FROM tb_ai_process_flows WHERE is_active = 1 ORDER BY created_at DESC'
            if limit:
                sql += f' LIMIT {limit}'
            
            cursor.execute(sql)
            return [dict(row) for row in cursor.fetchall()]
        finally:
            conn.close()

    # --- MÉTODOS DE COMPATIBILIDADE ---
    def save_tables(self, tables: List[Dict]):
        """Salva múltiplas tabelas"""
        for table in tables:
            self.save_table_metadata(table['table_name'], table)

    def batch_save_behavioral_patterns(self, patterns: List[Dict]):
        """Salva múltiplos padrões"""
        for p in patterns:
            self.save_behavioral_pattern(p)

# --- INSTÂNCIA GLOBAL E FUNÇÕES DE CONVENIÊNCIA ---
# Mantido para compatibilidade com o código existente

_storage_instance = None

def _get_storage():
    global _storage_instance
    if _storage_instance is None:
        _storage_instance = DataStorage()
    return _storage_instance

def get_config(key: str, default: Any = None) -> Any:
    return _get_storage().get_config(key, default)

def save_config(key: str, value: Any, value_type: str = 'string'):
    return _get_storage().save_config(key, value, value_type)

def load_tables(limit: int = None) -> List[Dict]:
    return _get_storage().load_tables(limit)

def save_tables(tables: List[Dict]):
    return _get_storage().save_tables(tables)

def get_table_metadata(table_name: str) -> Optional[Dict]:
    # ChromaDB get retorna uma lista, pegamos o primeiro se existir
    collection = _get_storage()._get_collection("table_metadata")
    result = collection.get(ids=[table_name])
    if result['metadatas']:
        meta = result['metadatas'][0]
        # Decodificar campos JSON
        return {
            'table_name': meta.get('table_name'),
            'table_description': meta.get('table_description'),
            'schema_info': json.loads(meta.get('schema_info', '{}')),
            'columns_info': json.loads(meta.get('columns_info', '[]')),
            'sample_data': json.loads(meta.get('sample_data', '[]')),
            'record_count': meta.get('record_count', 0),
            'is_active': meta.get('is_active', 1),
            'deep_profile': json.loads(meta.get('deep_profile', '{}')),
            'semantic_context': json.loads(meta.get('semantic_context', '[]')),
            'export_status': meta.get('export_status', 'Sucesso')
        }
    return None

def save_table_metadata(table_name: str, metadata: Dict):
    return _get_storage().save_table_metadata(table_name, metadata)

def get_knowledge(category: str = None, limit: int = 100) -> List[Dict]:
    return _get_storage().get_knowledge(category, limit)

def save_knowledge(category: str, title: str, content: str, tags: str = '', priority: int = 1):
    return _get_storage().save_knowledge(category, title, content, tags, priority)

def export_config():
    """Exporta toda a configuração (Mock para compatibilidade)"""
    return {"status": "success", "message": "Configurações já estão no SQLite local."}

def import_config(config_data):
    """Importa configuração (Mock para compatibilidade)"""
    return {"status": "success", "message": "Importação via SQLite direta."}
