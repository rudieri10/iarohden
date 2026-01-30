"""
STORAGE - Sistema de Armazenamento Local (ChromaDB + SQLite)
Substitui o Oracle para máxima velocidade e eficiência.
"""

import json
import os
import sqlite3
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Optional, Generator

import chromadb
from chromadb.api.types import Documents, Embeddings, EmbeddingFunction


def _resolve_rohden_ai_data_dir() -> Path:
    return Path(r"C:\ia\banco_ia")


db_path = _resolve_rohden_ai_data_dir() / "ai_storage.db"
chroma_path = str(_resolve_rohden_ai_data_dir() / "chroma_db")


class RohdenEmbeddingFunction(EmbeddingFunction):
    """Função de embedding customizada que usa o servidor Rohden AI via VectorManager"""

    def __init__(self):
        from ..ENGINE.vector_manager import VectorManager

        self.vm = VectorManager()
        self._dim = None

    def __call__(self, input: Documents) -> Embeddings:
        embeddings = []
        for text in input:
            vector = self.vm.generate_embedding(text)
            if vector:
                if self._dim is None:
                    self._dim = len(vector)
                embeddings.append(vector)
            else:
                if self._dim is None:
                    self._dim = self.vm.get_embedding_dimension()
                embeddings.append([0.0] * self._dim)
        return embeddings


class DataStorage:
    """Sistema profissional de armazenamento local usando ChromaDB (Vetores) e SQLite (Relacional)"""

    def __init__(self):
        """Inicializa os bancos de dados locais"""
        self.base_path = _resolve_rohden_ai_data_dir()
        self.db_path = self.base_path / "ai_storage.db"
        self.chroma_path = str(self.base_path / "chroma_db")

        os.makedirs(self.base_path, exist_ok=True)

        self.embedding_fn = RohdenEmbeddingFunction()
        self.chroma_client = chromadb.PersistentClient(path=self.chroma_path)

        self._init_sqlite()

    def _get_collection(self, name: str):
        """Obtém ou cria uma coleção com a função de embedding correta"""
        try:
            return self.chroma_client.get_or_create_collection(
                name=name,
                embedding_function=self.embedding_fn,
            )
        except Exception as e:
            if "dimension" in str(e).lower() or "expecting" in str(e).lower():
                print(f"⚠️ Erro de dimensão detectado na coleção '{name}'. Recriando para novo modelo...")
                try:
                    self.chroma_client.delete_collection(name)
                    return self.chroma_client.get_or_create_collection(
                        name=name,
                        embedding_function=self.embedding_fn,
                    )
                except Exception:
                    pass
            raise

    def _init_sqlite(self):
        """Cria as tabelas do SQLite se não existirem"""
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()

        cursor.execute(
            '''
            CREATE TABLE IF NOT EXISTS tb_ai_configurations (
                key TEXT PRIMARY KEY,
                value TEXT,
                value_type TEXT,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        '''
        )

        cursor.execute(
            '''
            CREATE TABLE IF NOT EXISTS tb_ai_process_flows (
                process_name TEXT PRIMARY KEY,
                flow_sequence TEXT,
                timing_insights TEXT,
                description TEXT,
                is_active INTEGER DEFAULT 1,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        '''
        )

        cursor.execute(
            '''
            CREATE TABLE IF NOT EXISTS tb_ai_chat_history (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                chat_id TEXT,
                role TEXT,
                content TEXT,
                tokens INTEGER,
                model TEXT,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        '''
        )

        cursor.execute(
            '''
            CREATE TABLE IF NOT EXISTS tb_ai_contextual_memory (
                key TEXT PRIMARY KEY,
                value TEXT,
                expiration TIMESTAMP,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        '''
        )

        conn.commit()
        conn.close()

    def _get_sqlite_conn(self):
        """Retorna uma conexão com o SQLite que retorna dicionários"""
        conn = sqlite3.connect(self.db_path)
        conn.row_factory = sqlite3.Row
        return conn

    def get_config(self, key: str, default: Any = None) -> Any:
        """Obtém uma configuração do SQLite"""
        conn = self._get_sqlite_conn()
        cursor = conn.cursor()
        try:
            cursor.execute("SELECT value, value_type FROM tb_ai_configurations WHERE key = ?", (key,))
            result = cursor.fetchone()

            if not result:
                return default

            value, value_type = result["value"], result["value_type"]

            if value_type == "json":
                return json.loads(value)
            if value_type == "int":
                return int(value)
            if value_type == "float":
                return float(value)
            if value_type == "bool":
                return str(value).lower() == "true"
            return value
        finally:
            conn.close()

    def save_config(self, key: str, value: Any, value_type: str = "string"):
        """Salva uma configuração no SQLite"""
        conn = self._get_sqlite_conn()
        cursor = conn.cursor()
        try:
            value_str = json.dumps(value) if value_type == "json" else str(value)

            cursor.execute(
                '''
                INSERT INTO tb_ai_configurations (key, value, value_type, updated_at)
                VALUES (?, ?, ?, CURRENT_TIMESTAMP)
                ON CONFLICT(key) DO UPDATE SET
                    value = excluded.value,
                    value_type = excluded.value_type,
                    updated_at = CURRENT_TIMESTAMP
            ''',
                (key, value_str, value_type),
            )
            conn.commit()
        finally:
            conn.close()

    def load_tables(self, limit: int = None, include_embeddings: bool = False) -> List[Dict]:
        collection = self._get_collection("table_metadata")

        include = ["metadatas"]
        if include_embeddings:
            include.append("embeddings")

        results = collection.get(include=include)

        tables = []
        for i in range(len(results["ids"])):
            metadata = results["metadatas"][i]
            table_data = {
                "id": results["ids"][i],
                "table_name": metadata.get("table_name"),
                "table_description": metadata.get("table_description"),
                "schema_info": json.loads(metadata.get("schema_info", "{}")),
                "columns_info": json.loads(metadata.get("columns_info", "[]")),
                "sample_data": json.loads(metadata.get("sample_data", "[]")),
                "record_count": metadata.get("record_count", 0),
                "is_active": metadata.get("is_active", 1),
                "deep_profile": json.loads(metadata.get("deep_profile", "{}")),
                "semantic_context": json.loads(metadata.get("semantic_context", "[]")),
                "validated_rules": json.loads(metadata.get("validated_rules", "[]")),
                "export_status": metadata.get("export_status", "Sucesso"),
                "updated_at": metadata.get("updated_at"),
                "has_vector": False,
            }

            if include_embeddings and results.get("embeddings") is not None and i < len(results["embeddings"]) and results["embeddings"][i] is not None:
                try:
                    table_data["has_vector"] = len(results["embeddings"][i]) > 0
                except (TypeError, ValueError):
                    table_data["has_vector"] = False
            elif not include_embeddings:
                table_data["has_vector"] = "deep_profile" in table_data and len(table_data["deep_profile"]) > 0

            tables.append(table_data)

        if limit:
            return tables[:limit]
        return tables

    def find_similar_tables(self, query_text: str, limit: int = 3) -> List[Dict]:
        collection = self._get_collection("table_metadata")

        results = collection.query(query_texts=[query_text], n_results=limit)

        tables = []
        if results["metadatas"] and len(results["metadatas"][0]) > 0:
            for i, meta in enumerate(results["metadatas"][0]):
                table_data = {
                    "table_name": meta.get("table_name"),
                    "table_description": meta.get("table_description"),
                    "schema_info": json.loads(meta.get("schema_info", "{}")),
                    "columns_info": json.loads(meta.get("columns_info", "[]")),
                    "sample_data": json.loads(meta.get("sample_data", "[]")),
                    "record_count": meta.get("record_count", 0),
                    "is_active": meta.get("is_active", 1),
                    "deep_profile": json.loads(meta.get("deep_profile", "{}")),
                    "semantic_context": json.loads(meta.get("semantic_context", "[]")),
                    "similarity": 1.0 - (results["distances"][0][i] if "distances" in results else 0.5),
                }
                tables.append(table_data)

        return tables

    def save_table_metadata(self, table_name: str, metadata: Dict, retry_count: int = 0):
        try:
            collection = self._get_collection("table_metadata")

            chroma_meta = {
                "table_name": table_name,
                "table_description": metadata.get("table_description", ""),
                "schema_info": json.dumps(metadata.get("schema_info", {})),
                "columns_info": json.dumps(metadata.get("columns_info", [])),
                "sample_data": json.dumps(metadata.get("sample_data", [])),
                "record_count": metadata.get("record_count", 0),
                "is_active": 1 if metadata.get("is_active", True) else 0,
                "deep_profile": json.dumps(metadata.get("deep_profile", {})),
                "semantic_context": json.dumps(metadata.get("semantic_context", [])),
                "validated_rules": json.dumps(metadata.get("validated_rules", [])),
                "export_status": metadata.get("export_status", "Sucesso"),
                "updated_at": datetime.now().isoformat(),
            }

            embedding_list = None
            embedding = metadata.get("embedding_vector")
            if embedding:
                if isinstance(embedding, bytes) and len(embedding) > 0:
                    from ..ENGINE.vector_manager import VectorManager

                    vm = VectorManager()
                    embedding_list = vm.blob_to_vector(embedding)
                elif isinstance(embedding, list):
                    embedding_list = embedding

            text_content = f"Tabela: {table_name}\nDescrição: {metadata.get('table_description', '')}"

            upsert_args = {
                "ids": [f"meta_{table_name}"],
                "metadatas": [chroma_meta],
                "documents": [text_content],
            }

            if embedding_list:
                upsert_args["embeddings"] = [embedding_list]

            collection.upsert(**upsert_args)
        except Exception as e:
            if ("dimension" in str(e).lower() or "expecting" in str(e).lower()) and retry_count < 1:
                print(f"⚠️ Erro de dimensão no Upsert para {table_name}. Resetando coleção 'table_metadata'...")
                try:
                    self.chroma_client.delete_collection("table_metadata")
                    return self.save_table_metadata(table_name, metadata, retry_count + 1)
                except Exception:
                    pass
            raise

    def get_table_metadata(self, table_name: str) -> Optional[Dict]:
        collection = self._get_collection("table_metadata")
        result = collection.get(ids=[table_name], include=["metadatas", "embeddings"])
        if result["metadatas"]:
            meta = result["metadatas"][0]
            table_data = {
                "table_name": meta.get("table_name"),
                "table_description": meta.get("table_description"),
                "schema_info": json.loads(meta.get("schema_info", "{}")),
                "columns_info": json.loads(meta.get("columns_info", "[]")),
                "sample_data": json.loads(meta.get("sample_data", "[]")),
                "record_count": meta.get("record_count", 0),
                "is_active": meta.get("is_active", 1),
                "deep_profile": json.loads(meta.get("deep_profile", "{}")),
                "semantic_context": json.loads(meta.get("semantic_context", "[]")),
                "validated_rules": json.loads(meta.get("validated_rules", "[]")),
                "export_status": meta.get("export_status", "Sucesso"),
                "updated_at": meta.get("updated_at"),
                "has_vector": False,
            }

            if result.get("embeddings") is not None and len(result["embeddings"]) > 0 and result["embeddings"][0] is not None:
                try:
                    table_data["has_vector"] = len(result["embeddings"][0]) > 0
                    from ..ENGINE.vector_manager import VectorManager

                    vm = VectorManager()
                    table_data["embedding_vector"] = vm.vector_to_blob(result["embeddings"][0])
                except (TypeError, ValueError):
                    table_data["has_vector"] = False

            return table_data
        return None

    def get_patterns_count(self) -> int:
        collection = self._get_collection("behavioral_patterns")
        return collection.count()

    def clear_behavioral_patterns(self):
        try:
            self.chroma_client.delete_collection("behavioral_patterns")
            print("Coleção 'behavioral_patterns' removida com sucesso.")
        except Exception as e:
            print(f"Erro ao remover coleção behavioral_patterns: {e}")

        self._get_collection("behavioral_patterns")

    def clear_table_metadata(self):
        try:
            self.chroma_client.delete_collection("table_metadata")
            print("Coleção 'table_metadata' removida com sucesso.")
        except Exception as e:
            print(f"Erro ao remover coleção table_metadata: {e}")

        self._get_collection("table_metadata")

    def clear_knowledge_base(self):
        try:
            self.chroma_client.delete_collection("knowledge_base")
            print("Coleção 'knowledge_base' removida com sucesso.")
        except Exception as e:
            print(f"Erro ao remover coleção knowledge_base: {e}")

        self._get_collection("knowledge_base")

    def clear_all_sqlite_data(self):
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        try:
            cursor.execute("DELETE FROM tb_ai_configurations")
            cursor.execute("DELETE FROM tb_ai_process_flows")
            cursor.execute("DELETE FROM tb_ai_chat_history")
            cursor.execute("DELETE FROM tb_ai_contextual_memory")
            conn.commit()
            print("Todas as tabelas SQLite foram limpas.")
        except Exception as e:
            print(f"Erro ao limpar SQLite: {e}")
        finally:
            conn.close()

    def save_behavioral_pattern(self, pattern: Dict, retry_count: int = 0):
        try:
            collection = self._get_collection("behavioral_patterns")

            id_val = f"pat_{datetime.now().timestamp()}"

            chroma_meta = {
                "situation": pattern["situation"],
                "user_input": pattern["user_input"],
                "ai_action": pattern["ai_action"],
                "ai_response": pattern["ai_response"],
                "success_indicator": pattern.get("success_indicator", 1.0),
                "category": pattern.get("category", "Geral"),
                "tags": pattern.get("tags", ""),
                "created_at": datetime.now().isoformat(),
            }

            text_content = f"SITUAÇÃO: {pattern['situation']}\nINPUT: {pattern['user_input']}\nAÇÃO: {pattern['ai_action']}"

            collection.add(ids=[id_val], metadatas=[chroma_meta], documents=[text_content])
        except Exception as e:
            if ("dimension" in str(e).lower() or "expecting" in str(e).lower()) and retry_count < 1:
                print("⚠️ Erro de dimensão no Add Pattern. Resetando coleção 'behavioral_patterns'...")
                try:
                    self.chroma_client.delete_collection("behavioral_patterns")
                    return self.save_behavioral_pattern(pattern, retry_count + 1)
                except Exception:
                    pass
            raise

    def get_behavioral_patterns(self, category: Optional[str] = None, limit: int = None) -> List[Dict]:
        collection = self._get_collection("behavioral_patterns")

        where = {"category": category} if category else None
        results = collection.get(where=where, limit=limit)

        patterns = []
        for i in range(len(results["ids"])):
            patterns.append(results["metadatas"][i])

        return patterns

    def find_similar_patterns(self, user_input: str, limit: int = 3) -> List[Dict]:
        collection = self._get_collection("behavioral_patterns")

        results = collection.query(query_texts=[user_input], n_results=limit)

        patterns = []
        if results["metadatas"] and len(results["metadatas"][0]) > 0:
            for i, meta in enumerate(results["metadatas"][0]):
                meta["id"] = results["ids"][0][i]
                patterns.append(meta)

        return patterns

    def update_pattern_score(self, pattern_id: str, delta: int):
        collection = self._get_collection("behavioral_patterns")

        result = collection.get(ids=[pattern_id])
        if not result["metadatas"]:
            return

        meta = result["metadatas"][0]

        if delta > 0:
            meta["success_count"] = meta.get("success_count", 0) + 1
            meta["priority_score"] = meta.get("priority_score", 0.5) + 0.1
        else:
            meta["failure_count"] = meta.get("failure_count", 0) + 1
            meta["priority_score"] = max(0.1, meta.get("priority_score", 0.5) - 0.2)

        collection.update(ids=[pattern_id], metadatas=[meta])

    def get_knowledge(self, category: str = None, limit: int = 100) -> List[Dict]:
        collection = self._get_collection("knowledge_base")

        where = {"category": category} if category else None
        results = collection.get(where=where, limit=limit)

        knowledge = []
        for i in range(len(results["ids"])):
            item = results["metadatas"][i]
            item["id"] = results["ids"][i]
            knowledge.append(item)

        return knowledge

    def find_similar_knowledge(self, query_text: str, category: str = None, limit: int = 5) -> List[Dict]:
        collection = self._get_collection("knowledge_base")
        
        where = {"category": category} if category else None
        
        results = collection.query(
            query_texts=[query_text], 
            n_results=limit,
            where=where
        )
        
        knowledge = []
        if results["metadatas"] and len(results["metadatas"][0]) > 0:
            for i, meta in enumerate(results["metadatas"][0]):
                meta["id"] = results["ids"][0][i]
                meta["similarity"] = 1.0 - (results["distances"][0][i] if "distances" in results else 0.5)
                knowledge.append(meta)
                
        return knowledge

    def save_knowledge(self, category: str, title: str, content: str, tags: str = "", priority: int = 1, embedding_vector: bytes = None, metadata: Dict = None, retry_count: int = 0):
        try:
            collection = self._get_collection("knowledge_base")

            id_val = f"kb_{category}_{title}".replace(" ", "_")
            
            # Preparar metadados base
            knowledge_metadata = {
                "category": category,
                "title": title,
                "content": content,
                "tags": tags,
                "priority": priority,
                "created_at": datetime.now().isoformat()
            }
            
            # Adicionar metadados adicionais se fornecidos
            if metadata:
                # Achatar metadados para ChromaDB (não aceita dicts aninhados)
                for key, value in metadata.items():
                    if isinstance(value, dict):
                        # Se for dict, converter para string JSON
                        knowledge_metadata[f"{key}_json"] = json.dumps(value, ensure_ascii=False)
                    elif isinstance(value, list):
                        # Se for list, converter para string JSON
                        knowledge_metadata[f"{key}_json"] = json.dumps(value, ensure_ascii=False)
                    else:
                        knowledge_metadata[key] = value

            embedding_list = None
            if embedding_vector:
                if isinstance(embedding_vector, bytes) and len(embedding_vector) > 0:
                    from ..ENGINE.vector_manager import VectorManager
                    vm = VectorManager()
                    embedding_list = vm.blob_to_vector(embedding_vector)
                elif isinstance(embedding_vector, list):
                    embedding_list = embedding_vector

            upsert_args = {
                "ids": [id_val],
                "metadatas": [knowledge_metadata],
                "documents": [content],
            }
            
            if embedding_list:
                upsert_args["embeddings"] = [embedding_list]
                
            collection.upsert(**upsert_args)
            
        except Exception as e:
            if ("dimension" in str(e).lower() or "expecting" in str(e).lower()) and retry_count < 1:
                print("⚠️ Erro de dimensão no Save Knowledge. Resetando coleção 'knowledge_base'...")
                try:
                    self.chroma_client.delete_collection("knowledge_base")
                    return self.save_knowledge(category, title, content, tags, priority, embedding_vector, metadata, retry_count + 1)
                except Exception:
                    pass
            raise

    def save_process_flow(self, process_name: str, flow_sequence: List[str], timing_insights: List[Dict], description: str = ""):
        conn = self._get_sqlite_conn()
        cursor = conn.cursor()
        try:
            cursor.execute(
                '''
                INSERT INTO tb_ai_process_flows (process_name, flow_sequence, timing_insights, description, updated_at)
                VALUES (?, ?, ?, ?, CURRENT_TIMESTAMP)
                ON CONFLICT(process_name) DO UPDATE SET
                    flow_sequence = excluded.flow_sequence,
                    timing_insights = excluded.timing_insights,
                    description = excluded.description,
                    updated_at = CURRENT_TIMESTAMP
            ''',
                (process_name, json.dumps(flow_sequence), json.dumps(timing_insights), description),
            )
            conn.commit()
        finally:
            conn.close()

    def get_process_flows(self, limit: int = None) -> List[Dict]:
        conn = self._get_sqlite_conn()
        cursor = conn.cursor()
        try:
            sql = "SELECT * FROM tb_ai_process_flows WHERE is_active = 1 ORDER BY created_at DESC"
            if limit:
                sql += f" LIMIT {limit}"

            cursor.execute(sql)
            return [dict(row) for row in cursor.fetchall()]
        finally:
            conn.close()

    def save_tables(self, tables: List[Dict]):
        for table in tables:
            self.save_table_metadata(table["table_name"], table)

    def batch_save_behavioral_patterns(self, patterns: List[Dict]):
        for p in patterns:
            self.save_behavioral_pattern(p)


_storage_instance = None


def _get_storage():
    global _storage_instance
    if _storage_instance is None:
        _storage_instance = DataStorage()
    return _storage_instance


def get_config(key: str, default: Any = None) -> Any:
    return _get_storage().get_config(key, default)


def save_config(key: str, value: Any, value_type: str = "string"):
    return _get_storage().save_config(key, value, value_type)


def load_tables(limit: int = None) -> List[Dict]:
    return _get_storage().load_tables(limit)


def save_tables(tables: List[Dict]):
    return _get_storage().save_tables(tables)


def get_table_metadata(table_name: str) -> Optional[Dict]:
    return _get_storage().get_table_metadata(table_name)


def save_table_metadata(table_name: str, metadata: Dict):
    return _get_storage().save_table_metadata(table_name, metadata)


def get_knowledge(category: str = None, limit: int = 100) -> List[Dict]:
    return _get_storage().get_knowledge(category, limit)


def save_knowledge(category: str, title: str, content: str, tags: str = "", priority: int = 1, embedding_vector: bytes = None, metadata: Dict = None):
    return _get_storage().save_knowledge(category, title, content, tags, priority, embedding_vector, metadata)


def export_config():
    return {"status": "success", "message": "Configurações já estão no SQLite local."}


def import_config(config_data):
    return {"status": "success", "message": "Importação via SQLite direta."}
