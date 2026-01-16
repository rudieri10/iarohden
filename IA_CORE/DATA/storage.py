"""
STORAGE - Sistema de Armazenamento de Dados
Sistema robusto para gerenciar configurações e metadados
"""

import sqlite3
import json
import os
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Any
from pathlib import Path

class DataStorage:
    """Sistema profissional de armazenamento de dados da IA"""
    
    def __init__(self):
        self.db_path = os.path.join(os.path.dirname(__file__), 'rohden_ai.db')
        self._init_database()
    
    def _init_database(self):
        """Inicializa o banco de dados SQLite"""
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        
        # Tabela de configurações
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS configurations (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                key TEXT UNIQUE NOT NULL,
                value TEXT NOT NULL,
                value_type TEXT DEFAULT 'string',
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        ''')
        
        # Tabela de metadados de tabelas
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS table_metadata (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                table_name TEXT UNIQUE NOT NULL,
                table_description TEXT,
                schema_info TEXT,
                columns_info TEXT,
                sample_data TEXT,
                record_count INTEGER DEFAULT 0,
                is_active BOOLEAN DEFAULT 1,
                deep_profile TEXT,
                semantic_context TEXT, -- Tags e sinônimos para busca semântica
                embedding_vector BLOB, -- Vetor numérico para busca avançada
                export_status TEXT, -- Status da última exportação para o servidor
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        ''')

        # Tabela de fluxos de processos (Sequenciamento entre tabelas)
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS process_flows (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                process_name TEXT UNIQUE NOT NULL,
                flow_sequence TEXT, -- JSON com a ordem das tabelas
                timing_insights TEXT, -- JSON com os intervalos e gaps
                description TEXT,
                is_active BOOLEAN DEFAULT 1,
                embedding_vector BLOB,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        ''')

        # Migração: Verificar se a coluna deep_profile existe
        cursor.execute("PRAGMA table_info(table_metadata)")
        columns = [col[1] for col in cursor.fetchall()]
        if 'deep_profile' not in columns:
            cursor.execute("ALTER TABLE table_metadata ADD COLUMN deep_profile TEXT")
        if 'semantic_context' not in columns:
            cursor.execute("ALTER TABLE table_metadata ADD COLUMN semantic_context TEXT")
        if 'embedding_vector' not in columns:
            cursor.execute("ALTER TABLE table_metadata ADD COLUMN embedding_vector BLOB")
        if 'export_status' not in columns:
            cursor.execute("ALTER TABLE table_metadata ADD COLUMN export_status TEXT")
        
        # Migração para process_flows
        cursor.execute("PRAGMA table_info(process_flows)")
        columns_flows = [col[1] for col in cursor.fetchall()]
        if 'embedding_vector' not in columns_flows:
            cursor.execute("ALTER TABLE process_flows ADD COLUMN embedding_vector BLOB")

        # Migração para knowledge_base
        cursor.execute("PRAGMA table_info(knowledge_base)")
        columns_kb = [col[1] for col in cursor.fetchall()]
        if 'embedding_vector' not in columns_kb:
            cursor.execute("ALTER TABLE knowledge_base ADD COLUMN embedding_vector BLOB")
        
        # Tabela de conhecimento
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS knowledge_base (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                category TEXT NOT NULL,
                title TEXT NOT NULL,
                content TEXT NOT NULL,
                tags TEXT,
                priority INTEGER DEFAULT 1,
                is_active BOOLEAN DEFAULT 1,
                embedding_vector BLOB,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        ''')
        
        # Tabela de logs de alterações
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS change_logs (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                table_name TEXT,
                action TEXT,
                old_value TEXT,
                new_value TEXT,
                user_name TEXT,
                timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        ''')
        
        conn.commit()
        conn.close()
    
    def get_config(self, key: str, default: Any = None) -> Any:
        """Obtém uma configuração do banco"""
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        
        cursor.execute('SELECT value, value_type FROM configurations WHERE key = ?', (key,))
        result = cursor.fetchone()
        
        conn.close()
        
        if not result:
            return default
        
        value, value_type = result
        
        # Converter para o tipo correto
        if value_type == 'json':
            return json.loads(value)
        elif value_type == 'int':
            return int(value)
        elif value_type == 'float':
            return float(value)
        elif value_type == 'bool':
            return value.lower() == 'true'
        else:
            return value
    
    def save_config(self, key: str, value: Any, value_type: str = 'string'):
        """Salva uma configuração no banco"""
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        
        # Converter valor para string
        if value_type == 'json':
            value_str = json.dumps(value)
        else:
            value_str = str(value)
        
        # Inserir ou atualizar
        cursor.execute('''
            INSERT OR REPLACE INTO configurations (key, value, value_type, updated_at)
            VALUES (?, ?, ?, CURRENT_TIMESTAMP)
        ''', (key, value_str, value_type))
        
        conn.commit()
        conn.close()
    
    def load_tables(self) -> List[Dict]:
        """Carrega todas as tabelas configuradas"""
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        
        cursor.execute('''
            SELECT * FROM table_metadata WHERE is_active = 1
            ORDER BY table_name
        ''')
        
        # Obter nomes das colunas dinamicamente
        col_names = [description[0] for description in cursor.description]
        
        tables = []
        for row in cursor.fetchall():
            row_dict = dict(zip(col_names, row))
            tables.append({
                'id': row_dict.get('id'),
                'table_name': row_dict.get('table_name'),
                'table_description': row_dict.get('table_description'),
                'schema_info': json.loads(row_dict.get('schema_info')) if row_dict.get('schema_info') else {},
                'columns_info': json.loads(row_dict.get('columns_info')) if row_dict.get('columns_info') else [],
                'sample_data': json.loads(row_dict.get('sample_data')) if row_dict.get('sample_data') else [],
                'record_count': row_dict.get('record_count', 0),
                'is_active': row_dict.get('is_active', 1),
                'deep_profile': json.loads(row_dict.get('deep_profile')) if row_dict.get('deep_profile') else {},
                'semantic_context': json.loads(row_dict.get('semantic_context')) if row_dict.get('semantic_context') else [],
                'embedding_vector': row_dict.get('embedding_vector'), # BLOB direto
                'export_status': row_dict.get('export_status', 'Sucesso'),
                'created_at': row_dict.get('created_at'),
                'updated_at': row_dict.get('updated_at')
            })
        
        conn.close()
        return tables
    
    def save_tables(self, tables: List[Dict]):
        """Salva a configuração das tabelas"""
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        
        for table in tables:
            cursor.execute('''
                INSERT OR REPLACE INTO table_metadata 
                (table_name, table_description, schema_info, columns_info, 
                 sample_data, record_count, is_active, deep_profile, semantic_context, embedding_vector, export_status, updated_at)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, CURRENT_TIMESTAMP)
            ''', (
                table['table_name'],
                table.get('table_description', ''),
                json.dumps(table.get('schema_info', {})),
                json.dumps(table.get('columns_info', [])),
                json.dumps(table.get('sample_data', [])),
                table.get('record_count', 0),
                table.get('is_active', True),
                json.dumps(table.get('deep_profile', {})) if isinstance(table.get('deep_profile'), (dict, list)) else table.get('deep_profile'),
                json.dumps(table.get('semantic_context', [])) if isinstance(table.get('semantic_context'), (list, dict)) else table.get('semantic_context'),
                table.get('embedding_vector'), # Espera um BLOB (bytes)
                table.get('export_status', 'Sucesso')
            ))
        
        conn.commit()
        conn.close()
    
    def get_table_metadata(self, table_name: str) -> Optional[Dict]:
        """Obtém metadados de uma tabela específica"""
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        
        cursor.execute('''
            SELECT * FROM table_metadata 
            WHERE table_name = ? AND is_active = 1
        ''', (table_name,))
        
        result = cursor.fetchone()
        
        if not result:
            conn.close()
            return None

        # Obter nomes das colunas dinamicamente
        col_names = [description[0] for description in cursor.description]
        row_dict = dict(zip(col_names, result))
        conn.close()
        
        return {
            'id': row_dict.get('id'),
            'table_name': row_dict.get('table_name'),
            'table_description': row_dict.get('table_description'),
            'schema_info': json.loads(row_dict.get('schema_info')) if row_dict.get('schema_info') else {},
            'columns_info': json.loads(row_dict.get('columns_info')) if row_dict.get('columns_info') else [],
            'sample_data': json.loads(row_dict.get('sample_data')) if row_dict.get('sample_data') else [],
            'record_count': row_dict.get('record_count', 0),
            'is_active': row_dict.get('is_active', 1),
            'deep_profile': json.loads(row_dict.get('deep_profile')) if row_dict.get('deep_profile') else {},
            'semantic_context': json.loads(row_dict.get('semantic_context')) if row_dict.get('semantic_context') else [],
            'embedding_vector': row_dict.get('embedding_vector'),
            'export_status': row_dict.get('export_status', 'Pendente'),
            'created_at': row_dict.get('created_at'),
            'updated_at': row_dict.get('updated_at')
        }
    
    def save_table_metadata(self, table_name: str, metadata: Dict):
        """Salva metadados de uma tabela"""
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        
        cursor.execute('''
            INSERT OR REPLACE INTO table_metadata 
            (table_name, table_description, schema_info, columns_info, 
             sample_data, record_count, is_active, deep_profile, updated_at)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, CURRENT_TIMESTAMP)
        ''', (
            table_name,
            metadata.get('table_description', ''),
            json.dumps(metadata.get('schema_info', {})),
            json.dumps(metadata.get('columns_info', [])),
            json.dumps(metadata.get('sample_data', [])),
            metadata.get('record_count', 0),
            metadata.get('is_active', True),
            json.dumps(metadata.get('deep_profile', {})) if isinstance(metadata.get('deep_profile'), (dict, list)) else metadata.get('deep_profile'),
        ))
        
        conn.commit()
        conn.close()
    
    def get_knowledge(self, category: str = None, limit: int = 100) -> List[Dict]:
        """Obtém itens da base de conhecimento"""
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        
        if category:
            cursor.execute('''
                SELECT * FROM knowledge_base 
                WHERE category = ? AND is_active = 1
                ORDER BY priority DESC, created_at DESC
                LIMIT ?
            ''', (category, limit))
        else:
            cursor.execute('''
                SELECT * FROM knowledge_base 
                WHERE is_active = 1
                ORDER BY priority DESC, created_at DESC
                LIMIT ?
            ''', (limit,))
        
        knowledge = []
        for row in cursor.fetchall():
            knowledge.append({
                'id': row[0],
                'category': row[1],
                'title': row[2],
                'content': row[3],
                'tags': row[4],
                'priority': row[5],
                'is_active': row[6],
                'created_at': row[7],
                'updated_at': row[8]
            })
        
        conn.close()
        return knowledge
    
    def save_knowledge(self, category: str, title: str, content: str, 
                        tags: str = '', priority: int = 1, embedding_vector: bytes = None):
        """Salva um item na base de conhecimento com suporte opcional a vetores"""
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        
        cursor.execute('''
            INSERT INTO knowledge_base 
            (category, title, content, tags, priority, embedding_vector, updated_at)
            VALUES (?, ?, ?, ?, ?, ?, CURRENT_TIMESTAMP)
        ''', (category, title, content, tags, priority, embedding_vector))
        
        conn.commit()
        conn.close()
    
    def save_process_flow(self, process_name: str, flow_sequence: List[str], timing_insights: List[Dict], description: str = ''):
        """Salva ou atualiza um fluxo de processo descoberto"""
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        
        cursor.execute('''
            INSERT OR REPLACE INTO process_flows 
            (process_name, flow_sequence, timing_insights, description, updated_at)
            VALUES (?, ?, ?, ?, CURRENT_TIMESTAMP)
        ''', (
            process_name,
            json.dumps(flow_sequence),
            json.dumps(timing_insights),
            description
        ))
        
        conn.commit()
        conn.close()

    def get_process_flows(self) -> List[Dict]:
        """Recupera todos os fluxos de processos ativos"""
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        
        cursor.execute('SELECT * FROM process_flows WHERE is_active = 1')
        col_names = [description[0] for description in cursor.description]
        
        flows = []
        for row in cursor.fetchall():
            row_dict = dict(zip(col_names, row))
            flows.append({
                'id': row_dict['id'],
                'process_name': row_dict['process_name'],
                'flow_sequence': json.loads(row_dict['flow_sequence']) if row_dict['flow_sequence'] else [],
                'timing_insights': json.loads(row_dict['timing_insights']) if row_dict['timing_insights'] else [],
                'description': row_dict['description'],
                'created_at': row_dict['created_at'],
                'updated_at': row_dict['updated_at']
            })
            
        conn.close()
        return flows

    def export_config(self) -> Dict:
        """Exporta toda a configuração"""
        return {
            'tables': self.load_tables(),
            'knowledge': self.get_knowledge(),
            'metadata': {
                'export_date': datetime.now().isoformat(),
                'version': '1.0'
            }
        }
    
    def import_config(self, config: Dict):
        """Importa configuração de um dicionário"""
        if 'tables' in config:
            self.save_tables(config['tables'])
        
        if 'knowledge' in config:
            for item in config['knowledge']:
                self.save_knowledge(
                    item.get('category', 'general'),
                    item.get('title', ''),
                    item.get('content', ''),
                    item.get('tags', ''),
                    item.get('priority', 1)
                )

# Instância global do sistema de armazenamento
storage = DataStorage()

# Funções de conveniência
def get_config(key: str, default: Any = None) -> Any:
    return storage.get_config(key, default)

def save_config(key: str, value: Any, value_type: str = 'string'):
    storage.save_config(key, value, value_type)

def load_tables() -> List[Dict]:
    return storage.load_tables()

def save_tables(tables: List[Dict]):
    storage.save_tables(tables)

def get_table_metadata(table_name: str) -> Optional[Dict]:
    return storage.get_table_metadata(table_name)

def save_table_metadata(table_name: str, metadata: Dict):
    storage.save_table_metadata(table_name, metadata)

def get_knowledge(category: str = None, limit: int = 100) -> List[Dict]:
    return storage.get_knowledge(category, limit)

def save_knowledge(category: str, title: str, content: str, 
                    tags: str = '', priority: int = 1):
    storage.save_knowledge(category, title, content, tags, priority)

def export_config() -> Dict:
    return storage.export_config()

def import_config(config: Dict):
    storage.import_config(config)
