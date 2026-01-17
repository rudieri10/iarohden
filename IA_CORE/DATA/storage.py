"""
STORAGE - Sistema de Armazenamento de Dados
Sistema robusto para gerenciar configurações e metadados
"""

import json
import os
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Any
from pathlib import Path

# Importar conexão Oracle do projeto
try:
    from conecxaodb import get_connection
except ImportError:
    # Caso esteja rodando fora do contexto principal, tenta o caminho relativo
    import sys
    sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..', '..')))
    from conecxaodb import get_connection

class DataStorage:
    """Sistema profissional de armazenamento de dados da IA (MIGRAÇÃO ORACLE)"""
    
    def __init__(self):
        """Inicializa o sistema de armazenamento Oracle"""
        pass
    
    def _get_conn(self):
        """Obtém uma conexão com o Oracle"""
        return get_connection()

    def get_config(self, key: str, default: Any = None) -> Any:
        """Obtém uma configuração do banco Oracle"""
        conn = self._get_conn()
        if not conn: return default
        
        cursor = conn.cursor()
        try:
            cursor.execute('SELECT VALUE, VALUE_TYPE FROM SYSROH.TB_AI_CONFIGURATIONS WHERE KEY = :key', {'key': key})
            result = cursor.fetchone()
            
            if not result:
                return default
            
            value, value_type = result
            # Oracle CLOB pode vir como um objeto, precisamos ler se necessário
            if hasattr(value, 'read'):
                value = value.read()
            
            # Converter para o tipo correto
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
            cursor.close()
            conn.close()
    
    def save_config(self, key: str, value: Any, value_type: str = 'string'):
        """Salva uma configuração no banco Oracle"""
        conn = self._get_conn()
        if not conn: return
        
        cursor = conn.cursor()
        try:
            # Converter valor para string
            if value_type == 'json':
                value_str = json.dumps(value)
            else:
                value_str = str(value)
            
            # No Oracle usamos MERGE para "INSERT OR REPLACE"
            sql = """
                MERGE INTO SYSROH.TB_AI_CONFIGURATIONS t
                USING (SELECT :key as k FROM dual) s
                ON (t.KEY = s.k)
                WHEN MATCHED THEN
                    UPDATE SET VALUE = :val, VALUE_TYPE = :vtype, UPDATED_AT = CURRENT_TIMESTAMP
                WHEN NOT MATCHED THEN
                    INSERT (KEY, VALUE, VALUE_TYPE) VALUES (:key, :val, :vtype)
            """
            cursor.execute(sql, {'key': key, 'val': value_str, 'vtype': value_type})
            conn.commit()
        finally:
            cursor.close()
            conn.close()
    
    def load_tables(self) -> List[Dict]:
        """Carrega todas as tabelas configuradas do Oracle"""
        conn = self._get_conn()
        if not conn: return []
        
        cursor = conn.cursor()
        try:
            cursor.execute('''
                SELECT * FROM SYSROH.TB_AI_TABLE_METADATA 
                WHERE IS_ACTIVE = 1
                ORDER BY TABLE_NAME
            ''')
            
            col_names = [description[0].lower() for description in cursor.description]
            tables = []
            
            for row in cursor.fetchall():
                row_dict = dict(zip(col_names, row))
                
                # Ler CLOBs se necessário
                for k, v in row_dict.items():
                    if hasattr(v, 'read'):
                        row_dict[k] = v.read()
                
                tables.append({
                    'id': row_dict.get('id'),
                    'table_name': row_dict.get('table_name'),
                    'table_description': row_dict.get('table_description'),
                    'schema_info': json.loads(row_dict.get('schema_info')) if row_dict.get('schema_info') else {},
                    'columns_info': json.loads(row_dict.get('columns_info')) if row_dict.get('columns_info') else [],
                    'sample_data': json.loads(row_dict.get('sample_data')) if row_dict.get('sample_data') else [],
                    'record_count': row_dict.get('record_count', 0),
                    'is_active': row_dict.get('is_active', 1),
                    'deep_profile': json.loads(row_dict.get('deep_profile')) if row_dict.get('deep_profile') and row_dict.get('deep_profile').startswith('{') else row_dict.get('deep_profile'),
                    'semantic_context': json.loads(row_dict.get('semantic_context')) if row_dict.get('semantic_context') and row_dict.get('semantic_context').startswith('[') else row_dict.get('semantic_context'),
                    'embedding_vector': row_dict.get('embedding_vector'), # BLOB Oracle
                    'export_status': row_dict.get('export_status', 'Sucesso'),
                    'created_at': row_dict.get('created_at'),
                    'updated_at': row_dict.get('updated_at')
                })
            return tables
        finally:
            cursor.close()
            conn.close()
    
    def save_tables(self, tables: List[Dict]):
        """Salva a configuração das tabelas no Oracle"""
        conn = self._get_conn()
        if not conn: return
        
        cursor = conn.cursor()
        try:
            for table in tables:
                sql = """
                    MERGE INTO SYSROH.TB_AI_TABLE_METADATA t
                    USING (SELECT :tname as tn FROM dual) s
                    ON (t.TABLE_NAME = s.tn)
                    WHEN MATCHED THEN
                        UPDATE SET 
                            TABLE_DESCRIPTION = :tdesc, SCHEMA_INFO = :sinfo, COLUMNS_INFO = :cinfo,
                            SAMPLE_DATA = :sdata, RECORD_COUNT = :rcount, IS_ACTIVE = :active,
                            DEEP_PROFILE = :dprof, SEMANTIC_CONTEXT = :sctx, EMBEDDING_VECTOR = :evec,
                            EXPORT_STATUS = :estat, UPDATED_AT = CURRENT_TIMESTAMP
                    WHEN NOT MATCHED THEN
                        INSERT (TABLE_NAME, TABLE_DESCRIPTION, SCHEMA_INFO, COLUMNS_INFO, 
                                SAMPLE_DATA, RECORD_COUNT, IS_ACTIVE, DEEP_PROFILE, SEMANTIC_CONTEXT, 
                                EMBEDDING_VECTOR, EXPORT_STATUS)
                        VALUES (:tname, :tdesc, :sinfo, :cinfo, :sdata, :rcount, :active, :dprof, :sctx, :evec, :estat)
                """
                cursor.execute(sql, {
                    'tname': table['table_name'],
                    'tdesc': table.get('table_description', ''),
                    'sinfo': json.dumps(table.get('schema_info', {})),
                    'cinfo': json.dumps(table.get('columns_info', [])),
                    'sdata': json.dumps(table.get('sample_data', [])),
                    'rcount': table.get('record_count', 0),
                    'active': 1 if table.get('is_active', True) else 0,
                    'dprof': json.dumps(table.get('deep_profile', {})) if isinstance(table.get('deep_profile'), (dict, list)) else table.get('deep_profile'),
                    'sctx': json.dumps(table.get('semantic_context', [])) if isinstance(table.get('semantic_context'), (list, dict)) else table.get('semantic_context'),
                    'evec': table.get('embedding_vector'),
                    'estat': table.get('export_status', 'Sucesso')
                })
            conn.commit()
        finally:
            cursor.close()
            conn.close()
    
    def get_table_metadata(self, table_name: str) -> Optional[Dict]:
        """Obtém metadados de uma tabela específica no Oracle"""
        conn = self._get_conn()
        if not conn: return None
        
        cursor = conn.cursor()
        try:
            cursor.execute('''
                SELECT * FROM SYSROH.TB_AI_TABLE_METADATA 
                WHERE TABLE_NAME = :tname AND IS_ACTIVE = 1
            ''', {'tname': table_name})
            
            result = cursor.fetchone()
            if not result: return None
            
            col_names = [description[0].lower() for description in cursor.description]
            row_dict = dict(zip(col_names, result))
            
            # Ler CLOBs
            for k, v in row_dict.items():
                if hasattr(v, 'read'):
                    row_dict[k] = v.read()
            
            return {
                'id': row_dict.get('id'),
                'table_name': row_dict.get('table_name'),
                'table_description': row_dict.get('table_description'),
                'schema_info': json.loads(row_dict.get('schema_info')) if row_dict.get('schema_info') else {},
                'columns_info': json.loads(row_dict.get('columns_info')) if row_dict.get('columns_info') else [],
                'sample_data': json.loads(row_dict.get('sample_data')) if row_dict.get('sample_data') else [],
                'record_count': row_dict.get('record_count', 0),
                'is_active': row_dict.get('is_active', 1),
                'deep_profile': json.loads(row_dict.get('deep_profile')) if row_dict.get('deep_profile') and row_dict.get('deep_profile').startswith('{') else row_dict.get('deep_profile'),
                'semantic_context': json.loads(row_dict.get('semantic_context')) if row_dict.get('semantic_context') and row_dict.get('semantic_context').startswith('[') else row_dict.get('semantic_context'),
                'embedding_vector': row_dict.get('embedding_vector'),
                'export_status': row_dict.get('export_status', 'Sucesso'),
                'created_at': row_dict.get('created_at'),
                'updated_at': row_dict.get('updated_at')
            }
        finally:
            cursor.close()
            conn.close()

    # --- MÉTODOS DE PADRÕES COMPORTAMENTAIS ---
    
    def save_behavioral_pattern(self, pattern: Dict):
        """Salva um padrão no Oracle"""
        conn = self._get_conn()
        if not conn: return
        
        cursor = conn.cursor()
        try:
            cursor.execute('''
                INSERT INTO SYSROH.TB_AI_BEHAVIORAL_PATTERNS 
                (SITUATION, USER_INPUT, AI_ACTION, AI_RESPONSE, SUCCESS_INDICATOR, CATEGORY, TAGS, EMBEDDING_VECTOR)
                VALUES (:situation, :uinput, :aaction, :aresp, :sind, :cat, :tags, :evec)
            ''', {
                'situation': pattern['situation'],
                'uinput': pattern['user_input'],
                'aaction': pattern['ai_action'],
                'aresp': pattern['ai_response'],
                'sind': pattern.get('success_indicator'),
                'cat': pattern.get('category'),
                'tags': pattern.get('tags'),
                'evec': pattern.get('embedding_vector')
            })
            conn.commit()
        finally:
            cursor.close()
            conn.close()

    def batch_save_behavioral_patterns(self, patterns: List[Dict]):
        """Salva múltiplos padrões no Oracle"""
        conn = self._get_conn()
        if not conn: return
        
        cursor = conn.cursor()
        try:
            data = [
                {
                    'situation': p['situation'],
                    'uinput': p['user_input'],
                    'aaction': p['ai_action'],
                    'aresp': p['ai_response'],
                    'sind': p.get('success_indicator'),
                    'cat': p.get('category'),
                    'tags': p.get('tags'),
                    'evec': p.get('embedding_vector')
                }
                for p in patterns
            ]
            
            cursor.executemany('''
                INSERT INTO SYSROH.TB_AI_BEHAVIORAL_PATTERNS 
                (SITUATION, USER_INPUT, AI_ACTION, AI_RESPONSE, SUCCESS_INDICATOR, CATEGORY, TAGS, EMBEDDING_VECTOR)
                VALUES (:situation, :uinput, :aaction, :aresp, :sind, :cat, :tags, :evec)
            ''', data)
            conn.commit()
        finally:
            cursor.close()
            conn.close()

    def get_behavioral_patterns(self, category: Optional[str] = None) -> List[Dict]:
        """Recupera padrões do Oracle"""
        conn = self._get_conn()
        if not conn: return []
        
        cursor = conn.cursor()
        try:
            if category:
                cursor.execute('SELECT * FROM SYSROH.TB_AI_BEHAVIORAL_PATTERNS WHERE CATEGORY = :cat', {'cat': category})
            else:
                cursor.execute('SELECT * FROM SYSROH.TB_AI_BEHAVIORAL_PATTERNS')
                
            col_names = [description[0].lower() for description in cursor.description]
            patterns = []
            for row in cursor.fetchall():
                row_dict = dict(zip(col_names, row))
                # Ler CLOBs
                for k, v in row_dict.items():
                    if hasattr(v, 'read'):
                        row_dict[k] = v.read()
                patterns.append(row_dict)
            return patterns
        finally:
            cursor.close()
            conn.close()
    
    def save_table_metadata(self, table_name: str, metadata: Dict):
        """Salva metadados de uma tabela no Oracle"""
        conn = self._get_conn()
        if not conn: return
        
        cursor = conn.cursor()
        try:
            sql = """
                MERGE INTO SYSROH.TB_AI_TABLE_METADATA t
                USING (SELECT :tname as tn FROM dual) s
                ON (t.TABLE_NAME = s.tn)
                WHEN MATCHED THEN
                    UPDATE SET 
                        TABLE_DESCRIPTION = :tdesc, SCHEMA_INFO = :sinfo, COLUMNS_INFO = :cinfo,
                        SAMPLE_DATA = :sdata, RECORD_COUNT = :rcount, IS_ACTIVE = :active,
                        DEEP_PROFILE = :dprof, SEMANTIC_CONTEXT = :sctx, EMBEDDING_VECTOR = :evec,
                        UPDATED_AT = CURRENT_TIMESTAMP
                WHEN NOT MATCHED THEN
                    INSERT (TABLE_NAME, TABLE_DESCRIPTION, SCHEMA_INFO, COLUMNS_INFO, 
                            SAMPLE_DATA, RECORD_COUNT, IS_ACTIVE, DEEP_PROFILE, SEMANTIC_CONTEXT, EMBEDDING_VECTOR)
                    VALUES (:tname, :tdesc, :sinfo, :cinfo, :sdata, :rcount, :active, :dprof, :sctx, :evec)
            """
            cursor.execute(sql, {
                'tname': table_name,
                'tdesc': metadata.get('table_description', ''),
                'sinfo': json.dumps(metadata.get('schema_info', {})),
                'cinfo': json.dumps(metadata.get('columns_info', [])),
                'sdata': json.dumps(metadata.get('sample_data', [])),
                'rcount': metadata.get('record_count', 0),
                'active': 1 if metadata.get('is_active', True) else 0,
                'dprof': json.dumps(metadata.get('deep_profile', {})) if isinstance(metadata.get('deep_profile'), (dict, list)) else metadata.get('deep_profile'),
                'sctx': json.dumps(metadata.get('semantic_context', [])) if isinstance(metadata.get('semantic_context'), (list, dict)) else metadata.get('semantic_context'),
                'evec': metadata.get('embedding_vector')
            })
            conn.commit()
        finally:
            cursor.close()
            conn.close()
    
    def get_knowledge(self, category: str = None, limit: int = 100) -> List[Dict]:
        """Obtém itens da base de conhecimento do Oracle"""
        conn = self._get_conn()
        if not conn: return []
        
        cursor = conn.cursor()
        try:
            if category:
                cursor.execute('''
                    SELECT * FROM (
                        SELECT * FROM SYSROH.TB_AI_KNOWLEDGE_BASE 
                        WHERE CATEGORY = :cat AND IS_ACTIVE = 1
                        ORDER BY PRIORITY DESC, CREATED_AT DESC
                    ) WHERE ROWNUM <= :lim
                ''', {'cat': category, 'lim': limit})
            else:
                cursor.execute('''
                    SELECT * FROM (
                        SELECT * FROM SYSROH.TB_AI_KNOWLEDGE_BASE 
                        WHERE IS_ACTIVE = 1
                        ORDER BY PRIORITY DESC, CREATED_AT DESC
                    ) WHERE ROWNUM <= :lim
                ''', {'lim': limit})
            
            col_names = [description[0].lower() for description in cursor.description]
            knowledge = []
            for row in cursor.fetchall():
                row_dict = dict(zip(col_names, row))
                # Ler CLOBs
                for k, v in row_dict.items():
                    if hasattr(v, 'read'):
                        row_dict[k] = v.read()
                knowledge.append(row_dict)
            return knowledge
        finally:
            cursor.close()
            conn.close()
    
    def save_knowledge(self, category: str, title: str, content: str, 
                        tags: str = '', priority: int = 1, embedding_vector: bytes = None):
        """Salva um item na base de conhecimento no Oracle"""
        conn = self._get_conn()
        if not conn: return
        
        cursor = conn.cursor()
        try:
            cursor.execute('''
                INSERT INTO SYSROH.TB_AI_KNOWLEDGE_BASE 
                (CATEGORY, TITLE, CONTENT, TAGS, PRIORITY, EMBEDDING_VECTOR, UPDATED_AT)
                VALUES (:cat, :title, :content, :tags, :prio, :evec, CURRENT_TIMESTAMP)
            ''', {
                'cat': category,
                'title': title,
                'content': content,
                'tags': tags,
                'prio': priority,
                'evec': embedding_vector
            })
            conn.commit()
        finally:
            cursor.close()
            conn.close()
    
    def save_process_flow(self, process_name: str, flow_sequence: List[str], timing_insights: List[Dict], description: str = ''):
        """Salva um fluxo no Oracle"""
        conn = self._get_conn()
        if not conn: return
        
        cursor = conn.cursor()
        try:
            sql = """
                MERGE INTO SYSROH.TB_AI_PROCESS_FLOWS t
                USING (SELECT :pname as pn FROM dual) s
                ON (t.PROCESS_NAME = s.pn)
                WHEN MATCHED THEN
                    UPDATE SET FLOW_SEQUENCE = :fseq, TIMING_INSIGHTS = :tins, DESCRIPTION = :desc, UPDATED_AT = CURRENT_TIMESTAMP
                WHEN NOT MATCHED THEN
                    INSERT (PROCESS_NAME, FLOW_SEQUENCE, TIMING_INSIGHTS, DESCRIPTION)
                    VALUES (:pname, :fseq, :tins, :desc)
            """
            cursor.execute(sql, {
                'pname': process_name,
                'fseq': json.dumps(flow_sequence),
                'tins': json.dumps(timing_insights),
                'desc': description
            })
            conn.commit()
        finally:
            cursor.close()
            conn.close()

    def get_process_flows(self) -> List[Dict]:
        """Recupera fluxos do Oracle"""
        conn = self._get_conn()
        if not conn: return []
        
        cursor = conn.cursor()
        try:
            cursor.execute('SELECT * FROM SYSROH.TB_AI_PROCESS_FLOWS WHERE IS_ACTIVE = 1')
            col_names = [description[0].lower() for description in cursor.description]
            
            flows = []
            for row in cursor.fetchall():
                row_dict = dict(zip(col_names, row))
                # Ler CLOBs
                for k, v in row_dict.items():
                    if hasattr(v, 'read'):
                        row_dict[k] = v.read()
                
                flows.append({
                    'id': row_dict['id'],
                    'process_name': row_dict['process_name'],
                    'flow_sequence': json.loads(row_dict['flow_sequence']) if row_dict['flow_sequence'] else [],
                    'timing_insights': json.loads(row_dict['timing_insights']) if row_dict['timing_insights'] else [],
                    'description': row_dict['description'],
                    'created_at': row_dict['created_at'],
                    'updated_at': row_dict['updated_at']
                })
            return flows
        finally:
            cursor.close()
            conn.close()

    def export_config(self) -> Dict:
        """Exporta toda a configuração (MIGRAÇÃO ORACLE)"""
        return {
            'tables': self.load_tables(),
            'knowledge': self.get_knowledge(),
            'patterns': self.get_behavioral_patterns(),
            'flows': self.get_process_flows()
        }

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
