"""
CONNECTION MANAGER - Gerenciador de Conexões Multiusuário (Versão Oracle)
Sistema unificado para acesso ao banco Oracle SYSROH
"""

import threading
import sys
import os
from contextlib import contextmanager
from typing import Generator

# Adicionar o diretório raiz e infra ao path para importar conecxaodb
root_dir = os.path.abspath(os.path.join(os.path.dirname(__file__), '../../../../'))
sys.path.append(root_dir)
sys.path.append(os.path.join(root_dir, 'sys_backend', 'infra'))

try:
    try:
        from conecxaodb import get_connection
    except ImportError:
        from sys_backend.infra.conecxaodb import get_connection
except ImportError:
    # Fallback para o caso de não encontrar o conecxaodb
    def get_connection():
        import cx_Oracle
        ip = '192.168.1.253'
        porta = 1521
        service_name = 'rohden'
        usuario_oracle = 'SYSROH'
        senha_oracle = 'rohden'
        dsn_tns = cx_Oracle.makedsn(ip, porta, service_name=service_name)
        try:
            return cx_Oracle.connect(user=usuario_oracle, password=senha_oracle, dsn=dsn_tns)
        except:
            return None

class ConnectionManager:
    """Gerenciador de conexões Oracle para o ecossistema ROHDEN_AI"""
    
    def __init__(self, db_path: str = None, max_connections: int = 10):
        # db_path não é mais necessário para Oracle, mantido por compatibilidade
        self.db_path = db_path
        self.max_connections = max_connections
        self._lock = threading.Lock()
        self._connection_pool = []
        self._in_use = set()
    
    @contextmanager
    def get_connection(self) -> Generator:
        """Obtém uma conexão Oracle"""
        conn = None
        try:
            with self._lock:
                if self._connection_pool:
                    conn = self._connection_pool.pop()
                else:
                    conn = get_connection()
                    if not conn:
                        raise Exception("Falha ao conectar ao banco Oracle")
            
            with self._lock:
                self._in_use.add(id(conn))
            
            yield conn
            
        finally:
            if conn:
                with self._lock:
                    self._in_use.discard(id(conn))
                    if len(self._connection_pool) < self.max_connections:
                        self._connection_pool.append(conn)
                    else:
                        conn.close()
    
    def execute_query(self, sql: str, params: dict = None) -> list:
        """Executa query de forma segura no Oracle"""
        with self.get_connection() as conn:
            cursor = conn.cursor()
            try:
                # Oracle usa :nome para parâmetros
                if params:
                    cursor.execute(sql, params)
                else:
                    cursor.execute(sql)
                
                sql_upper = sql.strip().upper()
                if sql_upper.startswith('SELECT'):
                    columns = [col[0] for col in cursor.description]
                    rows = cursor.fetchall()
                    return [dict(zip(columns, row)) for row in rows]
                else:
                    conn.commit()
                    return [{'affected': cursor.rowcount}]
                    
            except Exception as e:
                conn.rollback()
                raise e
            finally:
                cursor.close()

    def close_all(self):
        """Fecha todas as conexões do pool"""
        with self._lock:
            for conn in self._connection_pool:
                try:
                    conn.close()
                except:
                    pass
            self._connection_pool.clear()
            self._in_use.clear()

# Instância global
_connection_manager = None

def get_connection_manager(db_path: str = None) -> ConnectionManager:
    """Obtém ou cria o gerenciador de conexões"""
    global _connection_manager
    if _connection_manager is None:
        _connection_manager = ConnectionManager(db_path)
    return _connection_manager

def execute_sql_safe(sql: str, params: dict = None, db_path: str = None) -> list:
    """Executa SQL de forma segura para múltiplos usuários no Oracle"""
    manager = get_connection_manager(db_path)
    return manager.execute_query(sql, params)
