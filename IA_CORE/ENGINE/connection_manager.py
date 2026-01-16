"""
CONNECTION MANAGER - Gerenciador de Conexões Multiusuário
Sistema sem bloqueios para múltiplos usuários simultâneos
"""

import sqlite3
import threading
import time
from contextlib import contextmanager
from typing import Optional, Generator
import os

class ConnectionManager:
    """Gerenciador de conexões SQLite sem bloqueios para múltiplos usuários"""
    
    def __init__(self, db_path: str, max_connections: int = 10):
        self.db_path = db_path
        self.max_connections = max_connections
        self._lock = threading.Lock()
        self._connection_pool = []
        self._in_use = set()
        
        # Inicializar o banco se não existir
        self._init_database()
    
    def _init_database(self):
        """Inicializa o banco de dados se não existir"""
        if not os.path.exists(self.db_path):
            conn = sqlite3.connect(self.db_path)
            conn.close()
    
    @contextmanager
    def get_connection(self) -> Generator[sqlite3.Connection, None, None]:
        """Obtém uma conexão do pool sem bloquear outros usuários"""
        conn = None
        try:
            # Tenta obter do pool
            with self._lock:
                if self._connection_pool:
                    conn = self._connection_pool.pop()
                elif len(self._in_use) < self.max_connections:
                    conn = sqlite3.connect(
                        self.db_path,
                        check_same_thread=False,  # Permite uso em múltiplas threads
                        timeout=30.0,  # Timeout de 30 segundos
                        isolation_level=None  # Autocommit para evitar locks longos
                    )
                    # Configurar para modo WAL (Write-Ahead Logging) - melhor para concorrência
                    conn.execute('PRAGMA journal_mode=WAL')
                    conn.execute('PRAGMA synchronous=NORMAL')
                    conn.execute('PRAGMA cache_size=10000')
                    conn.execute('PRAGMA temp_store=MEMORY')
                else:
                    # Pool cheio - criar nova conexão temporária
                    conn = sqlite3.connect(
                        self.db_path,
                        check_same_thread=False,
                        timeout=5.0,
                        isolation_level=None
                    )
                    conn.execute('PRAGMA journal_mode=WAL')
            
            # Marcar como em uso
            with self._lock:
                self._in_use.add(id(conn))
            
            yield conn
            
        finally:
            # Devolver para o pool
            if conn:
                try:
                    conn.commit()  # Garantir que as alterações sejam salvas
                except:
                    pass
                
                with self._lock:
                    self._in_use.discard(id(conn))
                    if len(self._connection_pool) < self.max_connections:
                        self._connection_pool.append(conn)
                    else:
                        conn.close()
    
    def execute_query(self, sql: str, params: tuple = None) -> list:
        """Executa query de forma segura sem bloqueios"""
        with self.get_connection() as conn:
            cursor = conn.cursor()
            try:
                if params:
                    cursor.execute(sql, params)
                else:
                    cursor.execute(sql)
                
                if sql.strip().upper().startswith('SELECT'):
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
                conn.close()
            self._connection_pool.clear()
            self._in_use.clear()

# Instância global do gerenciador
_connection_manager = None

def get_connection_manager(db_path: str) -> ConnectionManager:
    """Obtém ou cria o gerenciador de conexões"""
    global _connection_manager
    if _connection_manager is None or _connection_manager.db_path != db_path:
        _connection_manager = ConnectionManager(db_path)
    return _connection_manager

def execute_sql_safe(sql: str, params: tuple = None, db_path: str = None) -> list:
    """Executa SQL de forma segura para múltiplos usuários"""
    if db_path is None:
        from ..DATA.storage import storage
        db_path = storage.db_path
    
    manager = get_connection_manager(db_path)
    return manager.execute_query(sql, params)
