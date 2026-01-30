import os
import sys
import threading
from contextlib import contextmanager
from typing import Generator

# Configuração de path para infra
root_dir = os.path.abspath(os.path.join(os.path.dirname(__file__), '../../../../'))
sys.path.append(root_dir)
sys.path.append(os.path.join(root_dir, 'sys_backend', 'infra'))

try:
    try:
        from conecxaodb import get_connection as _get_conn
    except ImportError:
        from sys_backend.infra.conecxaodb import get_connection as _get_conn
except ImportError:
    def _get_conn():
        import cx_Oracle
        dsn = cx_Oracle.makedsn('192.168.1.253', 1521, service_name='rohden')
        return cx_Oracle.connect(user='SYSROH', password='rohden', dsn=dsn)

class ConnectionManager:
    """Gestão simplificada de conexões Oracle."""
    
    _instance = None
    _lock = threading.Lock()
    
    def __init__(self):
        self._pool = []
        self._max_pool = 5
        self._pool_lock = threading.Lock()

    @classmethod
    def get_instance(cls):
        if not cls._instance:
            with cls._lock:
                if not cls._instance:
                    cls._instance = ConnectionManager()
        return cls._instance

    @contextmanager
    def get_connection(self) -> Generator:
        conn = None
        try:
            with self._pool_lock:
                # Tenta pegar uma conexão válida do pool
                while self._pool:
                    potential_conn = self._pool.pop()
                    try:
                        potential_conn.ping()
                        conn = potential_conn
                        break
                    except:
                        try: potential_conn.close()
                        except: pass

            if not conn:
                conn = _get_conn()
                
            yield conn
            
        finally:
            if conn:
                try:
                    # Testar se conexão está viva antes de devolver
                    conn.ping()
                    with self._pool_lock:
                        if len(self._pool) < self._max_pool:
                            self._pool.append(conn)
                        else:
                            conn.close()
                except:
                    # Se falhar ping ou close, descarta
                    pass

    def execute_query(self, sql: str, params: dict = None) -> list:
        """Executa SQL com retry automático em caso de falha de conexão."""
        max_retries = 2
        last_error = None
        
        for attempt in range(max_retries):
            try:
                with self.get_connection() as conn:
                    cursor = conn.cursor()
                    try:
                        if params:
                            cursor.execute(sql, params)
                        else:
                            cursor.execute(sql)
                        
                        if cursor.description:
                            columns = [col[0] for col in cursor.description]
                            return [dict(zip(columns, row)) for row in cursor.fetchall()]
                        else:
                            # Se não for SELECT, commita
                            conn.commit()
                            return []
                    finally:
                        cursor.close()
                # Se sucesso, sai do loop
                break
            except Exception as e:
                last_error = e
                error_str = str(e)
                # Verifica erros de conexão para retry
                # 10054: Connection reset
                # ORA-03113, ORA-03114, ORA-03135: Connection lost
                is_conn_error = any(code in error_str for code in ['10054', 'ORA-03113', 'ORA-03114', 'ORA-03135', 'not connected'])
                
                if is_conn_error and attempt < max_retries - 1:
                    continue
                raise last_error
        return []
