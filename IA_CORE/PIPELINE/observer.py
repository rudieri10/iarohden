import time
import logging
from typing import Any
from ..STORAGE.simple_storage import SimpleStorage

logger = logging.getLogger(__name__)

class Observer:
    """
    OBSERVER: Monitora o mundo real (consultas SQL).
    Não julga, apenas registra.
    """
    
    def __init__(self):
        self.storage = SimpleStorage()
        
    def observe_query(self, sql: str, params: Any = None):
        """Context manager para observar execução de query."""
        return QueryContext(self, sql, params)

    def watch_query(self, func):
        """Decorator para observar funções que executam SQL."""
        def wrapper(*args, **kwargs):
            # Tenta extrair SQL dos argumentos (assume que é o primeiro arg string)
            sql = next((arg for arg in args if isinstance(arg, str)), "SQL Desconhecido")
            
            with self.observe_query(sql) as ctx:
                try:
                    result = func(*args, **kwargs)
                    # Tenta contar linhas se for lista
                    if isinstance(result, list):
                        ctx.row_count = len(result)
                    return result
                except Exception as e:
                    ctx.error = str(e)
                    raise e
        return wrapper

    def record_execution(self, sql: str, duration: float, rows: int, error: str = None):
        """Registra a execução finalizada."""
        try:
            self.storage.log_observation(sql, duration, rows, error)
        except Exception as e:
            logger.error(f"Falha ao observar query: {e}")

class QueryContext:
    def __init__(self, observer: Observer, sql: str, params: Any):
        self.observer = observer
        self.sql = sql
        self.params = params
        self.start_time = 0
        self.row_count = 0
        self.error = None
        
    def __enter__(self):
        self.start_time = time.time()
        return self
        
    def __exit__(self, exc_type, exc_val, exc_tb):
        duration = time.time() - self.start_time
        error = self.error if self.error else (str(exc_val) if exc_val else None)
        self.observer.record_execution(self.sql, duration, self.row_count, error)
