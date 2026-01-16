"""
DATA - Sistema de Dados e Configuração
Armazenamento profissional de configurações e metadados
"""

from .storage import (
    get_config, save_config, load_tables, save_tables,
    get_table_metadata, save_table_metadata,
    get_knowledge, save_knowledge,
    export_config, import_config
)

__all__ = [
    'get_config', 'save_config', 'load_tables', 'save_tables',
    'get_table_metadata', 'save_table_metadata',
    'get_knowledge', 'save_knowledge',
    'export_config', 'import_config'
]
