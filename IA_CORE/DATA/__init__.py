"""Pacote de persistência local do ROHDEN_AI.

Este pacote mantém o código de acesso ao storage (SQLite + ChromaDB).
Os dados ficam fora do repositório em C:\\ia\\banco_ia.
"""

from .storage import (
    get_config, save_config, load_tables, save_tables,
    get_table_metadata, save_table_metadata,
    get_knowledge, save_knowledge,
    export_config, import_config
)

__all__ = [
    "get_config", "save_config", "load_tables", "save_tables",
    "get_table_metadata", "save_table_metadata",
    "get_knowledge", "save_knowledge",
    "export_config", "import_config"
]
