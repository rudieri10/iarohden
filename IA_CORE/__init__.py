"""
Núcleo do Sistema de IA Rohden
Contém os módulos principais de inteligência artificial
"""

from .MEMORIA import MemoriaConversacional, memoria_system
from .ENGINE import LlamaEngine, get_llama_engine
from .PERSISTENCIA import (
    get_db_connection, init_db,
    create_chat, get_user_chats, get_chat_messages, add_message,
    delete_chat, update_chat_title,
    add_favorite, get_favorites, delete_favorite,
    record_user_query, get_suggestions,
    add_alert, get_alerts, update_alert_status, delete_alert,
    save_prediction, get_predictions
)
from .DATA import (
    get_config, save_config, load_tables, save_tables,
    get_table_metadata, save_table_metadata,
    get_knowledge, save_knowledge,
    export_config, import_config
)

__all__ = [
    'MemoriaConversacional', 'memoria_system', 
    'LlamaEngine', 'get_llama_engine',
    'get_db_connection', 'init_db',
    'create_chat', 'get_user_chats', 'get_chat_messages', 'add_message',
    'delete_chat', 'update_chat_title',
    'add_favorite', 'get_favorites', 'delete_favorite',
    'record_user_query', 'get_suggestions',
    'add_alert', 'get_alerts', 'update_alert_status', 'delete_alert',
    'save_prediction', 'get_predictions',
    'get_config', 'save_config', 'load_tables', 'save_tables',
    'get_table_metadata', 'save_table_metadata',
    'get_knowledge', 'save_knowledge',
    'export_config', 'import_config'
]
