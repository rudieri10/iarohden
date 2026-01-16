"""
Módulo DATABASE - Sistema de Persistência de Dados
Contém gerenciamento de banco SQLite para histórico e memória
"""

from .db_history import (
    get_db_connection, init_db,
    create_chat, get_user_chats, get_chat_messages, add_message,
    delete_chat, update_chat_title,
    add_favorite, get_favorites, delete_favorite,
    record_user_query, get_suggestions,
    add_alert, get_alerts, update_alert_status, delete_alert,
    save_prediction, get_predictions
)

__all__ = [
    'get_db_connection', 'init_db',
    'create_chat', 'get_user_chats', 'get_chat_messages', 'add_message',
    'delete_chat', 'update_chat_title',
    'add_favorite', 'get_favorites', 'delete_favorite',
    'record_user_query', 'get_suggestions',
    'add_alert', 'get_alerts', 'update_alert_status', 'delete_alert',
    'save_prediction', 'get_predictions'
]
