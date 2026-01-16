import sqlite3
import os
import json
from datetime import datetime

DATA_DIR = os.path.join(os.path.dirname(os.path.dirname(__file__)), 'DATA')
DB_PATH = os.path.join(DATA_DIR, 'rohden_ai.db')

def get_db_connection():
    if not os.path.exists(DATA_DIR):
        os.makedirs(DATA_DIR)
    
    conn = sqlite3.connect(DB_PATH)
    conn.execute('PRAGMA foreign_keys = ON')
    conn.row_factory = sqlite3.Row
    return conn

def init_db():
    conn = get_db_connection()
    cursor = conn.cursor()
    
    # Tabela de Chats (Sessões)
    cursor.execute('''
    CREATE TABLE IF NOT EXISTS chats (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        user_name TEXT NOT NULL,
        title TEXT DEFAULT 'Nova Conversa',
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    )
    ''')
    
    # Tabela de Mensagens
    cursor.execute('''
    CREATE TABLE IF NOT EXISTS messages (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        chat_id INTEGER NOT NULL,
        role TEXT NOT NULL,
        content TEXT NOT NULL,
        metadata TEXT, -- JSON com metadados (ex: tabelas_usadas)
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        FOREIGN KEY (chat_id) REFERENCES chats (id) ON DELETE CASCADE
    )
    ''')

    # Garantir que a coluna metadata existe (para migração)
    try:
        cursor.execute('ALTER TABLE messages ADD COLUMN metadata TEXT')
    except sqlite3.OperationalError:
        pass # Coluna já existe
    
    # Tabela de Favoritos
    cursor.execute('''
    CREATE TABLE IF NOT EXISTS favorites (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        user_name TEXT NOT NULL,
        query TEXT NOT NULL,
        title TEXT NOT NULL,
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    )
    ''')
    
    # Tabela de Sugestões/Padrões do Usuário (para Auto-complete)
    cursor.execute('''
    CREATE TABLE IF NOT EXISTS user_patterns (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        user_name TEXT NOT NULL,
        query TEXT NOT NULL,
        frequency INTEGER DEFAULT 1,
        last_used TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    )
    ''')
    
    # Tabela de Alertas Inteligentes
    cursor.execute('''
    CREATE TABLE IF NOT EXISTS intelligent_alerts (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        user_name TEXT NOT NULL,
        title TEXT NOT NULL,
        sql_query TEXT NOT NULL,
        condition_type TEXT NOT NULL, -- 'increase', 'decrease', 'threshold', 'change'
        threshold_value REAL,
        last_value REAL,
        status TEXT DEFAULT 'active', -- 'active', 'triggered', 'muted'
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        last_checked TIMESTAMP
    )
    ''')

    # Tabela de Predições Salvas
    cursor.execute('''
    CREATE TABLE IF NOT EXISTS ai_predictions (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        user_name TEXT NOT NULL,
        target_data TEXT NOT NULL,
        forecast_json TEXT NOT NULL,
        confidence_score REAL,
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    )
    ''')
    
    conn.commit()
    conn.close()

# --- Funções para Alertas ---
def add_alert(user_name, title, sql_query, condition_type, threshold_value):
    conn = get_db_connection()
    cursor = conn.cursor()
    cursor.execute('''
        INSERT INTO intelligent_alerts (user_name, title, sql_query, condition_type, threshold_value)
        VALUES (?, ?, ?, ?, ?)
    ''', (user_name, title, sql_query, condition_type, threshold_value))
    conn.commit()
    conn.close()

def get_alerts(user_name):
    conn = get_db_connection()
    cursor = conn.cursor()
    cursor.execute('SELECT * FROM intelligent_alerts WHERE user_name = ? ORDER BY created_at DESC', (user_name,))
    alerts = [dict(row) for row in cursor.fetchall()]
    conn.close()
    return alerts

def update_alert_status(alert_id, status):
    conn = get_db_connection()
    cursor = conn.cursor()
    cursor.execute('UPDATE intelligent_alerts SET status = ? WHERE id = ?', (status, alert_id))
    conn.commit()
    conn.close()

def delete_alert(alert_id):
    conn = get_db_connection()
    cursor = conn.cursor()
    cursor.execute('DELETE FROM intelligent_alerts WHERE id = ?', (alert_id,))
    conn.commit()
    conn.close()

# --- Funções para Predições ---
def save_prediction(user_name, target, forecast_json, confidence):
    conn = get_db_connection()
    cursor = conn.cursor()
    cursor.execute('''
        INSERT INTO ai_predictions (user_name, target_data, forecast_json, confidence_score)
        VALUES (?, ?, ?, ?)
    ''', (user_name, target, json.dumps(forecast_json), confidence))
    conn.commit()
    conn.close()

def get_predictions(user_name):
    conn = get_db_connection()
    cursor = conn.cursor()
    cursor.execute('SELECT * FROM ai_predictions WHERE user_name = ? ORDER BY created_at DESC LIMIT 10', (user_name,))
    preds = [dict(row) for row in cursor.fetchall()]
    conn.close()
    return preds

# Funções CRUD para Chats
def create_chat(user_name, title='Nova Conversa'):
    conn = get_db_connection()
    cursor = conn.cursor()
    cursor.execute('INSERT INTO chats (user_name, title) VALUES (?, ?)', (user_name, title))
    chat_id = cursor.lastrowid
    conn.commit()
    conn.close()
    return chat_id

def get_user_chats(user_name):
    conn = get_db_connection()
    cursor = conn.cursor()
    cursor.execute('SELECT * FROM chats WHERE user_name = ? ORDER BY updated_at DESC', (user_name,))
    chats = [dict(row) for row in cursor.fetchall()]
    conn.close()
    return chats

def get_chat_messages(chat_id):
    conn = get_db_connection()
    cursor = conn.cursor()
    cursor.execute('SELECT role, content, metadata FROM messages WHERE chat_id = ? ORDER BY created_at ASC', (chat_id,))
    messages = []
    for row in cursor.fetchall():
        msg = dict(row)
        if msg.get('metadata'):
            try:
                msg['metadata'] = json.loads(msg['metadata'])
                # Achatar metadados úteis para o histórico esperado pelo interpretador
                if isinstance(msg['metadata'], dict):
                    for k, v in msg['metadata'].items():
                        msg[k] = v
            except:
                pass
        messages.append(msg)
    conn.close()
    return messages

def add_message(chat_id, role, content, metadata=None):
    conn = get_db_connection()
    cursor = conn.cursor()
    meta_json = json.dumps(metadata) if metadata else None
    cursor.execute('''
        INSERT INTO messages (chat_id, role, content, metadata) 
        VALUES (?, ?, ?, ?)
    ''', (chat_id, role, content, meta_json))
    cursor.execute('UPDATE chats SET updated_at = CURRENT_TIMESTAMP WHERE id = ?', (chat_id,))
    conn.commit()
    conn.close()

def delete_chat(chat_id):
    conn = get_db_connection()
    cursor = conn.cursor()
    cursor.execute('DELETE FROM chats WHERE id = ?', (chat_id,))
    conn.commit()
    conn.close()

def update_chat_title(chat_id, title):
    conn = get_db_connection()
    cursor = conn.cursor()
    cursor.execute('UPDATE chats SET title = ? WHERE id = ?', (title, chat_id))
    conn.commit()
    conn.close()

# Funções para Favoritos
def add_favorite(user_name, query, title):
    conn = get_db_connection()
    cursor = conn.cursor()
    cursor.execute('INSERT INTO favorites (user_name, query, title) VALUES (?, ?, ?)', (user_name, query, title))
    conn.commit()
    conn.close()

def get_favorites(user_name):
    conn = get_db_connection()
    cursor = conn.cursor()
    cursor.execute('SELECT * FROM favorites WHERE user_name = ? ORDER BY created_at DESC', (user_name,))
    favs = [dict(row) for row in cursor.fetchall()]
    conn.close()
    return favs

def delete_favorite(fav_id):
    conn = get_db_connection()
    cursor = conn.cursor()
    cursor.execute('DELETE FROM favorites WHERE id = ?', (fav_id,))
    conn.commit()
    conn.close()

# Funções para Auto-complete / Sugestões
def record_user_query(user_name, query):
    if not query or len(query) < 3: return
    conn = get_db_connection()
    cursor = conn.cursor()
    cursor.execute('SELECT id, frequency FROM user_patterns WHERE user_name = ? AND query = ?', (user_name, query))
    row = cursor.fetchone()
    if row:
        cursor.execute('UPDATE user_patterns SET frequency = frequency + 1, last_used = CURRENT_TIMESTAMP WHERE id = ?', (row['id'],))
    else:
        cursor.execute('INSERT INTO user_patterns (user_name, query) VALUES (?, ?)', (user_name, query))
    conn.commit()
    conn.close()

def get_suggestions(user_name, partial_query=''):
    conn = get_db_connection()
    cursor = conn.cursor()
    if partial_query:
        cursor.execute('SELECT query FROM user_patterns WHERE user_name = ? AND query LIKE ? ORDER BY frequency DESC LIMIT 5', 
                       (user_name, f'%{partial_query}%'))
    else:
        cursor.execute('SELECT query FROM user_patterns WHERE user_name = ? ORDER BY frequency DESC LIMIT 5', (user_name,))
    
    suggestions = [row['query'] for row in cursor.fetchall()]
    conn.close()
    return suggestions

# Inicializa o banco ao carregar o módulo
init_db()
