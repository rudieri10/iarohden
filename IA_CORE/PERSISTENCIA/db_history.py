import os
import json
import sqlite3
from datetime import datetime
from pathlib import Path

# Caminho para o banco de dados local (mesmo usado no storage.py)
DB_PATH = Path(os.path.dirname(__file__)).parent / "DATA" / "ai_storage.db"

def get_db_connection():
    """Obtém uma conexão com o SQLite"""
    os.makedirs(DB_PATH.parent, exist_ok=True)
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    return conn

def init_db():
    """Garante que as tabelas necessárias existam no SQLite"""
    conn = get_db_connection()
    cursor = conn.cursor()
    
    try:
        # Tabela de Alertas
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS tb_ai_alerts (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                user_name TEXT NOT NULL,
                title TEXT NOT NULL,
                sql_query TEXT NOT NULL,
                condition_type TEXT,
                threshold_value TEXT,
                status TEXT DEFAULT 'PENDING',
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        ''')

        # Tabela de Predições
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS tb_ai_predictions (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                user_name TEXT NOT NULL,
                target_data TEXT,
                forecast_json TEXT,
                confidence_score REAL,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        ''')

        # Tabela de Chats
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS tb_ai_chats (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                user_name TEXT NOT NULL,
                title TEXT DEFAULT 'Nova Conversa',
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        ''')

        # Tabela de Mensagens
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS tb_ai_messages (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                chat_id INTEGER NOT NULL,
                role TEXT NOT NULL,
                content TEXT NOT NULL,
                metadata TEXT,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                FOREIGN KEY (chat_id) REFERENCES tb_ai_chats(id) ON DELETE CASCADE
            )
        ''')

        # Tabela de Favoritos
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS tb_ai_favorites (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                user_name TEXT NOT NULL,
                query_text TEXT NOT NULL,
                title TEXT,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        ''')

        # Tabela de Padrões de Usuário (Auto-complete)
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS tb_ai_user_patterns (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                user_name TEXT NOT NULL,
                query_text TEXT NOT NULL,
                frequency INTEGER DEFAULT 1,
                last_used TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                UNIQUE(user_name, query_text)
            )
        ''')

        # Tabela de Perfil de Usuário
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS tb_ai_user_profile (
                user_name TEXT PRIMARY KEY,
                preferences TEXT,
                interaction_style TEXT,
                favorite_metrics TEXT,
                response_format TEXT,
                last_interaction TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        ''')

        # Tabela de Contexto de Problemas Resolvidos
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS tb_ai_problem_context (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                user_name TEXT NOT NULL,
                problem_type TEXT,
                solution_used TEXT,
                sql_pattern TEXT,
                success_rating REAL,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        ''')

        # Tabela de Padrões de Linguagem
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS tb_ai_language_patterns (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                user_name TEXT NOT NULL,
                phrase_pattern TEXT,
                intent TEXT,
                frequency INTEGER DEFAULT 1,
                last_used TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        ''')

        # Tabela de Memória Contextual (Versão Robusta)
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS tb_ai_contextual_memory (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                user_name TEXT NOT NULL,
                context_type TEXT,
                content TEXT NOT NULL,
                importance INTEGER DEFAULT 1,
                expires_at TIMESTAMP,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        ''')

        # Tabela de Acesso de Usuário a Tabelas (Permissões)
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS tb_ai_user_table_access (
                user_name TEXT NOT NULL,
                table_name TEXT NOT NULL,
                access_level INTEGER DEFAULT 1,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                PRIMARY KEY (user_name, table_name)
            )
        ''')

        conn.commit()
    except Exception as e:
        print(f"Erro ao inicializar tabelas no SQLite: {e}")
    finally:
        conn.close()

# --- Funções para Alertas ---
def add_alert(user_name, title, sql_query, condition_type, threshold_value):
    conn = get_db_connection()
    cursor = conn.cursor()
    cursor.execute('''
        INSERT INTO tb_ai_alerts (user_name, title, sql_query, condition_type, threshold_value)
        VALUES (?, ?, ?, ?, ?)
    ''', (user_name, title, sql_query, condition_type, threshold_value))
    conn.commit()
    conn.close()

def get_alerts(user_name):
    conn = get_db_connection()
    cursor = conn.cursor()
    cursor.execute('SELECT * FROM tb_ai_alerts WHERE user_name = ? ORDER BY created_at DESC', (user_name,))
    alerts = [dict(row) for row in cursor.fetchall()]
    conn.close()
    return alerts

def update_alert_status(alert_id, status):
    conn = get_db_connection()
    cursor = conn.cursor()
    cursor.execute('UPDATE tb_ai_alerts SET status = ? WHERE id = ?', (status, alert_id))
    conn.commit()
    conn.close()

def delete_alert(alert_id):
    conn = get_db_connection()
    cursor = conn.cursor()
    cursor.execute('DELETE FROM tb_ai_alerts WHERE id = ?', (alert_id,))
    conn.commit()
    conn.close()

# --- Funções para Predições ---
def save_prediction(user_name, target, forecast_json, confidence):
    conn = get_db_connection()
    cursor = conn.cursor()
    cursor.execute('''
        INSERT INTO tb_ai_predictions (user_name, target_data, forecast_json, confidence_score)
        VALUES (?, ?, ?, ?)
    ''', (user_name, target, json.dumps(forecast_json), confidence))
    conn.commit()
    conn.close()

def get_predictions(user_name):
    conn = get_db_connection()
    cursor = conn.cursor()
    cursor.execute('''
        SELECT * FROM tb_ai_predictions 
        WHERE user_name = ? 
        ORDER BY created_at DESC
        LIMIT 10
    ''', (user_name,))
    preds = [dict(row) for row in cursor.fetchall()]
    conn.close()
    return preds

# Funções CRUD para Chats
def create_chat(user_name, title='Nova Conversa'):
    conn = get_db_connection()
    cursor = conn.cursor()
    cursor.execute('INSERT INTO tb_ai_chats (user_name, title) VALUES (?, ?)', (user_name, title))
    chat_id = cursor.lastrowid
    conn.commit()
    conn.close()
    return chat_id

def get_user_chats(user_name):
    conn = get_db_connection()
    cursor = conn.cursor()
    cursor.execute('SELECT * FROM tb_ai_chats WHERE user_name = ? ORDER BY updated_at DESC', (user_name,))
    chats = [dict(row) for row in cursor.fetchall()]
    conn.close()
    return chats

def get_chat_messages(chat_id):
    conn = get_db_connection()
    cursor = conn.cursor()
    cursor.execute('SELECT role, content, metadata FROM tb_ai_messages WHERE chat_id = ? ORDER BY created_at ASC', (chat_id,))
    messages = []
    for row in cursor.fetchall():
        msg = dict(row)
        if msg.get('metadata'):
            try:
                meta = json.loads(msg['metadata'])
                if isinstance(meta, dict):
                    msg.update(meta)
            except:
                pass
        messages.append(msg)
    conn.close()
    return messages

def add_message(chat_id, role, content, metadata=None):
    conn = get_db_connection()
    cursor = conn.cursor()
    meta_json = json.dumps(metadata) if metadata else None
    cursor.execute('INSERT INTO tb_ai_messages (chat_id, role, content, metadata) VALUES (?, ?, ?, ?)', 
                   (chat_id, role, content, meta_json))
    cursor.execute('UPDATE tb_ai_chats SET updated_at = CURRENT_TIMESTAMP WHERE id = ?', (chat_id,))
    conn.commit()
    conn.close()

def delete_chat(chat_id):
    conn = get_db_connection()
    cursor = conn.cursor()
    cursor.execute('DELETE FROM tb_ai_chats WHERE id = ?', (chat_id,))
    conn.commit()
    conn.close()

def update_chat_title(chat_id, title):
    conn = get_db_connection()
    cursor = conn.cursor()
    cursor.execute('UPDATE tb_ai_chats SET title = ? WHERE id = ?', (title, chat_id))
    conn.commit()
    conn.close()

# Funções para Favoritos
def add_favorite(user_name, query, title):
    conn = get_db_connection()
    cursor = conn.cursor()
    cursor.execute('INSERT INTO tb_ai_favorites (user_name, query_text, title) VALUES (?, ?, ?)', (user_name, query, title))
    conn.commit()
    conn.close()

def get_favorites(user_name):
    conn = get_db_connection()
    cursor = conn.cursor()
    cursor.execute('SELECT * FROM tb_ai_favorites WHERE user_name = ? ORDER BY created_at DESC', (user_name,))
    favs = [dict(row) for row in cursor.fetchall()]
    conn.close()
    return favs

def delete_favorite(fav_id):
    conn = get_db_connection()
    cursor = conn.cursor()
    cursor.execute('DELETE FROM tb_ai_favorites WHERE id = ?', (fav_id,))
    conn.commit()
    conn.close()

# Funções para Auto-complete / Sugestões
def record_user_query(user_name, query):
    if not query or len(query) < 3: return
    conn = get_db_connection()
    cursor = conn.cursor()
    cursor.execute('''
        INSERT INTO tb_ai_user_patterns (user_name, query_text, frequency)
        VALUES (?, ?, 1)
        ON CONFLICT(user_name, query_text) DO UPDATE SET 
            frequency = frequency + 1, 
            last_used = CURRENT_TIMESTAMP
    ''', (user_name, query))
    conn.commit()
    conn.close()

def get_suggestions(user_name, partial_query=''):
    conn = get_db_connection()
    cursor = conn.cursor()
    if partial_query:
        cursor.execute('''
            SELECT query_text FROM tb_ai_user_patterns 
            WHERE user_name = ? AND query_text LIKE ? 
            ORDER BY frequency DESC LIMIT 5
        ''', (user_name, f'%{partial_query}%'))
    else:
        cursor.execute('''
            SELECT query_text FROM tb_ai_user_patterns 
            WHERE user_name = ? 
            ORDER BY frequency DESC LIMIT 5
        ''', (user_name,))
    suggestions = [row[0] for row in cursor.fetchall()]
    conn.close()
    return suggestions

# Inicializa o banco ao carregar o módulo
init_db()
