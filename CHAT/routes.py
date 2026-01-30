"""
ROUTES - Rotas do Chat
Versão super minimalista - apenas importações e chamadas diretas
"""

from . import chat_bp
from flask import render_template, request, jsonify, session
from ..IA_CORE import (
    get_llama_engine, memoria_system,
    create_chat, get_user_chats, get_chat_messages, add_message, 
    delete_chat, update_chat_title, add_favorite, get_favorites, 
    delete_favorite, record_user_query, get_suggestions,
    add_alert, get_alerts, update_alert_status, delete_alert,
    save_prediction, get_predictions, PassiveLearner
)
from ..IA_CORE.INTERFACE.chat_processor import ChatProcessor
from ..IA_CORE.INTERFACE.insight_formatter import InsightFormatter

import traceback
import threading

# Inicializar o aprendiz passivo e processadores
passive_learner = PassiveLearner()
chat_processor = ChatProcessor()
insight_formatter = InsightFormatter()

# ========================================
# ROTAS DA PÁGINA
# ========================================

@chat_bp.route('/')
def index():
    return render_template('chat.html')

# ========================================
# ROTAS DE CHATS
# ========================================

@chat_bp.route('/chats', methods=['GET'])
def list_chats():
    username = session.get('user_name') or session.get('usuario')
    if not username: return jsonify([])
    return jsonify(get_user_chats(username))

@chat_bp.route('/chats', methods=['POST'])
def new_chat():
    username = session.get('user_name') or session.get('usuario')
    if not username: return jsonify({'error': 'Não logado'}), 401
    title = request.json.get('title', 'Nova Conversa')
    chat_id = create_chat(username, title)
    return jsonify({'chat_id': chat_id})

@chat_bp.route('/chats/<int:chat_id>', methods=['GET'])
def get_chat(chat_id):
    messages = get_chat_messages(chat_id)
    return jsonify(messages)

@chat_bp.route('/chats/<int:chat_id>', methods=['DELETE'])
def remove_chat(chat_id):
    delete_chat(chat_id)
    return jsonify({'success': True})

@chat_bp.route('/chats/<int:chat_id>', methods=['PUT'])
def rename_chat(chat_id):
    title = request.json.get('title')
    update_chat_title(chat_id, title)
    return jsonify({'success': True})

# ========================================
# ROTA PRINCIPAL DA IA
# ========================================

@chat_bp.route('/ask', methods=['POST'])
def ask():
    """
    Rota principal da IA (Unificada).
    Pipeline: ChatProcessor (Decisão Inteligente) -> RAG/SQL/Chat -> InsightFormatter
    """
    return process_chat_request()

@chat_bp.route('/new_message', methods=['POST'])
def new_message():
    """Alias para /ask para compatibilidade"""
    return process_chat_request()

def process_chat_request():
    try:
        data = request.json
        user_message = data.get('message') or data.get('prompt')
        username = session.get('user_name') or session.get('usuario')
        chat_id = data.get('chat_id')
        
        if not user_message:
            return jsonify({'error': 'Mensagem vazia'}), 400

        # 1. Processar usando o ChatProcessor (decide se usa SQL ou Chat)
        result = chat_processor.process_message(user_message)
        
        # 2. Formatar resposta humanizada
        final_text = insight_formatter.format_response(result)
        
        # 3. Salvar histórico
        if chat_id:
            try:
                add_message(chat_id, 'user', user_message)
                # Metadados extras para debug
                metadata = {
                    'sql': result.get('generated_sql'),
                    'row_count': result.get('row_count'),
                    'error': result.get('error'),
                    'type': result.get('type')
                }
                add_message(chat_id, 'assistant', final_text, metadata=metadata)
                
                # Atualizar título se for primeira mensagem
                # (Lógica simplificada, poderia ser melhorada)
            except Exception as e:
                print(f"Erro ao salvar histórico: {e}")

        # Aprendizado Passivo (em background)
        if username and user_message and final_text:
            threading.Thread(
                target=passive_learner.analyze_interaction, 
                args=(username, user_message, final_text, chat_id),
                daemon=True
            ).start()

        return jsonify({
            'response': final_text,
            'sql_executed': result.get('generated_sql'),
            'row_count': result.get('row_count'),
            'debug_info': {
                'context_used': result.get('context_used'),
                'error': result.get('error'),
                'type': result.get('type')
            }
        })
    except Exception as e:
        traceback.print_exc()
        return jsonify({'error': str(e)}), 500

# Antiga rota mantida para fallback temporário, mas renomeada internamente se precisar
# @chat_bp.route('/ask_legacy', methods=['POST']) ... 
# Na verdade, vou deixar a /ask original quieta se quiserem usar, 
# mas vou adicionar a /new_message separada.

@chat_bp.route('/ask_legacy', methods=['POST'])
def ask_legacy():
    try:
        data = request.json

        user_message = data.get('prompt') or data.get('message')
        chat_id = data.get('chat_id')
        history = data.get('history', [])
        mode = data.get('mode', 'chat') # Novo parâmetro
        username = session.get('user_name') or session.get('usuario')
        
        if not user_message:
            return jsonify({'error': 'Mensagem vazia'}), 400
            
        if username:
            record_user_query(username, user_message)
            
        if chat_id:
            # Priorizar histórico do banco que contém metadados (tabelas_usadas)
            db_history = get_chat_messages(chat_id)
            if db_history:
                history = db_history
            
        engine = get_llama_engine()
        result = engine.generate_response(user_message, username=username, history=history, mode=mode)
        response_text = result.get('text', '')
        metadata = result.get('metadata', {})
        
        if chat_id:
            add_message(chat_id, 'user', user_message)
            add_message(chat_id, 'assistant', response_text, metadata=metadata)
            if not history:
                title = user_message[:30] + ('...' if len(user_message) > 30 else '')
                update_chat_title(chat_id, title)
        
        # Aprendizado Passivo (em background para não travar a resposta)
        if username and user_message and response_text:
            threading.Thread(
                target=passive_learner.analyze_interaction, 
                args=(username, user_message, response_text, chat_id),
                daemon=True
            ).start()

        suggestions = get_suggestions(username) if username else []
        
        return jsonify({
            'response': response_text,
            'suggestions': suggestions,
            'chat_id': chat_id
        })
        
    except Exception as e:
        return jsonify({'error': str(e)}), 500

# ========================================
# ROTAS DE FAVORITOS
# ========================================

@chat_bp.route('/favorites', methods=['GET'])
def list_favorites():
    username = session.get('user_name') or session.get('usuario')
    return jsonify(get_favorites(username))

@chat_bp.route('/favorites', methods=['POST'])
def new_favorite():
    username = session.get('user_name') or session.get('usuario')
    data = request.json
    add_favorite(username, data['query'], data['title'])
    return jsonify({'success': True})

@chat_bp.route('/favorites/<int:fav_id>', methods=['DELETE'])
def remove_favorite(fav_id):
    delete_favorite(fav_id)
    return jsonify({'success': True})

# ========================================
# ROTAS DE SUGESTÕES
# ========================================

@chat_bp.route('/suggestions', methods=['GET'])
def autocomplete():
    username = session.get('user_name') or session.get('usuario')
    partial = request.args.get('q', '')
    return jsonify(get_suggestions(username, partial))

# ========================================
# ROTAS DE ALERTAS
# ========================================

@chat_bp.route('/alerts', methods=['GET'])
def list_alerts():
    username = session.get('user_name') or session.get('usuario')
    return jsonify(get_alerts(username))

@chat_bp.route('/alerts', methods=['POST'])
def new_alert():
    username = session.get('user_name') or session.get('usuario')
    data = request.json
    add_alert(username, data['title'], data['sql'], data['type'], data['threshold'])
    return jsonify({'success': True})

@chat_bp.route('/alerts/<int:alert_id>', methods=['DELETE'])
def remove_alert(alert_id):
    delete_alert(alert_id)
    return jsonify({'success': True})

# ========================================
# ROTAS DE MEMÓRIA
# ========================================

@chat_bp.route('/memory/profile', methods=['GET'])
def get_memory_profile():
    username = session.get('user_name') or session.get('usuario')
    if not username:
        return jsonify({'error': 'Não logado'}), 401
    
    patterns = memoria_system.analyze_user_patterns(username)
    context = memoria_system.get_user_memory_context(username)
    
    return jsonify({
        'patterns': patterns,
        'context': context,
        'username': username
    })

@chat_bp.route('/memory/profile', methods=['POST'])
def update_memory_profile():
    username = session.get('user_name') or session.get('usuario')
    if not username:
        return jsonify({'error': 'Não logado'}), 401
    
    memoria_system.update_user_profile(username)
    return jsonify({'success': True, 'message': 'Perfil atualizado'})

@chat_bp.route('/memory/learn', methods=['POST'])
def add_contextual_memory():
    username = session.get('user_name') or session.get('usuario')
    if not username:
        return jsonify({'error': 'Não logado'}), 401
    
    data = request.json
    content = data.get('content')
    context_type = data.get('context_type', 'general')
    importance = data.get('importance', 1)
    expires_days = data.get('expires_days')
    
    if not content:
        return jsonify({'error': 'Conteúdo é obrigatório'}), 400
    
    memoria_system.learn_contextual_fact(username, content, context_type, importance, expires_days)
    return jsonify({'success': True})

@chat_bp.route('/memory/context', methods=['GET'])
def get_memory_context():
    username = session.get('user_name') or session.get('usuario')
    if not username:
        return jsonify({'error': 'Não logado'}), 401
    
    context = memoria_system.get_user_memory_context(username)
    return jsonify({'context': context})

@chat_bp.route('/memory/consolidate', methods=['POST'])
def consolidate_memories():
    username = session.get('user_name') or session.get('usuario')
    if not username:
        return jsonify({'error': 'Não logado'}), 401
    
    memoria_system.consolidate_memories(username)
    return jsonify({'success': True, 'message': 'Memórias consolidadas'})

@chat_bp.route('/memory/sentiment', methods=['GET'])
def get_sentiment_analysis():
    username = session.get('user_name') or session.get('usuario')
    if not username:
        return jsonify({'error': 'Não logado'}), 401
    
    from ..IA_CORE.PERSISTENCIA.db_history import get_db_connection
    conn = get_db_connection()
    cursor = conn.cursor()
    
    cursor.execute('''
    SELECT content, created_at FROM contextual_memory
    WHERE user_name = ? AND context_type = 'feedback'
    AND created_at >= datetime('now', '-7 days')
    ORDER BY created_at DESC
    ''', (username,))
    
    feedbacks = [dict(row) for row in cursor.fetchall()]
    conn.close()
    
    positive_count = sum(1 for f in feedbacks if any(word in f['content'].lower() for word in ['perfeito', 'ajudou', 'bom']))
    negative_count = sum(1 for f in feedbacks if any(word in f['content'].lower() for word in ['insatisfação', 'confuso', 'não']))
    
    sentiment_score = (positive_count - negative_count) / max(len(feedbacks), 1)
    
    return jsonify({
        'sentiment_score': round(sentiment_score, 2),
        'feedback_count': len(feedbacks),
        'recent_feedbacks': feedbacks[:5]
    })

