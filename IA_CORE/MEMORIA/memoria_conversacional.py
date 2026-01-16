import sqlite3
import json
import re
from datetime import datetime, timedelta
from collections import defaultdict, Counter
import os

# Importar conexão do PERSISTENCIA
from ..PERSISTENCIA.db_history import get_db_connection

DATA_DIR = os.path.join(os.path.dirname(os.path.dirname(__file__)), 'DATA')
DB_PATH = os.path.join(DATA_DIR, 'rohden_ai.db')

class MemoriaConversacional:
    """Sistema de Memória Conversacional Inteligente"""
    
    def __init__(self):
        self.init_memory_tables()
    
    def init_memory_tables(self):
        """Inicializa tabelas de memória conversacional"""
        conn = get_db_connection()
        cursor = conn.cursor()
        
        # Tabela de perfil do usuário
        cursor.execute('''
        CREATE TABLE IF NOT EXISTS user_profile (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            user_name TEXT NOT NULL UNIQUE,
            preferences TEXT, -- JSON com preferências
            interaction_style TEXT, -- direto, detalhado, visual
            favorite_metrics TEXT, -- JSON com métricas preferidas
            response_format TEXT, -- tabela, gráfico, resumo
            last_updated TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
        ''')
        
        # Tabela de contexto de problemas resolvidos
        cursor.execute('''
        CREATE TABLE IF NOT EXISTS problem_context (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            user_name TEXT NOT NULL,
            problem_type TEXT, -- tipo de problema
            solution_used TEXT, -- solução aplicada
            sql_pattern TEXT, -- padrão SQL usado
            success_rating INTEGER, -- 1-5
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
        ''')
        
        # Tabela de padrões de linguagem
        cursor.execute('''
        CREATE TABLE IF NOT EXISTS language_patterns (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            user_name TEXT NOT NULL,
            phrase_pattern TEXT, -- padrão de frase
            intent TEXT, -- intenção detectada
            frequency INTEGER DEFAULT 1,
            last_used TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
        ''')
        
        # Tabela de memória contextual (expansão do [LEARN])
        cursor.execute('''
        CREATE TABLE IF NOT EXISTS contextual_memory (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            user_name TEXT NOT NULL,
            context_type TEXT, -- preference, problem, metric, format
            content TEXT NOT NULL,
            importance INTEGER DEFAULT 1, -- 1-5
            expires_at TIMESTAMP, -- quando a memória expira
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
        ''')
        
        conn.commit()
        conn.close()
    
    def analyze_user_patterns(self, user_name, days_back=30):
        """Analisa padrões do usuário nos últimos N dias"""
        conn = get_db_connection()
        cursor = conn.cursor()
        
        cutoff_date = datetime.now() - timedelta(days=days_back)
        
        # 1. Analisar perguntas frequentes
        cursor.execute('''
        SELECT content FROM messages m 
        JOIN chats c ON m.chat_id = c.id 
        WHERE c.user_name = ? AND m.role = 'user' 
        AND m.created_at >= ?
        ORDER BY m.created_at DESC
        ''', (user_name, cutoff_date))
        
        user_queries = [row['content'] for row in cursor.fetchall()]
        
        # 2. Analisar respostas da IA para entender preferências
        cursor.execute('''
        SELECT content FROM messages m 
        JOIN chats c ON m.chat_id = c.id 
        WHERE c.user_name = ? AND m.role = 'assistant' 
        AND m.created_at >= ?
        ORDER BY m.created_at DESC
        ''', (user_name, cutoff_date))
        
        ai_responses = [row['content'] for row in cursor.fetchall()]
        
        conn.close()
        
        patterns = {
            'query_frequency': self._extract_query_patterns(user_queries),
            'format_preferences': self._analyze_format_preferences(ai_responses),
            'metrics_focus': self._extract_metrics_focus(user_queries + ai_responses),
            'interaction_style': self._determine_interaction_style(user_queries, ai_responses)
        }
        
        return patterns
    
    def _extract_query_patterns(self, queries):
        """Extrai padrões das consultas do usuário com detecção sofisticada"""
        patterns = Counter()
        
        # Mapeamento de sinônimos empresariais
        business_synonyms = {
            'cliente': ['cliente', 'contato', 'comprador', 'consumidor', 'pessoa'],
            'vendas': ['vendas', 'venda', 'vendeu', 'vendido', 'faturamento'],
            'financeiro': ['financeiro', 'dinheiro', 'caixa', 'financeira', 'contas'],
            'produto': ['produto', 'item', 'mercadoria', 'peça', 'serviço'],
            'pedido': ['pedido', 'encomenda', 'solicitação', 'requisição', 'ordem']
        }
        
        # Mapeamento de frases específicas para intenções
        phrase_intents = {
            'consulta_financeira': [
                'dinheiro em caixa', 'saldo', 'disponível', 'valor em caixa',
                'quanto tenho', 'total disponível', 'caixa atual'
            ],
            'comparativo': [
                'comparar', 'diferença', 'vs', 'contra', 'versus',
                'qual é melhor', 'mais que', 'menos que'
            ],
            'tendencia': [
                'tendência', 'tendencia', 'crescimento', 'queda', 'aumento',
                'diminuiu', 'subiu', 'evolução'
            ]
        }
        
        # Detecção de contexto temporal
        temporal_patterns = {
            'ontem': ['ontem', 'dia anterior', 'último dia'],
            'semana_passada': ['semana passada', 'última semana', 'semana anterior'],
            'mes_anterior': ['mês anterior', 'mês passado', 'último mês'],
            'hoje': ['hoje', 'agora', 'atualmente', 'neste momento']
        }
        
        for query in queries:
            query_lower = query.lower()
            
            # 1. Detectar sinônimos empresariais
            for main_term, synonyms in business_synonyms.items():
                if any(syn in query_lower for syn in synonyms):
                    patterns[main_term] += 1
            
            # 2. Detectar intenções específicas
            for intent, phrases in phrase_intents.items():
                if any(phrase in query_lower for phrase in phrases):
                    patterns[intent] += 1
            
            # 3. Detectar contexto temporal
            for temporal, time_phrases in temporal_patterns.items():
                if any(phrase in query_lower for phrase in time_phrases):
                    patterns[f'temporal_{temporal}'] += 1
            
            # 4. Detectar tipos de perguntas (mantido do original)
            if any(word in query_lower for word in ['qual', 'quantos', 'quantas', 'mostrar']):
                patterns['consulta_quantitativa'] += 1
            elif any(word in query_lower for word in ['prever', 'projeção', 'futuro', 'estimativa']):
                patterns['analise_preditiva'] += 1
            elif any(word in query_lower for word in ['problema', 'erro', 'defeito', 'issue']):
                patterns['problema_tecnico'] += 1
        
        return dict(patterns.most_common(15))
    
    def _analyze_format_preferences(self, responses):
        """Analisa preferências de formato das respostas"""
        format_count = Counter()
        
        for response in responses:
            if '|'.join(response.split()) in response or '---' in response:
                format_count['tabela'] += 1
            elif 'gráfico' in response.lower() or 'visual' in response.lower():
                format_count['grafico'] += 1
            elif len(response) < 200:
                format_count['resumo_curto'] += 1
            else:
                format_count['resumo_detalhado'] += 1
        
        return dict(format_count)
    
    def _extract_metrics_focus(self, texts):
        """Extrai métricas mais mencionadas"""
        metrics = Counter()
        
        metric_keywords = [
            'valor', 'total', 'média', 'quantidade', 'percentual', 'crescimento',
            'lucro', 'receita', 'custo', 'margem', 'estoque', 'vendas', 'clientes'
        ]
        
        for text in texts:
            for metric in metric_keywords:
                if metric in text.lower():
                    metrics[metric] += 1
        
        return dict(metrics.most_common(8))
    
    def _determine_interaction_style(self, queries, responses):
        """Determina o estilo de interação do usuário"""
        avg_query_length = sum(len(q.split()) for q in queries) / len(queries) if queries else 0
        formal_count = sum(1 for q in queries if any(word in q.lower() for word in ['por favor', 'gostaria', 'poderia']))
        
        if avg_query_length < 5:
            style = 'direto'
        elif formal_count > len(queries) * 0.3:
            style = 'formal'
        else:
            style = 'conversacional'
        
        return style
    
    def update_user_profile(self, user_name):
        """Atualiza o perfil do usuário com base nas análises"""
        patterns = self.analyze_user_patterns(user_name)
        
        conn = get_db_connection()
        cursor = conn.cursor()
        
        # Verificar se perfil já existe
        cursor.execute('SELECT id FROM user_profile WHERE user_name = ?', (user_name,))
        existing = cursor.fetchone()
        
        profile_data = {
            'preferences': patterns,
            'interaction_style': patterns['interaction_style'],
            'favorite_metrics': json.dumps(patterns['metrics_focus']),
            'response_format': max(patterns['format_preferences'], key=patterns['format_preferences'].get) if patterns['format_preferences'] else 'tabela'
        }
        
        if existing:
            cursor.execute('''
            UPDATE user_profile SET 
                preferences = ?, interaction_style = ?, 
                favorite_metrics = ?, response_format = ?,
                last_updated = CURRENT_TIMESTAMP
            WHERE user_name = ?
            ''', (
                json.dumps(patterns),
                profile_data['interaction_style'],
                profile_data['favorite_metrics'],
                profile_data['response_format'],
                user_name
            ))
        else:
            cursor.execute('''
            INSERT INTO user_profile (user_name, preferences, interaction_style, favorite_metrics, response_format)
            VALUES (?, ?, ?, ?, ?)
            ''', (user_name, json.dumps(patterns), profile_data['interaction_style'], 
                  profile_data['favorite_metrics'], profile_data['response_format']))
    def get_user_memory_context(self, user_name):
        """Obtém contexto de memória para enriquecer o prompt (versão ultra otimizada)"""
        conn = get_db_connection()
        cursor = conn.cursor()
        
        context = ""
        
        # 1. Perfil do usuário (única query)
        cursor.execute('SELECT interaction_style, response_format FROM user_profile WHERE user_name = ?', (user_name,))
        profile = cursor.fetchone()
        
        if profile:
            context += f"\n### PERFIL: {user_name} ###\n"
            context += f"Estilo: {profile['interaction_style'] or 'conversacional'}\n"
            context += f"Formato: {profile['response_format'] or 'tabela'}\n"
        
        # 2. Memória mais importante apenas (1 query)
        cursor.execute('''
        SELECT content FROM contextual_memory 
        WHERE user_name = ? AND importance >= 3
        ORDER BY importance DESC, created_at DESC
        LIMIT 1
        ''', (user_name,))
        
        top_memory = cursor.fetchone()
        if top_memory:
            context += f"\n### MEMÓRIA ###\n{top_memory['content']}\n"
        
        conn.close()
        return context
    
    def learn_contextual_fact(self, user_name, content, context_type='general', importance=1, expires_days=None):
        """Adiciona fato contextual à memória"""
        conn = get_db_connection()
        cursor = conn.cursor()
        
        expires_at = None
        if expires_days:
            expires_at = datetime.now() + timedelta(days=expires_days)
        
        cursor.execute('''
        INSERT INTO contextual_memory (user_name, context_type, content, importance, expires_at)
        VALUES (?, ?, ?, ?, ?)
        ''', (user_name, context_type, content, importance, expires_at))
        
        conn.commit()
        conn.close()
    
    def record_problem_solution(self, user_name, problem_type, solution_used, sql_pattern, success_rating):
        """Registra solução de problema para aprendizado futuro"""
        conn = get_db_connection()
        cursor = conn.cursor()
        
        cursor.execute('''
        INSERT INTO problem_context (user_name, problem_type, solution_used, sql_pattern, success_rating)
        VALUES (?, ?, ?, ?, ?)
        ''', (user_name, problem_type, solution_used, sql_pattern, success_rating))
        
        conn.commit()
        conn.close()
    
    def consolidate_memories(self, user_name):
        """Consolida memórias similares e resolve contradições"""
        conn = get_db_connection()
        cursor = conn.cursor()
        
        # 1. Buscar todas as memórias do usuário
        cursor.execute('''
        SELECT id, content, context_type, importance, created_at 
        FROM contextual_memory 
        WHERE user_name = ? AND (expires_at IS NULL OR expires_at > CURRENT_TIMESTAMP)
        ORDER BY created_at DESC
        ''', (user_name,))
        
        memories = [dict(row) for row in cursor.fetchall()]
        
        # 2. Agrupar memórias similares por similaridade de conteúdo
        memory_groups = []
        processed_ids = set()
        
        for memory in memories:
            if memory['id'] in processed_ids:
                continue
                
            # Encontrar memórias similares
            similar_memories = [memory]
            for other_memory in memories:
                if other_memory['id'] in processed_ids:
                    continue
                    
                similarity_score = self._calculate_similarity(memory['content'], other_memory['content'])
                if similarity_score > 0.7:  # 70% de similaridade
                    similar_memories.append(other_memory)
                    processed_ids.add(other_memory['id'])
            
            processed_ids.add(memory['id'])
            memory_groups.append(similar_memories)
        
        # 3. Processar cada grupo
        for group in memory_groups:
            if len(group) > 1:
                # Consolidar grupo de memórias similares
                self._consolidate_memory_group(user_name, group, cursor)
        
        # 4. Detectar contradições
        self._resolve_contradictions(user_name, cursor)
        
        conn.commit()
        conn.close()
    
    def _calculate_similarity(self, text1, text2):
        """Calcula similaridade entre dois textos"""
        words1 = set(text1.lower().split())
        words2 = set(text2.lower().split())
        
        intersection = words1.intersection(words2)
        union = words1.union(words2)
        
        return len(intersection) / len(union) if union else 0
    
    def _consolidate_memory_group(self, user_name, memory_group, cursor):
        """Consolida um grupo de memórias similares"""
        if len(memory_group) <= 1:
            return
        
        # Manter a memória mais importante e recente
        consolidated = max(memory_group, key=lambda m: (m['importance'], m['created_at']))
        
        # Combinar conteúdos similares
        all_contents = [m['content'] for m in memory_group]
        unique_words = set()
        for content in all_contents:
            unique_words.update(content.lower().split())
        
        # Criar conteúdo consolidado
        consolidated_content = f"Consolidado: {' '.join(list(unique_words)[:10])}"
        
        # Aumentar importância baseada na frequência
        consolidated['importance'] = min(5, consolidated['importance'] + len(memory_group) - 1)
        
        # Atualizar memória principal
        cursor.execute('''
        UPDATE contextual_memory 
        SET content = ?, importance = ?
        WHERE id = ?
        ''', (consolidated_content, consolidated['importance'], consolidated['id']))
        
        # Remover memórias duplicadas
        duplicate_ids = [m['id'] for m in memory_group if m['id'] != consolidated['id']]
        if duplicate_ids:
            placeholders = ','.join('?' * len(duplicate_ids))
            cursor.execute(f'DELETE FROM contextual_memory WHERE id IN ({placeholders})', duplicate_ids)
    
    def _resolve_contradictions(self, user_name, cursor):
        """Detecta e resolve contradições nas preferências"""
        # Buscar preferências que podem contradizer
        cursor.execute('''
        SELECT content, importance FROM contextual_memory 
        WHERE user_name = ? AND context_type = 'preference'
        AND (expires_at IS NULL OR expires_at > CURRENT_TIMESTAMP)
        ''', (user_name,))
        
        preferences = [dict(row) for row in cursor.fetchall()]
        
        # Detectar contradições de formato
        format_preferences = {}
        for pref in preferences:
            content = pref['content'].lower()
            if 'tabela' in content:
                format_preferences['tabela'] = format_preferences.get('tabela', 0) + pref['importance']
            elif 'gráfico' in content:
                format_preferences['grafico'] = format_preferences.get('grafico', 0) + pref['importance']
            elif 'resumo' in content:
                format_preferences['resumo'] = format_preferences.get('resumo', 0) + pref['importance']
        
        # Se houver contradição, manter a preferência mais forte
        if len(format_preferences) > 1:
            strongest_format = max(format_preferences, key=format_preferences.get)
            
            # Remover preferências contraditórias
            for format_type in format_preferences:
                if format_type != strongest_format:
                    cursor.execute('''
                    DELETE FROM contextual_memory 
                    WHERE user_name = ? AND context_type = 'preference' 
                    AND LOWER(content) LIKE ?
                    ''', (user_name, f'%{format_type}%'))
            
            # Adicionar memória consolidada
            self.learn_contextual_fact(
                user_name, 
                f"Preferência consolidada: {strongest_format}", 
                'preference', 
                5,  # Máxima importância
                90  # Expira em 90 dias
            )
    
    def extract_learning_from_interaction(self, user_name, user_query, ai_response):
        """Extrai aprendizado automático da interação com análise de sentimento"""
        # Detectar preferências de formato
        if 'tabela' in user_query.lower() or '|'.join(ai_response.split()) in ai_response:
            self.learn_contextual_fact(user_name, "Prefere ver dados em formato de tabela", "preference", 3)
        
        # Detectar métricas importantes
        metrics = re.findall(r'\b(valor|total|média|quantidade|receita|lucro)\b', user_query.lower())
        for metric in metrics[:2]:  # Limitar para não sobrecarregar
            self.learn_contextual_fact(user_name, f"Interesse em métricas de {metric}", "metric", 2)
        
        # ANÁLISE DE SENTIMENTO BÁSICA
        sentiment_score = self._analyze_sentiment(user_query, ai_response)
        
        # Se detectar insatisfação, ajustar aprendizado
        if sentiment_score < 0:
            # Usuário pode estar insatisfeito, registrar para ajuste futuro
            self.learn_contextual_fact(user_name, "Possível insatisfação com resposta anterior", "feedback", 4, 30)
        
        # Detectar padrão de repetição (possível resposta inadequada)
        if self._is_repeated_question(user_name, user_query):
            self.learn_contextual_fact(user_name, "Pergunta repetida - resposta anterior pode não ter sido clara", "feedback", 3, 15)
    
    def _analyze_sentiment(self, user_query, ai_response):
        """Análise de sentimento básica da interação"""
        user_lower = user_query.lower()
        
        # Indicadores positivos
        positive_indicators = [
            'obrigado', 'perfeito', 'excelente', 'ótimo', 'bom', 'ajudou',
            'funcionou', 'consegui', 'resolveu', 'show', 'legal'
        ]
        
        # Indicadores negativos
        negative_indicators = [
            'não', 'errado', 'erro', 'problema', 'dificuldade', 'não funcionou',
            'tentei', 'de novo', 'outra vez', 'não ajudou', 'confuso'
        ]
        
        # Contar indicadores
        positive_count = sum(1 for indicator in positive_indicators if indicator in user_lower)
        negative_count = sum(1 for indicator in negative_indicators if indicator in user_lower)
        
        # Calcular score simples (-1 a +1)
        total_indicators = positive_count + negative_count
        if total_indicators == 0:
            return 0  # Neutro
        
        return (positive_count - negative_count) / total_indicators
    
    def _is_repeated_question(self, user_name, current_query):
        """Verifica se a pergunta atual é similar a perguntas recentes"""
        conn = get_db_connection()
        cursor = conn.cursor()
        
        # Buscar perguntas recentes (últimas 24 horas)
        cursor.execute('''
        SELECT content FROM messages m
        JOIN chats c ON m.chat_id = c.id
        WHERE c.user_name = ? AND m.role = 'user'
        AND m.created_at >= datetime('now', '-1 day')
        ORDER BY m.created_at DESC
        LIMIT 10
        ''', (user_name,))
        
        recent_queries = [row['content'] for row in cursor.fetchall()]
        conn.close()
        
        # Verificar similaridade com perguntas recentes
        for query in recent_queries:
            similarity = self._calculate_similarity(current_query, query)
            if similarity > 0.8:  # 80% de similaridade indica repetição
                return True
        
        return False
    
    def auto_adjust_success_ratings(self, user_name):
        """Ajusta automaticamente success_rating baseado no feedback"""
        conn = get_db_connection()
        cursor = conn.cursor()
        
        # Buscar feedbacks recentes
        cursor.execute('''
        SELECT content, created_at FROM contextual_memory
        WHERE user_name = ? AND context_type = 'feedback'
        AND created_at >= datetime('now', '-7 days')
        ''', (user_name,))
        
        feedbacks = [dict(row) for row in cursor.fetchall()]
        
        # Analisar feedbacks para ajustar ratings
        for feedback in feedbacks:
            if 'insatisfação' in feedback['content'].lower():
                # Reduzir rating de problemas recentes
                cursor.execute('''
                UPDATE problem_context 
                SET success_rating = MAX(1, success_rating - 1)
                WHERE user_name = ? AND created_at >= datetime('now', '-3 days')
                ''', (user_name,))
            elif 'ajudou' in feedback['content'].lower() or 'perfeito' in feedback['content'].lower():
                # Aumentar rating de problemas recentes
                cursor.execute('''
                UPDATE problem_context 
                SET success_rating = MIN(5, success_rating + 1)
                WHERE user_name = ? AND created_at >= datetime('now', '-3 days')
                ''', (user_name,))
        
        conn.commit()
        conn.close()
    
    def extract_learning_from_interaction(self, user_name, user_query, ai_response):
        """Extrai aprendizado automático da interação com análise de sentimento (otimizado)"""
        # Detectar preferências de formato (apenas se claro)
        if 'tabela' in user_query.lower() and '|' in ai_response:
            self.learn_contextual_fact(user_name, "Prefere ver dados em formato de tabela", "preference", 3)
        
        # Detectar métricas importantes (limitado a 1 para performance)
        metrics = re.findall(r'\b(valor|total|média|quantidade|receita|lucro)\b', user_query.lower())
        if metrics:
            self.learn_contextual_fact(user_name, f"Interesse em métricas de {metrics[0]}", "metric", 2)
        
        # ANÁLISE DE SENTIMENTO BÁSICA (otimizada)
        sentiment_score = self._analyze_sentiment(user_query, ai_response)
        
        # Se detectar insatisfação, ajustar aprendizado
        if sentiment_score < 0:
            self.learn_contextual_fact(user_name, "Possível insatisfação com resposta anterior", "feedback", 4, 30)
        
        # Detectar padrão de repetição (apenas se for muito claro)
        if self._is_repeated_question(user_name, user_query):
            self.learn_contextual_fact(user_name, "Pergunta repetida - resposta anterior pode não ter sido clara", "feedback", 3, 15)
        
        # Detectar tipo de problema (simplificado)
        query_lower = user_query.lower()
        if any(word in query_lower for word in ['erro', 'problema', 'não funciona']):
            problem_type = "técnico"
        elif any(word in query_lower for word in ['comparar', 'diferença']):
            problem_type = "comparativo"
        elif any(word in query_lower for word in ['tendência', 'previsão']):
            problem_type = "preditivo"
        else:
            problem_type = "consulta"
        
        # Extrair padrão SQL se houver
        sql_match = re.search(r'\[SQL\](.*?)\[/SQL\]', ai_response, re.DOTALL | re.IGNORECASE)
        sql_pattern = sql_match.group(1).strip() if sql_match else None
        
        if sql_pattern and problem_type != "consulta":
            self.record_problem_solution(user_name, problem_type, ai_response[:200], sql_pattern, 4)
        
        # Consolidar memórias periodicamente (a cada 10 interações)
        import random
        if random.randint(1, 10) == 1:
            self.consolidate_memories(user_name)
        
        # Ajustar ratings baseado no feedback
        self.auto_adjust_success_ratings(user_name)

# Instância global para uso no sistema
memoria_system = MemoriaConversacional()
