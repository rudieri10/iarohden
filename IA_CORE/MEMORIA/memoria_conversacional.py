import json
import re
import sys
import os
from datetime import datetime, timedelta
from collections import defaultdict, Counter
from ..PERSISTENCIA import db_history

class MemoriaConversacional:
    """Sistema de Memória Conversacional Inteligente usando SQLite"""
    
    def __init__(self):
        db_history.init_db()
        try:
            from ..TRAINING.passive_learner import PassiveLearner
            self.passive_learner = PassiveLearner()
        except ImportError:
            self.passive_learner = None
    
    def init_memory_tables(self):
        """Redirecionado para o db_history"""
        db_history.init_db()
    
    def analyze_user_patterns(self, user_name, days_back=30):
        """Analisa padrões do usuário nos últimos N dias usando SQLite"""
        conn = db_history.get_db_connection()
        if not conn: return {}
        cursor = conn.cursor()
        
        cutoff_date = (datetime.now() - timedelta(days=days_back)).strftime('%Y-%m-%d %H:%M:%S')
        
        # 1. Analisar perguntas frequentes
        cursor.execute('''
            SELECT m.content FROM tb_ai_messages m 
            JOIN tb_ai_chats c ON m.chat_id = c.id 
            WHERE c.user_name = ? AND m.role = 'user' 
            AND m.created_at >= ?
            ORDER BY m.created_at DESC
        ''', (user_name, cutoff_date))
        
        user_queries = [row[0] for row in cursor.fetchall()]
        
        # 2. Analisar respostas da IA para entender preferências
        cursor.execute('''
            SELECT m.content FROM tb_ai_messages m 
            JOIN tb_ai_chats c ON m.chat_id = c.id 
            WHERE c.user_name = ? AND m.role = 'assistant' 
            AND m.created_at >= ?
            ORDER BY m.created_at DESC
        ''', (user_name, cutoff_date))
        
        ai_responses = [row[0] for row in cursor.fetchall()]
        
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
            
            # 4. Detectar tipos de perguntas
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
    
    def get_user_profile(self, user_name):
        """Recupera o perfil do usuário do SQLite"""
        conn = db_history.get_db_connection()
        if not conn: return {}
        cursor = conn.cursor()
        
        try:
            cursor.execute('SELECT * FROM tb_ai_user_profile WHERE user_name = ?', (user_name,))
            row = cursor.fetchone()
            
            if not row:
                return {}
            
            profile = dict(row)
            
            if profile.get('preferences'):
                try:
                    profile['preferences'] = json.loads(profile['preferences'])
                except:
                    pass
            
            if profile.get('favorite_metrics'):
                try:
                    profile['favorite_metrics'] = json.loads(profile['favorite_metrics'])
                except:
                    pass
                    
            return profile
        finally:
            conn.close()

    def update_user_profile(self, user_name, preferences=None, interaction_style=None, favorite_metrics=None, response_format=None):
        """Atualiza o perfil do usuário no SQLite"""
        patterns = self.analyze_user_patterns(user_name) if not preferences else {}
        
        conn = db_history.get_db_connection()
        if not conn: return
        cursor = conn.cursor()
        
        try:
            prefs = json.dumps(preferences) if preferences else json.dumps(patterns)
            style = interaction_style if interaction_style else patterns.get('interaction_style', 'conversacional')
            metrics = json.dumps(favorite_metrics) if favorite_metrics else json.dumps(patterns.get('metrics_focus', {}))
            
            if response_format:
                fmt = response_format
            elif patterns.get('format_preferences'):
                fmt = max(patterns.get('format_preferences', {}), key=patterns.get('format_preferences', {}).get)
            else:
                fmt = 'tabela'

            cursor.execute('''
                INSERT INTO tb_ai_user_profile (user_name, preferences, interaction_style, favorite_metrics, response_format, updated_at)
                VALUES (?, ?, ?, ?, ?, CURRENT_TIMESTAMP)
                ON CONFLICT(user_name) DO UPDATE SET 
                    preferences = excluded.preferences,
                    interaction_style = excluded.interaction_style,
                    favorite_metrics = excluded.favorite_metrics,
                    response_format = excluded.response_format,
                    updated_at = CURRENT_TIMESTAMP
            ''', (user_name, prefs, style, metrics, fmt))
            
            conn.commit()
        finally:
            conn.close()

    def get_user_memory_context(self, user_name):
        """Obtém contexto de memória para enriquecer o prompt"""
        conn = db_history.get_db_connection()
        if not conn: return ""
        cursor = conn.cursor()
        
        context = ""
        try:
            # 1. Perfil do usuário
            cursor.execute('SELECT interaction_style, response_format FROM tb_ai_user_profile WHERE user_name = ?', (user_name,))
            profile = cursor.fetchone()
            
            if profile:
                context += f"\n### PERFIL: {user_name} ###\n"
                context += f"Estilo: {profile['interaction_style'] or 'conversacional'}\n"
                context += f"Formato: {profile['response_format'] or 'tabela'}\n"
            
            # 2. Memória mais importante apenas
            cursor.execute('''
                SELECT content FROM tb_ai_contextual_memory 
                WHERE user_name = ? AND importance >= 3
                ORDER BY importance DESC, created_at DESC
                LIMIT 1
            ''', (user_name,))
            
            top_memory = cursor.fetchone()
            if top_memory:
                context += f"\n### MEMÓRIA ###\n{top_memory[0]}\n"
        finally:
            conn.close()
            
        return context
    
    def learn_contextual_fact(self, user_name, content, context_type='general', importance=1, expires_days=None):
        """Adiciona fato contextual à memória no SQLite"""
        conn = db_history.get_db_connection()
        if not conn: return
        cursor = conn.cursor()
        
        try:
            expires_at = None
            if expires_days:
                expires_at = (datetime.now() + timedelta(days=expires_days)).strftime('%Y-%m-%d %H:%M:%S')
            
            cursor.execute('''
                INSERT INTO tb_ai_contextual_memory (user_name, context_type, content, importance, expires_at)
                VALUES (?, ?, ?, ?, ?)
            ''', (user_name, context_type, content, importance, expires_at))
            
            conn.commit()
        finally:
            conn.close()
    
    def record_problem_solution(self, user_name, problem_type, solution_used, sql_pattern, success_rating):
        """Registra solução de problema para aprendizado futuro no SQLite"""
        conn = db_history.get_db_connection()
        if not conn: return
        cursor = conn.cursor()
        
        try:
            cursor.execute('''
                INSERT INTO tb_ai_problem_context (user_name, problem_type, solution_used, sql_pattern, success_rating)
                VALUES (?, ?, ?, ?, ?)
            ''', (user_name, problem_type, solution_used, sql_pattern, success_rating))
            
            conn.commit()
        finally:
            conn.close()
    
    def consolidate_memories(self, user_name):
        """Consolida memórias similares e resolve contradições no SQLite"""
        conn = db_history.get_db_connection()
        if not conn: return
        cursor = conn.cursor()
        
        try:
            # 1. Buscar todas as memórias do usuário
            cursor.execute('''
                SELECT id, content, context_type, importance, created_at 
                FROM tb_ai_contextual_memory 
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
                    if other_memory['id'] in processed_ids or other_memory['id'] == memory['id']:
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
        finally:
            conn.close()
    
    def _calculate_similarity(self, text1, text2):
        """Calcula similaridade entre dois textos"""
        words1 = set(text1.lower().split())
        words2 = set(text2.lower().split())
        
        intersection = words1.intersection(words2)
        union = words1.union(words2)
        
        return len(intersection) / len(union) if union else 0
    
    def _consolidate_memory_group(self, user_name, group, cursor):
        """Consolida um grupo de memórias similares em uma única memória mais forte"""
        # A memória mais recente ou com maior importância sobrevive
        survivor = max(group, key=lambda m: (m['importance'], m['created_at']))
        
        # Aumentar a importância da sobrevivente
        new_importance = min(5, survivor['importance'] + 1)
        
        # Deletar todas as outras do grupo
        other_ids = [m['id'] for m in group if m['id'] != survivor['id']]
        if other_ids:
            cursor.execute(f"DELETE FROM tb_ai_contextual_memory WHERE id IN ({','.join(['?']*len(other_ids))})", other_ids)
            
        # Atualizar a sobrevivente
        cursor.execute("UPDATE tb_ai_contextual_memory SET importance = ? WHERE id = ?", (new_importance, survivor['id']))

    def _resolve_contradictions(self, user_name, cursor):
        """Tenta resolver contradições simples na memória"""
        # 1. Buscar memórias por tipo para comparar
        cursor.execute('''
            SELECT id, content, context_type, created_at 
            FROM tb_ai_contextual_memory 
            WHERE user_name = ?
            ORDER BY context_type, created_at DESC
        ''', (user_name,))
        
        memories = [dict(row) for row in cursor.fetchall()]
        
        # Agrupar por tipo
        by_type = {}
        for m in memories:
            t = m['context_type']
            if t not in by_type: by_type[t] = []
            by_type[t].append(m)
            
        # Para cada tipo, se houver muitas memórias, remove as mais antigas que overlap keywords
        for t, group in by_type.items():
            if len(group) < 2: continue
            
            to_delete = []
            for i in range(len(group)):
                for j in range(i + 1, len(group)):
                    m1 = group[i] # Mais recente
                    m2 = group[j] # Mais antiga
                    
                    if m2['id'] in to_delete: continue
                    
                    # Se houver overlap de palavras chave importantes (> 50%)
                    # consideramos que a mais recente atualiza a mais antiga
                    words1 = set(re.findall(r'\w{4,}', m1['content'].lower()))
                    words2 = set(re.findall(r'\w{4,}', m2['content'].lower()))
                    
                    if not words1 or not words2: continue
                    
                    overlap = words1.intersection(words2)
                    if len(overlap) / min(len(words1), len(words2)) > 0.5:
                        to_delete.append(m2['id'])
            
            if to_delete:
                cursor.execute(f"DELETE FROM tb_ai_contextual_memory WHERE id IN ({','.join(['?']*len(to_delete))})", to_delete)
                print(f"🧹 Resolvidas {len(to_delete)} contradições/redundâncias para {user_name} no tipo {t}")
    
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
        
        return (positive_count - negative_count) / (total_indicators if total_indicators > 0 else 1)
    
    def _is_repeated_question(self, user_name, current_query):
        """Verifica se a pergunta atual é similar a perguntas recentes"""
        conn = db_history.get_db_connection()
        if not conn: return False
        cursor = conn.cursor()
        
        # Buscar perguntas recentes (últimas 24 horas)
        try:
            cursor.execute('''
            SELECT m.content FROM tb_ai_messages m
            JOIN tb_ai_chats c ON m.chat_id = c.id
            WHERE c.user_name = ? AND m.role = 'user'
            AND m.created_at >= datetime('now', '-1 day')
            ORDER BY m.created_at DESC
            LIMIT 10
            ''', (user_name,))
            
            recent_queries = [row['content'] for row in cursor.fetchall()]
        except Exception as e:
            print(f"Erro ao buscar perguntas repetidas: {e}")
            recent_queries = []
        finally:
            conn.close()
        
        # Verificar similaridade com perguntas recentes
        for query in recent_queries:
            similarity = self._calculate_similarity(current_query, query)
            if similarity > 0.8:  # 80% de similaridade indica repetição
                return True
        
        return False
    
    def auto_adjust_success_ratings(self, user_name):
        """Ajusta automaticamente success_rating baseado no feedback"""
        conn = db_history.get_db_connection()
        if not conn: return
        cursor = conn.cursor()
        
        # Buscar feedbacks recentes
        try:
            cursor.execute('''
            SELECT content, created_at FROM tb_ai_contextual_memory
            WHERE user_name = ? AND context_type = 'feedback'
            AND created_at >= datetime('now', '-7 days')
            ''', (user_name,))
            
            feedbacks = [dict(row) for row in cursor.fetchall()]
            
            # Analisar feedbacks para ajustar ratings
            for feedback in feedbacks:
                if 'insatisfação' in feedback['content'].lower():
                    # Reduzir rating de problemas recentes
                    cursor.execute('''
                    UPDATE tb_ai_problem_context 
                    SET success_rating = MAX(1, success_rating - 1)
                    WHERE user_name = ? AND created_at >= datetime('now', '-3 days')
                    ''', (user_name,))
                elif 'ajudou' in feedback['content'].lower() or 'perfeito' in feedback['content'].lower():
                    # Aumentar rating de problemas recentes
                    cursor.execute('''
                    UPDATE tb_ai_problem_context 
                    SET success_rating = MIN(5, success_rating + 1)
                    WHERE user_name = ? AND created_at >= datetime('now', '-3 days')
                    ''', (user_name,))
            
            conn.commit()
        except Exception as e:
            print(f"Erro ao ajustar ratings de sucesso: {e}")
        finally:
            conn.close()
    
    def extract_learning_from_interaction(self, user_name, user_query, ai_response):
        """Extrai aprendizado automático da interação com análise de sentimento (otimizado)"""
        # 1. Aprendizado Passivo Semântico (Novo sistema robusto)
        try:
            if self.passive_learner:
                self.passive_learner.analyze_interaction(user_name, user_query, ai_response)
        except Exception as e:
            print(f"Erro no aprendizado passivo: {e}")

        # 2. Detectar preferências de formato (legado/rápido)
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
