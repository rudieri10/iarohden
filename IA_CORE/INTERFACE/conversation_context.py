
from datetime import datetime
from ..MEMORIA.memoria_conversacional import MemoriaConversacional

class ConversationContext:
    """
    Gerenciador de Contexto Conversacional.
    Mantém o estado da conversa, entende referências a mensagens anteriores
    e constrói o cenário para a IA.
    """

    def __init__(self):
        self.memory_system = MemoriaConversacional()
        
    def build_context(self, user_message: str, username: str, history: list) -> dict:
        """
        Constrói o contexto completo para a interação atual.
        
        Args:
            user_message: Mensagem atual
            username: Nome do usuário
            history: Lista de mensagens anteriores [{'role': 'user', 'content': '...'}, ...]
            
        Returns:
            Dict com contexto enriquecido
        """
        # 1. Analisar Histórico Recente (Short-term memory)
        recent_history = history[-5:] if history else []
        
        # 2. Detectar Fluxo (Continuação vs Novo Tópico)
        flow_type = self._analyze_flow(user_message, recent_history)
        
        # 3. Recuperar Memória de Longo Prazo (Long-term memory)
        # Perfil do usuário, preferências, termos comuns
        user_profile = self.memory_system.get_user_profile(username)
        
        # 4. Extrair Entidades do Histórico (para resolver "ele", "disso", "aquilo")
        entities = self._extract_entities_from_history(recent_history)
        
        return {
            'flow_type': flow_type,
            'recent_history': recent_history,
            'user_profile': user_profile,
            'referenced_entities': entities,
            'timestamp': datetime.now().isoformat()
        }

    def _analyze_flow(self, message: str, history: list) -> str:
        """Determina o tipo de fluxo da conversa"""
        if not history:
            return 'new_conversation'
            
        last_msg = history[-1]['content'].lower()
        curr_msg = message.lower()
        
        # Palavras que indicam continuação
        continuation_markers = [
            'e', 'mas', 'então', 'porque', 'disso', 'ele', 'ela', 'esse', 'essa',
            'isso', 'aquilo', 'também', 'além disso', 'filtra', 'ordena'
        ]
        
        # Se começa com marcador de continuação
        if any(curr_msg.startswith(m + ' ') for m in continuation_markers):
            return 'continuation'
            
        # Se a mensagem anterior foi uma pergunta da IA
        if history[-1]['role'] == 'assistant' and '?' in last_msg:
            return 'answer'
            
        return 'new_topic' # Default

    def _extract_entities_from_history(self, history: list) -> dict:
        """
        Tenta extrair entidades mencionadas recentemente para resolver co-referências.
        Ex: Usuário falou de 'Cliente X' antes, agora diz 'vendas dele'.
        """
        entities = {
            'last_subject': None,
            'last_sql': None,
            'mentioned_tables': []
        }
        
        for msg in reversed(history):
            content = msg.get('content', '')
            
            # Tentar achar SQL anterior
            if 'SELECT' in content.upper() and 'FROM' in content.upper():
                entities['last_sql'] = content
                
            # (Aqui poderia ter uma extração de entidades mais complexa com NLP)
            
        return entities

    def enrich_prompt(self, base_prompt: str, context: dict) -> str:
        """Adiciona as informações de contexto ao prompt da IA"""
        
        context_str = "\n--- CONTEXTO CONVERSACIONAL ---\n"
        
        if context['flow_type'] == 'continuation':
            context_str += "📍 NOTA: O usuário está continuando o assunto anterior. Mantenha o contexto.\n"
        elif context['flow_type'] == 'new_topic':
            context_str += "📍 NOTA: Parece ser um novo tópico. Pode ignorar detalhes muito antigos.\n"
            
        if context['user_profile']:
            style = context['user_profile'].get('interaction_style', 'normal')
            context_str += f"👤 Estilo do Usuário: {style}\n"
            
        if context['referenced_entities'].get('last_sql'):
            context_str += f"📜 SQL Anterior (para referência): {context['referenced_entities']['last_sql']}\n"
            
        return base_prompt + context_str
