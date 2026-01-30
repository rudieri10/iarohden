# -*- coding: utf-8 -*-
from typing import List, Dict, Any
from ..INTERFACE.feedback_analyzer import FeedbackAnalyzer

class ConversationAnalyzer:
    """
    Analisa padrões nas conversas para melhorar o sistema.
    Detecta problemas de fluxo, mudanças de tópico e nível de satisfação.
    """
    
    def __init__(self):
        self.feedback_analyzer = FeedbackAnalyzer()

    def analyze_conversation_flow(self, chat_history: List[Dict[str, Any]]) -> Dict[str, Any]:
        """
        Analisa o fluxo da conversa para detectar problemas.
        
        Args:
            chat_history: Lista de mensagens (dicts com 'role' e 'content')
            
        Returns:
            Dict com métricas da conversa
        """
        analysis = {
            'conversation_length': len(chat_history),
            'topic_changes': self._count_topic_changes(chat_history),
            'clarification_requests': self._count_clarifications(chat_history),
            'satisfaction_score': self._estimate_satisfaction(chat_history)
        }
        return analysis
    
    def suggest_improvements(self, analysis: Dict[str, Any]) -> List[str]:
        """
        Sugere melhorias baseadas na análise.
        
        Args:
            analysis: Resultado do analyze_conversation_flow
            
        Returns:
            Lista de sugestões em texto
        """
        suggestions = []
        
        if analysis['clarification_requests'] > 2:
            suggestions.append("Respostas podem estar sendo muito técnicas ou vagas. Tente simplificar.")
        
        if analysis['topic_changes'] > 5 and analysis['conversation_length'] < 10:
            suggestions.append("Usuário parece estar explorando sem foco. Oferecer menu de ajuda.")
            
        if analysis['satisfaction_score'] < 0.4:
            suggestions.append("Satisfação baixa detectada. Sugerir contato humano ou suporte.")
            
        if analysis['conversation_length'] > 20:
            suggestions.append("Conversa muito longa. Verificar se o problema foi resolvido.")
            
        return suggestions

    def _count_topic_changes(self, chat_history: List[Dict[str, Any]]) -> int:
        """
        Conta mudanças de tópico baseadas em heurísticas simples.
        (Em um sistema ideal, usaria embeddings para medir distância semântica)
        """
        changes = 0
        last_intent = None
        
        for msg in chat_history:
            if msg.get('role') != 'user':
                continue
                
            # Tenta pegar intenção dos metadados se existir (armazenado pelo ChatProcessor 2.0)
            metadata = msg.get('metadata', {})
            current_intent = metadata.get('intent')
            
            # Se não tiver metadados, tenta inferir grosseiramente por palavras-chave (fallback)
            if not current_intent:
                content = msg.get('content', '').lower()
                if any(w in content for w in ['venda', 'faturamento', 'meta']):
                    current_intent = 'business'
                elif any(w in content for w in ['erro', 'problema', 'ajuda']):
                    current_intent = 'support'
                elif any(w in content for w in ['oi', 'ola', 'bom dia']):
                    current_intent = 'casual'
                else:
                    current_intent = 'unknown'
            
            if last_intent and current_intent != last_intent and current_intent != 'unknown':
                changes += 1
                
            if current_intent != 'unknown':
                last_intent = current_intent
                
        return changes

    def _count_clarifications(self, chat_history: List[Dict[str, Any]]) -> int:
        """Conta quantas vezes o usuário pediu clarificação"""
        count = 0
        clarification_signals = self.feedback_analyzer.clarification_signals
        
        for msg in chat_history:
            if msg.get('role') == 'user':
                content = msg.get('content', '').lower()
                if any(signal in content for signal in clarification_signals):
                    count += 1
        return count

    def _estimate_satisfaction(self, chat_history: List[Dict[str, Any]]) -> float:
        """
        Estima satisfação de 0.0 a 1.0 baseada nas últimas interações.
        Começa neutro (0.5).
        """
        score = 0.5
        
        # Analisa apenas as últimas 5 interações para ter o sentimento atual
        recent_msgs = chat_history[-5:] if chat_history else []
        
        for msg in recent_msgs:
            if msg.get('role') == 'user':
                feedback = self.feedback_analyzer.analyze_user_response(msg.get('content', ''))
                
                if feedback['sentiment'] == 'positive':
                    score += 0.15
                elif feedback['sentiment'] == 'negative':
                    score -= 0.2
                elif feedback['sentiment'] == 'clarification':
                    score -= 0.05
                    
        # Clamp score between 0.0 and 1.0
        return max(0.0, min(1.0, score))
