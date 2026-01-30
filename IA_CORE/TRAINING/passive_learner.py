import json
import re
from datetime import datetime
from ..DATA.storage import DataStorage
from ..ENGINE.vector_manager import VectorManager
from .ai_client import AIClient
from ..INTERFACE.feedback_analyzer import FeedbackAnalyzer
import os

from .analise_temporal import DataInsightEngine

class PassiveLearner:
    """
    Sistema de Aprendizado Passivo.
    Extrai conhecimento implícito e gerencia conflitos.
    """
    
    def __init__(self):
        self.storage = DataStorage()
        self.vector_manager = VectorManager()
        self.data_engine = DataInsightEngine()
        self.ai_client = AIClient()
        self.feedback_analyzer = FeedbackAnalyzer()
        
    def analyze_interaction(self, user_name, user_query, ai_response, chat_id=None):
        """Analisa a interação para aprendizado evolutivo."""
        learned_facts = []
        
        # Analisa feedback/sentimento
        feedback = self.feedback_analyzer.analyze_user_response(user_query)
        is_correction = feedback['sentiment'] == 'negative'
        is_positive = feedback['sentiment'] == 'positive'
        
        # Se for apenas um agradecimento ou feedback curto, ignorar extração complexa
        if is_positive and len(user_query.split()) < 5:
            return []

        potential_score = self._evaluate_knowledge_potential(user_query, ai_response, is_correction)
        
        if potential_score > 0.4:
            semantic_facts = self._extract_multidimensional_knowledge(user_query, ai_response, is_correction)
            
            for fact in semantic_facts:
                if fact.get('category') == 'Regra' and 'tabela' in fact.get('content', '').lower():
                    fact = self._validate_fact_against_data(fact)
                
                self._process_and_save_fact(user_name, fact, is_correction)
                learned_facts.append(fact)
        
        return learned_facts

    def _evaluate_knowledge_potential(self, query, response, is_correction):
        """Avalia o potencial de aprendizado (Heurística rápida)"""
        score = 0.0
        if not query or not response: return 0.0
        
        if is_correction: score += 0.5
        if len(query.split()) > 10: score += 0.2
        if any(k in query.lower() for k in ['sempre', 'nunca', 'regra', 'significa', 'chame']): score += 0.3
        if any(char.isdigit() for char in query): score += 0.1
        
        return min(score, 1.0)

    def _extract_multidimensional_knowledge(self, query, response, is_correction):
        """Extração estruturada via IA"""
        context_type = "CORREÇÃO" if is_correction else "INTERAÇÃO"
        
        prompt = f"""Analise esta {context_type} e extraia conhecimento:
Usuário: {query}
IA: {response}

Categorias: Regra, Preferência, Termo, Correção.
Retorne APENAS JSON: [{{"category": "...", "content": "...", "importance": 1-5, "is_correction": bool}}]"""

        try:
            result = self.ai_client.generate_json(prompt)
            if isinstance(result, list):
                return result
            # Se retornou um dict, verificamos se tem uma chave 'response' ou similar que contenha a lista
            # Mas o generate_json já tenta retornar o objeto parseado.
            return []
        except:
            return []

    def _validate_fact_against_data(self, fact):
        """Validação básica via DataInsightEngine"""
        content = fact['content'].lower()
        if "sempre" in content or "nunca" in content:
            fact['metadata'] = {"data_validated": False, "reason": "Aguardando validação"}
        return fact

    def _process_and_save_fact(self, user_name, fact, is_correction):
        """Salva o aprendizado com gerenciamento de conflitos"""
        content = fact['content']
        importance = fact.get('importance', 1)
        
        existing = self.storage.find_similar_knowledge(content, limit=1)
        if existing:
            old_fact = existing[0]
            if not (is_correction or importance > old_fact.get('priority', 0)):
                return

        vector = self.vector_manager.generate_embedding(content)
        if not vector: return
        
        tags = [f"user:{user_name}", f"category:{fact['category']}", f"importance:{importance}"]
        if is_correction: tags.append("correction")
        
        self.storage.save_knowledge(
            category='passive_learning',
            title=f"Aprendizado {fact['category']}",
            content=content,
            tags=", ".join(tags),
            priority=importance,
            embedding_vector=self.vector_manager.vector_to_blob(vector)
        )
        print(f"🧠 Aprendizado [{fact['category']}]: {content}")
