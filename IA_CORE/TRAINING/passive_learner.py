import json
import re
from datetime import datetime
from ..DATA.storage import DataStorage
from ..ENGINE.vector_manager import VectorManager
import requests
import os

class PassiveLearner:
    """
    Sistema de Aprendizado Passivo Semântico.
    Extrai conhecimento implícito de interações sem depender de tags [LEARN].
    """
    
    def __init__(self):
        self.storage = DataStorage()
        self.vector_manager = VectorManager()
        self.ai_url = os.getenv("ROHDEN_AI_INTERNAL_URL", "http://192.168.1.217:5005/api/generate")
        
    def analyze_interaction(self, user_name, user_query, ai_response):
        """
        Analisa uma única interação para extrair conhecimento.
        """
        learned_facts = []
        
        # 1. Extração Rápida (Regex)
        business_rules = self._extract_business_rules(user_query, ai_response)
        learned_facts.extend(business_rules)
        
        # 2. Extração Semântica Profunda (IA) - Apenas se a conversa parecer rica em conhecimento
        if self._is_knowledge_rich(user_query):
            semantic_facts = self._extract_semantic_deep(user_query, ai_response)
            learned_facts.extend(semantic_facts)
        
        # 3. Extração de Preferências
        preferences = self._extract_preferences(user_query, ai_response)
        learned_facts.extend(preferences)
        
        # Salvar fatos novos
        for fact in learned_facts:
            self._save_learned_fact(user_name, fact)
            
        return learned_facts

    def _is_knowledge_rich(self, query):
        """Identifica se a pergunta contém afirmações ou definições"""
        keywords = ['sempre', 'nunca', 'chame', 'considere', 'é quando', 'significa', 'regra']
        return any(k in query.lower() for k in keywords) or len(query.split()) > 10

    def _extract_semantic_deep(self, query, response):
        """Usa a IA para extrair conhecimento implícito de forma semântica"""
        prompt = f"""
Analise a interação abaixo e extraia QUALQUER conhecimento implícito, regra de negócio ou preferência do usuário.
Ignore a parte técnica de SQL, foque no CONHECIMENTO DE NEGÓCIO.

Interação:
Usuário: {query}
IA: {response}

Extraia fatos no formato JSON:
[{{"category": "Regra/Preferência/Terminologia", "content": "Descrição do fato", "importance": 1-5}}]
Se não houver nada relevante, retorne [].
"""
        try:
            payload = {
                "model": "llama3.1-gguf",
                "prompt": prompt,
                "stream": False,
                "options": {"temperature": 0.1, "num_predict": 256}
            }
            res = requests.post(self.ai_url, json=payload, timeout=5)
            if res.status_code == 200:
                text = res.json().get("response", "")
                # Extrair JSON da resposta
                match = re.search(r'\[\s*\{.*\}\s*\]', text, re.DOTALL)
                if match:
                    return json.loads(match.group(0))
        except:
            pass
        return []

    def _extract_preferences(self, query, response):
        """Detecta preferências de exibição"""
        prefs = []
        query_lower = query.lower()
        
        if "em tabela" in query_lower or "formato de tabela" in query_lower:
            prefs.append({'category': 'Preferência', 'content': "Prefere visualização em tabelas", 'importance': 2})
        
        if "resumido" in query_lower or "direto" in query_lower:
            prefs.append({'category': 'Preferência', 'content': "Prefere respostas curtas e diretas", 'importance': 2})
            
        if "detalhado" in query_lower or "explique" in query_lower:
            prefs.append({'category': 'Preferência', 'content': "Prefere explicações detalhadas", 'importance': 2})
            
        return prefs

    def _extract_synonyms(self, query, response):
        """Detecta sinônimos ou termos específicos da empresa"""
        syns = []
        # Ex: "O que chamamos de 'X' é na verdade o campo 'Y'"
        match = re.search(r"['\"](.*?)['\"]\s+é\s+(o mesmo que|a mesma coisa que|o campo)\s+['\"](.*?)['\"]", query.lower())
        if match:
            term, _, target = match.groups()
            syns.append({
                'category': 'Sinônimo',
                'content': f"Termo '{term}' refere-se a '{target}'",
                'importance': 4
            })
        return syns

    def _save_learned_fact(self, user_name, fact):
        """Salva o fato aprendido no storage com vetorização para busca semântica"""
        content = fact['content']
        category = fact['category']
        importance = fact.get('importance', 1)
        
        # Verificar duplicados no storage
        existing = self.storage.get_knowledge(category='passive_learning')
        if any(f['content'].lower() == content.lower() for f in existing):
            return

        print(f"🧠 Aprendizado Passivo: {content}")
        
        # Gerar vetor para o fato aprendido
        vector = self.vector_manager.generate_embedding(content)
        vector_blob = self.vector_manager.vector_to_blob(vector) if vector else None
        
        self.storage.save_knowledge(
            category='passive_learning',
            title=f"Aprendizado: {category}",
            content=content,
            tags=f"user:{user_name}, passive, importance:{importance}",
            priority=importance,
            embedding_vector=vector_blob
        )
