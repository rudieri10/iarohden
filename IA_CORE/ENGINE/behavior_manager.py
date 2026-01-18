
"""
BEHAVIOR MANAGER - Sistema de Gestão de Comportamento Dinâmico
Gerencia padrões de conversação e comportamentos aprendidos para eliminar prompts estáticos.
"""

from typing import Dict, List, Optional
import sys
import os

# Adicionar o diretório raiz ao path para importações
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '../../..')))

from SETORES_MODULOS.ROHDEN_AI.IA_CORE.DATA.storage import DataStorage

class BehaviorManager:
    """Gerenciador de padrões comportamentais e exemplos dinâmicos"""
    
    def __init__(self):
        self.storage = DataStorage()
        
    def register_pattern(self, situation: str, user_input: str, ai_action: str, 
                         ai_response: str, category: str, success_indicator: str = None, 
                         tags: str = None):
        """Registra um novo padrão de comportamento no sistema"""
        pattern = {
            'situation': situation,
            'user_input': user_input,
            'ai_action': ai_action,
            'ai_response': ai_response,
            'category': category,
            'success_indicator': success_indicator,
            'tags': tags
        }
        self.storage.save_behavioral_pattern(pattern)
        return True
        
    def get_patterns_by_category(self, category: str, limit: int = 5) -> List[Dict]:
        """Recupera exemplos de uma categoria específica para alimentar a IA (Otimizado para Oracle)"""
        return self.storage.get_behavioral_patterns(category=category, limit=limit)
        
    def get_all_patterns(self, limit: int = 50) -> List[Dict]:
        """Recupera os padrões mais recentes (Otimizado para Oracle)"""
        return self.storage.get_behavioral_patterns(limit=limit)

    def find_similar_patterns(self, user_input: str, limit: int = 3) -> List[Dict]:
        """Busca padrões similares usando a nova busca vetorial do ChromaDB (Ultra Rápido)"""
        return self.storage.find_similar_patterns(user_input, limit=limit)

    def format_patterns_for_prompt(self, user_input: Optional[str] = None) -> str:
        """Formata os padrões em uma string legível. Se user_input for fornecido, busca similares."""
        if user_input:
            patterns = self.find_similar_patterns(user_input)
            header = "\n### VEJA COMO VOCÊ RESPONDEU SITUAÇÕES SIMILARES (APRENDA POR IMITAÇÃO):\n"
        else:
            patterns = self.get_all_patterns()[:5] # Fallback para os primeiros 5
            header = "\n### EXEMPLOS DE COMPORTAMENTO ESPERADO:\n"
        
        if not patterns:
            return ""
            
        formatted = header
        for p in patterns:
            formatted += f"\nUsuário: {p['user_input']}\n"
            formatted += f"Você: {p['ai_response']}\n"
            
        formatted += "\nAgora responda de forma similar, mantendo o tom e a estrutura dos exemplos acima.\n"
        return formatted
