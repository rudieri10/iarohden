from typing import List, Dict, Optional
from .simple_storage import SimpleStorage

class KnowledgeBase:
    """
    Gestão de Conhecimento de Negócio.
    Abstrai o armazenamento para focar em regras e insights.
    """
    
    def __init__(self):
        self.storage = SimpleStorage()
        
    def learn_business_rule(self, table: str, rule: str, context: str):
        """Aprende uma nova regra de negócio confirmada."""
        title = f"Regra: {table}"
        content = f"Tabela: {table}\nRegra: {rule}\nContexto: {context}"
        self.storage.save_knowledge(title, content, category="business_rule", tags=f"table:{table}")
        
    def get_table_rules(self, table: str) -> List[str]:
        """Recupera regras conhecidas para uma tabela."""
        results = self.storage.find_knowledge(f"Regra para tabela {table}", limit=10)
        rules = []
        for item in results:
            if item.get("category") == "business_rule" and table in item.get("title", ""):
                rules.append(item["content"])
        return rules

    def learn_user_preference(self, user: str, preference: str):
        """Aprende preferência do usuário."""
        self.storage.save_knowledge(
            title=f"Pref: {user}", 
            content=preference, 
            category="user_preference", 
            tags=f"user:{user}"
        )
