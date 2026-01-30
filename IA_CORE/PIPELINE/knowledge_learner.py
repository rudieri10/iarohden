import logging
from ..STORAGE.knowledge_base import KnowledgeBase

logger = logging.getLogger(__name__)

class KnowledgeLearner:
    """
    LEARNER: O Escriba.
    Registra o que foi confirmado e aprendido.
    """
    
    def __init__(self):
        self.kb = KnowledgeBase()
        
    def learn_from_validation(self, validation_result: dict):
        """Aprende com o resultado de uma validação."""
        if validation_result.get("is_confirmed"):
            hypothesis = validation_result["hypothesis"]
            self.kb.learn_business_rule(
                table="Geral", # Idealmente extrair tabela da hipótese
                rule=hypothesis,
                context=f"Validado via SQL: {validation_result.get('validation_sql')}"
            )
            logger.info(f"Aprendido: {hypothesis}")

    def learn_from_user_feedback(self, user_query: str, ai_response: str, user_feedback: str):
        """Aprende com correção do usuário."""
        # Se usuário diz "Não, o status 'P' significa Pendente", aprendemos isso.
        # Simplificação: Salvamos o feedback cru para indexação
        self.kb.storage.save_knowledge(
            title=f"Feedback: {user_query[:30]}...",
            content=f"Contexto: {user_query} -> {ai_response}\nCorreção: {user_feedback}",
            category="user_correction",
            tags="feedback"
        )
