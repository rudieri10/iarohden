import logging
from ..CORE.connection_manager import ConnectionManager
from ..CORE.ai_client import AIClient

logger = logging.getLogger(__name__)

class DataValidator:
    """
    VALIDATOR: O Advogado do Diabo.
    Testa hipóteses contra a realidade (banco de dados).
    """
    
    def __init__(self):
        self.conn_manager = ConnectionManager()
        self.ai = AIClient()
        
    def validate_hypothesis(self, hypothesis: str) -> dict:
        """
        Gera uma query para validar uma hipótese e executa.
        Retorna veredito.
        """
        # 1. IA gera query de validação
        prompt = f"""
        Hipótese: "{hypothesis}"
        Gere uma consulta SQL Oracle para validar se essa hipótese é verdadeira.
        Exemplo: Se a hipótese é "Coluna STATUS é sempre 'A'", gere "SELECT COUNT(*) FROM table WHERE STATUS <> 'A'".
        Retorne apenas a query SQL crua.
        """
        validation_sql = self.ai.generate_text(prompt)
        
        # Limpeza básica
        validation_sql = validation_sql.replace("```sql", "").replace("```", "").strip()
        if ";" in validation_sql: validation_sql = validation_sql.replace(";", "")
        
        try:
            # 2. Executa validação
            result = self.conn_manager.execute_query(validation_sql)
            
            # 3. Analisa resultado
            count = 0
            if result and len(result) > 0:
                # Assume que a query retorna um count ou similar na primeira coluna
                first_val = list(result[0].values())[0]
                count = int(first_val) if str(first_val).isdigit() else 0
                
            # Se count > 0, existem exceções à regra -> Hipótese Falsa (ou parcial)
            # Se count == 0, não existem exceções -> Hipótese Verdadeira
            
            is_valid = (count == 0)
            
            return {
                "hypothesis": hypothesis,
                "validation_sql": validation_sql,
                "exceptions_count": count,
                "is_confirmed": is_valid,
                "verdict": "CONFIRMED" if is_valid else "REFUTED"
            }
            
        except Exception as e:
            logger.error(f"Erro ao validar hipótese: {e}")
            return {"error": str(e), "is_confirmed": False}
