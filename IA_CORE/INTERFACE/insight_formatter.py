from ..CORE.ai_client import AIClient
import json

class InsightFormatter:
    """
    Tradutor Técnico -> Humano.
    Transforma linhas de banco em respostas úteis.
    """
    
    def __init__(self):
        self.ai = AIClient()
        
    def format_response(self, process_result: dict) -> str:
        """Gera resposta natural baseada nos dados."""
        
        # Se for resposta direta do chat (sem dados)
        if process_result.get("type") == "chat":
            return process_result.get("direct_response")
        
        sql = process_result.get("generated_sql")
        data = process_result.get("results")
        error = process_result.get("error")
        question = process_result.get("user_message")
        
        if error:
            return f"😕 Tive um problema técnico ao buscar isso.\n\nO erro foi: `{error}`\n\nTentei executar: `{sql}`"
            
        if not data:
            return f"🔍 Não encontrei nenhum dado correspondente à sua busca.\n\n_SQL Executado: `{sql}`_\n\nTente ser mais específico ou verificar a grafia."
            
        # Amostra de dados para IA não estourar token
        data_sample = data[:5]
        count = process_result.get("row_count", 0)
        
        prompt = f"""
        Pergunta: "{question}"
        SQL Executado: "{sql}"
        Total de Linhas: {count}
        Amostra de Dados: {json.dumps(data_sample, default=str)}
        
        Responda ao usuário de forma natural, resumindo os insights.
        Se houver muitos dados, mencione o total.
        Use emojis moderados.
        """
        
        return self.ai.generate_text(prompt)
