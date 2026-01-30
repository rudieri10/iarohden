from ..CORE.ai_client import AIClient
import json

class InsightFormatter:
    """
    Tradutor Técnico -> Humano.
    Transforma linhas de banco em respostas úteis.
    """
    
    def __init__(self):
        self.ai = AIClient()
        
    def format_response(self, process_result: dict, user_history: list = None) -> str:
        """Gera resposta natural baseada nos dados e adiciona sugestões."""
        
        response_text = ""
        
        # Se for resposta direta do chat (sem dados)
        if process_result.get("type") == "chat":
            response_text = process_result.get("direct_response")
        
        elif process_result.get("error"):
            error = process_result.get("error")
            sql = process_result.get("generated_sql")
            response_text = f"😕 Tive um problema técnico ao buscar isso.\n\nO erro foi: `{error}`\n\nTentei executar: `{sql}`"
            
        elif not process_result.get("results") and process_result.get("type") != "chat":
            sql = process_result.get("generated_sql")
            response_text = f"🔍 Não encontrei nenhum dado correspondente à sua busca.\n\n_SQL Executado: `{sql}`_\n\nTente ser mais específico ou verificar a grafia."
            
        else:
            # Caso com dados
            sql = process_result.get("generated_sql")
            data = process_result.get("results")
            question = process_result.get("user_message")
            
            # Amostra de dados para IA não estourar token
            data_sample = data[:5] if data else []
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
            
            response_text = self.ai.generate_text(prompt)

        # Adicionar sugestões contextuais
        user_query = process_result.get("user_message", "")
        suggestions = self.generate_contextual_suggestions(user_query, process_result, user_history)
        
        if suggestions:
            response_text += "\n\n💡 **Sugestões:**\n" + "\n".join([f"- {s}" for s in suggestions])
            
        return response_text

    def generate_contextual_suggestions(self, user_query: str, response_data: dict, 
                                      user_history: list = None) -> list:
        """Gera sugestões baseadas no contexto atual"""
        suggestions = []
        query_lower = user_query.lower()
        
        # 1. Sugestões baseadas em Vendas
        if 'vendas' in query_lower or 'faturamento' in query_lower:
            suggestions.extend([
                "Quer ver a comparação com o mês anterior?",
                "Deseja analisar por região ou vendedor?",
                "Posso mostrar a projeção para o próximo mês"
            ])
            
        # 2. Sugestões baseadas em Clientes
        elif 'cliente' in query_lower or 'comprador' in query_lower:
            suggestions.extend([
                "Verificar histórico de compras deste cliente?",
                "Listar produtos mais comprados por ele?",
                "Verificar status financeiro/limite de crédito"
            ])
            
        # 3. Sugestões baseadas em Produtos
        elif 'produto' in query_lower or 'estoque' in query_lower:
            suggestions.extend([
                "Verificar giro de estoque deste item?",
                "Comparar vendas com produtos similares?",
                "Verificar fornecedores deste material"
            ])
            
        # 4. Sugestões baseadas em Erros
        if response_data.get('error'):
            suggestions.append("Tente reformular a pergunta com outros termos.")
            suggestions.append("Posso listar as tabelas disponíveis para ajuda.")

        # 5. Sugestões genéricas se a lista estiver vazia e não for chat casual
        if not suggestions and response_data.get('type') != 'chat':
            suggestions.extend([
                "Detalhar mais estes dados?",
                "Exportar para Excel?",
                "Criar um gráfico com estes números?"
            ])
            
        # Limitar a 3 sugestões para não poluir
        return suggestions[:3]
