from ..CORE.ai_client import AIClient
from ..STORAGE.knowledge_base import KnowledgeBase
from ..CORE.connection_manager import ConnectionManager
from ..PIPELINE.observer import Observer

class ChatProcessor:
    """
    Processador de Chat Conversacional.
    Orquestra RAG -> SQL -> Resposta.
    """
    
    def __init__(self):
        self.ai = AIClient()
        self.kb = KnowledgeBase()
        self.conn = ConnectionManager()
        self.observer = Observer()
        
    def process_message(self, user_message: str) -> dict:
        """Processa mensagem do usuário e retorna resposta + dados."""
        
        # 1. Recuperar Contexto (RAG)
        context_items = self.kb.storage.find_knowledge(user_message, limit=3)
        context_str = "\n".join([f"- {item['content']}" for item in context_items])
        
        # 2. Analisar Intenção e Gerar SQL ou Resposta
        prompt = f"""
        Você é um assistente especialista em Oracle e dados corporativos.
        Contexto Conhecido:
        {context_str}
        
        Pergunta do Usuário: "{user_message}"
        
        Decida a melhor ação:
        1. Se a pergunta requer dados do banco de dados, gere um SQL Oracle.
           - IMPORTANTE: Para buscas por NOME ou TEXTO, use SEMPRE `UPPER(coluna) LIKE UPPER('%valor%')` para ignorar maiúsculas/minúsculas e acentos.
           - Exemplo: `WHERE UPPER(NOME) LIKE UPPER('%JOACIR%')`
        2. Se for uma saudação, pergunta geral ou conversa fiada, responda diretamente.
        
        Responda EXATAMENTE neste formato JSON (sem markdown):
        {{
            "action": "sql" OR "chat",
            "content": "SQL Query aqui" OR "Sua resposta de texto aqui"
        }}
        """
        
        response_text = self.ai.generate_text(prompt)
        
        # Limpar markdown JSON se houver
        response_text = response_text.replace("```json", "").replace("```", "").strip()
        
        import json
        try:
            decision = json.loads(response_text)
        except:
            # Fallback para comportamento antigo (assumir SQL se falhar o parse)
            decision = {"action": "sql", "content": response_text}

        if decision.get("action") == "chat":
            return {
                "type": "chat",
                "user_message": user_message,
                "direct_response": decision["content"],
                "context_used": context_items
            }

        # Se for SQL
        sql = decision["content"]
        # Limpeza SQL extra
        sql = sql.replace(";", "").strip()
        
        # 3. Executar SQL (com Observação)
        results = []
        error = None
        row_count = 0
        
        with self.observer.observe_query(sql) as ctx:
            try:
                results = self.conn.execute_query(sql)
                row_count = len(results)
            except Exception as e:
                error = str(e)
                results = [{"error": error}]
                
        return {
            "type": "data",
            "user_message": user_message,
            "generated_sql": sql,
            "results": results,
            "row_count": row_count,
            "error": error,
            "context_used": context_items
        }
