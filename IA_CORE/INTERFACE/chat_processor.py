
import asyncio
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor

from ..CORE.ai_client import AIClient
from ..STORAGE.knowledge_base import KnowledgeBase
from ..CORE.connection_manager import ConnectionManager
from ..PIPELINE.observer import Observer
from .intent_classifier import IntentClassifier
from .conversation_context import ConversationContext
from ..CACHE.response_cache import ResponseCache
import json
import traceback

class ChatProcessor:
    """
    Processador de Chat Conversacional Inteligente 2.0.
    Orquestra Intent -> Context -> Action (RAG/SQL/Chat).
    Suporta processamento assíncrono para paralelismo.
    """
    
    def __init__(self):
        self.ai = AIClient()
        self.kb = KnowledgeBase()
        self.conn = ConnectionManager()
        self.observer = Observer()
        self.classifier = IntentClassifier()
        self.context_manager = ConversationContext()
        self.response_cache = ResponseCache()
        self._executor = ThreadPoolExecutor(max_workers=5)
        
    def process_message(self, user_message: str, username: str = 'user', chat_history: list = None) -> dict:
        """
        Wrapper síncrono para o processamento assíncrono.
        Mantém compatibilidade com rotas Flask síncronas.
        """
        try:
            # Tenta obter ou criar loop de evento
            try:
                loop = asyncio.get_event_loop()
            except RuntimeError:
                loop = asyncio.new_event_loop()
                asyncio.set_event_loop(loop)
                
            if loop.is_running():
                # Se já tem loop rodando (ex: uvicorn/asgi), usa run_coroutine_threadsafe ou similar
                # Mas para Flask WSGI padrão, isso não deve acontecer.
                # Fallback seguro: rodar síncrono se não conseguir async
                print("⚠️ Loop de evento já rodando, executando síncrono por segurança.")
                return self._process_message_sync(user_message, username, chat_history)
            else:
                return loop.run_until_complete(self.process_message_async(user_message, username, chat_history))
        except Exception as e:
            traceback.print_exc()
            return self._process_message_sync(user_message, username, chat_history)

    async def process_message_async(self, user_message: str, username: str = None, 
                                  chat_history: list = None) -> dict:
        """Processamento assíncrono com tarefas paralelas"""
        
        try:
            # 1. Verificar Cache Semântico (Rápido)
            cached_response = await self._check_cache_async(user_message)
            if cached_response:
                print(f"⚡ Resposta encontrada no cache semântico ({cached_response['similarity']:.2f})")
                return {
                    "type": "chat" if not cached_response.get('sql') else "data",
                    "direct_response": cached_response['response'],
                    "generated_sql": cached_response.get('sql'),
                    "intent": "cached",
                    "confidence": 1.0
                }

            # 2. Executar tarefas de análise em paralelo:
            # - Classificação de Intenção
            # - Construção de Contexto do Usuário
            # - Busca preliminar na Base de Conhecimento (para agilizar RAG se necessário)
            tasks = [
                self._classify_intent_async(user_message, chat_history),
                self._get_user_context_async(username, user_message, chat_history),
                self._search_knowledge_base_async(user_message)
            ]
            
            intent_result, user_context, kb_results = await asyncio.gather(*tasks)
            
            # Adicionar resultados da KB ao contexto se relevante
            if kb_results:
                user_context['kb_preview'] = kb_results
            
            # Log para debug
            print(f"🎯 Intenção: {intent_result['intent']} ({intent_result['confidence']:.2f}) - {intent_result['method']}")
            
            # 3. Roteamento Inteligente
            result = await self._route_by_intent_async(intent_result, user_message, user_context, chat_history)
            
            # 4. Salvar no Cache (se for sucesso e tiver dados úteis)
            if result.get('type') in ['data', 'chat'] and not result.get('error'):
                self._executor.submit(
                    self.response_cache.cache_response, 
                    user_message, 
                    result.get('direct_response') or result.get('generated_sql'), 
                    {'type': result.get('type')}
                )
                
            return result

        except Exception as e:
            traceback.print_exc()
            return {
                "type": "error",
                "error": str(e),
                "direct_response": "Desculpe, encontrei um erro interno no processamento paralelo."
            }

    # --- Métodos Auxiliares Assíncronos (Wrappers) ---

    async def _check_cache_async(self, message: str):
        loop = asyncio.get_event_loop()
        return await loop.run_in_executor(self._executor, self.response_cache.get_similar_response, message)

    async def _classify_intent_async(self, message: str, history: list):
        loop = asyncio.get_event_loop()
        # IntentClassifier pode precisar de contexto, mas aqui passamos None ou básico para paralelizar
        # O classificador deve ser robusto a context=None
        return await loop.run_in_executor(self._executor, self.classifier.classify, message, None)

    async def _get_user_context_async(self, username: str, message: str, history: list):
        loop = asyncio.get_event_loop()
        return await loop.run_in_executor(
            self._executor, 
            self.context_manager.build_context, 
            message, username, history or []
        )

    async def _search_knowledge_base_async(self, message: str):
        loop = asyncio.get_event_loop()
        return await loop.run_in_executor(
            self._executor, 
            self.kb.storage.find_knowledge, 
            message, 3
        )

    # --- Roteamento Assíncrono ---

    async def _route_by_intent_async(self, intent_result: dict, message: str, user_context: dict, history: list) -> dict:
        """Direciona para o handler correto de forma assíncrona"""
        intent = intent_result['intent']
        loop = asyncio.get_event_loop()
        
        # Mapeamento de handlers síncronos para execução em thread pool
        # (Idealmente converteríamos os handlers para async nativo, mas wrapper é seguro agora)
        
        if intent == 'greeting':
            return await loop.run_in_executor(self._executor, self._handle_casual_conversation, message, user_context, "greeting")
            
        elif intent == 'casual_chat':
            return await loop.run_in_executor(self._executor, self._handle_casual_conversation, message, user_context, "chat")
            
        elif intent in ['business_data', 'analytical']:
            return await loop.run_in_executor(self._executor, self._handle_data_request, message, user_context, history, intent)
            
        elif intent == 'operational':
            # Usa o kb_preview já carregado para evitar busca dupla se possível
            return await loop.run_in_executor(self._executor, self._handle_knowledge_request, message, user_context)
            
        elif intent == 'support':
            return await loop.run_in_executor(self._executor, self._handle_support_request, message, user_context)
            
        else:
            return await loop.run_in_executor(self._executor, self._handle_intelligent_routing, message, user_context, history)

    # --- Versão Síncrona Original (Renomeada para _process_message_sync) ---
    def _process_message_sync(self, user_message: str, username: str = 'user', chat_history: list = None) -> dict:
        """Versão síncrona legado/fallback"""
        try:
            # 1. Verificar Cache
            cached = self.response_cache.get_similar_response(user_message)
            if cached:
                return {
                    "type": "chat", "direct_response": cached['response'], 
                    "intent": "cached", "confidence": 1.0
                }

            # 1. Construir Contexto
            user_context = self.context_manager.build_context(user_message, username, chat_history or [])
            
            # 2. Classificar Intenção
            intent_result = self.classifier.classify(user_message, user_context)
            
            print(f"🎯 Intenção (Sync): {intent_result['intent']} ({intent_result['confidence']:.2f})")
            
            # 3. Roteamento
            result = self._route_by_intent(intent_result, user_message, user_context, chat_history)
            
            # 4. Cache
            if result.get('type') in ['data', 'chat'] and not result.get('error'):
                self.response_cache.cache_response(user_message, result.get('direct_response') or result.get('generated_sql'))
                
            return result
            
        except Exception as e:
            traceback.print_exc()
            return {
                "type": "error",
                "error": str(e),
                "direct_response": "Erro interno (Sync)."
            }


    def _route_by_intent(self, intent_result: dict, message: str, user_context: dict, history: list) -> dict:
        """Direciona para o handler correto baseado na intenção"""
        intent = intent_result['intent']
        confidence = intent_result['confidence']
        
        # Handlers Específicos
        if intent == 'greeting':
            return self._handle_casual_conversation(message, user_context, "greeting")
            
        elif intent == 'casual_chat':
            return self._handle_casual_conversation(message, user_context, "chat")
            
        elif intent in ['business_data', 'analytical']:
            return self._handle_data_request(message, user_context, history, intent)
            
        elif intent == 'operational':
            return self._handle_knowledge_request(message, user_context)
            
        elif intent == 'support':
            return self._handle_support_request(message, user_context)
            
        # Fallback ou Queries Complexas -> Roteamento Inteligente (IA decide)
        else:
            return self._handle_intelligent_routing(message, user_context, history)

    def _handle_casual_conversation(self, message: str, user_context: dict, sub_type: str) -> dict:
        """Trata conversas casuais e saudações sem gastar tokens excessivos"""
        
        # Se for apenas saudação simples, resposta rápida
        if sub_type == 'greeting' and len(message.split()) < 3:
            hour = datetime.now().hour
            greeting = "Bom dia" if 5 <= hour < 12 else "Boa tarde" if 12 <= hour < 18 else "Boa noite"
            name = user_context.get('user_profile', {}).get('user_name', '')
            name_part = f", {name}" if name else ""
            
            return {
                "type": "chat",
                "direct_response": f"{greeting}{name_part}! Como posso ajudar com os dados da Rohden hoje? 🚀"
            }
            
        # Conversa casual um pouco mais elaborada
        prompt = f"""
        Responda de forma amigável, profissional e breve ao usuário corporativo.
        Mensagem: "{message}"
        Contexto: O usuário está no sistema SYS_ROHDEN. Mantenha o foco em ajudar, mas seja simpático.
        """
        response = self.ai.generate_text(prompt, max_tokens=100)
        
        return {
            "type": "chat",
            "direct_response": response,
            "intent": "casual"
        }

    def _handle_data_request(self, message: str, user_context: dict, history: list, intent: str) -> dict:
        """Trata solicitações de dados (Gera SQL)"""
        
        # Recuperar conhecimento RAG para ajudar no SQL
        kb_items = self.kb.storage.find_knowledge(message, limit=3)
        context_str = "\n".join([f"- {item['content']}" for item in kb_items])
        
        # Enriquecer prompt com contexto conversacional
        base_prompt = f"""
        Você é um especialista em Oracle SQL para o sistema SYS_ROHDEN.
        Intenção Detectada: {intent.upper()}
        
        Contexto de Tabelas/Regras:
        {context_str}
        
        Pergunta: "{message}"
        
        Gere um SQL Oracle válido.
        - Use UPPER() para comparações de texto (ex: UPPER(nome) LIKE '%ROHDEN%').
        - Use NVL() para tratar nulos.
        - Limite resultados a 50 linhas se não especificado (ROWNUM <= 50).
        - NÃO use ponto e vírgula (;) no final.
        - Se a pergunta for sobre CONTATO, busque colunas de TELEFONE, EMAIL, CELULAR, RAMAL.
        
        Retorne APENAS o SQL puro (sem markdown, sem explicações).
        """
        
        # Adiciona contexto de fluxo (filtros anteriores, etc)
        full_prompt = self.context_manager.enrich_prompt(base_prompt, user_context)
        
        sql = self.ai.generate_text(full_prompt).replace("```sql", "").replace("```", "").strip()
        
        # Remover ponto e vírgula final se existir (causa ORA-00933)
        if sql.endswith(';'):
            sql = sql[:-1]
        
        # Executar
        results = []
        error = None
        row_count = 0
        
        with self.observer.observe_query(sql) as ctx:
            try:
                results = self.conn.execute_query(sql)
                row_count = len(results)
            except Exception as e:
                error = str(e)
                print(f"⚠️ Erro SQL: {error}")
                
                # FALLBACK: Se o SQL falhar, tentar responder via Base de Conhecimento (RAG Puro)
                # Pode ser que a pergunta seja sobre um processo ou dado não estruturado
                print("🔄 Tentando fallback para RAG (Knowledge Base)...")
                rag_result = self._handle_knowledge_request(message, user_context)
                
                # Modificar o prompt do RAG para ser direto se for fallback de dados
                if rag_result['intent'] != 'operational_fail':
                    # Se encontramos algo, vamos garantir que a resposta seja DIRETA
                    rag_result['intent'] = 'fallback_rag' 
                    rag_result['original_error'] = error
                    
                    # Forçar regeneração se a resposta for muito instrutiva/longa ou genérica
                    lower_resp = rag_result['direct_response'].lower()
                    if any(term in lower_resp for term in ["passo a passo", "para encontrar", "você pode", "tente usar", "verifique"]):
                        new_prompt = f"""
                        O usuário perguntou: "{message}"
                        
                        O sistema encontrou estas informações instrutivas (que o usuário NÃO quer):
                        {rag_result['direct_response']}
                        
                        O usuário quer APENAS O DADO ESPECÍFICO (ex: um telefone, um nome, um valor).
                        Se a informação exata NÃO estiver no texto acima, responda APENAS: "Não encontrei essa informação específica nos meus registros."
                        NÃO dê instruções de como procurar. NÃO invente dados.
                        """
                        rag_result['direct_response'] = self.ai.generate_text(new_prompt)
                        
                        # Se a nova resposta ainda for vaga, força erro
                        if "não encontrei" in rag_result['direct_response'].lower():
                             results = [{"error": "Informação não encontrada no banco ou documentos."}]
                             return {
                                "type": "data",
                                "generated_sql": sql,
                                "results": results,
                                "row_count": 0,
                                "error": error,
                                "intent": intent
                             }
                        
                    return rag_result
                
                # Se RAG também falhar, retorna erro amigável (sem parecer suporte técnico)
                results = [{"error": "Não consegui encontrar esses dados no banco nem na base de conhecimento."}]
                
        return {
            "type": "data",
            "generated_sql": sql,
            "results": results,
            "row_count": row_count,
            "error": error,
            "intent": intent,
            "context_used": kb_items
        }

    def _handle_knowledge_request(self, message: str, user_context: dict) -> dict:
        """Trata perguntas sobre processos/operacional (RAG puro)"""
        
        # Busca na base de conhecimento
        kb_items = self.kb.storage.find_knowledge(message, limit=4)
        
        if not kb_items:
            return {
                "type": "chat",
                "direct_response": "Não encontrei informações específicas sobre esse processo na minha base de conhecimento. Pode detalhar melhor?",
                "intent": "operational_fail"
            }
            
        context_str = "\n".join([f"- {item['content']}" for item in kb_items])
        
        prompt = f"""
        Use o contexto abaixo para responder a dúvida operacional do usuário.
        Contexto:
        {context_str}
        
        Pergunta: "{message}"
        
        Responda de forma instrutiva e passo a passo.
        """
        
        response = self.ai.generate_text(prompt)
        
        return {
            "type": "chat",
            "direct_response": response,
            "context_used": kb_items,
            "intent": "operational"
        }

    def _handle_support_request(self, message: str, user_context: dict) -> dict:
        """Trata solicitações de suporte"""
        # Aqui poderia abrir um ticket ou logar erro crítico
        
        response = "Entendi que você está enfrentando um problema. Vou registrar isso para a equipe de TI.\n\n" \
                   "Enquanto isso, tente recarregar a página ou verifique se sua conexão está estável. " \
                   "Se o erro persistir, entre em contato com o ramal 1234."
                   
        return {
            "type": "chat",
            "direct_response": response,
            "intent": "support"
        }

    def _handle_intelligent_routing(self, message: str, user_context: dict, history: list) -> dict:
        """
        Fallback Inteligente: A IA decide o que fazer quando a intenção não é clara.
        Similar ao método antigo, mas com mais contexto.
        """
        
        prompt = f"""
        Você é um assistente sênior. O classificador de intenções ficou em dúvida sobre a mensagem: "{message}".
        
        Analise o histórico e contexto.
        Histórico recente: {user_context.get('recent_history')}
        
        Decida:
        1. É um pedido de dados? -> Gere JSON {{"action": "sql", "content": "..."}}
        2. É uma conversa? -> Gere JSON {{"action": "chat", "content": "..."}}
        
        Priorize a segurança e a clareza.
        """
        
        response_text = self.ai.generate_text(prompt)
        
        try:
            # Tenta parsear JSON
            clean_resp = response_text.replace("```json", "").replace("```", "").strip()
            decision = json.loads(clean_resp)
            
            if decision.get("action") == "sql":
                # Reutiliza lógica de dados
                return self._handle_data_request(message, user_context, history, "complex_query")
            else:
                return {
                    "type": "chat",
                    "direct_response": decision.get("content", response_text),
                    "intent": "complex_chat"
                }
        except:
            # Se falhar JSON, assume chat direto
            return {
                "type": "chat",
                "direct_response": response_text,
                "intent": "complex_fallback"
            }
from datetime import datetime
