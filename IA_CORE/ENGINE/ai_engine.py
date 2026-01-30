import os
import sys
import requests
import json
import re
import random
from typing import Any, Dict, List, Optional, Tuple

# Adicionar o diretório raiz e infra ao path para importar conecxaodb
root_dir = os.path.abspath(os.path.join(os.path.dirname(__file__), '../../../../'))
sys.path.append(root_dir)
sys.path.append(os.path.join(root_dir, 'sys_backend', 'infra'))

try:
    try:
        from conecxaodb import get_connection
    except ImportError:
        from sys_backend.infra.conecxaodb import get_connection
except ImportError:
    # Fallback para o caso de não encontrar o conecxaodb
    def get_connection():
        import cx_Oracle
        ip = '192.168.1.253'
        porta = 1521
        service_name = 'rohden'
        usuario_oracle = 'SYSROH'
        senha_oracle = 'rohden'
        dsn_tns = cx_Oracle.makedsn(ip, porta, service_name=service_name)
        try:
            return cx_Oracle.connect(user=usuario_oracle, password=senha_oracle, dsn=dsn_tns)
        except:
            return None

from ..MEMORIA import memoria_system
from .sql_builder import SQLBuilder
from .vector_manager import VectorManager
from .behavior_manager import BehaviorManager
from ..DATA import storage
from ..CONFIG.ai_temperature_config import AI_TEMPERATURE_SETTINGS
from dotenv import load_dotenv

# Carregar variáveis de ambiente do arquivo .env de forma robusta
env_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), '..', '..', '..', '..', '.env')
if os.path.exists(env_path):
    load_dotenv(dotenv_path=env_path)
else:
    load_dotenv() # Fallback para o CWD

def load_local_config():
    """Carrega a configuração do sistema local (ChromaDB + SQLite)"""
    try:
        from ..DATA.storage import DataStorage
        st = DataStorage()
        tables_db = st.load_tables()
        tables = []
        metadata = {}
        
        for t in tables_db:
            t_name = t['table_name']
            tables.append({
                'name': t_name,
                'table': t_name,
                'description': t.get('table_description', '')
            })
            metadata[t_name] = {
                'columns': t.get('columns_info', []),
                'samples': t.get('sample_data', []),
                'last_processed': t.get('updated_at')
            }
            
        # Carregar fatos aprendidos (knowledge_base categoria 'learned')
        learned_db = st.get_knowledge(category='learned')
        learned = []
        for fact in learned_db:
            learned.append({
                'content': fact['content'],
                'user': fact.get('tags', ''),
                'date': fact.get('updated_at', '')
            })
            
        return {
            'tables': tables, 
            'metadata': metadata, 
            'knowledge': {},
            'learned': learned
        }
    except Exception as e:
        print(f"Erro ao carregar configuração local: {e}")
        return {'tables': [], 'metadata': {}, 'knowledge': {}, 'learned': []}

def save_local_config(config):
    """Salva a configuração no sistema local (ChromaDB + SQLite)"""
    try:
        from ..DATA.storage import DataStorage
        st = DataStorage()
        if 'learned' in config:
            for fact in config['learned']:
                # Verifica se já existe
                existing = st.get_knowledge(category='learned')
                if not any(f['content'] == fact['content'] for f in existing):
                    st.save_knowledge(
                        category='learned',
                        title='Fato Aprendido',
                        content=fact['content'],
                        tags=fact.get('user', ''),
                        priority=1
                    )
    except Exception as e:
        print(f"Erro ao salvar configuração local: {e}")

class LlamaEngine:
    _instance = None
    _config_cache = None
    _last_cache_time = 0
    _permission_cache = {}  # Cache de permissões por usuário
    _memory_cache = {}     # Cache de memória por usuário
    _last_memory_update = {}  # Controle de atualização de memória
    _plan_cache = {}       # Cache de planos por pergunta normalizada
    _decision_cache = {}   # Cache de decisão (CHAT/QUERY) por pergunta normalizada
    _training_cache = None
    _training_cache_time = 0
    _failed_ai_urls = {} # Cache de URLs que falharam (url: timestamp)

    def __new__(cls):
        if cls._instance is None:
            cls._instance = super(LlamaEngine, cls).__new__(cls)
            cls._instance.llm = None
            cls._instance.vector_manager = VectorManager()
            cls._instance.behavior_manager = BehaviorManager()
            
            # Configuração do Motor Principal (Rohden AI Server)
            cls._instance.ai_url = os.getenv("ROHDEN_AI_URL")
            cls._instance.ai_url_internal = os.getenv("ROHDEN_AI_INTERNAL_URL", "http://192.168.1.217:11434/api/generate")
            cls._instance.ai_model = os.getenv("ROHDEN_AI_MODEL", "qwen2.5:3b")
            cls._instance.api_key = os.getenv("ROHDEN_AI_KEY", "ROHDEN_AI_SECRET_2024")
            
            # Headers para o Servidor Rohden
            cls._instance.rohden_headers = {
                'X-ROHDEN-AI-KEY': cls._instance.api_key,
                'Content-Type': 'application/json',
                'User-Agent': 'RohdenAI-Assistant/1.0'
            }
            cls._instance._last_pattern_id = None
        return cls._instance

    def _load_cached_config(self):
        """Carrega a configuração com cache de 30 segundos"""
        import time
        now = time.time()
        if not self._config_cache or (now - self._last_cache_time > 30):
            self._config_cache = load_local_config()
            self._last_cache_time = now
        return self._config_cache

    def get_user_permissions(self, username):
        """Cache de permissões para evitar múltiplas leituras"""
        import time
        current_time = time.time()
        
        # Cache válido por 5 minutos
        if username in self._permission_cache:
            cache_time, permissions = self._permission_cache[username]
            if current_time - cache_time < 300:  # 5 minutos
                return permissions
        
        permissions = self._load_permissions(username)
        self._permission_cache[username] = (current_time, permissions)
        return permissions
    
    def _load_permissions(self, username):
        """Busca as permissões do usuário na tabela local tb_ai_user_table_access e metadados"""
        if not username:
            return []
            
        config = self._load_cached_config()
        metadata = config.get('metadata', {})
        allowed_tables = {} # {TABLE_NAME: ACCESS_LEVEL}
        
        # 1. Buscar permissões no SQLite (Local)
        from ..PERSISTENCIA.db_history import get_db_connection
        conn = get_db_connection()
        if conn:
            try:
                cursor = conn.cursor()
                # Tenta buscar na nova tabela SQLite
                query = "SELECT table_name, access_level FROM tb_ai_user_table_access WHERE UPPER(user_name) = UPPER(?)"
                cursor.execute(query, (username,))
                for row in cursor.fetchall():
                    allowed_tables[row['table_name'].upper()] = row['access_level'] if row['access_level'] else 1
            except Exception as e:
                print(f"⚠️ Erro ao carregar permissões locais: {e}")
            finally:
                conn.close()

        # 2. Construir lista de permissões com metadados
        permissions = []
        for table in config.get('tables', []):
            table_name_upper = table['name'].upper()
            
            # "Todos tem que ter a permissão 1" 
            # Se não houver permissão específica, assume Nível 1 por padrão
            current_level = allowed_tables.get(table_name_upper, 1)
            
            table_meta = metadata.get(table['name'], {})
            permissions.append({
                'table': table['name'],
                'description': table.get('description', 'Sem descrição'),
                'access_level': current_level,
                'columns': table_meta.get('columns', []),
                'samples': table_meta.get('samples', [])
            })
        return permissions

    def get_learned_memory(self, username):
        """Busca fatos aprendidos pela IA"""
        config = load_local_config()
        learned = config.get('learned', [])
        if not learned:
            return ""
        
        memory_text = "\n### FATOS APRENDIDOS ANTERIORMENTE ###\n"
        for fact in learned:
            memory_text += f"- {fact['content']}\n"
        return memory_text

    def learn_fact(self, content, username):
        """Salva um novo fato no arquivo JSON local"""
        config = load_local_config()
        if 'learned' not in config:
            config['learned'] = []
            
        # Evitar duplicados simples
        if any(f['content'].lower() == content.lower() for f in config['learned']):
            return
            
        from datetime import datetime
        config['learned'].append({
            'content': content,
            'user': username,
            'date': datetime.now().strftime("%d/%m/%Y %H:%M:%S")
        })
        save_local_config(config)

    def execute_sql(self, sql_query, params=None, dialect: Optional[str] = None):
        """Executa SQL com suporte a múltiplos usuários sem bloqueios"""
        try:
            sql_upper = (sql_query or "").strip().upper()
            if not sql_upper.startswith("SELECT"):
                return "Erro: Apenas comandos SELECT são permitidos."

            use_oracle = "SYSROH." in sql_upper or (dialect or "").lower() == "oracle"
            if use_oracle:
                conn = get_connection()
                if not conn:
                    return "Erro ao conectar ao Oracle."
                cursor = conn.cursor()
                try:
                    if params:
                        cursor.execute(sql_query, params)
                    else:
                        cursor.execute(sql_query)
                    columns = [col[0] for col in cursor.description] if cursor.description else []
                    rows = cursor.fetchall() if columns else []
                    results = [dict(zip(columns, row)) for row in rows]
                finally:
                    cursor.close()
                    conn.close()
            else:
                from .connection_manager import execute_sql_safe
                from ..DATA import storage

                results = execute_sql_safe(sql_query, tuple(params) if params else None, storage.db_path)

            return results
        except Exception as e:
            return f"Erro ao executar SQL: {str(e)}"

    def _format_results(self, query_plan: Dict[str, Any], results: List[Dict[str, Any]], prompt: str = "") -> str:
        """
        Formata resultados brutos. 
        REGRA: Não deve conter frases completas para o usuário, apenas estrutura de dados.
        """
        if not results:
            return "Nenhum dado encontrado para os critérios informados."

        prompt_lower = prompt.lower() if prompt else ""
        is_count_query = any(w in prompt_lower for w in ['quantos', 'quantas', 'total', 'quantidade', 'count'])

        # Caso de agregacao (ex: COUNT)
        is_aggregation = bool(query_plan.get('aggregations'))
        if not is_aggregation and len(results) == 1 and len(results[0]) == 1:
            # Se tiver apenas 1 linha e 1 coluna, provavelmente é um COUNT ou soma
            val = list(results[0].values())[0]
            if isinstance(val, (int, float)):
                is_aggregation = True

        if is_aggregation:
            total = None
            # Tenta encontrar a coluna de total
            for key in results[0].keys():
                if any(k in key.upper() for k in ['COUNT', 'TOTAL', 'SOMA', 'SUM', 'QTD']):
                    total = results[0][key]
                    break
            
            # Se não achou por nome, mas é a única coluna numérica
            if total is None and len(results[0]) == 1:
                total = list(results[0].values())[0]

            if total is not None:
                if is_count_query:
                    return f"Total: {total}"
                return f"Resultado: {total}"

        fields = query_plan.get('fields') or list(results[0].keys())
        fields = [f for f in fields if f in results[0]] or list(results[0].keys())

        if len(results) == 1:
            return self._format_single_row(results[0], fields)

        preview = results[:10] # Aumentado preview
        linhas = [self._format_row_line(row, fields) for row in preview]
        linhas = [linha for linha in linhas if linha]
        
        total = len(results)
        
        if is_count_query and total >= 50:
            res = f"Encontrei pelo menos {total} registros (limite de visualização atingido). Para o total exato, tente refinar a busca.\n"
        else:
            res = f"Registros: {total}\n"
            
        return res + "\n".join(linhas)

    def _format_single_row(self, row: Dict[str, Any], fields: List[str]) -> str:
        partes = []
        for field in fields:
            value = row.get(field)
            if value is None:
                continue
            partes.append(f"{field}: {value}")
        return " | ".join(partes)

    def _format_row_line(self, row: Dict[str, Any], fields: List[str]) -> str:
        partes = []
        for field in fields:
            value = row.get(field)
            if value is None:
                continue
            partes.append(f"{field}: {value}")
        if not partes:
            return ""
        return "- " + " | ".join(partes)

    def _humanize_field_name(self, name: str) -> str:
        return str(name).replace('_', ' ').title()

    def _table_label(self, table_name: Optional[str]) -> str:
        if not table_name:
            return ""
        mapping = {
            'TB_CONTATOS': 'contato',
            'TB_PRODUTOS': 'produto',
            'TB_VENDAS': 'venda'
        }
        return mapping.get(table_name.upper(), 'registro')

    def _humanize_response(self, prompt: str, results: List[Dict[str, Any]], raw_text: str, skip_semantic: bool = False, query_plan: Dict = None) -> str:
        """
        Humaniza os dados brutos usando Aprendizado por Imitação (Etapa 2).
        """
        # ETAPA 2.2: Sistema de Decisão por Padrões
        pattern_decision = ""
        if not results:
            pattern_decision = "⚠️ DECISÃO: Nenhum resultado encontrado. Informe amigavelmente que não localizou o registro."
        elif len(results) > 5:
            pattern_decision = "⚠️ DECISÃO: Muitos resultados encontrados (>5). Liste as opções de forma resumida e pergunte qual o usuário deseja refinar."
        elif len(results) == 1:
            pattern_decision = "✅ DECISÃO: Resultado único encontrado. Forneça a resposta direta e completa."
        else:
            pattern_decision = "✅ DECISÃO: Alguns resultados encontrados. Apresente-os de forma organizada."

        # ETAPA 2.1: IA Aprende por Imitação (Exemplos Similares)
        behavior_context = self.behavior_manager.format_patterns_for_prompt(prompt, skip_semantic=skip_semantic)
        
        # Combinamos tudo no contexto dinâmico (Apenas Padrões e Decisão)
        full_sys_prompt = f"{behavior_context}\n\n{pattern_decision}\n\nResponda amigavelmente baseando-se apenas nos dados acima."

        # Preparação dos dados para a IA
        data_json = json.dumps(results[:10], ensure_ascii=False, default=str)
        user_prompt = (
            f"Usuário: '{prompt}'\n"
            f"Dados: {data_json if results else '[Vazio]'}\n"
        )

        # Se a IA estiver offline, não perdemos tempo tentando
        if self._is_ia_offline():
            print("🔌 IA detectada como offline. Pulando humanização por imitação.")
            return raw_text if raw_text else self._format_results(query_plan or {'fields': []}, results, prompt=prompt)

        try:
            # Temperatura padrão 0.2 para chat
            response = self._call_ai_with_limits(user_prompt, full_sys_prompt, num_predict=400, num_ctx=2048, temperature=0.2)
            if not response or len(response.strip()) < 2:
                print("⚠️ IA retornou vazio na humanização. Usando texto bruto.")
                return raw_text if raw_text else self._format_results(query_plan or {'fields': []}, results, prompt=prompt)
            return response.strip()
        except Exception as e:
            print(f"❌ Erro na humanização por imitação: {e}")
            return raw_text if raw_text else self._format_results(query_plan or {'fields': []}, results, prompt=prompt)

    def _find_successful_patterns(self, prompt: str, skip_semantic: bool = False) -> List[Dict]:
        """Busca conversas similares bem-sucedidas no banco de padrões."""
        if skip_semantic:
            return self.behavior_manager.get_all_patterns(limit=3)
        return self.behavior_manager.find_similar_patterns(prompt, limit=3)

    def _get_pattern(self, category_key: str, skip_semantic: bool = False) -> Dict:
        """
        Retorna um padrão representativo para a situação atual.
        Mapeia chaves simples para categorias do banco.
        """
        mapping = {
            'multiple_results': 'Busca Ambígua',
            'single_result': 'Busca Direta',
            'no_results': 'Pergunta de Dados'
        }
        category = mapping.get(category_key, 'Busca Direta')
        patterns = self.behavior_manager.get_patterns_by_category(category)
        
        if patterns:
            return patterns[0]
            
        # Padrão de Fallback Mínimo se o banco estiver vazio
        return {
            'situation': 'Conversa Geral',
            'user_input': 'Olá',
            'ai_response': 'Olá! Como posso ajudar você hoje?',
            'ai_action': 'chat_default'
        }

    def _apply_pattern(self, pattern: Dict, prompt: str, results: List[Dict], skip_semantic: bool = False, query_plan: Dict = None) -> str:
        """
        Usa a IA apenas para 'preencher o template' baseado no padrão selecionado.
        """
        # Se não houver padrão real (apenas o fallback mínimo), usamos o humanizer padrão
        if not pattern or pattern.get('ai_action') == 'chat_default':
            return self._humanize_response(prompt, results, "", skip_semantic=skip_semantic, query_plan=query_plan)

        # Guardamos qual padrão foi usado para feedback posterior (Etapa 4)
        self._last_pattern_id = pattern.get('id')

        # Preparação do contexto de imitação
        behavior_context = f"SITUAÇÃO EXEMPLO: {pattern.get('situation')}\n"
        behavior_context += f"USUÁRIO: {pattern.get('user_input')}\n"
        behavior_context += f"VOCÊ RESPONDEU: {pattern.get('ai_response')}\n"

        # Dados reais encontrados
        data_json = json.dumps(results[:10], ensure_ascii=False, default=str)
        
        # Contexto do Sistema (Vazio - Sem prompts)
        sys_prompt = f"{behavior_context}\n"
        
        user_prompt = (
            f"Usuário: '{prompt}'\n"
            f"Dados: {data_json if results else '[Vazio]'}\n"
        )

        # Se a IA estiver offline, não perdemos tempo tentando
        if self._is_ia_offline():
            print("🔌 IA detectada como offline. Pulando preenchimento de template.")
            return self._format_results(query_plan or {'fields': []}, results, prompt=prompt)

        try:
            # Temperatura padrão 0.2
            response = self._call_ai_with_limits(user_prompt, sys_prompt, num_predict=400, temperature=0.2)
            if not response or len(response.strip()) < 2:
                print("⚠️ IA retornou vazio ao aplicar padrão. Usando formatação bruta.")
                return self._format_results(query_plan or {'fields': []}, results, prompt=prompt)
            return response.strip()
        except Exception as e:
            print(f"❌ Erro ao aplicar padrão: {e}")
            return self._format_results(query_plan or {'fields': []}, results, prompt=prompt)

    # --- ETAPA 4: SISTEMA DE FEEDBACK E EVOLUÇÃO ---
    
    def _analyze_feedback(self, next_user_message: str, history: List[Dict]) -> None:
        """Detecta sucesso ou fracasso na mensagem do usuário e atualiza padrões (Etapa 4.1)."""
        if not history or not hasattr(self, '_last_pattern_id') or not self._last_pattern_id:
            return

        msg_lower = next_user_message.lower()
        
        # Indicadores de Sucesso (Etapa 4.1)
        success_indicators = ['obrigado', 'valeu', 'perfeito', 'é isso', 'consegui', 'boa', 'show', 'ok', 'entendi']
        # Indicadores de Falha (Etapa 4.1)
        failure_indicators = ['não', 'errado', 'mas eu queria', 'não é isso', 'ruim', 'corrigir', 'ajuda', 'falhou']

        is_success = any(ind in msg_lower for ind in success_indicators)
        is_failure = any(ind in msg_lower for ind in failure_indicators)

        if is_success:
            print(f"🌟 Feedback Positivo detectado para o padrão {self._last_pattern_id}")
            self._update_pattern_score(self._last_pattern_id, 1)
        elif is_failure:
            print(f"⚠️ Feedback Negativo detectado para o padrão {self._last_pattern_id}")
            self._update_pattern_score(self._last_pattern_id, -1)

    def _update_pattern_score(self, pattern_id: str, delta: int) -> None:
        """Atualiza a pontuação e prioridade de um padrão no ChromaDB (Etapa 4.2)."""
        try:
            from ..DATA.storage import DataStorage
            st = DataStorage()
            st.update_pattern_score(pattern_id, delta)
        except Exception as e:
            print(f"❌ Erro ao atualizar score do padrão: {e}")
            # Limpa o ID para não processar feedback repetido
            self._last_pattern_id = None


    def _decide_by_examples(self, prompt: str, results: List[Dict], username: str = None, skip_semantic: bool = False, query_plan: Dict = None) -> str:
        """
        ETAPA 3.2: Função de Decisão por Exemplos.
        Analisa os resultados e decide o comportamento baseado em imitação.
        """
        # 1. Buscar conversas similares bem-sucedidas
        similar_conversations = self._find_successful_patterns(prompt, skip_semantic=skip_semantic)
        
        # 2. Analisar contexto atual
        result_count = len(results) if isinstance(results, list) else 0
        
        # 3. Aplicar padrão mais similar
        if result_count > 5:
            pattern = self._get_pattern('multiple_results', skip_semantic=skip_semantic)
        elif result_count == 1:
            pattern = self._get_pattern('single_result', skip_semantic=skip_semantic)
        else:
            pattern = self._get_pattern('no_results', skip_semantic=skip_semantic)
            
        # 4. Usar IA apenas para "preencher o template"
        return self._apply_pattern(pattern, prompt, results, skip_semantic=skip_semantic, query_plan=query_plan)

    def discover_relevant_tables(self, prompt, all_tables, top_n=3, skip_semantic: bool = False):
        """Usa busca vetorial e palavras-chave para encontrar as tabelas mais relevantes"""
        prompt_upper = prompt.upper()
        
        # 1. Busca por Palavras-Chave (Prioridade Máxima)
        keyword_matches = []
        for table in all_tables:
            t_name = table.get('table_name', '').upper()
            t_desc = table.get('table_description', '').upper()
            
            # Remove prefixos comuns para comparação
            clean_name = t_name.replace('TB_', '').replace('SYSROH.', '')
            
            # Pontuação básica por palavra-chave
            score = 0
            if clean_name in prompt_upper: score += 10
            if t_name in prompt_upper: score += 10
            
            # Busca semântica manual simples
            synonyms = {
                'CONTATOS': ['CONTATO', 'PESSOA', 'CLIENTE', 'TELEFONE', 'EMAIL', 'QUEM'],
                'PRODUTOS': ['PRODUTO', 'ITEM', 'ESTOQUE', 'PREÇO'],
                'VENDAS': ['VENDA', 'PEDIDO', 'FATURAMENTO', 'TOTAL', 'QUANTO']
            }
            
            for key, words in synonyms.items():
                if key in clean_name:
                    for word in words:
                        if word in prompt_upper:
                            score += 5
            
            if score > 0:
                keyword_matches.append((score, table))
        
        if keyword_matches:
            keyword_matches.sort(key=lambda x: x[0], reverse=True)
            print(f"Busca por Palavras-Chave encontrou {len(keyword_matches)} tabelas.")
            return [t for s, t in keyword_matches[:top_n]]

        # 2. Busca Vetorial (Se palavras-chave falharem e não for solicitado pular)
        if not skip_semantic:
            try:
                from ..DATA.storage import DataStorage
                st = DataStorage()
                relevant = st.find_similar_tables(prompt, limit=top_n)
                
                if relevant:
                    print(f"Busca Vetorial encontrou {len(relevant)} tabelas treinadas relevantes.")
                    return relevant
            except Exception as e:
                print(f"⚠️ Erro na busca vetorial de tabelas: {e}")
            
        # Fallback: Se vetores falharem ou não houver nada, retorna o que temos
        if not all_tables:
            return []
            
        trained_tables = [
            t for t in all_tables 
            if t.get('columns_info') and (t.get('table_description') or t.get('semantic_context'))
        ]
        
        return trained_tables[:top_n] if trained_tables else all_tables[:top_n]

    def _build_table_catalog(self, tables: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        catalog = []
        for table in tables:
            schema_info = table.get('schema_info') or {}
            if isinstance(schema_info, str):
                try:
                    schema_info = json.loads(schema_info)
                except Exception:
                    schema_info = {}
            schema = schema_info.get('schema', 'SYSROH')
            columns = [col.get('name') for col in table.get('columns_info', []) if col.get('name')]
            catalog.append({
                'schema': schema,
                'table': table.get('table_name'),
                'description': table.get('table_description', ''),
                'columns': columns
            })
        return catalog

    def _extract_json(self, text: str) -> Optional[Dict[str, Any]]:
        if not text:
            return None

        # DEBUG: Ver o que a IA está retornando
        print(f"🔍 TEXTO BRUTO DA IA: '{text}'")

        # Limpeza agressiva: Se a IA repetiu o histórico ou o prompt, pegamos apenas o primeiro JSON válido
        # Isso evita que o "HISTÓRICO" vaze para a resposta final
        
        # 1. Tenta encontrar blocos JSON primeiro (do mais longo para o mais curto)
        json_matches = re.findall(r'(\{.*?\})', text, re.DOTALL)
        if json_matches:
            # Ordenar por tamanho decrescente para pegar o JSON mais completo
            json_matches.sort(key=len, reverse=True)
            
            for match in json_matches:
                try:
                    # Limpeza agressiva
                    clean_match = match.strip()
                    for junk in ['```json', '```', 'ESTRUTURA DO JSON', 'FORMATO OBRIGATÓRIO', 'JSON:']:
                        clean_match = clean_match.replace(junk, '')
                    
                    clean_match = clean_match.strip()
                    
                    # Ignorar se parecer um template vazio ou de instrução
                    if '...' in clean_match or 'NOME_TABELA' in clean_match or 'NOME_DA_TABELA' in clean_match or 'SUA_RESPOSTA' in clean_match:
                        continue

                    data = json.loads(clean_match)
                    if 'action' in data:
                        # Validação extra
                        if data.get('action') == 'DATA_ANALYSIS' and (not data.get('target_table') or data.get('target_table') == 'NOME_TABELA'):
                            continue
                            
                        return data
                except:
                    continue

        # 2. Se não achou JSON estruturado, mas o texto é amigável, assume CHAT
        # Mas removemos qualquer menção a "HISTÓRICO" ou "U:" / "A:" que possa ter vazado
        if '{' not in text and len(text.strip()) > 5:
            # Limpa o texto de possíveis vazamentos de contexto
            clean_text = text.split("CONTEXTO ANTERIOR")[0].split("HISTÓRICO")[0].strip()
            # Remove marcadores de diálogo se houver
            clean_text = re.sub(r'^(Usuário|IA|U|A):\s*', '', clean_text, flags=re.MULTILINE)
            print("⚠️ RESPOSTA EM TEXTO PURO - Convertendo para CHAT")
            return {"action": "CHAT", "text": clean_text.strip()}

        # 3. Fallback original para blocos ```json
        json_block = re.search(r'```json\s*(\{.*?\})\s*```', text, re.DOTALL | re.IGNORECASE)
        if json_block:
            try:
                return json.loads(json_block.group(1))
            except: pass

        try:
            result = json.loads(text)
            print(f"✅ JSON VÁLIDO: {result}")
            return result
        except Exception as e:
            # Se falhou o parse mas tem conteúdo, ainda tenta tratar como CHAT
            if len(text.strip()) > 10:
                print(f"⚠️ ERRO JSON ({e}), mas tratando como CHAT por segurança.")
                return {"action": "CHAT", "text": text.strip()}
            
            print(f"❌ ERRO JSON CRÍTICO: {e}")
            return None

    _failed_ai_urls = {} # Cache de URLs que falharam (url: timestamp)

    def _is_ia_offline(self) -> bool:
        """Verifica se todas as URLs de IA conhecidas estão marcadas como falha recente."""
        import time
        now = time.time()
        available_urls = []
        if self.ai_url_internal: available_urls.append(self.ai_url_internal)
        if self.ai_url: available_urls.append(self.ai_url)
        
        if not available_urls:
            return True
            
        for url in available_urls:
            last_fail = self._failed_ai_urls.get(url, 0)
            if now - last_fail > 60: # Se uma URL não falhou nos últimos 60s, não está offline
                return False
        return True

    def _call_ai_with_limits(self, prompt: str, system_prompt: str, num_predict: int, num_ctx: int = 1024, temperature: float = 0.1, retries: int = 2, stop: List[str] = None) -> str:
        """Chamada direta para a IA com tratamento de erros 500 e failover entre URLs do ambiente."""
        import time
        now = time.time()
        
        # Stop tokens padrão
        default_stop = ["### Instruction:", "### Response:", "Pergunta do Usuário:", "Resposta do Assistente:"]
        if stop:
            default_stop.extend(stop)
        
        # Coleta todas as URLs configuradas no ambiente (sem hardcode)
        available_urls = []
        if self.ai_url_internal: available_urls.append(self.ai_url_internal)
        if self.ai_url: available_urls.append(self.ai_url)
        
        # Filtrar URLs que falharam recentemente (últimos 5 minutos)
        valid_urls = []
        for url in available_urls:
            last_fail = self._failed_ai_urls.get(url, 0)
            if now - last_fail > 300: # 5 minutos de cache
                valid_urls.append(url)
        
        if not valid_urls:
            # Se todas falharam, tenta a externa como última esperança se já passou 30s
            for url in available_urls:
                if now - self._failed_ai_urls.get(url, 0) > 30:
                    valid_urls.append(url)
        
        if not valid_urls:
            print("❌ ERRO: Nenhuma URL de IA disponível ou todas falharam recentemente.")
            return ""

        current_num_ctx = num_ctx
        
        for attempt in range(retries):
            # Tenta cada URL disponível
            for target_url in valid_urls:
                try:
                    # Garante o endpoint correto para Ollama
                    if not target_url.endswith('/api/generate') and '/api/' not in target_url:
                        target_url = target_url.rstrip('/') + '/api/generate'

                    # Detectar se é o endpoint /api/chat para ajustar o payload
                    is_chat_api = '/api/chat' in target_url

                    # Se for a segunda tentativa e deu erro 500, reduzimos o contexto
                    if attempt > 0:
                        current_num_ctx = max(512, int(current_num_ctx * 0.7))
                        print(f"🔄 Reduzindo contexto para {current_num_ctx} tokens na tentativa {attempt+1}")

                    if is_chat_api:
                        payload = {
                            "model": self.ai_model,
                            "messages": [
                                {"role": "system", "content": system_prompt},
                                {"role": "user", "content": prompt}
                            ],
                            "stream": False,
                            "options": {
                                "temperature": temperature,
                                "num_predict": int(num_predict),
                                "num_ctx": current_num_ctx,
                                "stop": default_stop
                            }
                        }
                    else:
                        payload = {
                            "model": self.ai_model,
                            "prompt": prompt,
                            "system": system_prompt,
                            "stream": False,
                            "options": {
                                "temperature": temperature,
                                "num_predict": int(num_predict),
                                "num_ctx": current_num_ctx,
                                "stop": default_stop
                            }
                        }

                    # Timeout agressivo para failover rápido (Reduzido para evitar esperas longas)
                    timeout = 15 if '192.168' in target_url else 25
                    print(f"Conectando Rohden AI em: {target_url} (Tentativa {attempt+1}/{retries})")

                    import urllib3
                    urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

                    try:
                        response = requests.post(
                            target_url,
                            json=payload,
                            headers=self.rohden_headers,
                            timeout=timeout,
                            verify=False
                        )
                    except requests.exceptions.Timeout:
                        print(f"⏱️ Timeout de {timeout}s em {target_url}. Marcando como falha temporária.")
                        self._failed_ai_urls[target_url] = time.time()
                        continue
                    except requests.exceptions.ConnectionError as ce:
                        print(f"🔌 Falha de conexão com {target_url}. Marcando como falha temporária.")
                        self._failed_ai_urls[target_url] = time.time()
                        continue

                    if response.status_code == 200:
                        res_data = response.json()
                        if is_chat_api:
                            ai_text = res_data.get("message", {}).get("content", "").strip()
                        else:
                            ai_text = res_data.get("response", "").strip()
                        
                        if ai_text:
                            # Se funcionou, remove do cache de falhas se existir
                            if target_url in self._failed_ai_urls:
                                del self._failed_ai_urls[target_url]
                            return ai_text
                    
                    elif response.status_code in [502, 503, 504]:
                        print(f"⚠️ Servidor em manutenção ou sobrecarregado (Status {response.status_code}) em {target_url}")
                        self._failed_ai_urls[target_url] = time.time()
                        time.sleep(2) 
                        continue
                    
                    # Se der 405 ou 404, tenta o endpoint de chat com o payload correto
                    elif response.status_code in [404, 405] and '/api/generate' in target_url:
                        alt_url = target_url.replace('/api/generate', '/api/chat')
                        print(f"Status {response.status_code} detectado. Tentando alternativa: {alt_url}")
                        
                        # Reconstroi o payload para o formato de chat se necessário
                        chat_payload = {
                            "model": self.ai_model,
                            "messages": [
                                {"role": "system", "content": system_prompt},
                                {"role": "user", "content": prompt}
                            ],
                            "stream": False,
                            "options": payload.get("options", {})
                        }
                        
                        try:
                            response = requests.post(alt_url, json=chat_payload, headers=self.rohden_headers, timeout=timeout, verify=False)
                            if response.status_code == 200:
                                res_data = response.json()
                                return res_data.get("message", {}).get("content", "").strip() or res_data.get("response", "").strip()
                        except:
                            pass

                    print(f"Erro na Rohden AI ({target_url}): Status {response.status_code}")
                    self._failed_ai_urls[target_url] = time.time()
                    
                except Exception as e:
                    print(f"Falha na conexão com {target_url}: {str(e)}")
                    self._failed_ai_urls[target_url] = time.time()
            
            # Se todas as URLs falharam, espera antes do próximo retry
            if attempt < retries - 1:
                time.sleep(1)

        return ""

    def _normalize_question(self, text: str) -> str:
        if not text:
            return ""
        txt = text.strip().lower()
        txt = re.sub(r'[^\w\s]', ' ', txt)
        txt = re.sub(r'\s+', ' ', txt).strip()
        return txt

    def _load_training_cache(self):
        import time
        now = time.time()
        if self._training_cache is not None and (now - self._training_cache_time) < 120:
            return

        cache = []
        try:
            items = storage.get_knowledge(category='QUERY_PLAN_TRAINING', limit=1000)
            for item in items:
                try:
                    payload = json.loads(item.get('content') or '{}')
                except Exception:
                    continue
                q = payload.get('normalized_question') or payload.get('question') or item.get('title')
                qn = self._normalize_question(q)
                plan = payload.get('query_plan')
                if qn and isinstance(plan, dict):
                    cache.append({'q': qn, 'plan': plan})
        except Exception:
            cache = []

        self._training_cache = cache
        self._training_cache_time = now

    def _find_trained_plan(self, prompt: str) -> Optional[Dict[str, Any]]:
        normalized = self._normalize_question(prompt)
        if not normalized:
            return None

        if normalized in self._plan_cache:
            return self._plan_cache.get(normalized)

        self._load_training_cache()
        if not self._training_cache:
            return None

        best_score = 0.0
        best_plan = None
        q_words = set(normalized.split())
        for item in self._training_cache:
            t = item.get('q')
            if not t:
                continue
            if t == normalized:
                best_plan = item.get('plan')
                best_score = 1.0
                break
            t_words = set(t.split())
            union = q_words.union(t_words)
            inter = q_words.intersection(t_words)
            score = (len(inter) / len(union)) if union else 0.0
            if score > best_score:
                best_score = score
                best_plan = item.get('plan')

        # Para perguntas muito curtas (menos de 4 caracteres), exige match exato (score 1.0)
        # No modo agente, somos mais flexíveis com o score de treinamento (0.75 vs 0.88)
        min_score = 1.0 if len(normalized) < 4 else 0.88
        
        # Tenta uma segunda chance se o score for baixo mas estivermos no modo agente
        if best_score < min_score and best_score >= 0.7:
            # Se for modo agente e houver palavras-chave fortes, aceitamos score menor
            data_keywords = {'quantos', 'total', 'contatos', 'vendas', 'produtos', 'quem'}
            if q_words.intersection(data_keywords):
                print(f"🎯 Score {best_score:.2f} aceito para treinamento no Modo Agente por palavras-chave.")
                min_score = 0.7

        if best_score >= min_score and isinstance(best_plan, dict):
            print(f"✅ Plano treinado encontrado (Score: {best_score:.2f}): '{normalized}'")
            self._plan_cache[normalized] = best_plan
            return best_plan
        elif best_score > 0.4:
            print(f"⚠️ Plano treinado com score insuficiente ({best_score:.2f}): '{normalized}' (Mínimo: {min_score})")
        
        return None

    def _validate_query_plan(self, plan: Dict[str, Any], tables: List[Dict[str, Any]]) -> Tuple[Optional[Dict[str, Any]], Optional[str]]:
        """
        Valida o plano de consulta técnica. 
        IMPORTANTE: Não retorna mensagens para o usuário final aqui.
        Retorna apenas códigos técnicos de erro para que a IA decida como responder.
        """
        if not plan:
            return None, "ERR_PLAN_EMPTY"
        
        plan_type = str(plan.get('type') or plan.get('action') or 'SELECT').upper()
        if plan_type == 'NONE':
            return None, "ERR_PLAN_NONE"
        
        # Se o tipo for DATA_ANALYSIS (comum em planos da IA), tratamos como SELECT
        if plan_type == 'DATA_ANALYSIS':
            plan_type = 'SELECT'
            plan['type'] = 'SELECT'

        # Suporte a SQL bruto injetado pelo fallback do extrator
        if plan_type == 'RAW_SQL':
            sql = plan.get('sql', '').upper()
            if not sql.startswith('SELECT'):
                return None, "ERR_RAW_SQL_NOT_SELECT"
            # Validação mínima de segurança
            forbidden = ['DROP', 'DELETE', 'UPDATE', 'INSERT', 'TRUNCATE', 'ALTER', 'GRANT']
            if any(cmd in sql for cmd in forbidden):
                return None, "ERR_RAW_SQL_FORBIDDEN_CMD"
            return plan, None

        if plan_type != 'SELECT':
            return None, "ERR_PLAN_INVALID_TYPE"

        table_map = {}
        for table in tables:
            t_name = table.get('table_name', '')
            schema_info = table.get('schema_info') or {}
            if isinstance(schema_info, str):
                try:
                    schema_info = json.loads(schema_info)
                except Exception:
                    schema_info = {}
            schema = schema_info.get('schema', 'SYSROH')
            columns = [col.get('name') for col in table.get('columns_info', []) if col.get('name')]
            
            # Mapeamento normalizado (Maiúsculo e sem prefixo de schema)
            table_map[t_name.upper()] = {
                'original_name': t_name,
                'schema': schema,
                'columns': columns
            }
            # Também mapeia sem o prefixo SYSROH. se houver
            if '.' in t_name:
                short_name = t_name.split('.')[-1].upper()
                table_map[short_name] = table_map[t_name.upper()]

        plan_table = str(plan.get('table') or plan.get('target_table') or '').upper()
        if not plan_table:
            return None, "ERR_PLAN_NO_TABLE"

        # Busca flexível da tabela
        target_info = table_map.get(plan_table)
        if not target_info:
            # Tenta buscar por substring se for modo flexível
            for t_key in table_map:
                if t_key in plan_table or plan_table in t_key:
                    target_info = table_map[t_key]
                    plan['table'] = target_info['original_name']
                    break
        
        if not target_info:
            print(f"❌ Tabela '{plan_table}' não encontrada no catálogo. Disponíveis: {list(table_map.keys())[:5]}...")
            return None, f"ERR_TABLE_NOT_AUTHORIZED:{plan_table}"

        table_name = target_info['original_name']
        plan['table'] = table_name
        schema = plan.get('schema') or target_info['schema']
        plan['schema'] = schema

        allowed_columns = target_info['columns']
        column_map = {c.upper(): c for c in allowed_columns}

        fields = plan.get('fields') or []
        # Se for contagem, aceitamos COUNT(*) mesmo que não esteja nas colunas
        if fields:
            sanitized = []
            for field in fields:
                field_str = str(field).upper()
                if field_str == '*' or 'COUNT(' in field_str or 'SUM(' in field_str:
                    sanitized.append(field)
                    continue
                if field_str in column_map:
                    sanitized.append(column_map[field_str])
            plan['fields'] = sanitized

        filters = plan.get('filters') or []
        sanitized_filters = []
        for flt in filters:
            if not flt or not isinstance(flt, dict):
                continue
            field = flt.get('field')
            if not field:
                continue
            key = str(field).upper()
            if key not in column_map:
                continue
            op = str(flt.get('op', '=')).upper()
            if op not in ('=', 'LIKE', '>', '>=', '<', '<='):
                op = '='
            
            value = flt.get('value')
            case_insensitive = bool(flt.get('case_insensitive'))
            
            # Reforço de segurança: se for LIKE, forçamos case_insensitive para melhor UX
            if op == 'LIKE':
                case_insensitive = True
                
            sanitized_filters.append({
                'field': column_map[key],
                'op': op,
                'value': value,
                'case_insensitive': case_insensitive,
                'normalize': flt.get('normalize')
            })
        plan['filters'] = sanitized_filters

        if plan.get('limit') is None:
            plan['limit'] = 50
        try:
            plan['limit'] = min(max(int(plan['limit']), 1), 200)
        except Exception:
            plan['limit'] = 50

        plan['dialect'] = plan.get('dialect', 'oracle')
        return plan, None

    def _store_training(self, question: str, plan: Dict[str, Any], sql: str, username: Optional[str]):
        try:
            payload = {
                'question': question,
                'normalized_question': self._normalize_question(question),
                'query_plan': plan,
                'sql': sql
            }
            storage.save_knowledge(
                category='QUERY_PLAN_TRAINING',
                title=question[:200],
                content=json.dumps(payload, ensure_ascii=False),
                tags=username or '',
                priority=1
            )
        except Exception:
            pass

    def _get_optimized_system_prompt(self, relevant_tables: List[Dict], interpretacao: Dict, user_query: Optional[str] = None) -> str:
        """Retorna apenas o contexto de comportamento (imitação) sem instruções fixas."""
        return self.behavior_manager.format_patterns_for_prompt(user_query)

    def generate_response(self, prompt, username=None, history=None, mode='chat'):
        """
        Gera uma resposta baseada na arquitetura contextual:
        1. IA Principal classifica e decide (CHAT ou DATA_ANALYSIS)
        2. Execução da ação decidida pela IA
        """
        import time
        start_time = time.time()
        
        # ETAPA 4.1: Analisar se esta nova mensagem é um feedback da resposta anterior
        self._analyze_feedback(prompt, history)
        
        # 0. Contexto de Memória (Estado do Usuário)
        user_memory = ""
        if username:
            try:
                user_memory = memoria_system.get_user_memory_context(username)
            except Exception:
                pass

        # Configuração de contexto de chat
        try:
            from ..CONFIG.context_config import CONTEXT_CONFIG
        except ImportError:
            CONTEXT_CONFIG = {
                "max_history_messages": 8,
                "max_chars_per_message": 500,
                "include_technical_metadata": True,
                "use_ellipsis": True
            }

        # Processamento de histórico
        processed_history = []
        if history and isinstance(history, list):
            history = history[-CONTEXT_CONFIG["max_history_messages"]:]
            for msg in history:
                if isinstance(msg, dict) and 'content' in msg:
                    content = msg['content']
                    max_chars = CONTEXT_CONFIG["max_chars_per_message"]
                    if len(content) > max_chars:
                        content = content[:max_chars] + "..."
                    
                    processed_history.append({
                        'role': msg.get('role', 'user'),
                        'content': content,
                        'metadata': msg.get('metadata', {})
                    })

        metadata = {
            'tabelas_usadas': [],
            'intencao': None,
            'user_memory_active': bool(user_memory),
            'mode': mode
        }

        try:
            # 1. CARREGAR TABELAS DISPONÍVEIS
            from ..DATA import storage
            config_tables = storage.load_tables()
            
            # 2. VERIFICAÇÃO DE TREINAMENTO PRÉVIO (Prioridade no modo Agente)
            trained_plan = self._find_trained_plan(prompt)
            
            # Se houver um plano treinado e estivermos no modo agente, usamos imediatamente
            if mode == 'agente' and trained_plan:
                print(f"🎯 Usando plano treinado (Modo Agente): '{prompt}'")
                ai_analysis = {
                    'action': 'DATA_ANALYSIS',
                    'plan': trained_plan,
                    'confidence': 1.0
                }
            else:
                # 3. ANÁLISE E DECISÃO PELA IA (Unificada)
                # ETAPA 3.1: O comportamento é guiado por análise e exemplos
                full_context_prompt = f"{user_memory}\n\nPergunta do Usuário: {prompt}" if user_memory else prompt
                ai_analysis = self._unified_ai_analysis(full_context_prompt, config_tables, history=processed_history, username=username, mode=mode)
            
            action = None
            chat_text = None
            query_plan = None
            
            if ai_analysis and isinstance(ai_analysis, dict):
                action = ai_analysis.get('action', '').upper()
                
                # Respeitar o modo selecionado pelo usuário
                if mode == 'chat':
                    # No modo chat, só fazemos DATA_ANALYSIS se a IA estiver MUITO segura ou se for uma pergunta óbvia de dados
                    # Caso contrário, mantemos CHAT
                    if action == 'DATA_ANALYSIS' and ai_analysis.get('confidence', 1.0) < 0.8:
                        action = 'CHAT'
                        chat_text = "Como posso ajudar você hoje?"
                elif mode == 'agente':
                    # No modo agente, forçamos a busca de dados.
                    # Se houver plano treinado, ele já foi definido acima.
                    # Se não, e a IA disse CHAT, tentamos forçar DATA_ANALYSIS a menos que seja saudação.
                    if action == 'CHAT':
                        prompt_lower = prompt.lower()
                        greetings = ["oi", "olá", "bom dia", "boa tarde", "boa noite", "quem é você", "o que você faz"]
                        if any(g in prompt_lower for g in greetings):
                            chat_text = "Olá! Sou o Samuca. Estou no modo **Agente da Empresa**, pronto para consultar dados no banco para você. Como posso ajudar com informações da Rohden hoje?"
                        else:
                            # Tenta forçar DATA_ANALYSIS para qualquer outra coisa que não seja saudação
                            action = 'DATA_ANALYSIS'
                
                if action == 'CHAT':
                    if not chat_text:
                        chat_text = ai_analysis.get('text')
                elif action == 'DATA_ANALYSIS' or action == 'QUERY':
                    action = 'DATA_ANALYSIS'
                    metadata['intencao'] = 'DATA_ANALYSIS'
                    # O plano pode estar na chave 'plan' ou ser a própria análise
                    query_plan = ai_analysis.get('plan') or (ai_analysis if 'sql' in ai_analysis else None)
                    if not query_plan and mode == 'agente':
                        query_plan = trained_plan

            # 4. FALLBACK PARA TREINAMENTO ESPECÍFICO (Para modo chat ou se o plano da IA falhou)
            if not action or (action == 'DATA_ANALYSIS' and not query_plan):
                if not trained_plan: # Se ainda não buscamos
                    trained_plan = self._find_trained_plan(prompt)
                
                if trained_plan:
                    query_plan = trained_plan
                    action = 'DATA_ANALYSIS'
                    metadata['intencao'] = 'DATA_ANALYSIS'
                elif not action:
                    action = 'CHAT'
                elif action == 'DATA_ANALYSIS' and mode == 'agente':
                    # Se estamos no modo agente e não conseguimos um plano de dados
                    return {'text': "Desculpe, como estou no modo **Agente da Empresa**, só posso responder perguntas relacionadas a dados do banco. Não consegui identificar quais dados você deseja consultar. Pode ser mais específico?", 'metadata': metadata}

            # 5. EXECUÇÃO DA AÇÃO DECIDIDA
            if mode == 'agente':
                print(f"🕵️ Agente Decision - Action: {action}, Has Plan: {query_plan is not None}")
            
            # Definir se devemos pular embeddings (Modo Agente ou servidor instável)
            skip_emb = (mode == 'agente')
            
            # FLUXO: CHAT
            if action == 'CHAT':
                # ETAPA 3: Decisão de resposta por imitação (mesmo para chat sem dados)
                if not chat_text:
                    chat_text = self._decide_by_examples(prompt, [], username, skip_semantic=skip_emb)
                return {'text': chat_text, 'metadata': metadata}
            
            # FLUXO: DATA_ANALYSIS
            if action == 'DATA_ANALYSIS':
                # Validação do plano
                validated_plan, plan_error = self._validate_query_plan(query_plan, config_tables)

                if not validated_plan:
                    print(f"❌ Falha na validação do plano (Modo {mode}): {plan_error}")
                    # Se o plano falhou, usamos o humanizer padrão que agora é guiado por padrões
                    if mode == 'agente':
                        return {'text': "Não consegui formular uma consulta válida para os dados da empresa. Por favor, tente perguntar de outra forma (ex: 'contato de fulano' ou 'pedidos de hoje').", 'metadata': metadata}
                    fallback_text = self._decide_by_examples(prompt, [], username, skip_semantic=skip_emb)
                    return {'text': fallback_text, 'metadata': metadata}

                # Execução SQL
                if validated_plan.get('table') and validated_plan['table'] not in metadata['tabelas_usadas']:
                    metadata['tabelas_usadas'].append(validated_plan['table'])

                try:
                    if validated_plan.get('type') == 'RAW_SQL':
                        sql_query = validated_plan.get('sql')
                        params = []
                        dialect = 'oracle'
                    else:
                        sql_query, params, dialect = SQLBuilder.from_plan(validated_plan).build()
                except Exception as build_err:
                    print(f"Erro ao montar SQL: {str(build_err)}")
                    fallback_text = self._decide_by_examples(prompt, [], username, skip_semantic=skip_emb, query_plan=validated_plan)
                    return {'text': fallback_text, 'metadata': metadata}

                # Persiste para treinamento futuro
                self._store_training(prompt, validated_plan, sql_query, username)

                # Executa no banco
                sql_result = self.execute_sql(sql_query, params=params, dialect=dialect)

                # ESTRATÉGIA DE RETENTATIVA ABRANGENTE (Se retornar vazio)
                if not sql_result and action == 'DATA_ANALYSIS':
                    print(f"⚠️ Busca restritiva retornou 0 resultados. Tentando busca abrangente...")
                    # Tenta um plano mais simples baseado apenas no contexto de dados, sem instruções complexas
                    target_table = next((t for t in config_tables if t.get('table_name').upper() == validated_plan.get('table').upper()), None)
                    if target_table:
                        broader_plan = self._generate_data_plan(prompt, target_table, history, username)
                        if broader_plan and broader_plan.get('plan'):
                            try:
                                sql_query_2, params_2, _ = SQLBuilder.from_plan(broader_plan.get('plan')).build(dialect)
                                sql_result = self.execute_sql(sql_query_2, params=params_2, dialect=dialect)
                                if sql_result:
                                    print(f"✅ Busca abrangente encontrou {len(sql_result)} candidatos.")
                                    validated_plan = broader_plan.get('plan') # Atualiza o plano para formatação correta
                            except:
                                pass

                # Formatação da resposta
                if isinstance(sql_result, str):
                    # Se sql_result for string, provavelmente é uma mensagem de erro do banco
                    # Usamos o humanizer padrão que agora é guiado por padrões
                    response = self._decide_by_examples(prompt, [], username, skip_semantic=skip_emb, query_plan=validated_plan)
                else:
                    # NOVA LÓGICA: Verifica se há excesso de resultados antes de humanizar normalmente
                    try:
                        from ..CONFIG.ai_temperature_config import handle_excessive_results
                        refinement_question = handle_excessive_results(prompt, sql_result, self)
                        if refinement_question:
                            return {'text': refinement_question, 'metadata': metadata}
                    except Exception as e:
                        print(f"⚠️ Erro ao processar refinamento de resultados: {e}")

                    # ETAPA 3: Decisão por Exemplos (Imitação de Comportamento)
                    # Se for modo agente e usamos um plano treinado, pulamos a busca semântica de padrões para evitar travamentos
                    skip_emb = (mode == 'agente' and trained_plan is not None)
                    response = self._decide_by_examples(prompt, sql_result, username, skip_semantic=skip_emb, query_plan=validated_plan)

                # Aprendizado Automático
                if username and prompt:
                    memoria_system.extract_learning_from_interaction(username, prompt, response)
                    
                return {'text': response, 'metadata': metadata}

            # Última instância: se chegar aqui sem ação (raro), usa o comportamento de imitação
            final_fallback = self._decide_by_examples(prompt, [], username, skip_semantic=skip_emb, query_plan=query_plan)
            return {'text': final_fallback, 'metadata': metadata}

        except Exception as e:
            # Em caso de erro crítico, tentamos uma resposta mínima de erro
            print(f"❌ ERRO CRÍTICO EM generate_response: {str(e)}")
            import traceback
            traceback.print_exc()
            return {'text': "Desculpe, ocorreu um erro técnico ao processar sua solicitação. Por favor, tente novamente em instantes.", 'metadata': metadata}


    def _get_related_training_examples(self, prompt: str) -> str:
        """Busca exemplos de treinamento relacionados à pergunta atual para guiar a IA"""
        self._load_training_cache()
        if not self._training_cache:
            return ""

        normalized = self._normalize_question(prompt)
        q_words = set(normalized.split())
        
        matches = []
        for item in self._training_cache:
            t = item.get('q', '')
            t_words = set(t.split())
            union = q_words.union(t_words)
            inter = q_words.intersection(t_words)
            score = (len(inter) / len(union)) if union else 0.0
            
            if score > 0.4: # Score moderado para pegar exemplos variados
                matches.append((score, item))
        
        matches.sort(key=lambda x: x[0], reverse=True)
        
        examples = ""
        for score, item in matches[:3]: # Pega os 3 melhores exemplos
            plan = item.get('plan')
            if plan:
                examples += f"- Pergunta: {item.get('q')}\n  Plano Sugerido: {json.dumps(plan, ensure_ascii=False)}\n"
        
        return examples

    def _unified_ai_analysis(self, prompt: str, tables: List[Dict[str, Any]], history: List[Dict[str, Any]] = None, username: str = None, mode: str = 'chat') -> Optional[Dict[str, Any]]:
        """
        FLUXO UNIFICADO: A IA decide se é CHAT ou DATA_ANALYSIS.
        Toda a decisão de fluxo é feita pela IA.
        """
        # 1. PREPARAR LISTA DE CAPACIDADES (Ultra simplificado)
        trained_topics = [
            f"- {t.get('table_name')}"
            for t in tables 
            if t.get('columns_info') and (t.get('table_description') or t.get('semantic_context'))
        ]
        topics_str = ", ".join(trained_topics) if trained_topics else "Nenhum tópico disponível."

        # 2. CARREGAR CONHECIMENTO ADICIONAL (Reduzido)
        learned_memory = self.get_learned_memory(username) if username else ""
        if len(learned_memory) > 300: learned_memory = learned_memory[:300] + "..."
        
        # 3. FORMATAR HISTÓRICO (Apenas as 5 últimas mensagens para contexto)
        history_context = ""
        if history:
            history_context = "\n### CONTEXTO RECENTE DA CONVERSA ###\n"
            for msg in history[-5:]:
                role = "USUÁRIO" if msg.get('role') == 'user' else "ROHDEN_AI"
                content = msg.get('content', '')
                history_context += f"[{role}]: {content[:200]}\n"
            history_context += "### FIM DO CONTEXTO ###\n"

        # 4. CONTEXTO DE DECISÃO (Apenas Exemplos e Capacidades)
        if mode == 'agente':
            system_decision_prompt = """Você é o Agente Especialista da Rohden. 
Sua única função é extrair dados do banco da empresa para ajudar o usuário.

REGRAS RÍGIDAS PARA MODO AGENTE:
1. Se o usuário fizer uma pergunta sobre dados (quem, quantos, qual, total, lista, contato, empresa, data, status), use SEMPRE "DATA_ANALYSIS".
2. Se o usuário perguntar "quantos", "total" ou "quantidade", use SEMPRE "DATA_ANALYSIS".
3. Você NÃO deve fazer conversas informais. Se o usuário apenas saudar (oi, olá), responda com CHAT mas mantendo o foco em dados.
4. Se a pergunta NÃO for sobre dados da empresa e NÃO for uma saudação, use CHAT e informe que você só pode responder sobre dados da empresa.
5. Responda APENAS em JSON puro.

FORMATO ESPERADO:
{
  "action": "DATA_ANALYSIS",
  "target_table": "NOME_DA_TABELA_MAIS_PROVAVEL",
  "reason": "O usuário deseja saber a quantidade total de contatos."
}
OU
{
  "action": "CHAT",
  "text": "Olá! Sou o Agente de Dados. Em que posso ajudar você com as informações da empresa hoje?"
}
"""
        else:
            system_decision_prompt = """Você é o Samuca, o cérebro da Rohden. 
Sua tarefa é decidir se a mensagem do usuário é uma saudação/conversa (CHAT) ou uma busca de dados (DATA_ANALYSIS).

REGRAS CRÍTICAS:
1. Se o usuário perguntar por NOMES de pessoas, empresas, DATAS, STATUS ou qualquer informação que pareça estar em um banco de dados, você DEVE usar "DATA_ANALYSIS".
2. Se o usuário perguntar por QUANTIDADES, totais, contagens ou estatísticas (ex: "quantos contatos", "total de vendas", "qual a média"), você DEVE usar "DATA_ANALYSIS".
3. Se o usuário apenas cumprimentar (oi, bom dia) ou fizer uma pergunta genérica sobre quem você é, use "CHAT".
4. Responda APENAS em JSON puro. Não explique fora do JSON.

FORMATO ESPERADO:
{
  "action": "DATA_ANALYSIS",
  "target_table": "NOME_DA_TABELA_MAIS_PROVAVEL",
  "reason": "O usuário está buscando o contato de uma pessoa específica."
}
OU
{
  "action": "CHAT",
  "text": "Olá! Como posso ajudar você hoje?"
}
"""
        decision_context = f"{system_decision_prompt}\n\n### TABELAS DISPONÍVEIS (CAPACIDADES) ###\n{topics_str}\n"
        
        # Injeção de Exemplos de Decisão (Busca Ambígua, Direta, etc)
        # No modo agente, pulamos busca semântica para evitar travamentos
        decision_examples = self.behavior_manager.format_patterns_for_prompt(prompt, skip_semantic=(mode == 'agente'))
        if decision_examples:
            decision_context += "\n### EXEMPLOS DE COMPORTAMENTO ###\n" + decision_examples

        # Se a IA estiver offline, não perdemos tempo tentando a decisão
        if self._is_ia_offline():
            print("🔌 IA detectada como offline na fase de decisão.")
            if mode == 'agente':
                return {"action": "DATA_ANALYSIS", "reason": "IA offline, forçando busca por plano treinado no modo agente."}
            return {"action": "CHAT", "text": "Olá! Estou operando em modo limitado no momento porque meu cérebro principal (IA) está offline. Posso ter dificuldades em entender perguntas complexas, mas ainda posso tentar ajudar com comandos básicos."}

        try:
            # Chamada de Decisão
            response = self._call_ai_with_limits(
                f"Usuário: {prompt}\n{history_context}", 
                decision_context, 
                num_predict=250, 
                num_ctx=2048,
                temperature=0.0
            )
            print(f"🔍 DECISÃO DA IA (INTENÇÃO): '{response}'")
            
            decision = self._extract_json(response)
            if not decision:
                # Se a IA falhou no JSON, tentamos uma última vez com contexto mínimo
                retry_context = "JSON: {\"action\": \"CHAT\", \"text\": \"...\"}"
                response_retry = self._call_ai_with_limits(prompt, retry_context, num_predict=100, num_ctx=1024)
                decision = self._extract_json(response_retry)
                
                if not decision:
                    # Se mesmo assim falhar, retornamos o texto bruto
                    return {"action": "CHAT", "text": response_retry.strip()}
            
            action = decision.get('action', 'CHAT').upper()
            
            # REFORÇO MODO AGENTE: Se for pergunta de dados mas a IA disse CHAT, força DATA_ANALYSIS
            if mode == 'agente' and action == 'CHAT':
                data_keywords = ['QUANTOS', 'QUEM', 'TOTAL', 'LISTA', 'CONTATO', 'EMPRESA', 'PRODUTO', 'VENDA', 'QUAL']
                prompt_upper = prompt.upper()
                if any(kw in prompt_upper for kw in data_keywords):
                    print(f"🔄 Forçando DATA_ANALYSIS no Modo Agente por palavra-chave: '{prompt}'")
                    action = 'DATA_ANALYSIS'
            
            if action == 'CHAT':
                return decision

            # Se for DATA_ANALYSIS, precisamos construir o plano técnico
            if action == 'DATA_ANALYSIS':
                target_table_name = decision.get('target_table')
                target_table = None
                
                if target_table_name:
                    target_table = next((t for t in tables if t.get('table_name').upper() == target_table_name.upper()), None)
                
                if not target_table:
                    relevant = self.discover_relevant_tables(prompt, tables, top_n=1)
                    if not relevant: 
                        # Retorna chat via imitação se não houver dados
                        return {"action": "CHAT", "text": self._decide_by_examples(prompt, [], username)}
                    target_table = relevant[0]

                # Agora sim fazemos o "heavy lifting" apenas para a tabela selecionada
                return self._generate_data_plan(prompt, target_table, history, username, mode=mode)

        except Exception as e:
            print(f"❌ ERRO na análise unificada: {e}")
            return None

    def _generate_data_plan(self, prompt: str, table: Dict[str, Any], history: List[Dict[str, Any]], username: str, mode: str = 'chat') -> Optional[Dict[str, Any]]:
        """Gera o plano técnico de SQL baseado no treinamento e catálogo da tabela."""
        table_copy = table.copy()
        all_cols = table_copy.get('columns_info', [])
        
        # Mantém as colunas originais mas garante que a descrição semântica esteja presente
        catalog = self._build_table_catalog([table_copy])
        
        # 1. FORMATAR HISTÓRICO PARA O PLANO (Essencial para resolver pronomes e correções)
        history_context = ""
        if history:
            history_context = "\n### CONTEXTO DA CONVERSA (USE PARA RESOLVER REFERÊNCIAS) ###\n"
            for msg in history[-3:]: # As 3 últimas são suficientes para o plano
                role = "USUÁRIO" if msg.get('role') == 'user' else "IA"
                content = msg.get('content', '')
                history_context += f"{role}: {content[:150]}\n"
            history_context += "### FIM DO CONTEXTO ###\n"

        # 2. Busca exemplos de treinamento específicos desta tabela
        self._load_training_cache()
        trained_examples = ""
        if self._training_cache:
            normalized = self._normalize_question(prompt)
            q_words = set(normalized.split())
            matches = []
            for item in self._training_cache:
                # Só usa exemplos que pertencem a esta tabela ou são muito similares
                plan = item.get('plan', {})
                if plan.get('table', '').upper() == table.get('table_name', '').upper():
                    t = item.get('q', '')
                    t_words = set(t.split())
                    score = len(q_words.intersection(t_words)) / len(q_words.union(t_words)) if q_words.union(t_words) else 0
                    if score > 0.2: matches.append((score, item))
            
            matches.sort(key=lambda x: x[0], reverse=True)
            for _, item in matches[:3]:
                trained_examples += f"Pergunta: {item.get('q')}\nPlano: {json.dumps(item.get('plan'))}\n"

        # 3. Contexto de Geração (Instruções, Catálogo e Histórico)
        if mode == 'agente':
            system_plan_prompt = """Você é o arquiteto de SQL ESPECIALISTA da Rohden. 
Sua missão é transformar perguntas naturais em consultas SQL precisas, PRIORIZANDO os padrões dos EXEMPLOS DE REFERÊNCIA.

REGRAS CRÍTICAS (MODO AGENTE):
1. Siga RIGOROSAMENTE o estilo dos EXEMPLOS DE REFERÊNCIA abaixo.
2. Se o usuário perguntar por um NOME ou DESCRIÇÃO, use a cláusula WHERE UPPER(coluna) LIKE '%VALOR_EM_MAIUSCULO%'.
3. Se a pergunta for sobre "contato", procure por colunas como NOME, EMAIL, TELEFONE ou similar.
4. Se o usuário perguntar "quantos", "total" ou "quantidade", gere um SQL com SELECT COUNT(*) ou similar.
5. Retorne APENAS o JSON. Não escreva explicações fora do JSON.
6. Limite o resultado a 5 linhas se não houver um filtro específico e não for uma contagem.

FORMATO JSON OBRIGATÓRIO:
{
  "action": "DATA_ANALYSIS",
  "target_table": "NOME_DA_TABELA",
  "sql": "SELECT ... FROM ... WHERE ...",
  "reason": "Explicação curta do que está sendo buscado."
}
"""
        else:
            system_plan_prompt = """Você é o arquiteto de SQL da Rohden. Sua missão é transformar perguntas naturais em consultas SQL precisas.

REGRAS CRÍTICAS:
1. Retorne APENAS o JSON. Não escreva explicações fora do JSON.
2. Se o usuário perguntar por um NOME ou DESCRIÇÃO, use a cláusula WHERE UPPER(coluna) LIKE '%VALOR_EM_MAIUSCULO%'.
3. Se a pergunta for sobre "contato", procure por colunas como NOME, EMAIL, TELEFONE ou similar.
4. Se o usuário perguntar "quantos", "total" ou "quantidade", gere um SQL com SELECT COUNT(*) ou similar.
5. Nunca diga que não pode fazer a busca por falta de ID. Use o que o usuário forneceu (nomes, partes de nomes).
6. Limite o resultado a 5 linhas se não houver um filtro específico e não for uma contagem.

FORMATO JSON OBRIGATÓRIO:
{
  "action": "DATA_ANALYSIS",
  "target_table": "NOME_DA_TABELA",
  "sql": "SELECT ... FROM ... WHERE ...",
  "reason": "Explicação curta do que está sendo buscado."
}
"""
        plan_context = (
            f"{system_plan_prompt}\n\n"
            f"### CATÁLOGO DA TABELA ###\n{json.dumps(catalog, ensure_ascii=False)}\n\n"
            f"{history_context}\n"
            f"### EXEMPLOS DE REFERÊNCIA ###\n{trained_examples}"
        )
        
        response = self._call_ai_with_limits(prompt, plan_context, num_predict=500, num_ctx=2048, temperature=0.0)
        print(f"🔍 PLANO DE DADOS GERADO ({mode}): '{response}'")
        
        plan = self._extract_json(response)
        
        # Fallback de emergência para Modo Agente (Se a IA falhar em gerar JSON ou SQL)
        if mode == 'agente' and (not plan or not plan.get('sql')):
            t_name = table.get('table_name')
            prompt_lower = prompt.lower()
            
            # Se a IA retornou algo que não é JSON, mas parece ser SQL
            if not plan and "SELECT" in response.upper():
                import re
                sql_match = re.search(r'SELECT.*', response.replace('\n', ' '), re.IGNORECASE)
                if sql_match:
                    return {
                        "action": "DATA_ANALYSIS",
                        "target_table": t_name,
                        "sql": sql_match.group(0).strip(),
                        "reason": "SQL extraído de resposta não-JSON."
                    }

            # Fallback absoluto baseado em palavras-chave
            sql_fallback = f"SELECT * FROM {t_name} FETCH FIRST 5 ROWS ONLY"
            if any(w in prompt_lower for w in ['quantos', 'total', 'quantidade', 'soma']):
                sql_fallback = f"SELECT COUNT(*) as TOTAL FROM {t_name}"
            
            return {
                "action": "DATA_ANALYSIS",
                "target_table": t_name,
                "sql": sql_fallback,
                "reason": "Fallback de emergência para modo Agente."
            }

        return plan

    def perform_advanced_training(self, table_name, columns, samples):
        """Realiza o treinamento semântico avançado de uma tabela (Apenas dados, sem instruções)"""
        data_context = {
            "tabela": table_name,
            "colunas": [f"{c['name']} ({c['type']})" for c in columns],
            "amostras": samples[:2]
        }
        
        prompt = f"Treinamento: {json.dumps(data_context, ensure_ascii=False)}"
        
        # Chamada sem system prompt instrutivo
        return self._call_ai(prompt, "")

    def _get_cached_memory_context(self, username):
        """Cache de contexto de memória para evitar múltiplas consultas"""
        import time
        current_time = time.time()
        
        # Cache válido por 2 minutos
        if username in self._memory_cache:
            cache_time, context = self._memory_cache[username]
            if current_time - cache_time < 120:  # 2 minutos
                return context
        
        # Buscar novo contexto
        context = memoria_system.get_user_memory_context(username)
        self._memory_cache[username] = (current_time, context)
        return context

    def _call_ai(self, prompt, system_prompt):
        """
        Encapsula a chamada à IA com foco exclusivo nos servidores Rohden AI.
        Usa limites seguros para estabilidade do servidor GGUF.
        """
        return self._call_ai_with_limits(prompt, system_prompt, num_predict=800, num_ctx=2048, temperature=0.1)

def get_llama_engine():
    return LlamaEngine()
