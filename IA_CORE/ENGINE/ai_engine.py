import os
import requests
import json
import re
import random
from typing import Any, Dict, List, Optional, Tuple
from conecxaodb import get_connection
from ..MEMORIA import memoria_system
from ..INTERPRETER import interpretar_pergunta, interpreter
from .sql_builder import SQLBuilder
from .vector_manager import VectorManager
from ..DATA import storage
from dotenv import load_dotenv

# Carregar variáveis de ambiente do arquivo .env de forma robusta
env_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), '..', '..', '..', '..', '.env')
if os.path.exists(env_path):
    load_dotenv(dotenv_path=env_path)
else:
    load_dotenv() # Fallback para o CWD

def load_local_config():
    """Carrega a configuração do sistema robusto de dados (SQLite)"""
    try:
        tables_db = storage.load_tables()
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
        learned_db = storage.get_knowledge(category='learned')
        learned = []
        for fact in learned_db:
            learned.append({
                'content': fact['content'],
                'user': fact.get('tags', ''), # Usando tags para o usuário por enquanto
                'date': fact['created_at']
            })
            
        return {
            'tables': tables, 
            'metadata': metadata, 
            'knowledge': {},
            'learned': learned
        }
    except Exception as e:
        print(f"Erro ao carregar configuração: {e}")
        return {'tables': [], 'metadata': {}, 'knowledge': {}, 'learned': []}

def save_local_config(config):
    """Salva a configuração no sistema robusto de dados (SQLite)"""
    # Esta função agora é usada principalmente para salvar fatos aprendidos
    # pois as tabelas são salvas via routes.py diretamente no storage
    try:
        if 'learned' in config:
            for fact in config['learned']:
                # Verifica se já existe
                existing = storage.get_knowledge(category='learned')
                if not any(f['content'] == fact['content'] for f in existing):
                    storage.save_knowledge(
                        category='learned',
                        title='Fato Aprendido',
                        content=fact['content'],
                        tags=fact.get('user', ''),
                        priority=1
                    )
    except Exception as e:
        print(f"Erro ao salvar configuração: {e}")

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
    
    def __new__(cls):
        if cls._instance is None:
            cls._instance = super(LlamaEngine, cls).__new__(cls)
            cls._instance.llm = None
            cls._instance.vector_manager = VectorManager()
            
            # Configuração do Motor Principal (Rohden AI Server)
            cls._instance.ai_url = os.getenv("ROHDEN_AI_URL")
            cls._instance.ai_url_internal = os.getenv("ROHDEN_AI_INTERNAL_URL")
            cls._instance.ai_url_localhost = "http://127.0.0.1:5005/api/generate"
            cls._instance.ai_model = os.getenv("ROHDEN_AI_MODEL", "llama3.1-gguf")
            cls._instance.api_key = os.getenv("ROHDEN_AI_KEY", "ROHDEN_AI_SECRET_2024")
            
            # Headers para o Servidor Rohden
            cls._instance.rohden_headers = {
                'X-ROHDEN-AI-KEY': cls._instance.api_key,
                'Content-Type': 'application/json',
                'User-Agent': 'RohdenAI-Assistant/1.0'
            }
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
        """Busca as permissões do usuário na tabela AI_USER_TABLE_ACCESS e metadados no JSON"""
        if not username:
            return []
            
        config = self._load_cached_config()
        metadata = config.get('metadata', {})
        allowed_tables = {} # {TABLE_NAME: ACCESS_LEVEL}
        
        # 1. Buscar permissões no Oracle
        conn = get_connection()
        if conn:
            try:
                cursor = conn.cursor()
                query = "SELECT TABLE_NAME, ACCESS_LEVEL FROM SYSROH.AI_USER_TABLE_ACCESS WHERE UPPER(USUARIO_DS) = UPPER(:v_user)"
                cursor.execute(query, {'v_user': username})
                for row in cursor.fetchall():
                    allowed_tables[row[0].upper()] = row[1] if row[1] else 1
            except Exception:
                return []
            finally:
                cursor.close()
                conn.close()

        # 2. Construir lista de permissões com metadados
        permissions = []
        for table in config.get('tables', []):
            table_name_upper = table['name'].upper()
            
            # "Todos tem que ter a permissão 1" 
            # Se não houver permissão específica no banco, assume Nível 1 por padrão
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
                from ..DATA.storage import storage

                results = execute_sql_safe(sql_query, tuple(params) if params else None, storage.db_path)

            return results
        except Exception as e:
            return f"Erro ao executar SQL: {str(e)}"

    def _format_results(self, query_plan: Dict[str, Any], results: List[Dict[str, Any]]) -> str:
        if not results:
            label = self._table_label(query_plan.get('table'))
            if label:
                return f"Nao encontrei nenhum {label} com esse criterio."
            return "Nao encontrei nenhum registro com esse criterio."

        # Caso de agregacao (ex: COUNT)
        if query_plan.get('aggregations'):
            total = None
            for key in ['TOTAL', 'total', 'Total']:
                if key in results[0]:
                    total = results[0][key]
                    break
            if total is not None:
                label = self._table_label(query_plan.get('table'))
                prefix = f"Total de {label}s" if label else "Total"
                return f"{prefix}: {total}."

        fields = query_plan.get('fields') or list(results[0].keys())
        fields = [f for f in fields if f in results[0]] or list(results[0].keys())

        if len(results) == 1:
            return self._format_single_row(results[0], fields)

        preview = results[:5]
        linhas = [self._format_row_line(row, fields) for row in preview]
        linhas = [linha for linha in linhas if linha]
        total = len(results)
        header = f"Encontrei {total} registros."
        if not linhas:
            return header
        return header + "\n" + "\n".join(linhas)

    def _format_single_row(self, row: Dict[str, Any], fields: List[str]) -> str:
        partes = []
        for field in fields:
            value = row.get(field)
            if value is None:
                continue
            partes.append(f"{self._humanize_field_name(field)}: {value}")
        if partes:
            return "Registro encontrado: " + ", ".join(partes)
        return "Registro encontrado."

    def _format_row_line(self, row: Dict[str, Any], fields: List[str]) -> str:
        partes = []
        for field in fields:
            value = row.get(field)
            if value is None:
                continue
            partes.append(f"{self._humanize_field_name(field)}: {value}")
        if not partes:
            return ""
        return "- " + ", ".join(partes)

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

    def _humanize_response(self, prompt: str, results: List[Dict[str, Any]], fallback_text: str) -> str:
        if not results:
            return fallback_text

        data_preview = results[:5]
        data_json = json.dumps(data_preview, ensure_ascii=False, default=str, indent=2)
        system_prompt = "Voce e um assistente corporativo. Responda curto e claro."
        user_prompt = (
            f"Pergunta: {prompt}\n"
            f"Dados:\n{data_json}\n\n"
            "Gere uma resposta objetiva e direta, sem mencionar tabelas tecnicas."
        )

        try:
            response = self._call_ai_with_limits(user_prompt, system_prompt, num_predict=384, num_ctx=2048, temperature=0.2)
            if response:
                return response.strip()
        except Exception:
            pass

        return fallback_text

    def discover_relevant_tables(self, prompt, all_tables, top_n=3):
        """Usa busca vetorial para encontrar as tabelas mais relevantes para a pergunta"""
        if not all_tables:
            return []
            
        # Tenta busca vetorial
        relevant = self.vector_manager.find_most_similar(prompt, all_tables, top_n=top_n)
        
        if relevant:
            print(f"Busca Vetorial encontrou {len(relevant)} tabelas relevantes.")
            return relevant
            
        # Fallback: Se vetores falharem, retorna as primeiras N (comportamento antigo)
        return all_tables[:top_n]

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

        # Tenta extrair bloco ```json```
        json_block = re.search(r'```json\s*(\{.*?\})\s*```', text, re.DOTALL | re.IGNORECASE)
        if json_block:
            text = json_block.group(1)

        # Fallback: pega do primeiro { ao ultimo }
        if '{' in text and '}' in text:
            text = text[text.find('{'):text.rfind('}') + 1]

        try:
            return json.loads(text)
        except Exception:
            return None

    def _call_ai_with_limits(self, prompt: str, system_prompt: str, num_predict: int, num_ctx: int = 1024, temperature: float = 0.1) -> str:
        """Wrapper leve para chamadas rápidas (decisão/chat/plano) com poucos tokens."""
        import time
        candidate_urls = []

        # Único endpoint: IP interno (dev e prod)
        candidate_urls.append(self.ai_url_internal or "http://192.168.1.217:5005/api/generate")

        candidate_urls = list(dict.fromkeys([u for u in candidate_urls if u]))
        now = time.time()
        def url_priority(u):
            if u in self._degraded_urls:
                fail_time, count = self._degraded_urls[u]
                if now - fail_time < 300:
                    return count + 10
            return 0
        candidate_urls.sort(key=url_priority)

        for url in candidate_urls:
            try:
                target_url = url
                if not target_url.endswith('/api/generate') and not ':5005' in target_url:
                    target_url = target_url.rstrip('/') + '/api/generate'

                payload = {
                    "model": self.ai_model or "llama3.1-gguf",
                    "prompt": prompt,
                    "system": system_prompt,
                    "stream": False,
                    "options": {
                        "temperature": temperature,
                        "num_predict": int(num_predict),
                        "num_ctx": int(num_ctx)
                    }
                }

                is_internal = any(x in target_url for x in ['127.0.0.1', 'localhost', '192.168.'])
                timeout = 12
                print(f"Tentando Rohden AI em: {target_url} (timeout: {timeout}s)")

                import urllib3
                urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

                response = requests.post(
                    target_url,
                    json=payload,
                    headers=self.rohden_headers,
                    timeout=timeout,
                    verify=False
                )

                if response.status_code == 200:
                    if url in self._degraded_urls:
                        del self._degraded_urls[url]
                    try:
                        res_json = response.json()
                        return res_json.get("response", "").strip()
                    except Exception:
                        return response.text.strip()

                fail_time, count = self._degraded_urls.get(url, (now, 0))
                self._degraded_urls[url] = (now, count + 1)
            except Exception as e:
                if "timed out" in str(e).lower():
                    print(f"TIMEOUT em {url} após {timeout}s. Tentando próximo...")
                else:
                    print(f"Falha ao conectar em {url}: {str(e)}")
                fail_time, count = self._degraded_urls.get(url, (now, 0))
                self._degraded_urls[url] = (now, count + 1)
                continue

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
            items = storage.get_knowledge(category='QUERY_PLAN_TRAINING', limit=300)
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

        if best_score >= 0.88 and isinstance(best_plan, dict):
            self._plan_cache[normalized] = best_plan
            return best_plan

        return None

    def _decide_action_with_ai(self, prompt: str, tables: List[Dict[str, Any]]) -> str:
        normalized = self._normalize_question(prompt)
        if normalized in self._decision_cache:
            return self._decision_cache.get(normalized) or 'QUERY'

        catalog = self._build_table_catalog(tables)
        system_prompt = (
            "Voce decide se precisa consultar dados (QUERY) ou se e conversa (CHAT). "
            "Responda APENAS JSON valido: {\"action\":\"QUERY\"|\"CHAT\"}."
        )
        user_prompt = (
            "Pergunta: " + prompt + "\n\n"
            "Se a pergunta pedir numeros, listas, informacoes de clientes/contatos/produtos/vendas, use QUERY. "
            "Se for cumprimento, conversa, agradecimento, use CHAT.\n\n"
            "Tabelas disponiveis:\n" + json.dumps(catalog, ensure_ascii=False)
        )

        try:
            response = self._call_ai_with_limits(user_prompt, system_prompt, num_predict=64, num_ctx=1024, temperature=0.0)
            if not response:
                action = 'CHAT'
            else:
                data = self._extract_json(response)
                action = str((data or {}).get('action') or 'CHAT').upper().strip()
        except Exception:
            action = 'CHAT'

        if action not in ('QUERY', 'CHAT'):
            action = 'QUERY'

        self._decision_cache[normalized] = action
        return action

    def _generate_query_plan_with_ai(self, prompt: str, tables: List[Dict[str, Any]]) -> Optional[Dict[str, Any]]:
        catalog = self._build_table_catalog(tables)
        system_prompt = (
            "Voce e um planejador de consultas corporativas. "
            "Retorne APENAS um JSON valido, sem explicacoes. "
            "Nao gere SQL. Se a pergunta nao for sobre dados, retorne {\"type\": \"NONE\"}."
        )

        example = {
            "type": "SELECT",
            "schema": "SYSROH",
            "table": "TB_CONTATOS",
            "fields": ["NOME", "EMAIL", "CELULAR"],
            "filters": [
                {"field": "NOME", "op": "LIKE", "value": "%Rudieri%", "case_insensitive": True}
            ],
            "limit": 5
        }

        user_prompt = (
            "Pergunta: " + prompt + "\n\n"
            "Tabelas disponiveis (use apenas estas):\n" + json.dumps(catalog, ensure_ascii=False) + "\n\n"
            "Formato esperado (exemplo):\n" + json.dumps(example, ensure_ascii=False) + "\n\n"
            "Regras:\n"
            "- table e schema devem existir nas tabelas disponiveis\n"
            "- fields/filters devem usar nomes exatos das colunas\n"
            "- para busca textual use LIKE com %\n"
            "- limite padrao 50 se nao souber"
        )

        response = self._call_ai_with_limits(user_prompt, system_prompt, num_predict=256, num_ctx=2048, temperature=0.0)
        if not response:
            return None
        plan = self._extract_json(response)
        if plan and str(plan.get('type', '')).upper() == 'NONE':
            return None
        return plan

    def _validate_query_plan(self, plan: Dict[str, Any], tables: List[Dict[str, Any]]) -> Tuple[Optional[Dict[str, Any]], Optional[str]]:
        if not plan:
            return None, "Posso ajudar com consultas a dados. O que voce precisa?"
        if str(plan.get('type', '')).upper() == 'NONE':
            return None, "Posso ajudar com consultas a dados. O que voce precisa?"
        if plan.get('type') != 'SELECT':
            return None, "Plano invalido."

        table_map = {}
        for table in tables:
            schema_info = table.get('schema_info') or {}
            if isinstance(schema_info, str):
                try:
                    schema_info = json.loads(schema_info)
                except Exception:
                    schema_info = {}
            schema = schema_info.get('schema', 'SYSROH')
            columns = [col.get('name') for col in table.get('columns_info', []) if col.get('name')]
            table_map[table.get('table_name')] = {
                'schema': schema,
                'columns': columns
            }

        table_name = plan.get('table')
        if table_name not in table_map:
            return None, "Tabela nao autorizada."

        schema = plan.get('schema') or table_map[table_name]['schema']
        plan['schema'] = schema

        allowed_columns = table_map[table_name]['columns']
        column_map = {c.upper(): c for c in allowed_columns}

        fields = plan.get('fields') or []
        if fields:
            sanitized = []
            for field in fields:
                if field == '*':
                    sanitized.append('*')
                    continue
                key = str(field).upper()
                if key in column_map:
                    sanitized.append(column_map[key])
            plan['fields'] = sanitized

        filters = plan.get('filters') or []
        sanitized_filters = []
        for flt in filters:
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
            sanitized_filters.append({
                'field': column_map[key],
                'op': op,
                'value': value,
                'case_insensitive': bool(flt.get('case_insensitive')),
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

    def _get_optimized_system_prompt(self, relevant_tables: List[Dict], interpretacao: Dict) -> str:
        """Constrói um system prompt cirúrgico, enviando apenas o necessário para a IA"""
        
        # Base fixa, humana e inteligente
        sys = "Você é o Rohden AI, o assistente inteligente oficial da Rohden. Seu tom de voz é amigável, leve, profissional e muito humano.\n"
        sys += "PROIBIDO: Usar frases robóticas como 'decidi responder', 'com base na análise' ou 'analisando os dados'.\n"
        sys += "PROIBIDO: Mencionar nomes técnicos de tabelas ou colunas (ex: TB_CONTATOS, ID_CONTATO) na resposta final. Use termos naturais (ex: 'contatos', 'código').\n"
        sys += "Vá direto ao ponto: responda de forma natural, como se estivesse conversando com um colega de trabalho.\n"
        sys += "Seja inteligente: varie suas respostas e use emojis de forma equilibrada para manter o tom leve.\n\n"
        
        # Se não houver tabelas relevantes, não enviamos nada de metadados
        if not relevant_tables:
            return sys + "Responda de forma prestativa. No momento, você não tem acesso a dados específicos para esta pergunta, mas pode conversar normalmente."

        # Se houver tabelas, enviamos apenas os nomes e descrições primeiro
        sys += "\nCONHECIMENTO DISPONÍVEL (DADOS):\n"
        for t in relevant_tables:
            sys += f"- {t['table_name']}: {t.get('table_description', 'Informações sobre ' + t['table_name'])}\n"
        
        # Sempre enviamos colunas se houver tabelas relevantes (para garantir que a IA saiba montar o SQL)
        if relevant_tables:
            sys += "\nCAMPOS QUE VOCÊ PODE CONSULTAR:\n"
            for t in relevant_tables:
                cols = [c['name'] for c in t.get('columns_info', [])[:15]] # Limite para foco
                sys += f"- {t['table_name']}: {', '.join(cols)}\n"
        
        sys += "\nDIRETRIZES:\n"
        sys += "1. Se a pergunta envolver busca de informações (quem é, qual o contato, quanto vendeu, etc), você DEVE obrigatoriamente usar [SQL]comando[/SQL] para consultar o banco.\n"
        sys += "2. O schema é OBRIGATORIAMENTE 'SYSROH'. Todas as tabelas devem ser prefixadas (ex: SYSROH.TB_CONTATOS).\n"
        sys += "3. Responda como um colega de trabalho prestativo, não como uma máquina.\n"
        sys += "4. Se não encontrar o dado no SQL, informe que não localizou, mas não peça desculpas antes de tentar buscar."
        return sys

    def generate_response(self, prompt, username=None, history=None):
        """Gera uma resposta humana baseada na consulta SQL e resultados"""
        import time
        start_time = time.time()
        
        # Objeto de metadados para persistência
        metadata = {
            'tabelas_usadas': [],
            'intencao': None,
            'confianca': 0
        }
        
        try:
            # Recarregar tabelas treinadas para garantir dados atualizados
            from ..DATA.storage import storage
            config = storage.load_tables()
            
            # 0. INTERPRETAÇÃO INTELIGENTE DA PERGUNTA (RÁPIDA)
            interpretacao = {'confianca_geral': 0}
            try:
                interpret_start = time.time()
                from ..INTERPRETER.interpreter import interpretar_pergunta
                interpretacao = interpretar_pergunta(prompt, username, history)
                print(f"Interpretação levou: {time.time() - interpret_start:.3f}s")
                
                # Atualizar metadados
                metadata['tabelas_usadas'] = interpretacao.get('entidades', {}).get('tabelas', [])
                metadata['intencao'] = interpretacao.get('intencao', {}).get('tipo')
                metadata['confianca'] = interpretacao.get('confianca_geral', 0)
            except Exception as e:
                print(f"Erro na interpretação: {e}")
                pass
            
            # 1. PREPARAÇÃO DO CONTEXTO BASE E BUSCA DE TABELAS
            confianca = interpretacao.get('confianca_geral', 0)
            entidades = interpretacao.get('entidades', {})
            tabelas_identificadas = entidades.get('tabelas', [])
            
            # Sempre começa com as tabelas que o interpretador já achou (por palavras-chave)
            relevant_tables = [t for t in config if t['table_name'] in tabelas_identificadas]
            
            # Se a confiança for baixa ou não achou nada, reforça com busca vetorial
            if confianca < 0.7 or not relevant_tables:
                vector_start = time.time()
                v_tables = self.discover_relevant_tables(prompt, config)
                print(f"Busca Vetorial levou: {time.time() - vector_start:.3f}s")
                
                # Merge sem duplicados
                existing_names = [t['table_name'] for t in relevant_tables]
                for vt in v_tables:
                    if vt['table_name'] not in existing_names:
                        relevant_tables.append(vt)
                        if vt['table_name'] not in metadata['tabelas_usadas']:
                            metadata['tabelas_usadas'].append(vt['table_name'])
            
            # Atualiza metadados final
            for t in relevant_tables:
                if t['table_name'] not in metadata['tabelas_usadas']:
                    metadata['tabelas_usadas'].append(t['table_name'])

            action = self._decide_action_with_ai(prompt, relevant_tables or config)
            if action == 'CHAT':
                system_prompt = "Voce e um assistente corporativo. Responda de forma curta e natural."
                chat_prompt = f"Usuario: {prompt}\nAssistente:"
                chat_resp = self._call_ai_with_limits(chat_prompt, system_prompt, num_predict=96, num_ctx=1024, temperature=0.7)
                return {'text': chat_resp or "Oi! Como posso ajudar?", 'metadata': metadata}

            trained_plan = self._find_trained_plan(prompt)

            # 2. IA gera plano semântico -> valida -> SQL determinístico
            query_plan = trained_plan or self._generate_query_plan_with_ai(prompt, relevant_tables or config)
            validated_plan, plan_error = self._validate_query_plan(query_plan, config)

            if not validated_plan:
                fallback_plan = interpretacao.get('query_plan')
                validated_plan, plan_error = self._validate_query_plan(fallback_plan, config)

            if not validated_plan:
                sugestoes = interpretacao.get('sugestoes', [])
                sugestao_texto = ""
                if sugestoes:
                    sugestao_texto = " Sugestões: " + ", ".join(sugestoes[:2])
                erro_texto = plan_error or "Não consegui identificar quais dados consultar."
                return {
                    'text': erro_texto + sugestao_texto,
                    'metadata': metadata
                }

            if validated_plan.get('table') and validated_plan['table'] not in metadata['tabelas_usadas']:
                metadata['tabelas_usadas'].append(validated_plan['table'])

            try:
                sql_query, params, dialect = SQLBuilder.from_plan(validated_plan).build()
            except Exception as build_err:
                return {
                    'text': f"Erro ao montar a consulta: {str(build_err)}",
                    'metadata': metadata
                }

            self._store_training(prompt, validated_plan, sql_query, username)

            # Executar o SQL
            sql_exec_start = time.time()
            sql_result = self.execute_sql(sql_query, params=params, dialect=dialect)
            print(f"Execução SQL levou: {time.time() - sql_exec_start:.3f}s")

            # Pós-processamento determinístico
            if isinstance(sql_result, str):
                response = sql_result
            else:
                response = self._format_results(validated_plan, sql_result)

            # IA opcional apenas para linguagem
            use_ai_language = os.getenv("ROHDEN_AI_LANGUAGE_ONLY", "false").lower() in ("1", "true", "yes")
            if use_ai_language and not isinstance(sql_result, str):
                response = self._humanize_response(prompt, sql_result, response)

            # 3. Verificar aprendizado (Tag [LEARN] e [LEARN_TYPO])
            # ... (mantém o resto igual)

            # 3. Verificar aprendizado (Tag [LEARN] e [LEARN_TYPO])
            learn_match = re.search(r'\[LEARN\]\s*(.*?)\s*\[/LEARN\]', response, re.DOTALL | re.IGNORECASE)
            if learn_match:
                self.learn_fact(learn_match.group(1).strip(), username)
                response = re.sub(r'\[LEARN\].*?\[/LEARN\]', '', response, flags=re.DOTALL | re.IGNORECASE).strip()
            
            learn_typo_match = re.search(r'\[LEARN_TYPO\]\s*(.*?)\s*\[/LEARN_TYPO\]', response, re.DOTALL | re.IGNORECASE)
            if learn_typo_match:
                try:
                    partes = learn_typo_match.group(1).split('|')
                    if len(partes) == 2:
                        erro, correto = partes
                        interpreter._aprender_termo(erro.strip(), correto.strip())
                except:
                    pass
                response = re.sub(r'\[LEARN_TYPO\].*?\[/LEARN_TYPO\]', '', response, flags=re.DOTALL | re.IGNORECASE).strip()
            
            # 4. APRENDIZADO AUTOMÁTICO CONVERSACIONAL (controlado com cache)
            if username and prompt:
                memoria_system.extract_learning_from_interaction(username, prompt, response)
                
            return {'text': response, 'metadata': metadata}
        except Exception as e:
            return {'text': f"Desculpe, encontrei um erro ao processar sua pergunta: {str(e)}", 'metadata': metadata}

    def perform_advanced_training(self, table_name, columns, samples):
        """Realiza o treinamento semântico avançado de uma tabela em uma única chamada de IA"""
        prompt = f"### ANALISE TÉCNICA DE TABELA PARA TREINAMENTO ###\n"
        prompt += f"Tabela: {table_name}\n"
        prompt += f"Colunas: {', '.join([f'{c['name']} ({c['type']})' for c in columns])}\n"
        prompt += f"Amostra de Dados (JSON): {json.dumps(samples[:2], indent=2)}\n\n"
        prompt += "### TAREFAS ###\n"
        prompt += "1. Explique o propósito desta tabela no contexto da empresa Rohden.\n"
        prompt += "2. Liste as 5 colunas mais importantes e o que elas representam para um usuário leigo.\n"
        prompt += "3. Gere 3 exemplos de perguntas reais que um gestor faria e o respectivo SQL Oracle (use sempre prefixo de schema SYSROH).\n\n"
        prompt += "Responda de forma estruturada, profissional e em Português."
        
        return self._call_ai(prompt, "Você é um Analista de Dados Sênior e Especialista em SQL Oracle.")

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

    _degraded_urls = {}    # Cache de URLs lentas ou offline

    def _call_ai_with_limits(self, prompt: str, system_prompt: str, num_predict: int, num_ctx: int = 1024, temperature: float = 0.1) -> str:
        """Wrapper leve para chamadas rápidas (decisão/chat/plano) com poucos tokens."""
        import time
        candidate_urls = []

        # Único endpoint: IP interno (dev e prod)
        candidate_urls.append(self.ai_url_internal or "http://192.168.1.217:5005/api/generate")

        candidate_urls = list(dict.fromkeys([u for u in candidate_urls if u]))
        now = time.time()
        def url_priority(u):
            if u in self._degraded_urls:
                fail_time, count = self._degraded_urls[u]
                if now - fail_time < 300:
                    return count + 10
            return 0
        candidate_urls.sort(key=url_priority)

        for url in candidate_urls:
            try:
                target_url = url
                if not target_url.endswith('/api/generate') and not ':5005' in target_url:
                    target_url = target_url.rstrip('/') + '/api/generate'

                payload = {
                    "model": self.ai_model or "llama3.1-gguf",
                    "prompt": prompt,
                    "system": system_prompt,
                    "stream": False,
                    "options": {
                        "temperature": temperature,
                        "num_predict": int(num_predict),
                        "num_ctx": int(num_ctx)
                    }
                }

                is_internal = any(x in target_url for x in ['127.0.0.1', 'localhost', '192.168.'])
                timeout = 12
                print(f"Tentando Rohden AI em: {target_url} (timeout: {timeout}s)")

                import urllib3
                urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

                response = requests.post(
                    target_url,
                    json=payload,
                    headers=self.rohden_headers,
                    timeout=timeout,
                    verify=False
                )

                if response.status_code == 200:
                    if url in self._degraded_urls:
                        del self._degraded_urls[url]
                    try:
                        res_json = response.json()
                        return res_json.get("response", "").strip()
                    except Exception:
                        return response.text.strip()

                fail_time, count = self._degraded_urls.get(url, (now, 0))
                self._degraded_urls[url] = (now, count + 1)
            except Exception as e:
                if "timed out" in str(e).lower():
                    print(f"TIMEOUT em {url} após {timeout}s. Tentando próximo...")
                else:
                    print(f"Falha ao conectar em {url}: {str(e)}")
                fail_time, count = self._degraded_urls.get(url, (now, 0))
                self._degraded_urls[url] = (now, count + 1)
                continue

        return ""

    def _call_ai(self, prompt, system_prompt):
        """
        Encapsula a chamada à IA com foco exclusivo nos servidores Rohden AI.
        Usa apenas o IP interno (dev e prod), sem fallback.
        """
        # Usa a mesma função rápida com tokens maiores (mantém compatibilidade)
        return self._call_ai_with_limits(prompt, system_prompt, num_predict=2048, num_ctx=4096, temperature=0.1)

def get_llama_engine():
    return LlamaEngine()
