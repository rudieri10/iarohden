import os
import requests
import json
import re
import random
from typing import Any, Dict, List, Optional, Tuple
from conecxaodb import get_connection
from ..MEMORIA import memoria_system
from .sql_builder import SQLBuilder
from .vector_manager import VectorManager
from ..DATA import storage
from ..CONFIG.ai_temperature_config import AI_TEMPERATURE_SETTINGS, get_personality
from ..CONFIG.prompt_manager import load_prompt
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
            cls._instance.ai_model = os.getenv("ROHDEN_AI_MODEL")
            cls._instance.api_key = os.getenv("ROHDEN_AI_KEY")
            
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
        """
        Formata resultados brutos. 
        REGRA: Não deve conter frases completas para o usuário, apenas estrutura de dados.
        """
        if not results:
            return "Nenhum dado encontrado para os critérios informados."

        # Caso de agregacao (ex: COUNT)
        if query_plan.get('aggregations'):
            total = None
            for key in ['TOTAL', 'total', 'Total', 'COUNT', 'count']:
                if key in results[0]:
                    total = results[0][key]
                    break
            if total is not None:
                return f"Resultado: {total}"

        fields = query_plan.get('fields') or list(results[0].keys())
        fields = [f for f in fields if f in results[0]] or list(results[0].keys())

        if len(results) == 1:
            return self._format_single_row(results[0], fields)

        preview = results[:10] # Aumentado preview
        linhas = [self._format_row_line(row, fields) for row in preview]
        linhas = [linha for linha in linhas if linha]
        
        total = len(results)
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

    def _humanize_response(self, prompt: str, results: List[Dict[str, Any]], raw_text: str) -> str:
        """
        Humaniza os dados brutos usando a IA como um juiz inteligente.
        """
        # Se não houver resultados, pedimos à IA para dar uma resposta humana de "não encontrado"
        if not results:
            sys_prompt = load_prompt("humanizer_empty.txt")
            user_prompt = f"Pergunta do usuário: '{prompt}'\nResultado: [Vazio]"
            try:
                return self._call_ai_with_limits(user_prompt, sys_prompt, num_predict=150, num_ctx=1024, temperature=0.5)
            except:
                return raw_text

        # Preparação dos dados para a IA decidir (O Juiz)
        data_json = json.dumps(results[:10], ensure_ascii=False, default=str)
        pers_prompt, temp = get_personality("CHAT")
        
        sys_prompt = load_prompt("humanizer_judge.txt", pers_prompt=pers_prompt)

        user_prompt = (
            f"O que o usuário quer: '{prompt}'\n"
            f"O que eu encontrei no banco: {data_json}\n\n"
            "Analise os dados acima e responda de forma inteligente. Se houver dúvida, pergunte ao usuário."
        )

        try:
            response = self._call_ai_with_limits(user_prompt, sys_prompt, num_predict=400, num_ctx=2048, temperature=temp)
            return response.strip() if response else raw_text
        except Exception:
            return raw_text

    def discover_relevant_tables(self, prompt, all_tables, top_n=3):
        """Usa busca vetorial para encontrar apenas as tabelas TREINADAS e relevantes"""
        if not all_tables:
            return []
            
        # Filtra apenas tabelas que possuem metadados mínimos (consideradas "treinadas")
        trained_tables = [
            t for t in all_tables 
            if t.get('columns_info') and (t.get('table_description') or t.get('semantic_context'))
        ]
        
        if not trained_tables:
            print("⚠️ Nenhuma tabela treinada encontrada no banco de dados.")
            return []

        # Tenta busca vetorial nas tabelas treinadas
        relevant = self.vector_manager.find_most_similar(prompt, trained_tables, top_n=top_n)
        
        if relevant:
            print(f"Busca Vetorial encontrou {len(relevant)} tabelas treinadas relevantes.")
            return relevant
            
        # Fallback: Se vetores falharem, retorna as primeiras N tabelas treinadas
        return trained_tables[:top_n]

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

    def _call_ai_with_limits(self, prompt: str, system_prompt: str, num_predict: int, num_ctx: int = 1024, temperature: float = 0.1, retries: int = 2, stop: List[str] = None) -> str:
        """Chamada direta para a IA com tratamento de erros 500 e failover entre URLs do ambiente."""
        import time
        
        # Stop tokens padrão
        default_stop = ["### Instruction:", "### Response:", "Pergunta do Usuário:", "Resposta do Assistente:"]
        if stop:
            default_stop.extend(stop)
        
        # Coleta todas as URLs configuradas no ambiente (sem hardcode)
        available_urls = []
        if self.ai_url_internal: available_urls.append(self.ai_url_internal)
        if self.ai_url: available_urls.append(self.ai_url)
        
        if not available_urls:
            print("❌ ERRO: Nenhuma URL de IA configurada no ambiente (.env)")
            return ""

        current_num_ctx = num_ctx
        
        for attempt in range(retries):
            # Tenta cada URL disponível
            for target_url in available_urls:
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

                    timeout = 90 # Timeout estendido para 90 segundos
                    print(f"Conectando Rohden AI em: {target_url} (Tentativa {attempt+1}/{retries})")

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
                        res_data = response.json()
                        if is_chat_api:
                            ai_text = res_data.get("message", {}).get("content", "").strip()
                        else:
                            ai_text = res_data.get("response", "").strip()
                        
                        if ai_text:
                            return ai_text
                    
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
                        
                        response = requests.post(alt_url, json=chat_payload, headers=self.rohden_headers, timeout=timeout, verify=False)
                        if response.status_code == 200:
                            res_data = response.json()
                            return res_data.get("message", {}).get("content", "").strip() or res_data.get("response", "").strip()

                    print(f"Erro na Rohden AI ({target_url}): Status {response.status_code}")
                    
                except Exception as e:
                    print(f"Falha na conexão com {target_url}: {str(e)}")
            
            # Se todas as URLs falharam, espera antes do próximo retry
            if attempt < retries - 1:
                time.sleep(2)

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

        # Para perguntas muito curtas (menos de 4 caracteres), exige match exato (score 1.0)
        min_score = 1.0 if len(normalized) < 4 else 0.88
        
        if best_score >= min_score and isinstance(best_plan, dict):
            self._plan_cache[normalized] = best_plan
            return best_plan

        return None

    def _validate_query_plan(self, plan: Dict[str, Any], tables: List[Dict[str, Any]]) -> Tuple[Optional[Dict[str, Any]], Optional[str]]:
        """
        Valida o plano de consulta técnica. 
        IMPORTANTE: Não retorna mensagens para o usuário final aqui.
        Retorna apenas códigos técnicos de erro para que a IA decida como responder.
        """
        if not plan:
            return None, "ERR_PLAN_EMPTY"
        
        plan_type = str(plan.get('type', '')).upper()
        if plan_type == 'NONE':
            return None, "ERR_PLAN_NONE"
        
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
            return None, f"ERR_TABLE_NOT_AUTHORIZED:{table_name}"

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

    def _get_optimized_system_prompt(self, relevant_tables: List[Dict], interpretacao: Dict) -> str:
        """Constrói um system prompt cirúrgico, enviando apenas o necessário para a IA"""
        
        # Base fixa, humana e inteligente
        sys = "Você é o Rohden AI, o assistente inteligente oficial da Rohden. Seu tom de voz é amigável, leve, profissional e muito humano.\n"
        sys += "PROIBIDO: Usar frases robóticas como 'decidi responder', 'com base na análise' ou 'analisando os dados'.\n"
        sys += "PROIBIDO: Mencionar nomes técnicos de tabelas ou colunas (ex: TB_CONTATOS, ID_CONTATO) na resposta final. Use termos naturais (ex: 'contatos', 'código').\n"
        sys += "Vá direto ao ponto: responda de forma natural, como se estivesse conversando com um colega de trabalho.\n"
        sys += "Seja inteligente: varie suas respostas e use emojis de forma equilibrada para manter o tom leve.\n\n"
        
        # Se não houver tabelas relevantes, o assistente apenas conversa
        if not relevant_tables:
            return sys + "Responda de forma prestativa e humana. Você está em modo de conversa geral no momento."

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
        """
        Gera uma resposta baseada na arquitetura contextual:
        1. IA Principal classifica e decide (CHAT ou DATA_ANALYSIS)
        2. Execução da ação decidida pela IA
        
        REGRA DE OURO (ZERO HARDCODE): 
        - É PROIBIDO o uso de strings de resposta hardcoded (fallbacks).
        - Toda e qualquer resposta ao usuário deve vir da IA ou ser formatada com base em dados reais.
        - Em caso de erro técnico, a IA deve ser consultada para explicar o erro ao usuário de forma humana.
        """
        import time
        start_time = time.time()
        
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
            'user_memory_active': bool(user_memory)
        }

        try:
            # 1. CARREGAR TABELAS DISPONÍVEIS
            from ..DATA.storage import storage
            config_tables = storage.load_tables()
            
            # 2. ANÁLISE E DECISÃO PELA IA (Unificada)
            # Passamos o prompt, tabelas, histórico e agora a memória do usuário
            full_context_prompt = f"{user_memory}\n\nPergunta do Usuário: {prompt}" if user_memory else prompt
            ai_analysis = self._unified_ai_analysis(full_context_prompt, config_tables, history=processed_history, username=username)
            
            action = None
            chat_text = None
            query_plan = None
            
            if ai_analysis and isinstance(ai_analysis, dict):
                action = ai_analysis.get('action', '').upper()
                if action == 'CHAT':
                    chat_text = ai_analysis.get('text')
                elif action == 'DATA_ANALYSIS' or action == 'QUERY':
                    action = 'DATA_ANALYSIS'
                    metadata['intencao'] = 'DATA_ANALYSIS'
                    query_plan = ai_analysis.get('plan')

            # 3. FALLBACK PARA TREINAMENTO ESPECÍFICO (Se a IA for incerta sobre o plano de dados)
            if not action or (action == 'DATA_ANALYSIS' and not query_plan):
                trained_plan = self._find_trained_plan(prompt)
                if trained_plan:
                    query_plan = trained_plan
                    action = 'DATA_ANALYSIS'
                    metadata['intencao'] = 'DATA_ANALYSIS'
                elif not action:
                    action = 'CHAT' # Default para chat se nada for decidido, mas o texto virá da IA

            # 4. EXECUÇÃO DA AÇÃO DECIDIDA
            
            # FLUXO: CHAT
            if action == 'CHAT':
                # Se não temos texto da IA ainda, pedimos um agora (garante zero hardcode)
                if not chat_text:
                    chat_text = self._call_ai(full_context_prompt, self._get_optimized_system_prompt([], {}))
                return {'text': chat_text, 'metadata': metadata}
            
            # FLUXO: DATA_ANALYSIS
            if action == 'DATA_ANALYSIS':
                # Validação do plano
                validated_plan, plan_error = self._validate_query_plan(query_plan, config_tables)

                if not validated_plan:
                    # Se o plano falhou, pedimos à IA para explicar o erro técnico (plan_error) de forma humana
                    error_msg = f"Ocorreu um problema técnico ao tentar acessar os dados: {plan_error}. Por favor, explique isso ao usuário de forma amigável e peça mais detalhes se necessário."
                    fallback_text = self._call_ai(error_msg, self._get_optimized_system_prompt([], {}))
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
                    error_msg = f"Erro ao montar SQL: {str(build_err)}. Explique isso ao usuário de forma humana."
                    fallback_text = self._call_ai(error_msg, self._get_optimized_system_prompt([], {}))
                    return {'text': fallback_text, 'metadata': metadata}

                # Persiste para treinamento futuro
                self._store_training(prompt, validated_plan, sql_query, username)

                # Executa no banco
                sql_result = self.execute_sql(sql_query, params=params, dialect=dialect)

                # ESTRATÉGIA DE RETENTATIVA ABRANGENTE (Se retornar vazio)
                if not sql_result and action == 'DATA_ANALYSIS':
                    print(f"⚠️ Busca restritiva retornou 0 resultados. Tentando busca abrangente...")
                    retry_prompt = f"A busca por '{prompt}' não retornou nada. Sugira um novo plano SQL muito mais simples e abrangente (ex: apenas pelo primeiro nome ou apenas pelo sobrenome) para encontrarmos candidatos na tabela."
                    target_table = next((t for t in tables if t.get('table_name').upper() == validated_plan.get('table').upper()), None)
                    if target_table:
                        broader_plan = self._generate_data_plan(retry_prompt, target_table, history, username)
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
                    error_msg = f"O banco de dados retornou um erro: {sql_result}. Explique isso de forma humana."
                    response = self._call_ai(error_msg, self._get_optimized_system_prompt([], {}))
                else:
                    # NOVA LÓGICA: Verifica se há excesso de resultados antes de humanizar normalmente
                    try:
                        from ..CONFIG.ai_temperature_config import handle_excessive_results
                        refinement_question = handle_excessive_results(prompt, sql_result, self)
                        if refinement_question:
                            return {'text': refinement_question, 'metadata': metadata}
                    except Exception as e:
                        print(f"⚠️ Erro ao processar refinamento de resultados: {e}")

                    response = self._format_results(validated_plan, sql_result)
                    # Humanização dos dados (Sempre via IA conforme regra de zero hardcode)
                    try:
                        response = self._humanize_response(prompt, sql_result, response)
                    except Exception:
                        pass # Mantém o response bruto apenas em falha crítica da IA

                # Aprendizado Automático
                if username and prompt:
                    memoria_system.extract_learning_from_interaction(username, prompt, response)
                    
                return {'text': response, 'metadata': metadata}

            # Última instância: se chegar aqui sem ação (raro), pede socorro à IA
            final_fallback = self._call_ai(f"Não consegui processar a pergunta: '{prompt}'. Responda de forma amigável pedindo para eu reformular.", self._get_optimized_system_prompt([], {}))
            return {'text': final_fallback, 'metadata': metadata}

        except Exception as e:
            # Em caso de erro crítico, a IA ainda tenta dar a última palavra
            try:
                critical_error_msg = f"Erro crítico no sistema: {str(e)}. Por favor, peça desculpas ao usuário em nome do Rohden AI e diga que estamos trabalhando nisso."
                ai_apology = self._call_ai(critical_error_msg, "Você é o Rohden AI.")
                return {'text': ai_apology, 'metadata': metadata}
            except:
                # Se até a IA falhar na desculpa, retornamos uma mensagem mínima técnica
                # Mas como o usuário quer zero hardcode, tentamos manter o mais neutro possível
                return {'text': "Não consegui processar sua solicitação agora. Por favor, tente novamente.", 'metadata': metadata}


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

    def _unified_ai_analysis(self, prompt: str, tables: List[Dict[str, Any]], history: List[Dict[str, Any]] = None, username: str = None) -> Optional[Dict[str, Any]]:
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

        # 4. PROMPT DE DECISÃO (Ultra-Agressivo para Dados)
        decision_system_prompt = load_prompt("intent_decision.txt", topics_str=topics_str)

        try:
            # Chamada de Decisão (Mais robusta)
            response = self._call_ai_with_limits(
                prompt, 
                decision_system_prompt + history_context, 
                num_predict=250, 
                num_ctx=2048,
                temperature=0.0
            )
            print(f"🔍 DECISÃO DA IA (INTENÇÃO): '{response}'")
            
            decision = self._extract_json(response)
            if not decision:
                # Se a IA falhou no JSON, tentamos uma última vez com um prompt ainda mais agressivo e sem contexto de histórico
                # para não confundir o modelo pequeno.
                retry_prompt = (
                    "Responda APENAS com este JSON preenchido:\n"
                    "{\"action\": \"CHAT\", \"text\": \"Sua resposta aqui\"}\n"
                    f"Pergunta do usuário: {prompt}"
                )
                response_retry = self._call_ai_with_limits(prompt, retry_prompt, num_predict=100, num_ctx=1024)
                decision = self._extract_json(response_retry)
                
                if not decision:
                    # Se mesmo assim falhar, retornamos o texto bruto da segunda tentativa limpo
                    return {"action": "CHAT", "text": response_retry.split('{')[0].strip()}
            
            action = decision.get('action', 'CHAT').upper()
            
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
                        response = self._call_ai_with_limits(prompt, "Explique que não temos dados sobre isso no momento.", num_predict=150, num_ctx=1024)
                        return {"action": "CHAT", "text": response}
                    target_table = relevant[0]

                # Agora sim fazemos o "heavy lifting" apenas para a tabela selecionada
                return self._generate_data_plan(prompt, target_table, history, username)

        except Exception as e:
            print(f"❌ ERRO na análise unificada: {e}")
            return None

    def _generate_data_plan(self, prompt: str, table: Dict[str, Any], history: List[Dict[str, Any]], username: str) -> Optional[Dict[str, Any]]:
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

        plan_system_prompt = load_prompt(
            "sql_plan_generation.txt",
            catalog=json.dumps(catalog, ensure_ascii=False),
            history_context=history_context,
            trained_examples=trained_examples
        )
        
        response = self._call_ai_with_limits(prompt, plan_system_prompt, num_predict=500, num_ctx=2048, temperature=0.0)
        print(f"🔍 PLANO DE DADOS GERADO: '{response}'")
        return self._extract_json(response)

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

    def _call_ai(self, prompt, system_prompt):
        """
        Encapsula a chamada à IA com foco exclusivo nos servidores Rohden AI.
        Usa limites seguros para estabilidade do servidor GGUF.
        """
        return self._call_ai_with_limits(prompt, system_prompt, num_predict=800, num_ctx=2048, temperature=0.1)

def get_llama_engine():
    return LlamaEngine()
