"""
INTERPRETER - Sistema Inteligente de Interpretação de Linguagem Natural
Camadas de processamento para entender perguntas do usuário
"""

import re
import json
import unicodedata
import os
import sqlite3
from datetime import datetime, timedelta
from typing import Dict, List, Tuple, Optional, Union
from collections import defaultdict, Counter

from ..DATA.storage import DataStorage

class QueryInterpreter:
    """Sistema multicamadas para interpretação inteligente de perguntas"""
    
    def __init__(self):
        self.storage = DataStorage()
        self.dicionario_empresarial = self._carregar_dicionario()
        self.mapeamento_semantico = self._carregar_mapeamento()
        self.contexto_conversacional = {}
        self.scores_confianca = {}
        self.db_path = self.storage.db_path
        self.metricas_padrao = ['venda', 'faturamento', 'quantidade', 'valor', 'lucro', 'estoque', 'total', 'soma', 'media', 'maximo', 'minimo', 'contagem', 'count']
        self.dimensoes_padrao = ['cliente', 'contato', 'produto', 'item', 'estado', 'cidade', 'regiao', 'vendedor', 'setor', 'categoria', 'grupo']
        
    def _carregar_dicionario(self):
        return {
            'vendas': ['faturamento', 'receita', 'vendido'],
            'clientes': ['contatos', 'compradores', 'parceiros'],
            'produtos': ['itens', 'mercadorias', 'materiais']
        }
        
    def _carregar_mapeamento(self):
        """Carrega mapeamento de tabelas e campos a partir do storage (DB)"""
        mapeamento = {}
        try:
            tables = self.storage.load_tables()
            for table in tables:
                table_name = table['table_name']
                schema_info = table.get('schema_info') or {}
                if isinstance(schema_info, str):
                    try:
                        schema_info = json.loads(schema_info)
                    except Exception:
                        schema_info = {}
                schema = schema_info.get('schema', 'SYSROH')
                
                # Definir aliases baseados no nome e descrição
                aliases = [table_name.lower()]
                if 'CONTATOS' in table_name: aliases.extend(['contato', 'cliente', 'pessoa', 'leads'])
                if 'PRODUTOS' in table_name: aliases.extend(['produto', 'item', 'mercadoria', 'estoque'])
                if 'VENDAS' in table_name: aliases.extend(['venda', 'pedido', 'faturamento', 'comercial'])
                
                # Extrair colunas e seus tipos/comentários
                campos = {}
                for col in table.get('columns_info', []):
                    col_name = col['name'].upper()
                    # Mapear semanticamente colunas comuns
                    if any(x in col_name for x in ['NOME', 'RAZAO', 'NOME_CONTATO']): campos['nome'] = col_name
                    if any(x in col_name for x in ['EMAIL', 'E_MAIL']): campos['email'] = col_name
                    if any(x in col_name for x in ['CELULAR', 'FONE', 'TELEFONE', 'WHATS']): campos['celular'] = col_name
                    if any(x in col_name for x in ['ESTADO', 'UF', 'SIGLA_UF']): campos['estado'] = col_name
                    if any(x in col_name for x in ['CIDADE', 'MUNICIPIO']): campos['cidade'] = col_name
                    if any(x in col_name for x in ['ID', 'CD_', 'CODIGO']): campos['id'] = col_name
                
                mapeamento[table_name] = {
                    'aliases': list(set(aliases)),
                    'campos': campos,
                    'description': table.get('table_description', ''),
                    'schema': schema
                }
        except Exception as e:
            print(f"Erro ao carregar mapeamento do DB: {e}")
            # Fallback para o básico se o DB falhar
            mapeamento = {
                'TB_CONTATOS': {'aliases': ['cliente', 'contato', 'pessoa'], 'campos': {'nome': 'NOME', 'email': 'EMAIL', 'celular': 'CELULAR'}, 'schema': 'SYSROH'},
                'TB_PRODUTOS': {'aliases': ['produto', 'item', 'estoque'], 'campos': {'nome': 'NOME'}, 'schema': 'SYSROH'},
                'TB_VENDAS': {'aliases': ['venda', 'pedido', 'faturamento'], 'campos': {'valor': 'VALOR_TOTAL'}, 'schema': 'SYSROH'}
            }
        return mapeamento

    def _levenshtein(self, s1: str, s2: str) -> int:
        """Calcula a distância de Levenshtein entre duas strings"""
        if len(s1) < len(s2):
            return self._levenshtein(s2, s1)

        if len(s2) == 0:
            return len(s1)

        previous_row = range(len(s2) + 1)
        for i, c1 in enumerate(s1):
            current_row = [i + 1]
            for j, c2 in enumerate(s2):
                insertions = previous_row[j + 1] + 1
                deletions = current_row[j] + 1
                substitutions = previous_row[j] + (c1 != c2)
                current_row.append(min(insertions, deletions, substitutions))
            previous_row = current_row

        return previous_row[-1]

    def _buscar_aprendizado(self, termo: str) -> Optional[str]:
        """Busca no banco de dados se já aprendemos esse erro de digitação"""
        try:
            conn = sqlite3.connect(self.db_path)
            cursor = conn.cursor()
            
            # Buscar na base de conhecimento (categoria LEARNING_TYPOS)
            cursor.execute('''
                SELECT content FROM knowledge_base 
                WHERE category = 'LEARNING_TYPOS' AND UPPER(title) = UPPER(?)
                AND is_active = 1
            ''', (termo,))
            
            result = cursor.fetchone()
            conn.close()
            
            if result:
                return result[0]
        except Exception as e:
            print(f"Erro ao buscar aprendizado: {e}")
        
        return None

    def _gerar_query_plan(self, intencao: Dict, entidades: Dict, temporal: Dict, pergunta: str = "") -> Optional[Dict]:
        """Gera um plano semântico determinístico para o SQL Builder"""
        if not entidades.get('tabelas'):
            return None

        tabela = entidades['tabelas'][0]
        mapeamento = self._carregar_mapeamento()
        campos = mapeamento.get(tabela, {}).get('campos', {})
        schema = mapeamento.get(tabela, {}).get('schema', 'SYSROH')
        pergunta_lower = pergunta.lower()

        plan = {
            'type': 'SELECT',
            'table': tabela,
            'schema': schema,
            'dialect': 'oracle',
            'fields': [],
            'filters': [],
            'limit': None
        }

        def add_field(field_name: str, target: List[str]):
            if field_name and field_name not in target:
                target.append(field_name)

        # Campos padrão para exibição
        default_fields = []
        for key in ['nome', 'email', 'celular', 'estado', 'cidade']:
            add_field(campos.get(key), default_fields)

        if intencao['tipo'] == 'BUSCAR_QUANTIDADE':
            plan['aggregations'] = [{'func': 'COUNT', 'field': '*', 'as': 'TOTAL'}]
            plan['limit'] = 1
            return plan

        if intencao['tipo'] == 'LISTAR_TUDO':
            plan['fields'] = entidades.get('campos') or default_fields or ['*']
            plan['limit'] = 100
            return plan

        if intencao['tipo'] == 'BUSCAR_DISTRIBUICAO':
            col_dist = None
            for p in ['estado', 'uf', 'cidade', 'regiao', 'ddd', 'categoria', 'grupo', 'vendedor']:
                if p in pergunta_lower:
                    col_dist = p
                    break

            col_real = campos.get(col_dist) if col_dist else None
            if not col_real:
                col_real = campos.get('estado', campos.get('uf', campos.get('cidade')))

            if col_real:
                plan['fields'] = [col_real]
                plan['aggregations'] = [{'func': 'COUNT', 'field': '*', 'as': 'TOTAL'}]
                plan['group_by'] = col_real
                plan['order_by'] = [{'field': 'TOTAL', 'direction': 'DESC'}]
                return plan

        if intencao['tipo'] == 'BUSCAR_ESPECIFICO':
            plan['fields'] = entidades.get('campos') or default_fields or ['*']
            plan['filters'] = self._build_filter_plan(entidades.get('filtros', []), campos, tabela)
            plan['limit'] = 5
            return plan

        # Fallback: se tiver filtros e tabela, tenta um SELECT básico
        if entidades.get('filtros'):
            plan['fields'] = entidades.get('campos') or default_fields or ['*']
            plan['filters'] = self._build_filter_plan(entidades.get('filtros', []), campos, tabela)
            plan['limit'] = 50
            return plan

        return None

    def _build_filter_plan(self, filtros: List[Dict], campos_tabela: Dict, tabela: str) -> List[Dict]:
        """Converte filtros extraídos em plano semântico validável"""
        filtros_plan = []

        def pick_field(preferred_key: str, fallback: str) -> Optional[str]:
            return campos_tabela.get(preferred_key) or fallback

        for filtro in filtros:
            valor = filtro.get('valor')
            if not valor:
                continue

            if filtro['tipo'] == 'numerico':
                campo_id = 'ID'
                if tabela == 'TB_CONTATOS':
                    campo_id = 'ID_CONTATO'
                elif tabela == 'TB_PRODUTOS':
                    campo_id = 'ID_PRODUTO'
                elif tabela == 'TB_VENDAS':
                    campo_id = 'ID_VENDA'
                elif campos_tabela.get('id'):
                    campo_id = campos_tabela['id']

                filtros_plan.append({
                    'field': campo_id,
                    'op': '=',
                    'value': valor
                })
                continue

            if filtro['tipo'] == 'texto':
                valor_str = str(valor)
                if '@' in valor_str:
                    campo = pick_field('email', 'EMAIL')
                    filtros_plan.append({
                        'field': campo,
                        'op': '=',
                        'value': valor_str,
                        'case_insensitive': True
                    })
                    continue

                digits = re.sub(r'\D', '', valor_str)
                if digits.isdigit() and len(digits) >= 8:
                    campo = pick_field('celular', 'CELULAR')
                    filtros_plan.append({
                        'field': campo,
                        'op': 'LIKE',
                        'value': f"%{digits}%",
                        'normalize': 'digits'
                    })
                    continue

                campo = pick_field('nome', 'NOME')
                filtros_plan.append({
                    'field': campo,
                    'op': 'LIKE',
                    'value': f"%{valor_str}%",
                    'case_insensitive': True
                })

        return filtros_plan

    def _aprender_termo(self, erro: str, correto: str):
        """Registra um novo aprendizado de termo"""
        if not erro or not correto or erro.lower() == correto.lower():
            return
            
        try:
            conn = sqlite3.connect(self.db_path)
            cursor = conn.cursor()
            
            # Verificar se já existe
            cursor.execute('''
                SELECT id FROM knowledge_base 
                WHERE category = 'LEARNING_TYPOS' AND UPPER(title) = UPPER(?)
            ''', (erro,))
            
            if cursor.fetchone():
                cursor.execute('''
                    UPDATE knowledge_base SET content = ?, updated_at = CURRENT_TIMESTAMP
                    WHERE category = 'LEARNING_TYPOS' AND UPPER(title) = UPPER(?)
                ''', (correto, erro))
            else:
                cursor.execute('''
                    INSERT INTO knowledge_base (category, title, content, priority)
                    VALUES ('LEARNING_TYPOS', ?, ?, 1)
                ''', (erro, correto))
                
            conn.commit()
            conn.close()
        except Exception as e:
            print(f"Erro ao salvar aprendizado: {e}")

    def interpretar_pergunta(self, pergunta: str, usuario: str = None, historico: List = None) -> Dict:
        """
        Processa a pergunta através das 4 camadas principais
        Retorna dicionário com interpretação completa
        """
        # ETAPA 1: Pré-Processamento
        pergunta_limpa = self._pre_processar(pergunta)
        
        # ETAPA 2: Análise de Intenção
        intencao = self._analisar_intencao(pergunta_limpa)
        
        # ETAPA 3: Extração de Entidades
        entidades = self._extrair_entidades(pergunta_limpa)
        
        # ETAPA 4: Processamento Temporal
        temporal = self._processar_temporal(pergunta_limpa)
        
        # ETAPA 5: Resolução e Validação
        interpretacao = self._resolver_validar(pergunta_limpa, intencao, entidades, temporal, usuario, historico)
        
        return interpretacao
    
    # ========================================
    # CAMADA 1: PRÉ-PROCESSAMENTO
    # ========================================
    
    def _pre_processar(self, pergunta: str) -> str:
        """Limpa e padroniza a entrada"""
        # 1. Correção automática de erros comuns
        pergunta = self._corrigir_erros_comuns(pergunta)
        
        # 2. Normalização
        pergunta = self._normalizar_texto(pergunta)
        
        # 3. Expansão de abreviações
        pergunta = self._expandir_abreviacoes(pergunta)
        
        # 4. Tokenização inteligente
        tokens = self._tokenizar_inteligente(pergunta)
        
        return " ".join(tokens)
    
    def _corrigir_erros_comuns(self, texto: str) -> str:
        """Correção automática de erros de digitação e aprendizado"""
        # 1. Verificar se a frase inteira ou partes dela foram aprendidas
        # (Isso ajuda com nomes compostos como "rudieri mchado")
        texto_corrigido = texto
        try:
            conn = sqlite3.connect(self.db_path)
            cursor = conn.cursor()
            cursor.execute("SELECT title, content FROM knowledge_base WHERE category = 'LEARNING_TYPOS' AND is_active = 1")
            aprendizados = cursor.fetchall()
            conn.close()
            
            for erro, correto in aprendizados:
                # Substituir com regex para garantir que seja palavra inteira ou frase exata
                pattern = re.compile(re.escape(erro), re.IGNORECASE)
                texto_corrigido = pattern.sub(correto, texto_corrigido)
        except Exception as e:
            print(f"Erro ao aplicar aprendizados: {e}")

        # 2. Correções fixas por palavra
        correcoes = {
            'vendaz': 'vendas',
            'clinte': 'cliente',
            'clintes': 'clientes',
            'prodto': 'produto',
            'produtos': 'produto',
            'qtd': 'quantidade',
            'vlr': 'valor',
            'pdt': 'produto',
            'qnts': 'quantos',
            'qntas': 'quantas',
            'mostr': 'mostra',
            'mostrr': 'mostra',
            'ateh': 'até',
            'exibir': 'mostrar',
            'exiba': 'mostrar',
            'liste': 'listar',
            'lista': 'listar',
        }
        
        palavras = texto_corrigido.split()
        corrigidas = []
        
        for palavra in palavras:
            palavra_lower = palavra.lower()
            if palavra_lower in correcoes:
                corrigidas.append(correcoes[palavra_lower])
            else:
                corrigidas.append(palavra)
        
        return " ".join(corrigidas)
    
    def _normalizar_texto(self, texto: str) -> str:
        """Remove acentos, pontuação desnecessária e padroniza"""
        # Remove acentos
        texto = unicodedata.normalize('NFKD', texto)
        texto = ''.join(c for c in texto if not unicodedata.combining(c))
        
        # Converte para minúsculo
        texto = texto.lower()
        
        # Remove pontuação excessiva mas mantém estrutura
        texto = re.sub(r'[^\w\s\?\!\%\,\.\-\+\/\:\(\)]', ' ', texto)
        
        # Remove espaços extras
        texto = re.sub(r'\s+', ' ', texto).strip()
        
        return texto
    
    def _expandir_abreviacoes(self, texto: str) -> str:
        """Expande abreviações comuns"""
        abreviacoes = {
            'qtd': 'quantidade',
            'vlr': 'valor',
            'pdt': 'produto',
            'cli': 'cliente',
            'vnd': 'vendas',
            'lcr': 'lucro',
            'mrg': 'margem',
            'estq': 'estoque',
            'ped': 'pedido',
            'orc': 'orcamento',
            'r$:': 'real',
            'us$': 'dolar',
            'rs': 'real',
            'km': 'quilometro',
            'kg': 'quilograma',
            'un': 'unidade',
            'pc': 'percentual',
            '%': 'percentual',
        }
        
        palavras = texto.split()
        expandidas = []
        
        for palavra in palavras:
            if palavra in abreviacoes:
                expandidas.append(abreviacoes[palavra])
            else:
                expandidas.append(palavra)
        
        return ' '.join(expandidas)
    
    def _tokenizar_inteligente(self, texto: str) -> List[str]:
        """Quebra a frase em partes significativas"""
        # Preserva expressões importantes
        expressoes = {
            'acima de': 'acima_de',
            'abaixo de': 'abaixo_de',
            'menor que': 'menor_que',
            'maior que': 'maior_que',
            'diferente de': 'diferente_de',
            'igual a': 'igual_a',
            'ultimo mes': 'ultimo_mes',
            'mes passado': 'mes_passado',
            'ano passado': 'ano_passado',
            'semana passada': 'semana_passada',
            'primeiro trimestre': 'primeiro_trimestre',
            'segundo trimestre': 'segundo_trimestre',
            'terceiro trimestre': 'terceiro_trimestre',
            'quarto trimestre': 'quarto_trimestre',
        }
        
        for expressao, token in expressoes.items():
            texto = texto.replace(expressao, token)
        
        return texto.split()
    
    # ========================================
    # CAMADA 2: ANÁLISE DE INTENÇÃO
    # ========================================
    
    def _analisar_intencao(self, pergunta: str) -> Dict:
        """Classifica o tipo de pergunta"""
        intencao = {
            'tipo': 'desconhecida',
            'confianca': 0.0,
            'subtipos': [],
            'acoes': []
        }
        
        # Padrões de intenção
        padroes = {
            'BUSCAR_DISTRIBUICAO': [
                r'distribuicao\s+(de|do|da|dos|das|geografica)\s+(.+?)',
                r'qual\s+(a|o)\s+distribuicao\s+(.+?)',
                r'onde\s+(estao|se\s+localizam)\s+(.+?)',
                r'como\s+estao?\s+divididos?\s+(.+?)',
                r'agrupar\s+(.+?)\s+por\s+(.+?)',
                r'quebra\s+de\s+(.+?)\s+por\s+(.+?)',
                r'perfil\s+(de|do|da|dos|das)\s+(.+?)',
                r'geografia\s+(de|do|da|dos|das)\s+(.+?)',
            ],
            'BUSCAR_QUANTIDADE': [
                r'quantos?\s+(.+?)\s+(tem|temos|existem)',
                r'quantas?\s+(.+?)\s+(tem|temos|existem)',
                r'numero\s+de\s+(.+?)',
                r'total\s+de\s+(.+?)',
                r'contar\s+(.+?)',
                r'count\s+(.+?)',
            ],
            'LISTAR_TUDO': [
                r'mostrar\s+tod[ao]s?\s+(.+?)',
                r'listar\s+tod[ao]s?\s+(.+?)',
                r'exibir\s+tod[ao]s?\s+(.+?)',
                r'tod[ao]s?\s+(.+?)',
                r'todos?\s+(.+?)',
            ],
            'COMPARAR_PERIODOS': [
                r'(.+?)\s+(vs|versus|contra)\s+(.+?)',
                r'comparar\s+(.+?)\s+com\s+(.+?)',
                r'diferenca\s+entre\s+(.+?)\s+e\s+(.+?)',
                r'(.+?)\s+(maior|menor)\s+que\s+(.+?)',
            ],
            'PREVER_TENDENCIA': [
                r'tendencia\s+de\s+(.+?)',
                r'prever\s+(.+?)',
                r'projecao\s+de\s+(.+?)',
                r'estimativa\s+de\s+(.+?)',
                r'qual\s+sera\s+(.+?)',
            ],
            'ANALISAR_CAUSA': [
                r'por\s+que\s+(.+?)',
                r'motivo\s+do\s+(.+?)',
                r'causa\s+do\s+(.+?)',
                r'razao\s+do\s+(.+?)',
                r'explicar\s+(.+?)',
            ],
            'GERAR_RELATORIO': [
                r'relatorio\s+de\s+(.+?)',
                r'resumo\s+de\s+(.+?)',
                r'resumo\s+de\s+(.+?)',
                r'consolidado\s+de\s+(.+?)',
                r'dashboard\s+de\s+(.+?)',
            ],
            'BUSCAR_ESPECIFICO': [
                r'buscar\s+(.+?)\s+(.+?)',
                r'encontrar\s+(.+?)\s+(.+?)',
                r'localizar\s+(.+?)\s+(.+?)',
                r'quem\s+e\s+(.+?)',
                r'qual\s+e\s+(.+?)',
                r'contato\s+de\s+(.+?)',
                r'telefone\s+de\s+(.+?)',
                r'celular\s+de\s+(.+?)',
                r'email\s+de\s+(.+?)',
                r'whats\s+de\s+(.+?)',
                r'whatsapp\s+de\s+(.+?)',
                r'quem\s+e\s+o\s+cliente\s+(.+?)',
                r'dados\s+do\s+cliente\s+(.+?)',
                r'info\s+de\s+(.+?)',
            ],
            'FILTRAR_CONDICOES': [
                r'(.+?)\s+(acima|abaixo|maior|menor)\s+(.+?)',
                r'(.+?)\s+entre\s+(.+?)\s+e\s+(.+?)',
                r'(.+?)\s+com\s+(.+?)',
                r'(.+?)\s+sem\s+(.+?)',
            ]
        }
        
        # Verificar cada padrão
        max_confianca = 0
        tipo_detectado = 'desconhecida'
        
        for tipo, regex_list in padroes.items():
            for regex in regex_list:
                if re.search(regex, pergunta, re.IGNORECASE):
                    # Confiança baseada na especificidade do padrão
                    confianca = 0.8 if len(regex) > 15 else 0.6
                    if confianca > max_confianca:
                        max_confianca = confianca
                        tipo_detectado = tipo
        
        intencao['tipo'] = tipo_detectado
        intencao['confianca'] = max_confianca
        
        return intencao
    
    # ========================================
    # CAMADA 3: EXTRAÇÃO DE ENTIDADES
    # ========================================
    
    def _extrair_entidades(self, pergunta: str) -> Dict:
        """Identifica os elementos-chave da pergunta"""
        entidades = {
            'metricas': [],
            'dimensoes': [],
            'filtros': [],
            'comparadores': [],
            'agregacoes': [],
            'tabelas': [],
            'campos': [],
            'setores': []
        }
        
        # Carregar sinônimos e setores do dicionário
        sinonimos = self.dicionario_empresarial.get('sinonimos', {})
        setores_map = self.dicionario_empresarial.get('setores', {})
        
        # Mapeamento reverso para identificar a "entidade raiz" a partir do sinônimo
        map_reverso = {}
        for raiz, lista_sinonimos in sinonimos.items():
            for s in lista_sinonimos:
                map_reverso[s] = raiz
        
        # Métricas e Dimensões (unificadas para busca)
        palavras_chave = list(map_reverso.keys())
        
        # Comparadores
        comparadores = [
            'maior', 'menor', 'melhor', 'pior', 'diferente', 'igual', 'acima', 'abaixo'
        ]
        
        # Agregações
        agregacoes = [
            'total', 'soma', 'media', 'maximo', 'minimo', 'contagem', 'count'
        ]
        
        palavras = pergunta.split()
        
        for palavra in palavras:
            palavra_limpa = self._normalizar_texto(palavra)
            
            # 1. Verificar se é um sinônimo conhecido
            if palavra_limpa in map_reverso:
                entidade_raiz = map_reverso[palavra_limpa]
                if entidade_raiz in ['venda', 'faturamento', 'quantidade', 'valor', 'lucro', 'estoque']:
                    entidades['metricas'].append(entidade_raiz)
                else:
                    entidades['dimensoes'].append(entidade_raiz)
            
            # 2. Verificar se está nas listas padrão (caso não esteja no dicionário)
            elif palavra_limpa in self.metricas_padrao:
                entidades['metricas'].append(palavra_limpa)
            elif palavra_limpa in self.dimensoes_padrao:
                entidades['dimensoes'].append(palavra_limpa)
            
            # 3. Verificar se é um setor
            for setor, termos in setores_map.items():
                if palavra_limpa in termos:
                    entidades['setores'].append(setor)
            
            if palavra_limpa in comparadores:
                entidades['comparadores'].append(palavra_limpa)
            elif palavra_limpa in agregacoes:
                entidades['agregacoes'].append(palavra_limpa)
        
        # Extrair filtros numéricos
        numeros = re.findall(r'\d+', pergunta)
        for numero in numeros:
            entidades['filtros'].append({'tipo': 'numerico', 'valor': int(numero)})
        
        # Extrair filtros de texto (entre aspas ou palavras capitalizadas que não são palavras reservadas)
        textos_aspas = re.findall(r'"([^"]+)"', pergunta)
        for texto in textos_aspas:
            entidades['filtros'].append({'tipo': 'texto', 'valor': texto})
        
        # Tentar identificar nomes próprios (palavras capitalizadas) se não houver aspas
        if not textos_aspas:
            # Pegar palavras que não são métricas/dimensões conhecidas
            potenciais_valores = []
            palavras_originais = pergunta.split()
            for i, p in enumerate(palavras_originais):
                p_limpa = self._normalizar_texto(p)
                # Se a palavra não é uma métrica, dimensão, comparador ou agregação
                if p_limpa not in self.metricas_padrao and \
                   p_limpa not in self.dimensoes_padrao and \
                   p_limpa not in comparadores and \
                   p_limpa not in agregacoes and \
                   p_limpa not in ['qual', 'quem', 'como', 'onde', 'quando', 'quando', 'quanto', 'quantos', 'mostra', 'lista', 'busca', 'encontrar', 'localizar']:
                    
                    if len(p_limpa) > 1: # Evitar letras soltas (exceto se for parte de um nome)
                        potenciais_valores.append(p)
            
            if potenciais_valores:
                # Se temos palavras sequenciais que parecem um nome
                valor_completo = " ".join(potenciais_valores)
                entidades['filtros'].append({'tipo': 'texto', 'valor': valor_completo})
                
                # Aprendizado automático: se o valor completo for muito próximo de algo conhecido
                # (Isso seria feito após a execução do SQL no engine, mas aqui podemos preparar)
        
        # Mapear para tabelas/campos
        entidades['tabelas'] = self._mapear_para_tabelas(entidades)
        entidades['campos'] = self._mapear_para_campos(entidades)
        
        return entidades
    
    def _mapear_para_tabelas(self, entidades: Dict) -> List[str]:
        """Mapeia entidades para tabelas do banco"""
        mapeamento = self._carregar_mapeamento()
        tabelas_encontradas = []
        
        # Verificar aliases de tabelas
        todas_entidades = entidades['metricas'] + entidades['dimensoes'] + entidades['setores']
        
        for tabela, config in mapeamento.items():
            aliases = config.get('aliases', [])
            for entidade in todas_entidades:
                if entidade in aliases:
                    tabelas_encontradas.append(tabela)
        
        return list(set(tabelas_encontradas))
    
    def _mapear_para_campos(self, entidades: Dict) -> List[str]:
        """Mapeia entidades para campos das tabelas"""
        mapeamento = self._carregar_mapeamento()
        campos_encontrados = []
        
        tabelas_alvo = entidades.get('tabelas', [])
        todas_entidades = entidades['metricas'] + entidades['dimensoes']
        
        for tabela in tabelas_alvo:
            campos_config = mapeamento.get(tabela, {}).get('campos', {})
            for entidade in todas_entidades:
                if entidade in campos_config:
                    campos_encontrados.append(campos_config[entidade])
        
        return list(set(campos_encontrados))
    
    # ========================================
    # CAMADA 4: PROCESSAMENTO TEMPORAL
    # ========================================
    
    def _processar_temporal(self, pergunta: str) -> Dict:
        """Interpreta referências de tempo"""
        temporal = {
            'tipo': 'desconhecido',
            'periodo': None,
            'data_inicio': None,
            'data_fim': None,
            'expressoes': []
        }
        
        hoje = datetime.now()
        
        # Padrões temporais
        padroes_temporais = {
            'hoje': (hoje.date(), hoje.date()),
            'ontem': (hoje.date() - timedelta(days=1), hoje.date() - timedelta(days=1)),
            'semana passada': (
                hoje.date() - timedelta(days=hoje.weekday() + 7),
                hoje.date() - timedelta(days=hoje.weekday() + 1)
            ),
            'mes passado': (
                (hoje.replace(day=1) - timedelta(days=1)).replace(day=1),
                hoje.replace(day=1) - timedelta(days=1)
            ),
            'ano passado': (
                hoje.replace(year=hoje.year - 1, month=1, day=1),
                hoje.replace(year=hoje.year - 1, month=12, day=31)
            ),
            'ultimo mes': (
                hoje.replace(day=1) - timedelta(days=1),
                hoje.date() - timedelta(days=1)
            ),
            'ultimos 30 dias': (
                hoje.date() - timedelta(days=30),
                hoje.date()
            ),
        }
        
        # Detectar expressões temporais
        for expressao, (inicio, fim) in padroes_temporais.items():
            if expressao in pergunta:
                temporal['expressoes'].append(expressao)
                temporal['data_inicio'] = inicio
                temporal['data_fim'] = fim
                temporal['tipo'] = 'periodo_relativo'
                break
        
        # Detectar datas específicas
        datas = re.findall(r'(\d{2}/\d{2}/\d{4})', pergunta)
        if datas:
            try:
                data = datetime.strptime(datas[0], '%d/%m/%Y').date()
                temporal['data_inicio'] = data
                temporal['data_fim'] = data
                temporal['tipo'] = 'data_especifica'
            except:
                pass
        
        return temporal
    
    # ========================================
    # CAMADA 5: RESOLUÇÃO E VALIDAÇÃO
    # ========================================
    
    def _resolver_validar(self, pergunta: str, intencao: Dict, entidades: Dict, 
                          temporal: Dict, usuario: str = None, historico: List = None) -> Dict:
        """Resolve ambiguidades e valida interpretação"""
        
        # 1. Resolver tabelas a partir do histórico se estiver vazio
        if not entidades.get('tabelas') and historico:
            for msg in reversed(historico):
                # Verificar se a mensagem tem metadados com tabelas_usadas
                metadata = msg.get('metadata', {})
                if isinstance(metadata, str):
                    try: metadata = json.loads(metadata)
                    except: metadata = {}
                
                tabelas_usadas = metadata.get('tabelas_usadas', [])
                if tabelas_usadas:
                    entidades['tabelas'] = tabelas_usadas
                    print(f"Resolvido tabelas do contexto histórico: {tabelas_usadas}")
                    break
        
        # 2. Se ainda estiver vazio, tentar inferir pelo contexto da pergunta
        if not entidades.get('tabelas'):
            # Busca simples por palavras-chave se o mapeador falhou
            mapeamento = self._carregar_mapeamento()
            for tabela, config in mapeamento.items():
                for alias in config.get('aliases', []):
                    if alias in pergunta.lower():
                        entidades['tabelas'] = [tabela]
                        break
                if entidades.get('tabelas'): break

        interpretacao = {
            'pergunta_original': pergunta,
            'pergunta_processada': pergunta,
            'intencao': intencao,
            'intent': intencao.get('tipo'),
            'entidades': entidades,
            'temporal': temporal,
            'sql_sugerido': None,
            'query_plan': None,
            'confianca_geral': 0.0,
            'ambiguidades': [],
            'sugestoes': []
        }
        
        # Calcular confiança geral
        confianca_intencao = intencao['confianca']
        confianca_entidades = 0.9 if entidades.get('tabelas') else 0.2
        confianca_temporal = 0.8 if temporal['tipo'] != 'desconhecido' else 0.5
        
        # Peso maior para intenção e entidades
        interpretacao['confianca_geral'] = (confianca_intencao * 0.4) + (confianca_entidades * 0.4) + (confianca_temporal * 0.2)
        
        # Gerar SQL sugerido
        interpretacao['sql_sugerido'] = self._gerar_sql_sugerido(intencao, entidades, temporal, pergunta)

        # Gerar plano semântico (fonte da verdade)
        interpretacao['query_plan'] = self._gerar_query_plan(intencao, entidades, temporal, pergunta)
        
        # Detectar ambiguidades
        interpretacao['ambiguidades'] = self._detectar_ambiguidades(entidades, temporal)
        
        # Gerar sugestões
        interpretacao['sugestoes'] = self._gerar_sugestoes(intencao, entidades, temporal)
        
        return interpretacao
    
    def _gerar_sql_sugerido(self, intencao: Dict, entidades: Dict, temporal: Dict, pergunta: str = "") -> Optional[str]:
        """Gera SQL baseado na interpretação"""
        
        if not entidades['tabelas']:
            return None
        
        tabela = entidades['tabelas'][0]  # Usar primeira tabela
        mapeamento = self._carregar_mapeamento()
        schema = mapeamento.get(tabela, {}).get('schema', 'SYSROH')
        table_ref = f"{schema}.{tabela}"
        pergunta_lower = pergunta.lower()
        
        # SELECT base
        if intencao['tipo'] == 'BUSCAR_QUANTIDADE':
            return f"SELECT COUNT(*) AS total FROM {table_ref}"
        
        elif intencao['tipo'] == 'LISTAR_TUDO':
            campos = '*'
            if entidades['campos']:
                campos = ', '.join(entidades['campos'][:3])  # Limitar a 3 campos
            return f"SELECT {campos} FROM {table_ref} FETCH FIRST 100 ROWS ONLY"
        
        elif intencao['tipo'] == 'BUSCAR_ESPECIFICO':
            campos = ', '.join(entidades['campos'][:3]) if entidades['campos'] else '*'
            where_clause = self._construir_where(entidades['filtros'], tabela)
            sql = f"SELECT {campos} FROM {table_ref}"
            if where_clause:
                sql += f" WHERE {where_clause}"
            sql += " FETCH FIRST 100 ROWS ONLY"
            return sql
        
        elif intencao['tipo'] == 'BUSCAR_DISTRIBUICAO':
            # Tentar identificar a coluna de distribuição (ex: "por estado", "por cidade")
            col_dist = None
            for p in ['estado', 'uf', 'cidade', 'regiao', 'ddd', 'categoria', 'grupo', 'vendedor']:
                if p in pergunta_lower:
                    col_dist = p
                    break
            
            mapeamento = self._carregar_mapeamento()
            campos = mapeamento.get(tabela, {}).get('campos', {})
            
            # Se a tabela for TB_CONTATOS e não especificou coluna, usa CELULAR para o engine fazer o de-para de DDD
            if tabela == 'TB_CONTATOS' and not col_dist:
                return f"SELECT CELULAR FROM {schema}.TB_CONTATOS WHERE CELULAR IS NOT NULL"
            
            # Tentar mapear a coluna solicitada
            col_real = campos.get(col_dist) if col_dist else None
            
            # Se não encontrou coluna específica, tenta fallbacks geográficos no mapeamento
            if not col_real:
                col_real = campos.get('estado', campos.get('uf', campos.get('cidade', None)))
            
            # Se ainda não encontrou, e for TB_CONTATOS, volta pro CELULAR
            if not col_real and tabela == 'TB_CONTATOS':
                return f"SELECT CELULAR FROM {schema}.TB_CONTATOS WHERE CELULAR IS NOT NULL"
            
            if col_real:
                return f"SELECT {col_real}, COUNT(*) as total FROM {table_ref} GROUP BY {col_real} ORDER BY total DESC"
            
        return None
    
    def _construir_where(self, filtros: List[Dict], tabela: str = None) -> str:
        """Constrói cláusula WHERE baseada nos filtros e no mapeamento semântico"""
        condicoes = []
        
        # Mapeamento de campos por tabela (simplificado)
        mapeamento = self._carregar_mapeamento()
        campos_tabela = mapeamento.get(tabela, {}).get('campos', {})
        
        for filtro in filtros:
            if not filtro.get('valor'): continue
            
            if filtro['tipo'] == 'numerico':
                campo_id = 'ID'
                if tabela == 'TB_CONTATOS': campo_id = 'ID_CONTATO'
                elif tabela == 'TB_PRODUTOS': campo_id = 'ID_PRODUTO'
                elif tabela == 'TB_VENDAS': campo_id = 'ID_VENDA'
                elif 'id' in campos_tabela: campo_id = campos_tabela['id']
                
                condicoes.append(f"{campo_id} = {filtro['valor']}")
            
            elif filtro['tipo'] == 'texto':
                valor = str(filtro['valor']).replace("'", "''") # Escape básico
                # Se for email (contém @)
                if '@' in valor:
                    campo = campos_tabela.get('email', 'EMAIL')
                    condicoes.append(f"UPPER({campo}) = UPPER('{valor}')")
                # Se parecer um telefone/celular (apenas números, mais de 8 dígitos)
                elif re.sub(r'\D', '', valor).isdigit() and len(re.sub(r'\D', '', valor)) >= 8:
                    campo = campos_tabela.get('celular', 'CELULAR')
                    num = re.sub(r'\D', '', valor)
                    condicoes.append(f"(REPLACE(REPLACE(REPLACE({campo}, '-', ''), ' ', ''), '(', '') LIKE '%{num}%')")
                else:
                    # Busca por nome (padrão)
                    campo = campos_tabela.get('nome', 'NOME')
                    condicoes.append(f"UPPER({campo}) LIKE UPPER('%{valor}%')")
        
        return ' AND '.join(condicoes) if condicoes else ''
    
    def _detectar_ambiguidades(self, entidades: Dict, temporal: Dict) -> List[str]:
        """Detecta possíveis ambiguidades na interpretação"""
        ambiguidades = []
        
        if len(entidades['tabelas']) > 1:
            ambiguidades.append("Múltiplas tabelas detectadas")
        
        if temporal['tipo'] == 'desconhecido' and 'quantidade' in str(entidades):
            ambiguidades.append("Período de tempo não especificado")
        
        if not entidades['metricas'] and not entidades['dimensoes']:
            ambiguidades.append("Entidades não identificadas claramente")
        
        return ambiguidades
    
    def _gerar_sugestoes(self, intencao: Dict, entidades: Dict, temporal: Dict) -> List[str]:
        """Gera sugestões baseadas na interpretação"""
        sugestoes = []
        
        if intencao['tipo'] == 'desconhecida':
            sugestoes.append("Tente: 'Quantos contatos temos na base?'")
            sugestoes.append("Tente: 'Mostrar todos os contatos'")
        
        if temporal['tipo'] == 'desconhecido':
            sugestoes.append("Adicione um período: 'no último mês', 'este ano'")
        
        return sugestoes
    
    # ========================================
    # MÉTODOS AUXILIARES
    # ========================================
    
    def _carregar_dicionario(self) -> Dict:
        """Carrega dicionário empresarial inteligente com foco na Rohden"""
        return {
            'sinonimos': {
                'cliente': ['cliente', 'contato', 'comprador', 'consumidor', 'parceiro', 'interessado'],
                'venda': ['venda', 'pedido', 'faturamento', 'receita', 'negocio'],
                'produto': ['produto', 'item', 'peca', 'servico', 'mercadoria', 'material'],
                'contato': ['contato', 'pessoa', 'lead', 'telefone', 'celular', 'email', 'whats', 'whatsapp'],
                'geografia': ['geografica', 'geografico', 'localizacao', 'regiao', 'estado', 'cidade', 'uf', 'ddd'],
            },
            'setores': {
                'comercial': ['vendas', 'comercial', 'vendedores'],
                'financeiro': ['pagamento', 'recebimento', 'financeiro', 'cobranca'],
                'producao': ['fabrica', 'producao', 'manufatura', 'pcp'],
                'logistica': ['entrega', 'frete', 'transporte', 'logistica'],
            },
            'jargoes': {
                'os': 'ordem de servico',
                'nf': 'nota fiscal',
                'dv': 'documento de venda',
                'pv': 'pedido de venda',
            }
        }

# Instância global do interpretador
interpreter = QueryInterpreter()

def interpretar_pergunta(pergunta: str, usuario: str = None, historico: List = None) -> Dict:
    """Função principal para interpretação de perguntas"""
    return interpreter.interpretar_pergunta(pergunta, usuario, historico)
