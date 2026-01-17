import re
import json
from collections import Counter
from datetime import datetime

from .analise_temporal import TemporalAnalyzer

class TrainingProfiler:
    """
    Sistema de Aprendizado PROFUNDO (Deep Profiling) sem IA.
    Realiza análise estatística, detecção de padrões e insights baseados em dados.
    """
    
    def __init__(self):
        self.temporal_analyzer = TemporalAnalyzer()
        # Padrões comuns para detecção
        self.patterns = {
            'email': r'^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$',
            'telefone_celular': r'^(\(?\d{2}\)?\s?)?9\d{4}-?\d{4}$',
            'telefone_fixo': r'^(\(?\d{2}\)?\s?)?[2-5]\d{3}-?\d{4}$',
            'cnpj': r'^\d{2}\.\d{3}\.\d{3}/\d{4}-\d{2}$',
            'cpf': r'^\d{3}\.\d{3}\.\d{3}-\d{2}$',
            'data_iso': r'^\d{4}-\d{2}-\d{2}',
            'data_br': r'^\d{2}/\d{2}/\d{4}'
        }
        # Prefixos e Sufixos Semânticos
        self.semantic_rules = {
            'prefixes': {
                'DT_': 'DATE',
                'DATA_': 'DATE',
                'VL_': 'FLOAT',
                'VALOR_': 'FLOAT',
                'QT_': 'QUANTITY',
                'QUANT_': 'QUANTITY',
                'QTD_': 'QUANTITY',
                'SL_': 'BALANCE',
                'SD_': 'BALANCE',
                'SALDO_': 'BALANCE',
                'FL_': 'BOOLEAN',
                'FLAG_': 'BOOLEAN',
                'IND_': 'BOOLEAN', # Indicador
                'CD_': 'ID',
                'ID_': 'ID',
                'COD_': 'ID',
                'ID_PAI_': 'PARENT',
                'ID_SUP_': 'SUPERVISOR',
                'ID_LIDER_': 'LEADER',
                'NIVEL_': 'LEVEL',
                'DEPTH_': 'LEVEL',
                'CREATED_': 'TIMESTAMP',
                'MODIFIED_': 'TIMESTAMP',
                'UPDATED_': 'TIMESTAMP',
                'REVISION_': 'VERSION',
                'SNAP_': 'SNAPSHOT'
            },
            'suffixes': {
                '_ID': 'ID',
                '_COD': 'ID',
                '_CODIGO': 'ID',
                '_PAI': 'PARENT',
                '_SUPERVISOR': 'SUPERVISOR',
                '_LIDER': 'LEADER',
                '_GESTOR': 'MANAGER',
                '_NIVEL': 'LEVEL',
                '_DEPTH': 'LEVEL',
                '_SUBGRUPO': 'SUBGROUP',
                '_DT': 'DATE',
                '_DATA': 'DATE',
                '_DATE': 'DATE',
                '_AT': 'TIMESTAMP',
                '_BY': 'AUDIT_USER',
                '_VERSION': 'VERSION',
                '_REVISION': 'VERSION',
                '_REVISAO': 'VERSION',
                '_SNAPSHOT': 'SNAPSHOT',
                '_LOG': 'LOG_TABLE',
                '_HIST': 'HISTORY_TABLE',
                '_HISTORICO': 'HISTORY_TABLE',
                '_VL': 'FLOAT',
                '_VALOR': 'FLOAT',
                '_QT': 'QUANTITY',
                '_QUANT': 'QUANTITY',
                '_QTD': 'QUANTITY',
                '_QUANTIDADE': 'QUANTITY',
                '_ESTOQUE': 'BALANCE',
                '_SALDO': 'BALANCE',
                '_FL': 'BOOLEAN',
                '_FLAG': 'BOOLEAN',
                '_ATIVO': 'BOOLEAN',
                '_STATUS': 'STATUS',
                '_ORIGEM': 'ORIGIN',
                '_DESTINO': 'DESTINATION',
                '_PAI': 'PARENT',
                '_FILHO': 'CHILD',
                '_CRIACAO': 'LIFECYCLE_START',
                '_INICIO': 'LIFECYCLE_START',
                '_APROVACAO': 'LIFECYCLE_STEP',
                '_ENTREGA': 'LIFECYCLE_END',
                '_FIM': 'LIFECYCLE_END',
                '_FINALIZACAO': 'LIFECYCLE_END'
            }
        }

    def get_semantic_type(self, name):
        """Identifica o tipo semântico (QUANTITY, DATE, etc) pelo nome do campo"""
        name_upper = name.upper()
        for prefix, p_type in self.semantic_rules['prefixes'].items():
            if name_upper.startswith(prefix):
                return p_type
        for suffix, s_type in self.semantic_rules['suffixes'].items():
            if name_upper.endswith(suffix):
                return s_type
        return None

    def _normalize_concept(self, name):
        """Extrai o conceito base de um nome de campo (ex: CD_CLIENTE -> CLIENTE)"""
        name = name.upper()
        # Remover prefixos conhecidos
        for prefix in self.semantic_rules['prefixes'].keys():
            if name.startswith(prefix):
                name = name[len(prefix):]
                break
        
        # Remover sufixos conhecidos
        for suffix in self.semantic_rules['suffixes'].keys():
            if name.endswith(suffix):
                name = name[:-len(suffix)]
                break
        
        # Remover termos genéricos que restaram
        for term in ['CODIGO', 'ID', 'CD', 'DATA', 'DT', 'VALOR', 'VL', 'QUANT', 'QT', 'FLAG', 'FL']:
            if name == term: return name # Se sobrar só o termo, mantém
            name = name.replace(f'_{term}', '').replace(f'{term}_', '')
            
        return name.strip('_')

    def analyze_column(self, name, values, declared_type=None):
        """Analisa profundamente uma única coluna"""
        total_count = len(values)
        if total_count == 0:
            return None

        # 1. Completude
        non_null_values = [v for v in values if v is not None and str(v).strip() != '']
        null_count = total_count - len(non_null_values)
        completeness = (len(non_null_values) / total_count) * 100

        # 2. Detecção de Tipo (Inferência + Semântica)
        inferred_type = "TEXT"
        semantic_type = None
        name_upper = name.upper()
        
        # Checar prefixos/sufixos primeiro
        for prefix, p_type in self.semantic_rules['prefixes'].items():
            if name_upper.startswith(prefix):
                semantic_type = p_type
                break
        if not semantic_type:
            for suffix, s_type in self.semantic_rules['suffixes'].items():
                if name_upper.endswith(suffix):
                    semantic_type = s_type
                    break

        if non_null_values:
            sample = non_null_values[:100]
            
            # Tentar Numérico
            is_numeric = True
            for v in sample:
                try:
                    float(str(v).replace(',', '.'))
                except (ValueError, TypeError):
                    is_numeric = False
                    break
            
            if is_numeric:
                # Verificar se é inteiro ou decimal
                is_int = True
                for v in sample:
                    if '.' in str(v) or ',' in str(v):
                        is_int = False
                        break
                inferred_type = "INTEGER" if is_int else "FLOAT"
            else:
                # Tentar Data
                is_date = True
                for v in sample:
                    v_str = str(v)
                    if not (re.match(self.patterns['data_iso'], v_str) or re.match(self.patterns['data_br'], v_str)):
                        is_date = False
                        break
                if is_date:
                    inferred_type = "DATE"
                else:
                    # Verificar Booleano
                    bool_values = {'true', 'false', '0', '1', 's', 'n', 'sim', 'nao', 'não', 't', 'f'}
                    if all(str(v).lower() in bool_values for v in sample):
                        inferred_type = "BOOLEAN"

        # Se temos um tipo semântico forte, ele pode sobrepor ou reforçar a inferência
        final_type = declared_type or semantic_type or inferred_type
        if final_type == 'ID' and inferred_type in ['INTEGER', 'TEXT']:
            final_type = inferred_type # Mantém o tipo físico mas sabemos que é ID

        if not non_null_values:
            return {
                'type': final_type,
                'inferred_type': inferred_type,
                'semantic_type': semantic_type,
                'completeness': 0,
                'cardinality': 0,
                'null_count': total_count
            }

        # 3. Cardinalidade
        unique_values = set(non_null_values)
        cardinality = len(unique_values)

        # 4. Distribuição (Top 10 valores - Aumentado)
        distribution = Counter(non_null_values).most_common(10)
        distribution_pct = [(val, count, (count/total_count)*100) for val, count in distribution]

        # 5. Padrões e Insights
        detected_patterns = []
        insights = []
        
        # --- INFERÊNCIA POR DISTRIBUIÇÃO DE DADOS ---
        cardinality_pct = (cardinality / total_count) * 100
        
        # 5.1 Identificadores (80%+ únicos)
        if cardinality_pct >= 80 and total_count > 5:
            insights.append("Alta cardinalidade detectada. Este campo é um provável IDENTIFICADOR ÚNICO.")
            if final_type not in ['ID', 'INTEGER', 'TEXT']:
                final_type = 'ID'

        # 5.2 Categorias/Status (< 10 valores distintos)
        if cardinality < 10 and total_count > 10:
            insights.append(f"Baixa cardinalidade ({cardinality} valores). Provável campo de CATEGORIA, STATUS ou CLASSIFICAÇÃO.")
            # Sugerir regra de validação baseada nos valores encontrados
            sample_cats = [str(v) for v, _ in distribution[:3]]
            insights.append(f"Regra Sugerida: Validar contra lista fixa de valores (Ex: {', '.join(sample_cats)}...).")

        # 5.3 Sequências Numéricas (Auto-incrementos)
        if inferred_type == 'INTEGER' and non_null_values:
            sorted_vals = sorted([int(v) for v in non_null_values if str(v).isdigit()])
            if len(sorted_vals) > 5:
                # Verificar se a diferença média entre valores é constante (geralmente 1)
                diffs = [sorted_vals[i+1] - sorted_vals[i] for i in range(len(sorted_vals)-1)]
                avg_diff = sum(diffs) / len(diffs)
                if 0.9 <= avg_diff <= 1.1 and all(d > 0 for d in diffs):
                    insights.append("Detectada SEQUÊNCIA NUMÉRICA estável. Provável campo AUTO-INCREMENTO ou CÓDIGO SEQUENCIAL.")

        # 5.4 Códigos Hierárquicos (Ex: 01.02.03)
        # Regex para padrões tipo 01.02 ou 01.02.03 ou 1.2.3
        hier_pattern = r'^\d+(\.\d+){1,4}$'
        str_vals = [str(v) for v in non_null_values]
        matches = [v for v in str_vals if re.match(hier_pattern, v)]
        if len(str_vals) > 0 and len(matches) / len(str_vals) > 0.5:
            insights.append("Detectado padrão de CÓDIGO HIERÁRQUICO (ex: 01.02.03). Indica estrutura de árvore (Pai/Filho/Neto).")

        # 5.5 Padrões Temporais (Sequências e Timestamps)
        is_temporal_name = (name_upper.startswith(('DT_', 'DATA_')) or 
                           name_upper.endswith(('_DT', '_DATA', '_DATE', '_AT')) or
                           'TIMESTAMP' in name_upper)
        
        if inferred_type == 'DATE' or is_temporal_name:
            if is_temporal_name:
                insights.append("Identificado como campo de DATA óbvio pela nomenclatura.")
            
            # Verificar se os dados estão em ordem cronológica
            try:
                # Converter para datetime para validar ordenação
                dates = []
                for v in non_null_values:
                    if isinstance(v, datetime): dates.append(v)
                    elif isinstance(v, str):
                        for fmt in ('%Y-%m-%d', '%d/%m/%Y', '%Y-%m-%d %H:%M:%S', '%Y-%m-%dT%H:%M:%S'):
                            try:
                                dates.append(datetime.strptime(v, fmt))
                                break
                            except: continue
                
                if len(dates) > 5:
                    is_sorted = all(dates[i] <= dates[i+1] for i in range(len(dates)-1))
                    if is_sorted:
                        insights.append("Sequência TEMPORAL detectada: Dados organizados cronologicamente.")
                    
                    # Verificar se é um timestamp (tem componente de hora/min/seg)
                    has_time = any(d.hour != 0 or d.minute != 0 or d.second != 0 for d in dates)
                    if has_time:
                        insights.append("Detectado padrão de TIMESTAMP (Data + Hora precisa).")
            except:
                pass

        # 5.6 Campos de Controle e Auditoria
        if any(k in name_upper for k in ['VERSION', 'VERSAO', 'REVISION', 'REVISAO', 'ROW_VERSION']) or name_upper.endswith(('_VERSION', '_REVISION')):
            insights.append("Campo de CONTROLE de versão detectado. Utilizado para concorrência otimista ou histórico.")
        
        if 'SNAPSHOT' in name_upper or name_upper.startswith('SNAP_'):
            insights.append("Campo de SNAPSHOT detectado: Representa o estado dos dados em um momento específico no tempo.")

        audit_user_keywords = ['CREATED_BY', 'MODIFIED_BY', 'USUARIO_', 'USER_ID', 'CRIADO_POR', 'ALTERADO_POR']
        if any(k in name_upper for k in audit_user_keywords):
            insights.append("Campo de AUDITORIA detectado: Identifica o usuário responsável pela ação.")
        
        trace_keywords = ['CREATED_AT', 'MODIFIED_AT', 'CRIADO_EM', 'ALTERADO_EM', 'DT_CRIACAO', 'DT_ALTERACAO', 'DATA_CADASTRO', 'DELETED_AT']
        if any(k in name_upper for k in trace_keywords):
            insights.append("Campo de RASTREABILIDADE detectado: Marca temporal automática do sistema.")
            if 'DELETED' in name_upper or 'EXCLUIDO' in name_upper:
                insights.append("Detectado padrão de EXCLUSÃO LÓGICA (Soft Delete).")

        # 5.7 Estados de Ciclo de Vida
        lifecycle_keywords = {
            'START': ['CRIACAO', 'INICIO', 'OPENED', 'START'],
            'STEP': ['APROVACAO', 'PROCESSAMENTO', 'APPROVED', 'STEP'],
            'END': ['ENTREGA', 'FIM', 'FINALIZACAO', 'CLOSED', 'FINISHED', 'COMPLETED']
        }
        
        for stage, keys in lifecycle_keywords.items():
            if any(k in name_upper for k in keys):
                if stage == 'START':
                    insights.append("Este campo marca o INÍCIO do ciclo de vida do registro.")
                elif stage == 'STEP':
                    insights.append("Este campo representa um ESTADO INTERMEDIÁRIO no fluxo de processo.")
                elif stage == 'END':
                    insights.append("Este campo marca a FINALIZAÇÃO do ciclo de vida do registro.")
                break

        # 5.8 Hierarquias (Padrões de Repetição)
        if not (cardinality_pct >= 80) and cardinality > 1:
            # Se um valor se repete muito mais que os outros (Lei de Pareto ou similar)
            most_common_pct = (distribution[0][1] / total_count) * 100
            if most_common_pct > 50:
                insights.append(f"Forte concentração no valor '{distribution[0][0]}' ({round(most_common_pct)}%). Indica uma HIERARQUIA ORGANIZACIONAL ou valor padrão (Default).")

        # Insights baseados em prefixos/sufixos (existentes)
        if name_upper.endswith('_ORIGEM'):
            insights.append("Este campo representa o ponto de ORIGEM de um fluxo ou processo.")
        elif name_upper.endswith('_DESTINO'):
            insights.append("Este campo representa o ponto de DESTINO de um fluxo ou processo.")
        elif name_upper.endswith('_PAI'):
            insights.append("Este campo indica uma hierarquia superior (Pai/Mestre).")
        elif name_upper.endswith('_FILHO'):
            insights.append("Este campo indica uma hierarquia inferior (Filho/Detalhe).")

        if name_upper.startswith('VL_') or name_upper.startswith('VALOR_'):
            insights.append("Detectado como um campo de VALOR MONETÁRIO ou FINANCEIRO.")
        elif name_upper.startswith('QT_') or name_upper.startswith('QUANT_'):
            insights.append("Detectado como um campo de QUANTIDADE ou VOLUME.")
        elif name_upper.startswith('FL_') or name_upper.startswith('IND_'):
            insights.append("Detectado como um INDICADOR ou FLAG (Sim/Não).")

        # Análise de string
        str_values = [str(v) for v in non_null_values]
        
        # Verificar regex
        for p_name, p_regex in self.patterns.items():
            matches = [v for v in str_values if re.match(p_regex, v)]
            if len(matches) / len(str_values) > 0.3: # Reduzido threshold para 30% para pegar mais padrões
                detected_patterns.append(p_name)

        # Insights específicos baseados em regras de negócio (ex: NOME)
        name_upper = name.upper()
        if 'NOME' in name_upper or 'RAZAO' in name_upper or 'NOME_CONTATO' in name_upper:
            pj_keywords = ['LTDA', 'S/A', 'SA', 'MEI', 'EPP', 'CNPJ', 'CONSTRUTORA', 'INDUSTRIA', 'ME', 'EIRELI', 'S.A']
            pj_count = sum(1 for v in str_values if any(k in v.upper() for k in pj_keywords))
            if pj_count > 0:
                insights.append(f"Identificados {pj_count} registros ({round((pj_count/len(str_values))*100,1)}%) que são Pessoa Jurídica.")
                insights.append("Regra: Se o campo contém termos empresariais (LTDA/SA), classificar como B2B/Empresa.")

            # Padrões de Nome (Aumentado)
            avg_words = sum(len(v.split()) for v in str_values) / len(str_values)
            if avg_words > 2:
                insights.append(f"Média de {round(avg_words,1)} palavras por nome. Indica nomes completos/razão social.")
            
        # Insights para Telefone/WhatsApp
        if 'CELULAR' in name_upper or 'TELEFONE' in name_upper or 'WHATS' in name_upper:
            ddd_counts = Counter([v[:2] for v in str_values if len(v) >= 2 and v[:2].isdigit()])
            top_ddd = ddd_counts.most_common(1)
            if top_ddd:
                insights.append(f"Região predominante: DDD {top_ddd[0][0]} ({round((top_ddd[0][1]/len(str_values))*100,1)}% dos contatos).")

        # Insights para Email
        if 'EMAIL' in name_upper or 'E-MAIL' in name_upper:
            domains = [v.split('@')[-1] for v in str_values if '@' in v]
            top_domains = Counter(domains).most_common(2)
            for domain, count in top_domains:
                insights.append(f"Domínio frequente: {domain} ({round((count/len(str_values))*100,1)}%).")

        # Outliers (Frequência muito baixa em colunas de baixa cardinalidade)
        if cardinality < total_count * 0.2: # Aumentado threshold para 20%
            rare_values = [val for val, count in Counter(non_null_values).items() if count == 1]
            if len(rare_values) > 0:
                insights.append(f"Detectados {len(rare_values)} valores únicos (outliers) que podem ser erros de digitação ou exceções.")

        return {
            'type': declared_type or inferred_type,
            'inferred_type': inferred_type,
            'completeness': round(completeness, 2),
            'null_count': null_count,
            'cardinality': cardinality,
            'cardinality_pct': round((cardinality / total_count) * 100, 2),
            'distribution': distribution_pct,
            'patterns': detected_patterns,
            'insights': insights,
            'sample_values': list(unique_values)[:10] # Aumentado para 10 amostras
        }

    def analyze_business_rules(self, columns, data):
        """Detecta regras de negócio implícitas (Constraints, Condicionais, Totais)"""
        rules = []
        if not data: return rules
        
        col_names = [c['name'] for c in columns]
        
        # 1. Campos Obrigatórios (Never NULL)
        for col in col_names:
            values = [row.get(col) for row in data]
            non_null = [v for v in values if v is not None and str(v).strip() != '']
            if len(non_null) == len(data) and len(data) > 5:
                rules.append({
                    'type': 'MANDATORY',
                    'field': col,
                    'description': f"O campo `{col}` é OBRIGATÓRIO (100% preenchido na amostra)."
                })

        # 2. Valores Condicionais (Ex: STATUS='ATIVO' quando DT_CANCELAMENTO IS NULL)
        # Procurar por colunas de status/flag e colunas de data/valor correlacionadas
        status_cols = [c for c in col_names if 'STATUS' in c.upper() or 'FL_' in c.upper() or 'IND_' in c.upper()]
        date_cols = [c for c in col_names if 'DT_' in c.upper() or 'DATA' in c.upper()]
        
        if status_cols and date_cols:
            for s_col in status_cols:
                for d_col in date_cols:
                    # Verificar se existe correlação: d_col is null => s_col tem um valor específico
                    null_d_rows = [row for row in data if row.get(d_col) is None or str(row.get(d_col)).strip() == '']
                    if 0 < len(null_d_rows) < len(data):
                        s_values = [str(row.get(s_col)).upper() for row in null_d_rows]
                        most_common_s = Counter(s_values).most_common(1)
                        if most_common_s and most_common_s[0][1] / len(null_d_rows) > 0.95:
                            rules.append({
                                'type': 'CONDITIONAL',
                                'condition': f"`{d_col}` IS NULL",
                                'result': f"`{s_col}` = '{most_common_s[0][0]}'",
                                'description': f"Regra Condicional: Quando `{d_col}` está vazio, `{s_col}` é geralmente '{most_common_s[0][0]}'."
                            })

        # 3. Totalizações Simples (Dentro da mesma linha)
        # Ex: VALOR_TOTAL = VALOR_ITEM + VALOR_FRETE
        num_cols = [c for c in col_names if self._is_numeric_col(c, data)]
        if len(num_cols) >= 3:
            for total_col in num_cols:
                if 'TOTAL' in total_col.upper() or 'SOMA' in total_col.upper():
                    other_nums = [c for c in num_cols if c != total_col]
                    # Testar combinações de 2 colunas
                    for i in range(len(other_nums)):
                        for j in range(i + 1, len(other_nums)):
                            c1, c2 = other_nums[i], other_nums[j]
                            matches = 0
                            for row in data:
                                try:
                                    v_t = float(str(row.get(total_col)).replace(',', '.'))
                                    v1 = float(str(row.get(c1)).replace(',', '.'))
                                    v2 = float(str(row.get(c2)).replace(',', '.'))
                                    if abs(v_t - (v1 + v2)) < 0.01: matches += 1
                                except: continue
                            
                            if matches / len(data) > 0.8:
                                rules.append({
                                    'type': 'TOTALIZATION',
                                    'formula': f"`{total_col}` = `{c1}` + `{c2}`",
                                    'description': f"Cálculo Detectado: `{total_col}` é a soma de `{c1}` e `{c2}`."
                                })
        return rules

    def _is_numeric_col(self, col_name, data):
        sample = [row.get(col_name) for row in data[:20] if row.get(col_name) is not None]
        if not sample: return False
        try:
            for v in sample: float(str(v).replace(',', '.'))
            return True
        except: return False

    def profile_table(self, table_name, columns, data):
        """Gera o profile completo da tabela"""
        # Garantir data e hora correta (Brasília/Local)
        now = datetime.now()
        analysis_date = now.strftime("%d/%m/%Y %H:%M:%S")
        
        # Converter data para lista de dicts para o TemporalAnalyzer se for lista de listas
        dict_data = []
        if data and isinstance(data[0], (list, tuple)):
            for row in data:
                dict_row = {}
                for i, col in enumerate(columns):
                    dict_row[col['name']] = row[i]
                dict_data.append(dict_row)
        else:
            dict_data = data

        profile = {
            'table_name': table_name,
            'total_rows': len(data),
            'analysis_date': analysis_date,
            'columns': {},
            'advanced_analysis': {}
        }

        # 1. Análise Básica de Colunas
        for i, col in enumerate(columns):
            col_name = col['name']
            col_type = col.get('type')
            col_values = [row[i] if isinstance(row, (list, tuple)) else row.get(col_name) for row in data]
            profile['columns'][col_name] = self.analyze_column(col_name, col_values, col_type)

        # 2. Análise Avançada (Temporal, Duplicatas, etc.)
        profile['advanced_analysis']['business_rules'] = self.analyze_business_rules(columns, dict_data)
        
        col_names = [c['name'].upper() for c in columns]
        
        # 2.1 Detecção de Duplicatas (Procurar IDs ou nomes/emails)
        key_cols = [c for c in ['ID', 'CD_CONTATO', 'EMAIL', 'E_MAIL', 'CPF', 'CNPJ'] if c in col_names]
        if key_cols:
            profile['advanced_analysis']['duplicates'] = self.temporal_analyzer.detect_duplicates(dict_data, key_cols)

        # 2.2 Análise Temporal (Procurar colunas de data)
        date_col = next((c for c in col_names if 'DT_' in c or 'DATA' in c), None)
        if date_col:
            profile['advanced_analysis']['temporal_trends'] = self.temporal_analyzer.temporal_trends(dict_data, date_col)

        # 2.3 Qualidade de Lead (Se for tabela de contatos)
        if 'TB_CONTATOS' in table_name.upper():
            scores = [self.temporal_analyzer.calculate_lead_quality(row) for row in dict_data]
            if scores:
                profile['advanced_analysis']['lead_quality'] = {
                    'avg_score': round(sum(scores)/len(scores), 2),
                    'min': min(scores),
                    'max': max(scores)
                }

        # 2.4 Segmentação Automática (Se tiver valor)
        value_col = next((c for c in col_names if 'VALOR' in c or 'TOTAL' in c or 'VL_' in c), None)
        if value_col:
            profile['advanced_analysis']['segmentation'] = self.temporal_analyzer.automatic_segmentation(dict_data, value_col)
            profile['advanced_analysis']['anomalies'] = self.temporal_analyzer.detect_anomalies(dict_data, value_col)

        # 2.5 Geográfico
        phone_col = next((c for c in col_names if 'CELULAR' in c or 'FONE' in c or 'TELEFONE' in c), None)
        city_col = next((c for c in col_names if 'CIDADE' in c or 'MUNICIPIO' in c), None)
        if phone_col or city_col:
            profile['advanced_analysis']['geographic'] = self.temporal_analyzer.geographic_analysis(dict_data, phone_col, city_col)

        # 2.6 Auditoria de Compliance
        sensitive = [c for c in ['CPF', 'CNPJ', 'SENHA', 'VALOR', 'VL_'] if c in col_names]
        if sensitive:
            profile['advanced_analysis']['compliance'] = self.temporal_analyzer.compliance_audit(dict_data, sensitive)

        # 3. Mapeamento de Dependências Temporais em Cascata (Cross-Table)
        # Se tivermos acesso a metadados de outras tabelas, podemos detectar a ordem de criação

        return profile

    def generate_summary_markdown(self, profile):
        """Gera um resumo Markdown ultra detalhado e visualmente rico"""
        table_name = profile['table_name']
        total_rows = profile['total_rows']
        adv = profile.get('advanced_analysis', {})
        
        md = f"# 🧠 Inteligência de Dados: {table_name}\n"
        md += f"> **Relatório gerado em:** {profile['analysis_date']}\n"
        md += f"> **Volume analisado:** {total_rows:,} registros\n\n"
        
        # --- Seção de Inteligência Avançada ---
        if adv:
            md += "## 🚀 Diagnóstico Estratégico\n"
            
            # Regras de Negócio Detectadas
            if 'business_rules' in adv and adv['business_rules']:
                md += "### **🎯 Regras de Negócio Identificadas**\n"
                for rule in adv['business_rules']:
                    md += f"- **{rule['type']}:** {rule['description']}\n"
                md += "\n"

            # Qualidade e Saúde dos Dados
            md += "### **📊 Saúde dos Dados**\n"
            if 'duplicates' in adv:
                d = adv['duplicates']
                status = "✅ Saudável" if d['duplicate_rate'] < 1 else "⚠️ Atenção"
                md += f"- **Duplicidade:** {d['duplicate_count']} registros repetidos ({d['duplicate_rate']}%). Status: {status}\n"
            
            if 'lead_quality' in adv:
                l = adv['lead_quality']
                md += f"- **Score de Qualidade:** Média de {l['avg_score']}/100 baseada em preenchimento de campos críticos.\n"
                
            # Temporal
            if 'temporal_trends' in adv:
                t = adv['temporal_trends']
                if t:
                    last_month = list(t.keys())[-1]
                    md += f"- **Sazonalidade:** Última atividade relevante detectada em {last_month}.\n"
            
            # Segmentação
            if 'segmentation' in adv:
                s = adv['segmentation']
                if s:
                    top_seg = max(s, key=s.get)
                    md += f"- **Perfil Principal:** {top_seg} (Segmento predominante).\n"
                
            # Geográfico
            if 'geographic' in adv:
                g = adv['geographic']
                if g:
                    top_geo = list(g.keys())[0]
                    md += f"- **Geografia:** Concentração principal em {top_geo}.\n"
            
            md += "\n"

        md += "## 💡 Insights e Padrões por Campo\n"
        
        has_insights = False
        for col_name, col_data in profile['columns'].items():
            if col_data['insights']:
                has_insights = True
                md += f"### **{col_name}**\n"
                for insight in col_data['insights']:
                    md += f"- {insight}\n"
                
                # Adicionar padrão detectado
                if col_data['patterns']:
                    md += f"- **Padrão:** `{', '.join(col_data['patterns'])}`\n"
                md += "\n"
        
        if not has_insights:
            md += "- Nenhuma regra especial detectada automaticamente nos campos individuais.\n\n"

        md += "## 📋 Estrutura Técnica\n"
        md += "| Campo | Preenchimento | Cardinalidade | Classificação |\n"
        md += "| :--- | :---: | :---: | :--- |\n"
        
        for col_name, col_data in profile['columns'].items():
            pattern = col_data['patterns'][0] if col_data['patterns'] else "Geral"
            comp = round(col_data['completeness'])
            status_icon = "🟢" if comp > 90 else ("🟡" if comp > 50 else "🔴")
            md += f"| {col_name} | {status_icon} {comp}% | {col_data['cardinality']} | {pattern} |\n"
            
        return md

class ProcessProfiler:
    """
    Analisa relacionamentos entre múltiplas tabelas para descobrir fluxos de negócio e timing.
    """
    
    def __init__(self):
        self.tp = TrainingProfiler() # Helper para usar regras semânticas

    def discover_flow(self, tables_metadata):
        """
        Descobre a sequência lógica de uso das tabelas de forma evolutiva.
        Analisa chaves compartilhadas, nomes de colunas e padrões temporais.
        """
        if not tables_metadata:
            return []

        # 1. Mapear tabelas e suas características
        table_info = {}
        all_tables = []
        for meta in tables_metadata:
            t_name = meta['table_name']
            all_tables.append(t_name)
            cols = meta.get('columns_info', [])
            sample = meta.get('sample_data', [])
            
            # Identificar IDs e Datas
            id_cols = {c['name'].upper() for c in cols if 
                       c['name'].upper().startswith(('ID_', 'CD_', 'COD_')) or 
                       c['name'].upper().endswith(('_ID', '_COD', '_CODIGO')) or
                       c.get('is_pk', False)}
            
            # Identificar Datas e Estados de Ciclo de Vida
            date_cols = {c['name'].upper() for c in cols if any(p in c['name'].upper() for p in ['DT_', 'DATA', 'DATE', 'CRIADO', 'INICIO', 'FIM', 'ENTREGA', 'APROVACAO', 'FINALIZACAO'])}
            
            # Identificar Campos de Versionamento e Auditoria
            version_cols = {c['name'].upper() for c in cols if any(p in c['name'].upper() for p in ['VERSION', 'REVISION', 'VERSAO', 'REVISAO', 'SNAPSHOT'])}
            soft_delete_cols = {c['name'].upper() for c in cols if any(p in c['name'].upper() for p in ['FL_ATIVO', 'DELETED_AT', 'EXCLUIDO', 'STATUS'])}
            
            # Mapear conceitos semânticos para as colunas
            semantic_concepts = {}
            semantic_types = {}
            for c in cols:
                c_name = c['name'].upper()
                concept = self.tp._normalize_concept(c_name)
                # Se o conceito for genérico (ID, COD) e for uma PK ou única, assume o nome da tabela
                if concept in ['ID', 'COD', 'CD', 'CODIGO'] and (c.get('is_pk', False) or c_name in id_cols):
                    # Tenta extrair o conceito do nome da tabela (ex: TB_CLIENTES -> CLIENTE)
                    t_concept = t_name.upper().replace('TB_', '').strip('_')
                    if t_concept.endswith('S'): t_concept = t_concept[:-1] # Plural simples
                    concept = t_concept
                semantic_concepts[c_name] = concept
                semantic_types[c_name] = self.tp.get_semantic_type(c_name)
            
            # Mapear valores das colunas ID para análise de cardinalidade
            id_values = {}
            for id_col in id_cols:
                values = [str(row.get(id_col)) for row in sample if row.get(id_col) is not None]
                if values:
                    id_values[id_col] = {
                        'set': set(values),
                        'list': values,
                        'total': len(values),
                        'unique': len(set(values)),
                        'concept': semantic_concepts.get(id_col)
                    }

            # Extrair timestamps médios de criação para análise de ordem
            creation_ts = None
            creation_cols = [c for c in date_cols if any(k in c for k in ['CRIACAO', 'CRIADO', 'START', 'INICIO', 'CADASTRO'])]
            if creation_cols and sample:
                ts_list = []
                for row in sample:
                    val = row.get(creation_cols[0])
                    if val:
                        if isinstance(val, datetime): ts_list.append(val.timestamp())
                        elif isinstance(val, str):
                            for fmt in ('%Y-%m-%d', '%d/%m/%Y', '%Y-%m-%d %H:%M:%S'):
                                try:
                                    ts_list.append(datetime.strptime(val, fmt).timestamp())
                                    break
                                except: continue
                if ts_list:
                    creation_ts = sum(ts_list) / len(ts_list)

            table_info[t_name] = {
                'id_cols': id_cols,
                'date_cols': date_cols,
                'version_cols': version_cols,
                'soft_delete_cols': soft_delete_cols,
                'semantic_concepts': semantic_concepts,
                'semantic_types': semantic_types,
                'id_values': id_values,
                'is_history': any(suffix in t_name.upper() for suffix in ['_HIST', '_LOG', '_AUDIT', 'HISTORICO']),
                'total_rows': meta.get('record_count', 0),
                'avg_creation_ts': creation_ts
            }

        # 2. Construir Grafo de Relacionamentos por IDs, Conceitos e Cardinalidade
        relationships = []
        for t1 in all_tables:
            # 2.0 Detecção de Auto-Relacionamento (Hierarquias na mesma tabela)
            info = table_info[t1]
            id_cols_list = list(info['id_values'].keys())
            
            for i in range(len(id_cols_list)):
                for j in range(len(id_cols_list)):
                    if i == j: continue
                    id1 = id_cols_list[i]
                    id2 = id_cols_list[j]
                    val1 = info['id_values'][id1]
                    val2 = info['id_values'][id2]
                    
                    # Se um campo referencia o outro na mesma tabela
                    # Critério: val2 é subconjunto de val1 (ex: supervisor_id está contido em funcionario_id)
                    # OU se tem sufixo hierárquico
                    is_hierarchical_concept = any(c in str(val2['concept']) for c in ['PARENT', 'SUPERVISOR', 'LEADER', 'MANAGER', 'PAI'])
                    intersection = val2['set'].intersection(val1['set'])
                    
                    if is_hierarchical_concept or (intersection and len(intersection) / len(val2['set']) > 0.5):
                        relationships.append({
                            'tables': (t1, t1),
                            'fields': (id1, id2),
                            'concepts': (val1['concept'], val2['concept']),
                            'cardinality': "1:N", # Geralmente um supervisor tem N subordinados
                            'overlap_count': len(intersection),
                            'type': "Hierarquia (Auto-Relacionamento)"
                        })

            for t2 in all_tables:
                if t1 == t2: continue
                
                # 2.1 Chaves em comum por NOME ou CONCEITO
                for id1, val1 in table_info[t1]['id_values'].items():
                    concept1 = val1['concept']
                    
                    for id2, val2 in table_info[t2]['id_values'].items():
                        concept2 = val2['concept']
                        
                        # Se os nomes são iguais OU os conceitos base são iguais (ex: CD_CLIENTE e CLIENTE_ID)
                        is_same_concept = (id1 == id2) or (concept1 == concept2 and concept1 not in ['ID', 'COD', 'CD'])
                        
                        # 2.2 Chaves em comum por VALORES (Relacionamentos Implícitos)
                        intersection = val1['set'].intersection(val2['set'])
                        
                        # Critério de match: Conceito igual OU Sobreposição significativa de dados
                        if is_same_concept or (intersection and (len(intersection) / min(len(val1['set']), len(val2['set']))) > 0.3):
                            
                            # ANALISAR CARDINALIDADE
                            t1_unique = val1['unique'] == val1['total']
                            t2_unique = val2['unique'] == val2['total']
                            
                            cardinality = "N:N"
                            if t1_unique and t2_unique: cardinality = "1:1"
                            elif t1_unique and not t2_unique: cardinality = "1:N"
                            elif not t1_unique and t2_unique: cardinality = "N:1"
                            
                            rel_type = "Explicito (FK)"
                            if id1 != id2:
                                if concept1 == concept2: rel_type = "Semântico (Conceito)"
                                else: rel_type = "Implícito (Dados)"
                            
                            # Check de Ordem Temporal (4.1 Análise de Ordem de Criação)
                            ts1 = table_info[t1].get('avg_creation_ts')
                            ts2 = table_info[t2].get('avg_creation_ts')
                            if ts1 and ts2 and ts1 < ts2:
                                rel_type += " (Sequência Temporal Detectada)"

                            # Check de Totais (4.2 Totalizações Cross-Table)
                            # Se t1 -> t2 é 1:N, verificar se t1 tem um campo TOTAL que é soma de t2
                            if cardinality == "1:N":
                                parent, child = t1, t2
                                parent_num = [c for c in table_info[parent]['semantic_concepts'] if table_info[parent]['semantic_concepts'][c] == 'VALOR' or 'TOTAL' in c]
                                child_num = [c for c in table_info[child]['semantic_concepts'] if table_info[child]['semantic_concepts'][c] == 'VALOR' or 'PRECO' in c]
                                
                                if parent_num and child_num:
                                    # Esta análise requer dados correlacionados reais, o que pode ser pesado.
                                    # Por enquanto, marcamos como "Potencial Totalização" se os nomes sugerirem.
                                    for p_col in parent_num:
                                        for c_col in child_num:
                                            if 'TOTAL' in p_col and ('VALOR' in c_col or 'PRECO' in c_col):
                                                rel_type += f" (Provável Totalização: {p_col} ≈ Σ {c_col})"

                            relationships.append({
                                'tables': (t1, t2),
                                'fields': (id1, id2),
                                'concepts': (concept1, concept2),
                                'cardinality': cardinality,
                                'overlap_count': len(intersection),
                                'type': rel_type
                            })

        # 3. Ordenação Evolutiva do Fluxo (Mantendo lógica original)
        priority_keywords = {
            'START': ['CONTATO', 'LEAD', 'CADASTRO', 'CLIENTE', 'PROSPECT'],
            'MIDDLE': ['ORCAMENTO', 'PROPOSTA', 'PEDIDO', 'REQUISICAO'],
            'TRANSACTION': ['VENDA', 'FATURAMENTO', 'NOTA', 'FISCAL'],
            'END': ['ENTREGA', 'LOGISTICA', 'FINANCEIRO', 'COBRANCA', 'PAGAMENTO']
        }

        def get_table_score(name):
            name = name.upper()
            if any(k in name for k in priority_keywords['START']): return 10
            if any(k in name for k in priority_keywords['MIDDLE']): return 20
            if any(k in name for k in priority_keywords['TRANSACTION']): return 30
            if any(k in name for k in priority_keywords['END']): return 40
            return 25

        # Ordena as tabelas baseada no score de ciclo de vida
        evolved_flow = sorted(all_tables, key=get_table_score)
        
        # 4. Detecção de Estruturas em Cascata
        cascades = self._detect_cascades(relationships)
        
        # 5. Detecção de Fluxos de Movimentação (NOVO)
        movements = self._detect_data_movements(relationships, table_info)
        
        return {
            'flow': evolved_flow,
            'relationships': relationships,
            'cascades': cascades,
            'table_info': table_info,
            'movements': movements
        }

    def _detect_data_movements(self, relationships, table_info):
        """
        Mapeia fluxos de movimentação entre tabelas (ex: Venda diminui Estoque)
        """
        movements = []
        seen = set()
        
        for rel in relationships:
            t1, t2 = rel['tables']
            if (t1, t2) in seen: continue
            seen.add((t1, t2))
            seen.add((t2, t1))
            
            info1 = table_info[t1]
            info2 = table_info[t2]
            
            # Buscar campos de quantidade/saldo
            q_cols1 = [c for c, s_type in info1['semantic_types'].items() if s_type in ['QUANTITY', 'BALANCE']]
            q_cols2 = [c for c, s_type in info2['semantic_types'].items() if s_type in ['QUANTITY', 'BALANCE']]
            
            if q_cols1 and q_cols2:
                # Heurística para transação vs estado
                trans_keywords = ['VENDA', 'MOV', 'PEDIDO', 'SAIDA', 'ENTRADA', 'ITEM', 'FATURA', 'ORDEM']
                state_keywords = ['ESTOQUE', 'SALDO', 'PRODUTO', 'ARMAZEM', 'INVENTARIO', 'CONTA']
                
                is_trans1 = any(k in t1.upper() for k in trans_keywords)
                is_state1 = any(k in t1.upper() for k in state_keywords)
                
                is_trans2 = any(k in t2.upper() for k in trans_keywords)
                is_state2 = any(k in t2.upper() for k in state_keywords)
                
                if is_trans1 and is_state2:
                    action = 'Diminui' if any(k in t1.upper() for k in ['VENDA', 'SAIDA', 'BAIXA']) else 'Aumenta'
                    movements.append({
                        'origin': t1,
                        'target': t2,
                        'origin_col': q_cols1[0],
                        'target_col': q_cols2[0],
                        'description': f"Aumentos em **{t1}.{q_cols1[0]}** resultam em {action} de **{t2}.{q_cols2[0]}**"
                    })
                elif is_trans2 and is_state1:
                    action = 'Diminui' if any(k in t2.upper() for k in ['VENDA', 'SAIDA', 'BAIXA']) else 'Aumenta'
                    movements.append({
                        'origin': t2,
                        'target': t1,
                        'origin_col': q_cols2[0],
                        'target_col': q_cols1[0],
                        'description': f"Aumentos em **{t2}.{q_cols2[0]}** resultam em {action} de **{t1}.{q_cols1[0]}**"
                    })
        
        return movements

    def _detect_cascades(self, relationships):
        """
        Mapeia sequências lógicas de dependência (Ex: Empresa -> Filial -> Depto)
        """
        # 1. Construir mapa de adjacência (apenas 1:N que são as expansões naturais)
        adj = {}
        for rel in relationships:
            if rel['cardinality'] == '1:N' and rel['type'] != 'Hierarquia (Auto-Relacionamento)':
                t1, t2 = rel['tables']
                if t1 not in adj: adj[t1] = []
                adj[t1].append(t2)
        
        # 2. Encontrar caminhos longos (Cascades)
        cascades = []
        
        def find_paths(current, path):
            if current not in adj or not adj[current]:
                if len(path) > 2: # Só cadeias com 3 ou mais tabelas
                    cascades.append(path)
                return
            
            for neighbor in adj[current]:
                if neighbor not in path: # Evitar ciclos
                    find_paths(neighbor, path + [neighbor])

        for start_node in adj.keys():
            find_paths(start_node, [start_node])
            
        # 3. Filtrar caminhos que são subconjuntos de outros caminhos maiores
        final_cascades = []
        cascades.sort(key=len, reverse=True)
        for c in cascades:
            is_sub = False
            for f in final_cascades:
                # Se c está contido em f
                if all(x in f for x in c):
                    is_sub = True
                    break
            if not is_sub:
                final_cascades.append(c)
                
        return final_cascades

    def analyze_timing(self, flow, tables_metadata):
        """
        Analisa o tempo médio entre os passos do fluxo e consistência entre tabelas.
        """
        insights = []
        
        # 1. Regras de Timing (Solicitadas)
        if "TB_CONTATOS" in flow and "TB_ORCAMENTO" in flow:
            insights.append({
                "from": "TB_CONTATOS",
                "to": "TB_ORCAMENTO",
                "rule": "ORCAMENTO sempre criado < 30 dias após CONTATO",
                "type": "conversion_speed"
            })
            
        if "TB_ORCAMENTO" in flow and "TB_VENDAS" in flow:
            insights.append({
                "from": "TB_ORCAMENTO",
                "to": "TB_VENDAS",
                "rule": "VENDA sempre < 15 dias após ORCAMENTO",
                "type": "closing_speed"
            })

        # 2. Análise de Consistência Cross-Tabela
        if "TB_VENDAS" in flow and "TB_ORCAMENTO" not in flow:
            insights.append({
                "alert": "Vendas detectadas sem registros de orçamento prévios no sistema.",
                "type": "consistency_gap"
            })

        # 3. Alertas de gap genéricos
        insights.append({
            "gap_limit": 60,
            "insight": "Gap > 60 dias entre passos do ciclo comercial = oportunidade perdida",
            "type": "churn_alert"
        })
        
        return insights

    def generate_process_summary(self, process_name, flow, timing_insights, relationships, cascades=None, table_info=None, business_rules=None, movements=None):
        """
        Gera um resumo executivo do processo em Markdown para consumo da IA e do usuário.
        """
        md = f"## 🔄 Inteligência de Processo Evolutiva: {process_name}\n"
        
        # 1. Fluxo Operacional
        md += "### 🛤️ Jornada do Dado e Fluxo Operacional:\n"
        md += "O sistema analisou as tabelas treinadas e identificou a seguinte progressão lógica baseada em chaves, temporalidade e semântica de negócio:\n\n"
        
        if flow:
            steps = []
            for i, step in enumerate(flow):
                steps.append(f"{i+1}. 🏁 **{step}**" if i == 0 else f"{i+1}. 🏆 **{step}**")
            md += " ➔ ".join(steps) + "\n"
        else:
            md += "*Fluxo ainda em fase de aprendizado. Treine mais tabelas para conectar os pontos.*\n"

        # 2. Mapeamento de Fluxos e Movimentação (NOVO)
        if movements:
            md += "\n### 📦 Mapeamento de Fluxos e Movimentação:\n"
            md += "A IA detectou fluxos de movimentação de valores/quantidades entre entidades:\n"
            for mov in movements:
                md += f"- {mov['description']}\n"

        # 3. Versionamento e Histórico
        if table_info:
            history_tables = {t: info for t, info in table_info.items() if info.get('is_history')}
            if history_tables:
                md += "\n### 📜 Versionamento e Histórico de Dados:\n"
                md += "Tabelas dedicadas à auditoria e logs de alterações:\n"
                for t, info in history_tables.items():
                    v_cols = list(info.get('version_cols', []))
                    v_str = f" (Versão via: {', '.join(v_cols)})" if v_cols else ""
                    md += f"- **{t}**: Registra histórico de transações{v_str}.\n"

        # 3. Ciclos de Vida
        if table_info:
            lifecycle_tables = {t: info for t, info in table_info.items() if info.get('date_cols') and len(info['date_cols']) >= 2}
            if lifecycle_tables:
                md += "\n### ⏳ Ciclos de Vida e Estados Temporais:\n"
                md += "Tabelas com múltiplos marcos temporais que indicam estados de evolução:\n"
                for t, info in lifecycle_tables.items():
                    dates = sorted(list(info['date_cols']))
                    md += f"- **{t}**: { ' ➔ '.join([f'**{d}**' for d in dates]) }\n"

        # 4. Regras de Negócio e Constraints (NOVO)
        if business_rules:
            md += "\n### ⚖️ Regras de Negócio e Constraints Detectadas:\n"
            md += "Padrões implícitos identificados nos dados:\n"
            for table, rules in business_rules.items():
                if rules:
                    md += f"#### Tabela: {table}\n"
                    for rule in rules:
                        md += f"- {rule['description']}\n"

        # 5. Dependências e Cascata (NOVO)
        if relationships:
            cascades = [r for r in relationships if 'Sequência Temporal' in r['type'] or 'Totalização' in r['type']]
            if cascades:
                md += "\n### 🌊 Dependências em Cascata e Totalizações:\n"
                for rel in cascades:
                    md += f"- **{rel['tables'][0]}** ➔ **{rel['tables'][1]}**: {rel['type']}\n"

        # Insights de BI
        md += "\n\n### 🧠 Insights de Business Intelligence:\n"
        for insight in timing_insights:
            md += f"- {insight}\n"
            
        md += "\n---\n"
        md += "*Este fluxo é atualizado automaticamente conforme novas tabelas são treinadas, evoluindo a compreensão do ecossistema Rohden.*"
        
        return md
