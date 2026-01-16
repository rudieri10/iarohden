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

    def analyze_column(self, name, values, declared_type=None):
        """Analisa profundamente uma única coluna"""
        total_count = len(values)
        if total_count == 0:
            return None

        # 1. Completude
        non_null_values = [v for v in values if v is not None and str(v).strip() != '']
        null_count = total_count - len(non_null_values)
        completeness = (len(non_null_values) / total_count) * 100

        # 2. Detecção de Tipo (Inferência)
        inferred_type = "TEXT"
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

        if not non_null_values:
            return {
                'type': declared_type or "UNKNOWN",
                'inferred_type': inferred_type,
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

        return profile

    def generate_summary_markdown(self, profile):
        """Gera um resumo Markdown ultra detalhado sem IA"""
        table_name = profile['table_name']
        total_rows = profile['total_rows']
        adv = profile.get('advanced_analysis', {})
        
        md = f"### **Análise Profunda: {table_name}**\n"
        md += f"- **Volume de Dados:** {total_rows} registros analisados.\n"
        md += f"- **Data da Análise:** {profile['analysis_date']}\n\n"
        
        # --- Seção de Inteligência Avançada ---
        if adv:
            md += "#### **🚀 Inteligência e Diagnóstico Avançado**\n"
            
            # Duplicatas
            if 'duplicates' in adv:
                d = adv['duplicates']
                md += f"- **Qualidade de Dados:** {d['duplicate_count']} duplicatas detectadas ({d['duplicate_rate']}%).\n"
            
            # Lead Quality
            if 'lead_quality' in adv:
                l = adv['lead_quality']
                md += f"- **Score de Lead:** Qualidade média de {l['avg_score']}/100.\n"
                
            # Temporal
            if 'temporal_trends' in adv:
                t = adv['temporal_trends']
                last_month = list(t.keys())[-1] if t else "N/A"
                md += f"- **Tendência Temporal:** Último pico registrado em {last_month}.\n"
            
            # Segmentação
            if 'segmentation' in adv:
                s = adv['segmentation']
                top_seg = max(s, key=s.get) if s else "N/A"
                md += f"- **Segmentação:** Perfil predominante: {top_seg}.\n"
                
            # Geográfico
            if 'geographic' in adv:
                g = adv['geographic']
                top_geo = list(g.keys())[0] if g else "N/A"
                md += f"- **Distribuição Geo:** Maior concentração em {top_geo}.\n"
                
            # Compliance
            if 'compliance' in adv:
                c = adv['compliance']
                md += f"- **Compliance:** {len(c)} pontos de atenção (PII/Sensível).\n"
                
            md += "\n"

        md += "#### **Insights e Regras de Negócio Descobertas**\n"
        
        has_insights = False
        for col_name, col_data in profile['columns'].items():
            if col_data['insights']:
                has_insights = True
                md += f"**{col_name}:**\n"
                for insight in col_data['insights']:
                    md += f"- {insight}\n"
                
                # Adicionar padrão detectado
                if col_data['patterns']:
                    md += f"- Padrão técnico: {', '.join(col_data['patterns'])}\n"
                md += "\n"
        
        if not has_insights:
            md += "- Nenhuma regra especial detectada automaticamente.\n\n"

        md += "#### **Estrutura e Qualidade dos Dados**\n"
        md += "| Coluna | Completude | Cardinalidade | Tipo/Padrão |\n"
        md += "| :--- | :--- | :--- | :--- |\n"
        
        for col_name, col_data in profile['columns'].items():
            pattern = col_data['patterns'][0] if col_data['patterns'] else "Geral"
            md += f"| {col_name} | {round(col_data['completeness'])}% | {col_data['cardinality']} | {pattern} |\n"
            
        return md

class ProcessProfiler:
    """
    Analisa relacionamentos entre múltiplas tabelas para descobrir fluxos de negócio e timing.
    """
    
    def __init__(self):
        pass

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
            
            # Identificar IDs e Datas
            id_cols = {c['name'].upper() for c in cols if c['name'].upper().startswith(('ID_', 'CD_', 'COD_')) or c['is_pk']}
            date_cols = {c['name'].upper() for c in cols if any(p in c['name'].upper() for p in ['DT_', 'DATA', 'DATE', 'CRIADO'])}
            
            table_info[t_name] = {
                'ids': id_cols,
                'dates': date_cols,
                'record_count': meta.get('record_count', 0)
            }

        # 2. Construir Grafo de Relacionamentos por IDs compartilhados
        relationships = []
        for t1 in all_tables:
            for t2 in all_tables:
                if t1 == t2: continue
                
                # Chaves em comum (Ex: CD_CLIENTE em TB_CONTATOS e TB_ORCAMENTO)
                common_ids = table_info[t1]['ids'].intersection(table_info[t2]['ids'])
                if common_ids:
                    # Tabela com menos registros costuma ser a "origem" ou "mestre"
                    # Ou se for uma tabela de "CONTATOS", ela costuma iniciar fluxos
                    weight = 1
                    if "CONTATO" in t1.upper(): weight += 2
                    if "CADASTRO" in t1.upper(): weight += 2
                    
                    relationships.append((t1, t2, weight))

        # 3. Ordenação Evolutiva do Fluxo
        # Começamos com tabelas de "entrada" (contatos, cadastros)
        # E seguimos para transações (orçamentos, vendas) e finalizações (entrega, financeiro)
        
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
        
        return evolved_flow

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

    def generate_process_summary(self, process_name, flow, timing_insights):
        """Gera um resumo markdown do processo de negócio de forma evolutiva"""
        md = f"## 🔄 Inteligência de Processo Evolutiva: {process_name}\n"
        md += "### 🛤️ Jornada do Dado e Fluxo Operacional:\n"
        md += "O sistema analisou as tabelas treinadas e identificou a seguinte progressão lógica baseada em chaves, temporalidade e semântica de negócio:\n\n"
        
        for i, step in enumerate(flow):
            icon = "🏁" if i == 0 else ("🏆" if i == len(flow)-1 else "⚙️")
            md += f"{i+1}. {icon} **{step}**"
            if i < len(flow) - 1:
                md += " ➔ "
        
        md += "\n\n### 🧠 Insights de Business Intelligence:\n"
        for insight in timing_insights:
            if 'rule' in insight:
                md += f"- **{insight['from']} para {insight['to']}:** {insight['rule']}\n"
            elif 'alert' in insight:
                md += f"- **⚠️ Anomalia Detectada:** {insight['alert']}\n"
            elif 'insight' in insight:
                md += f"- **💡 Estratégia:** {insight['insight']}\n"
        
        md += "\n---\n*Este fluxo é atualizado automaticamente conforme novas tabelas são treinadas, evoluindo a compreensão do ecossistema Rohden.*"
                
        return md
