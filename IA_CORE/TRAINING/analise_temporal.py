import re
import json
import math
import statistics
from collections import Counter, defaultdict
from datetime import datetime, timedelta

class DataInsightEngine:
    """
    Motor de Inteligência Analítica Universal.
    Capaz de analisar qualquer tabela empresarial para extrair padrões,
    correlações, anomalias e insights de negócio.
    """

    def __init__(self):
        self.geo_map = {
            '11': 'SP', '12': 'SP', '13': 'SP', '14': 'SP', '15': 'SP', '16': 'SP', '17': 'SP', '18': 'SP', '19': 'SP',
            '21': 'RJ', '22': 'RJ', '24': 'RJ',
            '27': 'ES', '28': 'ES',
            '31': 'MG', '32': 'MG', '33': 'MG', '34': 'MG', '35': 'MG', '37': 'MG', '38': 'MG',
            '41': 'PR', '42': 'PR', '43': 'PR', '44': 'PR', '45': 'PR', '46': 'PR',
            '47': 'SC', '48': 'SC', '49': 'SC',
            '51': 'RS', '53': 'RS', '54': 'RS', '55': 'RS',
            '61': 'DF', '62': 'GO', '64': 'GO',
            '63': 'TO',
            '65': 'MT', '66': 'MT',
            '67': 'MS',
            '68': 'AC',
            '69': 'RO',
            '71': 'BA', '73': 'BA', '74': 'BA', '75': 'BA', '77': 'BA',
            '79': 'SE',
            '81': 'PE', '82': 'AL', '83': 'PB', '84': 'RN', '85': 'CE', '86': 'PI', '87': 'PE', '88': 'CE', '89': 'PI',
            '91': 'PA', '92': 'AM', '93': 'PA', '94': 'PA', '95': 'RR', '96': 'AP', '97': 'AM', '98': 'MA', '99': 'MA'
        }

    def detect_duplicates(self, data, key_columns):
        """Detecção de duplicatas inteligente baseada em colunas chave."""
        if not data or not key_columns:
            return {"status": "skipped", "reason": "No data or keys"}

        seen = {}
        duplicates = []
        for i, row in enumerate(data):
            signature = tuple(str(row.get(col, '')).strip().upper() for col in key_columns if col in row)
            if not any(signature): continue
            
            if signature in seen:
                duplicates.append({"original_index": seen[signature], "duplicate_index": i, "signature": signature})
            else:
                seen[signature] = i
        
        total = len(data)
        dup_count = len(duplicates)
        return {
            "total_records": total,
            "unique_records": len(seen),
            "duplicate_count": dup_count,
            "duplicate_rate": round((dup_count / total) * 100, 2) if total > 0 else 0,
            "sample_duplicates": duplicates[:5]
        }

    def analyze_correlations(self, data, columns):
        """
        Detecta correlações e dependências entre colunas.
        Ex: Se Coluna A é 'X', Coluna B é sempre 'Y'.
        """
        if not data or len(data) < 5: return []
        
        correlations = []
        # Filtrar colunas com baixa cardinalidade (prováveis categorias/status)
        cat_cols = []
        for col in columns:
            vals = [str(row.get(col)).strip() for row in data if row.get(col) is not None]
            if 1 < len(set(vals)) < len(data) * 0.5:
                cat_cols.append(col)
        
        for i, col_a in enumerate(cat_cols):
            for col_b in cat_cols[i+1:]:
                # Matriz de co-ocorrência
                co_occurrence = defaultdict(Counter)
                for row in data:
                    val_a = str(row.get(col_a))
                    val_b = str(row.get(col_b))
                    co_occurrence[val_a][val_b] += 1
                
                # Verificar se existe uma dependência forte (regra de 95%)
                for val_a, counter_b in co_occurrence.items():
                    total_a = sum(counter_b.values())
                    for val_b, count in counter_b.items():
                        confidence = count / total_a
                        if confidence >= 0.95 and count > 2:
                            correlations.append({
                                "type": "DEPENDENCY",
                                "from": f"{col_a}='{val_a}'",
                                "to": f"{col_b}='{val_b}'",
                                "confidence": round(confidence * 100, 2),
                                "support": count
                            })
        return correlations

    def calculate_health_score(self, data, columns):
        """Calcula o Score de Saúde dos Dados (0-100) para qualquer tabela."""
        if not data: return 0
        
        total_rows = len(data)
        total_cells = total_rows * len(columns)
        
        # 1. Completude (Peso 40%)
        filled_cells = sum(1 for row in data for col in columns if row.get(col) is not None and str(row.get(col)).strip() != '')
        completeness = (filled_cells / total_cells) * 100
        
        # 2. Unicidade (Peso 30%) - Baseado em colunas que parecem IDs
        id_cols = [c for c in columns if any(k in c.upper() for k in ['ID', 'CD_', 'COD_'])]
        uniqueness = 100
        if id_cols:
            dup_info = self.detect_duplicates(data, [id_cols[0]])
            uniqueness = 100 - dup_info['duplicate_rate']
            
        # 3. Consistência de Formato (Peso 30%)
        consistency = 0
        for col in columns:
            vals = [type(row.get(col)) for row in data if row.get(col) is not None]
            if vals:
                most_common_type_pct = (Counter(vals).most_common(1)[0][1] / len(vals)) * 100
                consistency += most_common_type_pct
        consistency /= len(columns)
        
        final_score = (completeness * 0.4) + (uniqueness * 0.3) + (consistency * 0.3)
        return {
            "score": round(final_score, 2),
            "metrics": {
                "completeness": round(completeness, 2),
                "uniqueness": round(uniqueness, 2),
                "consistency": round(consistency, 2)
            }
        }

    def analyze_lifecycle(self, data, date_columns):
        """Identifica o ciclo de vida e tempo de processamento."""
        if len(date_columns) < 2 or not data: return {}
        
        # Tentar identificar data de início e fim
        start_col = next((c for c in date_columns if any(k in c.upper() for k in ['CRIACAO', 'INICIO', 'CADASTRO', 'OPEN'])), date_columns[0])
        end_col = next((c for c in date_columns if any(k in c.upper() for k in ['FIM', 'ENTREGA', 'FECHAMENTO', 'CLOSE', 'FINALIZACAO'])), date_columns[-1])
        
        if start_col == end_col: return {}
        
        durations = []
        for row in data:
            d1 = row.get(start_col)
            d2 = row.get(end_col)
            if d1 and d2:
                try:
                    if isinstance(d1, str): d1 = datetime.strptime(d1[:10], '%Y-%m-%d')
                    if isinstance(d2, str): d2 = datetime.strptime(d2[:10], '%Y-%m-%d')
                    diff = (d2 - d1).days
                    if diff >= 0: durations.append(diff)
                except: continue
        
        if not durations: return {}
        
        return {
            "avg_days": round(sum(durations) / len(durations), 1),
            "min_days": min(durations),
            "max_days": max(durations),
            "median_days": sorted(durations)[len(durations)//2],
            "cycle_start": start_col,
            "cycle_end": end_col
        }

    def detect_advanced_anomalies(self, data, numeric_col):
        """Detecção de anomalias usando IQR e Z-Score."""
        values = [float(str(row.get(numeric_col)).replace(',', '.')) for row in data if row.get(numeric_col) is not None]
        if len(values) < 4: return []
        
        # 1. IQR (Interquartile Range) - Robusto para qualquer distribuição
        sorted_vals = sorted(values)
        q1 = sorted_vals[int(len(sorted_vals) * 0.25)]
        q3 = sorted_vals[int(len(sorted_vals) * 0.75)]
        iqr = q3 - q1
        lower_bound = q1 - 1.5 * iqr
        upper_bound = q3 + 1.5 * iqr
        
        # 2. Z-Score
        avg = statistics.mean(values)
        std_dev = statistics.stdev(values) if len(values) > 1 else 0
        
        anomalies = []
        for i, val in enumerate(values):
            is_iqr_outlier = val < lower_bound or val > upper_bound
            z_score = (val - avg) / std_dev if std_dev > 0 else 0
            is_z_outlier = abs(z_score) > 3
            
            if is_iqr_outlier or is_z_outlier:
                anomalies.append({
                    "index": i,
                    "value": val,
                    "method": "IQR" if is_iqr_outlier and not is_z_outlier else ("Z-SCORE" if is_z_outlier and not is_iqr_outlier else "BOTH"),
                    "severity": "High" if abs(z_score) > 5 else "Medium"
                })
        return anomalies[:20] # Limitar a 20 anomalias

    def temporal_trends(self, data, date_column):
        """Análise temporal com detecção de sazonalidade e crescimento."""
        if not data or not date_column: return {}
            
        by_month = Counter()
        for row in data:
            dt = row.get(date_column)
            if dt:
                try:
                    if isinstance(dt, str): dt_obj = datetime.strptime(dt[:10], '%Y-%m-%d')
                    else: dt_obj = dt
                    by_month[dt_obj.strftime('%Y-%m')] += 1
                except: continue
                
        sorted_months = dict(sorted(by_month.items()))
        
        # Calcular crescimento mês a mês
        trends = []
        months = list(sorted_months.keys())
        for i in range(1, len(months)):
            m1, m2 = months[i-1], months[i]
            v1, v2 = sorted_months[m1], sorted_months[m2]
            growth = ((v2 - v1) / v1 * 100) if v1 > 0 else 0
            trends.append({"period": m2, "growth": round(growth, 1)})
            
        return {
            "monthly_volume": sorted_months,
            "growth_trends": trends[-6:], # Últimos 6 meses
            "peak_period": max(sorted_months, key=sorted_months.get) if sorted_months else None
        }

    def automatic_segmentation(self, data, value_col):
        """Segmentação dinâmica baseada em quartis (Universal)."""
        values = [float(str(row.get(value_col)).replace(',', '.')) for row in data if row.get(value_col) is not None]
        if len(values) < 4: return {}
        
        sorted_vals = sorted(values)
        q1 = sorted_vals[int(len(sorted_vals) * 0.25)]
        q2 = sorted_vals[int(len(sorted_vals) * 0.50)]
        q3 = sorted_vals[int(len(sorted_vals) * 0.75)]
        
        segments = Counter()
        for v in values:
            if v <= q1: segments['Bronze (Q1)'] += 1
            elif v <= q2: segments['Silver (Q2)'] += 1
            elif v <= q3: segments['Gold (Q3)'] += 1
            else: segments['Platinum (Q4)'] += 1
            
        return {
            "distribution": dict(segments),
            "thresholds": {"Q1": q1, "Q2": q2, "Q3": q3}
        }

    def geographic_analysis(self, data, phone_col=None, city_col=None):
        """Análise geográfica baseada em DDD ou Cidade/UF."""
        geo_dist = Counter()
        for row in data:
            # 1. Tentar por Telefone (DDD)
            if phone_col and row.get(phone_col):
                phone = re.sub(r'\D', '', str(row.get(phone_col, '')))
                if len(phone) >= 2:
                    state = self.geo_map.get(phone[:2])
                    if state:
                        geo_dist[state] += 1
                    else:
                        geo_dist['Outros'] += 1
            
            # 2. Tentar por Cidade/UF
            if city_col and row.get(city_col):
                city_raw = str(row.get(city_col, '')).strip().upper()
                if city_raw and city_raw != 'NONE' and city_raw != 'NAN':
                    # Tentar extrair UF se estiver no formato "CIDADE - UF" ou "CIDADE/UF"
                    uf_match = re.search(r'[\s/-]([A-Z]{2})$', city_raw)
                    if uf_match:
                        geo_dist[uf_match.group(1)] += 1
                    else:
                        geo_dist[city_raw] += 1
        
        return dict(geo_dist.most_common(10))

    def compliance_audit(self, data, sensitive_cols):
        """Auditoria de compliance (PII e campos sensíveis)."""
        findings = []
        for col in sensitive_cols:
            exposed = [i for i, row in enumerate(data) if row.get(col)]
            if exposed:
                findings.append({
                    "column": col,
                    "exposed_count": len(exposed),
                    "risk": "High" if any(k in col.upper() for k in ['CPF', 'CNPJ', 'SENHA', 'VALOR', 'VL_', 'SALARIO']) else "Medium"
                })
        return findings
