import re
import json
import math
from collections import Counter, defaultdict
from datetime import datetime, timedelta

class TemporalAnalyzer:
    """
    Sistema avançado de análise temporal, estatística e preditiva.
    Implementa detecção de duplicatas, score de qualidade, segmentação e anomalias.
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
        """
        Detecção de duplicatas inteligente baseada em colunas chave.
        Retorna estatísticas de duplicidade.
        """
        if not data or not key_columns:
            return {"status": "skipped", "reason": "No data or keys"}

        seen = {}
        duplicates = []
        
        for i, row in enumerate(data):
            # Criar uma 'assinatura' do registro baseada nas colunas chave
            signature = tuple(str(row.get(col, '')).strip().upper() for col in key_columns if col in row)
            if not any(signature): continue # Pular se assinatura vazia
            
            if signature in seen:
                duplicates.append({
                    "original_index": seen[signature],
                    "duplicate_index": i,
                    "signature": signature
                })
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

    def analyze_consistency(self, data, rules):
        """
        Análise de consistência cruzada baseada em regras de negócio.
        Ex: Se tem VALOR_VENDA, deve ter DATA_VENDA.
        """
        inconsistencies = []
        for i, row in enumerate(data):
            for rule_name, rule_fn in rules.items():
                if not rule_fn(row):
                    inconsistencies.append({
                        "index": i,
                        "rule": rule_name,
                        "data": {k: row.get(k) for k in row.keys() if k in str(rule_fn)}
                    })
        
        return {
            "total_checked": len(data),
            "inconsistency_count": len(inconsistencies),
            "consistency_rate": round(((len(data) - len(inconsistencies)) / len(data)) * 100, 2) if data else 100,
            "samples": inconsistencies[:5]
        }

    def calculate_lead_quality(self, row, weight_config=None):
        """
        Calcula o Score de Qualidade do Lead (0-100).
        """
        if not weight_config:
            weight_config = {
                'has_email': 20,
                'has_phone': 20,
                'has_name': 20,
                'has_company': 15,
                'is_recent': 25
            }
        
        score = 0
        
        # 1. Email válido
        email = str(row.get('EMAIL', row.get('E_MAIL', ''))).strip()
        if '@' in email and '.' in email:
            score += weight_config['has_email']
            
        # 2. Telefone válido
        phone = re.sub(r'\D', '', str(row.get('CELULAR', row.get('TELEFONE', ''))))
        if len(phone) >= 10:
            score += weight_config['has_phone']
            
        # 3. Nome completo
        name = str(row.get('NOME', row.get('RAZAO_SOCIAL', ''))).strip()
        if len(name.split()) >= 2:
            score += weight_config['has_name']
            
        # 4. Empresa (B2B)
        company = str(row.get('EMPRESA', row.get('CLIENTE', ''))).strip()
        if company and company.upper() != 'CONSUMIDOR FINAL':
            score += weight_config['has_company']
            
        # 5. Recência (Simulado se não tiver data real no row individual aqui)
        # Em um contexto real, compararíamos DT_CADASTRO com data atual
        dt_cad = row.get('DT_CADASTRO', row.get('DATA', None))
        if dt_cad:
            try:
                # Tentar converter se for string
                if isinstance(dt_cad, str):
                    dt_cad = datetime.strptime(dt_cad[:10], '%Y-%m-%d')
                days_old = (datetime.now() - dt_cad).days
                if days_old < 30: score += weight_config['is_recent']
                elif days_old < 90: score += weight_config['is_recent'] * 0.5
            except: pass
            
        return score

    def temporal_trends(self, data, date_column):
        """
        Análise temporal básica: volume por período.
        """
        if not data or not date_column:
            return {}
            
        by_month = Counter()
        for row in data:
            dt = row.get(date_column)
            if dt:
                try:
                    if isinstance(dt, str):
                        dt_obj = datetime.strptime(dt[:10], '%Y-%m-%d')
                    else:
                        dt_obj = dt
                    month_key = dt_obj.strftime('%Y-%m')
                    by_month[month_key] += 1
                except: continue
                
        return dict(sorted(by_month.items()))

    def automatic_segmentation(self, data, value_col=None, freq_col=None):
        """
        Segmentação automática (tipo RFM simplificado).
        """
        segments = defaultdict(int)
        for row in data:
            val = float(row.get(value_col, 0)) if value_col else 0
            
            if val > 10000: segments['VIP'] += 1
            elif val > 1000: segments['Standard'] += 1
            else: segments['Low Value'] += 1
            
        return dict(segments)

    def detect_anomalies(self, data, numeric_col):
        """
        Detecção de anomalias (Outliers estatísticos).
        """
        values = [float(row.get(numeric_col, 0)) for row in data if row.get(numeric_col) is not None]
        if not values: return []
        
        avg = sum(values) / len(values)
        variance = sum((x - avg) ** 2 for x in values) / len(values)
        std_dev = math.sqrt(variance)
        
        anomalies = []
        for i, val in enumerate(values):
            if abs(val - avg) > (3 * std_dev): # 3 Sigma rule
                anomalies.append({"index": i, "value": val, "deviation": round((val-avg)/std_dev, 2)})
                
        return anomalies

    def geographic_analysis(self, data, phone_col=None, city_col=None):
        """
        Análise geográfica baseada em DDD ou Cidade/UF.
        """
        geo_dist = Counter()
        for row in data:
            if phone_col:
                phone = re.sub(r'\D', '', str(row.get(phone_col, '')))
                if len(phone) >= 2:
                    ddd = phone[:2]
                    state = self.geo_map.get(ddd, 'Outros')
                    geo_dist[state] += 1
            elif city_col:
                city = str(row.get(city_col, '')).upper()
                if city: geo_dist[city] += 1
                
        return dict(geo_dist.most_common(10))

    def compliance_audit(self, data, sensitive_cols):
        """
        Auditoria de compliance (PII e campos obrigatórios).
        """
        findings = []
        for col in sensitive_cols:
            exposed = [i for i, row in enumerate(data) if row.get(col)]
            if exposed:
                findings.append({
                    "column": col,
                    "exposed_count": len(exposed),
                    "risk": "High" if col in ['CPF', 'CNPJ', 'SENHA', 'VALOR'] else "Medium"
                })
        return findings

    def historical_comparison(self, current_data, previous_data, metric_fn):
        """
        Comparação histórica de métricas.
        """
        curr_val = metric_fn(current_data)
        prev_val = metric_fn(previous_data)
        
        diff = curr_val - prev_val
        growth = (diff / prev_val * 100) if prev_val != 0 else 0
        
        return {
            "current": curr_val,
            "previous": prev_val,
            "growth_pct": round(growth, 2)
        }
