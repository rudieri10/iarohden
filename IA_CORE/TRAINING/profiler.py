import re
import json
import os
import requests
import time
from collections import Counter
from datetime import datetime

from .analise_temporal import DataInsightEngine

class TrainingProfiler:
    """
    Sistema de Deep Profiling Híbrido.
    Processa volumes com estatística e extrai significado via IA.
    """
    
    def __init__(self):
        self.data_engine = DataInsightEngine()
        self.ai_url = os.getenv("ROHDEN_AI_INTERNAL_URL", "http://192.168.1.217:11434/api/generate")
        self.ai_model = os.getenv("ROHDEN_AI_MODEL", "qwen2.5:3b")
        
        self.session = requests.Session()
        self.session.trust_env = False
        self.session.headers.update({
            "Content-Type": "application/json",
            "Connection": "keep-alive"
        })

    def analyze_column_stats(self, name, values):
        """Analisa estatísticas de uma coluna."""
        total_count = len(values)
        if total_count == 0: return None

        non_null_values = [v for v in values if v is not None and str(v).strip() != '']
        null_count = total_count - len(non_null_values)
        
        unique_values = set(non_null_values)
        cardinality = len(unique_values)

        distribution = Counter(non_null_values).most_common(20)
        distribution_pct = [
            {"val": str(val), "count": count, "pct": round((count/total_count)*100, 2)} 
            for val, count in distribution
        ]

        return {
            "name": name,
            "stats": {
                "total": total_count,
                "nulls": null_count,
                "completeness_pct": round((len(non_null_values) / total_count) * 100, 2),
                "cardinality": cardinality,
                "cardinality_pct": round((cardinality / total_count) * 100, 2),
            },
            "distribution": distribution_pct,
            "samples": [str(v) for v in list(unique_values)[:15]]
        }

    def _analyze_deep_with_ia(self, table_name, stats_profile, table_purpose=None):
        """IA analisa as estatísticas por lotes."""
        context_str = f"CONTEXTO: {table_purpose.get('summary')}" if table_purpose else ""
        BATCH_SIZE = 1 
        stats_items = list(stats_profile.items())
        num_cols = len(stats_items)
        
        final_analysis = {"columns": {}, "table_level_insights": [], "suggested_business_rules": []}
        total_batches = (num_cols + BATCH_SIZE - 1) // BATCH_SIZE
        start_time_all = time.time()
        
        for i in range(0, num_cols, BATCH_SIZE):
            batch_dict = {k: v for k, v in stats_items[i:i + BATCH_SIZE]}
            current_batch_num = (i // BATCH_SIZE) + 1
            
            elapsed = time.time() - start_time_all
            eta = (elapsed / (current_batch_num - 1)) * (total_batches - (current_batch_num - 1)) if current_batch_num > 1 else 0
            
            print(f"\n   👉 [{current_batch_num}/{total_batches}] Analisando: {', '.join(batch_dict.keys())} (Restam {int(eta)}s)")
            print(f"      [", end="", flush=True)

            prompt = f"""Analise as estatísticas da coluna '{list(batch_dict.keys())[0]}' da tabela '{table_name}'. {context_str}
            
            DADOS ESTATÍSTICOS:
            {json.dumps(batch_dict, indent=2, ensure_ascii=False)}

            OBJETIVO: Identificar regras de negócio, propósito e validações lógicas para esta coluna.
            Importante: Extraia regras que possam ser validadas via SQL (ex: NOT NULL, valores específicos, ranges).

            FORMATO DE RESPOSTA JSON (Obrigatório):
            {{
                "columns": {{
                    "{list(batch_dict.keys())[0]}": {{
                        "business_purpose": "Descrição sucinta do propósito funcional",
                        "classification": "Tipo de dado (ex: IDENTIFICADOR, CADASTRO, FINANCEIRO, CONTROLE)",
                        "insights": ["Insight relevante sobre a distribuição de dados"]
                    }}
                }},
                "table_level_insights": [],
                "suggested_business_rules": [
                    "Exemplo: A coluna {list(batch_dict.keys())[0]} não deve conter valores nulos (se nulls=0)",
                    "Exemplo: A coluna {list(batch_dict.keys())[0]} deve ter apenas valores X, Y (se for flag)"
                ]
            }}
            """

            try:
                response = self.session.post(self.ai_url, json={"model": self.ai_model, "prompt_template": "chat", "prompt": prompt, "stream": True, "format": "json"}, timeout=(5, 120), stream=True)
                
                batch_text = ""
                last_act = time.time()
                dots = 0
                for line in response.iter_lines():
                    if line:
                        last_act = time.time()
                        try:
                            chunk = json.loads(line.decode('utf-8'))
                            text = chunk.get('response', '')
                            batch_text += text
                            # print(f"DEBUG CHUNK: {text}", end="", flush=True) # Uncomment for detailed trace
                        except:
                            continue
                        
                        if text and dots < 20: 
                            print("■", end="", flush=True)
                            dots += 1
                        if chunk.get("done"): break
                    
                    if time.time() - last_act > 60: 
                        print(" [TIMEOUT NO STREAM] ", end="")
                        break
                
                while dots < 20: print("■", end="", flush=True); dots += 1
                
                batch_data = json.loads(batch_text)
                print(f"\n   🔍 DEBUG AI RAW: {batch_data.get('suggested_business_rules', '[]')}")
                final_analysis["columns"].update(batch_data.get("columns", {}))
                final_analysis["table_level_insights"].extend(batch_data.get("table_level_insights", []))
                final_analysis["suggested_business_rules"].extend(batch_data.get("suggested_business_rules", []))
                print("] OK")
            except Exception as e:
                print(f"\n   ❌ DEBUG RESPONSE: {batch_text}")
                print(f"] ❌ Erro: {e}")

        return final_analysis

    def _detect_table_purpose(self, table_name, column_names):
        """Detecta propósito da tabela via IA."""
        prompt = f"Analise a tabela {table_name} e colunas {', '.join(column_names[:100])}. Responda em JSON: {{\"summary\": \"...\", \"business_process\": \"...\", \"stakeholders\": [], \"details\": \"...\"}}"
        try:
            response = self.session.post(self.ai_url, json={"model": self.ai_model, "prompt": prompt, "stream": True, "format": "json"}, timeout=(5, 600), stream=True)
            full_res = ""
            print(f"   🔍 Identificando {table_name}: [", end="", flush=True)
            dots = 0
            for line in response.iter_lines():
                if line:
                    chunk = json.loads(line.decode('utf-8'))
                    text = chunk.get('response', '')
                    full_res += text
                    if text and dots < 20 and len(full_res) % 15 == 0:
                        print("■", end="", flush=True); dots += 1
                    if chunk.get("done"): break
            while dots < 20: print("■", end="", flush=True); dots += 1
            print("] OK")
            return json.loads(full_res)
        except: return {}

    def profile_table(self, table_name, columns_data):
        """Gera o perfil completo da tabela."""
        print(f"🔍 Profiling {table_name}...")
        purpose = self._detect_table_purpose(table_name, list(columns_data.keys()))
        stats_profile = {name: self.analyze_column_stats(name, vals) for name, vals in columns_data.items()}
        ia_intel = self._analyze_deep_with_ia(table_name, stats_profile, purpose)

        full_profile = {
            "table_name": table_name,
            "timestamp": datetime.now().isoformat(),
            "purpose": purpose,
            "columns": {name: {**stats, "intelligence": ia_intel.get("columns", {}).get(name, {})} for name, stats in stats_profile.items()},
            # Desduplicação segura para insights (que podem ser dicts ou strs)
            "table_insights": [json.loads(x) for x in {json.dumps(d, sort_keys=True) for d in ia_intel.get("table_level_insights", [])}],
            # Desduplicação manual para regras de negócio
            "business_rules": [json.loads(x) for x in {json.dumps(d, sort_keys=True) for d in ia_intel.get("suggested_business_rules", [])}]
        }
        return full_profile

    def generate_markdown_report(self, profile):
        """Relatório Markdown."""
        p = profile.get("purpose", {})
        md = [f"# 📊 Profile: {profile.get('table_name')}", f"Gerado: {profile.get('timestamp')}\n"]
        md.append(f"### 🎯 Propósito\n**Resumo:** {p.get('summary')}\n**Processo:** {p.get('business_process')}\n")
        md.append("## 💡 Insights\n" + "\n".join([f"- {i}" for i in profile.get("table_insights", [])]))
        md.append("\n## 📋 Colunas")
        for name, data in profile["columns"].items():
            intel, stats = data.get("intelligence", {}), data.get("stats", {})
            md.append(f"### 🔹 {name} ({intel.get('classification', 'DADO')})\n**Fim:** {intel.get('business_purpose')}")
            md.append(f"- Completude: {stats.get('completeness_pct')}% | Cardinalidade: {stats.get('cardinality')}")
        return "\n".join(md)

class ProcessProfiler:
    """Analista de Processos via IA."""
    def __init__(self):
        self.ai_url = os.getenv("ROHDEN_AI_INTERNAL_URL", "http://192.168.1.217:11434/api/generate")
        self.ai_model = os.getenv("ROHDEN_AI_MODEL", "qwen2.5:3b")
        self.session = requests.Session()
        self.session.trust_env = False
        self.session.headers.update({"Content-Type": "application/json", "Connection": "keep-alive"})

    def discover_flow(self, tables_meta):
        """Mapeia fluxos entre tabelas."""
        sums = [{"table": m['table_name'], "purpose": m.get('table_description', '')[:200]} for m in tables_meta]
        prompt = f"Analise as tabelas e mapeie o fluxo de negócio em JSON: {json.dumps(sums, ensure_ascii=False)}"
        try:
            response = self.session.post(self.ai_url, json={"model": self.ai_model, "prompt": prompt, "stream": True, "format": "json"}, timeout=(5, 600), stream=True)
            full_res, dots = "", 0
            print(f"   🤖 Mapeando processos: [", end="", flush=True)
            for line in response.iter_lines():
                if line:
                    chunk = json.loads(line.decode('utf-8'))
                    text = chunk.get('response', '')
                    full_res += text
                    if text and dots < 30 and len(full_res) % 40 == 0:
                        print("■", end="", flush=True); dots += 1
            while dots < 30: print("■", end="", flush=True); dots += 1
            print("] OK")
            return json.loads(full_res)
        except: return {"flow": [], "relationships": [], "cascades": [], "movements": [], "table_info": {}}

    def analyze_timing(self, flow, context=None):
        return {"critical_path": "Lógico", "bottlenecks": [], "frequency": "Transacional"}

    def generate_process_summary(self, name, flow, timing, relationships, cascades, table_info, business_rules, movements):
        md = [f"# 🚀 Process Map: {name}", f"Gerado: {datetime.now().strftime('%d/%m/%Y %H:%M')}\n", "## 📈 Workflow"]
        md.extend([f"{s.get('step')}. **{s.get('table')}**: {s.get('description')}" if isinstance(s, dict) else f"- {s}" for s in flow])
        md.append("\n## 🔗 Relacionamentos")
        md.extend([f"- `{r.get('from')}` -> `{r.get('to')}` ({r.get('type')})" for r in relationships])
        return "\n".join(md)
