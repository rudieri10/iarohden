import re
import json
import os
import time
from collections import Counter
from datetime import datetime

from .analise_temporal import DataInsightEngine
from .ai_client import AIClient

class TrainingProfiler:
    """
    Sistema de Deep Profiling Híbrido.
    Processa volumes com estatística e extrai significado via IA.
    """
    
    def __init__(self):
        self.data_engine = DataInsightEngine()
        self.ai_client = AIClient()

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

    def _analyze_deep_with_ia(self, table_name, stats_profile, table_purpose=None, columns_not_null=None):
        """IA analisa as estatísticas por lotes."""
        if not stats_profile:
             return {"columns": {}, "table_level_insights": [], "suggested_business_rules": []}

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

            prompt = f"""Analise a coluna '{list(batch_dict.keys())[0]}' da tabela '{table_name}'.

DADOS ESTATÍSTICOS:
{json.dumps(batch_dict, indent=2, ensure_ascii=False)}

REGRAS ABSOLUTAS - NUNCA VIOLE:

1. ❌ PROIBIDO gerar regras com valores específicos inventados
   Exemplo ERRADO: "ID_CONTATO IN ('5250', '5251', '5252')"
   Por quê: Você não conhece os valores reais

2. ❌ PROIBIDO gerar regras com datas fixas ou ranges de datas
   Exemplo ERRADO: "DT_CADASTRO BETWEEN '2025-12-02' AND '2025-12-04'"
   Por quê: Datas mudam constantemente

3. ❌ PROIBIDO gerar regras NOT NULL se há valores NULL nos dados
   Verifique: Se "nulls" > 0 nos stats, NÃO gere NOT NULL

4. ❌ PROIBIDO gerar regras UNIQUE se há duplicatas
   Verifique: Se cardinality < total, há duplicatas

5. ✅ PERMITIDO apenas:
   - NOT NULL (se nulls = 0)
   - CHECK com IN ('A', 'B') (se cardinality < 10 e valores são categóricos)
   - CHECK com REGEXP_LIKE para formatos (email, telefone)
   - CHECK com ranges genéricos (valor > 0)

SINTAXE ORACLE OBRIGATÓRIA:

✅ REGEXP_LIKE é função:
   CHECK (REGEXP_LIKE(EMAIL, '^[A-Z0-9._%+-]+@[A-Z0-9.-]+\\.[A-Z]{{2,}}$', 'i'))

✅ NOT NULL (Use MODIFY com parênteses):
   ALTER TABLE {table_name} MODIFY ({list(batch_dict.keys())[0]} NOT NULL)

✅ CHECK com IN:
   ALTER TABLE {table_name} ADD CONSTRAINT CK_NOME CHECK (coluna IN ('S', 'N'))

❌ NUNCA use:
   - coluna REGEXP_LIKE 'padrão'  ← SINTAXE INVÁLIDA
   - MODIFY COLUMN coluna          ← COLUMN é desnecessário
   - MODIFY coluna NOT NULL        ← Use MODIFY (coluna NOT NULL)
   - Múltiplas ações no mesmo ALTER TABLE
   - Valores numéricos específicos em IN (...)

FORMATO JSON (Obrigatório):
{{
    "columns": {{
        "{list(batch_dict.keys())[0]}": {{
            "business_purpose": "Propósito funcional em 1 frase",
            "classification": "IDENTIFICADOR|CADASTRO|FINANCEIRO|CONTROLE|TEXTO",
            "insights": ["Observação sobre distribuição"]
        }}
    }},
    "table_level_insights": [],
    "suggested_business_rules": [
        {{"description": "Descrição clara", "sql_rule": "SQL OBRIGATÓRIO para regras simples (NOT NULL, CHECK)"}}
    ]
}}

IMPORTANTE: SEMPRE gere SQL para regras de NOT NULL, UNIQUE e CHECK simples.
"""

            try:
                batch_data = self.ai_client.generate_json(prompt)
                
                if not batch_data:
                    print("\n   ⚠️ IA não retornou dados válidos para este lote.")
                    continue

                print(f"\n   🔍 DEBUG AI RAW: {batch_data.get('suggested_business_rules', '[]')}")
                final_analysis["columns"].update(batch_data.get("columns", {}))
                final_analysis["table_level_insights"].extend(batch_data.get("table_level_insights", []))
                
                # Filter out duplicate PRIMARY KEY rules to prevent ORA-02260
                new_rules = batch_data.get("suggested_business_rules", [])
                existing_rules = final_analysis["suggested_business_rules"]
                
                for rule in new_rules:
                    # Check if rule is a PRIMARY KEY definition
                    is_pk = "PRIMARY KEY" in rule.get('sql_rule', '').upper()
                    # Check if we already have a PRIMARY KEY rule
                    has_pk = any("PRIMARY KEY" in r.get('sql_rule', '').upper() for r in existing_rules)
                    
                    if not (is_pk and has_pk):
                        existing_rules.append(rule)
                        
                print("] OK")
            except Exception as e:
                print(f"] ❌ Erro ao processar resposta da IA: {e}")

        return final_analysis

    def _detect_table_purpose(self, table_name, column_names):
        """Detecta propósito da tabela via IA."""
        prompt = f"Analise a tabela {table_name} e colunas {', '.join(column_names[:100])}. Responda em JSON: {{\"summary\": \"...\", \"business_process\": \"...\", \"stakeholders\": [], \"details\": \"...\"}}"
        try:
            print(f"   🔍 Identificando {table_name}...", end="", flush=True)
            result = self.ai_client.generate_json(prompt, timeout=600)
            print(" OK")
            return result
        except: return {}

    def profile_table(self, table_name, columns_data):
        """Gera o perfil completo da tabela."""
        print(f"🔍 Profiling {table_name}...")
        purpose = self._detect_table_purpose(table_name, list(columns_data.keys()))
        stats_profile = {name: self.analyze_column_stats(name, vals) for name, vals in columns_data.items()}
        
        # Preparar informações de colunas já NOT NULL
        columns_not_null = []
        for col_name, col_stats in stats_profile.items():
            if col_stats and isinstance(col_stats, dict) and col_stats.get("stats", {}).get("nulls", 0) == 0:
                columns_not_null.append(col_name)
        
        ia_intel = self._analyze_deep_with_ia(table_name, stats_profile, purpose, columns_not_null)

        full_profile = {
            "table_name": table_name,
            "timestamp": datetime.now().isoformat(),
            "purpose": purpose,
            "columns": {name: {**stats, "intelligence": ia_intel.get("columns", {}).get(name, {})} for name, stats in stats_profile.items() if stats},
            # Desduplicação segura para insights (que podem ser dicts ou strs)
            "table_insights": [json.loads(x) for x in {json.dumps(d, sort_keys=True) for d in ia_intel.get("table_level_insights", []) if d}],
            # Desduplicação manual para regras de negócio
            "business_rules": [json.loads(x) for x in {json.dumps(d, sort_keys=True) for d in ia_intel.get("suggested_business_rules", []) if d}]
        }
        return full_profile

    def generate_markdown_report(self, profile):
        """Relatório Markdown."""
        p = profile.get("purpose", {})
        md = [f"# 📊 Profile: {profile.get('table_name')}", f"Gerado: {profile.get('timestamp')}\n"]
        md.append(f"### 🎯 Propósito\n**Resumo:** {p.get('summary')}\n**Processo:** {p.get('business_process')}\n")
        
        # Tratamento robusto para insights (podem ser strings ou dicts)
        insights_list = []
        for i in profile.get("table_insights", []):
            if isinstance(i, dict):
                insights_list.append(i.get('insight', i.get('description', json.dumps(i, ensure_ascii=False))))
            else:
                insights_list.append(str(i))
        
        if insights_list:
            md.append("## 💡 Insights\n" + "\n".join([f"- {i}" for i in insights_list]))
            
        md.append("\n## 📋 Colunas")
        for name, data in profile["columns"].items():
            intel, stats = data.get("intelligence", {}), data.get("stats", {})
            md.append(f"### 🔹 {name} ({intel.get('classification', 'DADO')})\n**Fim:** {intel.get('business_purpose')}")
            md.append(f"- Completude: {stats.get('completeness_pct')}% | Cardinalidade: {stats.get('cardinality')}")
        return "\n".join(md)

class ProcessProfiler:
    """Analista de Processos via IA."""
    def __init__(self):
        self.ai_client = AIClient()

    def discover_flow(self, tables_meta):
        """Mapeia fluxos entre tabelas."""
        sums = [{"table": m['table_name'], "purpose": m.get('table_description', '')[:200]} for m in tables_meta]
        prompt = f"Analise as tabelas e mapeie o fluxo de negócio em JSON: {json.dumps(sums, ensure_ascii=False)}"
        try:
            print(f"   🤖 Mapeando processos...", end="", flush=True)
            result = self.ai_client.generate_json(prompt)
            print(" OK")
            return result
        except: return {"flow": [], "relationships": [], "cascades": [], "movements": [], "table_info": {}}

    def analyze_timing(self, flow, context=None):
        return {"critical_path": "Lógico", "bottlenecks": [], "frequency": "Transacional"}

    def generate_process_summary(self, name, flow, timing, relationships, cascades, table_info, business_rules, movements):
        md = [f"# 🚀 Process Map: {name}", f"Gerado: {datetime.now().strftime('%d/%m/%Y %H:%M')}\n", "## 📈 Workflow"]
        md.extend([f"{s.get('step')}. **{s.get('table')}**: {s.get('description')}" if isinstance(s, dict) else f"- {s}" for s in flow])
        md.append("\n## 🔗 Relacionamentos")
        md.extend([f"- `{r.get('from')}` -> `{r.get('to')}` ({r.get('type')})" for r in relationships])
        return "\n".join(md)
