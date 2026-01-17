
import random
import json
from typing import List, Dict, Any
from ..ENGINE.vector_manager import VectorManager

class SyntheticPatternGenerator:
    """
    Gerador Massivo de Padrões Sintéticos.
    Cria milhares de comportamentos baseados na estrutura real das tabelas.
    """
    
    def __init__(self, storage):
        self.storage = storage
        self.vector_manager = VectorManager()

    def generate_for_table(self, table_name: str, profile: Dict[str, Any], columns: List[Dict], sample_data: List[Dict], limit_per_type: int = 5000, progress_callback=None):
        """
        Gera e salva padrões para uma tabela específica.
        Aumentado para suportar geração massiva de comportamentos com vetorização.
        """
        print(f"--- Gerando Padrões Sintéticos (Novo Modelo de Aprendizado) para {table_name} ---")
        patterns = []
        
        # 1. Extrair valores reais da amostra para tornar as perguntas realistas
        col_values = {}
        for col in columns:
            name = col['name']
            values = list(set([str(row.get(name)) for row in sample_data if row.get(name)]))
            if values:
                col_values[name] = values

        # Estimar total de padrões para a barra de progresso
        total_to_generate = 0
        for values in col_values.values():
            total_to_generate += len(values[:limit_per_type])
        
        if 'EMAIL' in col_values:
            total_to_generate += min(len(col_values['EMAIL']) * 2, limit_per_type)
        
        total_to_generate += 5 # Count templates
        
        current_count = 0
        import time
        start_time = time.time()

        def update_progress(msg):
            nonlocal current_count
            current_count += 1
            if progress_callback and current_count % 10 == 0:
                elapsed = time.time() - start_time
                avg_time = elapsed / current_count if current_count > 0 else 0
                remaining = (total_to_generate - current_count) * avg_time
                progress_callback(current_count, total_to_generate, msg, remaining)

        # 2. Gerar Padrões de Busca Simples (LIKE)
        for col_name, values in col_values.items():
            if len(patterns) >= 50000: break
            
            for val in values[:limit_per_type]:
                templates = [
                    f"quem é {val}",
                    f"buscar {col_name} {val}",
                    f"onde está {val}",
                    f"qual o {col_name} de {val}",
                    f"me mostre os dados de {val}",
                    f"encontre {val} na {table_name}",
                    f"procure por {val}",
                    f"detalhes de {val}",
                    f"localizar {val}",
                    f"quem seria {val}?",
                    f"dados cadastrais de {val}"
                ]
                
                query_plan = {
                    "action": "DATA_ANALYSIS",
                    "plan": {
                        "type": "SELECT",
                        "table": table_name,
                        "fields": [c['name'] for c in columns[:10]],
                        "filters": [{"field": col_name, "op": "LIKE", "value": f"%{val}%", "case_insensitive": True}]
                    }
                }
                
                user_input = random.choice(templates)
                update_progress(f"Vetorizando: {user_input[:30]}...")
                
                # VETORIZAÇÃO OBRIGATÓRIA (Sem Fallback conforme pedido)
                vector = self.vector_manager.generate_embedding(user_input)
                if not vector:
                    continue
                    
                patterns.append({
                    "situation": f"Busca direta por {col_name}",
                    "user_input": user_input,
                    "ai_action": "DATA_ANALYSIS",
                    "ai_response": json.dumps(query_plan),
                    "category": "Busca Direta",
                    "tags": f"{table_name}, {col_name}, sintético",
                    "embedding_vector": self.vector_manager.vector_to_blob(vector)
                })

        # 3. Gerar Padrões de Busca com Exclusão (NOT LIKE)
        if 'EMAIL' in col_values:
            email_values = col_values['EMAIL']
            other_cols = [c for c in col_values.keys() if c != 'EMAIL']
            
            for _ in range(min(len(email_values) * 2, limit_per_type)):
                col_ref = random.choice(other_cols) if other_cols else list(col_values.keys())[0]
                val_ref = random.choice(col_values[col_ref])
                
                exclude_domains = ["ROHDEN", "GMAIL", "OUTLOOK", "HOTMAIL", "TERRA", "YAHOO"]
                domain = random.choice(exclude_domains)
                
                excl_templates = [
                    f"qual o email de {val_ref} que não seja {domain.lower()}",
                    f"buscar email de {val_ref} exceto {domain.lower()}",
                    f"contato de {val_ref} sem ser do {domain.lower()}",
                    f"me dê o email de {val_ref} (filtrar fora {domain.lower()})",
                    f"quero o email não corporativo de {val_ref} ({domain.lower()})"
                ]
                
                user_input = random.choice(excl_templates)
                update_progress(f"Vetorizando filtro: {user_input[:30]}...")
                
                # VETORIZAÇÃO OBRIGATÓRIA (Sem Fallback)
                vector = self.vector_manager.generate_embedding(user_input)
                if not vector:
                    continue
                
                patterns.append({
                    "situation": f"Busca com filtro de exclusão de domínio ({domain})",
                    "user_input": user_input,
                    "ai_action": "DATA_ANALYSIS",
                    "ai_response": json.dumps(query_plan),
                    "category": "Busca Filtrada",
                    "tags": f"{table_name}, exclusao, email, sintético",
                    "embedding_vector": self.vector_manager.vector_to_blob(vector)
                })

        # 4. Gerar Padrões de Agregação (COUNT)
        count_templates = [
            f"quantos {table_name} existem",
            f"total de {table_name}",
            f"contar {table_name}",
            f"qual a quantidade de registros em {table_name}",
            f"me diga o número total de {table_name}"
        ]
        
        for user_input in count_templates:
            update_progress(f"Vetorizando contagem...")
            # VETORIZAÇÃO OBRIGATÓRIA
            vector = self.vector_manager.generate_embedding(user_input)
            if not vector:
                continue

            patterns.append({
                "situation": f"Contagem total de registros em {table_name}",
                "user_input": user_input,
                "ai_action": "DATA_ANALYSIS",
                "ai_response": json.dumps({
                    "action": "DATA_ANALYSIS",
                    "plan": {"type": "SELECT", "table": table_name, "aggregations": [{"func": "COUNT", "field": "*"}]}
                }),
                "category": "Pergunta de Dados",
                "tags": f"{table_name}, count, sintético",
                "embedding_vector": self.vector_manager.vector_to_blob(vector)
            })

        # 5. Salvar em lote para performance
        if patterns:
            print(f"Salvando {len(patterns)} novos padrões sintéticos vetorizados no banco...")
            self.storage.batch_save_behavioral_patterns(patterns)
            print("✅ Padrões sintéticos vetorizados salvos com sucesso.")
        
        return len(patterns)
