import json
from .profiler import TrainingProfiler, ProcessProfiler
from ..DATA.storage import DataStorage
from ..ENGINE.vector_manager import VectorManager

class TableTrainer:
    """
    Orquestrador de treinamento.
    Coordena a análise estatística, IA e o salvamento dos metadados.
    """
    
    def __init__(self):
        self.profiler = TrainingProfiler()
        self.process_profiler = ProcessProfiler()
        self.storage = DataStorage()
        self.vector_manager = VectorManager()

    def train_table(self, table_name, columns, sample_data, total_records, progress_callback=None):
        """
        Executa o fluxo de treinamento profundo baseado em dados reais.
        """
        def update_p(msg, percent):
            if progress_callback:
                progress_callback(percent, msg)

        print(f"Iniciando treinamento IA para {table_name}...")
        update_p(f"Analisando estrutura de {table_name}...", 5)
        
        # Preparar dados para o profiler
        columns_data = {col['name']: [row.get(col['name']) for row in sample_data] for col in columns}

        # 1. Gerar profile profundo (Estatística + IA)
        profile = self.profiler.profile_table(table_name, columns_data)
        update_p("Gerando insights e perfil IA...", 15)
        
        # 2. Gerar resumo legível dinâmico
        advanced_description = self.profiler.generate_markdown_report(profile)
        update_p("Preparando contexto semântico...", 20)
        
        # 3. Gerar Embeddings (Vetorização para Busca Semântica)
        update_p("Construindo base de conhecimento vetorial...", 20)
        
        # Iniciar construção do texto semântico (Foco em Estrutura + 15 Exemplos Reais)
        semantic_parts = [f"TABELA: {table_name}"]
        
        purpose = profile.get('purpose', {})
        if purpose:
            semantic_parts.append(f"PROPÓSITO: {purpose.get('summary', '')}")
            semantic_parts.append(f"DESCRIÇÃO: {purpose.get('details', '')}")
            semantic_parts.append(f"PROCESSO DE NEGÓCIO: {purpose.get('business_process', '')}")

        semantic_parts.append("ESTRUTURA E EXEMPLOS:")
        for col_name, col_data in profile.get('columns', {}).items():
            intel = col_data.get('intelligence', {})
            samples = col_data.get('samples', [])
            
            col_info = f"- COLUNA: {col_name} ({intel.get('classification', 'DADO')})"
            if intel.get('business_purpose'):
                col_info += f" | FIM: {intel['business_purpose']}"
            
            if samples:
                examples_str = ", ".join([str(s) for s in samples[:15]])
                col_info += f" | EXEMPLOS: {examples_str}"
            
            semantic_parts.append(col_info)

        if profile.get('table_insights'):
            semantic_parts.append("INSIGHTS DE NEGÓCIO: " + " ".join(profile['table_insights']))
            
        if profile.get('business_rules'):
            # Extrair texto das regras se forem dicionários
            rules_text = []
            for r in profile['business_rules']:
                if isinstance(r, dict):
                    rules_text.append(r.get('rule', str(r)))
                else:
                    rules_text.append(str(r))
            semantic_parts.append("REGRAS DETECTADAS: " + " ".join(rules_text))

        semantic_text = "\n".join(semantic_parts)
        
        vector_blob = None
        vector_success = False
        
        texts_to_try = [
            semantic_text, 
            f"Tabela {table_name}. Propósito: {purpose.get('summary', '')}. Colunas: {', '.join([c['name'] for c in columns])}", 
            f"Tabela {table_name}. Colunas: {', '.join([c['name'] for c in columns])}"
        ]

        for i, text in enumerate(texts_to_try):
            try:
                update_p(f"Vetorizando base de dados (Tentativa {i+1}/3)...", 25 + (i*5))
                vector = self.vector_manager.generate_embedding(text)
                if vector:
                    vector_blob = self.vector_manager.vector_to_blob(vector)
                    vector_success = True
                    break
            except Exception as e:
                print(f"Falha na tentativa {i+1} para {table_name}: {e}")

            if i < len(texts_to_try) - 1:
                import time
                time.sleep(5)
        
        if not vector_success:
            raise Exception(f"ERRO: Falha na vetorização de {table_name}.")

        update_p("Salvando metadados enriquecidos...", 45)
        
        # 4. Preparar metadados enriquecidos
        existing_meta = self.storage.get_table_metadata(table_name) or {}
        enriched_columns = []
        for col in columns:
            col_name = col['name']
            enriched_col = col.copy()
            if col_name in profile.get('columns', {}):
                col_profile = profile['columns'][col_name]
                intel = col_profile.get('intelligence', {})
                enriched_col.update({
                    'semantic_type': intel.get('classification'),
                    'business_purpose': intel.get('business_purpose'),
                    'detected_rules': intel.get('detected_rules', []),
                    'examples': col_profile.get('samples', [])[:15]
                })
            enriched_columns.append(enriched_col)

        table_meta = {
            'table_name': table_name,
            'table_description': advanced_description,
            'schema_info': existing_meta.get('schema_info', {}),
            'columns_info': enriched_columns,
            'sample_data': sample_data[:50] if sample_data else [],
            'record_count': total_records,
            'is_active': True,
            'deep_profile': profile,
            'semantic_context': existing_meta.get('semantic_context', []),
            'embedding_vector': vector_blob,
            'export_status': 'Sucesso'
        }
        
        self.storage.save_tables([table_meta])
        
        # VALIDAÇÃO AUTOMÁTICA DE REGRAS DE NEGÓCIO
        if profile.get('business_rules'):
            try:
                from .rule_validator import RuleValidator
                print(f"\n🔍 Validando {len(profile['business_rules'])} regras de negócio...")
                validator = RuleValidator()
                validated_rules = validator.validate_table_rules(
                    table_name, 
                    profile['business_rules']
                )
                # Atualizar metadados com regras validadas
                table_meta['validated_rules'] = validated_rules
                self.storage.save_table_metadata(table_name, table_meta)
                print(f"✅ Validação concluída!\n")
            except Exception as e:
                print(f"⚠️ Erro na validação de regras: {e}")
        
        update_p("Treinamento concluído!", 100)

        return {
            'success': True,
            'vector_success': vector_success,
            'profile': profile,
            'summary': advanced_description
        }

    def train_processes(self):
        """
        Analisa o banco de dados como um todo para descobrir fluxos de processos.
        Deve ser chamado após o treinamento individual das tabelas.
        """
        print("Iniciando treinamento de PROCESSOS e FLUXOS...")
        
        # 1. Carregar metadados das tabelas já treinadas
        tables_meta = self.storage.load_tables()
        
        # 2. Descobrir fluxos e relacionamentos
        discovery = self.process_profiler.discover_flow(tables_meta)
        flow = discovery.get('flow', [])
        relationships = discovery.get('relationships', [])
        cascades = discovery.get('cascades', [])
        table_info = discovery.get('table_info', {})
        movements = discovery.get('movements', [])
        
        # Coletar regras de negócio dos perfis das tabelas
        business_rules = {}
        for meta in tables_meta:
            profile = meta.get('deep_profile', {})
            rules = profile.get('business_rules', [])
            if rules:
                business_rules[meta['table_name']] = rules

        timing_insights = self.process_profiler.analyze_timing(flow, None)
        
        # 3. Gerar resumo do processo (Incluindo relacionamentos, cascatas, histórico, regras e fluxos)
        process_summary = self.process_profiler.generate_process_summary(
            "Ciclo Comercial Rohden", 
            flow, 
            timing_insights, 
            relationships,
            cascades,
            table_info,
            business_rules,
            movements
        )
        
        # 4. Salvar o fluxo descoberto
        self.storage.save_process_flow(
            "Ciclo Comercial Rohden",
            flow,
            timing_insights,
            process_summary
        )
        
        return {
            'success': True,
            'process_name': "Ciclo Comercial Rohden",
            'summary': process_summary
        }
