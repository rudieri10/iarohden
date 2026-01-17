import json
from .profiler import TrainingProfiler, ProcessProfiler
from .synthetic_generator import SyntheticPatternGenerator
from ..DATA.storage import DataStorage
from ..ENGINE.vector_manager import VectorManager

class TableTrainer:
    """
    Orquestrador de treinamento sem IA.
    Coordena a análise estatística e o salvamento dos metadados.
    """
    
    def __init__(self):
        self.profiler = TrainingProfiler()
        self.process_profiler = ProcessProfiler()
        self.storage = DataStorage()
        self.vector_manager = VectorManager()
        self.synthetic_gen = SyntheticPatternGenerator(self.storage)

    def train_table(self, table_name, columns, sample_data, total_records, progress_callback=None):
        """
        Executa o fluxo de treinamento profundo.
        1. Analisa estatisticamente os dados.
        2. Gera insights automáticos.
        3. Salva no banco de dados Oracle.
        """
        def update_p(msg, percent):
            if progress_callback:
                progress_callback(percent, msg)

        print(f"Iniciando treinamento PROFUNDO para {table_name}...")
        update_p(f"Analisando estrutura de {table_name}...", 5)
        
        # 1. Gerar profile estatístico
        profile = self.profiler.profile_table(table_name, columns, sample_data)
        update_p("Gerando resumo estatístico...", 15)
        
        # 2. Gerar resumo legível (substitui o que a IA fazia)
        advanced_description = self.profiler.generate_summary_markdown(profile)
        update_p("Preparando contexto semântico...", 20)
        
        # 3. Gerar Embeddings (Vetorização para Busca Semântica)
        # Incluir insights e colunas no texto para o vetor ser mais rico
        semantic_text = f"Tabela {table_name}: {advanced_description}. "
        semantic_text += f"Colunas: {', '.join([c['name'] for c in columns])}. "
        for col_name, col_data in profile.get('columns', {}).items():
            if col_data.get('insights'):
                semantic_text += f"Insight {col_name}: {' '.join(col_data['insights'])}. "

        print(f"Gerando vetor semântico para {table_name} (3 tentativas)...")
        vector_blob = None
        vector_success = False
        last_error = "Vetor retornado como Nulo"
        
        # Estratégias de texto para as tentativas (da mais rica para a mais simples)
        texts_to_try = [
            semantic_text, # 1. Completo (Insights + Stats)
            f"Tabela {table_name}. Descrição: {advanced_description[:500]}", # 2. Resumo curto
            f"Tabela {table_name}. Colunas: {', '.join([c['name'] for c in columns])}" # 3. Apenas Schema
        ]

        for i, text in enumerate(texts_to_try):
            try:
                update_p(f"Vetorizando tabela (Tentativa {i+1}/3)...", 25 + (i*5))
                print(f"Tentativa {i+1}/3 de vetorização para {table_name}...")
                vector = self.vector_manager.generate_embedding(text)
                if vector:
                    vector_blob = self.vector_manager.vector_to_blob(vector)
                    vector_success = True
                    print(f"✅ Vetorização concluída com sucesso na tentativa {i+1}.")
                    break
                else:
                    print(f"Aviso: Tentativa {i+1} falhou (servidor não retornou vetor).")
            except Exception as e:
                last_error = str(e)
                print(f"Falha na tentativa {i+1} para {table_name}: {last_error}")
        
        if not vector_success:
            error_msg = f"ERRO CRÍTICO: Falha total na vetorização da tabela {table_name} após 3 tentativas. O treinamento não pode prosseguir sem busca semântica."
            print(f"❌ {error_msg}")
            raise Exception(error_msg)

        update_p("Salvando metadados e schema...", 45)
        # 4. Preparar metadados para salvar
        existing_meta = self.storage.get_table_metadata(table_name) or {}
        
        # Enriquecer colunas com tipos inferidos
        enriched_columns = []
        for col in columns:
            col_name = col['name']
            enriched_col = col.copy()
            if col_name in profile.get('columns', {}):
                col_profile = profile['columns'][col_name]
                enriched_col['inferred_type'] = col_profile.get('inferred_type')
                # Se o tipo original for genérico ou nulo, usa o inferido
                if not enriched_col.get('type') or enriched_col['type'].upper() in ['TEXT', 'VARCHAR', 'UNKNOWN']:
                    enriched_col['type'] = col_profile.get('type')
            enriched_columns.append(enriched_col)

        # Salvar apenas uma amostra pequena nos metadados para não pesar o banco (50 linhas)
        # mas manter o profile baseado na amostra adaptativa processada
        sample_to_save = sample_data[:50] if sample_data else []
        table_meta = {
            'table_name': table_name,
            'table_description': advanced_description,
            'schema_info': existing_meta.get('schema_info', {}),
            'columns_info': enriched_columns,
            'sample_data': sample_to_save,
            'record_count': total_records,
            'is_active': True,
            'deep_profile': profile,
            'semantic_context': existing_meta.get('semantic_context', []),
            'embedding_vector': vector_blob,
            'export_status': existing_meta.get('export_status', 'Pendente')
        }
        
        # 5. Salvar no Storage
        self.storage.save_tables([table_meta])
        
        # 6. Geração Massiva de Padrões Sintéticos (Imitação Preventiva)
        try:
            # O SyntheticGenerator vai usar de 50% a 95% do progresso
            def synth_callback(curr, total, msg, remaining):
                if progress_callback:
                    p = 50 + int((curr / total) * 45)
                    progress_callback(p, msg, remaining)

            self.synthetic_gen.generate_for_table(table_name, profile, columns, sample_data, progress_callback=synth_callback)
        except Exception as e:
            print(f"⚠️ Aviso: Falha na geração de padrões sintéticos: {e}")

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
        flow = discovery['flow']
        relationships = discovery['relationships']
        cascades = discovery.get('cascades', [])
        table_info = discovery.get('table_info', {})
        movements = discovery.get('movements', [])
        
        # Coletar regras de negócio dos perfis das tabelas
        business_rules = {}
        for meta in tables_meta:
            profile = meta.get('profile', {})
            rules = profile.get('advanced_analysis', {}).get('business_rules', [])
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
