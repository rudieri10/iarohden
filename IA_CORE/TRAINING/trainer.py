import json
from .profiler import TrainingProfiler, ProcessProfiler
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

    def train_table(self, table_name, columns, sample_data, total_records):
        """
        Executa o fluxo de treinamento profundo.
        1. Analisa estatisticamente os dados.
        2. Gera insights automáticos.
        3. Salva no banco de dados SQLite.
        """
        print(f"Iniciando treinamento PROFUNDO para {table_name}...")
        
        # 1. Gerar profile estatístico
        profile = self.profiler.profile_table(table_name, columns, sample_data)
        
        # 2. Gerar resumo legível (substitui o que a IA fazia)
        advanced_description = self.profiler.generate_summary_markdown(profile)
        
        # 3. Gerar Embeddings (Vetorização para Busca Semântica)
        # Incluir insights e colunas no texto para o vetor ser mais rico
        semantic_text = f"Tabela {table_name}: {advanced_description}. "
        semantic_text += f"Colunas: {', '.join([c['name'] for c in columns])}. "
        for col_name, col_data in profile.get('columns', {}).items():
            if col_data.get('insights'):
                semantic_text += f"Insight {col_name}: {' '.join(col_data['insights'])}. "

        print(f"Gerando vetor semântico para {table_name}...")
        vector_blob = None
        vector_success = False
        
        try:
            # Tentativa 1: Texto Completo (Insights + Colunas)
            vector = self.vector_manager.generate_embedding(semantic_text)
            
            # Se falhar, tentativa 2: Texto Simplificado (Apenas Nome e Colunas)
            if not vector:
                print(f"Simplificando contexto para {table_name} e tentando novamente...")
                simple_text = f"Tabela {table_name}. Colunas: {', '.join([c['name'] for c in columns])}"
                vector = self.vector_manager.generate_embedding(simple_text)
            
            if vector:
                vector_blob = self.vector_manager.vector_to_blob(vector)
                vector_success = True
        except Exception as e:
            print(f"Erro na vetorização de {table_name}: {str(e)}")
        
        if not vector_success:
            print(f"⚠️ Aviso: Não foi possível gerar vetor para {table_name}. Usando indexação por palavras-chave como fallback.")

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

        # Salvar apenas uma amostra pequena nos metadados para não pesar o SQLite (50 linhas)
        # mas manter o profile baseado nas 5000 linhas processadas
        table_meta = {
            'table_name': table_name,
            'table_description': advanced_description,
            'schema_info': existing_meta.get('schema_info', {}),
            'columns_info': enriched_columns,
            'sample_data': sample_data[:50], 
            'record_count': total_records,
            'is_active': True,
            'deep_profile': profile,
            'semantic_context': existing_meta.get('semantic_context', []),
            'embedding_vector': vector_blob,
            'export_status': existing_meta.get('export_status', 'Pendente')
        }
        
        # 5. Salvar no Storage
        self.storage.save_tables([table_meta])
        
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
        
        # 2. Descobrir fluxos (Ex: Ciclo de Vendas)
        # Por enquanto, focamos no fluxo principal solicitado
        flow = self.process_profiler.discover_flow(tables_meta)
        timing_insights = self.process_profiler.analyze_timing(flow, None)
        
        # 3. Gerar resumo do processo
        process_summary = self.process_profiler.generate_process_summary("Ciclo Comercial Rohden", flow, timing_insights)
        
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
