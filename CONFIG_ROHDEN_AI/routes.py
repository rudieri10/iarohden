from . import config_rohden_ai_bp
from flask import render_template, request, jsonify
from conecxaodb import get_connection
import traceback
import os
import json
from datetime import datetime
import threading

# Importar o storage da IA Core
try:
    from SETORES_MODULOS.ROHDEN_AI.IA_CORE.DATA.storage import DataStorage
    storage = DataStorage()
    from SETORES_MODULOS.ROHDEN_AI.IA_CORE.ENGINE.ai_engine import get_llama_engine
    llama_engine = get_llama_engine()
    from SETORES_MODULOS.ROHDEN_AI.IA_CORE.TRAINING.trainer import TableTrainer
    trainer = TableTrainer()
except ImportError:
    # Fallback caso o caminho de importação seja diferente
    from ..IA_CORE.DATA.storage import DataStorage
    storage = DataStorage()
    from ..IA_CORE.ENGINE.ai_engine import get_llama_engine
    llama_engine = get_llama_engine()
    from ..IA_CORE.TRAINING.trainer import TableTrainer
    trainer = TableTrainer()

# Exportação para o IP do servidor (UNC Path)
AI_DATA_EXPORT_DIR = r'\\192.168.1.217\c$\IA\dados ia' 

# Armazenamento global temporário para progresso de treinamento
training_progress = {}

@config_rohden_ai_bp.route('/training_status')
def get_all_training_status():
    """Retorna o status de todos os treinamentos ativos"""
    return jsonify(training_progress)

@config_rohden_ai_bp.route('/training_progress/<table_name>')
def get_training_progress(table_name):
    """Retorna o progresso atual do treinamento para uma tabela"""
    progress = training_progress.get(table_name, {
        'percent': 0, 
        'status': 'Pendente', 
        'remaining': 0,
        'done': False
    })
    return jsonify(progress)

def ensure_export_dir():
    """Garante que a pasta de exportação exista"""
    if not os.path.exists(AI_DATA_EXPORT_DIR):
        try:
            os.makedirs(AI_DATA_EXPORT_DIR, exist_ok=True)
        except Exception:
            pass

@config_rohden_ai_bp.route('/')
def index():
    return render_template('config_rohden_ai.html')

@config_rohden_ai_bp.route('/get_schemas')
def get_schemas():
    """Lista todos os schemas disponíveis no banco de dados"""
    try:
        from .database_config import get_available_schemas
        schemas = get_available_schemas()
        return jsonify({'schemas': schemas})
    except Exception as e:
        return jsonify({'error': f'Erro ao buscar schemas: {str(e)}'}), 500

@config_rohden_ai_bp.route('/get_tables_from_schema/<schema_name>')
def get_tables_from_schema(schema_name):
    """Lista todas as tabelas de um schema específico"""
    try:
        from .database_config import get_tables_from_schema
        tables = get_tables_from_schema(schema_name)
        return jsonify({'tables': tables})
    except Exception as e:
        return jsonify({'error': f'Erro ao buscar tabelas: {str(e)}'}), 500

@config_rohden_ai_bp.route('/get_oracle_tables')
def get_oracle_tables():
    conn = get_connection()
    if not conn:
        return jsonify({'error': 'Erro ao conectar ao banco'}), 500
    try:
        cursor = conn.cursor()
        query = """
            SELECT table_name 
            FROM all_tables 
            WHERE owner = 'SYSROH' 
              AND table_name NOT LIKE 'BKP_%'
              AND table_name NOT LIKE '%_BKP'
              AND table_name NOT LIKE 'BACKUP_%'
              AND table_name NOT LIKE '%_BACKUP'
              AND table_name NOT LIKE 'AI_%'
              AND table_name NOT LIKE 'TMP_%'
              AND table_name NOT LIKE 'TEMP_%'
            ORDER BY table_name
        """
        cursor.execute(query)
        tables = [row[0] for row in cursor.fetchall()]
        return jsonify({'tables': tables})
    except Exception as e:
        return jsonify({'error': str(e)}), 500
    finally:
        cursor.close()
        conn.close()

@config_rohden_ai_bp.route('/save_config', methods=['POST'])
def save_config():
    data = request.json
    tables = data.get('tables', [])
    
    # Sincronizar com o Banco de Dados Oracle (SYSROH)
    try:
        db_tables = []
        for t in tables:
            t_name = t['name']
            # Buscar metadados existentes para não sobrescrever com vazio
            existing_meta = storage.get_table_metadata(t_name)
            
            db_tables.append({
                'table_name': t_name,
                'table_description': existing_meta.get('table_description') if existing_meta else f"Tabela {t_name}",
                'schema_info': t.get('schema_info', existing_meta.get('schema_info') if existing_meta else {'schema': t.get('schema', 'SYSROH')}),
                'columns_info': existing_meta.get('columns_info') if existing_meta else [],
                'sample_data': existing_meta.get('sample_data') if existing_meta else [],
                'deep_profile': existing_meta.get('deep_profile', {}),
                'semantic_context': existing_meta.get('semantic_context', []),
                'embedding_vector': existing_meta.get('embedding_vector'),
                'export_status': existing_meta.get('export_status', 'Pendente'),
                'record_count': existing_meta.get('record_count', 0),
                'is_active': True
            })
        
        if db_tables:
            storage.save_tables(db_tables)
            
    except Exception:
        pass
    
    return jsonify({'success': True, 'message': 'Configuração salva com sucesso!'})

@config_rohden_ai_bp.route('/process_table', methods=['POST'])
def process_table():
    data = request.json
    table_name = data.get('table_name', '').strip()
    schema_name = data.get('schema_name', 'SYSROH').strip()
    
    if not table_name:
        return jsonify({'error': 'Nome da tabela não fornecido'}), 400

    if not schema_name:
        schema_name = 'SYSROH'

    # Iniciar treinamento em segundo plano
    thread = threading.Thread(target=run_background_training, args=(table_name, schema_name))
    thread.daemon = True
    thread.start()

    return jsonify({
        'success': True,
        'message': f"Treinamento da tabela {table_name} iniciado em segundo plano."
    })

def run_background_training(table_name, schema_name):
    """Executa o treinamento em uma thread separada para não bloquear o servidor"""
    def update_progress(percent, msg, remaining=0):
        training_progress[table_name] = {
            'percent': percent,
            'status': msg,
            'remaining': remaining,
            'done': False
        }

    try:
        update_progress(0, "Conectando ao Oracle...")
        conn = get_connection()
        if not conn:
            update_progress(0, "Erro: Falha na conexão com o banco")
            return
            
        cursor = conn.cursor()
        
        # 0. Buscar contagem total
        full_table_name = f'"{schema_name}"."{table_name}"'
        update_progress(5, f"Contando registros em {table_name}...")
        cursor.execute(f'SELECT COUNT(*) FROM {full_table_name}')
        total_records = cursor.fetchone()[0]
        
        # 1. Buscar metadados
        update_progress(10, "Lendo estrutura da tabela...")
        cursor.execute(f"""
            SELECT 
                cols.column_name, 
                cols.data_type, 
                cols.nullable,
                coms.comments,
                (SELECT 'Y' FROM all_cons_columns acc 
                 JOIN all_constraints ac ON acc.constraint_name = ac.constraint_name 
                 WHERE acc.table_name = cols.table_name AND acc.column_name = cols.column_name 
                 AND ac.constraint_type = 'P' AND acc.owner = cols.owner) as is_pk
            FROM all_tab_columns cols
            LEFT JOIN all_col_comments coms 
                ON cols.owner = coms.owner 
                AND cols.table_name = coms.table_name 
                AND cols.column_name = coms.column_name
            WHERE cols.owner = :owner_name AND cols.table_name = :tbl_name
            ORDER BY cols.column_id
        """, {'owner_name': schema_name.upper(), 'tbl_name': table_name.upper()})
        
        columns_info = []
        for row in cursor.fetchall():
            columns_info.append({
                'name': row[0],
                'type': row[1],
                'nullable': row[2],
                'comment': row[3] if row[3] else '',
                'is_pk': row[4] == 'Y'
            })
            
        # 2. Amostragem
        update_progress(15, f"Extraindo amostra adaptativa ({total_records} registros)... ")
        if total_records <= 10000:
            sample_query = f'SELECT * FROM {full_table_name}'
            target_sample = total_records
        else:
            target_sample = min(int(total_records * 0.1), 25000)
            sample_percent = max(round((target_sample / total_records) * 100, 2), 0.01)
            sample_query = f'SELECT * FROM (SELECT * FROM {full_table_name} SAMPLE({sample_percent})) WHERE ROWNUM <= {target_sample}'
        
        cursor.execute(sample_query)
        cols = [col[0] for col in cursor.description]
        sample_rows = []
        for row in cursor.fetchall():
            row_dict = {}
            for i, val in enumerate(row):
                if hasattr(val, 'isoformat'):
                    row_dict[cols[i]] = val.isoformat()
                else:
                    row_dict[cols[i]] = val
            sample_rows.append(row_dict)
            
        # 3. Treinamento
        update_progress(20, "Iniciando motor de análise profunda...")
        training_result = trainer.train_table(table_name, columns_info, sample_rows, total_records, progress_callback=update_progress)
        vector_success = training_result.get('vector_success', False)
        
        # 4. Sincronização final
        update_progress(95, "Sincronizando dados com o servidor de IA...")
        export_success = True
        
        # Finalizar
        update_progress(100, "Concluído!", 0)
        training_progress[table_name]['done'] = True
        
        # Atualizar metadados
        current_meta = storage.get_table_metadata(table_name)
        if current_meta:
            current_meta['export_status'] = "Sucesso" if export_success else "Erro na exportação"
            storage.save_tables([current_meta])
            
        # Atualizar fluxos
        trainer.train_processes()

    except Exception as e:
        update_progress(0, f"Erro: {str(e)}")
        print(f"Erro treinamento thread: {traceback.format_exc()}")
    finally:
        if 'cursor' in locals(): cursor.close()
        if 'conn' in locals(): conn.close()

@config_rohden_ai_bp.route('/get_current_config')
def get_current_config():
    """Retorna a configuração atual do sistema baseada no banco de dados da IA"""
    tables_list = []
    metadata_map = {}
    
    # Carregar do Banco de Dados Oracle (SYSROH)
    try:
        tables_db = storage.load_tables()
        for t_db in tables_db:
            t_name = t_db['table_name']
            schema = t_db.get('schema_info', {}).get('schema', 'SYSROH')
            
            tables_list.append({
                'name': t_name, 
                'schema': schema,
                'description': t_db.get('table_description', ''),
                'record_count': t_db.get('record_count', 0),
                'updated_at': t_db.get('updated_at'),
                'has_vector': t_db.get('embedding_vector') is not None and len(t_db.get('embedding_vector', b'')) > 0,
                'export_status': t_db.get('export_status', 'Sucesso'),
                'processed': {
                    'last_processed': t_db.get('updated_at'),
                    'count': t_db.get('record_count', 0)
                }
            })
            
            metadata_map[t_name] = {
                'columns': t_db.get('columns_info', []),
                'samples': t_db.get('sample_data', []),
                'last_processed': t_db.get('updated_at'),
                'record_count': t_db.get('record_count', 0),
                'schema': schema,
                'description': t_db.get('table_description', '')
            }
    except Exception:
        pass
    
    return jsonify({
        'tables': tables_list, 
        'metadata': metadata_map
    })

@config_rohden_ai_bp.route('/get_process_flows')
def get_process_flows():
    """Retorna os fluxos de processos descobertos"""
    try:
        flows = storage.get_process_flows()
        # Limpar bytes não serializáveis
        for f in flows:
            if 'embedding_vector' in f:
                del f['embedding_vector']
        return jsonify({'flows': flows})
    except Exception as e:
        traceback.print_exc()
        return jsonify({'error': str(e)}), 500

def clean_for_json(obj):
    """Converte recursivamente objetos não serializáveis em formatos compatíveis.
    Otimizado para ignorar campos binários pesados (como embedding_vector)."""
    if obj is None:
        return None
        
    # Se for um LOB do Oracle que ainda não foi lido
    if hasattr(obj, 'read'):
        try:
            return obj.read()
        except:
            return "<unread lob>"

    if isinstance(obj, dict):
        # OTIMIZAÇÃO: Remove vetores de embedding que são pesados e não usados no frontend
        return {
            k: clean_for_json(v) 
            for k, v in obj.items() 
            if k.lower() not in ('embedding_vector', 'vector')
        }
    elif isinstance(obj, list):
        return [clean_for_json(i) for i in obj]
    elif isinstance(obj, bytes):
        # Se for bytes, só tenta decodificar se for pequeno (provavelmente texto)
        if len(obj) < 1000:
            try:
                return obj.decode('utf-8')
            except:
                return f"<binary data {len(obj)} bytes>"
        return f"<binary data {len(obj)} bytes>"
    elif isinstance(obj, datetime):
        return obj.isoformat()
    elif isinstance(obj, (int, float, str, bool)):
        return obj
    return str(obj)

@config_rohden_ai_bp.route('/get_ai_data')
def get_ai_data():
    """Retorna um compilado de todo o conhecimento da IA para o frontend"""
    import time
    start_time = time.time()
    try:
        # 1. Carregar dados do storage (com limites NO BANCO para performance)
        t0 = time.time()
        tables = storage.load_tables(limit=50)
        
        t1 = time.time()
        # Limitar padrões aos 50 mais recentes diretamente no SQL
        patterns = storage.get_behavioral_patterns(limit=50)
        
        # Pega a contagem total de forma leve
        total_patterns_count = storage.get_patterns_count()
        
        t2 = time.time()
        flows = storage.get_process_flows(limit=50)
        
        t3 = time.time()
        knowledge = storage.get_knowledge(limit=50)
        
        t4 = time.time()
        
        # 2. Limpar dados para JSON
        print(f"DEBUG: Iniciando clean_for_json (Tables: {len(tables)}, Patterns: {len(patterns)})")
        clean_data = {
            'tables': clean_for_json(tables),
            'patterns': clean_for_json(patterns),
            'flows': clean_for_json(flows),
            'knowledge': clean_for_json(knowledge)
        }
        t5 = time.time()
        
        data = {
            **clean_data,
            'stats': {
                'total_tables': len(tables),
                'total_patterns': total_patterns_count,
                'total_flows': len(flows)
            }
        }
        
        print(f"DEBUG: get_ai_data timings: tables={t1-t0:.2f}s, patterns_load={t2-t1:.2f}s, flows={t3-t2:.2f}s, knowledge={t4-t3:.2f}s, clean={t5-t4:.2f}s")
        print(f"DEBUG: get_ai_data total time: {time.time() - start_time:.2f}s")
        
        return jsonify(data)
    except Exception as e:
        print(f"Erro ao buscar dados da IA: {str(e)}")
        traceback.print_exc()
        return jsonify({'error': str(e)}), 500
