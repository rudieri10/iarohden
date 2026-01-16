from . import config_rohden_ai_bp
from flask import render_template, request, jsonify
from conecxaodb import get_connection
import traceback
import os
import json
import sqlite3
from datetime import datetime

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
    
    # Sincronizar com o Cérebro da IA (rohden_ai.db)
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

    # Se schema_name for vazio por algum motivo, garantir que tenha um valor padrão
    if not schema_name:
        schema_name = 'SYSROH'
        
    conn = get_connection()
    if not conn:
        return jsonify({'error': 'Erro de conexão'}), 500
        
    try:
        cursor = conn.cursor()
        
        # 0. Buscar contagem total de registros - Adicionando aspas e garantindo que não sejam vazias
        # ORA-01741 ocorre se tentarmos fazer ""."" (aspas vazias)
        full_table_name = f'"{schema_name}"."{table_name}"'
        cursor.execute(f'SELECT COUNT(*) FROM {full_table_name}')
        total_records = cursor.fetchone()[0]
        
        # 1. Buscar metadados das colunas, comentários e Constraints (PK/FK)
        # Usar literais sem aspas para o WHERE, pois o Oracle armazena em uppercase
        # Corrigido nomes de bind variables para evitar conflitos (ex: :owner_name em vez de :schema)
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
            
        # 1.1 Buscar Foreign Keys (Relacionamentos)
        fks_info = []
        try:
            cursor.execute(f"""
                SELECT 
                    cols.column_name, 
                    r_cols.table_name as ref_table, 
                    r_cols.column_name as ref_column
                FROM all_cons_columns cols
                JOIN all_constraints cons ON cols.constraint_name = cons.constraint_name
                JOIN all_cons_columns r_cols ON cons.r_constraint_name = r_cols.constraint_name
                WHERE cons.constraint_type = 'R' 
                  AND cons.owner = :owner_name
                  AND cons.table_name = :tbl_name
            """, {'owner_name': schema_name.upper(), 'tbl_name': table_name.upper()})
            for row in cursor.fetchall():
                fks_info.append({
                    'column': row[0],
                    'ref_table': row[1],
                    'ref_column': row[2]
                })
        except Exception:
            pass

        # 2. Buscar amostra de dados (5000 registros) para treinamento profundo sem IA
        # Aumentado para 5000 para maior precisão estatística e detecção de anomalias
        cursor.execute(f'SELECT * FROM (SELECT * FROM {full_table_name}) WHERE ROWNUM <= 5000')
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
            
        # 3. Treinamento Inteligente Sem IA (Usando o novo Trainer)
        try:
            training_result = trainer.train_table(table_name, columns_info, sample_rows, total_records)
            advanced_description = training_result.get('summary', f"Tabela {table_name} treinada.")
            vector_success = training_result.get('vector_success', False)
            
            # 3.1 Acionar descoberta de processos automaticamente
            try:
                trainer.train_processes()
            except Exception as proc_err:
                print(f"Aviso: Erro ao atualizar fluxos de processos: {str(proc_err)}")
        except Exception as train_err:
            return jsonify({'error': f"Erro no motor de treinamento: {str(train_err)}"}), 500
        
        # 4. Exportar tabela toda para banco de dados SQLite em \\192.168.1.217\c$\IA\dados ia
        export_msg = ""
        export_success = True
        try:
            ensure_export_dir()
            
            # Se for uma tabela muito grande (ex: TB_CONTATOS), vamos avisar no log
            if total_records > 100000:
                print(f"Exportando tabela grande ({total_records} registros): {table_name}...")

            # Definir caminho do arquivo .db
            export_file_path = os.path.join(AI_DATA_EXPORT_DIR, f"{table_name}.db")
            
            # Remover arquivo antigo se existir para garantir integridade
            if os.path.exists(export_file_path):
                try:
                    os.remove(export_file_path)
                except Exception as e:
                    print(f"Erro ao remover arquivo antigo {export_file_path}: {str(e)}")
                    # Se não conseguir remover, tentamos sobrescrever ou falhamos graciosamente

            # Conectar ao SQLite
            sqlite_conn = sqlite3.connect(export_file_path)
            sqlite_cursor = sqlite_conn.cursor()

            # Mapear tipos do Oracle para SQLite
            def map_type(oracle_type):
                oracle_type = oracle_type.upper()
                if 'NUMBER' in oracle_type: return 'REAL'
                if 'CHAR' in oracle_type or 'VARCHAR' in oracle_type: return 'TEXT'
                if 'DATE' in oracle_type or 'TIMESTAMP' in oracle_type: return 'TEXT'
                return 'TEXT'

            # Criar tabela no SQLite
            cols_def = []
            for col in columns_info:
                col_name = col['name']
                col_type = map_type(col['type'])
                pk_def = " PRIMARY KEY" if col['is_pk'] else ""
                cols_def.append(f'"{col_name}" {col_type}{pk_def}')
            
            create_sql = f'CREATE TABLE "{table_name}" ({", ".join(cols_def)})'
            sqlite_cursor.execute(create_sql)

            # Buscar todos os dados do Oracle - Adicionando aspas
            cursor.execute(f'SELECT * FROM {full_table_name}')
            all_cols = [col[0] for col in cursor.description]
            
            placeholders = ", ".join(["?" for _ in all_cols])
            insert_sql = f'INSERT INTO "{table_name}" VALUES ({placeholders})'
            
            count = 0
            # Usar fetchmany e transação para alta performance
            while True:
                rows = cursor.fetchmany(1000)
                if not rows:
                    break
                
                # Tratar valores para o SQLite
                processed_rows = []
                for row in rows:
                    processed_row = []
                    for val in row:
                        if val is None:
                            processed_row.append(None)
                        elif hasattr(val, 'isoformat'):
                            processed_row.append(val.isoformat())
                        else:
                            processed_row.append(val)
                    processed_rows.append(tuple(processed_row))
                
                sqlite_cursor.executemany(insert_sql, processed_rows)
                count += len(processed_rows)
            
            sqlite_conn.commit()
            sqlite_conn.close()
            
            export_msg = f" Exportado banco SQLite com {count} registros para o servidor de IA."
        except Exception as export_error:
            export_success = False
            export_msg = f" (Erro na exportação SQLite: {str(export_error)})"
            print(f"Erro na exportação da tabela {table_name}: {str(export_error)}")
        
        # 5. Atualizar o status de exportação no banco de dados da IA
        try:
            current_meta = storage.get_table_metadata(table_name)
            if current_meta:
                current_meta['export_status'] = "Sucesso" if export_success else f"Erro: {export_msg}"
                storage.save_tables([current_meta])
        except Exception as update_err:
            print(f"Erro ao atualizar status de exportação: {str(update_err)}")

        return jsonify({
            'success': True, 
            'vector_success': vector_success,
            'export_success': export_success,
            'message': f"Treinamento PROFUNDO concluído com sucesso para a tabela {table_name}.{export_msg}"
        })
    except Exception as e:
        return jsonify({'error': str(e)}), 500
    finally:
        cursor.close()
        conn.close()

@config_rohden_ai_bp.route('/get_current_config')
def get_current_config():
    """Retorna a configuração atual do sistema baseada no banco de dados da IA"""
    tables_list = []
    metadata_map = {}
    
    # Carregar do Cérebro da IA (rohden_ai.db)
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
        return jsonify({'flows': flows})
    except Exception as e:
        return jsonify({'error': str(e)}), 500
