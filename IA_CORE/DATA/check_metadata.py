
import json
import os
import sys

# Adicionar o diretório raiz ao path para importar conecxaodb
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '../../../../')))
try:
    from conecxaodb import get_connection
except ImportError:
    # Fallback para o caso de não encontrar o conecxaodb
    def get_connection():
        import cx_Oracle
        ip = '192.168.1.253'
        porta = 1521
        service_name = 'rohden'
        usuario_oracle = 'SYSROH'
        senha_oracle = 'rohden'
        dsn_tns = cx_Oracle.makedsn(ip, porta, service_name=service_name)
        try:
            return cx_Oracle.connect(user=usuario_oracle, password=senha_oracle, dsn=dsn_tns)
        except:
            return None

def check_table_metadata():
    conn = get_connection()
    if not conn:
        print("Erro: Não foi possível conectar ao banco Oracle.")
        return

    cursor = conn.cursor()
    
    try:
        # No Oracle, a tabela é SYSROH.TB_AI_TABLE_METADATA
        cursor.execute("SELECT * FROM SYSROH.TB_AI_TABLE_METADATA WHERE TABLE_NAME = 'TB_CONTATOS'")
        row = cursor.fetchone()
        
        if row:
            col_names = [description[0] for description in cursor.description]
            data = dict(zip(col_names, row))
            print("--- Metadados da TB_CONTATOS (Oracle) ---")
            print(f"Nome: {data['TABLE_NAME']}")
            print(f"Descrição: {data['TABLE_DESCRIPTION']}")
            # CLOBs precisam ser lidos
            schema_info = data['SCHEMA_INFO'].read() if hasattr(data['SCHEMA_INFO'], 'read') else data['SCHEMA_INFO']
            columns_info = data['COLUMNS_INFO'].read() if hasattr(data['COLUMNS_INFO'], 'read') else data['COLUMNS_INFO']
            sample_data = data['SAMPLE_DATA'].read() if hasattr(data['SAMPLE_DATA'], 'read') else data['SAMPLE_DATA']
            
            print(f"Schema Info: {schema_info}")
            print(f"Columns Info: {columns_info}")
            print(f"Sample Data: {sample_data}")
        else:
            print("TB_CONTATOS não encontrada em SYSROH.TB_AI_TABLE_METADATA.")
            
        # Também listar todas as tabelas em table_metadata para ver o que tem
        print("\n--- Outras tabelas em SYSROH.TB_AI_TABLE_METADATA ---")
        cursor.execute("SELECT TABLE_NAME FROM SYSROH.TB_AI_TABLE_METADATA")
        for t in cursor.fetchall():
            print(f"- {t[0]}")

    except Exception as e:
        print(f"Erro ao consultar metadados no Oracle: {e}")
    finally:
        cursor.close()
        conn.close()

if __name__ == "__main__":
    check_table_metadata()
