
import sqlite3
import json
import os

db_path = r'c:\Users\rudieri.ROHDEN\Documents\programacao\rohden\sys\SYS_ROHDEN\SETORES_MODULOS\ROHDEN_AI\IA_CORE\DATA\rohden_ai.db'

def check_table_metadata():
    if not os.path.exists(db_path):
        print(f"Erro: Arquivo {db_path} não encontrado.")
        return

    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()
    
    try:
        cursor.execute("SELECT * FROM table_metadata WHERE table_name = 'TB_CONTATOS'")
        row = cursor.fetchone()
        
        if row:
            col_names = [description[0] for description in cursor.description]
            data = dict(zip(col_names, row))
            print("--- Metadados da TB_CONTATOS ---")
            print(f"Nome: {data['table_name']}")
            print(f"Descrição: {data['table_description']}")
            print(f"Schema Info: {data['schema_info']}")
            print(f"Columns Info: {data['columns_info']}")
            print(f"Sample Data: {data['sample_data']}")
        else:
            print("TB_CONTATOS não encontrada em table_metadata.")
            
        # Também listar todas as tabelas em table_metadata para ver o que tem
        print("\n--- Outras tabelas em table_metadata ---")
        cursor.execute("SELECT table_name FROM table_metadata")
        for t in cursor.fetchall():
            print(f"- {t[0]}")

    except Exception as e:
        print(f"Erro ao consultar metadados: {e}")
    finally:
        conn.close()

if __name__ == "__main__":
    check_table_metadata()
