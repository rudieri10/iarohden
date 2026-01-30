"""
DATABASE CONFIG - Configurações do Banco de Dados
Configurações de conexão com o banco Oracle principal
"""

import cx_Oracle
import logging
from ..IA_CORE.PIPELINE.observer import Observer

observer = Observer()

# Configurações do Banco Principal
DATABASE_CONFIG = {
    'ip': '192.168.1.253',
    'porta': 1521,
    'service_name': 'rohden',
    'usuario_oracle': 'hds',
    'senha_oracle': 'super'
}

def get_database_connection():
    """
    Cria e retorna uma conexão com o banco Oracle principal.
    """
    ip = DATABASE_CONFIG['ip']
    porta = DATABASE_CONFIG['porta']
    service_name = DATABASE_CONFIG['service_name']
    usuario_oracle = DATABASE_CONFIG['usuario_oracle']
    senha_oracle = DATABASE_CONFIG['senha_oracle']
    
    print(f"Tentando conectar ao Oracle: {usuario_oracle}@{ip}:{porta}/{service_name}")  # Debug
    
    dsn = cx_Oracle.makedsn(ip, porta, service_name=service_name)
    
    try:
        conexao = cx_Oracle.connect(user=usuario_oracle, password=senha_oracle, dsn=dsn)
        print("Conexão com Oracle estabelecida com sucesso!")  # Debug
        return conexao
    except cx_Oracle.Error as error:
        print(f"Erro na conexão com Oracle (HDS): {error}")  # Debug
        logging.error(f"Erro na conexão com Oracle (HDS): {error}")
        return None

def get_available_schemas():
    """
    Lista todos os schemas disponíveis no banco de dados.
    """
    schemas = ['SYSROH', 'HDS']  # Sempre retorna os dois schemas
    
    conn = get_database_connection()
    if conn:
        try:
            cursor = conn.cursor()
            
            # Query para verificar schemas existentes
            query = """
            SELECT DISTINCT owner 
            FROM all_tables 
            WHERE owner IN ('SYSROH', 'HDS')
            ORDER BY owner
            """
            
            cursor.execute(query)
            found_schemas = [row[0] for row in cursor.fetchall()]
            
            # Se encontrou schemas, usa os encontrados
            if found_schemas:
                schemas = found_schemas
                
        except cx_Oracle.Error as error:
            logging.error(f"Erro ao listar schemas: {error}")
        finally:
            if conn:
                try:
                    conn.close()
                except:
                    pass
    
    return schemas

def get_tables_from_schema(schema_name):
    """
    Lista todas as tabelas de um schema específico.
    """
    tables = []
    
    conn = get_database_connection()
    if conn:
        try:
            cursor = conn.cursor()
            
            # Query para listar tabelas do schema (exatamente como você testou)
            query = """
            SELECT table_name 
            FROM all_tables 
            WHERE owner = :schema_name
            ORDER BY table_name
            """
            
            # Observer Hook
            with observer.observe_query(query) as ctx:
                cursor.execute(query, {'schema_name': schema_name})
                tables = [row[0] for row in cursor.fetchall()]
                ctx.row_count = len(tables)
            
        except cx_Oracle.Error as error:
            logging.error(f"Erro ao listar tabelas do schema {schema_name}: {error}")
        finally:
            if conn:
                try:
                    conn.close()
                except:
                    pass
    
    return tables

# Configuração de logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
