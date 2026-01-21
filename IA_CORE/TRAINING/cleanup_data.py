
import os
import sys

# Adicionar o diretório raiz ao path para importações
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '../../../../')))

from SETORES_MODULOS.ROHDEN_AI.IA_CORE.DATA.storage import DataStorage

def cleanup_ai_data():
    print("🧹 Iniciando limpeza completa dos dados da IA...")
    storage = DataStorage()
    
    # Limpar ChromaDB
    print("🗑️ Limpando coleções do ChromaDB...")
    storage.clear_behavioral_patterns()
    storage.clear_table_metadata()
    storage.clear_knowledge_base()
    
    # Limpar SQLite
    print("🗑️ Limpando dados do SQLite...")
    storage.clear_all_sqlite_data()
    
    print("✅ Limpeza concluída com sucesso!")

if __name__ == "__main__":
    cleanup_ai_data()
