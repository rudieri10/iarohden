# Configurações de Contexto e Histórico da Rohden AI

CONTEXT_CONFIG = {
    # Quantidade de mensagens recentes para manter no contexto
    "max_history_messages": 25,
    
    # Limite de caracteres por mensagem no histórico
    "max_chars_per_message": 400,
    
    # Incluir metadados técnicos (como tabelas usadas) no contexto interno
    "include_technical_metadata": True,
    
    # Se deve truncar com reticências
    "use_ellipsis": True
}
