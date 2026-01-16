# DATABASE - Sistema de Persistência

## 🗄️ O que faz

Módulo responsável por gerenciar toda persistência de dados do sistema ROHDEN_AI usando SQLite para histórico, conversas e configurações.

## 🎯 Funcionalidades

### 1. **Gerenciamento de Conversas**
- Criação e controle de sessões de chat
- Armazenamento de mensagens com timestamps
- Títulos automáticos baseado na primeira mensagem

### 2. **Histórico e Favoritos**
- Salvamento de consultas favoritas
- Registro de padrões de busca
- Sugestões baseadas no histórico

### 3. **Alertas Inteligentes**
- Configuração de alertas automáticos
- Monitoramento de condições SQL
- Controle de status (active, triggered, muted)

### 4. **Predições e Análises**
- Armazenamento de previsões geradas
- Scores de confiança
- Metadados das análises

### 5. **Conexão e Inicialização**
- Gerenciamento de conexão SQLite
- Criação automática de tabelas
- Configuração de row_factory

## 📊 Estrutura de Dados

### Tabelas Principais

#### `chats` - Sessões de Conversa
```sql
CREATE TABLE chats (
    id INTEGER PRIMARY KEY,
    user_name TEXT NOT NULL,
    title TEXT DEFAULT 'Nova Conversa',
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
```

#### `messages` - Mensagens das Conversas
```sql
CREATE TABLE messages (
    id INTEGER PRIMARY KEY,
    chat_id INTEGER NOT NULL,
    role TEXT NOT NULL,  -- 'user' ou 'assistant'
    content TEXT NOT NULL,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    FOREIGN KEY (chat_id) REFERENCES chats(id)
);
```

#### `favorites` - Consultas Favoritas
```sql
CREATE TABLE favorites (
    id INTEGER PRIMARY KEY,
    user_name TEXT NOT NULL,
    query TEXT NOT NULL,
    title TEXT NOT NULL,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
```

#### `user_patterns` - Padrões de Busca
```sql
CREATE TABLE user_patterns (
    id INTEGER PRIMARY KEY,
    user_name TEXT NOT NULL,
    query TEXT NOT NULL,
    frequency INTEGER DEFAULT 1,
    last_used TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
```

#### `intelligent_alerts` - Alertas
```sql
CREATE TABLE intelligent_alerts (
    id INTEGER PRIMARY KEY,
    user_name TEXT NOT NULL,
    title TEXT NOT NULL,
    sql_query TEXT NOT NULL,
    condition_type TEXT NOT NULL,  -- 'increase', 'decrease', 'threshold'
    threshold_value REAL,
    last_value REAL,
    status TEXT DEFAULT 'active',
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    last_checked TIMESTAMP
);
```

#### `ai_predictions` - Predições
```sql
CREATE TABLE ai_predictions (
    id INTEGER PRIMARY KEY,
    user_name TEXT NOT NULL,
    target_data TEXT NOT NULL,
    forecast_json TEXT NOT NULL,
    confidence_score REAL,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
```

## 🔧 Métodos Principais

### Conversas
```python
# Criar nova conversa
chat_id = create_chat(username, "Título da Conversa")

# Buscar conversas do usuário
chats = get_user_chats(username)

# Buscar mensagens de uma conversa
messages = get_chat_messages(chat_id)

# Adicionar mensagem
add_message(chat_id, 'user', "Pergunta do usuário")

# Atualizar título
update_chat_title(chat_id, "Novo título")

# Excluir conversa
delete_chat(chat_id)
```

### Favoritos e Padrões
```python
# Adicionar favorito
add_favorite(username, "SELECT * FROM tabela", "Minha consulta")

# Buscar favoritos
favorites = get_favorites(username)

# Registrar padrão de busca
record_user_query(username, "consulta frequente")

# Buscar sugestões
suggestions = get_suggestions(username, "consulta parcial")
```

### Alertas
```python
# Criar alerta
add_alert(username, "Vendas Altas", "SELECT COUNT(*) FROM vendas", "threshold", 1000)

# Buscar alertas
alerts = get_alerts(username)

# Atualizar status
update_alert_status(alert_id, 'triggered')

# Excluir alerta
delete_alert(alert_id)
```

### Predições
```python
# Salvar predição
save_prediction(username, "vendas_proximo_mes", forecast_data, 0.85)

# Buscar predições
predictions = get_predictions(username)
```

## 🗂️ Arquivo de Banco

### Localização
```
SETORES_MODULOS/ROHDEN_AI/DATA/rohden_ai.db
```

### Configuração
- **Tipo**: SQLite
- **Row Factory**: `sqlite3.Row` (acesso por nome)
- **Foreign Keys**: Ativadas
- **Criação**: Automática na primeira execução

## 📈 Performance

### Índices Automáticos
- Chaves primárias (auto-incremento)
- Foreign keys em chats/messages
- Timestamps para consultas temporais

### Otimizações
- Conexões reutilizáveis
- Queries parametrizadas
- Batch operations onde possível

## 🔄 Integração

O módulo DATABASE é usado por:
- **IA_CORE/MEMORIA**: Para persistência de memórias
- **CHAT/routes.py**: Para endpoints de API
- **IA_CORE/ENGINE**: Para histórico de conversas

## 🧪 Testes

```python
# Testar conexão
from IA_CORE.PERSISTENCIA import get_db_connection
conn = get_db_connection()
print("Conectado:", conn is not None)

# Testar criação de chat
from IA_CORE.PERSISTENCIA import create_chat
chat_id = create_chat("test_user", "Test Chat")
print("Chat criado:", chat_id)

# Testar adicionar mensagem
from IA_CORE.PERSISTENCIA import add_message
add_message(chat_id, 'user', 'Mensagem de teste')
print("Mensagem adicionada")
```

## 🚀 Benefícios

- **Persistência**: Dados salvos permanentemente
- **Histórico**: Conversas mantidas para contexto
- **Performance**: SQLite rápido e leve
- **Portabilidade**: Banco em arquivo único
- **Escalabilidade**: Estrutura pronta para crescimento
