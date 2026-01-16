# DATA - Sistema de Armazenamento

## 🗄️ O que faz

Sistema profissional e robusto para armazenar configurações, metadados e conhecimento da IA Rohden.

## 🏗️ Estrutura do Banco de Dados

### 📊 Tabelas Principais

#### **configurations**
Armazenamento de configurações do sistema:
```sql
CREATE TABLE configurations (
    id INTEGER PRIMARY KEY,
    key TEXT UNIQUE NOT NULL,
    value TEXT NOT NULL,
    value_type TEXT DEFAULT 'string',
    created_at TIMESTAMP,
    updated_at TIMESTAMP
)
```

#### **table_metadata**
Metadados das tabelas disponíveis:
```sql
CREATE TABLE table_metadata (
    id INTEGER PRIMARY KEY,
    table_name TEXT NOT NULL,
    table_description TEXT,
    schema_info TEXT,
    columns_info TEXT,
    sample_data TEXT,
    is_active BOOLEAN DEFAULT 1,
    created_at TIMESTAMP,
    updated_at TIMESTAMP
)
```

#### **knowledge_base**
Base de conhecimento da IA:
```sql
CREATE TABLE knowledge_base (
    id INTEGER PRIMARY KEY,
    category TEXT NOT NULL,
    title TEXT NOT NULL,
    content TEXT NOT NULL,
    tags TEXT,
    priority INTEGER DEFAULT 1,
    is_active BOOLEAN DEFAULT 1,
    created_at TIMESTAMP,
    updated_at TIMESTAMP
)
```

#### **change_logs**
Registro de alterações para auditoria:
```sql
CREATE TABLE change_logs (
    id INTEGER PRIMARY KEY,
    table_name TEXT,
    action TEXT,
    old_value TEXT,
    new_value TEXT,
    user_name TEXT,
    timestamp TIMESTAMP
)
```

## 🔧 Como Usar

### 📥 Importação
```python
from IA_CORE.DATA import (
    get_config, save_config, load_tables, save_tables,
    get_table_metadata, save_table_metadata,
    get_knowledge, save_knowledge
)
```

### 💾 Configurações
```python
# Salvar configuração
save_config('ai_model', 'llama-3.1b', 'string')

# Obter configuração
model = get_config('ai_model', 'default')

# Salvar configuração complexa
save_config('ai_settings', {
    'temperature': 0.1,
    'max_tokens': 512
}, 'json')
```

### 📊 Tabelas
```python
# Carregar todas as tabelas
tables = load_tables()

# Salvar configuração de tabelas
save_tables([{
    'table_name': 'TB_CONTATOS',
    'table_description': 'Tabela de contatos',
    'columns_info': [...],
    'sample_data': [...]
}])
```

### 🧠 Conhecimento
```python
# Salvar conhecimento
save_knowledge(
    category='sql',
    title='Como contar registros',
    content='Use SELECT COUNT(*) FROM tabela',
    tags='sql,contagem,básico',
    priority=2
)

# Obter conhecimento
knowledge = get_knowledge(category='sql', limit=10)
```

## 🚀 Benefícios

### ✅ **Profissional**
- **SQLite**: Banco de dados robusto e confiável
- **Tipos de dados**: Suporte a JSON, int, float, bool
- **Auditoria**: Registro completo de alterações
- **Performance**: Índices otimizados

### 📈 **Estruturado**
- **Separação clara**: Configurações, metadados, conhecimento
- **Versionamento**: Controle de alterações
- **Backup**: Exportação/importação completa

### 🔒 **Seguro**
- **SQL Injection Protection**: Usando parâmetros
- **Transações ACID**: Integridade dos dados
- **Validação**: Tipos de dados verificados

## 📁 Estrutura de Arquivos

```
IA_CORE/DATA/
├── __init__.py          # Exportações
├── storage.py           # Sistema principal
├── rohden_ai.db         # Banco SQLite
└── README.md           # Documentação
```

## 🔄 Migração do JSON

### 📥 De JSON para SQLite
```python
# Exportar configuração antiga
import json
with open('ai_config.json', 'r') as f:
    old_config = json.load(f)

# Importar para novo sistema
from IA_CORE.DATA import import_config
import_config(old_config)
```

### 📤 De SQLite para JSON
```python
# Exportar configuração atual
from IA_CORE.DATA import export_config
config = export_config()

# Salvar como JSON
with open('ai_config_backup.json', 'w') as f:
    json.dump(config, f, indent=2)
```

## 🎯 Exemplos Práticos

### 💾 Configurar Tabela
```python
from IA_CORE.DATA import save_table_metadata

metadata = {
    'table_name': 'TB_CONTATOS',
    'table_description': 'Tabela de contatos da empresa',
    'columns_info': [
        {'name': 'ID', 'type': 'NUMBER', 'description': 'ID do contato'},
        {'name': 'NOME', 'type': 'VARCHAR', 'description': 'Nome do contato'},
        {'name': 'EMAIL', 'type': 'VARCHAR', 'description': 'Email do contato'},
        {'name': 'TELEFONE', 'type': 'VARCHAR', 'description': 'Telefone do contato'}
    ],
    'sample_data': [
        {'ID': 1, 'NOME': 'João Silva', 'EMAIL': 'joao@email.com', 'TELEFONE': '11999999999'},
        {'ID': 2, 'NOME': 'Maria Santos', 'EMAIL': 'maria@email.com', 'TELEFONE': '11888888888'}
    ]
}

save_table_metadata('TB_CONTATOS', metadata)
```

### 🧠 Adicionar Conhecimento
```python
# Exemplos de SQL para a IA aprender
examples = [
    {
        'category': 'sql_basic',
        'title': 'Contar todos os registros',
        'content': 'SELECT COUNT(*) FROM tabela',
        'tags': 'sql,count,basico',
        'priority': 3
    },
    {
        'category': 'sql_filter',
        'title': 'Filtrar por nome',
        'content': 'SELECT * FROM tabela WHERE UPPER(NOME) LIKE UPPER("%joão%")',
        'tags': 'sql,filter,nome',
        'priority': 2
    },
    {
        'category': 'sql_aggregate',
        'title': 'Calcular média',
        'content': 'SELECT AVG(valor) FROM tabela',
        'tags': 'sql,aggregate,média',
        'priority': 2
    }
]

for example in examples:
    save_knowledge(**example)
```

## 🔍 Consultas Avançadas

### 📊 Buscar por Categoria
```python
# Buscar conhecimento sobre SQL
sql_knowledge = get_knowledge(category='sql')

# Buscar conhecimento de alta prioridade
high_priority = get_knowledge(limit=20)
```

### 📈 Metadados de Tabela
```python
# Obter metadados específicos
contatos_meta = get_table_metadata('TB_CONTATOS')

# Listar colunas disponíveis
columns = contatos_meta['columns_info']
for col in columns:
    print(f"{col['name']} ({col['type']}): {col['description']}")
```

## 🚀 Performance

### ⚡ Otimizações
- **Índices automáticos** em chaves primárias
- **Cache interno** para consultas frequentes
- **Conexão pool** para múltiplas operações
- **Transações** em lote para operações em massa

### 📊 Capacidade
- **Milhões de registros** sem problemas
- **Consultas complexas** com JOINs e subqueries
- **Backup automático** via exportação

## 🔧 Manutenção

### 🗃️ Backup
```python
# Backup completo
from IA_CORE.DATA import export_config
backup = export_config()

# Salvar backup com timestamp
from datetime import datetime
backup_file = f'backup_{datetime.now().strftime("%Y%m%d_%H%M%S")}.json'
with open(backup_file, 'w') as f:
    json.dump(backup, f, indent=2)
```

### 🧹 Limpeza
```python
# Limpar conhecimento antigo
from IA_CORE.DATA import storage
conn = sqlite3.connect(storage.db_path)
cursor = conn.cursor()
cursor.execute("DELETE FROM knowledge_base WHERE created_at < date('now', '-90 days')")
conn.commit()
conn.close()
```

## 📈 Evolução Futura

### 🔮 Próximas Versões
- **Cache distribuído** para múltiplas instâncias
- **Replicação** para alta disponibilidade
- **API REST** para acesso externo
- **Dashboard** para administração visual
- **Versionamento** automático de schema

**Sistema de dados profissional e escalável para a IA Rohden!** 🚀
