# IA_CORE - Núcleo do Sistema de IA Rohden

## 📁 Estrutura Organizada

```
IA_CORE/
├── README.md                    # Documentação geral
├── __init__.py                  # Exportações principais
├── ENGINE/                      # Motor de Processamento
│   ├── README.md                # Documentação do motor
│   ├── __init__.py              # Exportações do engine
│   └── ai_engine.py            # Motor principal da IA
├── MEMORIA/                     # Módulo de Memória
│   ├── README.md                # Documentação da memória
│   ├── __init__.py              # Exportações do módulo
│   └── memoria_conversacional.py # Sistema de memória conversacional
└── DATABASE/                    # Sistema de Persistência
    ├── README.md                # Documentação do banco
    ├── __init__.py              # Exportações do database
    └── db_history.py            # Gerenciamento SQLite
```

## 🧠 Módulos

### ENGINE/ - Motor Principal
- **ai_engine.py**: Cérebro do sistema, processamento NLP, geração SQL
- **Funcionalidades**: Processamento de linguagem, execução SQL segura, integração com IA
- **Segurança**: Validação SQL, controle de acesso, limites automáticos

### MEMORIA/ - Sistema de Memória
- **memoria_conversacional.py**: Memória inteligente e aprendizado
- **Funcionalidades**: Detecção de padrões, análise de sentimento, perfil de usuário
- **Otimizações**: Consolidação automática, contexto limitado

### DATABASE/ - Sistema de Persistência
- **db_history.py**: Gerenciamento completo de banco SQLite
- **Funcionalidades**: Histórico de conversas, favoritos, alertas, predições
- **Estrutura**: 6 tabelas principais com relacionamentos

## 🔧 Importação

```python
# Importação principal (tudo do IA_CORE)
from IA_CORE import (
    get_llama_engine, memoria_system, LlamaEngine, MemoriaConversacional,
    get_db_connection, create_chat, get_user_chats, add_message
)

# Importação específica
from IA_CORE.ENGINE import get_llama_engine, LlamaEngine
from IA_CORE.MEMORIA import memoria_system, MemoriaConversacional
from IA_CORE.PERSISTENCIA import get_db_connection, create_chat, add_message
```

## 📊 Funcionalidades Integradas

### Motor de IA (ENGINE)
- Processamento de linguagem natural
- Geração de SQL Oracle seguro
- Execução e interpretação de resultados
- Tags especiais ([SQL], [LEARN], [PREDICTION])

### Memória Conversacional (MEMORIA)
- Análise de padrões de usuário
- Detecção de sinônimos e intenções
- Consolidação de memórias similares
- Análise de sentimento básica

### Persistência de Dados (DATABASE)
- Histórico completo de conversas
- Consultas favoritas e sugestões
- Alertas inteligentes automáticos
- Predições e análises salvas

## 🚀 Integração

O sistema está integrado ao:
- **CHAT/routes.py** - Endpoints da API Flask
- **CONFIG_ROHDEN_AI/** - Configurações do sistema
- **DATA/** - Arquivos de configuração e banco

## 📈 Performance Otimizada

### Controle de Contexto
- **Memória**: 3 memórias mais importantes
- **Banco**: 1 tabela × 3 colunas principais
- **Histórico**: 1 mensagem × 50 caracteres
- **Randomização**: Atualizações controladas

### Segurança
- Apenas comandos SELECT permitidos
- Bloqueio de objetos SYS/SYSTEM
- Limite automático de 100 linhas
- Validação contra SQL injection

### Persistência
- SQLite para portabilidade
- Índices automáticos
- Conexões reutilizáveis
- Backup automático

## 🔮 Expansão Futura

Estrutura preparada para novos módulos:
- **PROCESSAMENTO/** - NLP avançado
- **APRENDIZADO/** - Machine learning
- **ANALISE/** - Análise preditiva
- **INTEGRACAO/** - APIs externas

## 🧪 Testes

Use o guia `TESTE_MEMORIA.md` para testar:
- Memória conversacional
- Detecção de padrões
- Análise de sentimento
- Integração completa
- Persistência de dados
