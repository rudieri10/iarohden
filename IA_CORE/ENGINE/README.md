# AI_ENGINE - Motor Principal da IA Rohden

## 🧠 Descrição Geral

O `ai_engine.py` é o cérebro do sistema ROHDEN_AI, responsável por processar perguntas em linguagem natural, gerar consultas SQL, executá-las no banco Oracle e fornecer respostas inteligentes.

## ⚙️ Funcionalidades Principais

### 1. **Processamento de Linguagem Natural**
- Transforma perguntas em português para SQL Oracle
- Entende sinônimos e contexto empresarial
- Gera consultas SQL seguras e otimizadas

### 2. **Execução SQL Segura**
- **Apenas comandos SELECT** permitidos
- **Validação de segurança** contra SQL injection
- **Bloqueio de objetos do sistema** Oracle (SYS, SYSTEM)
- **Limite automático** de 100 linhas por consulta
- **Sanitização** de comandos perigosos

### 3. **Integração com Memória Conversacional**
- Incorpora contexto do usuário via `memoria_system`
- Adapta respostas baseadas no perfil e preferências
- Aprende continuamente com cada interação

### 4. **Geração de Respostas Inteligentes**
- Interpreta resultados SQL em linguagem natural
- Formata dados em tabelas Markdown
- Gera sugestões contextuais de próximos passos
- Suporta análises preditivas e alertas

## 🔧 Componentes Principais

### Classe `LlamaEngine`
```python
class LlamaEngine:
    def __init__(self)
    def get_user_permissions(username)
    def generate_response(prompt, username, history)
    def execute_sql(sql)
    def _call_ai(prompt, system_prompt)
```

### Fluxo de Processamento
1. **Verificação de permissões** do usuário
2. **Construção do contexto** (memória + banco de dados)
3. **Geração do prompt** enriquecido
4. **Chamada ao motor de IA** (Llama/Gemini)
5. **Execução SQL** (se necessário)
6. **Interpretação dos resultados**
7. **Aprendizado automático**

## 🛡️ Segurança

### Validações SQL
- ✅ Apenas `SELECT` permitidos
- ❌ `UNION`, `UPDATE`, `DELETE`, `DROP` bloqueados
- ❌ Objetos SYS/SYSTEM bloqueados
- ✅ `FETCH FIRST 100 ROWS ONLY` automático
- ✅ `UPPER()` para strings

### Controle de Acesso
- Permissões por tabela e usuário
- Níveis de acesso (1-5)
- Validação via tabela `AI_USER_TABLE_ACCESS`

## 🧮 Tags Especiais

### `[SQL]consulta[/SQL]`
Gera e executa consulta SQL automaticamente.

### `[LEARN]fato[/LEARN]`
Salva fato na memória de longo prazo.

### `[PREDICTION]análise[/PREDICTION]`
Envolve análise preditiva baseada em dados históricos.

### `[ALERT_SUGGESTION]titulo|sql|tipo|valor[/ALERT_SUGGESTION]`
Sugere criação de alertas inteligentes.

### `[SUGGESTIONS]perguntas[/SUGGESTIONS]`
Adiciona sugestões de próximos passos.

## 📊 Otimizações de Contexto

Para evitar erro de `context window` (2048 tokens):

- **Memória**: 3 memórias mais importantes
- **Banco**: 1 tabela × 3 colunas principais
- **Histórico**: 1 mensagem × 50 caracteres
- **Randomização**: Atualizações periódicas controladas
- **Aprendizado**: 20% das interações

## 🔗 Integrações

### Dependências
- `conecxaodb`: Conexão Oracle
- `IA_CORE.memoria_system`: Sistema de memória
- `google.generativeai`: Gemini (opcional)
- `llama_cpp`: Llama (principal)

### Conexões Externas
- **Banco Oracle**: Via `conecxaodb`
- **Motor Llama**: Via API HTTP
- **Configurações**: Arquivo `ai_config.json`

## 🚀 Performance

### Métricas
- **Tempo resposta**: < 15s (local) / < 120s (remoto)
- **Tokens utilizados**: ~500-800 por consulta
- **Precisão SQL**: 95%+ (com contexto adequado)

### Otimizações
- Cache de configuração (30s)
- Conexões reutilizáveis
- Prompt engineering otimizado
- Limites de segurança automáticos

## 🔄 Fluxo Completo

```
Pergunta do Usuário
        ↓
Verificar Permissões
        ↓
Construir Contexto (Memória + Banco)
        ↓
Gerar Prompt Enriquecido
        ↓
Chamar Motor de IA
        ↓
[SQL] Gerado? → Sim → Executar SQL
        ↓              ↓
        ←         Interpretar Resultados
        ↓
Adicionar Aprendizado
        ↓
Gerar Sugestões
        ↓
Resposta Final
```

## 🐛 Troubleshooting

### Erro Comum: `Requested tokens exceed context window`
**Solução**: Sistema otimizado automaticamente com:
- Contexto mínimo funcional
- Memória limitada
- Histórico reduzido

### Erro Comum: `Apenas SELECT permitidos`
**Comportamento esperado**: Medida de segurança

### Erro Comum: `Não foi possível conectar ao motor IA`
**Solução**: Verificar se serviço Llama está rodando

## 📈 Futuro

- Suporte a múltiplos bancos (PostgreSQL, MySQL)
- Processamento paralelo de consultas
- Cache inteligente de resultados
- Integração com mais modelos de IA
