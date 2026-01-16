# MEMORIA - Sistema de Memória Conversacional

## 🧠 O que faz

Módulo inteligente que aprende com cada interação do usuário, armazenando preferências, padrões de linguagem e contexto pessoal.

## 🎯 Funcionalidades

### 1. **Análise de Padrões**
- Detecta sinônimos: "cliente" = "contato" = "pessoa"
- Identifica intenções: "dinheiro em caixa" = consulta financeira
- Reconhece contexto temporal: "ontem", "semana passada"

### 2. **Perfil Comportamental**
- Estilo de interação (direto, formal, conversacional)
- Formato preferido de resposta (tabela, gráfico, resumo)
- Métricas de interesse do usuário

### 3. **Memória Contextual**
- Armazena fatos com importância e expiração
- Agrupa por tipo: preference, metric, feedback
- Mantém contexto relevante entre conversas

### 4. **Análise de Sentimento**
- Detecta satisfação: "perfeito", "obrigado", "ajudou"
- Identifica insatisfação: "não funcionou", "confuso"
- Reconhece repetição de perguntas

### 5. **Consolidação Inteligente**
- Agrupa memórias similares (70% similaridade)
- Remove duplicados automaticamente
- Resolve contradições de preferências

## 📊 Como Funciona

```
Interação do Usuário
        ↓
Análise de Padrões + Sentimento
        ↓
Extração de Aprendizado
        ↓
Atualização do Perfil
        ↓
Consolidação Periódica
```

## 🔧 Métodos Principais

```python
# Analisar padrões do usuário
patterns = memoria_system.analyze_user_patterns(username)

# Obter contexto para enriquecer respostas
context = memoria_system.get_user_memory_context(username)

# Adicionar memória manualmente
memoria_system.learn_contextual_fact(
    username, 
    "Prefere ver dados em tabela", 
    "preference", 
    3
)

# Consolidar memórias duplicadas
memoria_system.consolidate_memories(username)

# Análise de sentimento
score = memoria_system._analyze_sentiment(pergunta, resposta)

# Verificar repetição
is_repeated = memoria_system._is_repeated_question(username, pergunta)
```

## 📈 Estrutura de Dados

### Tabelas Criadas no SQLite
- `user_profile` - Perfil comportamental do usuário
- `contextual_memory` - Memórias contextuais com expiração
- `problem_context` - Problemas resolvidos e soluções
- `language_patterns` - Padrões de linguagem detectados

### Exemplo de Memória
```json
{
    "content": "Usuário prefere dados em formato de tabela",
    "context_type": "preference",
    "importance": 3,
    "expires_at": "2026-04-14"
}
```

## 🧪 Testes Específicos do Módulo

```python
# Testar detecção de padrões
patterns = memoria_system.analyze_user_patterns("usuario_teste")
print("Padrões:", patterns)

# Testar similaridade de textos
similarity = memoria_system._calculate_similarity(
    "cliente gosta de tabela", 
    "contato prefere tabela"
)
print(f"Similaridade: {similarity:.2f}")

# Testar análise de sentimento
sentiment = memoria_system._analyze_sentiment(
    "Perfeito, obrigado!", 
    "Aqui estão os dados"
)
print(f"Sentimento: {sentiment}")
```

## 📊 Otimizações de Performance

### Controle de Contexto
- **Memórias ativas**: 3 mais importantes
- **Problemas recentes**: 2 mais relevantes
- **Perfil**: Apenas dados essenciais

### Frequência de Atualização
- **Perfil**: 10% das interações
- **Aprendizado**: 20% das interações
- **Consolidação**: A cada 10 interações

## 🔄 Integração

O módulo MEMORIA é importado por:
- **IA_CORE/ENGINE**: Para enriquecer contexto da IA
- **CHAT/routes.py**: Para endpoints de API
- **Banco SQLite**: Para persistência de dados

## 🚀 Benefícios

- **Personalização**: Respostas adaptadas ao usuário
- **Contexto**: Memória mantida entre conversas
- **Aprendizado**: Sistema melhora com o uso
- **Organização**: Memórias consolidadas automaticamente
