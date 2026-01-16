# Como Testar o Sistema de Memória Conversacional

## 📋 Pré-requisitos

1. **Sistema ROHDEN_AI funcionando**
2. **Usuário logado no sistema**
3. **Acesso apenas à tabela TB_CONTATOS** (única tabela liberada)

## 🧪 Testes Manuais via Interface Web

### 1. Teste Básico de Memória (Apenas Contatos)
```bash
# Perguntas sobre a tabela disponível:
"Quantos contatos temos na base?"
"Mostrar todos os contatos em formato de tabela"
"Quais são os contatos cadastrados?"
"Listar nomes dos contatos"
"Buscar contato pelo nome Lucas"
```

### 2. Teste de Sinônimos (Contexto de Contatos)
```bash
# Teste diferentes termos para contatos:
"Quantos clientes cadastrados?"      # Deve reconhecer como "contato"
"Mostrar lista de pessoas"          # Deve reconhecer como "contato"
"Quem são os compradores?"          # Deve reconhecer como "contato"
"Ver todos os contatos"             # Termo direto
```

### 3. Teste de Contexto Temporal (com Contatos)
```bash
# Teste expressões temporais (se houver data de cadastro):
"Contatos cadastrados ontem"
"Novos contatos da semana passada"
"Contatos do mês anterior"
```

### 4. Teste de Análise de Sentimento
```bash
# Teste feedback positivo:
"Perfeito, obrigado!"
"Excelente, funcionou bem"
"Show, encontrei o contato"

# Teste feedback negativo:
"Não funcionou"
"Não encontrei o contato"
"Está confuso, tente de novo"
```

### 5. Teste de Formato de Resposta
```bash
# Teste preferências de formato:
"Mostrar contatos em tabela"
"Quero ver os dados em formato visual"
"Resumir os contatos"
```

## 🔧 Testes via API (Postman/cURL)

### 1. Verificar Perfil do Usuário
```bash
curl -X GET "http://localhost:5000/memory/profile" \
  -H "Cookie: session=<seu_session_id>"
```

### 2. Verificar Contexto Atual
```bash
curl -X GET "http://localhost:5000/memory/context" \
  -H "Cookie: session=<seu_session_id>"
```

### 3. Adicionar Memória Manualmente
```bash
curl -X POST "http://localhost:5000/memory/learn" \
  -H "Content-Type: application/json" \
  -H "Cookie: session=<seu_session_id>" \
  -d '{
    "content": "Prefere relatórios visuais com gráficos",
    "context_type": "preference",
    "importance": 4,
    "expires_days": 30
  }'
```

### 4. Forçar Consolidação de Memórias
```bash
curl -X POST "http://localhost:5000/memory/consolidate" \
  -H "Cookie: session=<seu_session_id>"
```

### 5. Analisar Sentimento Recente
```bash
curl -X GET "http://localhost:5000/memory/sentiment" \
  -H "Cookie: session=<seu_session_id>"
```

## 🧪 Testes Automáticos (Python)

### Script de Teste Completo
```python
import requests
import json

class TestMemorySystem:
    def __init__(self, base_url, session_cookie):
        self.base_url = base_url
        self.session = requests.Session()
        self.session.cookies.set('session', session_cookie)
    
    def test_profile(self):
        """Testa perfil do usuário"""
        response = self.session.get(f"{self.base_url}/memory/profile")
        print("Perfil:", response.json())
        return response.status_code == 200
    
    def test_context(self):
        """Testa contexto de memória"""
        response = self.session.get(f"{self.base_url}/memory/context")
        print("Contexto:", response.json())
        return response.status_code == 200
    
    def test_add_memory(self):
        """Testa adicionar memória"""
        data = {
            "content": "Usuário prefere dados em formato de resumo curto",
            "context_type": "preference",
            "importance": 3
        }
        response = self.session.post(f"{self.base_url}/memory/learn", json=data)
        print("Memória adicionada:", response.json())
        return response.status_code == 200
    
    def test_consolidation(self):
        """Testa consolidação"""
        response = self.session.post(f"{self.base_url}/memory/consolidate")
        print("Consolidação:", response.json())
        return response.status_code == 200
    
    def test_sentiment(self):
        """Testa análise de sentimento"""
        response = self.session.get(f"{self.base_url}/memory/sentiment")
        print("Sentimento:", response.json())
        return response.status_code == 200
    
    def run_all_tests(self):
        """Executa todos os testes"""
        tests = [
            ("Perfil", self.test_profile),
            ("Contexto", self.test_context),
            ("Adicionar Memória", self.test_add_memory),
            ("Consolidação", self.test_consolidation),
            ("Sentimento", self.test_sentiment)
        ]
        
        results = []
        for name, test_func in tests:
            try:
                result = test_func()
                results.append((name, "PASS" if result else "FAIL"))
            except Exception as e:
                results.append((name, f"ERROR: {e}"))
        
        print("\n=== RESULTADOS ===")
        for name, status in results:
            print(f"{name}: {status}")

# Uso:
# tester = TestMemorySystem("http://localhost:5000", "sua_sessao")
# tester.run_all_tests()
```

## 🔍 Testes de Validação

### 1. Verificar Tabelas no Banco
```sql
-- Verificar se tabelas foram criadas
.tables

-- Verificar dados de perfil
SELECT * FROM user_profile WHERE user_name = 'seu_usuario';

-- Verificar memórias contextuais
SELECT * FROM contextual_memory WHERE user_name = 'seu_usuario' ORDER BY created_at DESC;

-- Verificar padrões de linguagem
SELECT * FROM language_patterns WHERE user_name = 'seu_usuario';

-- Verificar problemas resolvidos
SELECT * FROM problem_context WHERE user_name = 'seu_usuario';
```

### 2. Testar Detecção de Padrões
```python
# Teste específico de padrões
from memoria_conversacional import memoria_system

# Analisar padrões de um usuário
patterns = memoria_system.analyze_user_patterns("seu_usuario")
print("Padrões detectados:", json.dumps(patterns, indent=2))
```

### 3. Testar Similaridade
```python
# Testar cálculo de similaridade
similarity = memoria_system._calculate_similarity(
    "cliente preferem tabela", 
    "contato gosta de tabela visual"
)
print(f"Similaridade: {similarity:.2f}")
```

## 📊 Cenários de Teste Específicos (Apenas Contatos)

### Cenário 1: Detecção de Sinônimos de Contatos
1. Faça 3 perguntas usando "contato"
2. Faça 2 perguntas usando "cliente"  
3. Faça 2 perguntas usando "pessoa"
4. Verifique se o sistema agrupa como mesmo conceito

### Cenário 2: Consolidação de Memória de Formato
1. Faça perguntas sobre "tabela" 3 vezes
2. Faça perguntas sobre "resumo" 2 vezes
3. Execute consolidação
4. Verifique se restou apenas 1 memória consolidada por formato

### Cenário 3: Análise de Sentimento com Contatos
1. Faça perguntas sobre contatos com feedback positivo ("perfeito", "obrigado")
2. Faça perguntas sobre contatos com feedback negativo ("não funcionou")
3. Verifique score de sentimento

### Cenário 4: Detecção de Repetição
1. Faça "Quantos contatos temos?" 2 vezes em 24h
2. Verifique se sistema detectou repetição
3. Confirme se memória de feedback foi criada

### Cenário 5: Preferências de Formato
1. Peça "Mostrar contatos em tabela"
2. Peça "Ver contatos visualmente" 
3. Verifique se sistema aprendeu preferência

## 🚨 Verificação de Erros Comuns

### 1. Erro de Importação
```bash
# Verificar se módulos importam corretamente
python -c "from memoria_conversacional import memoria_system; print('OK')"
```

### 2. Erro no Banco
```bash
# Verificar se banco SQLite está acessível
python -c "from db_history import get_db_connection; conn = get_db_connection(); print('DB OK')"
```

### 3. Erro na API
```bash
# Verificar se endpoints respondem
curl -X GET "http://localhost:5000/memory/profile" -v
```

## 📈 Métricas de Sucesso (Contexto Contatos)

- **Perfil criado**: ✅ user_profile tem dados sobre contatos
- **Memórias armazenadas**: ✅ contextual_memory tem registros sobre preferências
- **Padrões detectados**: ✅ query_frequency mostra "contato" como principal
- **Sinônimos funcionando**: ✅ "cliente" e "pessoa" agrupados
- **Consolidação funcionando**: ✅ memórias duplicadas removidas
- **Sentimento analisado**: ✅ feedback com scores
- **API respondendo**: ✅ todos endpoints HTTP 200

## 🔄 Teste Contínuo

Para testes automatizados contínuos:
```bash
# Criar script de teste agendado
python test_memory_system.py
```

## 💡 Dicas para Testes com Apenas Contatos

1. **Varie as perguntas**: Use diferentes termos para a mesma coisa
2. **Teste formatos**: Peça tabela, resumo, visual
3. **Dê feedback**: Use "obrigado", "perfeito", "não funcionou"
4. **Repita perguntas**: Teste detecção de repetição
5. **Verifique perfil**: Use os endpoints para ver o que aprendeu

Execute estes testes focados em contatos para validar o sistema antes de liberar mais tabelas!
