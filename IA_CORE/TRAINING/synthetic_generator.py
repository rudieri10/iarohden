import json
import re
import random
import time
from typing import List, Dict, Optional
from ..ENGINE.ai_engine import get_llama_engine

class SyntheticPatternGenerator:
    """
    Gerador de padrões de treinamento 100% autônomo via IA Local.
    Elimina heurísticas manuais e foca no entendimento da IA sobre os dados.
    """
    
    def __init__(self, storage=None):
        self.storage = storage
        self.engine = get_llama_engine()
        # Regex para detectar IDs (números com 3 ou mais dígitos que parecem chaves primárias)
        self.id_pattern = re.compile(r'\b\d{3,}\b')
        # Regex para detectar perguntas focadas puramente em IDs
        self.id_focus_pattern = re.compile(r'(buscar|ver|onde|quem|id|código|tabela).*\b\d{2,}\b', re.IGNORECASE)

    def generate_with_ai(self, table_name: str, columns: List[Dict], sample_data: List[Dict], profile: Dict = None, total_records: int = 0, progress_callback=None):
        """
        Geração 100% autônoma. A IA analisa a estrutura e dados para entender o propósito
        e gerar perguntas naturais que um usuário faria.
        """
        if not sample_data:
            print(f"⚠️ Sem dados de amostra para {table_name}. Abortando geração por IA.")
            return 0

        # Preparar o contexto da tabela
        schema_text = "\n".join([f"- {c['name']} ({c.get('type', 'TEXT')}): {c.get('comment', '')}" for c in columns])
        
        # Iniciar geração em lotes para estabilidade e diversidade
        all_patterns = self._generate_autonomous_batches(
            table_name, 
            schema_text, 
            sample_data, 
            progress_callback
        )
        
        if not all_patterns:
            return 0

        # Salvar padrões gerados
        count = 0
        for p in all_patterns:
            try:
                # Enriquecer com metadados obrigatórios
                p['table_name'] = table_name
                p['source'] = 'SAMUCA_AI_AUTONOMOUS'
                
                if self.storage:
                    self.storage.save_behavioral_pattern(p)
                count += 1
            except Exception as e:
                print(f"Erro ao salvar padrão: {e}")

        return count

    def _generate_autonomous_batches(self, table_name: str, schema_text: str, samples: List[Dict], progress_callback=None):
        """
        Divide a geração em lotes pequenos para não sobrecarregar o Samuca e garantir qualidade.
        """
        all_generated = []
        
        # Configuração de lotes para estabilidade (10 lotes de 5 amostras)
        num_batches = 10
        samples_per_batch = 5
        patterns_per_batch = 5
        
        system_prompt = f"""Você é o 'Samuca', o especialista em dados da Rohden.
Sua missão é ANALISAR a estrutura de uma tabela e uma amostra de dados reais para entender PARA QUE ela serve.
Após entender o propósito,gere resumidamente o proposito dela e tambem  gere perguntas e respostas NATURAIS que um funcionário da Rohden faria sobre esses dados.

REGRAS CRÍTICAS:
1. NUNCA use IDs ou códigos numéricos nas perguntas (ex: 'buscar 5215' é PROIBIDO).
2. Use dados que aparecem na amostra.
3. Se encontrar um ID na amostra, busque o NOME ou DESCRIÇÃO correspondente na mesma linha para usar na pergunta.
4. As perguntas devem ser variadas , faça o maximo possivel de perguntas diferentes.
5. Importante: Para perguntas de contagem, o ai_action deve ser 'DATA_ANALYSIS' e a ai_response deve focar no número total.
6. Responda SEMPRE em formato JSON puro, uma lista de objetos.
7. Cada objeto deve ter: user_input, ai_response, situation, category e ai_action: 'CHAT' ou 'DATA_ANALYSIS'.

Exemplo de formato esperado:
[
  {{
    "user_input": "Quem é o contato da empresa X?",
    "ai_response": "O contato principal é Fulano de Tal.",
    "situation": "Busca de contato por empresa",
    "category": "CONTATOS",
    "ai_action": "CHAT"
  }},
  {{
    "user_input": "Quantos contatos temos cadastrados?",
    "ai_response": "Atualmente temos 150 contatos registrados.",
    "situation": "Contagem total de registros",
    "category": "CONTATOS",
    "ai_action": "DATA_ANALYSIS"
  }}
]
"""

        for b in range(num_batches):
            try:
                msg = f"Samuca analisando Lote {b+1}/{num_batches} de {table_name}..."
                if progress_callback:
                    progress_callback(b, num_batches, msg)
                
                print(f"   -> {msg}")
                
                # Selecionar amostra aleatória para este lote
                batch_samples = random.sample(samples, min(samples_per_batch, len(samples)))
                
                prompt = f"""ANALISE ESTA TABELA: {table_name}
ESTRUTURA:
{schema_text}

AMOSTRA DE DADOS REAIS:
{json.dumps(batch_samples, ensure_ascii=False, indent=2, default=str)}

TAREFA:
1. Entenda o que esta tabela representa no contexto da Rohden.
2. Gere {patterns_per_batch} padrões de conversação (JSON) baseados nestes dados específicos.
3. Garanta que pelo menos 1 pergunta seja de CONTAGEM TOTAL ou SOMA (ex: 'quantos contatos...', 'qual o valor total...').
4. Foque em perguntas humanas reais. Ignore colunas técnicas de sistema (IDs internos, flags de deleção).
5. Converta IDs de sistema em nomes legíveis na pergunta.
"""

                # Chamada com limites conservadores para estabilidade
                response = self.engine._call_ai_with_limits(
                    prompt, 
                    system_prompt=system_prompt,
                    num_predict=1000,
                    num_ctx=4000
                )
                
                batch_patterns = self._extract_patterns_from_response(response)
                
                # Filtrar padrões inúteis (focados em ID ou vazios)
                filtered = [p for p in batch_patterns if not self._is_meaningless_pattern(p)]
                
                all_generated.extend(filtered)
                
                # Pequena pausa para o servidor respirar entre lotes
                time.sleep(1)
                
            except Exception as e:
                print(f"Erro no lote {b+1}: {e}")
                continue

        return all_generated

    def _is_meaningless_pattern(self, pattern: Dict) -> bool:
        """
        Detecta se um padrão gerado pela IA é inútil ou focado em IDs.
        """
        user_input = pattern.get('user_input', '').strip()
        
        if not user_input or len(user_input) < 5:
            return True
            
        # Bloquear perguntas que contêm IDs numéricos longos (3+ dígitos)
        if self.id_pattern.search(user_input):
            return True
            
        # Bloquear padrões mecânicos de busca por ID
        if self.id_focus_pattern.search(user_input):
            return True
            
        return False

    def _extract_patterns_from_response(self, response: str) -> List[Dict]:
        """
        Extrai e limpa a lista de padrões JSON da resposta da IA.
        """
        try:
            # Tentar encontrar o bloco JSON na resposta
            json_match = re.search(r'\[\s*\{.*\}\s*\]', response, re.DOTALL)
            if json_match:
                content = json_match.group(0)
            else:
                content = response
                
            # Limpezas comuns
            content = content.strip()
            if content.startswith('```json'):
                content = content[7:]
            if content.endswith('```'):
                content = content[:-3]
            
            data = json.loads(content)
            if isinstance(data, list):
                return data
            elif isinstance(data, dict):
                return [data]
            return []
        except Exception as e:
            print(f"Erro ao parsear JSON da IA: {e}")
            # Log do erro para debug se necessário
            return []

    def _clean_json_response(self, text: str) -> str:
        """Limpeza adicional se necessário"""
        return text.strip()
