import random
import json
import re
from typing import List, Dict, Any
from ..ENGINE.vector_manager import VectorManager

class SyntheticPatternGenerator:
    """
    Gerador 100% Autônomo de Padrões de Treinamento via IA Local (Samuca).
    A IA analisa a estrutura, entende o propósito e gera perguntas naturais sem intervenção humana.
    """
    
    def __init__(self, storage):
        self.storage = storage
        self.vector_manager = VectorManager()

    def generate_with_ai(self, table_name: str, columns: List[Dict], sample_data: List[Dict], profile: Dict = None, total_records: int = 0, progress_callback=None):
        """
        Geração Principal Autônoma.
        """
        from ..ENGINE.ai_engine import LlamaEngine
        engine = LlamaEngine()
        
        print(f"--- Iniciando Geração 100% Autônoma para {table_name} ---")
        if progress_callback:
            progress_callback(0, 100, f"Samuca analisando {table_name}...", 0)

        # Preparar contexto técnico puro (sem dicas manuais)
        schema_summary = []
        for c in columns:
            col_name = c['name']
            col_info = f"- {col_name} ({c.get('type', 'TEXT')})"
            schema_summary.append(col_info)
        
        schema_text = "\n".join(schema_summary)
        samples = sample_data[:60] # Amostra generosa para a IA entender o contexto
        
        # PROMPT 100% AUTÔNOMO: A IA deve descobrir o que a tabela faz.
        system_prompt = """Você é um Analista de Dados Sênior da Rohden.
Sua tarefa é analisar a estrutura e os dados de uma tabela e gerar padrões de treinamento para uma IA de atendimento.

REGRAS CRÍTICAS DE NEGÓCIO:
1. ANALISE PRIMEIRO: Olhe para as colunas e os dados da amostra. Entenda o propósito real desta tabela (ex: é uma tabela de preços? de estoque? de contatos? de log?). Não assuma nada antes de olhar os dados.
2. PERGUNTAS NATURAIS: Gere perguntas que um usuário real faria (ex: "Quem é...", "Qual o contato de...", "Quais os pedidos de...").
3. PROIBIDO IDs: NUNCA use IDs numéricos (ex: 5215, 102, 001) nas perguntas. Um usuário nunca pergunta por ID. Converta IDs em nomes reais presentes nos dados.
4. FOCO EM DADOS TEXTUAIS: Busque por nomes, descrições, e-mails, endereços e informações que humanos entendem.
5. RESPOSTA RICA: A 'ai_response' deve ser completa e informativa, usando os dados da amostra.
6. ZERO HEURÍSTICA: Não use padrões pré-definidos. Deixe a IA decidir o que é importante.
"""

        try:
            # Geração em lotes para garantir qualidade e evitar timeouts do Ollama
            all_patterns = self._generate_autonomous_batches(table_name, columns, samples, engine)
            
            if not all_patterns:
                print(f"⚠️ Samuca não conseguiu gerar padrões autônomos para {table_name}.")
                return 0

            print(f"🧠 Samuca gerou {len(all_patterns)} padrões autônomos de alta qualidade.")
            
            # Vetorizar e Salvar
            total_final = len(all_patterns)
            final_patterns_with_vectors = []
            
            for idx, p in enumerate(all_patterns):
                if progress_callback and idx % 10 == 0:
                    progress_callback(50 + int((idx/total_final)*45), 100, f"Vetorizando {idx+1}/{total_final}...", 0)
                
                # Filtro final de segurança contra IDs lixo
                if self._is_junk_id_query(p.get('user_input', '')):
                    continue

                vector = self.vector_manager.generate_embedding(p['user_input'])
                if vector:
                    p['embedding_vector'] = self.vector_manager.vector_to_blob(vector)
                    final_patterns_with_vectors.append(p)

            if final_patterns_with_vectors:
                print(f"✅ Salvando {len(final_patterns_with_vectors)} padrões 100% autônomos...")
                self.storage.batch_save_behavioral_patterns(final_patterns_with_vectors)
                return len(final_patterns_with_vectors)
            
            return 0

        except Exception as e:
            print(f"❌ Erro na geração autônoma: {e}")
            return 0

    def _generate_autonomous_batches(self, table_name, columns, samples, engine) -> List[Dict]:
        """Gera em lotes para manter a autonomia sem estourar o contexto da IA"""
        all_p = []
        schema_text = "\n".join([f"- {c['name']} ({c.get('type', 'TEXT')})" for c in columns])
        
        system_prompt = "Você é um Analista de Dados. Entenda a tabela e gere perguntas e respostas REAIS sem usar IDs."

        # Realiza 4 tentativas (lotes) para diversificar os dados usados
        for b in range(4):
            print(f"   -> Samuca analisando Lote {b+1}/4...")
            # Pega uma fatia diferente dos dados em cada lote
            start_idx = (b * 15) % len(samples)
            batch_samples = samples[start_idx : start_idx + 15]
            
            prompt = f"""Tabela: {table_name}
Estrutura: {schema_text}
Amostra: {json.dumps(batch_samples, ensure_ascii=False)}

MISSÃO:
1. Gere 20 padrões JSON variados.
2. Cada padrão deve ter: user_input, ai_response, situation, category, ai_action: CHAT.
3. FOCO EM NOMES E TEXTOS, NUNCA EM IDs.
4. Converta qualquer ID da amostra no nome correspondente na pergunta.
"""
            
            try:
                resp = engine._call_ai_with_limits(prompt, system_prompt, 4000, 12000)
                extracted = self._extract_patterns_from_response(resp)
                if extracted:
                    # Filtrar IDs imediatamente
                    valid = [p for p in extracted if not self._is_junk_id_query(p.get('user_input', ''))]
                    all_p.extend(valid)
            except Exception as e:
                print(f"      ! Falha no lote {b+1}: {e}")
                continue
                
        return all_p

    def _is_junk_id_query(self, text: str) -> bool:
        """Detecta se a pergunta é apenas um ID numérico ou busca por ID (lixo de treinamento)"""
        if not text: return True
        
        # Se tem 3 ou mais números seguidos e pouca letra, é provável que seja busca por ID
        # Ex: "buscar 5215", "quem é 102", "ID 999"
        numbers = re.findall(r'\d{2,}', text) # Reduzi para 2 números para ser mais rígido
        
        # Se a string é muito curta e contém números, bloqueia
        if numbers and len(text) < 25:
            return True
            
        # Bloqueia se contiver a palavra ID seguida de números
        if re.search(r'ID\s*\d+', text, re.IGNORECASE):
            return True
            
        # Palavras chave de busca por ID
        bad_patterns = ['BUSCAR ID', 'QUEM É ID', 'PESQUISAR ID', 'ID ', 'CÓDIGO', 'CODIGO']
        if any(x in text.upper() for x in bad_patterns):
            # Se tiver a palavra código/ID mas for uma pergunta longa e natural, talvez seja válida
            # Mas se for curta, bloqueia.
            if len(text) < 30:
                return True
        
        return False

    def _extract_patterns_from_response(self, text: str) -> List[Dict]:
        """Extrai JSON da resposta da IA de forma robusta"""
        if not text: return []
        
        results = []
        # Tenta encontrar o array completo
        array_match = re.search(r'\[\s*\{.*\}\s*\]', text, re.DOTALL)
        if array_match:
            try:
                data = json.loads(array_match.group(0))
                if isinstance(data, list):
                    results = data
            except:
                pass
        
        # Se falhou, tenta objetos individuais
        if not results:
            objs = re.findall(r'\{[^{}]*\}', text, re.DOTALL)
            for obj_str in objs:
                try:
                    item = json.loads(obj_str)
                    if isinstance(item, dict) and 'user_input' in item:
                        results.append(item)
                except:
                    continue
        
        # Validação básica
        valid = []
        for item in results:
            if isinstance(item, dict) and item.get('user_input') and item.get('ai_response'):
                valid.append(item)
        
        return valid
