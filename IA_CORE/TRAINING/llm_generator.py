
import os
import json
import requests
from typing import List, Dict, Any
from ..ENGINE.vector_manager import VectorManager
from dotenv import load_dotenv

load_dotenv()

class LLMPatternGenerator:
    """
    Gerador de Padrões usando LLM Externo (Groq/Gemini).
    Usa a inteligência de modelos externos para criar padrões de imitação naturais
    durante a fase de treinamento, sem depender de LLM no chat em tempo real.
    """
    
    def __init__(self, storage):
        self.storage = storage
        self.vector_manager = VectorManager()
        self.api_key = os.getenv("GROQ_API_KEY") or os.getenv("API_KEY3") # Tenta chaves comuns no projeto
        self.api_url = "https://api.groq.com/openai/v1/chat/completions"
        self.model = os.getenv("GROQ_MODEL", "llama-3.3-70b-versatile")

    def generate_for_table(self, table_name: str, profile: Dict[str, Any], columns: List[Dict], sample_data: List[Dict], progress_callback=None):
        """
        Usa o Groq para gerar padrões de imitação baseados nos dados reais.
        """
        if not self.api_key:
            print("⚠️ Groq API Key não encontrada. Pulando geração por LLM.")
            return 0

        print(f"--- Gerando Padrões via LLM (Groq) para {table_name} ---")
        
        # Preparar contexto para o LLM
        schema_summary = ", ".join([f"{c['name']} ({c.get('type', 'TEXT')})" for c in columns])
        sample_json = json.dumps(sample_data[:5], indent=2) # Envia apenas 5 linhas de exemplo
        
        prompt = f"""
        Você é um especialista em treinamento de IA para o sistema Rohden.
        Seu objetivo é gerar 15 exemplos de interações entre um USUÁRIO e uma IA baseados nos dados reais da tabela '{table_name}'.
        
        ESTRUTURA DA TABELA:
        {schema_summary}
        
        AMOSTRA DE DADOS REAIS:
        {sample_json}
        
        REGRAS:
        1. Gere interações naturais (como um humano perguntaria).
        2. A resposta da IA deve ser informativa e direta, citando os dados da amostra.
        3. Se houver nomes, e-mails ou valores na amostra, use-os nos exemplos.
        4. O formato de saída deve ser estritamente um JSON contendo uma lista de objetos.
        5. Cada objeto deve ter: "user_input" (a pergunta) e "ai_response" (a resposta ideal).
        
        EXEMPLO DE SAÍDA:
        [
          {{"user_input": "Qual o e-mail do Rudieri?", "ai_response": "O e-mail do Rudieri cadastrado é rudieri@rohden.com.br."}},
          {{"user_input": "Buscar dados da empresa Rohden", "ai_response": "Localizei a Rohden no sistema. O CNPJ é 00.000.000/0001-00 e o status está Ativo."}}
        ]
        
        Gere 15 exemplos variados (busca por nome, busca por ID, busca por status, etc).
        Retorne APENAS o JSON.
        """

        headers = {
            "Authorization": f"Bearer {self.api_key}",
            "Content-Type": "application/json"
        }
        
        payload = {
            "model": self.model,
            "messages": [
                {"role": "system", "content": "Você é um gerador de dados sintéticos que responde apenas em JSON."},
                {"role": "user", "content": prompt}
            ],
            "temperature": 0.7,
            "response_format": {"type": "json_object"}
        }

        try:
            # Timeout reduzido para evitar esperas longas se o Groq estiver lento/fora
            response = requests.post(self.api_url, headers=headers, json=payload, timeout=25)
            response.raise_for_status()
            
            result = response.json()
            content = result['choices'][0]['message']['content']
            
            # Tentar parsear o conteúdo
            raw_data = json.loads(content)
            # Pode vir como {"patterns": [...]} ou direto como lista
            raw_patterns = raw_data if isinstance(raw_data, list) else raw_data.get('patterns', [])
            
            if not raw_patterns and isinstance(raw_data, dict):
                # Se for um dicionário com chaves aleatórias, pega a primeira lista que achar
                for val in raw_data.values():
                    if isinstance(val, list):
                        raw_patterns = val
                        break

            processed_patterns = []
            for i, p in enumerate(raw_patterns):
                if progress_callback:
                    progress_callback(i, len(raw_patterns), f"Vetorizando padrão LLM {i}/{len(raw_patterns)}...", 0)
                
                user_in = p.get('user_input')
                ai_out = p.get('ai_response')
                
                if user_in and ai_out:
                    vector = self.vector_manager.generate_embedding(user_in)
                    if vector:
                        processed_patterns.append({
                            "situation": f"LLM Generated Pattern for {table_name}",
                            "user_input": user_in,
                            "ai_action": "CHAT",
                            "ai_response": ai_out,
                            "category": "LLM_Generated",
                            "tags": f"{table_name}, llm, imitação",
                            "embedding_vector": self.vector_manager.vector_to_blob(vector)
                        })

            if processed_patterns:
                print(f"Salvando {len(processed_patterns)} padrões gerados por LLM...")
                self.storage.batch_save_behavioral_patterns(processed_patterns)
                return len(processed_patterns)
                
        except Exception as e:
            print(f"❌ Erro na geração via LLM: {e}")
            return 0
        
        return 0
