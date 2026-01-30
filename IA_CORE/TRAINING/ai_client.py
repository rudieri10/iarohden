import os
import json
import requests
import logging
import re
from typing import Optional, Dict, Any

logger = logging.getLogger(__name__)

class AIClient:
    """
    Cliente centralizado para interação com o motor de IA (Ollama/LLM).
    Gerencia conexões, timeouts e parsing de respostas.
    """
    
    def __init__(self):
        self.ai_url = os.getenv("ROHDEN_AI_INTERNAL_URL", "http://192.168.1.217:11434/api/generate")
        self.ai_model = os.getenv("ROHDEN_AI_MODEL", "qwen2.5:3b")
        
        self.session = requests.Session()
        self.session.trust_env = False
        self.session.headers.update({
            "Content-Type": "application/json",
            "Connection": "keep-alive"
        })
        
        logger.info(f"AIClient inicializado: {self.ai_url} model={self.ai_model}")

    def generate_json(self, prompt: str, timeout: int = 120, stream: bool = True) -> Any:
        """
        Gera uma resposta JSON da IA. Retorna Dict ou List dependendo da resposta.
        """
        try:
            payload = {
                "model": self.ai_model,
                "prompt": prompt,
                "stream": stream,
                "format": "json",
                "options": {"temperature": 0.2}
            }
            
            response = self.session.post(self.ai_url, json=payload, timeout=(5, timeout), stream=stream)
            response.raise_for_status()
            
            full_res = ""
            for line in response.iter_lines():
                if line:
                    try:
                        chunk = json.loads(line.decode('utf-8'))
                        text = chunk.get('response', '')
                        full_res += text
                        if chunk.get("done"): break
                    except:
                        continue
            
            if not full_res.strip():
                logger.warning("Resposta da IA vazia")
                return {}

            # Tentar limpar markdown se existir
            text = full_res.strip()
            if "```" in text:
                match = re.search(r'```(?:json)?\s*([\s\S]*?)\s*```', text)
                if match:
                    text = match.group(1)
            
            return json.loads(text)
            
        except Exception as e:
            logger.error(f"Erro na requisição à IA: {e}")
            return {}

    def generate_sql(self, prompt: str, timeout: int = 60) -> str:
        """
        Gera uma resposta de SQL (texto puro).
        """
        try:
            payload = {
                "model": self.ai_model,
                "prompt": prompt,
                "stream": False,
                "options": {"temperature": 0.1} 
            }
            
            response = self.session.post(self.ai_url, json=payload, timeout=(5, timeout))
            response.raise_for_status()
            
            result = response.json()
            text = result.get('response', '').strip()
            
            # Limpeza básica de markdown code blocks se houver
            text = text.replace("```sql", "").replace("```", "").strip()
            
            return text
            
        except Exception as e:
            logger.error(f"Erro ao gerar SQL via IA: {e}")
            return ""
