import os
import json
import requests
import logging
import re
from typing import Optional, Dict, Any, Union
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

logger = logging.getLogger(__name__)

class AIClient:
    """
    Cliente simplificado para interação com LLM (Ollama).
    Focado em robustez e simplicidade.
    """
    
    def __init__(self):
        self.ai_url = os.getenv("ROHDEN_AI_INTERNAL_URL", "http://192.168.1.217:11434/api/generate")
        self.ai_model = os.getenv("ROHDEN_AI_MODEL", "qwen2.5:3b")
        
        self.session = requests.Session()
        self.session.trust_env = False
        
        # Configurar estratégia de retentativa (Retry)
        retry_strategy = Retry(
            total=3,  # Tentar 3 vezes
            backoff_factor=0.5,  # Esperar 0.5s, 1s, 2s...
            status_forcelist=[429, 500, 502, 503, 504],
            allowed_methods=["HEAD", "GET", "OPTIONS", "POST"],
            connect=True,
            read=True
        )
        adapter = HTTPAdapter(max_retries=retry_strategy)
        self.session.mount("http://", adapter)
        self.session.mount("https://", adapter)
        
        self.session.headers.update({
            "Content-Type": "application/json",
            "Connection": "keep-alive"
        })
        
    def generate_json(self, prompt: str, timeout: int = 120) -> Union[Dict, list]:
        """Gera resposta JSON limpa e validada."""
        try:
            payload = {
                "model": self.ai_model,
                "prompt": prompt,
                "stream": False,
                "format": "json",
                "options": {"temperature": 0.1}
            }
            
            response = self.session.post(self.ai_url, json=payload, timeout=timeout)
            response.raise_for_status()
            
            result = response.json()
            content = result.get('response', '{}')
            
            # Limpeza de markdown se necessário
            if "```" in content:
                match = re.search(r'```(?:json)?\s*([\s\S]*?)\s*```', content)
                if match:
                    content = match.group(1)
            
            return json.loads(content)
            
        except Exception as e:
            logger.error(f"Erro AI (JSON): {e}")
            return {}

    def generate_text(self, prompt: str, timeout: int = 60) -> str:
        """Gera resposta texto simples."""
        try:
            payload = {
                "model": self.ai_model,
                "prompt": prompt,
                "stream": False,
                "options": {"temperature": 0.3}
            }
            
            response = self.session.post(self.ai_url, json=payload, timeout=timeout)
            response.raise_for_status()
            
            return response.json().get('response', '').strip()
            
        except Exception as e:
            logger.error(f"Erro AI (Texto): {e}")
            return f"Erro: {str(e)}"
