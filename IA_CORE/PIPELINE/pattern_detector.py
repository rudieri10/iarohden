import re
import json
import logging
from collections import Counter
from ..STORAGE.simple_storage import SimpleStorage
from ..CORE.ai_client import AIClient

logger = logging.getLogger(__name__)

class PatternDetector:
    """
    DETECTOR: Identifica padrões no caos (logs).
    Gera hipóteses para serem validadas.
    """
    
    def __init__(self):
        self.storage = SimpleStorage()
        self.ai = AIClient()
        
    def analyze_recent_history(self):
        """Analisa logs recentes em busca de padrões."""
        logs = self.storage.get_recent_logs(limit=100)
        if not logs: return
        
        # 1. Análise de Erros Recorrentes
        errors = [l['error_message'] for l in logs if l['error_message']]
        if errors:
            common_errors = Counter(errors).most_common(3)
            for err, count in common_errors:
                self.storage.save_pattern(
                    type="ERROR_HOTSPOT",
                    desc=f"Erro frequente ({count}x): {err[:100]}...",
                    confidence=0.8
                )

        # 2. Análise de Filtros (SQL Parsing simplificado)
        sqls = [l['sql_text'] for l in logs if not l['error_message']]
        self._analyze_sql_patterns(sqls)

    def _analyze_sql_patterns(self, sqls: list):
        """Usa IA para detectar padrões semânticos em SQLs."""
        if not sqls: return
        
        sample = "\n".join(sqls[:10])
        prompt = f"""
        Analise estas consultas SQL recentes e identifique 3 padrões de uso.
        SQLs:
        {sample}
        
        Retorne JSON:
        {{
            "patterns": [
                {{"description": "Usuários sempre filtram por ATIVO='S'", "confidence": 0.9}},
                {{"description": "Tabela CLIENTES é frequentemente unida com VENDAS", "confidence": 0.8}}
            ]
        }}
        """
        
        result = self.ai.generate_json(prompt)
        if result and "patterns" in result:
            for p in result["patterns"]:
                self.storage.save_pattern(
                    type="USAGE_PATTERN",
                    desc=p.get("description"),
                    confidence=p.get("confidence", 0.5)
                )
