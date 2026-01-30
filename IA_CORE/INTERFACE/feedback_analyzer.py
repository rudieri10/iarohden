# -*- coding: utf-8 -*-
import re
from typing import Dict, Any, List, Optional

class FeedbackAnalyzer:
    """
    Detecta satisfação do usuário automaticamente com base na resposta à IA.
    Analisa sentimento, solicitações de clarificação e sinais de erro.
    """

    def __init__(self):
        # Sinais positivos fortes
        self.positive_signals = [
            'obrigado', 'obrigada', 'valeu', 'perfeito', 'exato', 
            'ajudou', 'ótimo', 'excelente', 'isso mesmo', 'correto',
            'show', 'top', 'boa', 'muito bom', 'tks', 'thx', 'brabo',
            'sensacional', 'maravilha', 'beleza'
        ]
        
        # Sinais negativos/insatisfação
        self.negative_signals = [
            'não era isso', 'errado', 'nada a ver', 'incorreto', 
            'confuso', 'não entendi', 'ruim', 'péssimo', 'não ajudou',
            'falhou', 'erro', 'mentira', 'alucinou', 'não funcionou',
            'diferente', 'nada disso'
        ]
        
        # Sinais de pedido de clarificação (não necessariamente negativo, mas requer atenção)
        self.clarification_signals = [
            'como assim', 'explique melhor', 'não entendi', 
            'pode detalhar', 'mais detalhes', 'o que significa',
            'como funciona', 'me explica', 'não ficou claro'
        ]

    def analyze_user_response(self, user_message: str, previous_ai_response: Optional[str] = None) -> Dict[str, Any]:
        """
        Analisa se o usuário ficou satisfeito com a resposta anterior.
        
        Args:
            user_message (str): A mensagem atual do usuário.
            previous_ai_response (str, optional): A resposta anterior da IA (para contexto).
            
        Returns:
            dict: {
                'sentiment': 'positive' | 'negative' | 'neutral' | 'clarification',
                'confidence': float (0.0 a 1.0),
                'detected_signals': list[str],
                'is_feedback': bool
            }
        """
        if not user_message:
            return self._default_result()

        message_lower = user_message.lower().strip()
        detected_signals = []
        
        # 1. Verificar sinais positivos
        for signal in self.positive_signals:
            # Busca palavra exata ou frase no início/fim/meio
            if re.search(r'\b' + re.escape(signal) + r'\b', message_lower):
                detected_signals.append(signal)
        
        if detected_signals:
            return {
                'sentiment': 'positive',
                'confidence': 0.9 if len(detected_signals) > 1 else 0.7,
                'detected_signals': detected_signals,
                'is_feedback': True
            }

        # 2. Verificar sinais negativos
        for signal in self.negative_signals:
            if re.search(r'\b' + re.escape(signal) + r'\b', message_lower):
                detected_signals.append(signal)
                
        if detected_signals:
             return {
                'sentiment': 'negative',
                'confidence': 0.9 if len(detected_signals) > 1 else 0.7,
                'detected_signals': detected_signals,
                'is_feedback': True
            }

        # 3. Verificar pedido de clarificação
        for signal in self.clarification_signals:
            if re.search(r'\b' + re.escape(signal) + r'\b', message_lower):
                detected_signals.append(signal)
                
        if detected_signals:
             return {
                'sentiment': 'clarification',
                'confidence': 0.8,
                'detected_signals': detected_signals,
                'is_feedback': True
            }

        # 4. Análise de sentimento básica para frases curtas (ex: "sim", "não")
        # Se a mensagem for muito curta e seguir uma pergunta da IA, pode ser feedback
        if len(message_lower.split()) <= 3:
            if message_lower in ['sim', 's', 'yes', 'aham', 'isso']:
                return {'sentiment': 'positive', 'confidence': 0.5, 'detected_signals': [message_lower], 'is_feedback': True}
            if message_lower in ['não', 'n', 'no', 'nops', 'nem']:
                return {'sentiment': 'negative', 'confidence': 0.5, 'detected_signals': [message_lower], 'is_feedback': True}

        # Default: Neutro/Não é feedback explícito
        return self._default_result()

    def _default_result(self) -> Dict[str, Any]:
        return {
            'sentiment': 'neutral',
            'confidence': 0.0,
            'detected_signals': [],
            'is_feedback': False
        }
