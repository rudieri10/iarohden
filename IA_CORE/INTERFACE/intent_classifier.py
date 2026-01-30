
import re
import json
from ..CORE.ai_client import AIClient

class IntentClassifier:
    """
    Classificador de Intenções para o Chatbot Corporativo.
    Combina regras heurísticas (regex) com análise de IA para determinar
    o objetivo do usuário.
    """

    def __init__(self):
        self.ai = AIClient()
        
        # 1. Definição de Regras Heurísticas (Regex)
        # Prioridade alta: Comandos claros
        self.patterns = {
            'greeting': [
                r'\b(oi|ola|olá|bom dia|boa tarde|boa noite|eai|eae|opa)\b',
                r'\b(tudo bem|como vai|como est[aá])\b'
            ],
            'business_data': [
                r'\b(vendas?|faturamento|lucro|receita|custo|margem|preço)\b',
                r'\b(cliente|fornecedor|produto|item|pedido|nota fiscal|nf)\b',
                r'\b(estoque|saldo|quantidade|qtd|total|quanto)\b',
                r'\b(relatório|lista|planilha|dados|tabela)\b',
                r'\b(compras?|gastos?|pagamentos?)\b',
                r'\b(contato|telefone|email|e-mail|celular|ramal|whatsapp|ligar para)\b'
            ],
            'analytical': [
                r'\b(analis(e|ar)|compar(ar|ativo)|diferença|evolução)\b',
                r'\b(por que|motivo|causa|expli(que|car))\b',
                r'\b(tendência|previsão|projeção|futuro)\b',
                r'\b(melhor|pior|maior|menor|top|ranking)\b'
            ],
            'operational': [
                r'\b(como (fazer|criar|gerar|cadastrar|alterar|excluir))\b',
                r'\b(processo|procedimento|passo a passo|tutorial)\b',
                r'\b(onde fica|caminho para|acessar)\b'
            ],
            'support': [
                r'\b(erro|bug|falha|problema|não funciona|travou)\b',
                r'\b(ajuda|socorro|suporte|dúvida)\b',
                r'\b(sistema|lento|caiu|fora do ar)\b'
            ],
            'casual_chat': [
                r'\b(obrigado|valeu|agradecido|grato|tks|thanks)\b',
                r'\b(piada|fale sobre você|quem é você|seupai)\b',
                r'\b(legal|bacana|interessante|show)\b'
            ]
        }

    def classify(self, message: str, context: dict = None) -> dict:
        """
        Classifica a intenção da mensagem do usuário.
        
        Args:
            message: Texto da mensagem
            context: Dicionário com contexto anterior (opcional)
            
        Returns:
            Dict com 'intent', 'confidence', 'method' e 'details'
        """
        message_lower = message.lower().strip()
        
        # 1. Tentativa Heurística (Rápida)
        heuristic_result = self._classify_heuristic(message_lower)
        
        # Se a confiança for alta, retorna direto
        if heuristic_result['confidence'] >= 0.8:
            return heuristic_result
            
        # 2. Se for ambíguo ou complexo, usa IA (Lenta mas precisa)
        # Se a mensagem for muito longa (> 10 palavras), provavelmente é complexa
        if len(message.split()) > 10 or heuristic_result['intent'] == 'unknown':
            return self._classify_with_ai(message, context)
            
        return heuristic_result

    def _classify_heuristic(self, message: str) -> dict:
        """Aplica regras de regex para classificar"""
        scores = {key: 0.0 for key in self.patterns.keys()}
        
        # Pontuação baseada em matches
        for intent, regex_list in self.patterns.items():
            for pattern in regex_list:
                if re.search(pattern, message):
                    scores[intent] += 1.0
                    
        # Refinamento de Pesos
        # Business data tem prioridade se tiver palavras fortes
        if scores['business_data'] > 0 and scores['greeting'] > 0:
            scores['greeting'] = 0 # Ignora "Oi, quero ver as vendas" como greeting
            
        # Se tiver conflito entre business_data e support (ex: "contato do suporte")
        # Support ganha se tiver palavras explícitas de erro/suporte
        if scores['support'] > 0 and scores['business_data'] > 0:
            # Verifica se tem palavras fortes de suporte na mensagem original
            if re.search(r'\b(suporte|erro|bug|falha|problema|ajuda)\b', message):
                scores['business_data'] = 0
            else:
                scores['support'] = 0
            
        # Encontrar a melhor pontuação
        best_intent = max(scores, key=scores.get)
        max_score = scores[best_intent]
        
        if max_score == 0:
            return {
                'intent': 'unknown',
                'confidence': 0.0,
                'method': 'heuristic',
                'details': scores
            }
            
        # Normalizar confiança (max 0.9 para deixar margem pra dúvida)
        confidence = min(0.9, 0.5 + (max_score * 0.2))
        
        return {
            'intent': best_intent,
            'confidence': confidence,
            'method': 'heuristic',
            'details': scores
        }

    def _classify_with_ai(self, message: str, context: dict) -> dict:
        """Usa o LLM para classificar intenções complexas"""
        
        prompt = f"""
        Atue como um classificador de intenções para um sistema corporativo.
        
        Categorias possíveis:
        - greeting: Saudações, cumprimentos simples.
        - business_data: Solicitação de dados, relatórios, números, SQL.
        - analytical: Pedido de análise, comparação, explicação de dados.
        - operational: Dúvidas sobre como usar o sistema ou processos.
        - support: Relato de erros, problemas técnicos.
        - casual_chat: Conversa informal, agradecimentos, papo furado.
        - complex_query: Perguntas que misturam várias categorias ou requerem raciocínio profundo.
        
        Mensagem do Usuário: "{message}"
        
        Responda APENAS um JSON no formato:
        {{
            "intent": "categoria_escolhida",
            "confidence": 0.0 a 1.0,
            "reason": "breve explicação"
        }}
        """
        
        try:
            response = self.ai.generate_text(prompt, temperature=0.1) # Baixa temperatura para precisão
            # Limpar markdown
            response = response.replace("```json", "").replace("```", "").strip()
            result = json.loads(response)
            result['method'] = 'ai'
            return result
        except Exception as e:
            print(f"⚠️ Erro no classificador IA: {e}")
            # Fallback seguro
            return {
                'intent': 'complex_query', # Assume complexo se falhar
                'confidence': 0.5,
                'method': 'fallback',
                'error': str(e)
            }
