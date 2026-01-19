import json
from datetime import datetime, date

# Configurações de Temperatura para diferentes estados da IA
AI_TEMPERATURE_SETTINGS = {
    "STRICT": 0.0,       # Para SQL, cálculos e extração de dados (Zero alucinação)
    "TECHNICAL": 0.1,    # Para explicações técnicas e documentação
    "NORMAL": 0.2,       # Equilíbrio padrão para o dia a dia
    "FRIENDLY": 0.4,     # Respostas mais cordiais e empáticas
    "CREATIVE": 0.5,     # Temperatura '5' (0.5) para sugestões e refinamentos
    "BRAINSTORM": 0.7,   # Para geração de ideias e novos conceitos
    "INSPIRED": 0.9      # Máxima criatividade (pode variar mais as respostas)
}

def handle_excessive_results(prompt, results, engine_instance):
    """
    Função para quando houver muitos resultados. 
    A IA pergunta ao usuário como ele deseja filtrar os dados, usando apenas contexto de dados.
    """
    total_results = len(results)
    
    # Consideramos "muitos resultados" acima de 15 registros
    if total_results > 15:
        # Extrai uma amostra dos campos e dados para ajudar a IA a perguntar
        sample_data = results[:3]
        fields = list(results[0].keys()) if results else []
        
        # Converte datetime para string para serialização JSON
        def datetime_serializer(obj):
            if isinstance(obj, (datetime, date)):
                return obj.isoformat()
            raise TypeError(f"Type {type(obj)} not serializable")

        # Converte a amostra para formatos serializáveis
        serializable_sample = []
        for row in sample_data:
            new_row = {}
            for k, v in row.items():
                if hasattr(v, 'isoformat'): # datetime ou date
                    new_row[k] = v.isoformat()
                else:
                    new_row[k] = v
            serializable_sample.append(new_row)

        user_prompt = (
            f"Contexto: {total_results} resultados encontrados.\n"
            f"Campos: {fields}\n"
            f"Amostra: {json.dumps(serializable_sample, ensure_ascii=False)}"
        )
        
        # Chamada sem system prompt instrutivo, apenas temperatura criativa
        return engine_instance._call_ai_with_limits(
            user_prompt, 
            "", # System prompt vazio
            temperature=AI_TEMPERATURE_SETTINGS["CREATIVE"],
            num_predict=250
        )
    
    return None
