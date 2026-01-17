import json

# Configurações de Temperatura para diferentes estados e personalidades da IA
AI_TEMPERATURE_SETTINGS = {
    "STRICT": 0.0,       # Para SQL, cálculos e extração de dados (Zero alucinação)
    "TECHNICAL": 0.1,    # Para explicações técnicas e documentação
    "NORMAL": 0.2,       # Equilíbrio padrão para o dia a dia
    "FRIENDLY": 0.4,     # Respostas mais cordiais e empáticas
    "CREATIVE": 0.5,     # Temperatura '5' (0.5) para sugestões e refinamentos
    "BRAINSTORM": 0.7,   # Para geração de ideias e novos conceitos
    "INSPIRED": 0.9      # Máxima criatividade (pode variar mais as respostas)
}

# Definições de Personalidade (System Prompts baseados no estado)
AI_PERSONALITIES = {
    "DATA_EXPERT": "Você é o Especialista em Dados da Rohden. Sua linguagem é precisa, direta e baseada em fatos.",
    "HELPER": "Você é o Assistente prestativo da Rohden. Você é educado, usa emojis moderadamente e busca sempre facilitar a vida do usuário.",
    "ARCHITECT": "Você é o Arquiteto de Sistemas da Rohden. Você pensa em estrutura, segurança e performance em cada resposta.",
    "RECRUITER": "Você é o Especialista em Pessoas da Rohden. Sua fala é acolhedora, humana e foca no bem-estar dos colaboradores.",
    "MOTIVATIONAL": "Você é o Coach de Inovação da Rohden. Você encoraja novas ideias, é entusiasta e foca em soluções criativas."
}

def get_personality(name: str) -> tuple:
    """Retorna o prompt da personalidade e a temperatura sugerida"""
    mapping = {
        "DATA": (AI_PERSONALITIES["DATA_EXPERT"], AI_TEMPERATURE_SETTINGS["STRICT"]),
        "CHAT": (AI_PERSONALITIES["HELPER"], AI_TEMPERATURE_SETTINGS["NORMAL"]),
        "TECH": (AI_PERSONALITIES["ARCHITECT"], AI_TEMPERATURE_SETTINGS["TECHNICAL"]),
        "HR": (AI_PERSONALITIES["RECRUITER"], AI_TEMPERATURE_SETTINGS["FRIENDLY"]),
        "IDEA": (AI_PERSONALITIES["MOTIVATIONAL"], AI_TEMPERATURE_SETTINGS["BRAINSTORM"])
    }
    return mapping.get(name.upper(), (AI_PERSONALITIES["HELPER"], AI_TEMPERATURE_SETTINGS["NORMAL"]))

def handle_excessive_results(prompt, results, engine_instance):
    """
    Função para quando houver muitos resultados. 
    Em vez de responder algo vazio ou truncado, a IA pergunta ao usuário 
    como ele deseja filtrar os dados, usando uma temperatura mais alta (0.5).
    """
    total_results = len(results)
    
    # Consideramos "muitos resultados" acima de 15 registros
    if total_results > 15:
        sys_prompt = (
            "Você é o Rohden AI. Encontramos muitos registros no banco de dados que correspondem à sua busca. "
            "Sua tarefa é ser proativo e perguntar ao usuário como ele deseja filtrar esses resultados para ser mais específico. "
            "Dê exemplos do que foi encontrado (ex: nomes, cidades, tipos) e peça mais detalhes. "
            "NUNCA responda apenas que não encontrou ou mostre uma lista gigante. "
            "Mantenha um tom amigável e prestativo."
        )
        
        # Extrai uma amostra dos campos e dados para ajudar a IA a perguntar
        sample_data = results[:3]
        fields = list(results[0].keys()) if results else []
        
        user_prompt = (
            f"Pergunta original: '{prompt}'\n"
            f"Quantidade encontrada: {total_results} registros.\n"
            f"Campos disponíveis na tabela: {fields}\n"
            f"Amostra dos dados encontrados: {json.dumps(sample_data, ensure_ascii=False)}\n\n"
            "Com base nisso, faça uma pergunta inteligente ao usuário para ajudá-lo a filtrar o que ele realmente precisa."
        )
        
        # Chamada com temperatura 0.5 (CREATIVE)
        return engine_instance._call_ai_with_limits(
            user_prompt, 
            sys_prompt, 
            temperature=AI_TEMPERATURE_SETTINGS["CREATIVE"],
            num_predict=250
        )
    
    return None
