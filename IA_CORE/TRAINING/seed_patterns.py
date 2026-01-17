
import sys
import os
import random

# Adicionar o diretório raiz ao path para importações
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '../../../../')))

from SETORES_MODULOS.ROHDEN_AI.IA_CORE.DATA.storage import DataStorage
from SETORES_MODULOS.ROHDEN_AI.IA_CORE.ENGINE.vector_manager import VectorManager

def generate_mass_patterns():
    storage = DataStorage()
    vector_manager = VectorManager()
    patterns = []
    
    # ... (listas omitidas para brevidade) ...
    
    # Listas expandidas para gerar milhares de variações únicas
    first_names = [
        "Bruno", "Maria", "João", "Ana", "Pedro", "Carla", "Ricardo", "Fernanda", 
        "Lucas", "Juliana", "Marcos", "Patrícia", "Roberto", "Sônia", "Diego", "Aline",
        "Gabriel", "Beatriz", "Rafael", "Letícia", "Gustavo", "Camila", "Felipe", "Larissa",
        "Tiago", "Vanessa", "Leonardo", "Eduarda", "Marcelo", "Tatiane", "Rodrigo", "Priscila",
        "André", "Mônica", "Fernando", "Cláudia", "Daniel", "Renata", "Alexandre", "Sandra",
        "Vitor", "Simone", "Paulo", "Lúcia", "José", "Tereza", "Antônio", "Francisca",
        "Luiz", "Margarida", "Francisco", "Rosa", "Carlos", "Ana Paula", "Mário", "Márcia"
    ]
    
    last_names = [
        "Kestring", "Silva", "Santos", "Oliveira", "Souza", "Pereira", "Lima", "Ferreira",
        "Alves", "Rodrigues", "Costa", "Barbosa", "Gomes", "Martins", "Araújo", "Cardoso",
        "Teixeira", "Melo", "Cabral", "Ribeiro", "Vieira", "Carvalho", "Freitas", "Lopes",
        "Moura", "Nascimento", "Mendes", "Moreira", "Nunes", "Pinto", "Rocha", "Soares"
    ]
    
    depts = [
        "Vendas", "RH", "Estoque", "Financeiro", "Logística", "TI", "Produção", "Comercial",
        "Marketing", "Jurídico", "Qualidade", "Manutenção", "Expedição", "Compras", "Projetos",
        "Engenharia", "Segurança", "Faturamento", "Contabilidade", "Auditoria", "PCP"
    ]
    
    cities = [
        "São Paulo", "Rio de Janeiro", "Curitiba", "Joinville", "Belo Horizonte", "Porto Alegre",
        "Salvador", "Fortaleza", "Brasília", "Manaus", "Recife", "Goiânia", "Belém", "Campinas",
        "São Luís", "Maceió", "Natal", "Teresina", "João Pessoa", "Aracaju", "Cuiabá"
    ]
    
    products = [
        "Porta", "Janela", "Batente", "Guarnição", "Rodapé", "Painel", "Fechadura", "Dobradiça",
        "Vidro", "Verniz", "Selador", "Cola", "Prego", "Parafuso", "Puxador", "Trilho",
        "Eucalipto", "Pínus", "MDF", "Compensado", "Lâmina", "Borda", "Montante", "Travessa"
    ]

    # 1. BUSCA AMBÍGUA (1500 novos padrões)
    print("Gerando 1500 padrões de Busca Ambígua...")
    for _ in range(1500):
        name = random.choice(first_names)
        dept1 = random.choice(depts)
        dept2 = random.choice(depts)
        while dept1 == dept2: dept2 = random.choice(depts)
        
        patterns.append({
            'situation': 'multiple_results_found',
            'user_input': f"quem é o {name.lower()}",
            'ai_action': 'list_options_and_ask',
            'ai_response': f"Encontrei dois colaboradores com o nome {name}: um no setor de {dept1} e outro no {dept2}. Qual deles você gostaria de contatar?",
            'category': 'Busca Ambígua',
            'success_indicator': 'user_provided_refinement'
        })

    # 2. BUSCA DIRETA (1500 novos padrões)
    print("Gerando 1500 padrões de Busca Direta...")
    for _ in range(1500):
        name = random.choice(first_names)
        surname = random.choice(last_names)
        ext = random.randint(1000, 9999)
        email = f"{name.lower()}.{surname.lower()}@rohden.com.br"
        
        patterns.append({
            'situation': 'single_result_found',
            'user_input': f"contato de {name.lower()} {surname.lower()}",
            'ai_action': 'direct_answer',
            'ai_response': f"O contato de {name} {surname} é o ramal {ext} ou pelo e-mail {email}.",
            'category': 'Busca Direta',
            'success_indicator': 'info_delivered'
        })

    # 3. PERGUNTA DE DADOS (1000 novos padrões)
    print("Gerando 1000 padrões de Pergunta de Dados...")
    for _ in range(1000):
        target = random.choice(products + depts)
        val = random.randint(5, 2000)
        unit = "unidades" if target in products else "colaboradores"
        
        patterns.append({
            'situation': 'data_aggregation',
            'user_input': f"total de {target.lower()}",
            'ai_action': 'direct_answer',
            'ai_response': f"O sistema registra um total de {val} {unit} para {target}.",
            'category': 'Pergunta de Dados',
            'success_indicator': 'metric_delivered'
        })

    # 4. CICLO DE VIDA E STATUS (500 novos padrões)
    print("Gerando 500 padrões de Ciclo de Vida...")
    for _ in range(500):
        item_id = random.randint(50000, 99999)
        stage = random.choice(["Triagem", "Aprovação", "Produção", "Embalagem", "Expedição", "Entregue"])
        city = random.choice(cities)
        
        patterns.append({
            'situation': 'lifecycle_tracking',
            'user_input': f"onde está a carga {item_id}",
            'ai_action': 'direct_answer',
            'ai_response': f"A carga {item_id} está atualmente na fase de {stage} em {city}.",
            'category': 'Pergunta de Dados',
            'success_indicator': 'location_delivered'
        })

    # 5. HISTÓRICO E AUDITORIA (500 novos padrões)
    print("Gerando 500 padrões de Histórico...")
    for _ in range(500):
        user = random.choice(first_names)
        action = random.choice(["alterou", "deletou", "criou", "atualizou"])
        record = random.randint(100, 500)
        
        patterns.append({
            'situation': 'audit_check',
            'user_input': f"quem {action} o registro {record}",
            'ai_action': 'direct_answer',
            'ai_response': f"De acordo com o log de auditoria, o usuário {user} {action} o registro {record} na data de hoje.",
            'category': 'Pergunta de Dados',
            'success_indicator': 'audit_delivered'
        })

    # Salvar em massa (Total 5000 novos)
    print(f"Vetorizando e salvando {len(patterns)} novos padrões no banco de dados...")
    
    vectorized_patterns = []
    for p in patterns:
        vector = vector_manager.generate_embedding(p['user_input'])
        if vector:
            p['embedding_vector'] = vector_manager.vector_to_blob(vector)
            vectorized_patterns.append(p)
        else:
            print(f"⚠️ Falha ao vetorizar: {p['user_input']}")
            
    if vectorized_patterns:
        storage.batch_save_behavioral_patterns(vectorized_patterns)
        print(f"✅ Semeadura massiva ({len(vectorized_patterns)}) concluída com sucesso!")
    else:
        print("❌ Falha crítica: Nenhum padrão foi vetorizado.")

if __name__ == "__main__":
    generate_mass_patterns()
