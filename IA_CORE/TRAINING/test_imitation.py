
import sys
import os

# Adicionar o diretório raiz ao path para importações
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '../../../../')))

from SETORES_MODULOS.ROHDEN_AI.IA_CORE.ENGINE.behavior_manager import BehaviorManager

def test_imitation_learning():
    bm = BehaviorManager()
    
    # 1. Registrar alguns padrões se não existirem
    print("--- Registrando Padrões de Teste ---")
    bm.register_pattern(
        situation="Busca de contato único",
        user_input="telefone da maria silva",
        ai_action="direct_answer",
        ai_response="Maria Silva: (11) 88888-8888",
        category="Busca Direta"
    )
    
    bm.register_pattern(
        situation="Busca de contato ambígua",
        user_input="contato do bruno",
        ai_action="list_options_and_ask",
        ai_response="Encontrei 3 Brunos: Bruno Silva (Empresa A), Bruno Santos (Empresa B), Bruno Oliveira (Empresa C). Qual você precisa?",
        category="Busca Ambígua"
    )

    # 2. Testar busca de similares
    print("\n--- Testando Busca de Similares ---")
    query = "me passa o contato do bruno"
    similars = bm.find_similar_patterns(query)
    print(f"Query: {query}")
    for p in similars:
        print(f"Encontrado: {p['user_input']} -> {p['ai_response']}")

    # 3. Testar formatação de prompt
    print("\n--- Testando Formatação de Prompt ---")
    formatted = bm.format_patterns_for_prompt(query)
    print(formatted)

if __name__ == "__main__":
    test_imitation_learning()
