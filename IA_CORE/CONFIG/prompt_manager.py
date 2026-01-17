import os

def load_prompt(filename, **kwargs):
    """
    Carrega um prompt de um arquivo na pasta PROMPTS e substitui variáveis.
    """
    base_path = os.path.join(os.path.dirname(__file__), 'PROMPTS')
    file_path = os.path.join(base_path, filename)
    
    if not os.path.exists(file_path):
        print(f"⚠️ Prompt não encontrado: {filename}")
        return ""
        
    with open(file_path, 'r', encoding='utf-8') as f:
        content = f.read()
        
    if kwargs:
        try:
            return content.format(**kwargs)
        except KeyError as e:
            print(f"⚠️ Variável de prompt faltando: {e} em {filename}")
            return content
            
    return content
