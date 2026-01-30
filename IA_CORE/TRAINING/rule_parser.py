"""
RULE PARSER - Parser Robusto para Regras de Negócio da IA
Trata saídas instáveis do Qwen2.5-3B e normaliza para formato padrão.
"""

import re
import json
import logging
from datetime import datetime
from typing import Dict, List, Optional, Union, Tuple

logger = logging.getLogger(__name__)

# --- ORACLE SQL TRANSLATION HELPERS ---

def translate_to_oracle(sql: str) -> Tuple[str, List[str]]:
    """Traduz sintaxe genérica/MySQL para Oracle SQL."""
    changes = []
    new_sql = sql
    
    # Fix REGEXP_LIKE: CHECK (col REGEXP_LIKE 'pattern') -> CHECK (REGEXP_LIKE(col, 'pattern', 'i'))
    pattern1 = r"(\w+)\s+REGEXP_LIKE\s+'([^']+)'"
    def fix_regexp(match):
        changes.append("Fixed REGEXP_LIKE syntax")
        return f"REGEXP_LIKE({match.group(1)}, '{match.group(2)}', 'i')"
    
    if re.search(pattern1, new_sql, re.IGNORECASE):
        new_sql = re.sub(pattern1, fix_regexp, new_sql, flags=re.IGNORECASE)
    
    # Fix LIMIT (not supported in Oracle constraints/simple queries)
    if ' LIMIT ' in new_sql.upper():
        changes.append("Removed LIMIT clause")
        new_sql = re.sub(r'\s+LIMIT\s+\d+', '', new_sql, flags=re.IGNORECASE)
        
    # Fix TEXT -> CLOB
    if ' TEXT' in new_sql.upper():
        changes.append("Converted TEXT to CLOB")
        new_sql = re.sub(r'\sTEXT\b', ' CLOB', new_sql, flags=re.IGNORECASE)
        
    # Fix BOOLEAN -> NUMBER(1)
    if ' BOOLEAN' in new_sql.upper():
        changes.append("Converted BOOLEAN to NUMBER(1)")
        new_sql = re.sub(r'\sBOOLEAN\b', ' NUMBER(1)', new_sql, flags=re.IGNORECASE)

    return new_sql, changes

def is_oracle_syntax_error(error_message: str) -> bool:
    """Verifica se o erro é de sintaxe SQL do Oracle."""
    # ORA-009xx são erros de sintaxe (missing keyword, invalid identifier, etc)
    # ORA-01735: invalid ALTER TABLE option
    syntax_codes = ['ORA-009', 'ORA-01735', 'ORA-01747', 'ORA-02438']
    return any(code in str(error_message).upper() for code in syntax_codes)

def is_ignorable_error(error_message: str) -> bool:
    """Verifica se o erro pode ser ignorado (ex: constraint já existe)."""
    # ORA-02260: table can have only one primary key
    # ORA-02261: such unique or primary key already exists
    # ORA-02275: such a referential constraint already exists
    # ORA-01430: column being added already exists in table
    # ORA-01442: column to be modified to NOT NULL is already NOT NULL
    ignorable = ['ORA-02260', 'ORA-02261', 'ORA-02275', 'ORA-01430', 'ORA-01442', 'ORA-02264']
    return any(code in str(error_message).upper() for code in ignorable)

def generate_retry_prompt(sql: str, error: str) -> str:
    """Gera prompt para a IA corrigir o SQL."""
    return f"""
The following Oracle SQL query failed:
SQL: {sql}
Error: {error}

Please fix the SQL syntax for Oracle Database 19c.
Ensure correct syntax for ALTER TABLE, CHECK constraints, and data types.
Return ONLY the corrected SQL query.
"""

def safe_parse_rule(rule_input: Union[str, Dict]) -> Dict:
    """
    Parser robusto para regras retornadas pela IA.
    Normaliza chaves variáveis e trata strings puras.
    
    Args:
        rule_input: Saída da IA (dict ou string)
    
    Returns:
        Dict com formato padronizado: {"description": str, "sql": str, "type": str}
    """
    
    # 1. Se for string, tentar extrair JSON ou identificar SQL puro
    if isinstance(rule_input, str):
        rule_input = _extract_json_from_string(rule_input) or rule_input
        
        # Se ainda for string, tentar identificar SQL
        if isinstance(rule_input, str):
            parsed = _parse_sql_from_string(rule_input)
            # Traduzir SQL se necessário
            if parsed.get('sql'):
                translated, _ = translate_to_oracle(parsed['sql'])
                parsed['sql'] = translated
            return parsed
    
    # 2. Se for dict, normalizar chaves
    if isinstance(rule_input, dict):
        parsed = _normalize_rule_dict(rule_input)
        # Traduzir SQL se necessário
        if parsed.get('sql'):
            translated, _ = translate_to_oracle(parsed['sql'])
            parsed['sql'] = translated
        return parsed
    
    # 3. Fallback para texto não estruturado
    return {
        "description": str(rule_input)[:200],
        "sql": "",
        "type": "unstructured"
    }

def _extract_json_from_string(text: str) -> Optional[Dict]:
    """Extrai JSON de texto que pode ter explicações fora."""
    # Procura por conteúdo entre chaves
    json_match = re.search(r'\{.*\}', text, re.DOTALL)
    
    if json_match:
        json_str = json_match.group(0)
        try:
            # Tenta parse direto
            return json.loads(json_str)
        except json.JSONDecodeError:
            try:
                # Tenta substituir aspas simples por duplas
                json_str_fixed = json_str.replace("'", '"')
                return json.loads(json_str_fixed)
            except json.JSONDecodeError:
                logger.warning(f"JSON malformado encontrado: {json_str[:100]}...")
                return None
    
    return None

def _normalize_rule_dict(rule_dict: Dict) -> Dict:
    """Normaliza chaves variáveis para formato padrão."""
    
    # Mapeamento de chaves possíveis para chaves padrão
    sql_keys = ['sql_rule', 'validation_sql', 'sql', 'sql_validation_expression', 'sql_check_suggestion']
    desc_keys = ['description', 'rule', 'business_rule', 'rule_description', 'original']
    type_keys = ['rule_type', 'type']
    
    # Extrai SQL
    sql = ""
    for key in sql_keys:
        if key in rule_dict and rule_dict[key]:
            sql = str(rule_dict[key]).strip()
            break
    
    # Extrai descrição
    description = ""
    for key in desc_keys:
        if key in rule_dict and rule_dict[key]:
            description = str(rule_dict[key]).strip()
            break
    
    # Se não tiver descrição, usa o dict inteiro como descrição
    if not description:
        description = str(rule_dict)[:200]
    
    # Extrai tipo
    rule_type = "custom"
    for key in type_keys:
        if key in rule_dict and rule_dict[key]:
            rule_type = str(rule_dict[key]).lower().strip()
            break
    
    # Sanitização de datas fixas
    sql, date_warnings = _sanitize_fixed_dates(sql)
    if date_warnings:
        logger.warning(f"Data fixa detectada em regra: {date_warnings}")
    
    # Se não tem SQL, tenta inferir da descrição
    if not sql:
        sql = _infer_sql_from_description(description)
    
    return {
        "description": description,
        "sql": sql,
        "type": rule_type
    }

def _parse_sql_from_string(text: str) -> Dict:
    """Identifica SQL em texto puro e retorna estrutura padrão."""
    text_upper = text.upper().strip()
    
    # Padrões SQL comuns
    sql_patterns = [
        r'(SELECT\s+.+)',
        r'(ALTER\s+TABLE\s+\w+\s+ADD\s+CONSTRAINT.+)',
        r'(CHECK\s*\(.+\))',
        r'(NOT\s+NULL)',
        r'(UNIQUE)',
        r'(PRIMARY\s+KEY)',
        r'(FOREIGN\s+KEY)',
        r'(REFERENCES\s+.+)'
    ]
    
    sql_match = None
    for pattern in sql_patterns:
        match = re.search(pattern, text_upper, re.IGNORECASE)
        if match:
            sql_match = match.group(1)
            break
    
    # Se encontrou SQL, usa-o
    if sql_match:
        sql, date_warnings = _sanitize_fixed_dates(sql_match)
        if date_warnings:
            logger.warning(f"Data fixa detectada: {date_warnings}")
        
        # Tenta inferir tipo
        rule_type = _infer_sql_type(sql_match)
        
        return {
            "description": text[:200],
            "sql": sql,
            "type": rule_type
        }
    
    # Se não encontrou SQL, retorna como texto não estruturado
    return {
        "description": text[:200],
        "sql": "",
        "type": "unstructured"
    }

def _sanitize_fixed_dates(sql: str) -> tuple[str, List[str]]:
    """
    Identifica e sanitiza datas fixas em SQL.
    Retorna tuple: (sql_sanitizado, lista_de_avisos)
    """
    warnings = []
    
    # Padrão para datas ISO (YYYY-MM-DD ou YYYY-MM-DDTHH:MM:SS)
    date_pattern = r"'(\d{4}-\d{2}-\d{2})(?:T(\d{2}:\d{2}:\d{2}))?'"
    
    current_year = datetime.now().year
    
    def replace_date(match):
        date_part = match.group(1)
        time_part = match.group(2)
        
        # Verifica se é data atual ou futura próxima
        try:
            year = int(date_part.split('-')[0])
            if year >= current_year:
                warnings.append(f"Data fixa detectada: {date_part}")
                # Substitui por parâmetro (ex: :data_param)
                return ":date_param"
        except:
            pass
        
        # Mantém a data original se não for problemática
        if time_part:
            return f"TO_TIMESTAMP('{date_part} {time_part}', 'YYYY-MM-DD HH24:MI:SS')"
        return f"TO_DATE('{date_part}', 'YYYY-MM-DD')"
    
    sanitized_sql = re.sub(date_pattern, replace_date, sql)
    return sanitized_sql, warnings

def _infer_sql_from_description(description: str) -> str:
    """Tenta inferir SQL a partir da descrição em português."""
    desc_upper = description.upper()
    
    # NOT NULL
    if any(term in desc_upper for term in ["NÃO PODE SER NULO", "NÃO PODE CONTER NULOS", "NOT NULL"]):
        col_match = re.search(r'(?:COLUNA|CAMPO)\s+([A-Z0-9_]+)', desc_upper)
        if col_match:
            return f"{col_match.group(1)} IS NOT NULL"
    
    # Valores permitidos
    if "VALORES PERMITIDOS" in desc_upper or "DEVE CONTER" in desc_upper:
        col_match = re.search(r'(?:COLUNA|CAMPO)\s+([A-Z0-9_]+)', desc_upper)
        vals = re.findall(r"'([^']+)'", description)
        if col_match and vals:
            vals_str = ", ".join([f"'{v}'" for v in vals])
            return f"{col_match.group(1)} IN ({vals_str})"
    
    # Range/Between
    if "ENTRE" in desc_upper or "BETWEEN" in desc_upper:
        col_match = re.search(r'(?:COLUNA|CAMPO)\s+([A-Z0-9_]+)', desc_upper)
        vals = re.findall(r"(\d+(?:\.\d+)?)", description)
        if col_match and len(vals) >= 2:
            return f"{col_match.group(1)} BETWEEN '{vals[0]}' AND '{vals[1]}'"
    
    return ""

def _infer_sql_type(sql: str) -> str:
    """Infere o tipo da regra a partir do SQL."""
    sql_upper = sql.upper()
    
    if "NOT NULL" in sql_upper:
        return "not_null"
    elif "UNIQUE" in sql_upper:
        return "unique"
    elif "PRIMARY KEY" in sql_upper:
        return "primary_key"
    elif "FOREIGN KEY" in sql_upper or "REFERENCES" in sql_upper:
        return "foreign_key"
    elif "CHECK" in sql_upper or "IN (" in sql_upper:
        return "check"
    elif "BETWEEN" in sql_upper:
        return "range"
    else:
        return "custom"

# Função para atualizar o prompt do sistema
def get_normalized_system_prompt() -> str:
    """Retorna o prompt de sistema normalizado para evitar variações."""
    return """Sua saída deve ser estritamente um JSON Schema. Nunca use chaves diferentes de 'description' e 'sql_rule'. Não use datas estáticas em constraints de CHECK. Se não houver regra, retorne uma lista vazia []."""

class SQLGenerator:
    """Gerador de SQL de validação para regras parseadas."""
    
    def _fix_oracle_syntax(self, sql_cond: str) -> str:
        """Aplica correções específicas para dialeto Oracle (Datas, etc)."""
        if not sql_cond: return sql_cond
        
        # 1. Corrigir Datas ISO para TO_DATE ou TO_TIMESTAMP
        # Ex: '2025-12-04T11:16:48' -> TO_TIMESTAMP('2025-12-04 11:16:48', 'YYYY-MM-DD HH24:MI:SS')
        # Regex para ISO 8601 parcial ou completa
        iso_pattern = r"'(\d{4}-\d{2}-\d{2})(?:T(\d{2}:\d{2}:\d{2}))?(?:\.\d+)?Z?'"
        
        def replace_iso(match):
            date_part = match.group(1)
            time_part = match.group(2)
            if time_part:
                return f"TO_TIMESTAMP('{date_part} {time_part}', 'YYYY-MM-DD HH24:MI:SS')"
            return f"TO_DATE('{date_part}', 'YYYY-MM-DD')"
            
        sql_cond = re.sub(iso_pattern, replace_iso, sql_cond)
        
        return sql_cond

    def generate(self, table_name: str, rule: Dict) -> Optional[str]:
        """
        Gera SQL de validação para uma regra.
        
        A query retorna o número de EXCEÇÕES (linhas que violam a regra).
        Se retornar 0 → Regra é VÁLIDA
        Se retornar > 0 → Regra é INVÁLIDA
        """
        rule_type = rule.get('type')
        sql_suggestion = rule.get('sql', rule.get('sql_suggestion', '')).strip() # Ajustado para pegar 'sql' também
        
        if not sql_suggestion:
            return None
            
        # Aplicar correções de sintaxe Oracle
        sql_suggestion = self._fix_oracle_syntax(sql_suggestion)
        sql_upper = sql_suggestion.upper()
        
        # 1. Se for SELECT COUNT ou SELECT simples, retorna como está
        if sql_upper.startswith('SELECT') and 'COUNT' in sql_upper:
            # Se já contar exceções, retorna direto
            if 'EXCEPTION' in sql_upper or 'VIOLATION' in sql_upper:
                return sql_suggestion
            # Se for SELECT COUNT simples, converter para contar exceções
            if 'WHERE' in sql_upper:
                return sql_suggestion
            # Se for COUNT(*) sem WHERE, não é regra de validação
            return None
            
        # 2. Se for ALTER TABLE com MODIFY COLUMN (NOT NULL)
        if 'MODIFY COLUMN' in sql_upper and 'NOT NULL' in sql_upper:
            # Extrair nome da coluna
            col_match = re.search(r'MODIFY\s+COLUMN\s+(\w+)', sql_upper, re.IGNORECASE)
            if col_match:
                col_name = col_match.group(1)
                return f"SELECT COUNT(*) AS EXCEPTIONS FROM {table_name} WHERE {col_name} IS NULL"
                
        # 3. Se for ALTER TABLE com CHECK constraint
        if 'ADD CONSTRAINT' in sql_upper and 'CHECK' in sql_upper:
            # Extrair condição do CHECK
            check_match = re.search(r'CHECK\s*\((.*?)\)\s*$', sql_suggestion, re.IGNORECASE | re.DOTALL)
            if check_match:
                condition = check_match.group(1).strip()
                return f"SELECT COUNT(*) AS EXCEPTIONS FROM {table_name} WHERE NOT ({condition})"
                
        # 4. Se for ALTER TABLE com UNIQUE
        if 'ADD CONSTRAINT' in sql_upper and 'UNIQUE' in sql_upper:
            # Extrair coluna do UNIQUE
            unique_match = re.search(r'UNIQUE\s*\((.*?)\)', sql_upper, re.IGNORECASE)
            if unique_match:
                cols = unique_match.group(1)
                return f"""
                SELECT COUNT(*) - COUNT(DISTINCT {cols}) AS EXCEPTIONS 
                FROM {table_name}
                """
                
        # 5. Para outros tipos de SQL, tentar extrair condição WHERE
        if 'WHERE' in sql_upper:
            where_match = re.search(r'WHERE\s+(.+?)(?:\s+GROUP\s+BY|\s+ORDER\s+BY|\s*$)', sql_suggestion, re.IGNORECASE | re.DOTALL)
            if where_match:
                condition = where_match.group(1).strip().rstrip(';')
                # Se já for uma verificação de exceções, retorna
                if any(x in condition.upper() for x in ['IS NULL', 'NOT IN', '!=', '<>']):
                    return f"SELECT COUNT(*) AS EXCEPTIONS FROM {table_name} WHERE {condition}"
                    
        # 6. Fallback: tratar como condição CHECK
        if not any(danger in sql_upper for danger in ['DROP', 'DELETE', 'TRUNCATE', 'UPDATE']):
            return f"SELECT COUNT(*) AS EXCEPTIONS FROM {table_name} WHERE NOT ({sql_suggestion})"
            
        return None
    
    def _parse_condition(self, condition: str) -> str:
        """Converte condição em linguagem natural para SQL."""
        if not condition:
            return ""
        
        # Substituir operadores comuns
        condition = condition.replace(' é ', ' = ')
        condition = condition.replace(' igual a ', ' = ')
        condition = condition.replace(' diferente de ', ' != ')
        
        # Adicionar aspas em valores se necessário
        # Ex: "STATUS = A" -> "STATUS = 'A'"
        condition = re.sub(r'=\s*([A-Za-z]\w*)\b(?!\()', r"= '\1'", condition)
        
        return condition.upper()
