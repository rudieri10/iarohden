"""
RULE VALIDATOR - Validação Automática de Regras de Negócio
Valida regras detectadas pela IA executando queries SQL no Oracle.
"""

import re
import json
from datetime import datetime
from typing import Dict, List, Optional, Tuple
from ..ENGINE.connection_manager import get_connection_manager


class RuleParser:
    """Parser de regras em linguagem natural para estrutura validável."""
    
    def __init__(self):
        # NENHUM padrão regex hardcoded.
        # A validação agora depende inteiramente da inteligência da IA.
        pass
    
    def parse(self, rule_input: Dict | str) -> Optional[Dict]:
        """
        Parseia uma regra, esperando uma estrutura da IA.
        Se receber apenas texto, não tentará adivinhar via regex.
        """
        # Tentar converter string que parece dict (ex: "{'rule': ...}")
        if isinstance(rule_input, str) and rule_input.strip().startswith("{") and rule_input.strip().endswith("}"):
            try:
                import ast
                rule_input = ast.literal_eval(rule_input)
            except:
                pass # Segue como string normal se falhar

        if isinstance(rule_input, dict):
             # Normalizar chaves que a IA inventa
             sql_sugg = rule_input.get("sql_check_suggestion") or rule_input.get("sql_example") or rule_input.get("validação_SQL") or rule_input.get("validation_SQL")
             rule_type = rule_input.get("rule_type") or rule_input.get("type")
             # Se type for URL ou lixo, ignorar
             if rule_type and len(rule_type) > 50: rule_type = "CUSTOM_SQL"
             
             desc = rule_input.get("rule_description") or rule_input.get("rule") or rule_input.get("business_rule") or rule_input.get("description")
             
             # Se a IA mandou example SQL que na verdade é um SELECT count, usá-lo
             ex = rule_input.get("example")
             if ex and "SELECT" in str(ex).upper(): sql_sugg = ex

             if sql_sugg or rule_type:
                # Regra estruturada pela IA
                return {
                    'type': (rule_type or 'CUSTOM_SQL').lower(),
                    'original': desc or str(rule_input),
                    'params': rule_input.get('validation_params') or rule_input.get('params'),
                    'sql_suggestion': sql_sugg
                }

        # Se chegamos aqui, a IA mandou texto puro e não temos fallback.
        # Retornamos None ou um tipo genérico que falhará na validação mas será logado.
        return {
            'type': 'unstructured_text',
            'original': str(rule_input),
            'status': 'UNPARSEABLE'
        }
    
    def _build_rule_structure(self, rule_type: str, match, original_text: str) -> Dict:
        """Constrói a estrutura da regra baseado no tipo."""
        groups = match.groups()
        
        if rule_type == 'allowed_values':
            # Extrair lista de valores (simples)
            raw_values = groups[1].strip()
            # Tentar limpar formato "X, Y ou Z" ou "'X', 'Y'"
            cleaned = raw_values.replace(' ou ', ', ').replace(' e ', ', ')
            # Basic split - melhorias podem ser feitas
            values_list = [v.strip().strip("'").strip('"') for v in cleaned.split(',')]
            
            return {
                'type': 'allowed_values',
                'column': groups[0].upper(),
                'allowed_values': values_list,
                'original': original_text
            }
        elif rule_type in ['always_value', 'never_value', 'greater_than', 'less_than']:
            return {
                'type': rule_type,
                'column': groups[0].upper(),
                'value': groups[1].strip(),
                'condition': groups[2].strip() if len(groups) > 2 and groups[2] else None,
                'original': original_text
            }
        elif rule_type == 'range':
            return {
                'type': rule_type,
                'column': groups[0].upper(),
                'min_value': groups[1].strip(),
                'max_value': groups[2].strip(),
                'original': original_text
            }
        elif rule_type == 'not_null':
            return {
                'type': rule_type,
                'column': groups[0].upper(),
                'original': original_text
            }
        elif rule_type == 'always_condition':
            return {
                'type': rule_type,
                'condition': groups[0].strip(),
                'result': groups[1].strip(),
                'original': original_text
            }
        
        return None


class SQLGenerator:
    """Gerador de SQL de validação para regras parseadas."""
    
    def generate(self, table_name: str, rule: Dict) -> Optional[str]:
        """
        Gera SQL de validação para uma regra.
        
        A query retorna o número de EXCEÇÕES (linhas que violam a regra).
        Se retornar 0 → Regra é VÁLIDA
        Se retornar > 0 → Regra é INVÁLIDA
        """
        rule_type = rule.get('type')
        
        # 1. Prioridade: SQL sugerido pela IA (se seguro/validado)
        # 1. Prioridade: SQL sugerido pela IA (se seguro/validado)
        if rule.get('sql_suggestion'):
             cond = rule['sql_suggestion']
             # Limpeza de segurança e sintaxe
             cond = cond.strip().rstrip(';')
             cond_upper = cond.upper()

             # Se for CONSTRAINT CHECK (ex: CHECK (COL IS NOT NULL)), converter para WHERE
             if "CHECK" in cond_upper and "(" in cond:
                 # Extrair conteúdo dos parenteses do CHECK
                 try:
                    match = re.search(r'CHECK\s*\((.*)\)', cond, re.IGNORECASE)
                    if match:
                        check_body = match.group(1)
                        # O SQL de validação deve buscar o CONTRÁRIO (exceções)
                        return f"SELECT COUNT(*) AS EXCEPTIONS FROM {table_name} WHERE NOT ({check_body})"
                 except: pass
            
             # Se for ALTER TABLE, ignorar ou tentar extrair check
             if cond_upper.startswith("ALTER TABLE"):
                 try:
                    match = re.search(r'CHECK\s*\((.*)\)', cond, re.IGNORECASE)
                    if match:
                         check_body = match.group(1)
                         return f"SELECT COUNT(*) AS EXCEPTIONS FROM {table_name} WHERE NOT ({check_body})"
                 except: pass

             # Placeholder de segurança simples: garantir que não tem DROP/DELETE
             if not any(x in cond_upper for x in ['DROP ', 'DELETE ', 'TRUNCATE ', 'UPDATE ']): # ALTER removido pois tratamos acima
                 # Se já for um SELECT completo, retorna ele
                 if cond_upper.startswith("SELECT"):
                     return cond
                 # Senão, assume que é uma condição WHERE
                 return f"SELECT COUNT(*) AS EXCEPTIONS FROM {table_name} WHERE NOT ({cond})" # Inverter lógica do check

        # 2. Tipos estruturados pela IA
        if rule_type == 'allowed_values':
            if 'params' in rule and 'allowed' in rule['params']:
                # Reconstruir regra para _generate_allowed_values
                new_rule = rule.copy()
                # Tentar inferir coluna do SQL se não vier
                if 'column' not in new_rule and rule.get('sql_suggestion'):
                     # Extração grosseira, apenas fallback
                     new_rule['column'] = rule['sql_suggestion'].split()[0] 
                
                # Se ainda não temos coluna, precisamos do pattern match antigo ou inferência
                if 'column' not in new_rule: return None 

                new_rule['allowed_values'] = rule['params']['allowed']
                return self._generate_allowed_values(table_name, new_rule)
                
        # 3. Tipos Clássicos (Regex)
        if rule_type == 'allowed_values': # Fallback para regex
            return self._generate_allowed_values(table_name, rule)
        elif rule_type == 'always_value':
            return self._generate_always_value(table_name, rule)
        elif rule_type == 'never_value':
            return self._generate_never_value(table_name, rule)
        elif rule_type == 'not_null':
            return self._generate_not_null(table_name, rule)
        elif rule_type == 'greater_than':
            return self._generate_greater_than(table_name, rule)
        elif rule_type == 'less_than':
            return self._generate_less_than(table_name, rule)
        elif rule_type == 'range':
            return self._generate_range(table_name, rule)
        
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
    
    def _generate_allowed_values(self, table_name: str, rule: Dict) -> str:
        """Gera SQL para regra 'só permite valores X, Y, Z'."""
        column = rule['column']
        values = rule['allowed_values']
        
        # Formatar valores para SQL
        sql_values = []
        for v in values:
            if v.replace('.', '').replace('-', '').isdigit():
                sql_values.append(v)
            else:
                sql_values.append(f"'{v}'")
        
        values_str = ", ".join(sql_values)
        return f"SELECT COUNT(*) AS EXCEPTIONS FROM {table_name} WHERE {column} NOT IN ({values_str}) AND {column} IS NOT NULL"

    def _generate_always_value(self, table_name: str, rule: Dict) -> str:
        """Gera SQL para regra 'sempre é X'."""
        column = rule['column']
        value = rule['value']
        condition = rule.get('condition')
        
        # Adicionar aspas se for string
        if not value.replace('.', '').replace('-', '').isdigit():
            value = f"'{value.upper()}'"
        
        where_clause = f"WHERE {column} != {value} OR {column} IS NULL"
        
        if condition:
            parsed_condition = self._parse_condition(condition)
            where_clause = f"WHERE ({parsed_condition}) AND ({column} != {value} OR {column} IS NULL)"
        
        return f"SELECT COUNT(*) AS EXCEPTIONS FROM {table_name} {where_clause}"
    
    def _generate_never_value(self, table_name: str, rule: Dict) -> str:
        """Gera SQL para regra 'nunca é X'."""
        column = rule['column']
        value = rule['value']
        condition = rule.get('condition')
        
        if not value.replace('.', '').replace('-', '').isdigit():
            value = f"'{value.upper()}'"
        
        where_clause = f"WHERE {column} = {value}"
        
        if condition:
            parsed_condition = self._parse_condition(condition)
            where_clause = f"WHERE ({parsed_condition}) AND {column} = {value}"
        
        return f"SELECT COUNT(*) AS EXCEPTIONS FROM {table_name} {where_clause}"
    
    def _generate_not_null(self, table_name: str, rule: Dict) -> str:
        """Gera SQL para regra 'nunca é NULL'."""
        column = rule['column']
        return f"SELECT COUNT(*) AS EXCEPTIONS FROM {table_name} WHERE {column} IS NULL"
    
    def _generate_greater_than(self, table_name: str, rule: Dict) -> str:
        """Gera SQL para regra 'sempre maior que X'."""
        column = rule['column']
        value = rule['value']
        condition = rule.get('condition')
        
        where_clause = f"WHERE {column} IS NULL OR {column} <= {value}"
        
        if condition:
            parsed_condition = self._parse_condition(condition)
            where_clause = f"WHERE ({parsed_condition}) AND ({column} IS NULL OR {column} <= {value})"
        
        return f"SELECT COUNT(*) AS EXCEPTIONS FROM {table_name} {where_clause}"
    
    def _generate_less_than(self, table_name: str, rule: Dict) -> str:
        """Gera SQL para regra 'sempre menor que X'."""
        column = rule['column']
        value = rule['value']
        condition = rule.get('condition')
        
        where_clause = f"WHERE {column} IS NULL OR {column} >= {value}"
        
        if condition:
            parsed_condition = self._parse_condition(condition)
            where_clause = f"WHERE ({parsed_condition}) AND ({column} IS NULL OR {column} >= {value})"
        
        return f"SELECT COUNT(*) AS EXCEPTIONS FROM {table_name} {where_clause}"
    
    def _generate_range(self, table_name: str, rule: Dict) -> str:
        """Gera SQL para regra 'está entre X e Y'."""
        column = rule['column']
        min_val = rule['min_value']
        max_val = rule['max_value']
        
        return f"SELECT COUNT(*) AS EXCEPTIONS FROM {table_name} WHERE {column} IS NULL OR {column} < {min_val} OR {column} > {max_val}"


class RuleValidator:
    """Orquestrador de validação de regras de negócio."""
    
    def __init__(self):
        self.parser = RuleParser()
        self.sql_generator = SQLGenerator()
        self.conn_manager = get_connection_manager()
    
    def validate_table_rules(self, table_name: str, rules: List[str]) -> List[Dict]:
        """
        Valida todas as regras de uma tabela.
        
        Args:
            table_name: Nome da tabela no Oracle
            rules: Lista de regras em linguagem natural
        
        Returns:
            Lista de regras validadas com status e confiança
        """
        validated_rules = []
        
        for rule_input in rules:
            # O Parser agora é inteligente e aceita Dict ou Str
            validated = self.validate_single_rule(table_name, rule_input)
            validated_rules.append(validated)
            
            # Recuperar texto para log
            rule_text = validated.get('rule_text', str(rule_input))
            
            # Log do resultado
            if validated['status'] == 'VALID':
                print(f"   ✓ Regra validada: \"{rule_text}\" ({validated['confidence']}% confiança)")
            elif validated['status'] == 'INVALID':
                print(f"   ✗ Regra inválida: \"{rule_text}\" ({validated['exceptions_found']} exceções)")
            else:
                print(f"   ⚠ Regra não parseável: \"{rule_text}\"")
        
        return validated_rules
    
    def validate_single_rule(self, table_name: str, rule_text: str) -> Dict:
        """Valida uma única regra."""
        result = {
            'rule_text': rule_text,
            'status': 'UNPARSEABLE',
            'confidence': 0.0,
            'validation_sql': None,
            'exceptions_found': None,
            'total_checked': None,
            'validated_at': datetime.now().isoformat()
        }
        
        # 1. Parsear a regra
        parsed_rule = self.parser.parse(rule_text)
        if not parsed_rule:
            return result
        
        # 2. Gerar SQL
        validation_sql = self.sql_generator.generate(table_name, parsed_rule)
        if not validation_sql:
            return result
        
        result['validation_sql'] = validation_sql
        
        # 3. Executar SQL no Oracle
        try:
            rows = self.conn_manager.execute_query(validation_sql)
            exceptions_found = rows[0]['EXCEPTIONS'] if rows else 0
            
            # 4. Contar total de registros na tabela
            count_sql = f"SELECT COUNT(*) AS TOTAL FROM {table_name}"
            total_rows = self.conn_manager.execute_query(count_sql)
            total_checked = total_rows[0]['TOTAL'] if total_rows else 0
            
            result['exceptions_found'] = exceptions_found
            result['total_checked'] = total_checked
            
            # 5. Determinar status e confiança
            if exceptions_found == 0:
                result['status'] = 'VALID'
                result['confidence'] = 100.0
            else:
                result['status'] = 'INVALID'
                # Confiança inversamente proporcional às exceções
                if total_checked > 0:
                    result['confidence'] = round((1 - (exceptions_found / total_checked)) * 100, 2)
                else:
                    result['confidence'] = 0.0
        
        except Exception as e:
            result['status'] = 'ERROR'
            result['error'] = str(e)
            print(f"   ❌ Erro ao validar regra: {e}")
        
        return result
