"""
RULE VALIDATOR - Validação Automática de Regras de Negócio com Auto-Cura
Valida regras detectadas pela IA executando queries SQL no Oracle.
Implementa re-tentativa automática e classificação inteligente.
"""

import re
import json
import asyncio
from datetime import datetime
from typing import Dict, List, Optional, Tuple
from .rule_parser import safe_parse_rule, SQLGenerator
from .auto_healing_validator import AutoHealingValidator, ValidationResult
from ..ENGINE.connection_manager import get_connection_manager


class RuleParser:
    """Parser de regras em linguagem natural para estrutura validável."""
    
    def __init__(self):
        # NENHUM padrão regex hardcoded.
        # A validação agora depende inteiramente da inteligência da IA.
        pass
    
    def parse(self, rule_input: Dict | str) -> Optional[Dict]:
        """
        Parseia uma regra usando o parser robusto.
        Usa safe_parse_rule para normalizar saídas instáveis da IA.
        """
        # Usa o novo parser robusto
        parsed = safe_parse_rule(rule_input)
        
        if parsed:
            return {
                'type': parsed.get('type', 'custom'),
                'original': str(rule_input),
                'sql_suggestion': parsed.get('sql', ''),
                'description': parsed.get('description', '')
            }
        
        return None
    
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


class RuleValidator:
    """Validador de regras com sistema de auto-cura."""
    
    def __init__(self):
        self.parser = RuleParser()
        self.sql_generator = SQLGenerator()
        self.auto_healing = AutoHealingValidator()
        self.connection_manager = get_connection_manager()
    
    async def validate_rule_with_healing(self, table_name: str, rule_input: Dict | str) -> ValidationResult:
        """
        Valida regra com auto-cura completa.
        Método principal que deve ser usado.
        """
        return await self.auto_healing.validate_rule_with_healing(table_name, rule_input)
    
    def validate_rule(self, table_name: str, rule_input: Dict | str) -> Optional[Dict]:
        """
        Método legado para compatibilidade.
        Valida regra sem auto-cura (síncrono).
        """
        # Parse da regra
        parsed = self.parser.parse(rule_input)
        if not parsed:
            return None
        
        # Gerar SQL
        sql = self.sql_generator.generate(table_name, parsed)
        if not sql:
            return None
        
        # Executar validação (síncrono)
        try:
            # Usar o ConnectionManager como context manager
            with self.connection_manager.get_connection() as conn:
                cursor = conn.cursor()
                cursor.execute(sql)
                result = cursor.fetchone()
                
                exceptions = result[0] if result and len(result) > 0 else 0
                
                cursor.close()
                
            return {
                'rule': parsed,
                'sql': sql,
                'exceptions': exceptions,
                'status': 'valid' if exceptions == 0 else 'invalid'
            }
            
        except Exception as e:
            return {
                'rule': parsed,
                'sql': sql,
                'error': str(e),
                'status': 'error'
            }

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
            # Usar o ConnectionManager como context manager
            with self.connection_manager.get_connection() as conn:
                cursor = conn.cursor()
                
                # Executar SQL de validação
                cursor.execute(validation_sql)
                validation_result = cursor.fetchone()
                exceptions_found = validation_result[0] if validation_result and len(validation_result) > 0 else 0
                
                # 4. Contar total de registros na tabela
                count_sql = f"SELECT COUNT(*) AS TOTAL FROM {table_name}"
                cursor.execute(count_sql)
                count_result = cursor.fetchone()
                total_checked = count_result[0] if count_result and len(count_result) > 0 else 0
                
                cursor.close()
                
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
