"""
AUTO-HEALING VALIDATOR - Sistema de Auto-Cura para Validação de Regras
Implementa re-tentativa automática, classificação inteligente e aprendizado.
"""

import re
import json
import logging
import hashlib
import time
from typing import Dict, List, Optional, Tuple
from datetime import datetime
from .rule_parser import safe_parse_rule, translate_to_oracle, is_oracle_syntax_error, is_ignorable_error, generate_retry_prompt
from ..ENGINE.connection_manager import get_connection_manager
from ..DATA.storage import DataStorage
from ..ENGINE.vector_manager import VectorManager
from .ai_client import AIClient

logger = logging.getLogger(__name__)

class ValidationResult:
    """Resultado estruturado da validação com classificação inteligente."""
    
    SUCCESS = "success"  # 100% válido
    PARTIAL = "partial"  # Válido com exceções
    SYNTAX_ERROR = "syntax_error"  # Erro de sintaxe SQL
    PERMANENT_ERROR = "permanent_error"  # Erro irrecuperável
    IGNORED = "ignored"  # Regra ignorada (comentário da IA)
    
    def __init__(self, status: str, sql: str, exceptions: int = 0, message: str = "", original_error: str = None):
        self.status = status
        self.sql = sql
        self.exceptions = exceptions
        self.message = message
        self.original_error = original_error
        self.timestamp = datetime.now().isoformat()

class AutoHealingValidator:
    """Validador com capacidade de auto-cura e aprendizado."""
    
    def __init__(self):
        self.storage = DataStorage()
        self.connection_manager = get_connection_manager()
        self.vector_manager = VectorManager()
        self.ai_client = AIClient()
        self.max_retries = 3
        
    def _generate_unique_constraint_name(self, table_name: str, column_name: str, constraint_type: str = "CK") -> str:
        """
        Gera nome único para constraint usando UUID curto.
        Evita ORA-02264 (name already used by an existing constraint).
        """
        import uuid
        
        # Usar UUID curto (8 caracteres) + timestamp
        unique_id = str(uuid.uuid4())[:8].upper()
        timestamp = str(int(time.time()))[-4:]  # Últimos 4 dígitos
        
        # Limitar tamanho (Oracle max = 30 chars)
        col_short = column_name[:6].upper()
        table_short = table_name[:6].upper()
        
        return f"{constraint_type}_{table_short}_{col_short}_{unique_id}"[:30]
    
    def _validate_parentheses(self, sql: str) -> bool:
        """Verifica se os parênteses estão balanceados no SQL."""
        open_count = sql.count('(')
        close_count = sql.count(')')
        return open_count == close_count
    
    def _simplify_alter_table(self, sql: str) -> str:
        """
        Simplifica comandos ALTER TABLE complexos para evitar ORA-01735.
        Separa múltiplas ações em comandos individuais.
        """
        sql_upper = sql.upper()
        
        # Se tiver múltiplas ações no mesmo ALTER TABLE
        if 'ADD CONSTRAINT' in sql_upper and ('MODIFY' in sql_upper or 'ADD CONSTRAINT' in sql_upper[sql_upper.find('ADD CONSTRAINT') + 15:]):
            # Extrair nome da tabela
            table_match = re.search(r'ALTER\s+TABLE\s+(\w+)', sql_upper)
            if not table_match:
                return sql
            
            table_name = table_match.group(1)
            
            # Separar ações
            actions = []
            
            # Encontrar todas as ADD CONSTRAINT
            constraint_pattern = r'ADD\s+CONSTRAINT\s+\w+\s+(?:CHECK|UNIQUE|PRIMARY KEY|FOREIGN KEY)\s*\([^)]+\)'
            constraints = re.findall(constraint_pattern, sql, re.IGNORECASE)
            for constraint in constraints:
                actions.append(f"ALTER TABLE {table_name} {constraint}")
            
            # Encontrar MODIFY
            modify_pattern = r'MODIFY\s+(?:COLUMN\s+)?\w+(?:\s+\w+)*'
            modifies = re.findall(modify_pattern, sql, re.IGNORECASE)
            for modify in modifies:
                actions.append(f"ALTER TABLE {table_name} {modify}")
            
            # Retornar apenas a primeira ação (mais simples)
            if actions:
                logger.info(f"ALTER TABLE complexo simplificado: {len(actions)} ações separadas")
                return actions[0]
        
        return sql
    
    def _preprocess_sql(self, sql: str) -> str:
        """
        Pré-processa SQL para evitar erros comuns.
        """
        # 1. Simplificar ALTER TABLE complexos
        sql = self._simplify_alter_table(sql)
        
        # 2. Validar parênteses
        if not self._validate_parentheses(sql):
            logger.warning(f"SQL com parênteses desbalanceados detectado: {sql}")
            # Tentar corrigir parênteses básicos
            open_count = sql.count('(')
            close_count = sql.count(')')
            if open_count > close_count:
                sql += ')' * (open_count - close_count)
            elif close_count > open_count:
                sql = sql[:sql.rfind(')')] * (close_count - open_count)
        
        return sql
    
    def _fix_constraint_names(self, sql: str) -> str:
        """
        Substitui nomes de constraints no SQL por nomes únicos.
        Padrão: ADD CONSTRAINT CK_NOME CHECK (...)
        """
        # Padrão para encontrar ADD CONSTRAINT com nome
        pattern = r'ADD\s+CONSTRAINT\s+(\w+)\s+'
        
        def replace_constraint_name(match):
            old_name = match.group(1)
            # Extrair informações do contexto para gerar novo nome
            table_match = re.search(r'ALTER\s+TABLE\s+(\w+)', sql, re.IGNORECASE)
            column_match = re.search(r'CHECK\s*\(\s*(\w+)', sql, re.IGNORECASE)
            
            table_name = table_match.group(1) if table_match else "TBL"
            column_name = column_match.group(1) if column_match else "COL"
            
            # Determinar tipo de constraint
            if old_name.upper().startswith('CK'):
                constraint_type = "CK"
            elif old_name.upper().startswith('UQ'):
                constraint_type = "UQ"
            elif old_name.upper().startswith('FK'):
                constraint_type = "FK"
            elif old_name.upper().startswith('PK'):
                constraint_type = "PK"
            else:
                constraint_type = "CK"  # Default para CHECK
            
            new_name = self._generate_unique_constraint_name(table_name, column_name, constraint_type)
            logger.info(f"Constraint name substituído: {old_name} → {new_name}")
            
            return f'ADD CONSTRAINT {new_name} '
        
        return re.sub(pattern, replace_constraint_name, sql, flags=re.IGNORECASE)
        
    async def validate_rule_with_healing(self, table_name: str, rule_input: Dict | str) -> ValidationResult:
        """
        Valida regra com auto-cura completa.
        1. Parse robusto
        2. Tradução SQL
        3. Validação com retry
        4. Classificação inteligente
        5. Aprendizado
        """
        # 1. Parse robusto da regra
        parsed_rule = safe_parse_rule(rule_input)
        if not parsed_rule:
            return ValidationResult(
                ValidationResult.PERMANENT_ERROR,
                "",
                message="Regra não parseável"
            )
        
        # 2. Tradução SQL para Oracle
        sql = parsed_rule.get('sql', '')
        if not sql:
            return ValidationResult(
                ValidationResult.PERMANENT_ERROR,
                "",
                message="SQL não encontrado na regra"
            )
        
        # 2.1. Pré-processar SQL para evitar erros comuns
        sql = self._preprocess_sql(sql)
        
        # 2.2. Fix constraint names para evitar ORA-02264
        sql = self._fix_constraint_names(sql)
        
        translated_sql, alterations = translate_to_oracle(sql)
        
        # 3. Validação com retry
        result = await self._validate_with_retry(table_name, translated_sql, parsed_rule)
        
        # 4. Classificação inteligente
        if result.status == ValidationResult.SUCCESS:
            await self._save_as_business_rule(table_name, parsed_rule, result)
        elif result.status == ValidationResult.PARTIAL:
            await self._save_as_quality_issue(table_name, parsed_rule, result)
        
        # 5. Aprendizado baseado no resultado
        await self._learn_from_validation(table_name, parsed_rule, result)
        
        return result
    
    async def _validate_with_retry(self, table_name: str, sql: str, rule: Dict) -> ValidationResult:
        """
        Valida SQL com mecanismo de retry.
        Tenta tradução automática, depois retry com IA se falhar.
        """
        # Tentativa 1: SQL traduzido
        result = await self._execute_validation_sql(table_name, sql, rule)
        
        if result.status == ValidationResult.SUCCESS:
            return result
            
        # Se for erro de sintaxe Oracle, tentar retry
        if is_oracle_syntax_error(result.original_error):
            logger.warning(f"Erro de sintaxe Oracle detectado: {result.original_error}")
            
            # Tentar retry com IA
            retry_result = await self._retry_with_ai(table_name, sql, rule, result.original_error)
            if retry_result:
                return retry_result
            
            # Se retry também falhou, marcar como erro de sintaxe permanente
            return ValidationResult(
                ValidationResult.SYNTAX_ERROR,
                sql,
                message=f"Erro de sintaxe Oracle persistente: {result.original_error}",
                original_error=result.original_error
            )
        
        # Se não for erro de sintaxe, retornar resultado original
        return result
    
    async def _retry_with_ai(self, table_name: str, sql: str, rule: Dict, last_error: str) -> Optional[ValidationResult]:
        """
        Tenta corrigir SQL usando a IA com base no erro.
        """
        try:
            logger.info(f"Tentando corrigir SQL com IA. Erro: {last_error}")
            
            # Gerar prompt de retry
            retry_prompt = generate_retry_prompt(sql, last_error)
            
            # Chamar IA para correção
            corrected_sql = self.ai_client.generate_sql(retry_prompt)
            
            if not corrected_sql:
                logger.warning("IA não retornou sugestão de correção.")
                return None
                
            logger.info(f"IA sugeriu correção: {corrected_sql}")
            
            # Atualizar regra com novo SQL
            new_rule = rule.copy()
            new_rule['sql'] = corrected_sql
            new_rule['description'] = f"{rule.get('description', '')} (Corrigido por IA)"
            
            # Validar novo SQL
            result = await self._execute_validation_sql(table_name, corrected_sql, new_rule)
            
            if result.status == ValidationResult.SUCCESS:
                result.message += " (Corrigido via IA)"
                return result
                
            return None
            
        except Exception as e:
            logger.error(f"Erro no retry com IA: {e}")
            return None
    
    def _pre_validate_sql(self, sql: str) -> Optional[str]:
        """
        Pré-valida SQL antes da execução.
        Retorna mensagem de erro se inválido, None se válido.
        """
        sql_upper = sql.upper().strip()
        
        # 1. Verificar parênteses balanceados
        if sql.count('(') != sql.count(')'):
            return "Parênteses desbalanceados"
        
        # 2. Verificar REGEXP_LIKE com pattern vazio
        if 'REGEXP_LIKE' in sql_upper and re.search(r"REGEXP_LIKE\s*\(\s*\w+\s*,\s*''\s*\)", sql):
            return "REGEXP_LIKE com pattern vazio"
        
        # 3. Verificar múltiplas ações em ALTER TABLE
        if sql_upper.startswith('ALTER TABLE'):
            if sql_upper.count('MODIFY') > 1 or (sql_upper.count('MODIFY') >= 1 and sql_upper.count('ADD CONSTRAINT') >= 1):
                return "Múltiplas ações em ALTER TABLE (não suportado)"
        
        # 4. Verificar operadores inválidos
        invalid_operators = [' = = ', ' ! ! ', ' < < ', ' > > ']
        for op in invalid_operators:
            if op in sql:
                return f"Operador inválido detectado: {op.strip()}"
        
        return None
    
    def _convert_ddl_to_select(self, sql: str) -> Optional[str]:
        """
        Converte DDL de validação (ALTER TABLE) em SELECT COUNT(*) para validação não intrusiva.
        Retorna SQL de contagem de exceções ou None se não conseguir converter.
        """
        sql_upper = sql.upper().strip()
        
        # Extrair tabela
        table_match = re.search(r'ALTER\s+TABLE\s+(\w+)', sql_upper)
        if not table_match:
            return None
        table_name = table_match.group(1)
        
        # Caso 1: MODIFY (... NOT NULL)
        if 'MODIFY' in sql_upper and 'NOT NULL' in sql_upper:
            try:
                # Extrair parte após MODIFY
                modify_part = sql_upper.split('MODIFY', 1)[1]
                # Regex para pegar a coluna: permite '(' opcional, espaços, nome, espaços, NOT NULL
                col_match = re.search(r'[\(\s]*(\w+)\s+NOT\s+NULL', modify_part)
                if col_match:
                    col_name = col_match.group(1)
                    # Verificar exceções: count > 0 significa que existem nulos (regra violada)
                    return f"SELECT COUNT(*) FROM {table_name} WHERE {col_name} IS NULL"
            except Exception:
                pass
                
        # Caso 2: ADD CONSTRAINT ... CHECK (...)
        if 'CHECK' in sql_upper:
            try:
                # Encontrar início do CHECK
                check_idx = sql_upper.find('CHECK')
                if check_idx == -1: return None
                
                # Encontrar primeiro '(' após CHECK
                start_paren = sql_upper.find('(', check_idx)
                if start_paren == -1: return None
                
                # Encontrar o parêntese de fechamento correspondente
                count = 0
                end_paren = -1
                for i in range(start_paren, len(sql)):
                    char = sql[i]
                    if char == '(': count += 1
                    elif char == ')': count -= 1
                    
                    if count == 0:
                        end_paren = i
                        break
                
                if end_paren != -1:
                    # Extrair condição mantendo case original
                    condition = sql[start_paren+1:end_paren]
                    # Verificar exceções: count > 0 significa que existem linhas que NÃO satisfazem a condição
                    return f"SELECT COUNT(*) FROM {table_name} WHERE NOT ({condition})"
            except Exception:
                pass

        return None

    async def _execute_validation_sql(self, table_name: str, sql: str, rule: Dict = None) -> ValidationResult:
        """
        Executa SQL de validação e retorna resultado estruturado.
        Trata diferentes tipos de SQL (DDL vs DQL) e erros Oracle específicos.
        """
        if rule is None:
            rule = {}

        # PRÉ-VALIDAÇÃO: Verificar se SQL é muito complexo
        sql_upper = sql.upper().strip()
        
        # Bloquear ALTER TABLE com múltiplas ações
        if sql_upper.startswith('ALTER TABLE'):
            if sql_upper.count('MODIFY') > 1:
                logger.warning("❌ SQL bloqueado: múltiplos MODIFY no mesmo ALTER TABLE")
                return ValidationResult(
                    ValidationResult.SYNTAX_ERROR,
                    sql,
                    message="ALTER TABLE com múltiplas ações não suportado. Use comandos separados."
                )
            
            if 'MODIFY' in sql_upper and 'ADD CONSTRAINT' in sql_upper:
                logger.warning("❌ SQL bloqueado: MODIFY + ADD CONSTRAINT no mesmo ALTER TABLE")
                return ValidationResult(
                    ValidationResult.SYNTAX_ERROR,
                    sql,
                    message="ALTER TABLE não pode combinar MODIFY e ADD CONSTRAINT. Use comandos separados."
                )
        
        # Pré-validação antes da execução
        validation_error = self._pre_validate_sql(sql)
        if validation_error:
            logger.warning(f"SQL pré-validação falhou: {validation_error}")
            return ValidationResult(
                ValidationResult.SYNTAX_ERROR,
                sql,
                message=f"Pré-validação falhou: {validation_error}"
            )
        
        try:
            # TENTATIVA 1: Validação Não-Intrusiva (SELECT COUNT)
            # Preferível para não bloquear tabelas e evitar erros de DDL em produção
            validation_query = self._convert_ddl_to_select(sql)
            
            if validation_query:
                with self.connection_manager.get_connection() as conn:
                    cursor = conn.cursor()
                    logger.debug(f"🔍 Validando via SELECT (Não-Intrusivo): {validation_query}")
                    cursor.execute(validation_query)
                    result_row = cursor.fetchone()
                    exceptions_count = result_row[0] if result_row else 0
                    cursor.close()
                
                if exceptions_count == 0:
                    return ValidationResult(
                        ValidationResult.SUCCESS,
                        sql,
                        exceptions=0,
                        message="Regra validada logicamente (sem exceções encontradas)"
                    )
                else:
                    logger.warning(f"⚠️ Regra válida mas com {exceptions_count} exceções (Quality Issue)")
                    
                    # Tentar salvar issue de qualidade (protegido contra erro de vetor)
                    try:
                        self.storage.save_knowledge(
                            category="data_quality",
                            title=f"Qualidade de Dados: {table_name} - {rule.get('description', 'Regra Violada')}",
                            content=f"Regra SQL '{sql}' violada por {exceptions_count} registros.",
                            tags=f"table:{table_name},quality_issue:violation",
                            priority=7
                        )
                    except Exception as ve:
                        logger.warning(f"Falha ao salvar knowledge de qualidade (ignorado): {ve}")
                        
                    return ValidationResult(
                        ValidationResult.PARTIAL,
                        sql,
                        exceptions=exceptions_count,
                        message=f"Regra lógica válida mas com {exceptions_count} exceções nos dados"
                    )

            # TENTATIVA 2: Execução Direta (DDL/DML)
            # Se não conseguiu converter para SELECT, tenta executar (comportamento legado)
            # Usar o ConnectionManager como context manager (não é async)
            with self.connection_manager.get_connection() as conn:
                cursor = conn.cursor()
                
                sql_upper = sql.upper().strip()
                
                # Verificar se é DDL (ALTER, CREATE, DROP) ou DML (INSERT, UPDATE, DELETE)
                is_ddl = any(sql_upper.startswith(cmd) for cmd in ['ALTER', 'CREATE', 'DROP', 'TRUNCATE'])
                is_dml = any(sql_upper.startswith(cmd) for cmd in ['INSERT', 'UPDATE', 'DELETE'])
                
                if is_ddl or is_dml:
                    # Para DDL/DML, executar diretamente e considerar sucesso se não houver erro
                    cursor.execute(sql)
                    conn.commit()  # Commit para DDL/DML
                    
                    cursor.close()
                    
                    return ValidationResult(
                        ValidationResult.SUCCESS,
                        sql,
                        exceptions=0,
                        message="Comando DDL/DML executado com sucesso"
                    )
                else:
                    # Para SELECT (consultas), processar resultado
                    cursor.execute(sql)
                    result = cursor.fetchone()
                    
                    # Interpretar resultado
                    if result and len(result) > 0:
                        exceptions = result[0] if isinstance(result[0], int) else 0
                        
                        if exceptions == 0:
                            status = ValidationResult.SUCCESS
                            message = "Regra validada com sucesso (100% confiança)"
                        elif exceptions > 0:
                            status = ValidationResult.PARTIAL
                            message = f"Regra válida mas com {exceptions} exceções"
                        else:
                            status = ValidationResult.SUCCESS
                            message = "Regra validada (sem exceções)"
                            exceptions = 0
                    else:
                        status = ValidationResult.SUCCESS
                        message = "Regra validada (sem exceções)"
                        exceptions = 0
                    
                    cursor.close()
                    
                    return ValidationResult(status, sql, exceptions, message)
            
        except Exception as e:
            error_msg = str(e)
            
            # 1. CASOS DE SUCESSO IMPLÍCITO (Regras já aplicadas)
            
            # ORA-01442: column already NOT NULL
            if "ORA-01442" in error_msg:
                column_name = self._extract_column_from_sql(sql)
                table_name = self._extract_table_from_sql(sql)
                logger.debug(f"✅ Restrição já existente: {column_name} em {table_name} já é NOT NULL")
                return ValidationResult(
                    ValidationResult.SUCCESS, sql, exceptions=0,
                    message="Restrição já existente (sucesso implícito)"
                )

            # ORA-02260/02261/02264/02275: Constraints/PKs já existentes
            if any(code in error_msg for code in ["ORA-02260", "ORA-02261", "ORA-02264", "ORA-02275"]):
                logger.debug(f"✅ Constraint/PK já existente: {error_msg}")
                return ValidationResult(
                    ValidationResult.SUCCESS, sql, exceptions=0,
                    message="Constraint já existente (sucesso implícito)"
                )

            # 2. CASOS IGNORÁVEIS
            if is_ignorable_error(error_msg):
                logger.info(f"📝 Regra ignorada (conhecido/comentário): {rule.get('description', 'Sem descrição')}")
                return ValidationResult(
                    ValidationResult.IGNORED, sql,
                    message="Regra ignorada (erro conhecido ou comentário)"
                )
            
            # 3. ERROS DE QUALIDADE DE DADOS (Partial Success)

            # ORA-02293: Constraint CHECK violada
            if "ORA-02293" in error_msg and "check constraint violated" in error_msg:
                # ... Lógica de contagem de exceções mantida abaixo ...
                pass 

            # ORA-02296: cannot enable - null values found
            elif "ORA-02296" in error_msg and "null values found" in error_msg:
                 # ... Lógica mantida abaixo ...
                 pass

            # ORA-02299: Duplicate keys found
            elif "ORA-02299" in error_msg and "duplicate keys found" in error_msg:
                 # ... Lógica mantida abaixo ...
                 pass

            # 4. ERROS DE SINTAXE (Syntax Error)
            elif any(code in error_msg for code in ["ORA-009", "ORA-01756", "ORA-01735"]):
                logger.warning(f"❌ Erro de sintaxe SQL gerado pela IA: {error_msg}")
                return ValidationResult(
                    ValidationResult.SYNTAX_ERROR, sql,
                    message=f"Erro de sintaxe SQL: {error_msg}"
                )

            # === BLOCOS DE TRATAMENTO DE QUALIDADE (MANTIDOS DA LÓGICA ORIGINAL) ===
            
            # ORA-02293: Constraint CHECK violada - contar exceções
            if "ORA-02293" in error_msg and "check constraint violated" in error_msg:
                # Constraint CHECK tem dados que violam - contar exceções
                table_name = self._extract_table_from_sql(sql)
                
                # Extrair nome da constraint do erro
                constraint_match = re.search(r'cannot validate \([^.]+\.([^)]+)\)', error_msg)
                constraint_name = constraint_match.group(1) if constraint_match else None
                
                # Tentar extrair condição do CHECK
                check_match = re.search(r'CHECK\s*\((.*?)\)\s*$', sql, re.IGNORECASE | re.DOTALL)
                
                if check_match and table_name:
                    condition = check_match.group(1).strip()
                    
                    # Contar exceções (linhas que violam a condição)
                    count_sql = f"SELECT COUNT(*) FROM {table_name} WHERE NOT ({condition})"
                    
                    try:
                        with self.connection_manager.get_connection() as conn:
                            cursor = conn.cursor()
                            cursor.execute(count_sql)
                            exceptions_count = cursor.fetchone()[0]
                            cursor.close()
                        
                        logger.warning(f"⚠️ Constraint CHECK com {exceptions_count} exceções: {constraint_name}")
                        
                        # Salvar como issue de qualidade
                        try:
                            self.storage.save_knowledge(
                                category="data_quality",
                                title=f"Qualidade de Dados: check_constraint_violation em {table_name}",
                                content=f"Constraint CHECK violada: {exceptions_count} registros não atendem a condição {constraint_name}",
                                tags=f"table:{table_name},quality_issue:check_constraint_violation,oracle_error:true",
                                priority=7,
                                metadata={
                                    'table_name': table_name,
                                    'constraint_name': constraint_name,
                                    'exceptions': exceptions_count,
                                    'sql': sql,
                                    'error_code': 'ORA-02293',
                                    'error_message': error_msg
                                }
                            )
                        except Exception as ve:
                             logger.warning(f"Falha ao salvar knowledge de qualidade (ignorado): {ve}")

                        return ValidationResult(
                            ValidationResult.PARTIAL,
                            sql,
                            exceptions=exceptions_count,
                            message=f"Constraint CHECK violada: {exceptions_count} exceções"
                        )
                    except Exception as count_err:
                        logger.error(f"Erro ao contar exceções: {count_err}")
                
                # Se falhou ao contar, retorna erro
                return ValidationResult(
                    ValidationResult.SYNTAX_ERROR,
                    sql,
                    message=f"Constraint CHECK violada",
                    original_error=error_msg
                )

            # ORA-02296: cannot enable - null values found (Para MODIFY NOT NULL)
            elif "ORA-02296" in error_msg and "null values found" in error_msg:
                table_name = self._extract_table_from_sql(sql)
                column_name = self._extract_column_from_sql(sql)
                
                if table_name and column_name:
                    try:
                        count_sql = f"SELECT COUNT(*) FROM {table_name} WHERE {column_name} IS NULL"
                        with self.connection_manager.get_connection() as conn:
                            cursor = conn.cursor()
                            cursor.execute(count_sql)
                            null_count = cursor.fetchone()[0]
                            cursor.close()
                            
                        logger.warning(f"⚠️ Regra NOT NULL violada: {column_name} tem {null_count} nulos")
                        
                        try:
                            self.storage.save_knowledge(
                                category="data_quality",
                                title=f"Qualidade de Dados: null_values em {table_name}",
                                content=f"Coluna {column_name} tem {null_count} valores nulos, impedindo NOT NULL",
                                tags=f"table:{table_name},column:{column_name},quality_issue:null_values",
                                priority=7,
                                metadata={
                                    'table_name': table_name,
                                    'column_name': column_name,
                                    'nulls': null_count,
                                    'error_code': 'ORA-02296'
                                }
                            )
                        except Exception as ve:
                             logger.warning(f"Falha ao salvar knowledge de qualidade (ignorado): {ve}")
                            
                        return ValidationResult(
                            ValidationResult.PARTIAL,
                            sql,
                            exceptions=null_count,
                            message=f"NOT NULL violado: {null_count} valores nulos encontrados"
                        )
                    except Exception as e:
                        logger.error(f"Erro ao contar nulos: {e}")

            # ORA-02299: Duplicate keys found (UNIQUE constraint violada)
            elif "ORA-02299" in error_msg and "duplicate keys found" in error_msg:
                table_name = self._extract_table_from_sql(sql)
                column_name = self._extract_column_from_sql(sql)
                
                logger.warning(f"⚠️ UNIQUE constraint violada: {table_name}.{column_name} tem duplicatas")
                
                # Contar duplicatas
                if table_name and column_name:
                    try:
                        dup_sql = f"""
                        SELECT COUNT(*) - COUNT(DISTINCT {column_name}) AS duplicates
                        FROM {table_name}
                        WHERE {column_name} IS NOT NULL
                        """
                        with self.connection_manager.get_connection() as conn:
                            cursor = conn.cursor()
                            cursor.execute(dup_sql)
                            dup_count = cursor.fetchone()[0]
                            cursor.close()
                        
                        self.storage.save_knowledge(
                            category="data_quality",
                            title=f"Qualidade de Dados: duplicate_values em {table_name}",
                            content=f"Coluna {column_name} tem {dup_count} valores duplicados",
                            tags=f"table:{table_name},column:{column_name},quality_issue:duplicate_values",
                            priority=6,
                            metadata={
                                'table_name': table_name,
                                'column_name': column_name,
                                'duplicates': dup_count,
                                'error_code': 'ORA-02299'
                            }
                        )
                        
                        return ValidationResult(
                            ValidationResult.PARTIAL,
                            sql,
                            exceptions=dup_count,
                            message=f"UNIQUE violada: {dup_count} duplicatas em {column_name}"
                        )
                    except Exception as e:
                        logger.error(f"Erro ao contar duplicatas: {e}")
                
                return ValidationResult(
                    ValidationResult.SYNTAX_ERROR,
                    sql,
                    message="UNIQUE constraint violada",
                    original_error=error_msg
                )

            # 5. ERRO GENÉRICO (Se chegou até aqui, é erro não tratado)
            logger.error(f"Erro ao executar SQL de validação: {error_msg}")
            return ValidationResult(
                ValidationResult.SYNTAX_ERROR,
                sql,
                message=f"Erro de execução: {error_msg}",
                original_error=error_msg
            )


    

    
    def _extract_column_from_sql(self, sql: str) -> Optional[str]:
        """Extrai nome da coluna do SQL."""
        sql_upper = sql.upper()
        
        # Padrões para extrair coluna
        patterns = [
            r'MODIFY\s+(\w+)',  # MODIFY COLUMN
            r'CHECK\s*\(\s*(\w+)',  # CHECK (coluna
            r'ADD\s+CONSTRAINT\s+\w+\s+CHECK\s*\(\s*(\w+)',  # ADD CONSTRAINT CK_... CHECK (coluna
            r'WHERE\s+(\w+)',  # WHERE coluna
            r'UNIQUE\s*\(\s*(\w+)',  # UNIQUE (coluna
            r'(\w+)\s+IS\s+(?:NOT\s+)?NULL',  # coluna IS [NOT] NULL
        ]
        
        for pattern in patterns:
            match = re.search(pattern, sql_upper)
            if match:
                return match.group(1)
        
        return None
    
    def _extract_table_from_sql(self, sql: str) -> Optional[str]:
        """Extrai nome da tabela do SQL."""
        sql_upper = sql.upper()
        
        # Padrões para extrair tabela
        patterns = [
            r'ALTER\s+TABLE\s+(\w+)',
            r'CREATE\s+(?:UNIQUE\s+)?INDEX\s+\w+\s+ON\s+(\w+)',
            r'FROM\s+(\w+)',
            r'UPDATE\s+(\w+)',
            r'INSERT\s+INTO\s+(\w+)',
        ]
        
        for pattern in patterns:
            match = re.search(pattern, sql_upper)
            if match:
                return match.group(1)
        
        return None
    
    async def _save_as_business_rule(self, table_name: str, rule: Dict, result: ValidationResult):
        """Salva regra validada como Business Rule no conhecimento."""
        try:
            knowledge = {
                "category": "business_rule",
                "title": f"Regra de Negócio: {rule.get('description', 'Sem descrição')}",
                "content": f"Regra validada para tabela {table_name}: {rule.get('description')}",
                "sql": result.sql,
                "table_name": table_name,
                "validation_status": "validated",
                "exceptions": 0,
                "confidence": 100.0,
                "tags": f"table:{table_name},rule_type:business_rule,validated:true",
                "priority": 5,
                "metadata": {
                    "rule_type": rule.get('type'),
                    "original_rule": rule,
                    "validation_result": result.__dict__
                }
            }
            
            # Gerar embedding e salvar
            from ..ENGINE.vector_manager import VectorManager
            vector_manager = VectorManager()
            embedding = vector_manager.generate_embedding(knowledge["content"])
            
            if embedding:
                self.storage.save_knowledge(
                    category=knowledge["category"],
                    title=knowledge["title"],
                    content=knowledge["content"],
                    tags=knowledge["tags"],
                    priority=knowledge["priority"],
                    embedding_vector=vector_manager.vector_to_blob(embedding),
                    metadata=knowledge["metadata"]
                )
            
            logger.info(f"Regra de negócio salva: {knowledge['title']}")
            
        except Exception as e:
            logger.error(f"Erro ao salvar regra de negócio: {e}")

    async def _save_as_quality_issue(self, table_name: str, rule: Dict, result: ValidationResult):
        """Salva problema de qualidade identificado."""
        try:
            issue_type = self._classify_quality_issue(rule, result)
            recommendation = self._generate_cleanup_recommendation(issue_type, rule)
            
            knowledge = {
                "category": "data_quality",
                "title": f"Qualidade: {issue_type} em {table_name}",
                "content": f"Problema identificado: {result.message}. Recomendação: {recommendation}",
                "tags": f"table:{table_name},quality_issue:{issue_type},auto_detected:true",
                "priority": 7,
                "metadata": {
                    "issue_type": issue_type,
                    "sql": result.sql,
                    "exceptions": result.exceptions,
                    "original_rule": rule
                }
            }
            
            # Gerar embedding e salvar
            embedding = self.vector_manager.generate_embedding(knowledge["content"])
            
            if embedding:
                self.storage.save_knowledge(
                    category=knowledge["category"],
                    title=knowledge["title"],
                    content=knowledge["content"],
                    tags=knowledge["tags"],
                    priority=knowledge["priority"],
                    embedding_vector=self.vector_manager.vector_to_blob(embedding),
                    metadata=knowledge["metadata"]
                )
            
            logger.warning(f"Issue de qualidade salvo: {knowledge['title']}")
            
        except Exception as e:
            logger.error(f"Erro ao salvar issue de qualidade: {e}")
    
    async def _learn_from_validation(self, table_name: str, rule: Dict, result: ValidationResult):
        """
        Aprende com o resultado para melhorar validações futuras.
        """
        try:
            # Se NOT NULL falhou, aprender que coluna pode ter nulos
            if (rule.get('type') == 'not_null' and 
                result.status == ValidationResult.PARTIAL and 
                result.exceptions > 0):
                
                column = self._extract_column_from_rule(rule)
                if column:
                    await self._store_column_insight(table_name, column, "nullable", {
                        "null_count": result.exceptions,
                        "learned_at": datetime.now().isoformat()
                    })
            
            # Se UNIQUE falhou, aprender que coluna tem duplicatas
            if (rule.get('type') == 'unique' and 
                result.status == ValidationResult.PARTIAL and 
                result.exceptions > 0):
                
                column = self._extract_column_from_rule(rule)
                if column:
                    await self._store_column_insight(table_name, column, "has_duplicates", {
                        "duplicate_count": result.exceptions,
                        "learned_at": datetime.now().isoformat()
                    })
            
            # Se CHECK de valores falhou, aprender valores permitidos reais
            if (rule.get('type') == 'check' and 
                result.status == ValidationResult.PARTIAL):
                
                column = self._extract_column_from_rule(rule)
                if column:
                    await self._store_column_insight(table_name, column, "invalid_values", {
                        "invalid_count": result.exceptions,
                        "learned_at": datetime.now().isoformat()
                    })
                    
        except Exception as e:
            logger.error(f"Erro no aprendizado: {e}")
    
    def _classify_quality_issue(self, rule: Dict, result: ValidationResult) -> str:
        """Classifica o tipo de issue de qualidade baseado na regra."""
        rule_type = rule.get('type', '').lower()
        
        if rule_type == 'not_null':
            return "null_values"
        elif rule_type == 'unique':
            return "duplicate_values"
        elif 'regexp' in rule.get('sql', '').lower():
            return "format_invalid"
        elif 'between' in rule.get('sql', '').lower():
            return "range_invalid"
        else:
            return "data_inconsistency"
    
    def _extract_column_from_rule(self, rule: Dict) -> Optional[str]:
        """Extrai nome da coluna da regra."""
        sql = rule.get('sql', '')
        
        # Tentar extrair de diferentes padrões
        patterns = [
            r'MODIFY\s+(\w+)',
            r'CHECK\s*\(\s*(\w+)',
            r'WHERE\s+(\w+)',
            r'UNIQUE\s*\(\s*(\w+)',
            r'(\w+)\s+IS\s+NOT\s+NULL'
        ]
        
        for pattern in patterns:
            match = re.search(pattern, sql, re.IGNORECASE)
            if match:
                return match.group(1)
        
        return None
    
    def _generate_cleanup_recommendation(self, issue_type: str, rule: Dict) -> str:
        """Gera recomendação de limpeza baseada no tipo de issue."""
        recommendations = {
            "null_values": "Considere atualizar valores nulos ou tornar a coluna opcional",
            "duplicate_values": "Identifique e remova registros duplicados",
            "format_invalid": "Padronize o formato dos dados usando UPDATE com REGEXP_REPLACE",
            "range_invalid": "Verifique se os valores estão dentro do range esperado",
            "data_inconsistency": "Investigue a origem dos dados inconsistentes"
        }
        
        return recommendations.get(issue_type, "Investigue e corrija os dados")
    
    async def _store_column_insight(self, table_name: str, column: str, insight_type: str, data: Dict):
        """Armazena insight sobre coluna para uso futuro."""
        try:
            knowledge = {
                "category": "column_insight",
                "title": f"Insight: {column} em {table_name}",
                "content": f"Coluna {column} da tabela {table_name} possui {insight_type}: {json.dumps(data)}",
                "table_name": table_name,
                "column_name": column,
                "insight_type": insight_type,
                "tags": f"table:{table_name},column:{column},insight:{insight_type}",
                "priority": 3,
                "metadata": data
            }
            
            # Gerar embedding e salvar
            from ..ENGINE.vector_manager import VectorManager
            vector_manager = VectorManager()
            embedding = vector_manager.generate_embedding(knowledge["content"])
            
            if embedding:
                self.storage.save_knowledge(
                    category=knowledge["category"],
                    title=knowledge["title"],
                    content=knowledge["content"],
                    tags=knowledge["tags"],
                    priority=knowledge["priority"],
                    embedding_vector=vector_manager.vector_to_blob(embedding),
                    metadata=knowledge["metadata"]
                )
                
        except Exception as e:
            logger.error(f"Erro ao armazenar insight da coluna: {e}")
