import re
from typing import Any, Dict, List, Tuple

IDENTIFIER_RE = re.compile(r"^[A-Z0-9_]+$", re.IGNORECASE)


class SQLBuilder:
    """Builder determinístico de SQL a partir de um plano semântico."""

    def __init__(self, plan: Dict[str, Any]):
        self.plan = plan or {}

    @classmethod
    def from_plan(cls, plan: Dict[str, Any]) -> "SQLBuilder":
        return cls(plan)

    def build(self, dialect: str = "oracle") -> Tuple[str, List[Any], str]:
        if not self.plan:
            raise ValueError("Plano de consulta vazio.")

        dialect = (self.plan.get("dialect") or dialect or "oracle").lower()
        table = self.plan.get("table")
        if not table or not self._is_identifier(table):
            raise ValueError("Tabela inválida no plano.")

        schema = self.plan.get("schema")
        table_ref = f"{schema}.{table}" if schema and self._is_identifier(schema) else table

        fields = self._build_fields()
        sql = f"SELECT {fields} FROM {table_ref}"

        where_sql, params = self._build_where(dialect)
        if where_sql:
            sql += f" WHERE {where_sql}"

        group_by = self.plan.get("group_by")
        if group_by:
            group_by_expr = self._safe_field(group_by)
            if group_by_expr:
                sql += f" GROUP BY {group_by_expr}"

        order_by = self.plan.get("order_by", [])
        if order_by:
            order_parts = []
            for item in order_by:
                field = self._safe_field(item.get("field"))
                if not field:
                    continue
                direction = (item.get("direction") or "ASC").upper()
                if direction not in ("ASC", "DESC"):
                    direction = "ASC"
                order_parts.append(f"{field} {direction}")
            if order_parts:
                sql += " ORDER BY " + ", ".join(order_parts)

        limit = self.plan.get("limit")
        if limit:
            limit_value = int(limit)
            if dialect == "oracle":
                sql += f" FETCH FIRST {limit_value} ROWS ONLY"
            else:
                sql += f" LIMIT {limit_value}"

        return sql, params, dialect

    def _build_fields(self) -> str:
        aggregations = self.plan.get("aggregations") or []
        fields = self.plan.get("fields") or []
        field_parts: List[str] = []

        for field in fields:
            safe = self._safe_field(field)
            if safe:
                field_parts.append(safe)

        for agg in aggregations:
            func = (agg.get("func") or "").upper()
            if func not in ("COUNT", "SUM", "AVG", "MIN", "MAX"):
                continue
            raw_field = agg.get("field", "*")
            field_expr = "*" if raw_field == "*" else self._safe_field(raw_field)
            if not field_expr:
                continue
            agg_expr = f"{func}({field_expr})"
            alias = agg.get("as")
            if alias and self._is_identifier(alias):
                agg_expr += f" AS {alias}"
            field_parts.append(agg_expr)

        return ", ".join(field_parts) if field_parts else "*"

    def _build_where(self, dialect: str) -> Tuple[str, List[Any]]:
        filters = self.plan.get("filters") or []
        clauses: List[str] = []
        params: List[Any] = []

        for flt in filters:
            field = self._safe_field(flt.get("field"))
            if not field:
                continue

            op = (flt.get("op") or "=").upper()
            if op not in ("=", "LIKE", ">", ">=", "<", "<="):
                op = "="

            value = flt.get("value")
            column_expr = field
            if flt.get("normalize") == "digits":
                column_expr = self._digits_expr(field)

            if flt.get("case_insensitive"):
                column_expr = f"UPPER({column_expr})"
                if isinstance(value, str):
                    value = value.upper()

            placeholder = self._placeholder(len(params) + 1, dialect)
            clauses.append(f"{column_expr} {op} {placeholder}")
            params.append(value)

        return " AND ".join(clauses), params

    def _digits_expr(self, field: str) -> str:
        return (
            "REPLACE(REPLACE(REPLACE(REPLACE(" + field + ", '-', ''), ' ', ''), '(', ''), ')', '')"
        )

    def _placeholder(self, index: int, dialect: str) -> str:
        return f":{index}" if dialect == "oracle" else "?"

    def _safe_field(self, field: str) -> str:
        if not field:
            return ""
        if field == "*":
            return "*"
        return field if self._is_identifier(field) else ""

    def _is_identifier(self, value: str) -> bool:
        return bool(IDENTIFIER_RE.match(str(value).strip()))
