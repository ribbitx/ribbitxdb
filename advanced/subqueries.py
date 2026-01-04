from typing import List, Dict, Any, Optional
from ..query.parser import SQLParser

class SubqueryExecutor:
    
    def __init__(self, storage_manager, schema_manager, index_manager):
        self.storage = storage_manager
        self.schema = schema_manager
        self.indexes = index_manager
        self.parser = SQLParser()
    
    def execute_scalar_subquery(self, subquery_sql: str, context: Dict[str, Any]) -> Any:
        from ..query.executor import QueryExecutor
        executor = QueryExecutor(self.storage, self.schema, self.indexes)
        result = executor.execute(subquery_sql)
        
        if result and len(result) > 0:
            first_row = result[0]
            if isinstance(first_row, dict):
                return list(first_row.values())[0]
            return first_row[0] if isinstance(first_row, (list, tuple)) else first_row
        return None
    
    def execute_exists_subquery(self, subquery_sql: str, context: Dict[str, Any]) -> bool:
        from ..query.executor import QueryExecutor
        executor = QueryExecutor(self.storage, self.schema, self.indexes)
        result = executor.execute(subquery_sql)
        return len(result) > 0
    
    def execute_in_subquery(self, subquery_sql: str, context: Dict[str, Any]) -> List[Any]:
        from ..query.executor import QueryExecutor
        executor = QueryExecutor(self.storage, self.schema, self.indexes)
        result = executor.execute(subquery_sql)
        
        values = []
        for row in result:
            if isinstance(row, dict):
                values.append(list(row.values())[0])
            elif isinstance(row, (list, tuple)):
                values.append(row[0])
            else:
                values.append(row)
        return values
    
    def execute_correlated_subquery(self, subquery_sql: str, outer_row: Dict[str, Any]) -> Any:
        modified_sql = self._substitute_outer_references(subquery_sql, outer_row)
        return self.execute_scalar_subquery(modified_sql, {})
    
    def _substitute_outer_references(self, sql: str, outer_row: Dict[str, Any]) -> str:
        for column, value in outer_row.items():
            if isinstance(value, str):
                sql = sql.replace(f"OUTER.{column}", f"'{value}'")
            else:
                sql = sql.replace(f"OUTER.{column}", str(value))
        return sql
