from typing import List, Dict, Any, Optional

class CTEExecutor:
    
    def __init__(self, storage_manager, schema_manager, index_manager):
        self.storage = storage_manager
        self.schema = schema_manager
        self.indexes = index_manager
        self.materialized_ctes = {}
    
    def execute_with_clause(self, ctes: List[Dict[str, Any]], main_query: str) -> List[Dict[str, Any]]:
        self.materialized_ctes.clear()
        
        for cte in ctes:
            cte_name = cte['name']
            cte_query = cte['query']
            
            from ..query.executor import QueryExecutor
            executor = QueryExecutor(self.storage, self.schema, self.indexes)
            result = executor.execute(cte_query)
            
            self.materialized_ctes[cte_name] = result
        
        modified_query = self._replace_cte_references(main_query)
        
        from ..query.executor import QueryExecutor
        executor = QueryExecutor(self.storage, self.schema, self.indexes)
        executor.cte_data = self.materialized_ctes
        return executor.execute(modified_query)
    
    def _replace_cte_references(self, query: str) -> str:
        for cte_name in self.materialized_ctes.keys():
            query = query.replace(f"FROM {cte_name}", f"FROM __CTE_{cte_name}__")
            query = query.replace(f"JOIN {cte_name}", f"JOIN __CTE_{cte_name}__")
        return query
    
    def get_cte_data(self, cte_name: str) -> Optional[List[Dict[str, Any]]]:
        return self.materialized_ctes.get(cte_name)
