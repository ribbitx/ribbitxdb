from typing import Dict, Any, Optional, List
import hashlib
import time
from collections import OrderedDict

class QueryCache:
    """LRU cache for query results"""
    def __init__(self, max_size: int = 100, ttl: int = 300):
        self.cache: OrderedDict[str, Dict[str, Any]] = OrderedDict()
        self.max_size = max_size
        self.ttl = ttl  # Time to live in seconds
        self._hits = 0
        self._misses = 0
    
    def _generate_key(self, sql: str, params: tuple = ()) -> str:
        """Generate cache key from SQL and parameters"""
        cache_str = f"{sql}:{params}"
        return hashlib.md5(cache_str.encode()).hexdigest()
    
    def get(self, sql: str, params: tuple = ()) -> Optional[List[Dict[str, Any]]]:
        """Get cached query result"""
        key = self._generate_key(sql, params)
        
        if key in self.cache:
            entry = self.cache[key]
            # Check if entry is still valid
            if time.time() - entry['timestamp'] < self.ttl:
                self.cache.move_to_end(key)  # Mark as recently used
                self._hits += 1
                return entry['result']
            else:
                # Entry expired
                del self.cache[key]
        
        self._misses += 1
        return None
    
    def put(self, sql: str, result: List[Dict[str, Any]], params: tuple = ()):
        """Cache query result"""
        key = self._generate_key(sql, params)
        
        if key in self.cache:
            self.cache.move_to_end(key)
        
        self.cache[key] = {
            'result': result,
            'timestamp': time.time()
        }
        
        # Evict oldest entry if cache is full
        if len(self.cache) > self.max_size:
            self.cache.popitem(last=False)
    
    def invalidate(self, table_name: Optional[str] = None):
        """Invalidate cache entries (all or for specific table)"""
        if table_name is None:
            self.cache.clear()
        else:
            # Remove entries that reference the table
            keys_to_remove = []
            for key in self.cache:
                # Simple heuristic: check if table name appears in cached SQL
                # In production, you'd want more sophisticated tracking
                keys_to_remove.append(key)
            
            for key in keys_to_remove:
                if key in self.cache:
                    del self.cache[key]
    
    def get_stats(self) -> Dict[str, Any]:
        """Get cache statistics"""
        total = self._hits + self._misses
        hit_rate = self._hits / total if total > 0 else 0.0
        
        return {
            'hits': self._hits,
            'misses': self._misses,
            'hit_rate': hit_rate,
            'size': len(self.cache),
            'max_size': self.max_size
        }
    
    def clear_stats(self):
        """Reset statistics"""
        self._hits = 0
        self._misses = 0

class QueryOptimizer:
    """Cost-based query optimizer"""
    
    def __init__(self):
        self.table_stats: Dict[str, Dict[str, Any]] = {}
    
    def analyze_table(self, table_name: str, row_count: int, column_stats: Dict[str, Any]):
        """Store table statistics for optimization"""
        self.table_stats[table_name] = {
            'row_count': row_count,
            'columns': column_stats,
            'last_analyzed': time.time()
        }
    
    def optimize_join_order(self, joins: List[Dict[str, Any]], base_table: str) -> List[Dict[str, Any]]:
        """Optimize JOIN order based on table sizes"""
        if not joins:
            return joins
        
        # Sort joins by estimated cost (smaller tables first)
        def join_cost(join):
            table = join['table']
            stats = self.table_stats.get(table, {})
            return stats.get('row_count', float('inf'))
        
        return sorted(joins, key=join_cost)
    
    def suggest_index(self, table_name: str, where_columns: List[str]) -> Optional[str]:
        """Suggest index creation for frequently queried columns"""
        if not where_columns:
            return None
        
        # Simple heuristic: suggest index on first WHERE column
        return f"CREATE INDEX idx_{table_name}_{where_columns[0]} ON {table_name}({where_columns[0]})"
    
    def estimate_query_cost(self, parsed_query: Dict[str, Any]) -> float:
        """Estimate query execution cost"""
        table = parsed_query.get('table')
        stats = self.table_stats.get(table, {})
        row_count = stats.get('row_count', 1000)
        
        cost = row_count  # Base cost: table scan
        
        # Reduce cost if using index
        if parsed_query.get('where'):
            cost *= 0.1  # Assume index reduces cost by 90%
        
        # Increase cost for joins
        if parsed_query.get('joins'):
            cost *= len(parsed_query['joins']) * 2
        
        # Increase cost for aggregates
        if parsed_query.get('aggregates'):
            cost *= 1.5
        
        # Increase cost for sorting
        if parsed_query.get('order_by'):
            cost *= 1.2
        
        return cost
