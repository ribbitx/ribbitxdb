from typing import List, Dict, Any, Callable
from collections import deque

class WindowFunctionExecutor:
    
    def __init__(self):
        self.functions = {
            'ROW_NUMBER': self._row_number,
            'RANK': self._rank,
            'DENSE_RANK': self._dense_rank,
            'LAG': self._lag,
            'LEAD': self._lead,
            'FIRST_VALUE': self._first_value,
            'LAST_VALUE': self._last_value,
            'NTILE': self._ntile
        }
    
    def execute_window_function(self, rows: List[Dict[str, Any]], 
                                function_name: str, 
                                partition_by: List[str],
                                order_by: List[tuple],
                                args: List[Any] = None) -> List[Any]:
        
        if not rows:
            return []
        
        partitions = self._partition_rows(rows, partition_by)
        results = []
        
        for partition in partitions:
            sorted_partition = self._sort_partition(partition, order_by)
            func = self.functions.get(function_name.upper())
            
            if func:
                partition_results = func(sorted_partition, args or [])
                results.extend(partition_results)
            else:
                results.extend([None] * len(sorted_partition))
        
        return results
    
    def _partition_rows(self, rows: List[Dict[str, Any]], partition_by: List[str]) -> List[List[Dict[str, Any]]]:
        if not partition_by:
            return [rows]
        
        partitions = {}
        for row in rows:
            key = tuple(row.get(col) for col in partition_by)
            if key not in partitions:
                partitions[key] = []
            partitions[key].append(row)
        
        return list(partitions.values())
    
    def _sort_partition(self, partition: List[Dict[str, Any]], order_by: List[tuple]) -> List[Dict[str, Any]]:
        if not order_by:
            return partition
        
        def sort_key(row):
            return tuple(row.get(col) for col, _ in order_by)
        
        reverse = any(direction == 'DESC' for _, direction in order_by)
        return sorted(partition, key=sort_key, reverse=reverse)
    
    def _row_number(self, partition: List[Dict[str, Any]], args: List[Any]) -> List[int]:
        return list(range(1, len(partition) + 1))
    
    def _rank(self, partition: List[Dict[str, Any]], args: List[Any]) -> List[int]:
        ranks = []
        current_rank = 1
        prev_value = None
        
        for i, row in enumerate(partition):
            current_value = tuple(row.values())
            if prev_value is not None and current_value != prev_value:
                current_rank = i + 1
            ranks.append(current_rank)
            prev_value = current_value
        
        return ranks
    
    def _dense_rank(self, partition: List[Dict[str, Any]], args: List[Any]) -> List[int]:
        ranks = []
        current_rank = 1
        prev_value = None
        
        for row in partition:
            current_value = tuple(row.values())
            if prev_value is not None and current_value != prev_value:
                current_rank += 1
            ranks.append(current_rank)
            prev_value = current_value
        
        return ranks
    
    def _lag(self, partition: List[Dict[str, Any]], args: List[Any]) -> List[Any]:
        offset = args[0] if args else 1
        default = args[1] if len(args) > 1 else None
        column = args[2] if len(args) > 2 else list(partition[0].keys())[0]
        
        results = []
        for i in range(len(partition)):
            if i < offset:
                results.append(default)
            else:
                results.append(partition[i - offset].get(column))
        
        return results
    
    def _lead(self, partition: List[Dict[str, Any]], args: List[Any]) -> List[Any]:
        offset = args[0] if args else 1
        default = args[1] if len(args) > 1 else None
        column = args[2] if len(args) > 2 else list(partition[0].keys())[0]
        
        results = []
        for i in range(len(partition)):
            if i + offset >= len(partition):
                results.append(default)
            else:
                results.append(partition[i + offset].get(column))
        
        return results
    
    def _first_value(self, partition: List[Dict[str, Any]], args: List[Any]) -> List[Any]:
        if not partition:
            return []
        
        column = args[0] if args else list(partition[0].keys())[0]
        first_val = partition[0].get(column)
        return [first_val] * len(partition)
    
    def _last_value(self, partition: List[Dict[str, Any]], args: List[Any]) -> List[Any]:
        if not partition:
            return []
        
        column = args[0] if args else list(partition[0].keys())[0]
        last_val = partition[-1].get(column)
        return [last_val] * len(partition)
    
    def _ntile(self, partition: List[Dict[str, Any]], args: List[Any]) -> List[int]:
        n = args[0] if args else 4
        partition_size = len(partition)
        bucket_size = partition_size // n
        remainder = partition_size % n
        
        results = []
        current_bucket = 1
        count_in_bucket = 0
        bucket_limit = bucket_size + (1 if current_bucket <= remainder else 0)
        
        for _ in partition:
            results.append(current_bucket)
            count_in_bucket += 1
            
            if count_in_bucket >= bucket_limit and current_bucket < n:
                current_bucket += 1
                count_in_bucket = 0
                bucket_limit = bucket_size + (1 if current_bucket <= remainder else 0)
        
        return results
