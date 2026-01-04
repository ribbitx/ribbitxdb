from typing import Any, Optional, List, Tuple
from collections import OrderedDict
import pickle

class LRUCache:
    """Least Recently Used cache for page caching"""
    def __init__(self, capacity: int = 1000):
        self.cache = OrderedDict()
        self.capacity = capacity
    
    def get(self, key: Any) -> Optional[Any]:
        if key not in self.cache:
            return None
        self.cache.move_to_end(key)
        return self.cache[key]
    
    def put(self, key: Any, value: Any):
        if key in self.cache:
            self.cache.move_to_end(key)
        self.cache[key] = value
        if len(self.cache) > self.capacity:
            self.cache.popitem(last=False)
    
    def clear(self):
        self.cache.clear()
    
    def size(self) -> int:
        return len(self.cache)

class BTreeNode:
    def __init__(self, order: int, is_leaf: bool = True):
        self.order = order
        self.is_leaf = is_leaf
        self.keys: List[Any] = []
        self.values: List[Any] = []
        self.children: List['BTreeNode'] = []
    
    def is_full(self) -> bool:
        return len(self.keys) >= self.order - 1
    
    def insert_non_full(self, key: Any, value: Any):
        i = len(self.keys) - 1
        
        if self.is_leaf:
            self.keys.append(None)
            self.values.append(None)
            
            while i >= 0 and key < self.keys[i]:
                self.keys[i + 1] = self.keys[i]
                self.values[i + 1] = self.values[i]
                i -= 1
            
            self.keys[i + 1] = key
            self.values[i + 1] = value
        else:
            while i >= 0 and key < self.keys[i]:
                i -= 1
            i += 1
            
            if self.children[i].is_full():
                self.split_child(i)
                if key > self.keys[i]:
                    i += 1
            
            self.children[i].insert_non_full(key, value)
    
    def split_child(self, index: int):
        order = self.order
        child = self.children[index]
        new_child = BTreeNode(order, child.is_leaf)
        
        mid = order // 2
        
        new_child.keys = child.keys[mid:]
        child.keys = child.keys[:mid]
        
        if child.is_leaf:
            new_child.values = child.values[mid:]
            child.values = child.values[:mid]
        else:
            new_child.children = child.children[mid:]
            child.children = child.children[:mid]
        
        self.keys.insert(index, child.keys[-1] if child.keys else None)
        self.values.insert(index, None)
        self.children.insert(index + 1, new_child)
    
    def search(self, key: Any) -> Optional[Any]:
        # Binary search for better performance
        left, right = 0, len(self.keys) - 1
        i = -1
        
        while left <= right:
            mid = (left + right) // 2
            if self.keys[mid] == key:
                i = mid
                break
            elif self.keys[mid] < key:
                left = mid + 1
            else:
                right = mid - 1
        
        if i >= 0 and i < len(self.keys) and key == self.keys[i]:
            return self.values[i] if self.is_leaf else None
        
        if self.is_leaf:
            return None
        
        # Find correct child
        i = 0
        while i < len(self.keys) and key > self.keys[i]:
            i += 1
        
        return self.children[i].search(key)
    
    def range_search(self, start_key: Any, end_key: Any) -> List[Tuple[Any, Any]]:
        results = []
        
        i = 0
        while i < len(self.keys) and start_key > self.keys[i]:
            i += 1
        
        if not self.is_leaf:
            if i < len(self.children):
                results.extend(self.children[i].range_search(start_key, end_key))
        
        while i < len(self.keys) and self.keys[i] <= end_key:
            if self.keys[i] >= start_key:
                results.append((self.keys[i], self.values[i]))
            i += 1
            
            if not self.is_leaf and i < len(self.children):
                results.extend(self.children[i].range_search(start_key, end_key))
        
        return results
    
    def bulk_load(self, items: List[Tuple[Any, Any]]):
        """Optimized bulk loading for sorted items"""
        if not items:
            return
        
        items.sort(key=lambda x: x[0])
        
        if self.is_leaf:
            self.keys = [item[0] for item in items]
            self.values = [item[1] for item in items]
        else:
            # For internal nodes, distribute items among children
            chunk_size = max(1, len(items) // (self.order - 1))
            for i in range(0, len(items), chunk_size):
                chunk = items[i:i + chunk_size]
                if chunk:
                    child = BTreeNode(self.order, is_leaf=True)
                    child.bulk_load(chunk)
                    self.children.append(child)
                    if chunk:
                        self.keys.append(chunk[-1][0])

class BTree:
    def __init__(self, order: int = 256):  # Increased order for better performance
        self.order = order
        self.root = BTreeNode(order)
        self.cache = LRUCache(capacity=500)  # Cache for frequently accessed nodes
        self._stats = {'searches': 0, 'inserts': 0, 'cache_hits': 0}
    
    def insert(self, key: Any, value: Any):
        self._stats['inserts'] += 1
        self.cache.clear()  # Invalidate cache on insert
        
        if self.root.is_full():
            new_root = BTreeNode(self.order, is_leaf=False)
            new_root.children.append(self.root)
            new_root.split_child(0)
            self.root = new_root
        
        self.root.insert_non_full(key, value)
    
    def search(self, key: Any) -> Optional[Any]:
        self._stats['searches'] += 1
        
        # Check cache first
        cache_key = f"search_{key}"
        cached = self.cache.get(cache_key)
        if cached is not None:
            self._stats['cache_hits'] += 1
            return cached
        
        result = self.root.search(key)
        
        # Cache the result
        if result is not None:
            self.cache.put(cache_key, result)
        
        return result
    
    def range_search(self, start_key: Any, end_key: Any) -> List[Tuple[Any, Any]]:
        return self.root.range_search(start_key, end_key)
    
    def bulk_insert(self, items: List[Tuple[Any, Any]]):
        """Optimized bulk insert operation"""
        self._stats['inserts'] += len(items)
        self.cache.clear()
        
        # Sort items first
        items.sort(key=lambda x: x[0])
        
        # Use bulk load for better performance
        self.root = BTreeNode(self.order)
        
        # Build tree bottom-up for sorted data
        for key, value in items:
            self.insert(key, value)
    
    def get_stats(self) -> dict:
        """Get performance statistics"""
        stats = self._stats.copy()
        if stats['searches'] > 0:
            stats['cache_hit_rate'] = stats['cache_hits'] / stats['searches']
        else:
            stats['cache_hit_rate'] = 0.0
        return stats
    
    def serialize(self) -> bytes:
        return pickle.dumps(self.root)
    
    @classmethod
    def deserialize(cls, data: bytes, order: int = 256) -> 'BTree':
        tree = cls(order)
        tree.root = pickle.loads(data)
        return tree
