from typing import Dict, Optional, Any
from .btree import BTree
from ..utils.constants import BTREE_ORDER

class IndexManager:
    def __init__(self):
        self.indexes: Dict[str, BTree] = {}
    
    def create_index(self, index_name: str, order: int = BTREE_ORDER) -> bool:
        if index_name in self.indexes:
            return False
        self.indexes[index_name] = BTree(order)
        return True
    
    def drop_index(self, index_name: str) -> bool:
        if index_name not in self.indexes:
            return False
        del self.indexes[index_name]
        return True
    
    def get_index(self, index_name: str) -> Optional[BTree]:
        return self.indexes.get(index_name)
    
    def insert(self, index_name: str, key: Any, value: Any) -> bool:
        if index_name not in self.indexes:
            return False
        self.indexes[index_name].insert(key, value)
        return True
    
    def search(self, index_name: str, key: Any) -> Optional[Any]:
        if index_name not in self.indexes:
            return None
        return self.indexes[index_name].search(key)
    
    def range_search(self, index_name: str, start_key: Any, end_key: Any):
        if index_name not in self.indexes:
            return []
        return self.indexes[index_name].range_search(start_key, end_key)
    
    def index_exists(self, index_name: str) -> bool:
        return index_name in self.indexes
