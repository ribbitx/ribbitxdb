from typing import Dict, List, Optional, Any
from .types import DataType, TypeConverter
import pickle

class Column:
    def __init__(self, name: str, data_type: DataType, 
                 primary_key: bool = False, not_null: bool = False,
                 unique: bool = False, default: Any = None, 
                 autoincrement: bool = False, check: str = None, 
                 foreign_key: dict = None):
        self.name = name
        self.data_type = data_type
        self.primary_key = primary_key
        self.not_null = not_null
        self.unique = unique
        self.default = default
        self.autoincrement = autoincrement
        self.check = check
        self.foreign_key = foreign_key
    
    def to_dict(self) -> dict:
        return {
            'name': self.name,
            'data_type': self.data_type.value,
            'primary_key': self.primary_key,
            'not_null': self.not_null,
            'unique': self.unique,
            'default': self.default,
            'autoincrement': self.autoincrement,
            'check': self.check,
            'foreign_key': self.foreign_key
        }
    
    @classmethod
    def from_dict(cls, data: dict) -> 'Column':
        return cls(
            name=data['name'],
            data_type=DataType(data['data_type']),
            primary_key=data.get('primary_key', False),
            not_null=data.get('not_null', False),
            unique=data.get('unique', False),
            default=data.get('default'),
            autoincrement=data.get('autoincrement', False),
            check=data.get('check'),
            foreign_key=data.get('foreign_key')
        )

class Table:
    def __init__(self, name: str, columns: List[Column]):
        self.name = name
        self.columns = columns
        self.column_map = {col.name: col for col in columns}
        self.primary_key = next((col.name for col in columns if col.primary_key), None)
    
    def get_column(self, name: str) -> Optional[Column]:
        return self.column_map.get(name)
    
    def validate_row(self, row: Dict[str, Any]) -> bool:
        for col in self.columns:
            value = row.get(col.name)
            
            if value is None:
                if col.not_null and col.default is None:
                    return False
                continue
            
            if not TypeConverter.validate_type(value, col.data_type):
                return False
        
        return True
    
    def to_dict(self) -> dict:
        return {
            'name': self.name,
            'columns': [col.to_dict() for col in self.columns]
        }
    
    @classmethod
    def from_dict(cls, data: dict) -> 'Table':
        columns = [Column.from_dict(col) for col in data['columns']]
        return cls(data['name'], columns)

class SchemaManager:
    def __init__(self):
        self.tables: Dict[str, Table] = {}
        self.indexes: Dict[str, List[str]] = {}
    
    def create_table(self, table: Table) -> bool:
        if table.name in self.tables:
            return False
        self.tables[table.name] = table
        return True
    
    def drop_table(self, table_name: str) -> bool:
        if table_name not in self.tables:
            return False
        del self.tables[table_name]
        if table_name in self.indexes:
            del self.indexes[table_name]
        return True
    
    def get_table(self, table_name: str) -> Optional[Table]:
        return self.tables.get(table_name)
    
    def table_exists(self, table_name: str) -> bool:
        return table_name in self.tables
    
    def list_tables(self) -> List[str]:
        return list(self.tables.keys())
    
    def create_index(self, table_name: str, column_name: str) -> bool:
        if table_name not in self.tables:
            return False
        
        table = self.tables[table_name]
        if column_name not in table.column_map:
            return False
        
        if table_name not in self.indexes:
            self.indexes[table_name] = []
        
        if column_name not in self.indexes[table_name]:
            self.indexes[table_name].append(column_name)
        
        return True
    
    def serialize(self) -> bytes:
        data = {
            'tables': {name: table.to_dict() for name, table in self.tables.items()},
            'indexes': self.indexes
        }
        return pickle.dumps(data)
    
    @classmethod
    def deserialize(cls, data: bytes) -> 'SchemaManager':
        manager = cls()
        loaded = pickle.loads(data)
        
        for name, table_data in loaded['tables'].items():
            manager.tables[name] = Table.from_dict(table_data)
        
        manager.indexes = loaded.get('indexes', {})
        return manager
