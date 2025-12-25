from typing import Dict, List, Any, Optional
from datetime import datetime
import json

class SystemTables:
    
    SYSTEM_TABLES = {
        '__ribbit_tables',
        '__ribbit_columns',
        '__ribbit_indexes',
        '__ribbit_migrations',
        '__ribbit_views'
    }
    
    @staticmethod
    def is_system_table(table_name: str) -> bool:
        return table_name in SystemTables.SYSTEM_TABLES
    
    @staticmethod
    def create_system_tables(storage_engine) -> None:
        SystemTables._create_tables_table(storage_engine)
        SystemTables._create_columns_table(storage_engine)
        SystemTables._create_indexes_table(storage_engine)
        SystemTables._create_migrations_table(storage_engine)
        SystemTables._create_views_table(storage_engine)
    
    @staticmethod
    def _create_tables_table(storage_engine) -> None:
        schema = {
            'name': '__ribbit_tables',
            'columns': [
                {'name': 'name', 'type': 'TEXT', 'primary_key': True, 'not_null': True},
                {'name': 'type', 'type': 'TEXT', 'not_null': True},
                {'name': 'sql', 'type': 'TEXT'},
                {'name': 'created_at', 'type': 'TEXT', 'not_null': True}
            ]
        }
        
        if not storage_engine.table_exists('__ribbit_tables'):
            storage_engine.create_table('__ribbit_tables', schema['columns'])
    
    @staticmethod
    def _create_columns_table(storage_engine) -> None:
        schema = {
            'name': '__ribbit_columns',
            'columns': [
                {'name': 'table_name', 'type': 'TEXT', 'not_null': True},
                {'name': 'column_name', 'type': 'TEXT', 'not_null': True},
                {'name': 'column_type', 'type': 'TEXT', 'not_null': True},
                {'name': 'not_null', 'type': 'INTEGER', 'default': 0},
                {'name': 'default_value', 'type': 'TEXT'},
                {'name': 'primary_key', 'type': 'INTEGER', 'default': 0},
                {'name': 'autoincrement', 'type': 'INTEGER', 'default': 0},
                {'name': 'unique_constraint', 'type': 'INTEGER', 'default': 0},
                {'name': 'position', 'type': 'INTEGER', 'not_null': True},
                {'name': 'check_expression', 'type': 'TEXT'},
                {'name': 'foreign_key', 'type': 'TEXT'}
            ]
        }
        
        if not storage_engine.table_exists('__ribbit_columns'):
            storage_engine.create_table('__ribbit_columns', schema['columns'])
    
    @staticmethod
    def _create_indexes_table(storage_engine) -> None:
        schema = {
            'name': '__ribbit_indexes',
            'columns': [
                {'name': 'name', 'type': 'TEXT', 'primary_key': True, 'not_null': True},
                {'name': 'table_name', 'type': 'TEXT', 'not_null': True},
                {'name': 'column_name', 'type': 'TEXT', 'not_null': True},
                {'name': 'unique_index', 'type': 'INTEGER', 'default': 0},
                {'name': 'created_at', 'type': 'TEXT', 'not_null': True}
            ]
        }
        
        if not storage_engine.table_exists('__ribbit_indexes'):
            storage_engine.create_table('__ribbit_indexes', schema['columns'])
    
    @staticmethod
    def _create_migrations_table(storage_engine) -> None:
        schema = {
            'name': '__ribbit_migrations',
            'columns': [
                {'name': 'id', 'type': 'INTEGER', 'primary_key': True, 'autoincrement': True},
                {'name': 'name', 'type': 'TEXT', 'unique': True, 'not_null': True},
                {'name': 'applied_at', 'type': 'TEXT', 'not_null': True}
            ]
        }
        
        if not storage_engine.table_exists('__ribbit_migrations'):
            storage_engine.create_table('__ribbit_migrations', schema['columns'])
    
    @staticmethod
    def register_table(storage_engine, table_name: str, columns: List[Dict], sql: str = None) -> None:
        if SystemTables.is_system_table(table_name):
            return
        
        now = datetime.now().isoformat()
        
        storage_engine.insert('__ribbit_tables', {
            'name': table_name,
            'type': 'table',
            'sql': sql or '',
            'created_at': now
        })
        
        for i, col in enumerate(columns):
            storage_engine.insert('__ribbit_columns', {
                'table_name': table_name,
                'column_name': col['name'],
                'column_type': col['type'],
                'not_null': 1 if col.get('not_null', False) else 0,
                'default_value': str(col.get('default')) if col.get('default') is not None else None,
                'primary_key': 1 if col.get('primary_key', False) else 0,
                'autoincrement': 1 if col.get('autoincrement', False) else 0,
                'unique_constraint': 1 if col.get('unique', False) else 0,
                'position': i,
                'check_expression': col.get('check'),
                'foreign_key': json.dumps(col.get('foreign_key')) if col.get('foreign_key') else None
            })
    
    @staticmethod
    def unregister_table(storage_engine, table_name: str) -> None:
        if SystemTables.is_system_table(table_name):
            return
        
        storage_engine.delete('__ribbit_tables', {'name': table_name})
        storage_engine.delete('__ribbit_columns', {'table_name': table_name})
        
        indexes = storage_engine.select('__ribbit_indexes', where={'table_name': table_name})
        for index in indexes:
            storage_engine.delete('__ribbit_indexes', {'name': index['name']})
    
    @staticmethod
    def register_index(storage_engine, index_name: str, table_name: str, column_name: str, unique: bool = False) -> None:
        now = datetime.now().isoformat()
        
        storage_engine.insert('__ribbit_indexes', {
            'name': index_name,
            'table_name': table_name,
            'column_name': column_name,
            'unique_index': 1 if unique else 0,
            'created_at': now
        })
    
    @staticmethod
    def unregister_index(storage_engine, index_name: str) -> None:
        storage_engine.delete('__ribbit_indexes', {'name': index_name})
    
    @staticmethod
    def get_table_info(storage_engine, table_name: str) -> Optional[Dict]:
        tables = storage_engine.select('__ribbit_tables', where={'name': table_name})
        if not tables:
            return None
        return tables[0]
    
    @staticmethod
    def get_table_columns(storage_engine, table_name: str) -> List[Dict]:
        columns = storage_engine.select('__ribbit_columns', where={'table_name': table_name})
        return sorted(columns, key=lambda x: x['position'])
    
    @staticmethod
    def get_table_indexes(storage_engine, table_name: str) -> List[Dict]:
        return storage_engine.select('__ribbit_indexes', where={'table_name': table_name})
    
    @staticmethod
    def get_all_tables(storage_engine, include_system: bool = False) -> List[Dict]:
        all_tables = storage_engine.select('__ribbit_tables')
        if not include_system:
            all_tables = [t for t in all_tables if not SystemTables.is_system_table(t['name'])]
        return all_tables
    
    @staticmethod
    def get_all_indexes(storage_engine) -> List[Dict]:
        return storage_engine.select('__ribbit_indexes')
    
    @staticmethod
    def table_exists(storage_engine, table_name: str) -> bool:
        if SystemTables.is_system_table(table_name):
            return True
        tables = storage_engine.select('__ribbit_tables', where={'name': table_name})
        return len(tables) > 0
    
    @staticmethod
    def register_migration(storage_engine, migration_name: str) -> None:
        now = datetime.now().isoformat()
        storage_engine.insert('__ribbit_migrations', {
            'name': migration_name,
            'applied_at': now
        })
    
    @staticmethod
    def unregister_migration(storage_engine, migration_name: str) -> None:
        storage_engine.delete('__ribbit_migrations', {'name': migration_name})
    
    @staticmethod
    def get_applied_migrations(storage_engine) -> List[Dict]:
        return storage_engine.select('__ribbit_migrations')
    
    @staticmethod
    def migration_applied(storage_engine, migration_name: str) -> bool:
        migrations = storage_engine.select('__ribbit_migrations', where={'name': migration_name})
        return len(migrations) > 0

    @staticmethod
    def _create_views_table(storage_engine) -> None:
        schema = {
            'name': '__ribbit_views',
            'columns': [
                {'name': 'name', 'type': 'TEXT', 'primary_key': True, 'not_null': True},
                {'name': 'sql', 'type': 'TEXT', 'not_null': True},
                {'name': 'definition', 'type': 'BLOB'},
                {'name': 'created_at', 'type': 'TEXT', 'not_null': True}
            ]
        }
        
        if not storage_engine.table_exists('__ribbit_views'):
            storage_engine.create_table('__ribbit_views', schema['columns'])

    @staticmethod
    @staticmethod
    def register_view(storage_engine, name: str, sql: str, definition: Dict = None) -> None:
        now = datetime.now().isoformat()
        import pickle
        blob = pickle.dumps(definition) if definition else None
        storage_engine.insert('__ribbit_views', {
            'name': name,
            'sql': sql,
            'definition': blob,
            'created_at': now
        })

    @staticmethod
    def unregister_view(storage_engine, name: str) -> None:
        storage_engine.delete('__ribbit_views', {'name': name})

    @staticmethod
    def get_view(storage_engine, name: str) -> Optional[Dict]:
        views = storage_engine.select('__ribbit_views', where={'name': name})
        if views:
            return views[0]
        return None
