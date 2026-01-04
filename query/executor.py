from typing import List, Dict, Any, Optional
import re
import pickle
import struct
import json
from datetime import datetime
from functools import reduce

from .parser import SQLParser
from ..schema.metadata import SchemaManager, Table, Column
from ..schema.types import DataType, TypeConverter
from ..schema.system_tables import SystemTables
from ..index.manager import IndexManager
from ..security.hasher import BLAKE2Hasher
from ..storage.engine import StorageEngine
from ..storage.page import Page
from ..transaction.manager import TransactionManager
from ..utils.exceptions import (
    SQLSyntaxError, TableNotFoundError, TableAlreadyExistsError,
    ColumnNotFoundError, TypeMismatchError, ConstraintViolationError,
    TransactionError, UnsupportedFeatureError
)

class QueryExecutor:
    def __init__(self, storage: StorageEngine, schema: SchemaManager, 
                 index_manager: IndexManager, hasher: BLAKE2Hasher,
                 transaction_manager: TransactionManager = None):
        self.storage = storage
        self.schema = schema
        self.index_manager = index_manager
        self.hasher = hasher
        self.transaction_manager = transaction_manager or TransactionManager()
        self.parser = SQLParser()
        self.table_pages: Dict[str, List[int]] = {}
        
        # Initialize system tables
        SystemTables.create_system_tables(self)
        self._load_metadata()
    
    def _load_metadata(self):
        # Load tables from system tables
        try:
            stored_tables = SystemTables.get_all_tables(self)
            for table_info in stored_tables:
                table_name = table_info['name']
                # Don't skip system tables - they need to be in schema too
                # if SystemTables.is_system_table(table_name):
                #     continue
                
                columns_data = SystemTables.get_table_columns(self, table_name)
                columns = []
                for col_data in columns_data:
                    columns.append(Column(
                        name=col_data['column_name'],
                        data_type=self._parse_data_type(col_data['column_type']),
                        primary_key=bool(col_data['primary_key']),
                        not_null=bool(col_data['not_null']),
                        unique=bool(col_data.get('unique_constraint', 0)),
                        default=col_data.get('default_value'),
                        autoincrement=bool(col_data.get('autoincrement', 0)),
                        check=col_data.get('check_expression'),
                        foreign_key=json.loads(col_data['foreign_key']) if col_data.get('foreign_key') else None
                    ))
                
                table = Table(table_name, columns)
                self.schema.create_table(table)
                # Load pages would happen here in a real implementation
                # self.table_pages[table_name] = self._find_pages(table_name)
        except Exception:
            pass # First run or error loading metadata
    
    def execute(self, sql: str) -> Any:
        parsed = self.parser.parse(sql)
        query_type = parsed['type']
        
        try:
            if query_type == 'SELECT':
                return self.execute_select(parsed)
            elif query_type == 'INSERT':
                return self.execute_insert(parsed)
            elif query_type == 'UPDATE':
                return self.execute_update(parsed)
            elif query_type == 'DELETE':
                return self.execute_delete(parsed)
            elif query_type == 'CREATE':
                return self.execute_create(parsed)
            elif query_type == 'CREATE_VIEW':
                return self.execute_create_view(parsed)
            elif query_type == 'CREATE_INDEX':
                return self.execute_create_index(parsed)
            elif query_type == 'ALTER':
                return self.execute_alter(parsed)
            elif query_type == 'DROP':
                return self.execute_drop(parsed)
            elif query_type == 'DROP_VIEW':
                return self.execute_drop_view(parsed)
            elif query_type == 'DROP_INDEX':
                return self.execute_drop_index(parsed)
            elif query_type == 'PRAGMA':
                return self.execute_pragma(parsed)
            elif query_type == 'BEGIN':
                return self.execute_begin()
            elif query_type == 'COMMIT':
                return self.execute_commit()
            elif query_type == 'ROLLBACK':
                return self.execute_rollback(parsed)
            elif query_type == 'SAVEPOINT':
                return self.execute_savepoint(parsed)
            elif query_type == 'RELEASE':
                return self.execute_release(parsed)
            elif query_type == 'DESCRIBE':
                return self.execute_describe(parsed)
            elif query_type == 'SHOW':
                return self.execute_show(parsed)
            elif query_type == 'EXPLAIN':
                return self.execute_explain(parsed)
            else:
                raise UnsupportedFeatureError(query_type)
        except Exception as e:
            if self.transaction_manager.has_active_transaction():
                # In strict mode, might rollback on error
                pass
            raise
    
    # ... (rest of methods implemented below) ...
    # Breaking this into parts for size
    
    def execute_create(self, parsed: Dict[str, Any]) -> bool:
        table_name = parsed['table']
        if_not_exists = parsed.get('if_not_exists', False)
        
        if self.schema.get_table(table_name) or self.table_exists(table_name):
            if if_not_exists:
                return False
            raise TableAlreadyExistsError(table_name)
        
        columns = []
        for i, col_def in enumerate(parsed['columns']):
            data_type = self._parse_data_type(col_def['type'])
            column = Column(
                name=col_def['name'],
                data_type=data_type,
                primary_key=col_def.get('primary_key', False),
                not_null=col_def.get('not_null', False),
                unique=col_def.get('unique', False),
                default=col_def.get('default'),
                autoincrement=col_def.get('autoincrement', False),
                check=col_def.get('check'),
                foreign_key=col_def.get('references')
            )
            columns.append(column)
        
        table = Table(table_name, columns)
        success = self.schema.create_table(table)
        
        if success:
            self.table_pages[table_name] = []
            
            # Register in system tables
            col_dicts = []
            for col in columns:
                col_dicts.append({
                    'name': col.name,
                    'type': col.data_type.name,
                    'primary_key': col.primary_key,
                    'not_null': col.not_null,
                    'unique': col.unique,
                    'default': col.default,
                    'autoincrement': col.autoincrement,
                    'check': col.check,
                    'foreign_key': col.foreign_key
                })
            
            # Reconstruct SQL for storage
            sql = f"CREATE TABLE {table_name} (" + ", ".join([f"{c.name} {c.data_type}" for c in columns]) + ")"
            
            SystemTables.register_table(self, table_name, col_dicts, sql)
            
            # Create PK index
            if table.primary_key:
                self.index_manager.create_index(f"{table_name}_pk")
        
        return success
    
    def execute_create_index(self, parsed: Dict[str, Any]) -> bool:
        index_name = parsed['index_name']
        table_name = parsed['table']
        columns = parsed['columns']
        unique = parsed['unique']
        if_not_exists = parsed.get('if_not_exists', False)
        
        if not self.table_exists(table_name):
            raise TableNotFoundError(table_name)
        
        # Check if index exists (mock check for now)
        try:
            self.index_manager.create_index(index_name)
            
            # Register in system tables
            for col in columns:
                SystemTables.register_index(self, index_name, table_name, col, unique)
                
            return True
        except Exception:
            if if_not_exists:
                return False
            raise

    def execute_drop(self, parsed: Dict[str, Any]) -> bool:
        table_name = parsed['table']
        if_exists = parsed.get('if_exists', False)
        
        if not self.table_exists(table_name):
            if if_exists:
                return False
            raise TableNotFoundError(table_name)
        
        if table_name in self.table_pages:
            del self.table_pages[table_name]
        
        # Cleanup indexes
        self.index_manager.drop_index(f"{table_name}_pk")
        
        # Remove from system tables
        SystemTables.unregister_table(self, table_name)
        
        return self.schema.drop_table(table_name)

    def execute_drop_view(self, parsed: Dict[str, Any]) -> bool:
        view_name = parsed['view_name']
        if_exists = parsed['if_exists']
        
        if not SystemTables.get_view(self, view_name):
            if if_exists:
                return False
            raise TableNotFoundError(f"View {view_name} not found")
            
        SystemTables.unregister_view(self, view_name)
        return True

    def execute_insert(self, parsed: Dict[str, Any]) -> int:
        table_name = parsed['table']
        table = self.schema.get_table(table_name)
        
        if not table:
            raise TableNotFoundError(table_name)
        
        columns = parsed.get('columns')
        values = parsed['values']
        
        # Handle Dictionary-based rows (from executemany or internal)
        if isinstance(values, dict):
             row_dicts = [values]
        # Handle List of Lists (bulk insert)
        elif values and isinstance(values[0], list) and not columns:
             # Basic implementation implies single row INSERT VALUES (...)
             # To support bulk, we'd iterate. For now, assume single row if simple list
              row_dicts = [dict(zip([col.name for col in table.columns], values))]
        # Handle parsed VALUES (...)
        else:
             if columns:
                 row_dicts = [dict(zip(columns, values))]
             else:
                 row_dicts = [dict(zip([col.name for col in table.columns], values))]

        count = 0
        for row_dict in row_dicts:
             processed_row = {}
             for col in table.columns:
                 if col.name in row_dict:
                     val = row_dict[col.name]
                 else:
                     # Handle DEFAULT and AUTOINCREMENT
                     if col.autoincrement:
                         val = self._next_autoincrement_value(table_name, col.name)
                         # Update stats
                     elif col.default is not None:
                         val = self._evaluate_default(col.default)
                     else:
                         val = None
                     
                 # Validate NOT NULL
                 if col.not_null and val is None:
                     raise ConstraintViolationError("NOT NULL", f"Column '{col.name}' cannot be NULL")
                 
                 processed_row[col.name] = val

             if not table.validate_row(processed_row):
                 raise ConstraintViolationError("Validation", f"Invalid row for table {table_name}")
             
             self._insert_row_internal(table_name, processed_row, table)
             
             if self.transaction_manager.has_active_transaction():
                 undo_op = {
                     'type': 'DELETE',
                     'table': table_name,
                     'row': processed_row
                 }
                 self.transaction_manager.get_active_transaction().add_operation(undo_op)
                 
             count += 1
        
        return count

    def _next_autoincrement_value(self, table_name: str, col_name: str) -> int:
        # Simple max+1 implementation. 
        # In real world, use a sequence table or page header stats.
        current_max = 0
        rows = self._scan_table(table_name)
        for row in rows:
            val = row.get(col_name)
            if isinstance(val, int) and val > current_max:
                current_max = val
        return current_max + 1

    def _evaluate_default(self, default_val: Any) -> Any:
        if isinstance(default_val, str):
            if default_val.upper() == 'CURRENT_TIMESTAMP':
                return datetime.now().isoformat()
            if default_val.upper() == 'CURRENT_DATE':
                return datetime.now().strftime('%Y-%m-%d')
            if default_val.upper() == 'CURRENT_TIME':
                return datetime.now().strftime('%H:%M:%S')
        return default_val

    def execute_select(self, parsed: Dict[str, Any]) -> List[Dict[str, Any]]:
        table_name = parsed['table']
        
        # 1. Get Source Rows (Table Scan or View)
        all_rows = self._get_source_rows(table_name)
        
        # Handle JOINs
        if parsed.get('joins'):
             all_rows = self._execute_joins(all_rows, parsed['joins'], table_name)
        
        # Apply WHERE clause
        if parsed.get('where'):
             all_rows = self._filter_rows_advanced(all_rows, parsed['where'])
        
        # Handle aggregates and GROUP BY
        if parsed.get('aggregates') or parsed.get('group_by'):
             all_rows = self._execute_aggregates(all_rows, parsed)
        else:
             # Apply DISTINCT
             if parsed.get('distinct'):
                 all_rows = self._apply_distinct(all_rows, parsed['columns'])
             
             # Apply column selection
             if parsed['columns'] != ['*']:
                 all_rows = [{col: row.get(col) for col in parsed['columns']} for row in all_rows]
        
        # Apply ORDER BY
        if parsed.get('order_by'):
             all_rows = self._apply_order_by(all_rows, parsed['order_by'])
        
        # Handle UNION (Before Limit? Usually Union is results, then Limit applied on top if parsed at top level)
        # But here 'union' is next SELECT.
        # If Limit applies to first select only:
        if parsed.get('offset'):
             offset = parsed['offset']
             all_rows = all_rows[offset:]
        if parsed.get('limit'):
             limit = parsed['limit']
             all_rows = all_rows[:limit]

        if parsed.get('union'):
             union_info = parsed['union']
             next_rows = self.execute_select(union_info['next'])
             
             if union_info['all']:
                 all_rows.extend(next_rows)
             else:
                 # Union Distinct
                 seen = set()
                 unique_rows = []
                 # We need to hash rows. Using tuple of sorted items.
                 # Assuming JSON-serializable types roughly.
                 for row in all_rows + next_rows:
                     # Handle non-hashable values (like lists/dicts)? 
                     # For basic types it works.
                     try:
                         key = tuple(sorted([(k, v) for k, v in row.items() if k is not None]))
                         if key not in seen:
                             seen.add(key)
                             unique_rows.append(row)
                     except:
                         # Fallback if unhashable
                         unique_rows.append(row)
                 all_rows = unique_rows
        
        return all_rows

    def execute_update(self, parsed: Dict[str, Any]) -> int:
        table_name = parsed['table']
        table = self.schema.get_table(table_name)
        
        if not table:
             raise TableNotFoundError(table_name)
        
        updates = parsed['updates']
        where_clause = parsed.get('where')
        
        all_rows = self._scan_table(table_name)
        
        updated_rows = []
        count = 0
        
        # Check transaction
        if self.transaction_manager.has_active_transaction():
            # Log for rollback
            pass

        for row in all_rows:
             should_update = True
             if where_clause:
                 should_update = self._matches_where_advanced(row, where_clause)
             
             if should_update:
                 for col, value in updates.items():
                     if col in row:
                         row[col] = value
                 count += 1
             updated_rows.append(row)
        
        self._rewrite_table(table_name, updated_rows, table)
        return count

    def execute_delete(self, parsed: Dict[str, Any]) -> int:
        table_name = parsed['table']
        table = self.schema.get_table(table_name)
        
        if not table:
             raise TableNotFoundError(table_name)
        
        where_clause = parsed.get('where')
        all_rows = self._scan_table(table_name)
        
        if where_clause:
             remaining_rows = [row for row in all_rows if not self._matches_where_advanced(row, where_clause)]
             deleted_count = len(all_rows) - len(remaining_rows)
        else:
             deleted_count = len(all_rows)
             remaining_rows = []
        
        self._rewrite_table(table_name, remaining_rows, table)
        return deleted_count

    def execute_pragma(self, parsed: Dict[str, Any]) -> Any:
        name = parsed['name']
        args = parsed['args']
        
        if name == 'table_exists':
             return 1 if self.table_exists(args[0]) else 0
        elif name == 'table_info':
             if not self.table_exists(args[0]):
                 return []
             return SystemTables.get_table_columns(self, args[0])
        elif name == 'database_list':
             return [{'file': 'main', 'name': 'main'}]
        else:
             return None

    def execute_begin(self):
        self.transaction_manager.begin_transaction()
    
    def execute_commit(self):
        if self.transaction_manager.has_active_transaction():
             self.transaction_manager.get_active_transaction().commit()
             self.transaction_manager.active_transaction = None
    
    def execute_rollback(self, parsed):
        savepoint = parsed.get('savepoint')
        if not self.transaction_manager.has_active_transaction():
             return
        
        txn = self.transaction_manager.get_active_transaction()
        
        ops_to_undo = []
        if savepoint:
             if savepoint in txn.savepoints:
                 pos = txn.savepoints[savepoint]
                 ops_to_undo = txn.operations[pos:]
                 txn.rollback_to_savepoint(savepoint)
        else:
             ops_to_undo = txn.operations
             self.transaction_manager.rollback_transaction(txn)
             
        # Execute undo in reverse order
        for op in reversed(ops_to_undo):
            self._undo_operation(op)

    def _undo_operation(self, op: Dict[str, Any]):
        # Removed try/except to debug
        if op['type'] == 'DELETE':
            # Undo of INSERT is DELETE
            # We need to delete the specific row we inserted
            table_name = op['table']
            row = op['row']
            table = self.schema.get_table(table_name)
            
            # Construct WHERE clause to match this specific row
            # Ideally use PK
            if table.primary_key:
                pk_val = row.get(table.primary_key)
                self.execute_delete({
                    'table': table_name,
                    'where': {'operator': '=', 'left': {'type': 'identifier', 'value': table.primary_key}, 
                              'right': {'type': 'literal', 'value': pk_val}}
                })
            else:
                # Best effort: match all cols
                pass
        elif op['type'] == 'INSERT':
            # Undo of DELETE is INSERT
            self.insert(op['table'], op['row'])

    def execute_savepoint(self, parsed):
        name = parsed['name']
        if self.transaction_manager.has_active_transaction():
             self.transaction_manager.get_active_transaction().create_savepoint(name)
        else:
             # Implicit transaction start? For now require BEGIN
             pass

    def execute_release(self, parsed):
        # RELEASE SAVEPOINT is standard, but often ignored in simple engines
        pass

    def execute_describe(self, parsed: Dict[str, Any]) -> List[Dict]:
        table_name = parsed['table']
        if not self.table_exists(table_name):
             raise TableNotFoundError(table_name)
        
        columns = SystemTables.get_table_columns(self, table_name)
        # Format for output
        result = []
        for col in columns:
             result.append({
                 'Field': col['column_name'],
                 'Type': col['column_type'],
                 'Null': 'YES' if not col['not_null'] else 'NO',
                 'Key': 'PRI' if col['primary_key'] else '',
                 'Default': col['default_value'],
                 'Extra': 'auto_increment' if col['autoincrement'] else ''
             })
        return result

    def execute_show(self, parsed: Dict[str, Any]) -> List[str]:
        what = parsed['what']
        if what == 'TABLES':
             tables = SystemTables.get_all_tables(self)
             return [{'table_name': t['name']} for t in tables]
        elif what == 'INDEXES':
             indexes = SystemTables.get_all_indexes(self)
             if parsed.get('table'):
                 indexes = [i for i in indexes if i['table_name'] == parsed['table']]
             return indexes

    def execute_explain(self, parsed: Dict[str, Any]) -> List[Dict]:
        return [
             {'id': 1, 'select_type': 'SIMPLE', 'table': parsed['query'].get('table', 'UNKNOWN'), 'type': 'ALL', 'rows': '1000', 'Extra': 'Using where'}
        ]

    # ... Helper methods like _scan_table, _matches_where_advanced, etc. same as before ...
    # Rewriting them to ensure they use self.storage correctly and handle system tables
    
    def _scan_table(self, table_name: str) -> List[Dict[str, Any]]:
        # System tables are stored as normal tables now, so we just scan them.
        # No special handling needed here unless they were virtual.
        
        if table_name not in self.table_pages:
             # Just return empty list if not found in memory map, assuming lazy load failed or empty
             return []
        
        table = self.schema.get_table(table_name)
        if not table: return []
        
        rows = []
        for page_id in self.table_pages[table_name]:
            page = self.storage.get_page(page_id)
            if not page: continue
            
            offset = 0
            data_end = len(page.data) - page.get_free_space()
            while offset < data_end:
                try:
                    if offset + 4 > data_end: break
                    size_bytes = page.read_record(offset, 4)
                    if not size_bytes or size_bytes == b'\x00\x00\x00\x00': break
                    row_size = struct.unpack('<I', size_bytes)[0]
                    if offset + 4 + row_size > data_end: break
                    
                    serialized_row = page.read_record(offset + 4, row_size)
                    row_obj = pickle.loads(serialized_row)
                    row_data = row_obj['data']
                    row_hash = row_obj['hash']
                    
                    if self.hasher.verify_row(row_data, row_hash):
                        row_dict = {}
                        for i, col in enumerate(table.columns):
                            if i < len(row_data):
                                row_dict[col.name] = row_data[i]
                            else:
                                # Schema evolution: Column added after row insertion
                                row_dict[col.name] = col.default
                        rows.append(row_dict)
                    
                    offset += 4 + row_size
                except: break
        return rows
    
    def _insert_row_internal(self, table_name, row_dict, table):
        row_data = [row_dict.get(col.name) for col in table.columns]
        row_hash = self.hasher.hash_row(row_data)
        
        serialized_row = pickle.dumps({'data': row_data, 'hash': row_hash})
        row_size = len(serialized_row)
        row_with_size = struct.pack('<I', row_size) + serialized_row
        
        if table_name not in self.table_pages: self.table_pages[table_name] = []
        
        if not self.table_pages[table_name]:
            page = self.storage.allocate_page(Page.TYPE_TABLE)
            self.table_pages[table_name] = [page.header.page_id]
        else:
            page_id = self.table_pages[table_name][-1]
            page = self.storage.get_page(page_id)
        
        if page.get_free_space() < len(row_with_size):
            page = self.storage.allocate_page(Page.TYPE_TABLE)
            self.table_pages[table_name].append(page.header.page_id)
        
        offset = len(page.data) - page.get_free_space()
        page.write_record(offset, row_with_size)
        
        # PK Index update would go here
        
    def _rewrite_table(self, table_name, rows, table):
        # Clear existing pages
        if table_name in self.table_pages:
            for page_id in self.table_pages[table_name]:
                page = self.storage.get_page(page_id)
                if page: page.clear()
        self.table_pages[table_name] = []
        
        # Re-insert
        for row in rows:
            self._insert_row_internal(table_name, row, table)

    def _parse_data_type(self, type_str: str) -> DataType:
        type_upper = type_str.upper()
        if type_upper in ('INTEGER', 'INT'): return DataType.INTEGER
        elif type_upper in ('REAL', 'FLOAT', 'DOUBLE'): return DataType.REAL
        elif type_upper in ('TEXT', 'VARCHAR', 'STRING', 'JSON'): return DataType.TEXT
        elif type_upper == 'BLOB': return DataType.BLOB
        else: return DataType.TEXT

    def table_exists(self, table_name: str) -> bool:
        # System tables must exist in schema to be queried/inserted
        return self.schema.get_table(table_name) is not None

    # Implementations of selects for system tables helper
    def create_table(self, name, columns):
        # Translate simple column dicts to parser format
        cols_parsed = []
        for col in columns:
            cols_parsed.append({
                'name': col['name'],
                'type': col['type'],
                'primary_key': col.get('primary_key', False),
                'not_null': col.get('not_null', False),
                'unique': col.get('unique', False), # fixed from unique_constraint
                'default': col.get('default'),
                'autoincrement': col.get('autoincrement', False)
            })
            
        return self.execute_create({
            'table': name,
            'columns': cols_parsed,
            'if_not_exists': True
        })

    def insert(self, table_name, data):
        # Used by SystemTables to write back to system tables
        return self.execute_insert({
            'table': table_name,
            'values': data
        })
    
    def select(self, table_name, where=None):
        pars = {'table': table_name, 'columns': ['*']}
        if where:
            # Convert dict where to AST where
            if len(where) == 1:
                k, v = list(where.items())[0]
                pars['where'] = {'column': k, 'operator': '=', 'value': v}
        return self.execute_select(pars)

    def delete(self, table_name, where=None):
        pars = {'table': table_name}
        if where:
             if len(where) == 1:
                k, v = list(where.items())[0]
                pars['where'] = {'column': k, 'operator': '=', 'value': v}
        return self.execute_delete(pars)

    # ... Include other helper methods (_matches_where_advanced, _evaluate_condition, _execute_joins, etc.) ...
    # For brevity in this artifact, assume they are copied from previous implementation or imported
    # but since I am overwriting the file, I MUST include them.

    def _execute_joins(self, left_rows, joins, left_table):
        result = left_rows
        for join in joins:
            join_type = join['type']
            join_table = join['table']
            on_clause = join['on']
            right_rows = self._scan_table(join_table)
            
            if join_type == 'INNER':
                result = self._inner_join(result, right_rows, on_clause)
            elif join_type == 'LEFT':
                result = self._left_join(result, right_rows, on_clause)
            elif join_type == 'RIGHT':
                result = self._right_join(result, right_rows, on_clause)
        return result

    def _inner_join(self, left, right, on):
        result = []
        for left_row in left:
            for right_row in right:
                if left_row.get(on['left']) == right_row.get(on['right']):
                    result.append({**left_row, **right_row})
        return result

    def _left_join(self, left, right, on):
        result = []
        for left_row in left:
            matched = False
            for right_row in right:
                if left_row.get(on['left']) == right_row.get(on['right']):
                    result.append({**left_row, **right_row})
                    matched = True
            if not matched: result.append(left_row)
        return result

    def _right_join(self, left, right, on):
        return self._left_join(right, left, {'left': on['right'], 'right': on['left']})

    def _execute_aggregates(self, rows, parsed):
        aggregates = parsed.get('aggregates', [])
        group_by = parsed.get('group_by')
        having = parsed.get('having')
        
        if group_by:
            groups = {}
            for row in rows:
                key = tuple(row.get(col) for col in group_by)
                if key not in groups: groups[key] = []
                groups[key].append(row)
            
            result = []
            for key, group_rows in groups.items():
                agg_row = {}
                for i, col in enumerate(group_by): agg_row[col] = key[i]
                for agg in aggregates: agg_row[agg['alias']] = self._calculate_aggregate(group_rows, agg)
                if having:
                    if self._matches_where_advanced(agg_row, having): result.append(agg_row)
                else:
                    result.append(agg_row)
            return result
        else:
            result_row = {}
            for agg in aggregates: result_row[agg['alias']] = self._calculate_aggregate(rows, agg)
            return [result_row]

    def _calculate_aggregate(self, rows, agg):
        func = agg['function']
        col = agg['column']
        if func == 'COUNT':
            if col == '*': return len(rows)
            return sum(1 for row in rows if row.get(col) is not None)
        values = [row.get(col) for row in rows if row.get(col) is not None]
        if not values: return None
        if func == 'SUM': return sum(values)
        elif func == 'AVG': return sum(values) / len(values)
        elif func == 'MIN': return min(values)
        elif func == 'MAX': return max(values)
        return None

    def _apply_distinct(self, rows, columns):
        seen = set()
        result = []
        for row in rows:
            if columns == ['*']: key = tuple(sorted(row.items()))
            else: key = tuple(row.get(col) for col in columns)
            if key not in seen:
                seen.add(key)
                result.append(row)
        return result

    def _apply_order_by(self, rows, order_by):
        def compare_key(row):
            keys = []
            for order in order_by:
                val = row.get(order['column'])
                if val is None: val = '' if isinstance(val, str) else 0
                keys.append(val if order['direction'] == 'ASC' else self._negate_for_sort(val))
            return tuple(keys)
        return sorted(rows, key=compare_key)

    def _negate_for_sort(self, val):
        if isinstance(val, (int, float)): return -val
        elif isinstance(val, str): return ''.join(chr(255 - ord(c)) for c in val)
        return val

    def _filter_rows_advanced(self, rows, where_clause):
        return [row for row in rows if self._evaluate_condition(row, where_clause)]

    def _matches_where_advanced(self, row, where_clause):
        # Alias for _evaluate_condition
        return self._evaluate_condition(row, where_clause)

    def _evaluate_expression(self, row, expr):
        if not isinstance(expr, dict): return expr
        
        t = expr.get('type')
        if t == 'literal': return expr['value']
        elif t == 'identifier': return row.get(expr['value'])
        elif t == 'function': return None # Functions not supported in WHERE yet
        elif 'operator' in expr: return self._evaluate_condition(row, expr)
        return None

    def _evaluate_condition(self, row, condition):
        if not isinstance(condition, dict): return bool(condition)

        # Legacy format support
        if 'column' in condition:
            column = condition['column']
            operator = condition['operator']
            value = condition['value']
            row_value = row.get(column)
            
            if operator == '=': return row_value == value
            elif operator == '!=': return row_value != value
            elif operator == '<': return row_value is not None and value is not None and row_value < value
            elif operator == '>': return row_value is not None and value is not None and row_value > value
            elif operator == '<=': return row_value is not None and value is not None and row_value <= value
            elif operator == '>=': return row_value is not None and value is not None and row_value >= value
            elif operator == 'LIKE': return self._match_like_pattern(str(row_value) if row_value is not None else '', value)
            elif operator == 'IN': return row_value in value
            elif operator == 'BETWEEN': return value[0] <= row_value <= value[1]
            return False

        # Modern AST format
        if 'operator' in condition:
            op = condition['operator'].upper()
            
            # Handle logical operators
            if op == 'AND':
                return self._evaluate_condition(row, condition['left']) and self._evaluate_condition(row, condition['right'])
            elif op == 'OR':
                return self._evaluate_condition(row, condition['left']) or self._evaluate_condition(row, condition['right'])
            elif op == 'NOT':
                return not self._evaluate_condition(row, condition['right']) # Unary NOT usually on right? Or expression?
            
            # Evaluate constraints
            left_val = self._evaluate_expression(row, condition.get('left'))
            right_val = self._evaluate_expression(row, condition.get('right'))
            
            if op == '=': return left_val == right_val
            elif op == '!=': return left_val != right_val
            elif op == 'IS': 
                 if right_val is None: return left_val is None
                 return left_val is right_val
            elif op == 'IS NOT':
                 if right_val is None: return left_val is not None
                 return left_val is not right_val
            elif op == '<': 
                if left_val is None or right_val is None: return False
                return left_val < right_val
            elif op == '>': 
                if left_val is None or right_val is None: return False
                return left_val > right_val
            elif op == '<=': 
                if left_val is None or right_val is None: return False
                return left_val <= right_val
            elif op == '>=': 
                if left_val is None or right_val is None: return False
                return left_val >= right_val
            elif op == 'LIKE':
                return self._match_like_pattern(str(left_val) if left_val is not None else '', right_val)
            elif op == 'IN':
                # Right side might be a list or tuple from parser if literal list
                if isinstance(right_val, (list, tuple)):
                    return left_val in right_val
                return False # Subquery IN not handled here
        
        return False

    def _match_like_pattern(self, text, pattern):
        regex_pattern = pattern.replace('%', '.*').replace('_', '.')
        return bool(re.match(f'^{regex_pattern}$', text, re.IGNORECASE))

    def _get_source_rows(self, table_name: str) -> List[Dict[str, Any]]:
        if SystemTables.is_system_table(table_name):
            return self._scan_table(table_name)

        # Check View
        view = SystemTables.get_view(self, table_name)
        if view:
             import pickle
             # Try definition blob
             if view.get('definition'):
                  try:
                      def_dict = pickle.loads(view['definition'])
                      return self.execute_select(def_dict)
                  except Exception as e:
                      # Log the actual error for debugging
                      raise TableNotFoundError(f"View {table_name} definition error: {e}")
             
             raise TableNotFoundError(f"View {table_name} has no definition blob")
             
        # Check Table
        if not self.schema.table_exists(table_name):
             if SystemTables.is_system_table(table_name):
                  # Fallback for system tables if not in schema (should be)
                  # System tables are usually managed by StorageEngine direct selects
                  # But schema manager should have them?
                  # If we return direct scan:
                  return self.storage.select(table_name)

             raise TableNotFoundError(table_name)
             
        return self._scan_table(table_name)

    def execute_create_view(self, parsed: Dict[str, Any]) -> str:
        view_name = parsed['view_name']
        if_not_exists = parsed['if_not_exists']
        
        if self.schema.table_exists(view_name) or SystemTables.get_view(self, view_name):
            if if_not_exists:
                return f"View {view_name} already exists."
            raise TableAlreadyExistsError(f"Table or View {view_name} already exists")
        
        # parsed['definition'] is the select dict
        SystemTables.register_view(self, view_name, parsed.get('sql', ''), parsed['definition'])
        return f"View {view_name} created."

    def execute_alter(self, parsed: Dict[str, Any]) -> str:
        table_name = parsed['table']
        operation = parsed['operation']
        
        if not self.schema.table_exists(table_name):
            raise TableNotFoundError(f"Table {table_name} not found")
        
        if operation['type'] == 'RENAME':
            new_name = operation['new_name']
            if self.schema.table_exists(new_name):
                raise TableAlreadyExistsError(f"Table {new_name} already exists")
            
            table = self.schema.get_table(table_name)
            self.schema.drop_table(table_name)
            table.name = new_name
            self.schema.create_table(table)
            
            if table_name in self.table_pages:
                self.table_pages[new_name] = self.table_pages.pop(table_name)
            
            SystemTables.unregister_table(self, table_name)
            cols_dicts = [c.to_dict() for c in table.columns]
            for c in cols_dicts:
                c['type'] = str(c['data_type'])
            
            SystemTables.register_table(self, new_name, cols_dicts)
            
            # self._save_metadata()
            return f"Table renamed to {new_name}"
            
        elif operation['type'] == 'ADD_COLUMN':
            col_def = operation['column']
            new_col = Column(
                col_def['name'],
                self._parse_data_type(col_def['type']),
                primary_key=col_def.get('primary_key', False),
                not_null=col_def.get('not_null', False),
                unique=col_def.get('unique', False),
                default=col_def.get('default'),
                autoincrement=col_def.get('autoincrement', False),
                check=col_def.get('check'),
                foreign_key=col_def.get('references')
            )
            
            table = self.schema.get_table(table_name)
            if table.get_column(new_col.name):
                raise ColumnNotFoundError(f"Column {new_col.name} already exists")
            
            table.columns.append(new_col)
            table.column_map[new_col.name] = new_col
            
            position = len(table.columns) - 1
            
            self.insert('__ribbit_columns', {
                'table_name': table_name,
                'column_name': new_col.name,
                'column_type': str(new_col.data_type),
                'not_null': 1 if new_col.not_null else 0,
                'default_value': str(new_col.default) if new_col.default is not None else None,
                'primary_key': 0,
                'autoincrement': 0,
                'unique_constraint': 1 if new_col.unique else 0,
                'position': position,
                'check_expression': new_col.check,
                'foreign_key': json.dumps(new_col.foreign_key) if new_col.foreign_key else None
            })
            
            # self._save_metadata()
            return f"Column {new_col.name} added to {table_name}"
            
        return "Unknown ALTER operation"
