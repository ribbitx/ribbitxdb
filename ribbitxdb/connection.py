from typing import Optional, Any
from .storage.engine import StorageEngine
from .schema.metadata import SchemaManager
from .index.manager import IndexManager
from .security.hasher import BLAKE2Hasher
from .query.executor import QueryExecutor
from .transaction.manager import TransactionManager
from .cursor import Cursor
from .utils.exceptions import ProgrammingError, DatabaseError
import pickle
from .storage.page import Page

class Connection:
    def __init__(self, database: str, compression_level: int = 6):
        self.database = database
        self.storage = StorageEngine(database, compression_level)
        self.schema = SchemaManager()
        self.index_manager = IndexManager()
        self.hasher = BLAKE2Hasher()
        self.transaction_manager = TransactionManager()
        
        # Pass transaction_manager to executor
        self.executor = QueryExecutor(
            self.storage, 
            self.schema, 
            self.index_manager, 
            self.hasher,
            self.transaction_manager
        )
        
        self.is_closed = False
        
        # Load metadata (page map for system tables etc)
        self._load_metadata()
        
        # Retry loading schema now that we have the page map
        # This is CRITICAL because Executor.__init__ tried earlier but failed due to missing map
        if hasattr(self.executor, '_load_metadata'):
            self.executor._load_metadata()
    
    def _load_metadata(self):
        try:
            meta_page = self.storage.get_page(0)
            if meta_page:
                # Read size from first 4 bytes
                size_bytes = meta_page.read_record(0, 4)
                if size_bytes and size_bytes != b'\x00\x00\x00\x00':
                    import struct
                    size = struct.unpack('<I', size_bytes)[0]
                    # Read the full record (size + data) from offset 0
                    full_record = meta_page.read_record(0, 4 + size)
                    if full_record and len(full_record) >= 4 + size:
                        # Extract just the data part (skip the 4-byte size prefix)
                        data = full_record[4:4+size]
                        meta_dict = pickle.loads(data)
                        if 'table_pages' in meta_dict:
                            self.executor.table_pages = meta_dict['table_pages']
        except:
            pass
    
    def _save_metadata(self):
        if self.is_closed: return
        
        try:
            meta_page = self.storage.get_page(0)
            if not meta_page:
                 meta_page = self.storage.allocate_page(Page.TYPE_META)
            
            meta_page.clear()
            
            data = {
                'table_pages': self.executor.table_pages
            }
            serialized = pickle.dumps(data)
            
            import struct
            record = struct.pack('<I', len(serialized)) + serialized
            
            if len(record) <= meta_page.get_free_space():
                meta_page.write_record(0, record)
                meta_page.dirty = True # Force dirty just in case
        except:
            pass
    
    def table_exists(self, table_name: str) -> bool:
        if self.is_closed:
            raise ProgrammingError("Cannot check table on closed connection")
        return self.executor.table_exists(table_name)
    
    def cursor(self) -> Cursor:
        if self.is_closed:
            raise ProgrammingError("Cannot create cursor on closed connection")
        return Cursor(self)
    
    def commit(self):
        if self.is_closed:
            raise ProgrammingError("Cannot commit on closed connection")
        
        if self.transaction_manager.has_active_transaction():
            transaction = self.transaction_manager.get_active_transaction()
            self.transaction_manager.commit_transaction(transaction)
        
        self._save_metadata()
        self.storage.flush()
    
    def rollback(self):
        if self.is_closed:
            raise ProgrammingError("Cannot rollback on closed connection")
        
        # Rollback execution logic is handled by executor.execute_rollback usually,
        # but here we are at Connection level. 
        # If user calls conn.rollback(), we should tell executor to rollback?
        # Or just rollback transaction state?
        # If we just rollback state, data changes persist.
        # We need to call executor.execute_rollback()!
        
        # But wait, conn.rollback() usually triggers rollback of current txn.
        if self.transaction_manager.has_active_transaction():
            # We construct a fake parsed command to pass to executor
             self.executor.execute_rollback({}) # Empty dict implies standard rollback
    
    def close(self):
        if not self.is_closed:
            try:
                self.commit()
            except Exception:
                pass
            self.storage.close()
            self.is_closed = True
    
    def execute(self, sql: str, parameters: tuple = None) -> Cursor:
        cursor = self.cursor()
        cursor.execute(sql, parameters)
        return cursor
    
    def executemany(self, sql: str, seq_of_parameters) -> Cursor:
        cursor = self.cursor()
        cursor.executemany(sql, seq_of_parameters)
        return cursor
    
    def __enter__(self):
        return self
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        if exc_type is None:
            self.commit()
        else:
            self.rollback()
        self.close()
    
    def __del__(self):
        if hasattr(self, 'is_closed') and not self.is_closed:
            self.close()

def connect(database: str, compression_level: int = 6) -> Connection:
    return Connection(database, compression_level)
