"""
Write-Ahead Log (WAL) for Replication
"""

import os
import struct
import time
from typing import List, Dict, Any

class WALEntry:
    """WAL entry"""
    def __init__(self, lsn: int, timestamp: int, sql: str, params: tuple):
        self.lsn = lsn  # Log Sequence Number
        self.timestamp = timestamp
        self.sql = sql
        self.params = params
    
    def serialize(self) -> bytes:
        """Serialize to bytes"""
        import pickle
        data = pickle.dumps({
            'lsn': self.lsn,
            'timestamp': self.timestamp,
            'sql': self.sql,
            'params': self.params
        })
        return struct.pack('<I', len(data)) + data
    
    @classmethod
    def deserialize(cls, data: bytes) -> 'WALEntry':
        """Deserialize from bytes"""
        import pickle
        entry_data = pickle.loads(data)
        return cls(
            lsn=entry_data['lsn'],
            timestamp=entry_data['timestamp'],
            sql=entry_data['sql'],
            params=entry_data['params']
        )

class WriteAheadLog:
    """Write-Ahead Log for replication"""
    
    def __init__(self, wal_path: str):
        self.wal_path = wal_path
        self.current_lsn = 0
        self._load_lsn()
    
    def _load_lsn(self):
        """Load current LSN from WAL"""
        if os.path.exists(self.wal_path):
            with open(self.wal_path, 'rb') as f:
                while True:
                    size_bytes = f.read(4)
                    if not size_bytes:
                        break
                    size = struct.unpack('<I', size_bytes)[0]
                    entry_data = f.read(size)
                    if len(entry_data) < size:
                        break
                    entry = WALEntry.deserialize(entry_data)
                    self.current_lsn = max(self.current_lsn, entry.lsn)
    
    def append(self, sql: str, params: tuple = ()) -> int:
        """Append entry to WAL"""
        self.current_lsn += 1
        entry = WALEntry(
            lsn=self.current_lsn,
            timestamp=int(time.time()),
            sql=sql,
            params=params
        )
        
        with open(self.wal_path, 'ab') as f:
            f.write(entry.serialize())
        
        return self.current_lsn
    
    def read_from(self, lsn: int) -> List[WALEntry]:
        """Read entries from LSN"""
        entries = []
        
        if not os.path.exists(self.wal_path):
            return entries
        
        with open(self.wal_path, 'rb') as f:
            while True:
                size_bytes = f.read(4)
                if not size_bytes:
                    break
                
                size = struct.unpack('<I', size_bytes)[0]
                entry_data = f.read(size)
                
                if len(entry_data) < size:
                    break
                
                entry = WALEntry.deserialize(entry_data)
                if entry.lsn > lsn:
                    entries.append(entry)
        
        return entries
    
    def get_current_lsn(self) -> int:
        """Get current LSN"""
        return self.current_lsn
    
    def truncate(self, lsn: int):
        """Truncate WAL up to LSN"""
        entries = self.read_from(lsn)
        
        # Rewrite WAL with remaining entries
        temp_path = self.wal_path + '.tmp'
        with open(temp_path, 'wb') as f:
            for entry in entries:
                f.write(entry.serialize())
        
        os.replace(temp_path, self.wal_path)
