import os
import shutil
import time
import json
from typing import Optional, Dict, Any

class DatabaseBackup:
    
    def __init__(self, database_path: str):
        self.database_path = database_path
    
    def create_backup(self, backup_path: Optional[str] = None, 
                     compress: bool = True, encrypt: bool = False,
                     encryption_key: Optional[bytes] = None) -> str:
        if backup_path is None:
            timestamp = time.strftime('%Y%m%d_%H%M%S')
            backup_path = f"{self.database_path}.backup_{timestamp}"
        
        if not os.path.exists(self.database_path):
            raise FileNotFoundError(f"Database file not found: {self.database_path}")
        
        shutil.copy2(self.database_path, backup_path)
        
        if compress:
            backup_path = self._compress_backup(backup_path)
        
        if encrypt and encryption_key:
            backup_path = self._encrypt_backup(backup_path, encryption_key)
        
        metadata = {
            'original_path': self.database_path,
            'backup_time': time.time(),
            'compressed': compress,
            'encrypted': encrypt,
            'size': os.path.getsize(backup_path)
        }
        
        metadata_path = backup_path + '.meta'
        with open(metadata_path, 'w') as f:
            json.dump(metadata, f, indent=2)
        
        return backup_path
    
    def _compress_backup(self, backup_path: str) -> str:
        import lzma
        
        compressed_path = backup_path + '.xz'
        
        with open(backup_path, 'rb') as f_in:
            with lzma.open(compressed_path, 'wb', preset=9) as f_out:
                shutil.copyfileobj(f_in, f_out)
        
        os.remove(backup_path)
        return compressed_path
    
    def _encrypt_backup(self, backup_path: str, encryption_key: bytes) -> str:
        from ..security.encryption import encrypt_file
        
        encrypted_path = backup_path + '.enc'
        encrypt_file(backup_path, encrypted_path, encryption_key)
        
        os.remove(backup_path)
        return encrypted_path
    
    def list_backups(self, backup_dir: Optional[str] = None) -> list:
        if backup_dir is None:
            backup_dir = os.path.dirname(self.database_path)
        
        backups = []
        base_name = os.path.basename(self.database_path)
        
        for filename in os.listdir(backup_dir):
            if filename.startswith(f"{base_name}.backup_"):
                backup_path = os.path.join(backup_dir, filename)
                metadata_path = backup_path + '.meta'
                
                if os.path.exists(metadata_path):
                    with open(metadata_path, 'r') as f:
                        metadata = json.load(f)
                        metadata['path'] = backup_path
                        backups.append(metadata)
        
        return sorted(backups, key=lambda x: x['backup_time'], reverse=True)
    
    def delete_old_backups(self, keep_count: int = 5, backup_dir: Optional[str] = None):
        backups = self.list_backups(backup_dir)
        
        if len(backups) > keep_count:
            for backup in backups[keep_count:]:
                backup_path = backup['path']
                metadata_path = backup_path + '.meta'
                
                if os.path.exists(backup_path):
                    os.remove(backup_path)
                if os.path.exists(metadata_path):
                    os.remove(metadata_path)
