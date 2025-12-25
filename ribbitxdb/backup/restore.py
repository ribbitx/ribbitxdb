import os
import shutil
import json
from typing import Optional

class DatabaseRestore:
    
    def __init__(self, database_path: str):
        self.database_path = database_path
    
    def restore_from_backup(self, backup_path: str, 
                           decryption_key: Optional[bytes] = None,
                           verify: bool = True) -> bool:
        if not os.path.exists(backup_path):
            raise FileNotFoundError(f"Backup file not found: {backup_path}")
        
        metadata_path = backup_path + '.meta'
        metadata = {}
        
        if os.path.exists(metadata_path):
            with open(metadata_path, 'r') as f:
                metadata = json.load(f)
        
        temp_path = backup_path
        
        if metadata.get('encrypted') and decryption_key:
            temp_path = self._decrypt_backup(backup_path, decryption_key)
        
        if metadata.get('compressed'):
            temp_path = self._decompress_backup(temp_path)
        
        if os.path.exists(self.database_path):
            backup_current = self.database_path + '.pre_restore'
            shutil.copy2(self.database_path, backup_current)
        
        shutil.copy2(temp_path, self.database_path)
        
        if temp_path != backup_path:
            os.remove(temp_path)
        
        if verify:
            return self._verify_restore()
        
        return True
    
    def _decrypt_backup(self, backup_path: str, decryption_key: bytes) -> str:
        from ..security.encryption import decrypt_file
        
        decrypted_path = backup_path.replace('.enc', '')
        decrypt_file(backup_path, decrypted_path, decryption_key)
        
        return decrypted_path
    
    def _decompress_backup(self, backup_path: str) -> str:
        import lzma
        
        decompressed_path = backup_path.replace('.xz', '')
        
        with lzma.open(backup_path, 'rb') as f_in:
            with open(decompressed_path, 'wb') as f_out:
                shutil.copyfileobj(f_in, f_out)
        
        return decompressed_path
    
    def _verify_restore(self) -> bool:
        try:
            import ribbitxdb
            conn = ribbitxdb.connect(self.database_path)
            cursor = conn.cursor()
            cursor.execute("SELECT COUNT(*) FROM sqlite_master WHERE type='table'")
            conn.close()
            return True
        except Exception:
            return False
