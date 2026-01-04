import hashlib
from typing import Any, List
from ..utils.constants import HASH_ALGORITHM, HASH_SIZE

class BLAKE2Hasher:
    def __init__(self):
        self.algorithm = HASH_ALGORITHM
        self.digest_size = HASH_SIZE
    
    def hash_data(self, data: bytes) -> bytes:
        h = hashlib.blake2b(data, digest_size=self.digest_size)
        return h.digest()
    
    def hash_row(self, row_data: List[Any]) -> bytes:
        serialized = self._serialize_row(row_data)
        return self.hash_data(serialized)
    
    def verify_hash(self, data: bytes, expected_hash: bytes) -> bool:
        computed_hash = self.hash_data(data)
        return computed_hash == expected_hash
    
    def verify_row(self, row_data: List[Any], expected_hash: bytes) -> bool:
        computed_hash = self.hash_row(row_data)
        return computed_hash == expected_hash
    
    def _serialize_row(self, row_data: List[Any]) -> bytes:
        result = b''
        for item in row_data:
            if item is None:
                result += b'\x00'
            elif isinstance(item, int):
                result += b'\x01' + str(item).encode('utf-8')
            elif isinstance(item, float):
                result += b'\x02' + str(item).encode('utf-8')
            elif isinstance(item, str):
                result += b'\x03' + item.encode('utf-8')
            elif isinstance(item, bytes):
                result += b'\x04' + item
            else:
                result += b'\x05' + str(item).encode('utf-8')
        return result
    
    def hash_with_salt(self, data: bytes, salt: bytes) -> bytes:
        h = hashlib.blake2b(data, digest_size=self.digest_size, salt=salt[:16])
        return h.digest()
