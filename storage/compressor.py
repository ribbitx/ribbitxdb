import lzma
from ..utils.constants import DEFAULT_COMPRESSION_LEVEL

class LZMACompressor:
    def __init__(self, level=DEFAULT_COMPRESSION_LEVEL):
        self.level = level
        self.preset = level | lzma.PRESET_EXTREME if level >= 6 else level
    
    def compress(self, data: bytes) -> bytes:
        if not data:
            return b''
        return lzma.compress(data, preset=self.preset)
    
    def decompress(self, data: bytes) -> bytes:
        if not data:
            return b''
        return lzma.decompress(data)
    
    def compress_stream(self, data: bytes, chunk_size: int = 8192) -> bytes:
        if not data:
            return b''
        
        compressor = lzma.LZMACompressor(preset=self.preset)
        result = b''
        
        for i in range(0, len(data), chunk_size):
            chunk = data[i:i + chunk_size]
            result += compressor.compress(chunk)
        
        result += compressor.flush()
        return result
    
    def decompress_stream(self, data: bytes, chunk_size: int = 8192) -> bytes:
        if not data:
            return b''
        
        decompressor = lzma.LZMADecompressor()
        result = b''
        
        for i in range(0, len(data), chunk_size):
            chunk = data[i:i + chunk_size]
            result += decompressor.decompress(chunk)
        
        return result
