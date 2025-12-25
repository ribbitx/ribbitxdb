import os
import struct
from typing import Dict, Optional
from .page import Page, PageHeader
from .compressor import LZMACompressor
from ..utils.constants import PAGE_SIZE, CACHE_SIZE, VERSION
from ..utils.exceptions import OperationalError

class StorageEngine:
    def __init__(self, filename: str, compression_level: int = 6):
        self.filename = filename
        self.compressor = LZMACompressor(compression_level)
        self.page_cache: Dict[int, Page] = {}
        self.next_page_id = 0
        self.file_handle = None
        self.is_new = not os.path.exists(filename)
        
        self._open_database()
    
    def _open_database(self):
        mode = 'r+b' if not self.is_new else 'w+b'
        try:
            self.file_handle = open(self.filename, mode)
            if self.is_new:
                self._initialize_database()
            else:
                self._load_metadata()
        except IOError as e:
            raise OperationalError(f"Cannot open database: {e}")
    
    def _initialize_database(self):
        header = self._create_file_header()
        self.file_handle.write(header)
        self.file_handle.flush()
        
        meta_page = Page(0, Page.TYPE_META)
        self._write_page(meta_page)
        self.next_page_id = 1
    
    def _create_file_header(self) -> bytes:
        header = struct.pack(
            '<5sHIII',
            b'RBXDB',
            VERSION,
            PAGE_SIZE,
            0,
            0
        )
        return header + b'\x00' * (PAGE_SIZE - len(header))
    
    def _load_metadata(self):
        self.file_handle.seek(0)
        header = self.file_handle.read(PAGE_SIZE)
        
        magic, version, page_size, _, _ = struct.unpack('<5sHIII', header[:19])
        
        if magic != b'RBXDB':
            raise OperationalError("Invalid database file")
        
        if version != VERSION:
            raise OperationalError(f"Unsupported database version: {version}")
        
        file_size = os.path.getsize(self.filename)
        self.next_page_id = (file_size - PAGE_SIZE) // PAGE_SIZE
    
    def allocate_page(self, page_type: int = Page.TYPE_FREE) -> Page:
        page_id = self.next_page_id
        self.next_page_id += 1
        
        page = Page(page_id, page_type)
        self.page_cache[page_id] = page
        return page
    
    def get_page(self, page_id: int) -> Optional[Page]:
        if page_id in self.page_cache:
            return self.page_cache[page_id]
        
        page = self._read_page(page_id)
        if page and len(self.page_cache) < CACHE_SIZE:
            self.page_cache[page_id] = page
        
        return page
    
    def _read_page(self, page_id: int) -> Optional[Page]:
        offset = PAGE_SIZE + (page_id * PAGE_SIZE)
        
        try:
            self.file_handle.seek(offset)
            compressed_data = self.file_handle.read(PAGE_SIZE)
            
            if len(compressed_data) < PAGE_SIZE:
                return None
            
            # Strip trailing zeros (padding) before decompression
            # LZMA compressed data doesn't end with zeros, so we can safely strip them
            compressed_data = compressed_data.rstrip(b'\x00')
            
            # Try to decompress
            try:
                data = self.compressor.decompress(compressed_data)
            except:
                # If decompression fails, data was stored uncompressed
                # Re-read the full PAGE_SIZE since we stripped zeros
                self.file_handle.seek(offset)
                data = self.file_handle.read(PAGE_SIZE)
            
            return Page.from_bytes(data)
        except Exception:
            return None
    
    def _write_page(self, page: Page):
        offset = PAGE_SIZE + (page.header.page_id * PAGE_SIZE)
        
        page_data = page.to_bytes()
        compressed_data = self.compressor.compress(page_data)
        
        if len(compressed_data) >= PAGE_SIZE:
            compressed_data = page_data
        
        compressed_data = compressed_data.ljust(PAGE_SIZE, b'\x00')
        
        self.file_handle.seek(offset)
        self.file_handle.write(compressed_data)
        page.dirty = False
    
    def flush(self):
        for page in self.page_cache.values():
            if page.dirty:
                self._write_page(page)
        self.file_handle.flush()
    
    def close(self):
        self.flush()
        if self.file_handle:
            self.file_handle.close()
            self.file_handle = None
    
    def __del__(self):
        if self.file_handle:
            self.close()
