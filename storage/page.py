import struct
from typing import Optional, List
from ..utils.constants import PAGE_SIZE, MAGIC_NUMBER

class PageHeader:
    HEADER_SIZE = 32
    
    def __init__(self, page_id: int, page_type: int, free_space: int, 
                 record_count: int, next_page: int = 0, prev_page: int = 0):
        self.page_id = page_id
        self.page_type = page_type
        self.free_space = free_space
        self.record_count = record_count
        self.next_page = next_page
        self.prev_page = prev_page
    
    def to_bytes(self) -> bytes:
        header_bytes = struct.pack(
            '<5sHIHHII',
            MAGIC_NUMBER,
            self.page_id,
            self.page_type,
            self.free_space,
            self.record_count,
            self.next_page,
            self.prev_page
        )
        # Pad to HEADER_SIZE (32 bytes) for alignment
        return header_bytes.ljust(self.HEADER_SIZE, b'\x00')
    
    @classmethod
    def from_bytes(cls, data: bytes) -> 'PageHeader':
        # Unpack only the actual struct size (23 bytes), not the padded HEADER_SIZE (32 bytes)
        STRUCT_SIZE = struct.calcsize('<5sHIHHII')  # 23 bytes
        magic, page_id, page_type, free_space, record_count, next_page, prev_page = struct.unpack(
            '<5sHIHHII', data[:STRUCT_SIZE]
        )
        if magic != MAGIC_NUMBER:
            raise ValueError("Invalid page magic number")
        return cls(page_id, page_type, free_space, record_count, next_page, prev_page)

class Page:
    TYPE_FREE = 0
    TYPE_TABLE = 1
    TYPE_INDEX = 2
    TYPE_OVERFLOW = 3
    TYPE_META = 4
    
    def __init__(self, page_id: int, page_type: int = TYPE_FREE):
        self.header = PageHeader(page_id, page_type, PAGE_SIZE - PageHeader.HEADER_SIZE, 0)
        self.data = bytearray(PAGE_SIZE - PageHeader.HEADER_SIZE)
        self.dirty = False
    
    def write_record(self, offset: int, data: bytes) -> bool:
        if offset + len(data) > len(self.data):
            return False
        
        self.data[offset:offset + len(data)] = data
        self.header.record_count += 1
        self.header.free_space -= len(data)
        self.dirty = True
        return True
    
    def read_record(self, offset: int, length: int) -> bytes:
        return bytes(self.data[offset:offset + length])
    
    def get_free_space(self) -> int:
        return self.header.free_space
    
    def to_bytes(self) -> bytes:
        return self.header.to_bytes() + bytes(self.data)
    
    @classmethod
    def from_bytes(cls, data: bytes) -> 'Page':
        header = PageHeader.from_bytes(data)
        page = cls(header.page_id, header.page_type)
        page.header = header
        page.data = bytearray(data[PageHeader.HEADER_SIZE:PAGE_SIZE])
        return page
    
    def clear(self):
        self.data = bytearray(PAGE_SIZE - PageHeader.HEADER_SIZE)
        self.header.free_space = PAGE_SIZE - PageHeader.HEADER_SIZE
        self.header.record_count = 0
        self.dirty = True
