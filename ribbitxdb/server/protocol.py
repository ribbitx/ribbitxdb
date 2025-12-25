"""
RibbitXDB Binary Protocol

Message Format:
+--------+--------+------------+----------+
| Magic  | Type   | Length     | Payload  |
| 2 bytes| 1 byte | 4 bytes    | N bytes  |
+--------+--------+------------+----------+

Magic: 0x5242 ('RB' in ASCII)
Type: Message type identifier
Length: Payload length in bytes
Payload: Message-specific data
"""

import struct
import json
from typing import Dict, Any, Optional
from enum import IntEnum

class MessageType(IntEnum):
    """Protocol message types"""
    # Connection
    CONNECT = 0x01
    DISCONNECT = 0x02
    
    # Authentication
    AUTH_CHALLENGE = 0x10
    AUTH_RESPONSE = 0x11
    AUTH_SUCCESS = 0x12
    AUTH_FAILURE = 0x13
    
    # Query
    QUERY = 0x20
    QUERY_RESULT = 0x21
    QUERY_ERROR = 0x22
    
    # Transaction
    BEGIN = 0x30
    COMMIT = 0x31
    ROLLBACK = 0x32
    
    # Replication
    REPL_SYNC = 0x40
    REPL_DATA = 0x41
    
    # Heartbeat
    PING = 0x50
    PONG = 0x51

class ProtocolError(Exception):
    """Protocol-level error"""
    pass

class Message:
    """Protocol message"""
    MAGIC = 0x5242  # 'RB'
    HEADER_SIZE = 7  # 2 (magic) + 1 (type) + 4 (length)
    
    def __init__(self, msg_type: MessageType, payload: bytes = b''):
        self.msg_type = msg_type
        self.payload = payload
    
    def serialize(self) -> bytes:
        """Serialize message to bytes"""
        header = struct.pack(
            '>HBI',  # Big-endian: short, byte, int
            self.MAGIC,
            self.msg_type,
            len(self.payload)
        )
        return header + self.payload
    
    @classmethod
    def deserialize(cls, data: bytes) -> 'Message':
        """Deserialize message from bytes"""
        if len(data) < cls.HEADER_SIZE:
            raise ProtocolError("Incomplete message header")
        
        magic, msg_type, payload_len = struct.unpack('>HBI', data[:cls.HEADER_SIZE])
        
        if magic != cls.MAGIC:
            raise ProtocolError(f"Invalid magic number: {magic:04x}")
        
        if len(data) < cls.HEADER_SIZE + payload_len:
            raise ProtocolError("Incomplete message payload")
        
        payload = data[cls.HEADER_SIZE:cls.HEADER_SIZE + payload_len]
        
        return cls(MessageType(msg_type), payload)
    
    @classmethod
    def create_json(cls, msg_type: MessageType, data: Dict[str, Any]) -> 'Message':
        """Create message with JSON payload"""
        payload = json.dumps(data).encode('utf-8')
        return cls(msg_type, payload)
    
    def get_json(self) -> Dict[str, Any]:
        """Get JSON payload"""
        return json.loads(self.payload.decode('utf-8'))
    
    def __repr__(self):
        return f"Message(type={self.msg_type.name}, payload_len={len(self.payload)})"

class ProtocolHandler:
    """Handle protocol-level communication"""
    
    def __init__(self):
        self.buffer = b''
    
    def feed(self, data: bytes):
        """Feed data into buffer"""
        self.buffer += data
    
    def get_message(self) -> Optional[Message]:
        """Extract next complete message from buffer"""
        if len(self.buffer) < Message.HEADER_SIZE:
            return None
        
        # Peek at length
        _, _, payload_len = struct.unpack('>HBI', self.buffer[:Message.HEADER_SIZE])
        total_len = Message.HEADER_SIZE + payload_len
        
        if len(self.buffer) < total_len:
            return None
        
        # Extract message
        msg_data = self.buffer[:total_len]
        self.buffer = self.buffer[total_len:]
        
        return Message.deserialize(msg_data)
    
    def send_message(self, msg: Message) -> bytes:
        """Serialize message for sending"""
        return msg.serialize()
