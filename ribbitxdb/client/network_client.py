"""
Network Client for RibbitXDB
"""

import socket
import ssl
import hashlib
from typing import Optional, List, Dict, Any
from ..server.protocol import Message, MessageType, ProtocolHandler

class NetworkConnection:
    """Network connection to RibbitXDB server"""
    
    def __init__(self, host: str, port: int, user: str, password: str,
                 tls_cert: Optional[str] = None,
                 tls_key: Optional[str] = None,
                 tls_ca: Optional[str] = None,
                 tls_verify: bool = True):
        self.host = host
        self.port = port
        self.user = user
        self.password = password
        self.tls_cert = tls_cert
        self.tls_key = tls_key
        self.tls_ca = tls_ca
        self.tls_verify = tls_verify
        
        self.socket = None
        self.protocol = ProtocolHandler()
        self.session_id = None
        self.token = None
        self.connected = False
    
    def connect(self):
        """Connect to server"""
        # Create socket
        self.socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        
        # Wrap with TLS if configured
        if self.tls_cert or self.tls_ca:
            context = ssl.SSLContext(ssl.PROTOCOL_TLS_CLIENT)
            
            if self.tls_ca:
                context.load_verify_locations(self.tls_ca)
                if self.tls_verify:
                    context.check_hostname = True
                    context.verify_mode = ssl.CERT_REQUIRED
                else:
                    context.check_hostname = False
                    context.verify_mode = ssl.CERT_NONE
            
            if self.tls_cert and self.tls_key:
                context.load_cert_chain(self.tls_cert, self.tls_key)
            
            self.socket = context.wrap_socket(self.socket, server_hostname=self.host)
        
        # Connect
        self.socket.connect((self.host, self.port))
        
        # Send CONNECT message
        connect_msg = Message.create_json(MessageType.CONNECT, {
            'protocol_version': 1
        })
        self._send(connect_msg)
        
        # Receive challenge
        challenge_msg = self._receive()
        if challenge_msg.msg_type != MessageType.AUTH_CHALLENGE:
            raise ConnectionError("Expected AUTH_CHALLENGE")
        
        challenge_data = challenge_msg.get_json()
        challenge = bytes.fromhex(challenge_data['challenge'])
        self.session_id = challenge_data['session_id']
        
        # Compute response
        # First hash password
        password_hash = hashlib.blake2b(self.password.encode(), digest_size=32).digest()
        # Then hash with challenge
        response_hash = hashlib.blake2b(password_hash + challenge, digest_size=32).hexdigest()
        
        # Send auth response
        auth_msg = Message.create_json(MessageType.AUTH_RESPONSE, {
            'username': self.user,
            'password_hash': response_hash
        })
        self._send(auth_msg)
        
        # Receive auth result
        auth_result = self._receive()
        if auth_result.msg_type == MessageType.AUTH_SUCCESS:
            result_data = auth_result.get_json()
            self.token = result_data['token']
            self.connected = True
        else:
            error_data = auth_result.get_json()
            raise ConnectionError(f"Authentication failed: {error_data.get('error')}")
    
    def execute(self, sql: str, params: tuple = ()) -> List[Dict[str, Any]]:
        """Execute SQL query"""
        if not self.connected:
            raise ConnectionError("Not connected")
        
        query_msg = Message.create_json(MessageType.QUERY, {
            'sql': sql,
            'params': list(params),
            'token': self.token
        })
        self._send(query_msg)
        
        result_msg = self._receive()
        
        if result_msg.msg_type == MessageType.QUERY_RESULT:
            result_data = result_msg.get_json()
            return result_data.get('rows', [])
        elif result_msg.msg_type == MessageType.QUERY_ERROR:
            error_data = result_msg.get_json()
            raise Exception(f"Query error: {error_data.get('error')}")
        else:
            raise Exception(f"Unexpected response: {result_msg.msg_type}")
    
    def commit(self):
        """Commit transaction"""
        if not self.connected:
            raise ConnectionError("Not connected")
        
        commit_msg = Message(MessageType.COMMIT)
        self._send(commit_msg)
        
        result_msg = self._receive()
        if result_msg.msg_type == MessageType.QUERY_ERROR:
            error_data = result_msg.get_json()
            raise Exception(f"Commit error: {error_data.get('error')}")
    
    def rollback(self):
        """Rollback transaction"""
        if not self.connected:
            raise ConnectionError("Not connected")
        
        rollback_msg = Message(MessageType.ROLLBACK)
        self._send(rollback_msg)
        
        result_msg = self._receive()
        if result_msg.msg_type == MessageType.QUERY_ERROR:
            error_data = result_msg.get_json()
            raise Exception(f"Rollback error: {error_data.get('error')}")
    
    def close(self):
        """Close connection"""
        if self.connected:
            disconnect_msg = Message(MessageType.DISCONNECT)
            self._send(disconnect_msg)
            self.connected = False
        
        if self.socket:
            self.socket.close()
    
    def _send(self, msg: Message):
        """Send message"""
        data = self.protocol.send_message(msg)
        self.socket.sendall(data)
    
    def _receive(self) -> Message:
        """Receive message"""
        while True:
            msg = self.protocol.get_message()
            if msg:
                return msg
            
            data = self.socket.recv(4096)
            if not data:
                raise ConnectionError("Connection closed")
            
            self.protocol.feed(data)
    
    def __enter__(self):
        self.connect()
        return self
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()

class NetworkCursor:
    """Cursor for network connection"""
    
    def __init__(self, connection: NetworkConnection):
        self.connection = connection
        self.results = []
        self.rowcount = 0
    
    def execute(self, sql: str, params: tuple = ()):
        """Execute SQL"""
        self.results = self.connection.execute(sql, params)
        self.rowcount = len(self.results)
    
    def fetchone(self) -> Optional[Dict[str, Any]]:
        """Fetch one row"""
        if self.results:
            return self.results.pop(0)
        return None
    
    def fetchall(self) -> List[Dict[str, Any]]:
        """Fetch all rows"""
        results = self.results
        self.results = []
        return results
    
    def fetchmany(self, size: int = 1) -> List[Dict[str, Any]]:
        """Fetch many rows"""
        results = self.results[:size]
        self.results = self.results[size:]
        return results
