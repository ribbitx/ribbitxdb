"""
RibbitXDB TCP Server with TLS Support
"""

import socket
import ssl
import threading
import logging
from typing import Optional, Callable, Dict, Any
from .protocol import ProtocolHandler, Message, MessageType, ProtocolError
from .session import SessionManager
from ..connection import Connection
from ..cursor import Cursor

logger = logging.getLogger(__name__)

class ClientHandler(threading.Thread):
    """Handle individual client connection"""
    
    def __init__(self, client_socket: socket.socket, address: tuple, 
                 server: 'RibbitXDBServer'):
        super().__init__(daemon=True)
        self.socket = client_socket
        self.address = address
        self.server = server
        self.protocol = ProtocolHandler()
        self.session_id = None
        self.authenticated = False
        self.running = True
        
    def run(self):
        """Main client handling loop"""
        logger.info(f"Client connected from {self.address}")
        
        try:
            while self.running:
                # Receive data
                data = self.socket.recv(4096)
                if not data:
                    break
                
                # Feed to protocol handler
                self.protocol.feed(data)
                
                # Process messages
                while True:
                    msg = self.protocol.get_message()
                    if not msg:
                        break
                    
                    response = self.handle_message(msg)
                    if response:
                        self.socket.sendall(self.protocol.send_message(response))
        
        except Exception as e:
            logger.error(f"Client handler error: {e}")
        finally:
            self.cleanup()
    
    def handle_message(self, msg: Message) -> Optional[Message]:
        """Handle incoming message"""
        try:
            if msg.msg_type == MessageType.CONNECT:
                return self.handle_connect(msg)
            
            elif msg.msg_type == MessageType.AUTH_RESPONSE:
                return self.handle_auth(msg)
            
            elif msg.msg_type == MessageType.QUERY:
                if not self.authenticated:
                    return Message.create_json(MessageType.QUERY_ERROR, {
                        'error': 'Not authenticated'
                    })
                return self.handle_query(msg)
            
            elif msg.msg_type == MessageType.BEGIN:
                return self.handle_begin(msg)
            
            elif msg.msg_type == MessageType.COMMIT:
                return self.handle_commit(msg)
            
            elif msg.msg_type == MessageType.ROLLBACK:
                return self.handle_rollback(msg)
            
            elif msg.msg_type == MessageType.PING:
                return Message(MessageType.PONG)
            
            elif msg.msg_type == MessageType.DISCONNECT:
                self.running = False
                return None
            
            else:
                logger.warning(f"Unknown message type: {msg.msg_type}")
                return None
        
        except Exception as e:
            logger.error(f"Message handling error: {e}")
            return Message.create_json(MessageType.QUERY_ERROR, {
                'error': str(e)
            })
    
    def handle_connect(self, msg: Message) -> Message:
        """Handle connection request"""
        data = msg.get_json()
        protocol_version = data.get('protocol_version', 1)
        
        if protocol_version != 1:
            return Message.create_json(MessageType.AUTH_FAILURE, {
                'error': 'Unsupported protocol version'
            })
        
        # Generate challenge for authentication
        challenge = self.server.session_manager.generate_challenge()
        self.session_id = self.server.session_manager.create_session(challenge)
        
        return Message.create_json(MessageType.AUTH_CHALLENGE, {
            'challenge': challenge.hex(),
            'session_id': self.session_id
        })
    
    def handle_auth(self, msg: Message) -> Message:
        """Handle authentication"""
        data = msg.get_json()
        username = data.get('username')
        password_hash = data.get('password_hash')
        
        # Verify credentials
        if self.server.auth_manager.verify_user(username, password_hash, 
                                                 self.server.session_manager.get_challenge(self.session_id)):
            self.authenticated = True
            token = self.server.session_manager.create_token(self.session_id, username)
            
            return Message.create_json(MessageType.AUTH_SUCCESS, {
                'token': token,
                'username': username
            })
        else:
            return Message.create_json(MessageType.AUTH_FAILURE, {
                'error': 'Invalid credentials'
            })
    
    def handle_query(self, msg: Message) -> Message:
        """Handle SQL query"""
        data = msg.get_json()
        sql = data.get('sql')
        params = data.get('params', [])
        
        try:
            # Get connection for this session
            conn = self.server.get_connection(self.session_id)
            cursor = conn.cursor()
            
            # Execute query
            cursor.execute(sql, params)
            
            # Get results
            if sql.strip().upper().startswith('SELECT'):
                rows = cursor.fetchall()
                return Message.create_json(MessageType.QUERY_RESULT, {
                    'rows': rows,
                    'rowcount': len(rows)
                })
            else:
                conn.commit()
                return Message.create_json(MessageType.QUERY_RESULT, {
                    'rowcount': cursor.rowcount
                })
        
        except Exception as e:
            return Message.create_json(MessageType.QUERY_ERROR, {
                'error': str(e)
            })
    
    def handle_begin(self, msg: Message) -> Message:
        """Handle transaction begin"""
        # Transactions are implicit in RibbitXDB
        return Message.create_json(MessageType.QUERY_RESULT, {'status': 'ok'})
    
    def handle_commit(self, msg: Message) -> Message:
        """Handle transaction commit"""
        try:
            conn = self.server.get_connection(self.session_id)
            conn.commit()
            return Message.create_json(MessageType.QUERY_RESULT, {'status': 'ok'})
        except Exception as e:
            return Message.create_json(MessageType.QUERY_ERROR, {'error': str(e)})
    
    def handle_rollback(self, msg: Message) -> Message:
        """Handle transaction rollback"""
        try:
            conn = self.server.get_connection(self.session_id)
            conn.rollback()
            return Message.create_json(MessageType.QUERY_RESULT, {'status': 'ok'})
        except Exception as e:
            return Message.create_json(MessageType.QUERY_ERROR, {'error': str(e)})
    
    def cleanup(self):
        """Cleanup client resources"""
        logger.info(f"Client disconnected from {self.address}")
        if self.session_id:
            self.server.session_manager.destroy_session(self.session_id)
        try:
            self.socket.close()
        except:
            pass

class RibbitXDBServer:
    """RibbitXDB TCP Server"""
    
    def __init__(self, host: str = '0.0.0.0', port: int = 5432,
                 database_path: str = 'server.rbx',
                 tls_cert: Optional[str] = None,
                 tls_key: Optional[str] = None,
                 tls_ca: Optional[str] = None,
                 require_client_cert: bool = False):
        self.host = host
        self.port = port
        self.database_path = database_path
        self.tls_cert = tls_cert
        self.tls_key = tls_key
        self.tls_ca = tls_ca
        self.require_client_cert = require_client_cert
        
        self.server_socket = None
        self.running = False
        self.clients = []
        
        # Import here to avoid circular dependency
        from ..auth.user_manager import UserManager
        from ..auth.authenticator import Authenticator
        
        self.session_manager = SessionManager()
        self.user_manager = UserManager(database_path)
        self.auth_manager = Authenticator(self.user_manager)
        
        # Connection pool
        self.connections: Dict[str, Connection] = {}
    
    def start(self):
        """Start the server"""
        self.server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.server_socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        
        # Wrap with TLS if configured
        if self.tls_cert and self.tls_key:
            context = ssl.SSLContext(ssl.PROTOCOL_TLS_SERVER)
            context.load_cert_chain(self.tls_cert, self.tls_key)
            
            if self.tls_ca:
                context.load_verify_locations(self.tls_ca)
                if self.require_client_cert:
                    context.verify_mode = ssl.CERT_REQUIRED
                else:
                    context.verify_mode = ssl.CERT_OPTIONAL
            
            self.server_socket = context.wrap_socket(self.server_socket, server_side=True)
            logger.info("TLS enabled")
        
        self.server_socket.bind((self.host, self.port))
        self.server_socket.listen(100)
        self.running = True
        
        logger.info(f"RibbitXDB Server listening on {self.host}:{self.port}")
        
        # Accept connections
        while self.running:
            try:
                client_socket, address = self.server_socket.accept()
                handler = ClientHandler(client_socket, address, self)
                handler.start()
                self.clients.append(handler)
            except Exception as e:
                if self.running:
                    logger.error(f"Accept error: {e}")
    
    def stop(self):
        """Stop the server"""
        self.running = False
        if self.server_socket:
            self.server_socket.close()
        
        # Close all client connections
        for client in self.clients:
            client.running = False
    
    def get_connection(self, session_id: str) -> Connection:
        """Get or create connection for session"""
        if session_id not in self.connections:
            import ribbitxdb
            self.connections[session_id] = ribbitxdb.connect(self.database_path)
        return self.connections[session_id]

def start_server(host='0.0.0.0', port=5432, **kwargs):
    """Start RibbitXDB server"""
    server = RibbitXDBServer(host, port, **kwargs)
    try:
        server.start()
    except KeyboardInterrupt:
        logger.info("Server shutting down...")
        server.stop()
