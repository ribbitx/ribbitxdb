"""
Session Management for RibbitXDB Server
"""

import secrets
import time
import hashlib
from typing import Dict, Optional

class Session:
    """Client session"""
    def __init__(self, session_id: str, challenge: bytes):
        self.session_id = session_id
        self.challenge = challenge
        self.username = None
        self.token = None
        self.created_at = time.time()
        self.last_activity = time.time()
        self.authenticated = False
    
    def update_activity(self):
        """Update last activity timestamp"""
        self.last_activity = time.time()
    
    def is_expired(self, timeout: int = 3600) -> bool:
        """Check if session is expired"""
        return (time.time() - self.last_activity) > timeout

class SessionManager:
    """Manage client sessions"""
    
    def __init__(self, session_timeout: int = 3600):
        self.sessions: Dict[str, Session] = {}
        self.session_timeout = session_timeout
    
    def generate_challenge(self) -> bytes:
        """Generate random challenge for authentication"""
        return secrets.token_bytes(32)
    
    def create_session(self, challenge: bytes) -> str:
        """Create new session"""
        session_id = secrets.token_hex(16)
        self.sessions[session_id] = Session(session_id, challenge)
        return session_id
    
    def get_session(self, session_id: str) -> Optional[Session]:
        """Get session by ID"""
        session = self.sessions.get(session_id)
        if session and not session.is_expired(self.session_timeout):
            session.update_activity()
            return session
        elif session:
            # Clean up expired session
            del self.sessions[session_id]
        return None
    
    def get_challenge(self, session_id: str) -> Optional[bytes]:
        """Get challenge for session"""
        session = self.get_session(session_id)
        return session.challenge if session else None
    
    def create_token(self, session_id: str, username: str) -> str:
        """Create authentication token"""
        session = self.get_session(session_id)
        if not session:
            raise ValueError("Invalid session")
        
        # Generate token
        token_data = f"{session_id}:{username}:{time.time()}".encode()
        token = hashlib.sha256(token_data).hexdigest()
        
        session.token = token
        session.username = username
        session.authenticated = True
        
        return token
    
    def verify_token(self, token: str) -> Optional[str]:
        """Verify token and return username"""
        for session in self.sessions.values():
            if session.token == token and not session.is_expired(self.session_timeout):
                session.update_activity()
                return session.username
        return None
    
    def destroy_session(self, session_id: str):
        """Destroy session"""
        if session_id in self.sessions:
            del self.sessions[session_id]
    
    def cleanup_expired(self):
        """Remove expired sessions"""
        expired = [
            sid for sid, session in self.sessions.items()
            if session.is_expired(self.session_timeout)
        ]
        for sid in expired:
            del self.sessions[sid]
    
    def get_active_count(self) -> int:
        """Get count of active sessions"""
        self.cleanup_expired()
        return len(self.sessions)
