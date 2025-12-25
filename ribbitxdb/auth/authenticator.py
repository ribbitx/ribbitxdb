"""
Authentication System for RibbitXDB
"""

import hashlib
from typing import Optional
from .user_manager import UserManager

class Authenticator:
    """Handle user authentication"""
    
    def __init__(self, user_manager: UserManager):
        self.user_manager = user_manager
    
    def verify_user(self, username: str, password_hash: str, challenge: bytes) -> bool:
        """Verify user credentials with challenge-response"""
        user = self.user_manager.get_user(username)
        if not user:
            return False
        
        # Recreate expected hash
        # Client should send: BLAKE2(password_hash + challenge)
        expected_hash = hashlib.blake2b(user.password_hash + challenge, digest_size=32).hexdigest()
        
        return password_hash == expected_hash
    
    def authenticate(self, username: str, password: str) -> bool:
        """Simple password authentication (for direct connections)"""
        user = self.user_manager.get_user(username)
        if not user:
            return False
        
        # Hash provided password with user's salt
        password_hash, _ = self.user_manager.hash_password(password, user.salt)
        
        return password_hash == user.password_hash
