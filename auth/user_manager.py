"""
User Management System for RibbitXDB
"""

import hashlib
import secrets
import time
from typing import Optional, List, Dict, Any
import ribbitxdb

class User:
    """Database user"""
    def __init__(self, user_id: int, username: str, password_hash: bytes, 
                 salt: bytes, created_at: int, is_superuser: bool = False):
        self.user_id = user_id
        self.username = username
        self.password_hash = password_hash
        self.salt = salt
        self.created_at = created_at
        self.is_superuser = is_superuser

class UserManager:
    """Manage database users"""
    
    def __init__(self, database_path: str):
        self.database_path = database_path
        self._init_system_tables()
        self._create_default_admin()
    
    def _init_system_tables(self):
        """Initialize system tables for user management"""
        conn = ribbitxdb.connect(self.database_path)
        cursor = conn.cursor()
        
        # Users table
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS _users (
                user_id INTEGER PRIMARY KEY,
                username TEXT UNIQUE NOT NULL,
                password_hash BLOB NOT NULL,
                salt BLOB NOT NULL,
                created_at INTEGER NOT NULL,
                is_superuser INTEGER DEFAULT 0
            )
        """)
        
        # Roles table
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS _roles (
                role_id INTEGER PRIMARY KEY,
                role_name TEXT UNIQUE NOT NULL,
                description TEXT
            )
        """)
        
        # User roles mapping
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS _user_roles (
                user_id INTEGER,
                role_id INTEGER,
                PRIMARY KEY (user_id, role_id)
            )
        """)
        
        # Permissions table
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS _permissions (
                permission_id INTEGER PRIMARY KEY,
                user_id INTEGER,
                database_name TEXT,
                table_name TEXT,
                permission_type TEXT NOT NULL,
                granted_at INTEGER NOT NULL
            )
        """)
        
        conn.commit()
        conn.close()
    
    def _create_default_admin(self):
        """Create default admin user if not exists"""
        if not self.user_exists('admin'):
            self.create_user('admin', 'admin123', is_superuser=True)
    
    def hash_password(self, password: str, salt: Optional[bytes] = None) -> tuple:
        """Hash password with BLAKE2 and salt"""
        if salt is None:
            salt = secrets.token_bytes(32)
        
        # Use BLAKE2b for password hashing
        hasher = hashlib.blake2b(salt=salt, digest_size=32)
        hasher.update(password.encode('utf-8'))
        password_hash = hasher.digest()
        
        return password_hash, salt
    
    def create_user(self, username: str, password: str, 
                    is_superuser: bool = False) -> int:
        """Create new user"""
        if self.user_exists(username):
            raise ValueError(f"User '{username}' already exists")
        
        password_hash, salt = self.hash_password(password)
        created_at = int(time.time())
        
        conn = ribbitxdb.connect(self.database_path)
        cursor = conn.cursor()
        
        # Get next user ID
        cursor.execute("SELECT MAX(user_id) FROM _users")
        result = cursor.fetchone()
        user_id = (result[0] or 0) + 1 if result else 1
        
        cursor.execute("""
            INSERT INTO _users (user_id, username, password_hash, salt, created_at, is_superuser)
            VALUES (?, ?, ?, ?, ?, ?)
        """, (user_id, username, password_hash, salt, created_at, 1 if is_superuser else 0))
        
        conn.commit()
        conn.close()
        
        return user_id
    
    def drop_user(self, username: str) -> bool:
        """Drop user"""
        if username == 'admin':
            raise ValueError("Cannot drop admin user")
        
        conn = ribbitxdb.connect(self.database_path)
        cursor = conn.cursor()
        
        cursor.execute("DELETE FROM _users WHERE username = ?", (username,))
        deleted = cursor.rowcount > 0
        
        conn.commit()
        conn.close()
        
        return deleted
    
    def user_exists(self, username: str) -> bool:
        """Check if user exists"""
        conn = ribbitxdb.connect(self.database_path)
        cursor = conn.cursor()
        
        cursor.execute("SELECT 1 FROM _users WHERE username = ?", (username,))
        exists = cursor.fetchone() is not None
        
        conn.close()
        return exists
    
    def get_user(self, username: str) -> Optional[User]:
        """Get user by username"""
        conn = ribbitxdb.connect(self.database_path)
        cursor = conn.cursor()
        
        cursor.execute("""
            SELECT user_id, username, password_hash, salt, created_at, is_superuser
            FROM _users WHERE username = ?
        """, (username,))
        
        row = cursor.fetchone()
        conn.close()
        
        if row:
            return User(
                user_id=row['user_id'],
                username=row['username'],
                password_hash=row['password_hash'],
                salt=row['salt'],
                created_at=row['created_at'],
                is_superuser=bool(row['is_superuser'])
            )
        return None
    
    def list_users(self) -> List[Dict[str, Any]]:
        """List all users"""
        conn = ribbitxdb.connect(self.database_path)
        cursor = conn.cursor()
        
        cursor.execute("""
            SELECT user_id, username, created_at, is_superuser
            FROM _users
            ORDER BY username
        """)
        
        users = cursor.fetchall()
        conn.close()
        
        return users
    
    def change_password(self, username: str, new_password: str) -> bool:
        """Change user password"""
        user = self.get_user(username)
        if not user:
            return False
        
        password_hash, salt = self.hash_password(new_password)
        
        conn = ribbitxdb.connect(self.database_path)
        cursor = conn.cursor()
        
        cursor.execute("""
            UPDATE _users 
            SET password_hash = ?, salt = ?
            WHERE username = ?
        """, (password_hash, salt, username))
        
        conn.commit()
        conn.close()
        
        return True
    
    def grant_permission(self, username: str, database_name: str, 
                        table_name: str, permission_type: str):
        """Grant permission to user"""
        user = self.get_user(username)
        if not user:
            raise ValueError(f"User '{username}' does not exist")
        
        conn = ribbitxdb.connect(self.database_path)
        cursor = conn.cursor()
        
        cursor.execute("""
            INSERT INTO _permissions (user_id, database_name, table_name, permission_type, granted_at)
            VALUES (?, ?, ?, ?, ?)
        """, (user.user_id, database_name, table_name, permission_type, int(time.time())))
        
        conn.commit()
        conn.close()
    
    def revoke_permission(self, username: str, database_name: str,
                         table_name: str, permission_type: str):
        """Revoke permission from user"""
        user = self.get_user(username)
        if not user:
            raise ValueError(f"User '{username}' does not exist")
        
        conn = ribbitxdb.connect(self.database_path)
        cursor = conn.cursor()
        
        cursor.execute("""
            DELETE FROM _permissions
            WHERE user_id = ? AND database_name = ? AND table_name = ? AND permission_type = ?
        """, (user.user_id, database_name, table_name, permission_type))
        
        conn.commit()
        conn.close()
    
    def check_permission(self, username: str, database_name: str,
                        table_name: str, permission_type: str) -> bool:
        """Check if user has permission"""
        user = self.get_user(username)
        if not user:
            return False
        
        # Superuser has all permissions
        if user.is_superuser:
            return True
        
        conn = ribbitxdb.connect(self.database_path)
        cursor = conn.cursor()
        
        # Check exact permission
        cursor.execute("""
            SELECT 1 FROM _permissions
            WHERE user_id = ? AND database_name = ? AND table_name = ? AND permission_type = ?
        """, (user.user_id, database_name, table_name, permission_type))
        
        has_perm = cursor.fetchone() is not None
        
        # Check wildcard permissions
        if not has_perm:
            cursor.execute("""
                SELECT 1 FROM _permissions
                WHERE user_id = ? AND database_name = ? AND table_name = '*' AND permission_type = ?
            """, (user.user_id, database_name, permission_type))
            has_perm = cursor.fetchone() is not None
        
        conn.close()
        return has_perm
