"""
Authorization and Permission System
"""

from typing import Optional
from .user_manager import UserManager

class Authorizer:
    """Handle authorization and permission checking"""
    
    def __init__(self, user_manager: UserManager):
        self.user_manager = user_manager
    
    def can_execute(self, username: str, sql: str, database_name: str = 'main') -> tuple:
        """Check if user can execute SQL statement
        
        Returns: (allowed: bool, reason: str)
        """
        user = self.user_manager.get_user(username)
        if not user:
            return False, "User does not exist"
        
        # Superuser can do anything
        if user.is_superuser:
            return True, "Superuser"
        
        # Parse SQL to determine required permission
        sql_upper = sql.strip().upper()
        
        if sql_upper.startswith('SELECT'):
            return self._check_select(username, sql, database_name)
        elif sql_upper.startswith('INSERT'):
            return self._check_insert(username, sql, database_name)
        elif sql_upper.startswith('UPDATE'):
            return self._check_update(username, sql, database_name)
        elif sql_upper.startswith('DELETE'):
            return self._check_delete(username, sql, database_name)
        elif sql_upper.startswith('CREATE'):
            return self._check_create(username, sql, database_name)
        elif sql_upper.startswith('DROP'):
            return self._check_drop(username, sql, database_name)
        else:
            return False, "Unknown SQL statement type"
    
    def _extract_table_name(self, sql: str) -> Optional[str]:
        """Extract table name from SQL (simple parser)"""
        tokens = sql.split()
        
        # Find FROM, INTO, UPDATE, or TABLE keyword
        for i, token in enumerate(tokens):
            if token.upper() in ('FROM', 'INTO', 'UPDATE', 'TABLE'):
                if i + 1 < len(tokens):
                    return tokens[i + 1].strip('();,')
        
        return None
    
    def _check_select(self, username: str, sql: str, database_name: str) -> tuple:
        """Check SELECT permission"""
        table_name = self._extract_table_name(sql) or '*'
        
        if self.user_manager.check_permission(username, database_name, table_name, 'SELECT'):
            return True, "Permission granted"
        
        return False, f"No SELECT permission on {database_name}.{table_name}"
    
    def _check_insert(self, username: str, sql: str, database_name: str) -> tuple:
        """Check INSERT permission"""
        table_name = self._extract_table_name(sql) or '*'
        
        if self.user_manager.check_permission(username, database_name, table_name, 'INSERT'):
            return True, "Permission granted"
        
        return False, f"No INSERT permission on {database_name}.{table_name}"
    
    def _check_update(self, username: str, sql: str, database_name: str) -> tuple:
        """Check UPDATE permission"""
        table_name = self._extract_table_name(sql) or '*'
        
        if self.user_manager.check_permission(username, database_name, table_name, 'UPDATE'):
            return True, "Permission granted"
        
        return False, f"No UPDATE permission on {database_name}.{table_name}"
    
    def _check_delete(self, username: str, sql: str, database_name: str) -> tuple:
        """Check DELETE permission"""
        table_name = self._extract_table_name(sql) or '*'
        
        if self.user_manager.check_permission(username, database_name, table_name, 'DELETE'):
            return True, "Permission granted"
        
        return False, f"No DELETE permission on {database_name}.{table_name}"
    
    def _check_create(self, username: str, sql: str, database_name: str) -> tuple:
        """Check CREATE permission"""
        table_name = self._extract_table_name(sql) or '*'
        
        if self.user_manager.check_permission(username, database_name, table_name, 'CREATE'):
            return True, "Permission granted"
        
        return False, f"No CREATE permission on {database_name}.{table_name}"
    
    def _check_drop(self, username: str, sql: str, database_name: str) -> tuple:
        """Check DROP permission"""
        table_name = self._extract_table_name(sql) or '*'
        
        if self.user_manager.check_permission(username, database_name, table_name, 'DROP'):
            return True, "Permission granted"
        
        return False, f"No DROP permission on {database_name}.{table_name}"
