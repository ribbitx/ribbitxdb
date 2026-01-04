import os
import re
from typing import List, Dict, Optional
from datetime import datetime
from ..schema.system_tables import SystemTables
from ..utils.exceptions import MigrationError

class Migration:
    def __init__(self, name: str, up_sql: str, down_sql: str = None):
        self.name = name
        self.up_sql = up_sql
        self.down_sql = down_sql
        self.applied_at = None
    
    def __repr__(self):
        return f"Migration({self.name}, applied={self.applied_at is not None})"

class MigrationManager:
    def __init__(self, connection):
        self.connection = connection
        self.migrations_dir = None
        self._ensure_migrations_table()
    
    def _ensure_migrations_table(self):
        try:
            cursor = self.connection.cursor()
            cursor.execute("SELECT COUNT(*) FROM __ribbit_migrations")
        except:
            pass
    
    def set_migrations_directory(self, directory: str):
        if not os.path.exists(directory):
            raise MigrationError(f"Migrations directory does not exist: {directory}")
        self.migrations_dir = directory
    
    def create_migration(self, name: str, up_sql: str, down_sql: str = None) -> str:
        if not self.migrations_dir:
            raise MigrationError("Migrations directory not set. Call set_migrations_directory() first.")
        
        timestamp = datetime.now().strftime('%Y%m%d%H%M%S')
        filename = f"{timestamp}_{name}.sql"
        filepath = os.path.join(self.migrations_dir, filename)
        
        content = f"-- Migration: {name}\n"
        content += f"-- Created: {datetime.now().isoformat()}\n\n"
        content += "-- UP\n"
        content += up_sql.strip() + "\n\n"
        
        if down_sql:
            content += "-- DOWN\n"
            content += down_sql.strip() + "\n"
        
        with open(filepath, 'w') as f:
            f.write(content)
        
        return filepath
    
    def load_migrations(self) -> List[Migration]:
        if not self.migrations_dir:
            raise MigrationError("Migrations directory not set")
        
        migrations = []
        
        for filename in sorted(os.listdir(self.migrations_dir)):
            if not filename.endswith('.sql'):
                continue
            
            filepath = os.path.join(self.migrations_dir, filename)
            migration = self._parse_migration_file(filepath, filename)
            if migration:
                migrations.append(migration)
        
        return migrations
    
    def _parse_migration_file(self, filepath: str, filename: str) -> Optional[Migration]:
        with open(filepath, 'r') as f:
            content = f.read()
        
        name = filename.replace('.sql', '')
        
        up_match = re.search(r'-- UP\s*\n(.*?)(?:-- DOWN|$)', content, re.DOTALL)
        down_match = re.search(r'-- DOWN\s*\n(.*?)$', content, re.DOTALL)
        
        up_sql = up_match.group(1).strip() if up_match else None
        down_sql = down_match.group(1).strip() if down_match else None
        
        if not up_sql:
            return None
        
        return Migration(name, up_sql, down_sql)
    
    def get_pending_migrations(self) -> List[Migration]:
        all_migrations = self.load_migrations()
        applied = self.get_applied_migrations()
        applied_names = {m['name'] for m in applied}
        
        return [m for m in all_migrations if m.name not in applied_names]
    
    def get_applied_migrations(self) -> List[Dict]:
        cursor = self.connection.cursor()
        cursor.execute("SELECT * FROM __ribbit_migrations ORDER BY applied_at")
        rows = cursor.fetchall()
        # id=0, name=1, applied_at=2
        return [{'name': row[1], 'applied_at': row[2]} for row in rows]
    
    def up(self, migration_name: Optional[str] = None) -> List[str]:
        applied = []
        
        if migration_name:
            migration = self._find_migration(migration_name)
            if not migration:
                raise MigrationError(f"Migration not found: {migration_name}")
            
            if self._is_applied(migration.name):
                raise MigrationError(f"Migration already applied: {migration_name}")
            
            self._apply_migration(migration)
            applied.append(migration.name)
        else:
            pending = self.get_pending_migrations()
            for migration in pending:
                self._apply_migration(migration)
                applied.append(migration.name)
        
        return applied
    
    def down(self, migration_name: Optional[str] = None, steps: int = 1) -> List[str]:
        rolled_back = []
        
        if migration_name:
            migration = self._find_migration(migration_name)
            if not migration:
                raise MigrationError(f"Migration not found: {migration_name}")
            
            if not self._is_applied(migration.name):
                raise MigrationError(f"Migration not applied: {migration_name}")
            
            self._rollback_migration(migration)
            rolled_back.append(migration.name)
        else:
            applied = self.get_applied_migrations()
            applied.reverse()
            
            for i, applied_migration in enumerate(applied):
                if i >= steps:
                    break
                
                migration = self._find_migration(applied_migration['name'])
                if migration:
                    self._rollback_migration(migration)
                    rolled_back.append(migration.name)
        
        return rolled_back
    
    def status(self) -> Dict[str, List[str]]:
        all_migrations = self.load_migrations()
        applied = self.get_applied_migrations()
        applied_names = {m['name'] for m in applied}
        
        return {
            'applied': [m.name for m in all_migrations if m.name in applied_names],
            'pending': [m.name for m in all_migrations if m.name not in applied_names]
        }
    
    def _find_migration(self, name: str) -> Optional[Migration]:
        migrations = self.load_migrations()
        for migration in migrations:
            if migration.name == name:
                return migration
        return None
    
    def _is_applied(self, name: str) -> bool:
        cursor = self.connection.cursor()
        cursor.execute("SELECT COUNT(*) as count FROM __ribbit_migrations WHERE name = ?", (name,))
        result = cursor.fetchone()
        return result[0] > 0
    
    def _apply_migration(self, migration: Migration):
        cursor = self.connection.cursor()
        
        try:
            for statement in migration.up_sql.split(';'):
                statement = statement.strip()
                if statement:
                    cursor.execute(statement)
            
            now = datetime.now().isoformat()
            cursor.execute(
                "INSERT INTO __ribbit_migrations (name, applied_at) VALUES (?, ?)",
                (migration.name, now)
            )
            
            self.connection.commit()
        except Exception as e:
            self.connection.rollback()
            raise MigrationError(f"Failed to apply migration {migration.name}: {e}")
    
    def _rollback_migration(self, migration: Migration):
        if not migration.down_sql:
            raise MigrationError(f"Migration {migration.name} has no down migration")
        
        cursor = self.connection.cursor()
        
        try:
            for statement in migration.down_sql.split(';'):
                statement = statement.strip()
                if statement:
                    cursor.execute(statement)
            
            cursor.execute("DELETE FROM __ribbit_migrations WHERE name = ?", (migration.name,))
            
            self.connection.commit()
        except Exception as e:
            self.connection.rollback()
            raise MigrationError(f"Failed to rollback migration {migration.name}: {e}")
