from typing import List, Dict, Any, Optional

class BatchOperations:
    
    def __init__(self, connection):
        self.connection = connection
        self.cursor = connection.cursor()
    
    def batch_insert(self, table: str, rows: List[Dict[str, Any]], 
                    chunk_size: int = 1000) -> int:
        if not rows:
            return 0
        
        columns = list(rows[0].keys())
        placeholders = ', '.join(['?' for _ in columns])
        column_names = ', '.join(columns)
        
        sql = f"INSERT INTO {table} ({column_names}) VALUES ({placeholders})"
        
        total_inserted = 0
        for i in range(0, len(rows), chunk_size):
            chunk = rows[i:i + chunk_size]
            
            for row in chunk:
                values = [row[col] for col in columns]
                self.cursor.execute(sql, tuple(values))
                total_inserted += 1
            
            self.connection.commit()
        
        return total_inserted
    
    def batch_update(self, table: str, updates: List[Dict[str, Any]], 
                    key_column: str = 'id', chunk_size: int = 1000) -> int:
        if not updates:
            return 0
        
        total_updated = 0
        for i in range(0, len(updates), chunk_size):
            chunk = updates[i:i + chunk_size]
            
            for update in chunk:
                key_value = update.pop(key_column)
                set_clauses = ', '.join([f"{col} = ?" for col in update.keys()])
                values = list(update.values()) + [key_value]
                
                sql = f"UPDATE {table} SET {set_clauses} WHERE {key_column} = ?"
                self.cursor.execute(sql, tuple(values))
                total_updated += self.cursor.rowcount
                
                update[key_column] = key_value
            
            self.connection.commit()
        
        return total_updated
    
    def batch_delete(self, table: str, conditions: List[Dict[str, Any]], 
                    chunk_size: int = 1000) -> int:
        if not conditions:
            return 0
        
        total_deleted = 0
        for i in range(0, len(conditions), chunk_size):
            chunk = conditions[i:i + chunk_size]
            
            for condition in chunk:
                where_clauses = ' AND '.join([f"{col} = ?" for col in condition.keys()])
                values = list(condition.values())
                
                sql = f"DELETE FROM {table} WHERE {where_clauses}"
                self.cursor.execute(sql, tuple(values))
                total_deleted += self.cursor.rowcount
            
            self.connection.commit()
        
        return total_deleted
    
    def bulk_upsert(self, table: str, rows: List[Dict[str, Any]], 
                   key_columns: List[str], chunk_size: int = 1000) -> Dict[str, int]:
        stats = {'inserted': 0, 'updated': 0}
        
        for i in range(0, len(rows), chunk_size):
            chunk = rows[i:i + chunk_size]
            
            for row in chunk:
                key_conditions = {col: row[col] for col in key_columns}
                where_clauses = ' AND '.join([f"{col} = ?" for col in key_columns])
                key_values = [row[col] for col in key_columns]
                
                check_sql = f"SELECT COUNT(*) FROM {table} WHERE {where_clauses}"
                self.cursor.execute(check_sql, tuple(key_values))
                exists = self.cursor.fetchone()[0] > 0
                
                if exists:
                    update_cols = [col for col in row.keys() if col not in key_columns]
                    if update_cols:
                        set_clauses = ', '.join([f"{col} = ?" for col in update_cols])
                        update_values = [row[col] for col in update_cols] + key_values
                        
                        sql = f"UPDATE {table} SET {set_clauses} WHERE {where_clauses}"
                        self.cursor.execute(sql, tuple(update_values))
                        stats['updated'] += 1
                else:
                    columns = list(row.keys())
                    placeholders = ', '.join(['?' for _ in columns])
                    column_names = ', '.join(columns)
                    values = [row[col] for col in columns]
                    
                    sql = f"INSERT INTO {table} ({column_names}) VALUES ({placeholders})"
                    self.cursor.execute(sql, tuple(values))
                    stats['inserted'] += 1
            
            self.connection.commit()
        
        return stats

