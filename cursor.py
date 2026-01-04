from typing import Any, List, Optional, Tuple
from .utils.exceptions import ProgrammingError

class Cursor:
    def __init__(self, connection):
        self.connection = connection
        self.arraysize = 1
        self.rowcount = -1
        self.description = None
        self._results: List[Any] = []
        self._result_index = 0
        self.lastrowid = None
    
    def execute(self, sql: str, parameters: tuple = None) -> 'Cursor':
        if self.connection.is_closed:
            raise ProgrammingError("Cannot execute on closed connection")
        
        if parameters:
            sql = self._bind_parameters(sql, parameters)
        
        try:
            result = self.connection.executor.execute(sql)
            
            if isinstance(result, list):
                self._results = result
                self.rowcount = len(result)
                
                if result and isinstance(result[0], dict):
                    self.description = [(col, None, None, None, None, None, None) 
                                       for col in result[0].keys()]
            elif isinstance(result, bool):
                self.rowcount = 1 if result else 0
                self._results = []
            elif isinstance(result, int):
                self.rowcount = result
                self._results = []
            else:
                self.rowcount = -1
                self._results = []
            
            self._result_index = 0
            
        except Exception as e:
            raise ProgrammingError(f"Error executing SQL: {e}")
        
        return self
    
    def executemany(self, sql: str, seq_of_parameters) -> 'Cursor':
        if self.connection.is_closed:
            raise ProgrammingError("Cannot execute on closed connection")
        
        total_rows = 0
        for parameters in seq_of_parameters:
            self.execute(sql, parameters)
            if self.rowcount > 0:
                total_rows += self.rowcount
        
        self.rowcount = total_rows
        return self
    
    def fetchone(self) -> Optional[Tuple]:
        if self._result_index >= len(self._results):
            return None
        
        result = self._results[self._result_index]
        self._result_index += 1
        
        if isinstance(result, dict):
            return tuple(result.values())
        return result
    
    def fetchmany(self, size: int = None) -> List[Tuple]:
        if size is None:
            size = self.arraysize
        
        results = []
        for _ in range(size):
            row = self.fetchone()
            if row is None:
                break
            results.append(row)
        
        return results
    
    def fetchall(self) -> List[Tuple]:
        results = []
        while True:
            row = self.fetchone()
            if row is None:
                break
            results.append(row)
        
        return results
    
    def close(self):
        self._results = []
        self._result_index = 0
        self.description = None
        self.rowcount = -1
    
    def _bind_parameters(self, sql: str, parameters: tuple) -> str:
        if not parameters:
            return sql
        
        result = sql
        for i, param in enumerate(parameters):
            placeholder = '?'
            if isinstance(param, str):
                value = f"'{param}'"
            elif param is None:
                value = 'NULL'
            else:
                value = str(param)
            
            result = result.replace(placeholder, value, 1)
        
        return result
    
    def __iter__(self):
        return self
    
    def __next__(self):
        row = self.fetchone()
        if row is None:
            raise StopIteration
        return row
