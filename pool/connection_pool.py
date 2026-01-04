import threading
import time
import queue
from typing import Optional, Dict, Any
from ..connection import Connection

class ConnectionPool:
    
    def __init__(self, database: str, min_connections: int = 5, 
                 max_connections: int = 20, timeout: int = 30,
                 max_idle_time: int = 300):
        self.database = database
        self.min_connections = min_connections
        self.max_connections = max_connections
        self.timeout = timeout
        self.max_idle_time = max_idle_time
        
        self._pool = queue.Queue(maxsize=max_connections)
        self._active_connections = 0
        self._lock = threading.Lock()
        self._connection_times = {}
        
        self._initialize_pool()
        self._start_cleanup_thread()
    
    def _initialize_pool(self):
        for _ in range(self.min_connections):
            conn = self._create_connection()
            self._pool.put(conn)
    
    def _create_connection(self) -> Connection:
        import ribbitxdb
        conn = ribbitxdb.connect(self.database)
        conn_id = id(conn)
        self._connection_times[conn_id] = time.time()
        
        with self._lock:
            self._active_connections += 1
        
        return conn
    
    def get_connection(self, timeout: Optional[int] = None) -> Connection:
        timeout = timeout or self.timeout
        
        try:
            conn = self._pool.get(timeout=timeout)
            self._connection_times[id(conn)] = time.time()
            return conn
        except queue.Empty:
            with self._lock:
                if self._active_connections < self.max_connections:
                    return self._create_connection()
            
            raise TimeoutError(f"Could not acquire connection within {timeout} seconds")
    
    def release_connection(self, conn: Connection):
        if conn is None:
            return
        
        conn_id = id(conn)
        self._connection_times[conn_id] = time.time()
        
        try:
            self._pool.put_nowait(conn)
        except queue.Full:
            conn.close()
            with self._lock:
                self._active_connections -= 1
            if conn_id in self._connection_times:
                del self._connection_times[conn_id]
    
    def _start_cleanup_thread(self):
        def cleanup():
            while True:
                time.sleep(60)
                self._cleanup_idle_connections()
        
        thread = threading.Thread(target=cleanup, daemon=True)
        thread.start()
    
    def _cleanup_idle_connections(self):
        current_time = time.time()
        connections_to_close = []
        
        temp_conns = []
        while not self._pool.empty():
            try:
                conn = self._pool.get_nowait()
                conn_id = id(conn)
                
                if conn_id in self._connection_times:
                    idle_time = current_time - self._connection_times[conn_id]
                    
                    if idle_time > self.max_idle_time and self._active_connections > self.min_connections:
                        connections_to_close.append(conn)
                    else:
                        temp_conns.append(conn)
                else:
                    temp_conns.append(conn)
            except queue.Empty:
                break
        
        for conn in temp_conns:
            self._pool.put(conn)
        
        for conn in connections_to_close:
            conn.close()
            conn_id = id(conn)
            if conn_id in self._connection_times:
                del self._connection_times[conn_id]
            
            with self._lock:
                self._active_connections -= 1
    
    def close_all(self):
        while not self._pool.empty():
            try:
                conn = self._pool.get_nowait()
                conn.close()
            except queue.Empty:
                break
        
        self._connection_times.clear()
        with self._lock:
            self._active_connections = 0
    
    def get_stats(self) -> Dict[str, Any]:
        return {
            'active_connections': self._active_connections,
            'pool_size': self._pool.qsize(),
            'max_connections': self.max_connections,
            'min_connections': self.min_connections
        }
    
    def __enter__(self):
        return self
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close_all()

class PooledConnection:
    
    def __init__(self, pool: ConnectionPool):
        self.pool = pool
        self.conn = pool.get_connection()
    
    def __enter__(self):
        return self.conn
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        self.pool.release_connection(self.conn)
    
    def __getattr__(self, name):
        return getattr(self.conn, name)
