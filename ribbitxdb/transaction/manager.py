from typing import List, Dict, Any
from enum import Enum
import time

class TransactionState(Enum):
    ACTIVE = 1
    COMMITTED = 2
    ABORTED = 3

class Transaction:
    def __init__(self, transaction_id: int):
        self.transaction_id = transaction_id
        self.state = TransactionState.ACTIVE
        self.operations: List[Dict[str, Any]] = []
        self.start_time = time.time()
        self.savepoints: Dict[str, int] = {}
    
    def add_operation(self, operation: Dict[str, Any]):
        self.operations.append(operation)
    
    def commit(self):
        self.state = TransactionState.COMMITTED
    
    def abort(self):
        self.state = TransactionState.ABORTED
    
    def is_active(self) -> bool:
        return self.state == TransactionState.ACTIVE
    
    def create_savepoint(self, name: str):
        self.savepoints[name] = len(self.operations)
    
    def rollback_to_savepoint(self, name: str) -> bool:
        if name not in self.savepoints:
            return False
        
        position = self.savepoints[name]
        self.operations = self.operations[:position]
        return True

class TransactionManager:
    def __init__(self):
        self.transactions: Dict[int, Transaction] = {}
        self.next_transaction_id = 1
        self.active_transaction: Transaction = None
    
    def begin_transaction(self) -> Transaction:
        transaction_id = self.next_transaction_id
        self.next_transaction_id += 1
        
        transaction = Transaction(transaction_id)
        self.transactions[transaction_id] = transaction
        self.active_transaction = transaction
        
        return transaction
    
    def commit_transaction(self, transaction: Transaction) -> bool:
        if not transaction.is_active():
            return False
        
        transaction.commit()
        
        if self.active_transaction == transaction:
            self.active_transaction = None
        
        return True
    
    def rollback_transaction(self, transaction: Transaction) -> bool:
        if not transaction.is_active():
            return False
        
        transaction.abort()
        
        if self.active_transaction == transaction:
            self.active_transaction = None
        
        return True
    
    def get_active_transaction(self) -> Transaction:
        return self.active_transaction
    
    def has_active_transaction(self) -> bool:
        return self.active_transaction is not None
