class RibbitXDBError(Exception):
    """Base class for all RibbitXDB exceptions."""
    pass

# DB-API 2.0 Standard Exceptions
class Warning(Exception):
    pass

class Error(RibbitXDBError):
    pass

class InterfaceError(Error):
    pass

class DatabaseError(Error):
    pass

class DataError(DatabaseError):
    pass

class OperationalError(DatabaseError):
    pass

class IntegrityError(DatabaseError):
    pass

class InternalError(DatabaseError):
    pass

class ProgrammingError(DatabaseError):
    pass

class NotSupportedError(DatabaseError):
    pass

# Custom Exceptions (Inheriting from Standard where appropriate)

class SQLSyntaxError(ProgrammingError):
    def __init__(self, message, line=None, column=None, hint=None, context=None, token=None):
        self.message = message
        self.line = line
        self.column = column
        self.hint = hint
        self.context = context
        self.token = token
        super().__init__(self._format_message())
    
    def _format_message(self):
        parts = [f"SQLSyntaxError: {self.message}"]
        if self.line is not None and self.column is not None:
            parts.append(f" at line {self.line}, column {self.column}")
        formatted = ''.join(parts)
        if self.context:
            formatted += f"\n\n  {self.context}"
            if self.column is not None:
                formatted += f"\n  {' ' * (self.column - 1)}^"
        if self.hint:
            formatted += f"\n\nHint: {self.hint}"
        return formatted

class UnsupportedFeatureError(NotSupportedError):
    def __init__(self, feature, hint=None):
        message = f"Feature '{feature}' is not supported yet"
        pass # NotSupportedError doesn't take hint usually, just message
        super().__init__(message)
        self.hint = hint

class TableNotFoundError(ProgrammingError):
    def __init__(self, table_name):
        self.table_name = table_name
        super().__init__(f"Table '{table_name}' does not exist")

class TableAlreadyExistsError(ProgrammingError):
    def __init__(self, table_name):
        self.table_name = table_name
        super().__init__(f"Table '{table_name}' already exists")

class ColumnNotFoundError(ProgrammingError):
    def __init__(self, column_name, table_name=None):
        self.column_name = column_name
        self.table_name = table_name
        if table_name:
            super().__init__(f"Column '{column_name}' not found in table '{table_name}'")
        else:
            super().__init__(f"Column '{column_name}' not found")

class TypeMismatchError(DataError):
    def __init__(self, expected_type, actual_type, column=None):
        self.expected_type = expected_type
        self.actual_type = actual_type
        self.column = column
        if column:
            super().__init__(f"Type mismatch for column '{column}': expected {expected_type}, got {actual_type}")
        else:
            super().__init__(f"Type mismatch: expected {expected_type}, got {actual_type}")

class ConstraintViolationError(IntegrityError):
    def __init__(self, constraint_type, details=None):
        self.constraint_type = constraint_type
        self.details = details
        message = f"{constraint_type} constraint violation"
        if details:
            message += f": {details}"
        super().__init__(message)

class TransactionError(OperationalError):
    pass

class IndexError(ProgrammingError):
    pass

class PermissionError(ProgrammingError):
    def __init__(self, operation, table=None):
        self.operation = operation
        self.table = table
        if table:
            super().__init__(f"Permission denied: {operation} on table '{table}'")
        else:
            super().__init__(f"Permission denied: {operation}")

class MigrationError(OperationalError):
    pass

class ValidationError(DataError):
    pass
