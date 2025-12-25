from enum import Enum
from typing import Any, Optional

class DataType(Enum):
    NULL = 0
    INTEGER = 1
    REAL = 2
    TEXT = 3
    BLOB = 4

class TypeConverter:
    @staticmethod
    def python_to_sql(value: Any) -> tuple[DataType, Any]:
        if value is None:
            return (DataType.NULL, None)
        elif isinstance(value, bool):
            return (DataType.INTEGER, int(value))
        elif isinstance(value, int):
            return (DataType.INTEGER, value)
        elif isinstance(value, float):
            return (DataType.REAL, value)
        elif isinstance(value, str):
            return (DataType.TEXT, value)
        elif isinstance(value, (bytes, bytearray)):
            return (DataType.BLOB, bytes(value))
        else:
            return (DataType.TEXT, str(value))
    
    @staticmethod
    def sql_to_python(data_type: DataType, value: Any) -> Any:
        if data_type == DataType.NULL:
            return None
        elif data_type == DataType.INTEGER:
            return int(value) if value is not None else None
        elif data_type == DataType.REAL:
            return float(value) if value is not None else None
        elif data_type == DataType.TEXT:
            return str(value) if value is not None else None
        elif data_type == DataType.BLOB:
            return bytes(value) if value is not None else None
        return value
    
    @staticmethod
    def infer_type(value: Any) -> DataType:
        type_info, _ = TypeConverter.python_to_sql(value)
        return type_info
    
    @staticmethod
    def validate_type(value: Any, expected_type: DataType) -> bool:
        if value is None:
            return True
        
        actual_type = TypeConverter.infer_type(value)
        return actual_type == expected_type or expected_type == DataType.NULL
