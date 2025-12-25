"""
Replication Module
"""

from .wal import WriteAheadLog, WALEntry

__all__ = ['WriteAheadLog', 'WALEntry']
