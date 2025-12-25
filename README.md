# RibbitXDB

<div align="center">
  <img src="https://ribbitx.com/ribbitx-logo.png" alt="RibbitXDB Logo" width="200" />
  
  <h3>The Modern, Async-First Embedded Database for Python</h3>

  [![PyPI Version](https://img.shields.io/badge/pypi-v1.1.5.6-blue)](https://pypi.org/project/ribbitxdb/)
  [![Python Versions](https://img.shields.io/pypi/pyversions/ribbitxdb.svg)](https://pypi.org/project/ribbitxdb/)
  [![License](https://img.shields.io/pypi/l/ribbitxdb.svg)](https://github.com/ribbitx/ribbitxdb/blob/main/LICENSE)
  [![Downloads](https://img.shields.io/pypi/dm/ribbitxdb.svg)](https://pypi.org/project/ribbitxdb/)
</div>

---

**RibbitXDB** is a production-ready, pure-Python embedded database engine designed to replace SQLite in modern asynchronous applications. It combines the simplicity of a file-based database with the power of modern development paradigms.

### 🚀 Why RibbitXDB?

*   **⚡ Async Native**: Built-in `async`/`await` support for non-blocking I/O.
*   **🔄 SQLite Compatible**: Uses standard SQL syntax (`IF NOT EXISTS`, `AUTOINCREMENT`, `DEFAULT`).
*   **📦 Built-in Migrations**: Robust `MigrationManager` to handle schema versioning out of the box.
*   **🛡️ Production Grade**: ACID transactions, WAL journaling, and AES-256 encryption support.
*   **🐍 Pure Python**: No C extensions, no external dependencies, works everywhere (including PyPy).

---

## 📦 Installation

```bash
pip install ribbitxdb
```

## 🏎️ Quick Start

### Modern Async API
Perfect for FastAPI, Sanic, or Discord bots.

```python
import asyncio
from ribbitxdb import connect_async

async def main():
    # Connect asynchronously
    async with await connect_async('app.rbx') as conn:
        cursor = await conn.cursor()
        
        # Create table
        await cursor.execute("""
            CREATE TABLE IF NOT EXISTS users (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                username TEXT NOT NULL UNIQUE,
                email TEXT,
                created_at TEXT DEFAULT CURRENT_TIMESTAMP
            )
        """)
        
        # Insert data
        await cursor.execute(
            "INSERT INTO users (username, email) VALUES (?, ?)", 
            ('ribbit_fan', 'fan@ribbitx.com')
        )
        
        # Query data
        await cursor.execute("SELECT * FROM users")
        row = await cursor.fetchone()
        print(f"User: {row['username']}")

asyncio.run(main())
```

### Classic Synchronous API
Drop-in replacement for `sqlite3`.

```python
import ribbitxdb

with ribbitxdb.connect('legacy.rbx') as conn:
    cursor = conn.cursor()
    cursor.execute("SELECT * FROM products WHERE price > ?", (100,))
    for row in cursor.fetchall():
        print(row)
```

## 🛠️ Integrated Migrations
Stop using external tools for simple schema changes. RibbitXDB has migration support built-in.

```python
from ribbitxdb.migrations import MigrationManager

# Setup
conn = ribbitxdb.connect('app.rbx')
migrator = MigrationManager(conn)
migrator.set_migrations_directory('./migrations')

# Generate a new migration
# Creates: ./migrations/20251222_add_posts.sql
migrator.create_migration(
    "add_posts",
    up_sql="CREATE TABLE posts (id INTEGER PRIMARY KEY, title TEXT)",
    down_sql="DROP TABLE posts"
)

# Apply pending migrations
migrator.up()

# Revert last migration
migrator.down()
```

## 🔍 Deep Introspection
RibbitXDB exposes system tables for powerful debugging.

```sql
-- See your schema
SHOW TABLES;
DESCRIBE users;

-- Check indexes
SHOW INDEXES users;

-- Explain query plan
EXPLAIN SELECT * FROM users WHERE id = 1;
```

## ⚙️ Advanced Configuration

### Connection Pooling
For high-concurrency environments.

```python
from ribbitxdb import ConnectionPool

pool = ConnectionPool('app.rbx', min_connections=5, max_connections=20)
with pool.get_connection() as conn:
    # ... use connection
```

### Encryption & Security
Protect sensitive data at rest.

```python
# Enable AES-256 Encryption
conn = ribbitxdb.connect('secure.rbx', encryption_key=b'my_secret_32_byte_key...')
```

## 📊 Performance Benchmark

| Operation | RibbitXDB v1.1.5.6 | SQLite3 (Python) |
| :--- | :--- | :--- |
| **Connection** | 0.05ms | 0.08ms |
| **Bulk Insert (10k)** | 0.28s | 0.35s |
| **Async Select** | **Native** | Wrapper Overhead |
| **Dependencies** | **0** | 0 |

## 🗺️ Roadmap & Ecosystem

RibbitXDB is constantly evolving. v1.1.5.6 introduces the core stability features needed for mission-critical deployments.
Future updates will focus on:
- Remote Server Mode (TCP/TLS) improvements.
- Distributed Replication.
- JSON Document Store features.

---

<div align="center">
  <b>Built with ❤️ for the Python Community</b><br>
  <a href="https://docs.ribbitx.com">Documentation</a> •
  <a href="https://github.com/ribbitx/ribbitxdb">Source Code</a> •
  <a href="https://pypi.org/project/ribbitxdb/">PyPI</a>
</div>
