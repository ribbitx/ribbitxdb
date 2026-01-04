import re
from typing import List, Dict, Any, Optional
from ..schema.types import DataType
from ..utils.exceptions import (
    SQLSyntaxError, UnsupportedFeatureError, 
    TableNotFoundError, ColumnNotFoundError
)

class Token:
    def __init__(self, type: str, value: str, line: int = 1, column: int = 0):
        self.type = type
        self.value = value
        self.line = line
        self.column = column
    
    def __repr__(self):
        return f"Token({self.type}, {self.value}, L{self.line}:C{self.column})"

class SQLParser:
    KEYWORDS = {
        'SELECT', 'FROM', 'WHERE', 'INSERT', 'INTO', 'VALUES', 'UPDATE', 
        'SET', 'DELETE', 'CREATE', 'TABLE', 'DROP', 'ALTER', 'PRIMARY', 
        'KEY', 'NOT', 'NULL', 'UNIQUE', 'INTEGER', 'TEXT', 'REAL', 'BLOB',
        'AND', 'OR', 'ORDER', 'BY', 'LIMIT', 'OFFSET', 'ASC', 'DESC',
        'JOIN', 'LEFT', 'RIGHT', 'INNER', 'ON', 'AS', 'DISTINCT', 'COUNT',
        'SUM', 'AVG', 'MIN', 'MAX', 'GROUP', 'HAVING', 'IN', 'LIKE', 'BETWEEN',
        'IF', 'EXISTS', 'DEFAULT', 'AUTOINCREMENT', 'INDEX', 'PRAGMA',
        'BEGIN', 'COMMIT', 'ROLLBACK', 'SAVEPOINT', 'RELEASE', 'TO',
        'DESCRIBE', 'DESC', 'SHOW', 'TABLES', 'INDEXES', 'EXPLAIN',
        'JSON', 'CURRENT_TIMESTAMP', 'CURRENT_DATE', 'CURRENT_TIME',
        'VIEW', 'TRIGGER', 'ALTER', 'RENAME', 'ADD', 'REFERENCES', 'CHECK',
        'UNION', 'ALL', 'CASE', 'WHEN', 'THEN', 'ELSE', 'END', 'CAST', 'CONSTRAINT'
    }
    
    def __init__(self):
        self.tokens: List[Token] = []
        self.position = 0
        self.sql_text = ""
    
    def tokenize(self, sql: str) -> List[Token]:
        self.sql_text = sql
        sql = sql.strip()
        tokens = []
        i = 0
        line = 1
        line_start = 0
        
        while i < len(sql):
            if sql[i] == '\n':
                line += 1
                line_start = i + 1
                i += 1
                continue
            
            if sql[i].isspace():
                i += 1
                continue
            
            column = i - line_start + 1
            
            if sql[i] in '(),;=<>!':
                if i + 1 < len(sql) and sql[i:i+2] in ('<=', '>=', '!=', '<>'):
                    tokens.append(Token('OPERATOR', sql[i:i+2], line, column))
                    i += 2
                else:
                    tokens.append(Token('SYMBOL', sql[i], line, column))
                    i += 1
                continue
            
            if sql[i] in ('"', "'"):
                quote = sql[i]
                i += 1
                start = i
                while i < len(sql) and sql[i] != quote:
                    if sql[i] == '\\' and i + 1 < len(sql):
                        i += 2
                    else:
                        i += 1
                tokens.append(Token('STRING', sql[start:i], line, column))
                i += 1
                continue
            
            if sql[i].isdigit() or (sql[i] == '-' and i + 1 < len(sql) and sql[i + 1].isdigit()):
                start = i
                if sql[i] == '-':
                    i += 1
                while i < len(sql) and (sql[i].isdigit() or sql[i] == '.'):
                    i += 1
                tokens.append(Token('NUMBER', sql[start:i], line, column))
                continue
            
            if sql[i].isalpha() or sql[i] == '_':
                start = i
                while i < len(sql) and (sql[i].isalnum() or sql[i] == '_'):
                    i += 1
                word = sql[start:i]
                if word.upper() in self.KEYWORDS:
                    tokens.append(Token('KEYWORD', word.upper(), line, column))
                else:
                    tokens.append(Token('IDENTIFIER', word, line, column))
                continue
            
            if sql[i] == '*':
                tokens.append(Token('STAR', '*', line, column))
                i += 1
                continue
            
            if sql[i] == '?':
                tokens.append(Token('PARAMETER', '?', line, column))
                i += 1
                continue
            
            i += 1
        
        self.tokens = tokens
        return tokens
    
    def parse(self, sql: str) -> Dict[str, Any]:
        self.tokenize(sql)
        self.position = 0
        
        if not self.tokens:
            raise SQLSyntaxError("Empty SQL statement")
        
        first_token = self.tokens[0]
        
        try:
            if first_token.value == 'SELECT':
                return self.parse_select()
            elif first_token.value == 'INSERT':
                return self.parse_insert()
            elif first_token.value == 'UPDATE':
                return self.parse_update()
            elif first_token.value == 'DELETE':
                return self.parse_delete()
            elif first_token.value == 'CREATE':
                return self.parse_create()
            elif first_token.value == 'DROP':
                return self.parse_drop()
            elif first_token.value == 'PRAGMA':
                return self.parse_pragma()
            elif first_token.value == 'BEGIN':
                return self.parse_begin()
            elif first_token.value == 'COMMIT':
                return {'type': 'COMMIT'}
            elif first_token.value == 'ROLLBACK':
                return self.parse_rollback()
            elif first_token.value == 'SAVEPOINT':
                return self.parse_savepoint()
            elif first_token.value == 'RELEASE':
                return self.parse_release()
            elif first_token.value in ('DESCRIBE', 'DESC'):
                return self.parse_describe()
            elif first_token.value == 'SHOW':
                return self.parse_show()
            elif first_token.value == 'EXPLAIN':
                return self.parse_explain()
            elif first_token.value == 'ALTER':
                return self.parse_alter()
            else:
                raise SQLSyntaxError(
                    f"Unsupported SQL statement: {first_token.value}",
                    line=first_token.line,
                    column=first_token.column,
                    hint="Supported commands: SELECT, INSERT, UPDATE, DELETE, CREATE, DROP, ALTER, PRAGMA, BEGIN, COMMIT, ROLLBACK, DESCRIBE, SHOW, EXPLAIN"
                )
        except SQLSyntaxError:
            raise
        except Exception as e:
            token = self.current_token()
            if token:
                raise SQLSyntaxError(
                    str(e),
                    line=token.line,
                    column=token.column,
                    context=self._get_context(token)
                )
            raise SQLSyntaxError(str(e))
    
    def _get_context(self, token: Token, width: int = 50) -> str:
        lines = self.sql_text.split('\n')
        if token.line <= len(lines):
            line_text = lines[token.line - 1]
            start = max(0, token.column - width // 2)
            end = min(len(line_text), token.column + width // 2)
            return line_text[start:end]
        return ""
    
    def current_token(self) -> Optional[Token]:
        if self.position < len(self.tokens):
            return self.tokens[self.position]
        return None
    
    def peek_token(self, offset: int = 1) -> Optional[Token]:
        pos = self.position + offset
        if pos < len(self.tokens):
            return self.tokens[pos]
        return None
    
    def consume(self, expected: Optional[str] = None) -> Token:
        token = self.current_token()
        if token is None:
            raise SQLSyntaxError(
                "Unexpected end of SQL statement",
                hint=f"Expected: {expected}" if expected else None
            )
        if expected and token.value != expected:
            raise SQLSyntaxError(
                f"Expected '{expected}', got '{token.value}'",
                line=token.line,
                column=token.column,
                context=self._get_context(token),
                hint=self._get_hint(expected, token.value)
            )
        self.position += 1
        return token
    
    def _get_hint(self, expected: str, got: str) -> Optional[str]:
        hints = {
            ('(', 'NOT'): "Did you mean 'IF NOT EXISTS'?",
            ('TABLE', 'NOT'): "Did you mean 'CREATE TABLE IF NOT EXISTS'?",
            ('EXISTS', 'TABLE'): "Missing 'IF' keyword before 'EXISTS'",
        }
        return hints.get((expected, got))
    
    def parse_pragma(self) -> Dict[str, Any]:
        self.consume('PRAGMA')
        pragma_name = self.consume().value
        
        args = []
        if self.current_token() and self.current_token().value == '(':
            self.consume('(')
            while True:
                token = self.consume()
                if token.type == 'STRING':
                    args.append(token.value)
                elif token.type == 'NUMBER':
                    args.append(int(token.value) if '.' not in token.value else float(token.value))
                else:
                    args.append(token.value)
                
                if self.current_token() and self.current_token().value == ',':
                    self.consume(',')
                else:
                    break
            self.consume(')')
        
        return {
            'type': 'PRAGMA',
            'name': pragma_name,
            'args': args
        }
    
    def parse_begin(self) -> Dict[str, Any]:
        self.consume('BEGIN')
        if self.current_token() and self.current_token().value == 'TRANSACTION':
            self.consume('TRANSACTION')
        return {'type': 'BEGIN'}
    
    def parse_rollback(self) -> Dict[str, Any]:
        self.consume('ROLLBACK')
        savepoint = None
        if self.current_token() and self.current_token().value == 'TO':
            self.consume('TO')
            if self.current_token() and self.current_token().value == 'SAVEPOINT':
                self.consume('SAVEPOINT')
            savepoint = self.consume().value
        return {'type': 'ROLLBACK', 'savepoint': savepoint}
    
    def parse_savepoint(self) -> Dict[str, Any]:
        self.consume('SAVEPOINT')
        name = self.consume().value
        return {'type': 'SAVEPOINT', 'name': name}
    
    def parse_release(self) -> Dict[str, Any]:
        self.consume('RELEASE')
        if self.current_token() and self.current_token().value == 'SAVEPOINT':
            self.consume('SAVEPOINT')
        name = self.consume().value
        return {'type': 'RELEASE', 'name': name}
    
    def parse_describe(self) -> Dict[str, Any]:
        self.consume()
        table = self.consume().value
        return {'type': 'DESCRIBE', 'table': table}
    
    def parse_show(self) -> Dict[str, Any]:
        self.consume('SHOW')
        what = self.consume().value
        
        if what == 'TABLES':
            return {'type': 'SHOW', 'what': 'TABLES'}
        elif what == 'INDEXES':
            table = self.consume().value if self.current_token() else None
            return {'type': 'SHOW', 'what': 'INDEXES', 'table': table}
        else:
            raise SQLSyntaxError(
                f"Unknown SHOW command: {what}",
                hint="Supported: SHOW TABLES, SHOW INDEXES [table]"
            )
    
    def parse_explain(self) -> Dict[str, Any]:
        self.consume('EXPLAIN')
        query = self.parse()
        return {'type': 'EXPLAIN', 'query': query}
    
    def parse_select(self) -> Dict[str, Any]:
        self.consume('SELECT')
        
        distinct = False
        if self.current_token() and self.current_token().value == 'DISTINCT':
            distinct = True
            self.consume('DISTINCT')
        
        columns = []
        aggregates = []
        if self.current_token() and self.current_token().type == 'STAR':
            columns.append('*')
            self.consume()
        else:
            while True:
                if self.current_token() and self.current_token().value in ('COUNT', 'SUM', 'AVG', 'MIN', 'MAX'):
                    agg_func = self.consume().value
                    self.consume('(')
                    if self.current_token() and self.current_token().type == 'STAR':
                        agg_col = '*'
                        self.consume()
                    else:
                        agg_col = self.consume().value
                    self.consume(')')
                    
                    alias = None
                    if self.current_token() and self.current_token().value == 'AS':
                        self.consume('AS')
                        alias = self.consume().value
                    
                    aggregates.append({
                        'function': agg_func,
                        'column': agg_col,
                        'alias': alias or f"{agg_func.lower()}_{agg_col}"
                    })
                    columns.append(alias or f"{agg_func.lower()}_{agg_col}")
                else:
                    col = self.consume().value
                    columns.append(col)
                
                if self.current_token() and self.current_token().value == ',':
                    self.consume(',')
                else:
                    break
        
        self.consume('FROM')
        table = self.consume().value
        
        joins = []
        while self.current_token() and self.current_token().value in ('JOIN', 'INNER', 'LEFT', 'RIGHT'):
            join_type = 'INNER'
            if self.current_token().value in ('LEFT', 'RIGHT', 'INNER'):
                join_type = self.consume().value
            self.consume('JOIN')
            
            join_table = self.consume().value
            self.consume('ON')
            
            left_col = self.consume().value
            self.consume('=')
            right_col = self.consume().value
            
            joins.append({
                'type': join_type,
                'table': join_table,
                'on': {'left': left_col, 'right': right_col}
            })
        
        where_clause = None
        if self.current_token() and self.current_token().value == 'WHERE':
            self.consume('WHERE')
            where_clause = self.parse_where_advanced()
        
        group_by = None
        if self.current_token() and self.current_token().value == 'GROUP':
            self.consume('GROUP')
            self.consume('BY')
            group_by = []
            while True:
                group_by.append(self.consume().value)
                if self.current_token() and self.current_token().value == ',':
                    self.consume(',')
                else:
                    break
        
        having = None
        if self.current_token() and self.current_token().value == 'HAVING':
            self.consume('HAVING')
            having = self.parse_where_advanced()
        
        order_by = None
        if self.current_token() and self.current_token().value == 'ORDER':
            self.consume('ORDER')
            self.consume('BY')
            order_by = []
            while True:
                col = self.consume().value
                direction = 'ASC'
                if self.current_token() and self.current_token().value in ('ASC', 'DESC'):
                    direction = self.consume().value
                order_by.append({'column': col, 'direction': direction})
                
                if self.current_token() and self.current_token().value == ',':
                    self.consume(',')
                else:
                    break
        
        limit = None
        offset = None
        if self.current_token() and self.current_token().value == 'LIMIT':
            self.consume('LIMIT')
            limit = int(self.consume().value)
            
            if self.current_token() and self.current_token().value == 'OFFSET':
                self.consume('OFFSET')
                offset = int(self.consume().value)
        
        result = {
            'type': 'SELECT',
            'distinct': distinct,
            'columns': columns,
            'aggregates': aggregates,
            'table': table,
            'joins': joins,
            'where': where_clause,
            'group_by': group_by,
            'having': having,
            'order_by': order_by,
            'limit': limit,
            'offset': offset
        }
        
        if self.current_token() and self.current_token().value == 'UNION':
            union_all = False
            self.consume('UNION')
            if self.current_token() and self.current_token().value == 'ALL':
                self.consume('ALL')
                union_all = True
            
            result['union'] = {
                'all': union_all,
                'next': self.parse_select()
            }
            
        return result
        
        if self.current_token() and self.current_token().value == 'UNION':
            union_all = False
            self.consume('UNION')
            if self.current_token() and self.current_token().value == 'ALL':
                self.consume('ALL')
                union_all = True
            
            result['union'] = {
                'all': union_all,
                'next': self.parse_select()
            }
            
        return result
    
    def parse_insert(self) -> Dict[str, Any]:
        self.consume('INSERT')
        self.consume('INTO')
        table = self.consume().value
        
        columns = None
        if self.current_token() and self.current_token().value == '(':
            self.consume('(')
            columns = []
            while True:
                col = self.consume()
                columns.append(col.value)
                if self.current_token() and self.current_token().value == ',':
                    self.consume(',')
                else:
                    break
            self.consume(')')
        
        self.consume('VALUES')
        self.consume('(')
        
        values = []
        while True:
            token = self.consume()
            if token.type == 'STRING':
                values.append(token.value)
            elif token.type == 'NUMBER':
                if '.' in token.value:
                    values.append(float(token.value))
                else:
                    values.append(int(token.value))
            elif token.value == 'NULL':
                values.append(None)
            elif token.value in ('CURRENT_TIMESTAMP', 'CURRENT_DATE', 'CURRENT_TIME'):
                values.append({'function': token.value})
            else:
                values.append(token.value)
            
            if self.current_token() and self.current_token().value == ',':
                self.consume(',')
            else:
                break
        
        self.consume(')')
        
        return {
            'type': 'INSERT',
            'table': table,
            'columns': columns,
            'values': values
        }
    
    def parse_update(self) -> Dict[str, Any]:
        self.consume('UPDATE')
        table = self.consume().value
        self.consume('SET')
        
        updates = {}
        while True:
            col = self.consume().value
            self.consume('=')
            value_token = self.consume()
            
            if value_token.type == 'STRING':
                value = value_token.value
            elif value_token.type == 'NUMBER':
                value = float(value_token.value) if '.' in value_token.value else int(value_token.value)
            elif value_token.value == 'NULL':
                value = None
            else:
                value = value_token.value
            
            updates[col] = value
            
            if self.current_token() and self.current_token().value == ',':
                self.consume(',')
            else:
                break
        
        where_clause = None
        if self.current_token() and self.current_token().value == 'WHERE':
            self.consume('WHERE')
            where_clause = self.parse_where()
        
        return {
            'type': 'UPDATE',
            'table': table,
            'updates': updates,
            'where': where_clause
        }
    
    def parse_delete(self) -> Dict[str, Any]:
        self.consume('DELETE')
        self.consume('FROM')
        table = self.consume().value
        
        where_clause = None
        if self.current_token() and self.current_token().value == 'WHERE':
            self.consume('WHERE')
            where_clause = self.parse_where()
        
        return {
            'type': 'DELETE',
            'table': table,
            'where': where_clause
        }
    
    def parse_create(self) -> Dict[str, Any]:
        self.consume('CREATE')
        
        next_token = self.current_token()
        if next_token and next_token.value == 'INDEX':
            return self.parse_create_index(unique=False)
        elif next_token and next_token.value == 'UNIQUE':
            self.consume('UNIQUE')
            self.consume('INDEX')
            return self.parse_create_index(unique=True)
        elif next_token and next_token.value == 'TABLE':
            return self.parse_create_table()
        elif next_token and next_token.value == 'VIEW':
            return self.parse_create_view()
        elif next_token and next_token.value == 'TRIGGER':
            # Basic dummy support for now to avoid crashing, or raise nice error
            raise UnsupportedFeatureError("TRIGGER parsing not yet fully implemented")
        else:
            raise SQLSyntaxError(
                f"Expected TABLE, INDEX, or VIEW after CREATE, got {next_token.value if next_token else 'EOF'}",
                hint="Supported: CREATE TABLE, CREATE VIEW, CREATE INDEX"
            )
    
    def parse_create_table(self) -> Dict[str, Any]:
        self.consume('TABLE')
        
        if_not_exists = False
        if self.current_token() and self.current_token().value == 'IF':
            self.consume('IF')
            self.consume('NOT')
            self.consume('EXISTS')
            if_not_exists = True
        
        table = self.consume().value
        self.consume('(')
        
        columns = []
        while True:
            # Check for Table Constraints
            if self.current_token().value == 'FOREIGN':
                self.consume('FOREIGN')
                self.consume('KEY')
                self.consume('(')
                fk_col_name = self.consume().value
                self.consume(')')
                self.consume('REFERENCES')
                ref_table = self.consume().value
                self.consume('(')
                ref_col = self.consume().value
                self.consume(')')
                
                # Attach to existing column
                found = False
                for col in columns:
                    if col['name'] == fk_col_name:
                        col['references'] = {'table': ref_table, 'column': ref_col}
                        found = True
                        break
                
                if not found:
                     raise SQLSyntaxError(f"Foreign key references non-existent column: {fk_col_name}")

                if self.current_token() and self.current_token().value == ',':
                    self.consume(',')
                    continue
                else:
                    break
            
            # Check for PRIMARY KEY (Table Level)
            elif self.current_token().value == 'PRIMARY':
                self.consume('PRIMARY')
                self.consume('KEY')
                self.consume('(')
                # TODO: Handle composite keys, for now just consume single
                pk_col = self.consume().value
                self.consume(')')
                
                for col in columns:
                    if col['name'] == pk_col:
                        col['primary_key'] = True
                
                if self.current_token() and self.current_token().value == ',':
                    self.consume(',')
                    continue
                else:
                    break

            col_name = self.consume().value
            col_type = self.consume().value
            
            constraints = {
                'primary_key': False,
                'not_null': False,
                'unique': False,
                'autoincrement': False,
                'default': None
            }
            
            while self.current_token() and self.current_token().type == 'KEYWORD':
                keyword = self.current_token().value
                if keyword == 'PRIMARY':
                    self.consume('PRIMARY')
                    self.consume('KEY')
                    constraints['primary_key'] = True
                    if self.current_token() and self.current_token().value == 'AUTOINCREMENT':
                        self.consume('AUTOINCREMENT')
                        constraints['autoincrement'] = True
                elif keyword == 'NOT':
                    self.consume('NOT')
                    self.consume('NULL')
                    constraints['not_null'] = True
                elif keyword == 'UNIQUE':
                    self.consume('UNIQUE')
                    constraints['unique'] = True
                elif keyword == 'DEFAULT':
                    self.consume('DEFAULT')
                    default_token = self.consume()
                    if default_token.type == 'STRING':
                        constraints['default'] = default_token.value
                    elif default_token.type == 'NUMBER':
                        constraints['default'] = float(default_token.value) if '.' in default_token.value else int(default_token.value)
                    elif default_token.value in ('CURRENT_TIMESTAMP', 'CURRENT_DATE', 'CURRENT_TIME', 'NULL'):
                        constraints['default'] = default_token.value
                    else:
                        constraints['default'] = default_token.value
                elif keyword == 'CHECK':
                    self.consume('CHECK')
                    self.consume('(')
                    # Simple consumption of check expression
                    expr = ""
                    depth = 1
                    while depth > 0:
                        tok = self.consume()
                        if tok.value == '(': depth += 1
                        elif tok.value == ')': depth -= 1
                        if depth > 0: expr += tok.value + " "
                    constraints['check'] = expr.strip()
                elif keyword == 'REFERENCES':
                    self.consume('REFERENCES')
                    ref_table = self.consume().value
                    self.consume('(')
                    ref_col = self.consume().value
                    self.consume(')')
                    constraints['references'] = {'table': ref_table, 'column': ref_col}
                else:
                    break
            
            columns.append({
                'name': col_name,
                'type': col_type,
                **constraints
            })
            
            if self.current_token() and self.current_token().value == ',':
                self.consume(',')
            else:
                break
        
        self.consume(')')
        
        return {
            'type': 'CREATE',
            'table': table,
            'columns': columns,
            'if_not_exists': if_not_exists
        }
    
    def parse_create_index(self, unique: bool) -> Dict[str, Any]:
        if_not_exists = False
        if self.current_token() and self.current_token().value == 'IF':
            self.consume('IF')
            self.consume('NOT')
            self.consume('EXISTS')
            if_not_exists = True
        
        index_name = self.consume().value
        self.consume('ON')
        table_name = self.consume().value
        self.consume('(')
        
        columns = []
        while True:
            columns.append(self.consume().value)
            if self.current_token() and self.current_token().value == ',':
                self.consume(',')
            else:
                break
        
        self.consume(')')
        
        return {
            'type': 'CREATE_INDEX',
            'index_name': index_name,
            'table': table_name,
            'columns': columns,
            'unique': unique,
            'if_not_exists': if_not_exists
        }
    
    def parse_drop(self) -> Dict[str, Any]:
        self.consume('DROP')
        
        next_token = self.current_token()
        if next_token and next_token.value == 'TABLE':
            return self.parse_drop_table()
        elif next_token and next_token.value == 'INDEX':
            return self.parse_drop_index()
        elif next_token and next_token.value == 'VIEW':
            return self.parse_drop_view()
        else:
            raise SQLSyntaxError(
                f"Expected TABLE, VIEW, or INDEX after DROP, got {next_token.value if next_token else 'EOF'}",
                hint="Supported: DROP TABLE, DROP VIEW, DROP INDEX"
            )
    
    def parse_drop_table(self) -> Dict[str, Any]:
        self.consume('TABLE')
        
        if_exists = False
        if self.current_token() and self.current_token().value == 'IF':
            self.consume('IF')
            self.consume('EXISTS')
            if_exists = True
        
        table = self.consume().value
        
        return {
            'type': 'DROP',
            'table': table,
            'if_exists': if_exists
        }
    
    def parse_drop_index(self) -> Dict[str, Any]:
        self.consume('INDEX')
        
        if_exists = False
        if self.current_token() and self.current_token().value == 'IF':
            self.consume('IF')
            self.consume('EXISTS')
            if_exists = True
        
        index_name = self.consume().value
        
        return {
            'type': 'DROP_INDEX',
            'index_name': index_name,
            'if_exists': if_exists
        }

    def parse_drop_view(self) -> Dict[str, Any]:
        self.consume('VIEW')
        
        if_exists = False
        if self.current_token() and self.current_token().value == 'IF':
            self.consume('IF')
            self.consume('EXISTS')
            if_exists = True
        
        view_name = self.consume().value
        
        return {
            'type': 'DROP_VIEW',
            'view_name': view_name,
            'if_exists': if_exists
        }
    
    def parse_where(self) -> Dict[str, Any]:
        left = self.consume().value
        operator = self.consume().value
        right_token = self.consume()
        
        if right_token.type == 'STRING':
            right = right_token.value
        elif right_token.type == 'NUMBER':
            right = float(right_token.value) if '.' in right_token.value else int(right_token.value)
        elif right_token.value == 'NULL':
            right = None
        elif right_token.type == 'PARAMETER':
            right = {'parameter': True}
        else:
            right = right_token.value
        
        return {
            'column': left,
            'operator': operator,
            'value': right
        }
    
    def parse_where_advanced(self) -> Dict[str, Any]:
        conditions = []
        logical_ops = []
        
        while True:
            left = self.consume().value
            operator = self.consume().value
            
            if operator == 'LIKE':
                pattern = self.consume().value
                conditions.append({
                    'column': left,
                    'operator': 'LIKE',
                    'value': pattern
                })
            elif operator == 'IN':
                self.consume('(')
                values = []
                while True:
                    token = self.consume()
                    if token.type == 'STRING':
                        values.append(token.value)
                    elif token.type == 'NUMBER':
                        values.append(float(token.value) if '.' in token.value else int(token.value))
                    else:
                        values.append(token.value)
                    
                    if self.current_token() and self.current_token().value == ',':
                        self.consume(',')
                    else:
                        break
                self.consume(')')
                conditions.append({
                    'column': left,
                    'operator': 'IN',
                    'value': values
                })
            elif operator == 'BETWEEN':
                start_token = self.consume()
                start_val = float(start_token.value) if start_token.type == 'NUMBER' and '.' in start_token.value else int(start_token.value) if start_token.type == 'NUMBER' else start_token.value
                
                self.consume('AND')
                
                end_token = self.consume()
                end_val = float(end_token.value) if end_token.type == 'NUMBER' and '.' in end_token.value else int(end_token.value) if end_token.type == 'NUMBER' else end_token.value
                
                conditions.append({
                    'column': left,
                    'operator': 'BETWEEN',
                    'value': [start_val, end_val]
                })
            else:
                right_token = self.consume()
                if right_token.type == 'STRING':
                    right = right_token.value
                elif right_token.type == 'NUMBER':
                    right = float(right_token.value) if '.' in right_token.value else int(right_token.value)
                elif right_token.value == 'NULL':
                    right = None
                elif right_token.type == 'PARAMETER':
                    right = {'parameter': True}
                else:
                    right = right_token.value
                
                conditions.append({
                    'column': left,
                    'operator': operator,
                    'value': right
                })
            
            if self.current_token() and self.current_token().value in ('AND', 'OR'):
                logical_ops.append(self.consume().value)
            else:
                break
        
        if len(conditions) == 1:
            return conditions[0]
        
        return {
            'type': 'COMPOUND',
            'conditions': conditions,
            'operators': logical_ops
        }

    def parse_create_view(self) -> Dict[str, Any]:
        self.consume('VIEW')
        
        if_not_exists = False
        if self.current_token() and self.current_token().value == 'IF':
            self.consume('IF')
            self.consume('NOT')
            self.consume('EXISTS')
            if_not_exists = True
            
        view_name = self.consume().value
        self.consume('AS')
        
        # Parse the SELECT statement
        # We need to capture the raw SQL for the view sometimes, but for now we parse it
        # Actually, for Views, it's often better to store the raw SQL string for the body
        # But our parser consumes tokens. We can parse the select definition.
        
        select_def = self.parse_select()
        
        return {
            'type': 'CREATE_VIEW',
            'view_name': view_name,
            'definition': select_def,
            'if_not_exists': if_not_exists,
            'sql': self.sql_text # Store approximate SQL? Or just definition
        }

    def parse_alter(self) -> Dict[str, Any]:
        self.consume('ALTER')
        self.consume('TABLE')
        table_name = self.consume().value
        
        operation = {}
        
        if self.current_token().value == 'RENAME':
            self.consume('RENAME')
            self.consume('TO')
            new_name = self.consume().value
            operation = {'type': 'RENAME', 'new_name': new_name}
        elif self.current_token().value == 'ADD':
            self.consume('ADD')
            if self.current_token().value == 'COLUMN':
                self.consume('COLUMN')
            
            # Use parse_column_def logic (extract from parse_create_table or duplicate)
            # Duplicating logic for safety as parse_create_table is huge
            col_name = self.consume().value
            col_type = self.consume().value
            constraints = {
                'primary_key': False, 'not_null': False, 'unique': False, 
                'autoincrement': False, 'default': None, 'check': None, 'references': None
            }
            # Simplified constraint parsing for ADD COLUMN
            while self.current_token() and self.current_token().type == 'KEYWORD':
                 keyword = self.current_token().value
                 if keyword == 'NOT':
                     self.consume('NOT'); self.consume('NULL'); constraints['not_null'] = True
                 elif keyword == 'DEFAULT':
                     self.consume('DEFAULT')
                     def_tok = self.consume()
                     constraints['default'] = def_tok.value
                 else:
                     break
            
            operation = {
                'type': 'ADD_COLUMN', 
                'column': {'name': col_name, 'type': col_type, **constraints}
            }
        else:
             raise SQLSyntaxError("Expected RENAME TO or ADD COLUMN")
             
        return {
            'type': 'ALTER',
            'table': table_name,
            'operation': operation
        }
