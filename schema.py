import random
import string
from enum import Enum
from typing import List, Optional, Tuple

class ConstraintType(Enum):
    NOT_NULL = "NOT NULL"
    UNIQUE = "UNIQUE"
    PRIMARY_KEY = "PRIMARY KEY"
    DEFAULT = "DEFAULT"
    CHECK = "CHECK"

class ColumnType(Enum):
    INTEGER = "INTEGER"
    TEXT = "TEXT"
    REAL = "REAL"
    BOOLEAN = "BOOLEAN"

class Database:
    counter = 0

    def __init__(self):
        self.name = f"db{Database.counter}"
        Database.counter += 1
        self.tables = []

    def create_table(self):
        table = Table(self)
        self.tables.append(table)
        return table

    def to_sql(self):
        stmts = []
        for table in self.tables:
            stmts.append(table.create_table_sql())
            stmts.extend(table.insert_sql())
            stmts.extend(table.update_sql())
            stmts.extend(table.index_sql())
            stmts.extend(table.add_column_sql())
        stmts.extend(self.tables[2].delete_sql())
        stmts.extend(self.tables[3].delete_sql())
        return "\n".join(stmts)

    def to_json(self):
        return {
            table.name: {
                col.name: col.col_type.value for col in table.columns + table.extra_columns
            } for table in self.tables
        }

class Table:
    counter = 0

    def __init__(self, database):
        self.database = database
        self.name = f"t{Table.counter}"
        Table.counter += 1
        self.columns = []
        self.rows = []
        self.extra_columns = []
        self.indexes = []

    def create_column(self, col_type):
        column = Column(self, col_type)
        self.columns.append(column)
        return column

    def create_extra_column(self, col_type):
        column = Column(self, col_type)
        self.extra_columns.append(column)
        return column

    def create_index(self, column, unique=False):
        self.indexes.append((column, unique))

    def create_row(self, null_probability=0.5):
        row = Row(self, null_probability)
        self.rows.append(row)
        return row

    def create_table_sql(self):
        col_defs = []
        for col in self.columns:
            constraint_parts = []
            for ctype, val in col.constraints:
                if ctype in {ConstraintType.NOT_NULL, ConstraintType.UNIQUE, ConstraintType.PRIMARY_KEY}:
                    constraint_parts.append(ctype.value)
                elif ctype == ConstraintType.DEFAULT and val:
                    constraint_parts.append(f"DEFAULT {val}")
                elif ctype == ConstraintType.CHECK and val:
                    constraint_parts.append(f"CHECK ({val})")
            constraint_str = " ".join(constraint_parts)
            col_defs.append(f"{col.name} {col.col_type.value} {constraint_str}".strip())
        return f"CREATE TABLE {self.name} ({', '.join(col_defs)});"

    def insert_sql(self):
        stmts = []
        for row in self.rows:
            placeholders = []
            for val in row.values:
                if val is None:
                    placeholders.append("NULL")
                elif isinstance(val, str):
                    placeholders.append(f"'{val}'")
                else:
                    placeholders.append(str(val))
            stmt = f"INSERT INTO {self.name} VALUES ({', '.join(placeholders)});"
            stmts.append(stmt)
        return stmts

    def update_sql(self):
        stmts = []
        if not self.columns:
            return stmts
        col = random.choice(self.columns)
        value = Row.generate_value(self, col.col_type)
        stmt = f"UPDATE {self.name} SET {col.name} = {value} WHERE 1=1 LIMIT 50;"
        stmts.append(stmt)
        return stmts

    def delete_sql(self):
        if not self.columns:
            return []
        col = random.choice(self.columns)
        return [f"DELETE FROM {self.name} WHERE {col.name} < 20;",
                f"DELETE FROM {self.name} WHERE {col.name} IS NOT NULL;"]

    def index_sql(self):
        stmts = []
        for col, unique in self.indexes:
            index_name = f"idx_{self.name}_{col.name}"
            unique_str = "UNIQUE " if unique else ""
            stmt = f"CREATE {unique_str}INDEX {index_name} ON {self.name}({col.name});"
            stmts.append(stmt)
        return stmts

    def add_column_sql(self):
        stmts = []
        for col in self.extra_columns:
            stmt = f"ALTER TABLE {self.name} ADD COLUMN {col.name} {col.col_type.value};"
            stmts.append(stmt)
        return stmts

class Column:
    counter = 0

    def __init__(self, table, col_type):
        self.table = table
        self.name = f"c{Column.counter}"
        Column.counter += 1
        self.col_type = col_type
        self.constraints: List[Tuple[ConstraintType, Optional[str]]] = []

    def add_constraint(self, constraint_type: ConstraintType, value: Optional[str] = None):
        self.constraints.append((constraint_type, value))


class Row:
    def __init__(self, table, null_probability):
        self.table = table
        self.values = []
        for col in table.columns:
            if random.random() < null_probability:
                self.values.append(None)
            else:
                self.values.append(self.generate_value(col.col_type))

    def generate_value(self, col_type):
        if col_type == ColumnType.INTEGER:
            return random.randint(0, 100)
        elif col_type == ColumnType.TEXT:
            return ''.join(random.choices(string.ascii_letters, k=5))
        elif col_type == ColumnType.REAL:
            return round(random.uniform(0, 100), 2)
        elif col_type == ColumnType.BOOLEAN:
            return random.choice([0, 1])
        else:
            raise ValueError(f"Unsupported column type: {col_type}")

