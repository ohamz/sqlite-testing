import random
import string
from enum import Enum

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

class Table:
    counter = 0

    def __init__(self, database):
        self.database = database
        self.name = f"t_{database.name}_{Table.counter}"
        Table.counter += 1
        self.columns = []
        self.rows = []

    def create_column(self, col_type):
        column = Column(self, col_type)
        self.columns.append(column)
        return column

    def create_row(self, null_probability=0.25):
        row = Row(self, null_probability)
        self.rows.append(row)
        return row

class Column:
    counter = 0

    def __init__(self, table, col_type):
        self.table = table
        self.name = f"c_{table.name}_{Column.counter}"
        Column.counter += 1
        self.col_type = col_type

class Row:
    def __init__(self, table, null_probability=0.25):
        self.table = table
        self.values = []
        for col in table.columns:
            if random.random() < null_probability:
                self.values.append(None)
            else:
                self.values.append(self.generate_value(col.col_type))

    def generate_value(self, col_type):
        if col_type == ColumnType.INTEGER:
            return random.randint(0, 1000)
        elif col_type == ColumnType.TEXT:
            return ''.join(random.choices(string.ascii_letters, k=5))
        elif col_type == ColumnType.REAL:
            return round(random.uniform(0, 1000), 2)
        elif col_type == ColumnType.BOOLEAN:
            return random.choice([True, False])
        else:
            raise ValueError(f"Unsupported column type: {col_type}")
