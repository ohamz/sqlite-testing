from schema import Database, ColumnType

class Generator:
    """
    Generator class responsible for creating a database with different types of tables
    targeting specific bug classes: crashes and logic bugs.
    """

    def __init__(self):
        self.db = Database()

    def generate_small_clean_table(self):
        """
        - Create a small table with 2-4 columns and 5-10 rows.
        - Columns have random types.
        - Null probability is very low (0%).
        - Run basic SELECT queries: 
            * SELECT all
            * SELECT specific columns
            * Simple WHERE conditions (e.g., WHERE col > 10)
        """
        pass

    def generate_small_null_heavy_table(self):
        """
        - Create a small table with 2-4 columns and 5-10 rows.
        - High NULL probability (85%).
        - Random column types.
        - Run SELECT queries:
            * WHERE col IS NULL
            * WHERE col IS NOT NULL
            * COUNT non-null rows
        """
        pass

    def generate_empty_table(self):
        """
        - Create a table with 2-5 columns but no rows.
        - Columns can be any type.
        - Run queries:
            * SELECT all
            * Aggregate functions (e.g., COUNT, SUM)
            * WHERE conditions that should return empty
        """
        pass

    def generate_wide_table(self):
        """
        - Create a table with 20–50 columns and 5-10 rows.
        - Mix column types.
        - Low NULL probability.
        - Run queries:
            * SELECT many columns
            * Project onto subsets of columns
            * WHERE conditions on rarely used columns
        """
        pass

    def generate_weird_types_table(self):
        """
        - Create a table mixing all types: INTEGER, TEXT, REAL, BOOLEAN.
        - 5–10 columns, 10–50 rows.
        - Random NULL insertions.
        - Run queries:
            * Type-sensitive conditions (e.g., WHERE text_col = 'abc')
            * Type casting operations
        """
        pass

    def generate_boolean_only_table(self):
        """
        - Create a table where all columns are BOOLEAN type.
        - 5–10 columns, 10–50 rows.
        - High NULL probability to stress three-valued logic (TRUE/FALSE/NULL).
        - Run queries:
            * Logical conditions (AND, OR, NOT)
            * WHERE col IS TRUE / IS FALSE / IS NULL
        """
        pass

    def generate_real_only_table(self):
        """
        - Create a table where all columns are REAL (float) type.
        - 5–10 columns, 10–50 rows.
        - Introduce small/large floating point values.
        - Run queries:
            * WHERE col > 0.0
            * Aggregate SUM, AVG, etc.
            * ORDER BY floating point values
        """
        pass

    def generate_integer_only_table(self):
        """
        - Create a table where all columns are INTEGER type.
        - 5–10 columns, 10–50 rows.
        - NULLs randomly introduced.
        - Run queries:
            * Arithmetic operations (+, -, *)
            * WHERE filters with range conditions
            * Aggregates like MIN, MAX
        """
        pass

    def generate_tall_table(self):
        """
        - Create a table with 2-4 columns and a large number of rows (e.g., 10,000 to 100,000).
        - Random types.
        - Moderate NULL probability.
        - Run queries:
            * SELECT with LIMIT/OFFSET
            * Aggregate queries (e.g., AVG, MAX)
            * Heavy WHERE filters
        """
        pass

    def generate_database(self):
        """
        Main function to create the full database structure by calling all other generation functions.
        Will be responsible for assembling the final consistent database.
        """
        pass
