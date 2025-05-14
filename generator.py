from enum import Enum, auto
from typing import List
from sqlglot import parse_one, exp
import copy
import random

class MutationTechnique(Enum):
    PQS = auto()
    TLP = auto()
    EET = auto()
    GENERIC = auto()
class Generator:
    """
    Generator class responsible for creating a database with different types of tables
    targeting specific bug classes: crashes and logic bugs.
    """

    def __init__(self, db_json: str):
        self.schema = db_json
        self.comparison_operators = [exp.EQ, exp.NEQ, exp.GT, exp.LT, exp.GTE, exp.LTE]
        self.aggregate_functions = [exp.Count, exp.Sum, exp.Avg, exp.Max, exp.Min]

    def mutate_query(self, sql: str, technique: MutationTechnique, count: int) -> List[str]:
        """Dispatches to the appropriate mutation technique."""
        if technique == MutationTechnique.PQS:
            return self.pivoted_query_synthesis(sql, count)
        elif technique == MutationTechnique.TLP:
            return self.ternary_logic_partitioning(sql, count)
        elif technique == MutationTechnique.EET:
            return self.equivalent_expression_transformation(sql, count)
        elif technique == MutationTechnique.GENERIC:
            return self.generic_mutation(sql, count)
        else:
            return []

    def pivoted_query_synthesis(self, sql: str, count: int) -> List[str]:
        return []  # TODO

    def ternary_logic_partitioning(self, sql: str, count: int) -> List[str]:
        return []  # TODO

    def equivalent_expression_transformation(self, sql: str, count: int) -> List[str]:
        return []  # TODO

    # Helper function to get all columns from the schema
    def get_all_columns(self, ast):
        tables = {t.this for t in ast.find_all(exp.Table)}
        cols = []
        for t in tables:
            # print(f"Table: {t}")
            # print(f"Schema: {self.schema}")
            if t in self.schema:
                # print(f"Schema: {self.schema[t]}")
                cols.extend((col, typ) for col, typ in self.schema[t].items())
        return cols

    # Helper function to update all columns in the AST
    def update_all_columns(self, ast, new_table):
        valid_columns = list(self.schema[new_table].keys())

        for column in ast.find_all(exp.Column):
            # Pick a new valid column name
            new_col_name = random.choice(valid_columns)
            # Replace with new column (optionally qualified)
            column.set("this", new_col_name)
            if column.args.get("table"):
                column.set("table", new_table)


    def generic_mutation(self, sql: str, count: int) -> List[str]:
        """
        Perform schema-aware SQL mutations using sqlglot and self.schema.
        """
        try:
            original_ast = parse_one(sql, error_level='IGNORE')
        except Exception as e:
            print(f"Failed to parse SQL: {e}")
            return []

        mutations = []

        for _ in range(count):
            # print("\nNEW MUTATION")
            mutated_ast = copy.deepcopy(original_ast)

            # --- 1. Replace table and SELECT * or project subset of columns ---
            select = mutated_ast.find(exp.Select)
            main_table = random.choice(list(self.schema.keys()))
            table_expr = exp.Table(this=main_table)
            mutated_ast.set("from", exp.From(this=table_expr))
            self.update_all_columns(mutated_ast, main_table)


            if select:
                # Skip table replacement if using JOIN with USING 
                has_using_join = any(
                    isinstance(j.args.get("using"), list)
                    for j in mutated_ast.find_all(exp.Join)
                )
                if not has_using_join:
                    # Replace SELECT * or existing columns
                    if random.random() < 0.2:
                        # Keep SELECT * (randomly)
                        select.set("expressions", [exp.Star()])
                    else:
                        # Use a random subset of columns
                        columns = list(self.schema[main_table].items())
                        # columns = self.get_all_columns(mutated_ast)
                        col_subset = random.sample(columns, k=random.randint(1, len(columns)))
                        select.set("expressions", [exp.Column(this=col_name) for col_name, _ in col_subset])


            # --- 2. Mutate literals using type-aware replacements ---
            for literal in mutated_ast.find_all(exp.Literal):
                if literal.is_number:
                    new_num = random.randint(1, 100)
                    literal.replace(exp.Literal.number(str(new_num)))
                elif literal.is_string:
                    new_str = random.choice(["'foo'", "'bar'", "'pivot'", "'test'"])
                    literal.replace(exp.Literal.string(new_str))


            # --- 3. Flip comparison operators ---
            columns = self.get_all_columns(mutated_ast)
            for comp in mutated_ast.find_all(exp.Condition):
                left = comp.args.get("this")
                right = comp.args.get("expression")

                if not left or not right:
                    continue
                
                # Skip boolean and NULL expressions
                if (isinstance(right, (exp.Boolean, exp.Null, exp.Is))):
                    continue
                col_name, _ = random.choice(columns)
                new_op_cls = random.choice(self.comparison_operators)
                comp.replace(new_op_cls(this=exp.Column(this=col_name), expression=right))


            # --- 4. Add smart WHERE logic using schema ---
            where = mutated_ast.find(exp.Where)
            if where and random.random() < 0.45:
                # Add a AND/OR condition
                op_cls = random.choice([exp.And, exp.Or])
                bool_val = random.choice(["TRUE", "FALSE"])
                if random.random() < 0.35:
                    # Add a random TRUE/FALSE
                    expr = exp.Literal(this=bool_val, is_string=False)
                else:
                    # Add a random (NOT) IS NULL/TRUE/FALSE
                    col_name, _ = random.choice(columns)
                    expr = exp.Is(
                        this=exp.Column(this=col_name),
                        expression= exp.Null() if random.random() < 0.7 else bool_val
                    )
                # Randomly wrap with NOT
                if random.random() < 1:
                    expr = exp.Not(this=expr)

                new_condition = op_cls(this=where.this, expression=expr)
                where.set("this", new_condition)
                mutated_ast.set("where", where)


            # --- 5. Add/Modify JOIN clauses ---
            if not mutated_ast.find(exp.Join):
                # JOIN type mutation
                join_nodes = list(mutated_ast.find_all(exp.Join))
                for join in join_nodes:
                    new_type = random.choice(["inner", "left", "cross"])
                    join.set("kind", new_type)

                # JOIN insertion (ON + USING)
                if (isinstance(mutated_ast, exp.Select) and random.random() < 0.2):
                    current_table = main_table  # use consistent table
                    other_tables = [t for t in self.schema if t != current_table]

                    if not other_tables:
                        continue  # No other table to join

                    join_table = random.choice(list(self.schema.keys()))  # initially choose any table
                    current_schema = self.schema[current_table]
                    join_schema = self.schema[join_table]

                    use_using = random.random() < 0.5

                    if use_using:
                        # Only allow USING if tables are the same
                        join_table = current_table  # force same table for USING

                        common_cols = set(current_schema.keys()) & set(join_schema.keys())
                        if common_cols:
                            col = random.choice(list(common_cols))
                            mutated_ast = mutated_ast.join(
                                join_table,
                                using=[col],
                                join_type=None
                            )
                    else:
                        # Only allow ON if tables are different
                        join_table = random.choice(other_tables)
                        join_schema = self.schema[join_table]

                        compatible_pairs = [
                            (col1, col2)
                            for col1, type1 in current_schema.items()
                            for col2, type2 in join_schema.items()
                            if type1 == type2
                        ]

                        if not compatible_pairs:
                            continue  # skip if no valid join pair

                        col1, col2 = random.choice(compatible_pairs)
                        on_condition = f"{current_table}.{col1} = {join_table}.{col2}"
                        mutated_ast = mutated_ast.join(
                            join_table,
                            on=on_condition,
                            join_type=None
                        )


            # --- 6. Random GROUP BY addition ---
            columns = self.get_all_columns(mutated_ast)
            if isinstance(mutated_ast, exp.Select) and random.random() < 0.4:
                # Pick random GROUP BY columns
                group_columns = random.sample(columns, k=random.randint(1, min(3, len(columns))))
                col_names = [col for col, _ in group_columns]
                mutated_ast = mutated_ast.group_by(*col_names, append=False)

                # Add aggregates in SELECT if needed
                select_exprs = []
                for col_name, _ in columns:
                    if col_name in col_names:
                        select_exprs.append(exp.Column(this=col_name))
                    else:
                        # Randomly add an aggregate function
                        agg_cls = random.choice(self.aggregate_functions)
                        select_exprs.append(agg_cls(this=exp.Column(this=col_name)))
                mutated_ast.set("expressions", select_exprs)

                # Add a HAVING clause
                agg_candidates = [col for col in columns if col not in col_names]
                if agg_candidates and random.random() < 0.4:
                    having_col = random.choice(agg_candidates)
                    func_cls = random.choice(self.aggregate_functions)
                    op_cls = random.choice(self.comparison_operators)

                    having_expr = op_cls(
                        this=func_cls(this=exp.Column(this=having_col[0])),
                        expression=exp.Literal.number(str(random.randint(1, 100)))
                    )
                    mutated_ast = mutated_ast.having(having_expr, append=False)


            # --- 7. Random ORDER BY addition ---
            if isinstance(mutated_ast, exp.Select) and random.random() < 0.4:
                order_columns = random.sample(columns, k=random.randint(1, min(3, len(columns))))
                order_parts = []

                for col_name, _ in order_columns:
                    direction = random.choice(["ASC", "DESC"])
                    order_parts.append(f"{col_name} {direction}")

                # Join parts into a single string, ex: "x DESC, y ASC"
                order_str = ", ".join(order_parts)
                mutated_ast = mutated_ast.order_by(order_str, append=False)


            # --- 8. Random LIMIT addition ---
            if not mutated_ast.args.get("limit") and random.random() < 0.3:
                mutated_ast.set("limit", exp.Limit(
                    expression=exp.Literal.number(str(random.randint(1, 50)))
                ))


            # Convert back to SQL
            mutations.append(mutated_ast.sql(dialect="sqlite"))

        return mutations




db = {'t0': {'c0': 'INTEGER', 'c1': 'TEXT', 'c2': 'REAL', 'c15': 'INTEGER'}, 't1': {'c3': 'TEXT', 'c4': 'REAL', 'c5': 'BOOLEAN', 'c16': 'REAL'}, 't2': {'c6': 'REAL', 'c7': 'BOOLEAN', 'c8': 'INTEGER', 'c17': 'TEXT'}, 't3': {'c9': 'BOOLEAN', 'c10': 'INTEGER', 'c11': 'TEXT'}, 't4': {'c12': 'INTEGER', 'c13': 'TEXT', 'c14': 'REAL'}}
g = Generator(db)
sql = "SELECT * FROM t0 WHERE c0 > 5;"
mutations = g.generic_mutation(sql, 10)
# for m in mutations:
    # print(m)