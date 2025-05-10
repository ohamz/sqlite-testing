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
        comparison_operators = [exp.EQ, exp.NEQ, exp.GT, exp.LT, exp.GTE, exp.LTE]

        for _ in range(count):
            mutated_ast = copy.deepcopy(original_ast)

            # --- 1. Replace table and SELECT * or project subset of columns ---
            select = mutated_ast.find(exp.Select)
            table_expr = mutated_ast.find(exp.Table)

            if select:
                # --- Pick a new random table from schema ---
                new_table = random.choice(list(self.schema.keys()))
                table_expr = exp.Table(this=new_table)
                mutated_ast.set("from", exp.From(this=table_expr))

                # --- Get all columns from the new table ---
                new_columns = list(self.schema[new_table].keys())

                # --- Replace SELECT * or existing columns ---
                if random.random() < 0.3:
                    # Keep SELECT * (randomly)
                    select.set("expressions", [exp.Star()])
                else:
                    # Use a random subset of columns
                    col_subset = random.sample(new_columns, k=random.randint(1, len(new_columns)))
                    select.set("expressions", [exp.Column(this=col) for col in col_subset])


            # --- 2. Mutate literals using type-aware replacements ---
            for literal in mutated_ast.find_all(exp.Literal):
                if literal.is_number:
                    new_num = random.randint(1, 100)
                    literal.replace(exp.Literal.number(str(new_num)))
                elif literal.is_string:
                    new_str = random.choice(["'foo'", "'bar'", "'pivot'", "'test'"])
                    literal.replace(exp.Literal.string(new_str))

            # --- 3. Flip comparison operators ---
            columns = list(self.schema[table_expr.this].items())

            for comp in mutated_ast.find_all(exp.Condition):
                # Skip if left or right is a trivial literal (TRUE, FALSE, NULL)
                literals_to_skip = {"TRUE", "FALSE", "NULL"}

                left = comp.args.get("this")
                right = comp.args.get("expression")

                if (
                    isinstance(left, exp.Literal) and left.this.upper() in literals_to_skip
                ) or (
                    isinstance(right, exp.Literal) and right.this.upper() in literals_to_skip
                ):
                    continue  # Skip this condition

                if left and right:
                    col_name, _ = random.choice(columns)
                    new_op_cls = random.choice(comparison_operators)
                    comp.replace(new_op_cls(this=exp.Column(this=col_name), expression=right))

            # --- 4. Add smart WHERE logic using schema ---
            where = mutated_ast.find(exp.Where)
            print(f"WHERE: {where}")
            if where:
                if random.random() < 0.4:
                    # Add AND/OR TRUE/FALSE
                    op_cls = random.choice([exp.And, exp.Or])
                    if random.random() < 0.35:
                        bool_val = random.choice(["TRUE", "FALSE"])
                        new_condition = op_cls(
                            this=where.this,
                            expression=exp.Literal(this=bool_val, is_string=False)
                        )
                    else:
                        col_name, _ = random.choice(columns)
                        expr = exp.Is(
                            this=col_name,
                            expression= exp.Null(),
                            # negated=True
                        )
                        # null_check = is_expr if random.random() < 0.5 else exp.Not(this=is_expr)
                        new_condition = op_cls(this=where.this, expression=expr)
                    
                    where.set("this", new_condition)
            # if table_expr and table_expr.this in self.schema:
            #     columns = list(self.schema[table_expr.this].items())
            #     if columns:
            #         print(f"Table: {table_expr}")
            #         print(f"Columns: {columns}")
            #         col_name, col_type = random.choice(columns)
            #         col_expr = exp.Column(this=col_name)

            #         if col_type == "INTEGER":
            #             right = exp.Literal.number(str(random.randint(1, 100)))
            #         elif col_type == "REAL":
            #             right = exp.Literal.number(str(round(random.uniform(1.0, 10.0), 2)))
            #         elif col_type == "TEXT":
            #             right = exp.Literal.string(random.choice(["pivot", "abc", "test"]))
            #         elif col_type == "BOOLEAN":
            #             right = exp.Literal.number(str(random.choice([0, 1])))
            #         else:
            #             continue

            #         condition = exp.EQ(this=col_expr, expression=right)

            #         where = exp.Where(this=condition)
            #         print(f"WHERE: {where}")
            #         # if where:
            #         #     # Add condition with AND
            #         #     if random.random() < 0.5:
            #         #         new_condition = exp.And(this=where.this, expression=condition)
            #         #         where.set("this", new_condition)
            #         if where:
            #             if random.random() < 0.4:
            #                 # Add AND TRUE or OR FALSE
            #                 op_cls = random.choice([exp.And, exp.Or])
            #                 if random.random() < 0.35:
            #                     bool_val = random.choice(["TRUE", "FALSE"])
            #                     new_condition = op_cls(
            #                         this=where.this,
            #                         expression=exp.Literal(this=bool_val, is_string=False)
            #                     )
            #                 else:
            #                     expr = exp.Is(
            #                         this=copy.deepcopy(where.find(exp.Column)),
            #                         expression= exp.Null(),
            #                         # negated=True
            #                     )
            #                     # null_check = is_expr if random.random() < 0.5 else exp.Not(this=is_expr)
            #                     new_condition = op_cls(this=where.this, expression=expr)
                            
            #                 where.set("this", new_condition)
            #         #     else:
            #         #         # Replace it entirely
            #         #         where.set("this", condition)
            #         # else:
            #         #     mutated_ast.set("where", exp.Where(this=condition))

            # --- 5. Random LIMIT addition ---
            if not mutated_ast.args.get("limit") and random.random() < 0.25:
                mutated_ast.set("limit", exp.Limit(
                    expression=exp.Literal.number(str(random.randint(1, 10)))
                ))

            # Convert back to SQL
            mutations.append(mutated_ast.sql(dialect="sqlite"))

        return mutations


