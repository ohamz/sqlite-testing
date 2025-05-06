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

def mutate_query(sql: str, technique: MutationTechnique, count: int) -> List[str]:
    """Dispatches to the appropriate mutation technique."""
    if technique == MutationTechnique.PQS:
        return pivoted_query_synthesis(sql, count)
    elif technique == MutationTechnique.TLP:
        return ternary_logic_partitioning(sql, count)
    elif technique == MutationTechnique.EET:
        return equivalent_expression_transformation(sql, count)
    elif technique == MutationTechnique.GENERIC:
        return generic_mutation(sql, count)
    else:
        return []
    

def pivoted_query_synthesis(sql: str, count: int) -> List[str]:
    return []  # TODO

def ternary_logic_partitioning(sql: str, count: int) -> List[str]:
    return []  # TODO

def equivalent_expression_transformation(sql: str, count: int) -> List[str]:
    return []  # TODO

def generic_mutation(sql: str, count: int) -> List[str]:
    """
    Perform basic generic mutations using sqlglot.
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

        # --- 1. Mutate literals (numbers and strings) ---
        for literal in mutated_ast.find_all(exp.Literal):
            if literal.is_number:
                new_num = random.randint(1, 100)
                literal.replace(exp.Literal.number(str(new_num)))
            elif literal.is_string:
                new_str = random.choice(["'foo'", "'bar'", "'pivot'", "'test'"])
                literal.replace(exp.Literal.string(new_str))

        # --- 2. Randomly flip comparison operators ---
        for comp in mutated_ast.find_all(exp.Condition):
            if hasattr(comp, "left") and hasattr(comp, "right"):
                new_op_cls = random.choice(comparison_operators)
                comp.replace(new_op_cls(this=comp.left, expression=comp.right))

        # --- 3. Random logic mutation in WHERE clause ---
        where = mutated_ast.find(exp.Where)
        if where:
            if random.random() < 0.4:
                # Add AND TRUE or OR FALSE
                op_cls = random.choice([exp.And, exp.Or])
                if random.random() < 0.35:
                    bool_val = random.choice(["TRUE", "FALSE"])
                    new_condition = op_cls(
                        this=where.this,
                        expression=exp.Literal(this=bool_val, is_string=False)
                    )
                else:
                    expr = exp.Is(
                        this=copy.deepcopy(where.find(exp.Column)),
                        expression= exp.Null(),
                        # negated=True
                    )
                    # null_check = is_expr if random.random() < 0.5 else exp.Not(this=is_expr)
                    new_condition = op_cls(this=where.this, expression=expr)
                
                where.set("this", new_condition)

            if random.random() < 0.15:
                # Remove WHERE clause entirely
                mutated_ast.set("where", None)

        # --- 4. Random LIMIT addition ---
        if not mutated_ast.args.get("limit") and random.random() < 0.4:
            mutated_ast.set("limit", exp.Limit(
                expression=exp.Literal.number(str(random.randint(1, 10)))
            ))

        # Convert back to SQL
        mutations.append(mutated_ast.sql(dialect="sqlite"))

    return mutations

query = "SELECT * FROM t1 WHERE a > 5;"
mutations = generic_mutation(query, 10)
print("Original SQL:")
print(f"${query}\n")   
print("Mutated SQL Queries:")
for mutation in mutations:
    print(mutation)
