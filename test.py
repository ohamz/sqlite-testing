from sqlglot import parse_one
a= parse_one("SELECT b AS c")
# sqlglot.expressions[0].output_name
print(a.expressions)