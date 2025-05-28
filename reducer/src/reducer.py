from sqlglot import parse, exp, tokenize
from src.scripts import run_test
from typing import List, Optional, Set, Tuple, Dict
import copy
import re

class ReductionTracker:
    def __init__(self, initial_query: str):
        self.initial_tokens = self.count_tokens(initial_query)
        self.initial_query = initial_query
        self.steps = []
        self.current_tokens = self.initial_tokens
        
    def count_tokens(self, query: str) -> int:
        try:
            tokens = list(tokenize(query))
            return len(tokens)
        except:
            return len(re.findall(r'\w+|[^\w\s]', query))
    
    def record_step(self, step_name: str, new_query: str, description: str = ""):
        new_tokens = self.count_tokens(new_query)
        tokens_removed = self.current_tokens - new_tokens
        step_info = {
            'step': step_name, 'description': description,
            'tokens_before': self.current_tokens, 'tokens_after': new_tokens,
            'tokens_removed': tokens_removed,
            'reduction_percent': (tokens_removed / self.current_tokens * 100) if self.current_tokens > 0 else 0
        }
        self.steps.append(step_info)
        self.current_tokens = new_tokens
        if tokens_removed > 0:
            print(f"[REDUCTION] {step_name}: -{tokens_removed} tokens ({step_info['reduction_percent']:.1f}%)")
        
    def print_summary(self):
        total_removed = self.initial_tokens - self.current_tokens
        total_percent = (total_removed / self.initial_tokens * 100) if self.initial_tokens > 0 else 0
        print("\n" + "="*60)
        print("REDUCTION SUMMARY")
        print("="*60)
        print(f"Initial tokens: {self.initial_tokens}")
        print(f"Final tokens: {self.current_tokens}")
        print(f"Total removed: {total_removed} ({total_percent:.1f}%)")
        print("\nStep-by-step breakdown:")
        for step in self.steps:
            if step['tokens_removed'] > 0:
                print(f"  {step['step']}: -{step['tokens_removed']} tokens ({step['reduction_percent']:.1f}%)")
                if step['description']:
                    print(f"    {step['description']}")

def safe_parse(query: str) -> List[exp.Expression]:
    """Safely parse SQL with better error handling and fallback strategies."""
    
    # Clean and preprocess query
    query = preprocess_query(query)
    
    try:
        # Try normal parsing first
        result = parse(query)
        if result:
            return result
    except Exception as e:
        print(f"[WARNING] Standard parsing failed: {e}")
        
        # Try parsing with different dialects with specific configurations
        dialect = 'sqlite'
        
        try:
            print(f"[INFO] Trying {dialect} dialect...")
            result = parse(query, dialect=dialect)
            if result:
                print(f"[SUCCESS] Parsed with {dialect} dialect")
                return result
        except Exception as dialect_e:
            print(f"[DEBUG] {dialect} dialect failed: {dialect_e}")
        
        # Enhanced statement-by-statement parsing
        print("[INFO] Attempting enhanced statement-by-statement parsing...")
        return parse_statements_individually(query)

def preprocess_query(query: str) -> str:
    """Preprocess query to handle common parsing issues."""
    # Normalize whitespace
    query = re.sub(r'\s+', ' ', query.strip())
    
    # Handle SQLite-specific syntax that might cause issues
    # Fix UPDATE OR ROLLBACK/ABORT syntax
    query = re.sub(r'UPDATE\s+OR\s+(ROLLBACK|ABORT)\s+', r'UPDATE ', query, flags=re.IGNORECASE)
    
    # Handle INSERT OR variants
    query = re.sub(r'INSERT\s+OR\s+(IGNORE|REPLACE|FAIL|ABORT)\s+INTO', r'INSERT INTO', query, flags=re.IGNORECASE)
    
    # Handle REPLACE INTO (convert to INSERT for parsing)
    query = re.sub(r'REPLACE\s+INTO', 'INSERT INTO', query, flags=re.IGNORECASE)
    
    # Fix potential issues with CHECK constraints
    query = re.sub(r'CHECK\s*\([^)]+\)', '', query, flags=re.IGNORECASE)
    
    return query

def parse_statements_individually(query: str) -> List[exp.Expression]:
    """Parse statements one by one with enhanced error handling."""
    statements = []
    
    # More sophisticated statement splitting
    sql_parts = split_sql_statements(query)
    
    for i, part in enumerate(sql_parts):
        if not part.strip():
            continue
            
        success = False
        parsed_stmt = None
        
        # Try parsing with different approaches
        parsing_attempts = [
            lambda: parse(part),
            lambda: parse(part, dialect='sqlite'),
            lambda: parse(part, dialect='mysql'),
            lambda: parse(part, dialect='postgres'),
            lambda: parse_with_fallback_modifications(part)
        ]
        
        for attempt in parsing_attempts:
            try:
                result = attempt()
                if result:
                    statements.extend(result)
                    parsed_stmt = result[0] if result else None
                    success = True
                    print(f"[SUCCESS] Parsed statement {i+1}")
                    break
            except Exception as e:
                continue
        
        if not success:
            print(f"[WARNING] Failed to parse statement {i+1}: {part[:100]}...")
            # Create a placeholder for unparseable statements
            try:
                statements.append(create_command_placeholder(part))
            except:
                pass
    
    if statements:
        print(f"[INFO] Successfully parsed {len(statements)} statements with enhanced method")
        return statements
    else:
        raise Exception("All enhanced parsing methods failed")

def split_sql_statements(query: str) -> List[str]:
    """Enhanced SQL statement splitting that handles complex cases."""
    # Handle transaction blocks and nested statements
    statements = []
    current_stmt = ""
    paren_depth = 0
    in_string = False
    string_char = None
    i = 0
    
    while i < len(query):
        char = query[i]
        
        # Handle string literals
        if char in ("'", '"') and not in_string:
            in_string = True
            string_char = char
        elif char == string_char and in_string:
            # Check for escaped quotes
            if i > 0 and query[i-1] != '\\':
                in_string = False
                string_char = None
        
        if not in_string:
            if char == '(':
                paren_depth += 1
            elif char == ')':
                paren_depth -= 1
            elif char == ';' and paren_depth == 0:
                if current_stmt.strip():
                    statements.append(current_stmt.strip())
                current_stmt = ""
                i += 1
                continue
        
        current_stmt += char
        i += 1
    
    if current_stmt.strip():
        statements.append(current_stmt.strip())
    
    return statements

def parse_with_fallback_modifications(statement: str) -> List[exp.Expression]:
    """Apply various modifications to help parsing difficult statements."""
    modifications = [
        # Remove SQLite-specific syntax
        lambda s: re.sub(r'IF\s+NOT\s+EXISTS\s+', '', s, flags=re.IGNORECASE),
        lambda s: re.sub(r'OR\s+(ROLLBACK|ABORT|FAIL|IGNORE|REPLACE)', '', s, flags=re.IGNORECASE),
        # Simplify CHECK constraints
        lambda s: re.sub(r'CHECK\s*\([^)]*\)', '', s, flags=re.IGNORECASE),
        # Remove DEFAULT values that might be problematic
        lambda s: re.sub(r'DEFAULT\s+[^,\)]+', '', s, flags=re.IGNORECASE),
    ]
    
    for modify in modifications:
        try:
            modified = modify(statement)
            result = parse(modified)
            if result:
                return result
        except:
            continue
    
    return []

def create_command_placeholder(statement: str) -> exp.Expression:
    """Create a placeholder for unparseable statements."""
    return exp.Command(this=statement[:100] + "..." if len(statement) > 100 else statement)

def get_referenced_columns(tree: exp.Expression) -> Set[str]:
    """Enhanced column reference detection."""
    columns = set()
    
    def collect_columns(node):
        if isinstance(node, exp.Column):
            # Handle both simple and qualified column names
            if hasattr(node, 'name'):
                columns.add(node.name.lower())
            elif hasattr(node, 'this') and hasattr(node.this, 'name'):
                columns.add(node.this.name.lower())
            elif hasattr(node, 'this'):
                columns.add(str(node.this).lower())
        
        # Recursively search all child nodes
        if hasattr(node, 'args') and node.args:
            for child in node.args.values():
                if isinstance(child, exp.Expression):
                    collect_columns(child)
                elif isinstance(child, list):
                    for item in child:
                        if isinstance(item, exp.Expression):
                            collect_columns(item)
        
        # Handle specific node types
        for attr in ['this', 'where', 'having', 'expressions']:
            if hasattr(node, attr):
                child = getattr(node, attr)
                if isinstance(child, exp.Expression):
                    collect_columns(child)
                elif isinstance(child, list):
                    for item in child:
                        if isinstance(item, exp.Expression):
                            collect_columns(item)
    
    try:
        collect_columns(tree)
    except Exception as e:
        print(f"[DEBUG] Error collecting columns: {e}")
    
    return columns

def has_select_star(tree: exp.Expression) -> bool:
    """Check if query contains SELECT *"""
    def check_star(node):
        if isinstance(node, exp.Star):
            return True
        if hasattr(node, 'expressions') and node.expressions:
            for expr in node.expressions:
                if isinstance(expr, exp.Star) or (isinstance(expr, exp.Expression) and check_star(expr)):
                    return True
        if hasattr(node, 'args') and node.args:
            for child in node.args.values():
                if isinstance(child, exp.Expression) and check_star(child):
                    return True
                elif isinstance(child, list):
                    for item in child:
                        if isinstance(item, exp.Expression) and check_star(item):
                            return True
        return False
    return check_star(tree)

def reduce_parentheses(query: str, test_script: str, mode: str, dry_run: bool = False) -> str:
    """Enhanced parentheses reduction with better safety checks."""
    print("[INFO] Attempting to reduce unnecessary parentheses")
    current_query = query
    patterns = [
        # Safe patterns with validation
        (r'\(\(([^()]+)\)\)', r'(\1)'),  # Remove double parentheses
        (r'\((\w+)\)', r'\1'),  # Remove parentheses around single identifiers  
        (r'\((\d+(?:\.\d+)?)\)', r'\1'),  # Remove parentheses around numbers
        (r'\((true|false|null)\)', r'\1', re.IGNORECASE),  # Remove parentheses around literals
    ]
    
    reductions = 0
    for pattern_info in patterns:
        pattern = pattern_info[0]
        replacement = pattern_info[1]
        flags = pattern_info[2] if len(pattern_info) > 2 else 0
        
        iteration = 0
        while iteration < 10:  # Prevent infinite loops
            if flags:
                new_query = re.sub(pattern, replacement, current_query, flags=flags)
            else:
                new_query = re.sub(pattern, replacement, current_query)
                
            if new_query == current_query:
                break
            
            if dry_run or run_test(new_query, test_script, mode):
                current_query = new_query
                reductions += 1
                print(f"[SUCCESS] Reduced parentheses")
            else:
                break
            iteration += 1
    
    if reductions > 0:
        print(f"[INFO] Removed {reductions} unnecessary parentheses groups")
    return current_query

def simplify_expressions(tree: exp.Expression, test_script: str, mode: str, setup_sql: str, dry_run: bool = False) -> str:
    """Enhanced expression simplification with better error handling."""
    print("[INFO] Attempting to simplify expressions")
    
    def simplify_node(node):
        try:
            # Basic simplifications
            if isinstance(node, exp.Paren) and isinstance(node.this, (exp.Literal, exp.Column)):
                return node.this, True
            elif isinstance(node, exp.Neg) and isinstance(node.this, exp.Neg):
                return node.this.this, True
            elif isinstance(node, exp.Not) and isinstance(node.this, exp.Not):
                return node.this.this, True
            elif isinstance(node, exp.Not) and isinstance(node.this, exp.Boolean):
                return exp.Boolean(this=not node.this.this), True
            
            # Arithmetic simplifications
            elif isinstance(node, exp.Add):
                if isinstance(node.right, exp.Literal) and str(node.right.this) == "0":
                    return node.left, True
                elif isinstance(node.left, exp.Literal) and str(node.left.this) == "0":
                    return node.right, True
            elif isinstance(node, exp.Mul):
                if isinstance(node.right, exp.Literal) and str(node.right.this) == "1":
                    return node.left, True
                elif isinstance(node.left, exp.Literal) and str(node.left.this) == "1":
                    return node.right, True
                elif isinstance(node.right, exp.Literal) and str(node.right.this) == "0":
                    return exp.Literal.number("0"), True
                elif isinstance(node.left, exp.Literal) and str(node.left.this) == "0":
                    return exp.Literal.number("0"), True
        except Exception as e:
            print(f"[DEBUG] Simplification error: {e}")
        
        return node, False
    
    def traverse_and_simplify(node):
        if not isinstance(node, exp.Expression):
            return node, False
            
        changed = False
        try:
            # Traverse children first
            if hasattr(node, 'args') and node.args:
                for key, child in list(node.args.items()):
                    if isinstance(child, exp.Expression):
                        try:
                            new_child, child_changed = traverse_and_simplify(child)
                            if child_changed:
                                node.set(key, new_child)
                                changed = True
                        except Exception as e:
                            print(f"[DEBUG] Child traversal error: {e}")
                            continue
                    elif isinstance(child, list):
                        for i, item in enumerate(child):
                            if isinstance(item, exp.Expression):
                                try:
                                    new_item, item_changed = traverse_and_simplify(item)
                                    if item_changed:
                                        child[i] = new_item
                                        changed = True
                                except Exception as e:
                                    print(f"[DEBUG] List item traversal error: {e}")
                                    continue
        except Exception as e:
            print(f"[DEBUG] Node traversal error: {e}")
        
        # Try to simplify current node
        try:
            new_node, node_changed = simplify_node(node)
            if node_changed:
                return new_node, True
        except Exception as e:
            print(f"[DEBUG] Node simplification error: {e}")
        
        return node, changed
    
    try:
        simplified_tree, was_changed = traverse_and_simplify(tree)
        if was_changed:
            candidate_query = setup_sql + "\n" + simplified_tree.sql() + ";" if setup_sql else simplified_tree.sql() + ";"
            if dry_run or run_test(candidate_query, test_script, mode):
                print("[SUCCESS] Simplified expressions")
                return candidate_query
            else:
                print("[INFO] Expression simplification broke the query, reverting")
    except Exception as e:
        print(f"[ERROR] Expression simplification failed: {e}")
    
    return setup_sql + "\n" + tree.sql() + ";" if setup_sql else tree.sql() + ";"

def reduce_where_expressions(tree: exp.Expression, test_script: str, mode: str, setup_sql: str, dry_run: bool = False) -> str:
    """Enhanced WHERE clause reduction."""
    where = tree.find(exp.Where)
    if not where:
        return setup_sql + "\n" + tree.sql() + ";" if setup_sql else tree.sql() + ";"

    current_condition = where.this
    
    # Try simple replacements first
    simple_conditions = [
        exp.Boolean(this=True), 
        exp.Boolean(this=False), 
        exp.Literal.number("1"), 
        exp.Literal.number("0")
    ]
    
    for simple_cond in simple_conditions:
        try:
            where.set("this", simple_cond)
            candidate_query = setup_sql + "\n" + tree.sql() + ";" if setup_sql else tree.sql() + ";"
            if dry_run or run_test(candidate_query, test_script, mode):
                print(f"[SUCCESS] Simplified WHERE to: {simple_cond.sql()}")
                return candidate_query
            else:
                where.set("this", current_condition)
        except Exception as e:
            print(f"[DEBUG] WHERE simplification error: {e}")
            where.set("this", current_condition)

    # Try to reduce complex conditions
    try:
        if isinstance(current_condition, (exp.And, exp.Or)):
            conditions = []
            if hasattr(current_condition, 'expressions') and current_condition.expressions:
                conditions = list(current_condition.expressions)
            else:
                # Handle binary operators
                conditions = [current_condition.left, current_condition.right] if hasattr(current_condition, 'left') else []
            
            if conditions:
                print(f"[INFO] Attempting to reduce {len(conditions)} WHERE conditions")
                i = 0
                while i < len(conditions):
                    trial_conditions = conditions[:i] + conditions[i+1:]
                    if trial_conditions:
                        if len(trial_conditions) == 1:
                            new_condition = trial_conditions[0]
                        else:
                            new_condition = exp.and_(*trial_conditions) if isinstance(current_condition, exp.And) else exp.or_(*trial_conditions)
                    else:
                        new_condition = exp.Boolean(this=True)
                    
                    try:
                        where.set("this", new_condition)
                        candidate_query = setup_sql + "\n" + tree.sql() + ";" if setup_sql else tree.sql() + ";"
                        if dry_run or run_test(candidate_query, test_script, mode):
                            print(f"[SUCCESS] Removed WHERE condition at index {i}")
                            conditions = trial_conditions
                            current_condition = new_condition
                        else:
                            where.set("this", current_condition)
                            i += 1
                    except Exception as e:
                        print(f"[DEBUG] WHERE condition reduction error: {e}")
                        where.set("this", current_condition)
                        i += 1
    except Exception as e:
        print(f"[DEBUG] WHERE reduction error: {e}")

    return setup_sql + "\n" + tree.sql() + ";" if setup_sql else tree.sql() + ";"

def reduce_select_expressions(tree: exp.Expression, test_script: str, mode: str, setup_sql: str, dry_run: bool = False) -> str:
    """Enhanced SELECT expression reduction."""
    select = tree.find(exp.Select)
    if not select or has_select_star(tree):
        return setup_sql + "\n" + tree.sql() + ";" if setup_sql else tree.sql() + ";"

    expressions = list(select.expressions) if select.expressions else []
    if not expressions:
        return setup_sql + "\n" + tree.sql() + ";" if setup_sql else tree.sql() + ";"
        
    print(f"[INFO] Attempting to reduce {len(expressions)} SELECT expressions")
    referenced_columns = get_referenced_columns(tree)
    
    i = 0
    while i < len(expressions):
        # Check if this expression is referenced elsewhere
        expr_name = None
        if isinstance(expressions[i], exp.Column):
            expr_name = expressions[i].name.lower() if hasattr(expressions[i], 'name') else str(expressions[i]).lower()
        elif hasattr(expressions[i], 'alias'):
            expr_name = expressions[i].alias.lower()
        
        if expr_name and expr_name in referenced_columns:
            i += 1
            continue

        # Try removing this expression
        trial_exprs = expressions[:i] + expressions[i+1:]
        if not trial_exprs:  # Don't remove all expressions
            i += 1
            continue
            
        try:
            select.set("expressions", trial_exprs)
            candidate_query = setup_sql + "\n" + tree.sql() + ";" if setup_sql else tree.sql() + ";"
            if dry_run or run_test(candidate_query, test_script, mode):
                print(f"[SUCCESS] Removed SELECT expression at index {i}")
                expressions = trial_exprs
            else:
                select.set("expressions", expressions)
                i += 1
        except Exception as e:
            print(f"[DEBUG] SELECT reduction error: {e}")
            select.set("expressions", expressions)
            i += 1

    return setup_sql + "\n" + tree.sql() + ";" if setup_sql else tree.sql() + ";"

def reduce_table_definition(statements: List[exp.Expression], test_script: str, mode: str, dry_run: bool = False) -> List[exp.Expression]:
    """Enhanced table definition reduction with better column detection."""
    all_referenced_columns = set()
    has_star = False
    
    # Collect all referenced columns from all statements
    for stmt in statements:
        try:
            all_referenced_columns.update(get_referenced_columns(stmt))
            if has_select_star(stmt):
                has_star = True
        except Exception as e:
            print(f"[DEBUG] Error analyzing statement: {e}")
    
    print(f"[INFO] Detected referenced columns: {all_referenced_columns}")
    if has_star:
        print("[INFO] Query contains SELECT *, preserving all table columns")
    
    reduced_statements = []
    for stmt in statements:
        try:
            if isinstance(stmt, exp.Create) and hasattr(stmt, 'this') and isinstance(stmt.this, exp.Schema):
                columns = list(stmt.this.expressions) if stmt.this.expressions else []
                print(f"[INFO] Attempting to reduce {len(columns)} table columns")
                
                i = 0
                while i < len(columns):
                    col_name = None
                    try:
                        if isinstance(columns[i], exp.ColumnDef):
                            if hasattr(columns[i], 'this') and hasattr(columns[i].this, 'name'):
                                col_name = columns[i].this.name.lower()
                            elif hasattr(columns[i], 'this'):
                                col_name = str(columns[i].this).lower()
                        elif hasattr(columns[i], 'name'):
                            col_name = columns[i].name.lower()
                        elif hasattr(columns[i], 'this'):
                            col_name = str(columns[i].this).lower()
                    except Exception as e:
                        print(f"[DEBUG] Error extracting column name: {e}")
                    
                    # Keep column if it's referenced or we have SELECT *
                    if (col_name and col_name in all_referenced_columns) or has_star:
                        i += 1
                        continue

                    # Try removing this column
                    trial_columns = columns[:i] + columns[i+1:]
                    if not trial_columns:  # Don't remove all columns
                        i += 1
                        continue
                    
                    try:
                        test_stmt = copy.deepcopy(stmt)
                        test_stmt.this.set("expressions", trial_columns)
                        test_statements = reduced_statements + [test_stmt] + statements[len(reduced_statements)+1:]
                        test_query = ";\n".join(s.sql() for s in test_statements) + ";"

                        if dry_run or run_test(test_query, test_script, mode):
                            print(f"[SUCCESS] Removed column: {col_name}")
                            columns = trial_columns
                        else:
                            i += 1
                    except Exception as e:
                        print(f"[DEBUG] Column removal test error: {e}")
                        i += 1
                
                stmt.this.set("expressions", columns)
        except Exception as e:
            print(f"[DEBUG] Table definition reduction error: {e}")
            
        reduced_statements.append(stmt)
    return reduced_statements

def reduce_query(query_path: str, test_script: str, mode: str, dry_run: bool = False):
    """Main query reduction function with enhanced error handling."""
    try:
        with open(query_path) as f:
            full_sql = f.read()
    except Exception as e:
        print(f"[ERROR] Failed to read query file: {e}")
        return

    tracker = ReductionTracker(full_sql)
    print(f"[INFO] Initial query has {tracker.initial_tokens} tokens")

    try:
        statements = safe_parse(full_sql)
    except Exception as e:
        print(f"[ERROR] Failed to parse query: {e}")
        return

    if len(statements) < 1:
        print("[WARNING] No valid statements found")
        return

    print(f"[INFO] Successfully parsed {len(statements)} statements")
    current_sql = full_sql

    # Find the last meaningful statement as payload
    payload_idx = -1
    for i in range(len(statements)-1, -1, -1):
        if isinstance(statements[i], (exp.Select, exp.Insert, exp.Update, exp.Delete)):
            payload_idx = i
            break
    
    if payload_idx == -1:
        print("[WARNING] No SELECT/DML statement found, processing all statements")
        payload_idx = len(statements) - 1

    # Step 1: Reduce table definitions
    if payload_idx > 0:
        try:
            reduced_statements = reduce_table_definition(statements, test_script, mode, dry_run)
            new_sql = ";\n".join(stmt.sql() for stmt in reduced_statements) + ";"
            tracker.record_step("Table Definition Reduction", new_sql, "Removed unused table columns")
            current_sql = new_sql
            statements = reduced_statements
        except Exception as e:
            print(f"[ERROR] Table definition reduction failed: {e}")

    # Process payload statement
    setup_statements = statements[:payload_idx] if payload_idx > 0 else []
    payload_statement = statements[payload_idx]
    setup_sql = ";\n".join(stmt.sql() for stmt in setup_statements) + ";" if setup_statements else ""
    
    # Step 2: Expression simplification
    try:
        simplified_query = simplify_expressions(payload_statement, test_script, mode, setup_sql, dry_run)
        tracker.record_step("Expression Simplification", simplified_query, "Simplified expressions")
        current_sql = simplified_query
    except Exception as e:
        print(f"[ERROR] Expression simplification failed: {e}")

    # Step 3: Reduce SELECT expressions  
    try:
        updated_statements = safe_parse(current_sql)
        if updated_statements:
            payload_statement = updated_statements[payload_idx] if payload_idx < len(updated_statements) else updated_statements[-1]
            setup_sql = ";\n".join(stmt.sql() for stmt in updated_statements[:payload_idx]) + ";" if payload_idx > 0 else ""
        
        select_reduced = reduce_select_expressions(payload_statement, test_script, mode, setup_sql, dry_run)
        tracker.record_step("SELECT Expression Reduction", select_reduced, "Removed unnecessary SELECT expressions")
        current_sql = select_reduced
    except Exception as e:
        print(f"[ERROR] SELECT reduction failed: {e}")

    # Step 4: Reduce WHERE expressions
    try:
        updated_statements = safe_parse(current_sql)
        if updated_statements:
            payload_statement = updated_statements[payload_idx] if payload_idx < len(updated_statements) else updated_statements[-1]
            setup_sql = ";\n".join(stmt.sql() for stmt in updated_statements[:payload_idx]) + ";" if payload_idx > 0 else ""
        
        where_reduced = reduce_where_expressions(payload_statement, test_script, mode, setup_sql, dry_run)
        tracker.record_step("WHERE Expression Reduction", where_reduced, "Simplified WHERE conditions")
        current_sql = where_reduced
    except Exception as e:
        print(f"[ERROR] WHERE reduction failed: {e}")

    # Step 5: Reduce parentheses
    try:
        paren_reduced = reduce_parentheses(current_sql, test_script, mode, dry_run)
        tracker.record_step("Parentheses Reduction", paren_reduced, "Removed unnecessary parentheses")
        current_sql = paren_reduced
    except Exception as e:
        print(f"[ERROR] Parentheses reduction failed: {e}")

    tracker.print_summary()
    print("\n[INFO] Final reduced query:")
    print(current_sql)