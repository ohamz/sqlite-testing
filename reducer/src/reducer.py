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

def normalize_data_types(query: str) -> str:
    """Normalize SQLite-specific data types to standard SQL types."""
    # SQLite type mappings
    type_mappings = {
        r'NATIVE\s+CHARACTER': 'VARCHAR(255)', 
        r'VARYING\s+CHARACTER': 'VARCHAR(255)', 
        r'NATIVE\s+': '',
        r'VARYING\s+': '',
        r'UNSIGNED\s+BIG\s+INT': 'BIGINT',
        r'BIG\s+INT': 'BIGINT', 
        r'DATETIME': 'TIMESTAMP',
        r'REAL': 'FLOAT',
        r'TEXT': 'VARCHAR(255)',
        r'NUMERIC': 'DECIMAL',
        r'BOOLEAN': 'BOOLEAN'
    }
    
    normalized = query
    for pattern, replacement in type_mappings.items():
        normalized = re.sub(pattern, replacement, normalized, flags=re.IGNORECASE)
    
    return normalized

def clean_query_structure(query: str) -> str:
    """Clean up query structure and remove problematic elements."""
    # Remove multiple consecutive semicolons
    query = re.sub(r';+', ';', query)
    
    # Remove empty statements (just semicolons with whitespace)
    query = re.sub(r';\s*;', ';', query)
    
    # Clean up whitespace
    query = re.sub(r'\s+', ' ', query.strip())
    
    # Remove trailing semicolons before statement separators
    query = re.sub(r';\s*$', '', query.strip())
    
    return query

def safe_parse(query: str) -> List[exp.Expression]:
    """Enhanced SQL parsing with better SQLite support."""
    
    # Step 1: Clean and normalize the query
    query = clean_query_structure(query)
    query = normalize_data_types(query)
    query = preprocess_query(query)
    
    try:
        # Try parsing using SQLite dialect
        result = parse(query, dialect='sqlite')
        if result:
            print("[SUCCESS] Parsed with SQLite dialect")
            return result
    except Exception as e:
        print(f"[WARNING] SQLite parsing failed: {e}")
        
        # Try standard parsing
        try:
            result = parse(query)
            if result:
                print("[SUCCESS] Parsed with standard parser")
                return result
        except Exception as e2:
            print(f"[WARNING] Standard parsing failed: {e2}")
        
        # Enhanced statement-by-statement parsing
        print("[INFO] Attempting enhanced statement-by-statement parsing...")
        return parse_statements_individually(query)

def preprocess_query(query: str) -> str:
    """Enhanced query preprocessing for SQLite-specific syntax."""
    # Handle SQLite-specific INSERT variants
    query = re.sub(r'INSERT\s+OR\s+(IGNORE|REPLACE|FAIL|ABORT)\s+INTO', r'INSERT INTO', query, flags=re.IGNORECASE)
    query = re.sub(r'REPLACE\s+INTO', 'INSERT INTO', query, flags=re.IGNORECASE)
    
    # Handle CREATE variants
    query = re.sub(r'CREATE\s+OR\s+REPLACE\s+VIEW', 'CREATE VIEW', query, flags=re.IGNORECASE)
    query = re.sub(r'CREATE\s+TEMP(ORARY)?\s+VIEW', 'CREATE VIEW', query, flags=re.IGNORECASE)
    query = re.sub(r'CREATE\s+VIRTUAL\s+TABLE', 'CREATE TABLE', query, flags=re.IGNORECASE)
    query = re.sub(r'CREATE\s+TEMP(ORARY)?\s+TABLE', 'CREATE TABLE', query, flags=re.IGNORECASE)
    query = re.sub(r'CREATE\s+TEMP(ORARY)?\s+TRIGGER', 'CREATE TRIGGER', query, flags=re.IGNORECASE)
    query = re.sub(r'CREATE\s+UNIQUE\s+INDEX\s+IF\s+NOT\s+EXISTS', 'CREATE UNIQUE INDEX', query, flags=re.IGNORECASE)
    query = re.sub(r'CREATE\s+INDEX\s+IF\s+NOT\s+EXISTS', 'CREATE INDEX', query, flags=re.IGNORECASE)

    # Handle ALTER TABLE variants
    query = re.sub(r'ALTER\s+TABLE\s+IF\s+EXISTS', 'ALTER TABLE', query, flags=re.IGNORECASE)

    # Handle DELETE variants
    query = re.sub(r'DELETE\s+OR\s+(IGNORE|FAIL|ABORT|ROLLBACK)\s+FROM', 'DELETE FROM', query, flags=re.IGNORECASE)

    # Handle DROP variants
    query = re.sub(r'DROP\s+(TABLE|VIEW|INDEX|TRIGGER)\s+IF\s+EXISTS', r'DROP \1', query, flags=re.IGNORECASE)

    # Handle UPDATE variants
    query = re.sub(r'UPDATE\s+OR\s+(ROLLBACK|ABORT|FAIL|IGNORE|REPLACE)\s+', r'UPDATE ', query, flags=re.IGNORECASE)
    
    # Handle CHECK constraints
    query = re.sub(r'CHECK\s*\(\s*\w+\s*\([^)]*\)\s*[><=!]+\s*[^)]*\)', '', query, flags=re.IGNORECASE)
    query = re.sub(r'CHECK\s*\([^)]+\)', '', query, flags=re.IGNORECASE)
    query = re.sub(r',\s*\)', ')', query)
    
    # Handle PRAGMA statements (convert to comments)
    query = re.sub(r'PRAGMA\s+[^;]+;?', '-- PRAGMA removed', query, flags=re.IGNORECASE)
    
    # Handle REINDEX and ANALYZE statements
    query = re.sub(r'REINDEX\s*[^;]*;?', '-- REINDEX removed', query, flags=re.IGNORECASE)
    query = re.sub(r'ANALYZE\s*[^;]*;?', '-- ANALYZE removed', query, flags=re.IGNORECASE)
    
    # Handle window functions
    query = preprocess_window_functions(query)

    # Handle INDEX expressions
    query = preprocess_index_expressions(query)
    
    return query

def parse_statements_individually(query: str) -> List[exp.Expression]:
    """Enhanced individual statement parsing with trigger support."""
    statements = []
    sql_parts = split_sql_statements_advanced(query)
    
    for i, part in enumerate(sql_parts):
        if not part.strip() or part.strip().startswith('--'):
            continue
            
        success = False
        
        # Special handling for triggers and complex statements
        if re.match(r'CREATE\s+TRIGGER', part, re.IGNORECASE):
            print(f"[INFO] Parsing trigger statement {i+1}")
            parsed_stmt = parse_trigger_statement(part)
            if parsed_stmt:
                statements.append(parsed_stmt)
                success = True

        # Special handling for CREATE VIEW statements
        elif re.match(r'CREATE\s+(OR\s+REPLACE\s+)?((TEMP|TEMPORARY)\s+)?VIEW', part, re.IGNORECASE):
            print(f"[INFO] Parsing CREATE VIEW statement {i+1}")
            parsed_stmt = parse_create_view_statement(part)
            if parsed_stmt:
                statements.append(parsed_stmt)
                success = True

        # Special handling for ALTER TABLE statements  
        elif re.match(r'ALTER\s+TABLE', part, re.IGNORECASE):
            print(f"[INFO] Parsing ALTER TABLE statement {i+1}")
            parsed_stmt = parse_alter_table_statement(part)
            if parsed_stmt:
                statements.append(parsed_stmt)
                success = True

        # Special handling for DELETE statements
        elif re.match(r'DELETE\s+(OR\s+\w+\s+)?FROM', part, re.IGNORECASE):
            print(f"[INFO] Parsing DELETE statement {i+1}")
            parsed_stmt = parse_delete_statement(part)
            if parsed_stmt:
                statements.append(parsed_stmt)
                success = True

        # Special handling for CREATE INDEX statements
        elif re.match(r'CREATE\s+(UNIQUE\s+)?INDEX', part, re.IGNORECASE):
            print(f"[INFO] Parsing CREATE INDEX statement {i+1}")
            parsed_stmt = parse_create_index_statement(part)
            if parsed_stmt:
                statements.append(parsed_stmt)
                success = True

        else:
            # Enhanced parsing attempts with window function support
            parsing_attempts = [
                lambda: parse(part, dialect='sqlite'),
                lambda: parse(part),
                lambda: parse(normalize_data_types(part), dialect='sqlite'),
                lambda: parse_window_function_statement(part) if 'OVER' in part.upper() else None,
                lambda: parse_with_fallback_modifications(part)
            ]
            
            for attempt in parsing_attempts:
                try:
                    result = attempt()
                    if result:
                        if isinstance(result, list):
                            statements.extend(result)
                        else:
                            statements.append(result)
                        success = True
                        break
                except Exception:
                    continue
        
        if success:
            print(f"[SUCCESS] Parsed statement {i+1}")
        else:
            print(f"[WARNING] Failed to parse statement {i+1}: {part[:100]}...")
            # Create placeholder for unparseable statements
            try:
                statements.append(create_command_placeholder(part))
            except:
                pass
    
    if statements:
        print(f"[INFO] Successfully parsed {len(statements)} statements with enhanced method")
        return statements
    else:
        raise Exception("All enhanced parsing methods failed")

def split_sql_statements_advanced(query: str) -> List[str]:
    """Advanced SQL statement splitting with trigger and block support."""
    statements = []
    current_stmt = ""
    paren_depth = 0
    in_string = False
    string_char = None
    in_trigger = False
    trigger_depth = 0
    i = 0
    
    while i < len(query):
        char = query[i]
        
        # Handle string literals
        if char in ("'", '"') and not in_string:
            in_string = True
            string_char = char
        elif char == string_char and in_string:
            if i > 0 and query[i-1] != '\\':
                in_string = False
                string_char = None
        
        if not in_string:
            # Check for trigger start
            if not in_trigger and re.match(r'CREATE\s+TRIGGER', query[i:], re.IGNORECASE):
                in_trigger = True
                trigger_depth = 0
            
            # Handle parentheses and trigger blocks
            if char == '(':
                paren_depth += 1
            elif char == ')':
                paren_depth -= 1
            elif in_trigger:
                if re.match(r'BEGIN\b', query[i:], re.IGNORECASE):
                    trigger_depth += 1
                elif re.match(r'END\b', query[i:], re.IGNORECASE):
                    trigger_depth -= 1
                    if trigger_depth <= 0:
                        in_trigger = False
            
            # Statement termination logic
            if char == ';' and paren_depth == 0 and not in_trigger:
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

def parse_trigger_statement(statement: str) -> Optional[exp.Expression]:
    """Special handling for CREATE TRIGGER statements."""
    try:
        # Try to parse as-is first
        result = parse(statement, dialect='sqlite')
        if result:
            return result[0]
    except:
        pass
    
    # Create a simplified command placeholder for triggers
    print(f"[WARNING] '{statement[:60]}...' contains unsupported syntax. Falling back to parsing as a 'Command'.")
    return create_command_placeholder(statement)

def parse_create_view_statement(statement: str) -> Optional[exp.Expression]:
    """Special handling for CREATE VIEW statements."""
    try:
        # Remove SQLite-specific keywords that might cause issues
        cleaned = re.sub(r'OR\s+REPLACE\s+', '', statement, flags=re.IGNORECASE)
        cleaned = re.sub(r'TEMP(ORARY)?\s+', '', cleaned, flags=re.IGNORECASE)
        
        result = parse(cleaned, dialect='sqlite')
        if result:
            return result[0]
    except Exception as e:
        print(f"[DEBUG] CREATE VIEW parsing failed: {e}")
    
    return create_command_placeholder(statement)

def parse_alter_table_statement(statement: str) -> Optional[exp.Expression]:
    """Special handling for ALTER TABLE statements."""
    try:
        # Remove SQLite-specific keywords
        cleaned = re.sub(r'IF\s+EXISTS\s+', '', statement, flags=re.IGNORECASE)
        
        result = parse(cleaned, dialect='sqlite')
        if result:
            return result[0]
    except Exception as e:
        print(f"[DEBUG] ALTER TABLE parsing failed: {e}")
    
    return create_command_placeholder(statement)

def parse_delete_statement(statement: str) -> Optional[exp.Expression]:
    """Special handling for DELETE statements."""
    try:
        # Remove SQLite-specific OR clauses
        cleaned = re.sub(r'DELETE\s+OR\s+\w+\s+FROM', 'DELETE FROM', statement, flags=re.IGNORECASE)
        
        result = parse(cleaned, dialect='sqlite')
        if result:
            return result[0]
    except Exception as e:
        print(f"[DEBUG] DELETE parsing failed: {e}")
    
    return create_command_placeholder(statement)

def parse_create_index_statement(statement: str) -> Optional[exp.Expression]:
    """Special handling for CREATE INDEX statements with complex expressions."""
    try:
        # First, try to parse as-is
        result = parse(statement, dialect='sqlite')
        if result:
            return result[0]
    except Exception as e:
        print(f"[DEBUG] Direct INDEX parsing failed: {e}")
    
    try:
        # Remove IF NOT EXISTS clause
        cleaned = re.sub(r'IF\s+NOT\s+EXISTS\s+', '', statement, flags=re.IGNORECASE)
        
        # Try to simplify the expression part
        # Find the ON clause and simplify what comes after
        on_match = re.search(r'(CREATE\s+(?:UNIQUE\s+)?INDEX\s+\w+\s+ON\s+\w+)\s*\((.+)\)$', cleaned, re.IGNORECASE | re.DOTALL)
        if on_match:
            prefix = on_match.group(1)
            expr = on_match.group(2)
            
            # Extract simple column references
            columns = re.findall(r'\b(c\d+)\b', expr)
            if columns:
                # Create a simple column list
                simple_expr = ', '.join(columns[:2])  # Use first 2 columns
                simplified = f"{prefix}({simple_expr})"
                
                result = parse(simplified, dialect='sqlite')
                if result:
                    print("[SUCCESS] Parsed with simplified INDEX expression")
                    return result[0]
        
        # If that fails, try even simpler approach - just use first column found
        col_match = re.search(r'\b(c\d+)\b', statement)
        if col_match:
            table_match = re.search(r'ON\s+(\w+)', statement, re.IGNORECASE)
            if table_match:
                index_match = re.search(r'CREATE\s+(?:UNIQUE\s+)?INDEX\s+(\w+)', statement, re.IGNORECASE)
                if index_match:
                    simple_statement = f"CREATE INDEX {index_match.group(1)} ON {table_match.group(1)}({col_match.group(1)})"
                    result = parse(simple_statement, dialect='sqlite')
                    if result:
                        print("[SUCCESS] Parsed with minimal INDEX expression")
                        return result[0]
                        
    except Exception as e:
        print(f"[DEBUG] INDEX simplification failed: {e}")
    
    print(f"[WARNING] Could not parse INDEX statement, creating placeholder")
    return create_command_placeholder(statement)

def parse_with_fallback_modifications(statement: str) -> List[exp.Expression]:
    """Apply modifications to help parse difficult statements."""
    modifications = [
        # Remove SQLite-specific keywords
        lambda s: re.sub(r'IF\s+NOT\s+EXISTS\s+', '', s, flags=re.IGNORECASE),
        lambda s: re.sub(r'OR\s+(ROLLBACK|ABORT|FAIL|IGNORE|REPLACE)', '', s, flags=re.IGNORECASE),
        # Simplify CHECK constraints
        lambda s: re.sub(r'CHECK\s*\([^)]*\)', '', s, flags=re.IGNORECASE),
        # Remove DEFAULT values that might be problematic  
        lambda s: re.sub(r'DEFAULT\s+[^,\)]+', '', s, flags=re.IGNORECASE),
        # Simplify INDEX expressions with complex logic
        lambda s: re.sub(r'CREATE\s+(UNIQUE\s+)?INDEX\s+(\w+)\s+ON\s+(\w+)\s*\([^)]+\)', r'CREATE \1INDEX \2 ON \3(c0)', s, flags=re.IGNORECASE),
        # Normalize data types
        lambda s: normalize_data_types(s),
    ]
    
    for modify in modifications:
        try:
            modified = modify(statement)
            result = parse(modified, dialect='sqlite')
            if result:
                return result
        except:
            continue
    
    return []

def parse_window_function_statement(statement: str) -> Optional[exp.Expression]:
    """Special handling for statements with window functions."""
    try:
        # First try standard parsing
        result = parse(statement, dialect='sqlite')
        if result:
            return result[0]
    except Exception as e:
        print(f"[DEBUG] Standard window function parsing failed: {e}")
    
    # Try with simplified window functions
    simplified = preprocess_window_functions(statement)
    if simplified != statement:
        try:
            result = parse(simplified, dialect='sqlite')
            if result:
                print("[SUCCESS] Parsed with simplified window functions")
                return result[0]
        except Exception as e:
            print(f"[DEBUG] Simplified window function parsing failed: {e}")
    
    # Try removing window function clauses entirely for parsing
    no_window = re.sub(r'OVER\s*\([^)]*\)', '', statement, flags=re.IGNORECASE)
    try:
        result = parse(no_window, dialect='sqlite')
        if result:
            print("[SUCCESS] Parsed without window function clauses")
            return result[0]
    except Exception as e:
        print(f"[DEBUG] No-window parsing failed: {e}")
    
    return None

def create_command_placeholder(statement: str) -> exp.Expression:
    """Create a placeholder for unparseable statements."""
    return exp.Command(this=statement[:100] + "..." if len(statement) > 100 else statement)

def preprocess_window_functions(query: str) -> str:
    """Preprocess window functions to handle complex nested expressions."""
    # Pattern to match window functions with complex expressions
    window_pattern = r'(\w+)\s*\(\s*(.*?)\s*\)\s*OVER\s*\('
    
    def simplify_window_expr(match):
        func_name = match.group(1)
        inner_expr = match.group(2).strip()
        
        # Simplify complex nested expressions in window functions
        if 'UPPER' in inner_expr and '||' in inner_expr:
            # Replace complex string concatenations with simple column references
            simplified = re.sub(r'UPPER\s*\(\s*\([^)]+\|\|[^)]+\)\s*\)', 'col2', inner_expr)
            return f"{func_name}({simplified}) OVER("
        elif inner_expr.count('(') > 2:  # Very nested expression
            # Extract the main column if possible
            col_match = re.search(r't\d+\.(\w+)', inner_expr)
            if col_match:
                return f"{func_name}({col_match.group(1)}) OVER("
        
        return match.group(0)
    
    return re.sub(window_pattern, simplify_window_expr, query, flags=re.IGNORECASE)

def get_referenced_columns(tree: exp.Expression) -> Set[str]:
    """Enhanced column reference detection with better SQLite support."""
    columns = set()
    
    def collect_columns(node):
        if isinstance(node, exp.Column):
            # Handle qualified and unqualified column names
            if hasattr(node, 'name'):
                columns.add(str(node.name).lower())
            elif hasattr(node, 'this'):
                if hasattr(node.this, 'name'):
                    columns.add(str(node.this.name).lower())
                else:
                    columns.add(str(node.this).lower())
        
        # Handle identifier references
        elif isinstance(node, exp.Identifier):
            columns.add(str(node.this).lower())
        
        # Recursively search child nodes
        if hasattr(node, 'args') and node.args:
            for child in node.args.values():
                if isinstance(child, exp.Expression):
                    collect_columns(child)
                elif isinstance(child, list):
                    for item in child:
                        if isinstance(item, exp.Expression):
                            collect_columns(item)
        
        # Handle specific node attributes
        for attr in ['this', 'where', 'having', 'expressions', 'left', 'right']:
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
    """Check if query contains SELECT * with better detection."""
    def check_star(node):
        if isinstance(node, exp.Star):
            return True
        if isinstance(node, exp.Select) and hasattr(node, 'expressions'):
            for expr in node.expressions or []:
                if isinstance(expr, exp.Star):
                    return True
                elif hasattr(expr, 'this') and isinstance(expr.this, exp.Star):
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

def reduce_insert_statements(statements: List[exp.Expression], test_script: str, dry_run: bool = False) -> List[exp.Expression]:
    """Reduce repetitive INSERT statements by removing duplicates and similar values."""
    print("[INFO] Attempting to reduce INSERT statements")
    
    # Group INSERT statements by table
    table_inserts = {}
    other_statements = []
    
    for i, stmt in enumerate(statements):
        if isinstance(stmt, exp.Insert) and hasattr(stmt, 'this'):
            table_name = str(stmt.this).lower()
            if table_name not in table_inserts:
                table_inserts[table_name] = []
            table_inserts[table_name].append((i, stmt))
        else:
            other_statements.append((i, stmt))
    
    reduced_statements = [None] * len(statements)
    
    # Place non-INSERT statements
    for orig_idx, stmt in other_statements:
        reduced_statements[orig_idx] = stmt
    
    # Reduce INSERT statements per table
    for table_name, inserts in table_inserts.items():
        print(f"[INFO] Reducing {len(inserts)} INSERT statements for table {table_name}")
        
        # Try to keep only every nth INSERT to reduce redundancy
        reduction_factors = [2, 3, 4, 5]  # Keep every 2nd, 3rd, etc.
        
        for factor in reduction_factors:
            if len(inserts) <= factor:
                continue
                
            # Keep every nth statement
            kept_inserts = [inserts[i] for i in range(0, len(inserts), factor)]
            
            # Test if reduced set still works
            test_statements = [None] * len(statements)
            
            # Place other statements
            for orig_idx, stmt in other_statements:
                test_statements[orig_idx] = stmt
                
            # Place kept inserts
            for orig_idx, stmt in kept_inserts:
                test_statements[orig_idx] = stmt
            
            # Fill remaining slots with None and filter
            final_test_statements = [stmt for stmt in test_statements if stmt is not None]
            
            if final_test_statements:
                test_query = ";\n".join(stmt.sql() for stmt in final_test_statements) + ";"
                
                if dry_run or run_test(test_query, test_script):
                    print(f"[SUCCESS] Reduced {table_name} INSERTs by factor of {factor}")
                    inserts = kept_inserts
                    break
        
        # Place final inserts
        for orig_idx, stmt in inserts:
            reduced_statements[orig_idx] = stmt
    
    # Filter out None values
    return [stmt for stmt in reduced_statements if stmt is not None]

def reduce_parentheses(query: str, test_script: str, dry_run: bool = False) -> str:
    """Enhanced parentheses reduction with SQLite-aware patterns."""
    print("[INFO] Attempting to reduce unnecessary parentheses")
    current_query = query
    patterns = [
        # Safe patterns with validation
        (r'\(\(([^()]+)\)\)', r'(\1)'),  # Remove double parentheses
        (r'\((\w+)\)', r'\1'),  # Remove parentheses around single identifiers  
        (r'\((\d+(?:\.\d+)?)\)', r'\1'),  # Remove parentheses around numbers
        (r'\((true|false|null)\)', r'\1', re.IGNORECASE),  # Remove parentheses around literals
        (r'\((\-?\d+(?:\.\d+)?)\)', r'\1'),  # Remove parentheses around negative numbers
    ]
    
    reductions = 0
    for pattern_info in patterns:
        pattern = pattern_info[0]
        replacement = pattern_info[1]
        flags = pattern_info[2] if len(pattern_info) > 2 else 0
        
        iteration = 0
        while iteration < 5:  # Limit iterations
            if flags:
                new_query = re.sub(pattern, replacement, current_query, flags=flags)
            else:
                new_query = re.sub(pattern, replacement, current_query)
                
            if new_query == current_query:
                break
            
            if dry_run or run_test(new_query, test_script):
                current_query = new_query
                reductions += 1
                print(f"[SUCCESS] Reduced parentheses")
            else:
                break
            iteration += 1
    
    if reductions > 0:
        print(f"[INFO] Removed {reductions} unnecessary parentheses groups")
    return current_query

def reduce_window_functions(tree: exp.Expression, test_script: str, setup_sql: str, dry_run: bool = False) -> str:
    """Reduce complexity in window functions."""
    print("[INFO] Attempting to reduce window function complexity")
    
    def find_and_simplify_windows(node):
        if hasattr(node, 'args') and node.args:
            for key, child in node.args.items():
                if isinstance(child, exp.Expression):
                    find_and_simplify_windows(child)
                elif isinstance(child, list):
                    for item in child:
                        if isinstance(item, exp.Expression):
                            find_and_simplify_windows(item)
        
        # Look for window functions and try to simplify their expressions
        if isinstance(node, exp.Window):
            # Try to simplify the window expression
            if hasattr(node, 'this') and node.this:
                # For complex expressions, try replacing with simpler equivalents
                if isinstance(node.this, exp.Func):
                    func_name = type(node.this).__name__.lower()
                    if func_name in ['avg', 'sum', 'count']:
                        # Try to simplify the function arguments
                        if hasattr(node.this, 'expressions') and node.this.expressions:
                            for i, expr in enumerate(node.this.expressions):
                                if isinstance(expr, (exp.Func, exp.Concat)):
                                    # Replace complex expressions with simple column reference
                                    simple_col = exp.Column(this=exp.Identifier(this="col2"))
                                    node.this.expressions[i] = simple_col
                                    break
    
    try:
        modified_tree = copy.deepcopy(tree)
        find_and_simplify_windows(modified_tree)
        
        candidate_query = setup_sql + "\n" + modified_tree.sql() + ";" if setup_sql else modified_tree.sql() + ";"
        
        if dry_run or run_test(candidate_query, test_script):
            print("[SUCCESS] Simplified window functions")
            return candidate_query
        else:
            print("[INFO] Window function simplification broke the query, reverting")
    except Exception as e:
        print(f"[ERROR] Window function reduction failed: {e}")
    
    return setup_sql + "\n" + tree.sql() + ";" if setup_sql else tree.sql() + ";"

def preprocess_index_expressions(query: str) -> str:
    """Preprocess complex INDEX expressions to make them parseable."""
    
    def simplify_index_expr(match):
        index_part = match.group(0)
        
        # Extract the table name and column references
        table_match = re.search(r'ON\s+(\w+)', index_part, re.IGNORECASE)
        if not table_match:
            return index_part
            
        table_name = table_match.group(1)
        
        # Find the expression part after ON table_name
        expr_start = index_part.upper().find(f'ON {table_name.upper()}') + len(f'ON {table_name}')
        if expr_start >= len(index_part):
            return index_part
            
        expr_part = index_part[expr_start:].strip()
        
        # If it starts with parentheses, extract the content
        if expr_part.startswith('('):
            # Find all column references
            columns = re.findall(r'\b(c\d+)\b', expr_part)
            if columns:
                # Use the first few columns found as a simple column list
                simple_cols = ', '.join(columns[:3]) 
                return index_part[:expr_start] + f'({simple_cols})'
        
        return index_part
    
    # Match CREATE INDEX statements with complex expressions
    index_pattern = r'CREATE\s+(UNIQUE\s+)?INDEX\s+(\w+\s+)?ON\s+\w+\s*\([^)]*(?:\([^)]*\)[^)]*)*\)'
    return re.sub(index_pattern, simplify_index_expr, query, flags=re.IGNORECASE | re.DOTALL)

def simplify_expressions(tree: exp.Expression, test_script: str, setup_sql: str, dry_run: bool = False) -> str:
    """Enhanced expression simplification with SQLite-specific optimizations."""
    print("[INFO] Attempting to simplify expressions")
    
    def simplify_node(node):
        try:
            # Basic simplifications
            if isinstance(node, exp.Paren) and isinstance(node.this, (exp.Literal, exp.Column, exp.Identifier)):
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
            
            # SQLite-specific simplifications
            elif isinstance(node, exp.Is):
                # Simplify "column IS NULL" patterns
                if isinstance(node.right, exp.Null):
                    return node, False
                    
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
            if dry_run or run_test(candidate_query, test_script):
                print("[SUCCESS] Simplified expressions")
                return candidate_query
            else:
                print("[INFO] Expression simplification broke the query, reverting")
    except Exception as e:
        print(f"[ERROR] Expression simplification failed: {e}")
    
    return setup_sql + "\n" + tree.sql() + ";" if setup_sql else tree.sql() + ";"

def reduce_where_expressions(tree: exp.Expression, test_script: str, setup_sql: str, dry_run: bool = False) -> str:
    """Enhanced WHERE clause reduction with SQLite support."""
    where = tree.find(exp.Where)
    if not where:
        return setup_sql + "\n" + tree.sql() + ";" if setup_sql else tree.sql() + ";"

    current_condition = where.this
    
    # Try simple replacements first
    simple_conditions = [
        exp.Boolean(this=True), 
        exp.Literal.number("1")
    ]
    
    for simple_cond in simple_conditions:
        try:
            where.set("this", simple_cond)
            candidate_query = setup_sql + "\n" + tree.sql() + ";" if setup_sql else tree.sql() + ";"
            if dry_run or run_test(candidate_query, test_script):
                print(f"[SUCCESS] Simplified WHERE to: {simple_cond.sql()}")
                return candidate_query
            else:
                where.set("this", current_condition)
        except Exception as e:
            print(f"[DEBUG] WHERE simplification error: {e}")
            where.set("this", current_condition)

    return setup_sql + "\n" + tree.sql() + ";" if setup_sql else tree.sql() + ";"

def reduce_select_expressions(tree: exp.Expression, test_script: str, setup_sql: str, dry_run: bool = False) -> str:
    """Enhanced SELECT expression reduction with SQLite support."""
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
            expr_name = str(expressions[i]).lower()
        elif hasattr(expressions[i], 'alias'):
            expr_name = str(expressions[i].alias).lower()
        
        if expr_name and expr_name in referenced_columns:
            i += 1
            continue

        # Try removing this expression
        trial_exprs = expressions[:i] + expressions[i+1:]
        if not trial_exprs: 
            i += 1
            continue
            
        try:
            select.set("expressions", trial_exprs)
            candidate_query = setup_sql + "\n" + tree.sql() + ";" if setup_sql else tree.sql() + ";"
            if dry_run or run_test(candidate_query, test_script):
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

def reduce_table_definition(statements: List[exp.Expression], test_script: str, dry_run: bool = False) -> List[exp.Expression]:
    """Enhanced table definition reduction with SQLite support."""
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
                            if hasattr(columns[i], 'this'):
                                col_name = str(columns[i].this).lower()
                        elif hasattr(columns[i], 'name'):
                            col_name = str(columns[i].name).lower()
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
                    if not trial_columns: 
                        i += 1
                        continue
                    
                    try:
                        test_stmt = copy.deepcopy(stmt)
                        test_stmt.this.set("expressions", trial_columns)
                        test_statements = reduced_statements + [test_stmt] + statements[len(reduced_statements)+1:]
                        test_query = ";\n".join(s.sql() for s in test_statements) + ";"

                        if dry_run or run_test(test_query, test_script):
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

def reduce_query(query_path: str, test_script: str, dry_run: bool = False):
    """Main query reduction function with enhanced error handling."""
    print(f"[INFO] Starting query reduction for: {query_path}")
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
        if isinstance(statements[i], (exp.Select, exp.Insert, exp.Update, exp.Delete, exp.Create, exp.Alter, exp.Drop)):
            payload_idx = i
            break
    
    if payload_idx == -1:
        print("[WARNING] No SELECT/DML statement found, processing all statements")
        payload_idx = len(statements) - 1

    # Step 1: Reduce table definitions
    if payload_idx > 0:
        try:
            reduced_statements = reduce_table_definition(statements, test_script, dry_run)
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
        simplified_query = simplify_expressions(payload_statement, test_script, setup_sql, dry_run)
        tracker.record_step("Expression Simplification", simplified_query, "Simplified expressions")
        current_sql = simplified_query
    except Exception as e:
        print(f"[ERROR] Expression simplification failed: {e}")

    # Step 2.1: Reduce window function complexity
    try:
        updated_statements = safe_parse(current_sql)
        if updated_statements:
            payload_statement = updated_statements[payload_idx] if payload_idx < len(updated_statements) else updated_statements[-1]
            setup_sql = ";\n".join(stmt.sql() for stmt in updated_statements[:payload_idx]) + ";" if payload_idx > 0 else ""
        
        window_reduced = reduce_window_functions(payload_statement, test_script, setup_sql, dry_run)
        tracker.record_step("Window Function Reduction", window_reduced, "Simplified window function expressions")
        current_sql = window_reduced
    except Exception as e:
        print(f"[ERROR] Window function reduction failed: {e}")

    # Step 3: Reduce SELECT expressions  
    try:
        updated_statements = safe_parse(current_sql)
        if updated_statements:
            payload_statement = updated_statements[payload_idx] if payload_idx < len(updated_statements) else updated_statements[-1]
            setup_sql = ";\n".join(stmt.sql() for stmt in updated_statements[:payload_idx]) + ";" if payload_idx > 0 else ""
        
        select_reduced = reduce_select_expressions(payload_statement, test_script, setup_sql, dry_run)
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
        
        where_reduced = reduce_where_expressions(payload_statement, test_script, setup_sql, dry_run)
        tracker.record_step("WHERE Expression Reduction", where_reduced, "Simplified WHERE conditions")
        current_sql = where_reduced
    except Exception as e:
        print(f"[ERROR] WHERE reduction failed: {e}")

    # Step 5: Reduce parentheses
    try:
        paren_reduced = reduce_parentheses(current_sql, test_script, dry_run)
        tracker.record_step("Parentheses Reduction", paren_reduced, "Removed unnecessary parentheses")
        current_sql = paren_reduced
    except Exception as e:
        print(f"[ERROR] Parentheses reduction failed: {e}")

    tracker.print_summary()
    print("\n[INFO] Final reduced query:")
    print(current_sql)