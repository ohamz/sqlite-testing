import subprocess
import os
import re
import tempfile
from schema import Database, ColumnType, ConstraintType

TEMP_DB_PATH = "/home/test/test.db"

def setup_db(container_name, sqlite_dir, sqlite_binary, db_path=TEMP_DB_PATH, local_path="bugs/test.db"):
    # Step 1: Remove file only if it exists
    subprocess.run([
        "docker", "exec", container_name,
        "sh", "-c", f"if [ -f {db_path} ]; then rm {db_path}; fi"
    ], capture_output=True)

    # Step 2: Create a fresh new SQLite database file
    result = subprocess.run([
        "docker", "exec", container_name,
        "sh", "-c", f"cd {sqlite_dir} && ./{sqlite_binary} {db_path} ''"
    ], capture_output=True)

    if result.returncode != 0:
        print("Error creating DB:")
        print(result.stderr.decode())

    # Step 3: Create fixed schema
    db = Database()
    fixed_types = [ColumnType.INTEGER, ColumnType.TEXT, ColumnType.REAL, ColumnType.BOOLEAN]
    fixed_constraints = [ ConstraintType.PRIMARY_KEY, ConstraintType.NOT_NULL, ConstraintType.UNIQUE, ConstraintType.DEFAULT, ConstraintType.CHECK]

    for i in range(5):
        table = db.create_table()

        # Fixed 3 columns per table
        for j in range(3):
            col_type = fixed_types[(i+j) % len(fixed_types)]
            col_constraint = fixed_constraints[(i+j) % len(fixed_constraints)]
            col = table.create_column(col_type)
            col.add_constraint(col_constraint)

        # Add 4 rows of data
        for _ in range(400):
            table.create_row()

        # Add index: UNIQUE for last 2 tables
        index_col = table.columns[0]
        table.create_index(index_col, unique=(i >= 3))

    # Add 1 extra column to tables 0, 1, 2
    db.tables[0].create_extra_column(ColumnType.INTEGER)
    db.tables[1].create_extra_column(ColumnType.REAL)
    db.tables[2].create_extra_column(ColumnType.TEXT)

    sql = db.to_sql()
    stdout, stderr = run_query(container_name, sqlite_dir, sqlite_binary, sql, db_path=db_path)

    result = subprocess.run([
        "docker", "cp", f"{container_name}:{db_path}", local_path
    ], capture_output=True)

    if result.returncode != 0:
        print("Error copying DB from container:")
        print(result.stderr.decode())
    else:
        print(f"Database copied to {local_path}")

    # return db
    return db.to_json()

# Deletes all .gcda files before running
def clear_coverage(container_name, sqlite_dir):
    subprocess.run([
        "docker", "exec", container_name,
        "find", sqlite_dir, "-name", "*.gcda", "-delete"
    ])

# Run SQLite binary & generate .gcda files
def run_query(container_name, sqlite_dir, sqlite_binary, query, db_path=TEMP_DB_PATH):
    result = subprocess.run([
        "docker", "exec", "-i", container_name,
        "sh", "-c", f"cd {sqlite_dir} && ./{sqlite_binary} {db_path}"
    ], input=query.encode(), capture_output=True)

    return result.stdout.strip(), result.stderr.strip()

# Run the coverage results
def collect_coverage(container_name):
    result = subprocess.run([
        "docker", "exec", container_name,
        "sh", "-c", "gcov sqlite/sqlite3-sqlite3.gcda"
    ], check=True, capture_output=True, text=True)

    match = re.search(r"Lines executed:([\d.]+)%", result.stdout)
    if match:
        percent = float(match.group(1))
    else:
        print("Error: Could not record coverage.")
    return percent

# Copy the query to the container and then back to the local machine
def export_query_to_local(sql_query, container_name, i, type, local_dir = "bugs", container_tmp_dir = "/tmp"):
    filename = f"bug{i}.sql" if type == 'logical' else f"crash{i}.sql"
    local_path = f"{local_dir}/{filename}"
    container_path = f"{container_tmp_dir}/{filename}"

    # Create a temporary file locally
    with tempfile.NamedTemporaryFile("w", delete=False) as tmp:
        tmp.write(sql_query)
        tmp.flush()
        tmp_path = tmp.name

    # Copy it to the container
    result_cp_in = subprocess.run([
        "docker", "cp", tmp_path, f"{container_name}:{container_path}"
    ], capture_output=True)

    if result_cp_in.returncode != 0:
        print("Error copying query into container:")
        print(result_cp_in.stderr.decode())
        return

    # 3. Copy from container to the desired local directory
    result_cp_out = subprocess.run([
        "docker", "cp", f"{container_name}:{container_path}", local_path
    ], capture_output=True)

    if result_cp_out.returncode != 0:
        print("Error copying query to local machine:")
        print(result_cp_out.stderr.decode())
        return

    print(f"Query exported to {local_path}")
