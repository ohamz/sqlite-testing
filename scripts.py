import subprocess
import os
import re
# import random
from schema import Database, ColumnType, ConstraintType

TEMP_DB_PATH = "/tmp/temp.db"

def setup_db(container_name, sqlite_dir, sqlite_binary, db_path=TEMP_DB_PATH):
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
    # else:
    #     print(f"Fresh SQLite DB created at {db_path}")

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
    if stderr:
        # print("[ERROR SETUP]", stderr.decode() if hasattr(stderr, 'decode') else stderr)
        print("[ERROR IN SETUP]")
    else:
        print("[SETUP SUCCESS]")

    # return db_path
    return db.to_json()

# Deletes all .gcda files before running
def clear_coverage(container_name, sqlite_dir):
    subprocess.run([
        "docker", "exec", container_name,
        "find", sqlite_dir, "-name", "*.gcda", "-delete"
    ], check=True)

# Run SQLite binary & generate .gcda files
def run_query(container_name, sqlite_dir, sqlite_binary, query, db_path=TEMP_DB_PATH):
    result = subprocess.run([
        "docker", "exec", "-i", container_name,
        "sh", "-c", f"cd {sqlite_dir} && ./{sqlite_binary} {db_path}"
    ], input=query.encode(), capture_output=True) #, check=True)

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


def copy_coverage_files(container_name, sqlite_dir, dest_dir="coverage"):
    os.makedirs(dest_dir, exist_ok=True)

    subprocess.run([
        "docker", "cp",
        f"{container_name}:{sqlite_dir}/sqlite3-sqlite3.gcda",
        f"{dest_dir}/sqlite3-sqlite3.gcda"
    ], check=True)

    # subprocess.run([
    #     "docker", "cp",
    #     f"{container_name}:{sqlite_dir}/sqlite3.c.gcov",
    #     f"{dest_dir}/sqlite3.c.gcov"
    # ], check=True)

def write_results(stdout_new, stderr_new, stdout_old, stderr_old):
    os.makedirs("out", exist_ok=True)

    # Write results to files
    with open("out/new_version.txt", "w") as f:
        f.write("STDOUT:\n" + stdout_new + "\n\nSTDERR:\n" + stderr_new)

    with open("out/old_version.txt", "w") as f:
        f.write("STDOUT:\n" + stdout_old + "\n\nSTDERR:\n" + stderr_old)