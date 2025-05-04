import subprocess
import os

# Deletes all .gcda files before running
def clear_coverage(container_name, sqlite_dir):
    subprocess.run([
        "docker", "exec", container_name,
        "find", sqlite_dir, "-name", "*.gcda", "-delete"
    ], check=True)

# Run SQLite binary & generate .gcda files
def run_queries(container_name, sqlite_dir, sqlite_binary, queries):
    result = subprocess.run([
        "docker", "exec", "-i", container_name,
        "sh", "-c", f"cd {sqlite_dir} && ./{sqlite_binary} :memory:"
    ], input=queries.encode(), check=True, capture_output=True)

    return result.stdout.strip(), result.stderr.strip()

# Run the coverage results
def collect_coverage(container_name):
    subprocess.run([
        "docker", "exec", container_name,
        "sh", "-c", "gcov sqlite/sqlite3-sqlite3.gcda"
    ], check=True)

def copy_coverage_files(container_name, sqlite_dir, dest_dir="coverage"):
    os.makedirs(dest_dir, exist_ok=True)

    subprocess.run([
        "docker", "cp",
        f"{container_name}:{sqlite_dir}/sqlite3-sqlite3.gcda",
        f"{dest_dir}/sqlite3-sqlite3.gcda"
    ], check=True)

    subprocess.run([
        "docker", "cp",
        f"{container_name}:{sqlite_dir}/sqlite3.c.gcov",
        f"{dest_dir}/sqlite3.c.gcov"
    ], check=True)

def write_results(stdout_new, stderr_new, stdout_old, stderr_old):
    os.makedirs("out", exist_ok=True)

    # Write results to files
    with open("out/new_version.txt", "w") as f:
        f.write("STDOUT:\n" + stdout_new + "\n\nSTDERR:\n" + stderr_new)

    with open("out/old_version.txt", "w") as f:
        f.write("STDOUT:\n" + stdout_old + "\n\nSTDERR:\n" + stderr_old)