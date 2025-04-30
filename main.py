import subprocess

# Generator for the queries you want to run
def sql_generator():
    yield "CREATE TABLE test (id INTEGER PRIMARY KEY, name TEXT);"
    yield "INSERT INTO test (name) VALUES ('Alice');"
    yield "SELECT * FROM test;"

# Runs queries on a given SQLite version
def run_queries_on_version(version_path: str):
    print(f"Testing with SQLite at {version_path}:\n")

    # Join all queries into one input
    queries = "\n".join(sql_generator()) + "\n"

    result = subprocess.run(
        ['docker', 'exec', '-i', 'sqlite3', version_path, ':memory:'],
        input=queries.encode(),
        capture_output=True
    )

    if result.stdout:
        print("Output:", result.stdout.decode().strip())
    if result.stderr:
        print("Error:", result.stderr.decode().strip())

if __name__ == "__main__":
    # Test both versions
    run_queries_on_version("/usr/bin/sqlite3-3.26.0")
    # run_queries_on_version("/usr/bin/sqlite3-3.39.4")