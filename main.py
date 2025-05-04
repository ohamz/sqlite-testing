from scripts import (
    clear_coverage,
    run_queries,
    collect_coverage,
    copy_coverage_files,
    write_results,
)

# Generator for the queries you want to run
def sql_generator():
    yield """
    CREATE TABLE users (
        id INTEGER PRIMARY KEY,
        name TEXT
    );
    """
    yield """
    CREATE TABLE orders (
        id INTEGER PRIMARY KEY,
        user_id INTEGER,
        amount REAL,
        FOREIGN KEY(user_id) REFERENCES users(id)
    );
    """
    yield "INSERT INTO users (name) VALUES ('Alice'), ('Bob'), ('Charlie');"
    yield """
    INSERT INTO orders (user_id, amount) VALUES
        (1, 99.99),
        (1, 14.50),
        (2, 300.00),
        (3, 12.00),
        (3, 48.25);
    """
    yield """
    SELECT users.name, SUM(orders.amount) as total
    FROM users
    JOIN orders ON users.id = orders.user_id
    WHERE orders.amount > 20
    GROUP BY users.name
    HAVING total > 50;
    """

def main():
    server_container = "sqlite3"

    sqlite_dir = "/home/test/sqlite"
    sqlite_binary = "sqlite3"

    new_sqlite_dir = "/usr/bin"
    new_sqlite_binary = "sqlite3-3.39.4"

    queries = "\n".join(sql_generator()) + "\n"
    # queries = "\n".join(gen2()) + "\n"

    clear_coverage(server_container, sqlite_dir)

    stdout, stderr = run_queries(server_container, sqlite_dir, sqlite_binary, queries)

    print("\nExecution finished. Generating coverage...")
    collect_coverage(server_container)
    copy_coverage_files(server_container, sqlite_dir)

    print("\n\nChecking results on new version...")
    stdout_new, stderr_new = run_queries(server_container, new_sqlite_dir, new_sqlite_binary, queries)
    write_results(stdout_new.decode(), stderr_new.decode(), stdout.decode(), stderr.decode())

    if (not stderr.decode() != b""):
        print("> Error detected!")
    if (stdout_new.decode() == stdout.decode()):
        print("> Outputs are the same.")
    else:
        print("> Outputs are different! Check logs.")

    



if __name__ == "__main__":
    main()
