import random
from collections import deque
from queue_entry import QueueEntry
from scripts import (
    clear_coverage,
    run_queries,
    collect_coverage,
    copy_coverage_files,
    write_results,
)
from mutation_techniques import MutationTechnique, mutate_query


MAX_MUTATIONS = 10
MUTATION_ATTEMPTS = 3

queue = deque()

def seed_initial_queries():
    return [
        "CREATE TABLE t1(a INT);",
        "INSERT INTO t1 VALUES (1);",
        "SELECT * FROM t1 WHERE a > 0;"
    ]


def run_with_coverage(query):
    return

def initialize_queue():
    """
    Initialize the queue with initial queries and their coverage."""
    initial_queries = seed_initial_queries()
    for q in initial_queries:
        coverage = run_with_coverage(q)
        entry = QueueEntry(sql=q, new_coverage=coverage)
        queue.append(entry)


def main_loop():
    while queue:
        entry = queue.popleft()

        if entry.mutation_count >= MAX_MUTATIONS:
            # TODO: add more discarding criteria (not only coverage)
            if not entry.has_new_coverage():
                continue

            # Coverage increased, reset mutation count for additional mutations
            entry.reset_mutation_count()

        technique = random.choice(list(MutationTechnique))
        entry.last_technique = technique

        mutated_queries = mutate_query(entry.sql, technique, MUTATION_ATTEMPTS)

        for new_sql in mutated_queries:
            coverage = run_with_coverage(new_sql)
            if coverage is None:
                continue  # likely invalid

            new_entry = QueueEntry(
                sql=new_sql,
                mutation_count=0,
                prev_coverage=None,
                new_coverage=coverage,
                last_technique=technique
            )
            queue.append(new_entry)

        entry.mutation_count += 1
        queue.append(entry)  # requeue the parent for future mutations



# # Generator for the queries you want to run
# def sql_generator():
#     yield """
#     CREATE TABLE users (
#         id INTEGER PRIMARY KEY,
#         name TEXT
#     );
#     """
#     yield """
#     CREATE TABLE orders (
#         id INTEGER PRIMARY KEY,
#         user_id INTEGER,
#         amount REAL,
#         FOREIGN KEY(user_id) REFERENCES users(id)
#     );
#     """
#     yield "INSERT INTO users (name) VALUES ('Alice'), ('Bob'), ('Charlie');"
#     yield """
#     INSERT INTO orders (user_id, amount) VALUES
#         (1, 99.99),
#         (1, 14.50),
#         (2, 300.00),
#         (3, 12.00),
#         (3, 48.25);
#     """
#     yield """
#     SELECT users.name, SUM(orders.amount) as total
#     FROM users
#     JOIN orders ON users.id = orders.user_id
#     WHERE orders.amount > 20
#     GROUP BY users.name
#     HAVING total > 50;
#     """

# def gen2():
#     yield """
#     CREATE TABLE t1(a INT);
#     """
#     yield """
#     INSERT INTO t1 VALUES(1);
#     """
#     yield """
#     CREATE TABLE t2(b INT);
#     """
#     yield """
#     SELECT a, b 
#     FROM t1 
#     LEFT JOIN t2 ON true 
#     WHERE (b IS NOT NULL) IS false;
#     """


# def main():
#     server_container = "sqlite3"

#     sqlite_dir = "/home/test/sqlite"
#     sqlite_binary = "sqlite3"

#     new_sqlite_dir = "/usr/bin"
#     new_sqlite_binary = "sqlite3-3.39.4"

#     # queries = "\n".join(sql_generator()) + "\n"
#     queries = "\n".join(gen2()) + "\n"
#     # print(queries)

#     clear_coverage(server_container, sqlite_dir)

#     stdout, stderr = run_queries(server_container, sqlite_dir, sqlite_binary, queries)

#     print("\nExecution finished. Generating coverage...")
#     collect_coverage(server_container)
#     copy_coverage_files(server_container, sqlite_dir)

#     print("\n\nChecking results on new version...")
#     stdout_new, stderr_new = run_queries(server_container, new_sqlite_dir, new_sqlite_binary, queries)
#     write_results(stdout_new.decode(), stderr_new.decode(), stdout.decode(), stderr.decode())

#     if (not stderr.decode() != b""):
#         print("> Error detected!")
#     if (stdout_new.decode() == stdout.decode()):
#         print("> Outputs are the same.")
#     else:
#         print("> Outputs are different! Check logs.")

    
if __name__ == "__main__":
    main()
