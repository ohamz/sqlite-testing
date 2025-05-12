import random
from collections import deque
from queue_entry import QueueEntry
from scripts import (
    setup_db,
    clear_coverage,
    run_query,
    collect_coverage,
    copy_coverage_files,
    write_results,
)
# from mutation_techniques import MutationTechnique, mutate_query
from generator import Generator, MutationTechnique


MAX_MUTATIONS = 2
MUTATION_ATTEMPTS = 3

server_container = "sqlite3"

sqlite_dir = "/home/test/sqlite"
sqlite_binary = "sqlite3"

new_sqlite_dir = "/usr/bin"
new_sqlite_binary = "sqlite3-3.39.4"

queue = deque()

def seed_initial_queries():
    return [
        "SELECT * FROM t0 WHERE c0 > 5;"
    ]

    # return [
    #     "CREATE TABLE t10(aa INT, bb INT);",
    #     "CREATE INDEX t1x ON t10( ABS(aa), ABS(bb) );",
    #     "INSERT INTO t10 VALUES(-2,-3), (+2,-3), (-2,+3), (+2,+3);",
    #     "SELECT * FROM t10 WHERE ((ABS(aa)=1 AND 1=2) OR ABS(aa)=2) AND ABS(bb)=3;]",
    # ]


def run_with_coverage(query):
    print(f"Running query: {query}")
    stdout, stderr = run_query(server_container, sqlite_dir, sqlite_binary, query)
    print(f"\n{stderr}\n")
    # print(f"\n{stdout}\n")

    coverage = collect_coverage(server_container)
    print(f"Generating coverage... {coverage}")
    # copy_coverage_files(server_container, sqlite_dir)
    print(coverage)

    print("\n\nChecking results on new version...")
    stdout_new, stderr_new = run_query(server_container, new_sqlite_dir, new_sqlite_binary, query)
    write_results(stdout_new.decode(), stderr_new.decode(), stdout.decode(), stderr.decode())

    if (not stderr.decode() != b""):
        print("> Error detected!")
    if (stdout_new.decode() == stdout.decode()):
        print("> Outputs are the same.")
    else:
        print("> Outputs are different! Check logs.")
    return coverage, stdout_new.decode() == stdout.decode()

def initialize_queue():
    """
    Initialize the queue with initial queries and their coverage.
    """
    initial_queries = seed_initial_queries()
    for q in initial_queries:
        print(f"Running initial query: {q}")
        coverage, _ = run_with_coverage(q)
        entry = QueueEntry(sql=q, cov=coverage)
        queue.append(entry)
        print(f"Initial query coverage: {coverage}")


def main_loop():
    clear_coverage(server_container, sqlite_dir)
    print("Setting up database...")
    db = setup_db(server_container, sqlite_dir, sqlite_binary)
    gen = Generator(db)
    print(collect_coverage(server_container))
    # print()

    initial_queries = seed_initial_queries()
    print("Seeding initial queries...")
    for q in initial_queries:
        coverage, _ = run_with_coverage(q)
    print(f"Initial query coverage: {coverage}")
    # run_with_coverage("SELECT * FROM t0;")
    # print(db)
    # initialize_queue()
    queries_count = 0
    bugs_found = 0

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

        mutated_queries = gen.mutate_query(entry.sql, MutationTechnique.GENERIC, MUTATION_ATTEMPTS)

        for new_sql in mutated_queries:
            coverage, correct = run_with_coverage(new_sql)

            if not correct:
                bugs_found += 1
            elif coverage - entry.new_coverage > 0.05:
                print(f"New coverage: {coverage} (previous: {entry.new_coverage})")
            else:
                continue

            new_entry = QueueEntry(
                sql=new_sql,
                cov=coverage
            )
            queue.append(new_entry)
            queries_count += 1

        entry.mutation_count += 1
        entry.update_coverage(coverage)
        if coverage - entry.new_coverage > 0.05:
            queue.append(entry)  # requeue the parent for future mutations
        

        print(f"Queue size: {len(queue)}")


    print(f"Total queries executed: {queries_count}")
    print(f"Total bugs found: {bugs_found}")

    
if __name__ == "__main__":
    main_loop()
