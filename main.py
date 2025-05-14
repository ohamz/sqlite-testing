import random
from collections import deque
from queue_entry import QueueEntry
from scripts import (
    setup_db,
    clear_coverage,
    run_query,
    collect_coverage,
    export_query_to_local,
    copy_coverage_files,
    write_results
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

    is_logical = False
    is_crash = False
    if (stderr.decode() == b"Segmentation fault\n"):
        print("> Error detected!")
        is_crash = True
    elif (stdout_new.decode() == stdout.decode()):
        print("> Outputs are the same.")
    else:
        print("> Outputs are different! Check logs.")
        is_logical = True
    return coverage, is_logical, is_crash

def initialize_queue():
    """
    Initialize the queue with initial queries and their coverage.
    """
    initial_queries = seed_initial_queries()
    for q in initial_queries:
        print(f"Running initial query: {q}")
        coverage, _, _ = run_with_coverage(q)
        entry = QueueEntry(sql=q, cov=coverage)
        queue.append(entry)
        print(f"Initial query coverage: {coverage}")


def main_loop():
    clear_coverage(server_container, sqlite_dir)
    print("Setting up database...")
    db = setup_db(server_container, sqlite_dir, sqlite_binary)
    gen = Generator(db)
    print(collect_coverage(server_container))

    # initial_queries = seed_initial_queries()
    # print("Seeding initial queries...")
    # for q in initial_queries:
    #     coverage, _ = run_with_coverage(q)
    # print(f"Initial query coverage: {coverage}")
    # sql = "SELECT * FROM t4 CROSS JOIN t1 ON c13 > t1.c3 WHERE c12 <> NOT TRUE GROUP BY c5, c3 HAVING c12 >= 73 ORDER BY c14 ASC, c12 DESC LIMIT 58;"
    # run_with_coverage(sql)
    # export_query_to_local(sql, server_container, 0)

    print(db)
    initialize_queue()
    queries_count = 0
    bugs_found = 0
    crashes_found = 0

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
            coverage, bug, crash = run_with_coverage(new_sql)

            if bug:
                export_query_to_local(new_sql, server_container, bugs_found, 'logical')
                bugs_found += 1
            elif crash:
                export_query_to_local(new_sql, server_container, crashes_found, 'crash')
                crashes_found += 1
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
        print(f"Queries executed: {queries_count}")
        print(f"Bugs found: {bugs_found}")
        print(f"Crashes found: {crashes_found}")


    print(f"Total queries executed: {queries_count}")
    print(f"Total bugs found: {bugs_found}")

    
if __name__ == "__main__":
    main_loop()
