import argparse
from src.reducer import reduce_query

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--query', required=True)
    parser.add_argument('--test', required=True)
    parser.add_argument('--mode', required=True, choices=['crash', 'diff'])
    parser.add_argument('--dry-run', action='store_true', help="Skip test script and apply all reductions")
    args = parser.parse_args()

    reduce_query(
        query_path=args.query,
        test_script=args.test,
        mode=args.mode,
        dry_run=args.dry_run
    )

if __name__ == "__main__":
    main()
