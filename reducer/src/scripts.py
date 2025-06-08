import subprocess
import tempfile
import os

def run_test(query: str, test_script: str) -> bool:
    path = write_temp_query(query)
    result = subprocess.run(["bash", test_script, path])
    os.unlink(path)
    return result.returncode == 0

def write_temp_query(query: str) -> str:
    with tempfile.NamedTemporaryFile(mode='w', delete=False, suffix=".sql") as f:
        f.write(query)
        return f.name
