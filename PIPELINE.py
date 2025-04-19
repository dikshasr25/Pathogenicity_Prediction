import subprocess
import sys
import time
import os
import json

# Define test directory for temporary files
TEST_DIR = "test"
os.makedirs(TEST_DIR, exist_ok=True)
TIMING_LOG = "pipeline_timing.json"

def load_timing_data():
    if os.path.exists(TIMING_LOG):
        with open(TIMING_LOG, "r") as f:
            return json.load(f)
    return {}

def save_timing_data(data):
    with open(TIMING_LOG, "w") as f:
        json.dump(data, f, indent=4)

def estimate_runtime(num_rows):
    """Estimate runtime based on historical data and row count."""
    data = load_timing_data()
    
    # Basic linear estimation
    estimated_time = num_rows * 0.05
    
    # If historical data is available, use a weighted average
    if str(num_rows) in data:
        historical_time = data[str(num_rows)]
        estimated_time = (estimated_time * 0.7) + (historical_time * 0.3)
    
    return max(10.0, round(estimated_time, 2))  # Ensure a minimum runtime of 10 seconds


def log_execution_time(num_rows, elapsed_time):
    data = load_timing_data()
    data[num_rows] = elapsed_time
    save_timing_data(data)

def count_rows_in_file(filepath):
    try:
        with open(filepath, "r") as f:
            return sum(1 for _ in f) - 1  # Exclude header
    except Exception:
        return 0

def run_command(command, cwd=None):
    """Executes a shell command inside a specified directory (if provided)."""
    print(f"Executing: {command}")
    subprocess.run(command, shell=True, check=True, cwd=cwd)

def start_auto_acmg_server():
    """Starts the Auto-ACMG server, handling port conflicts if necessary."""
    print("Starting Auto-ACMG Server...")

    auto_acmg_dir = "auto-acmg"
    if not os.path.exists(auto_acmg_dir):
        print("Error: auto-acmg directory not found.")
        sys.exit(1)

    env = os.environ.copy()
    env["PIPENV_PIPFILE"] = os.path.abspath(os.path.join(auto_acmg_dir, "Pipfile"))
    log_path = os.path.join(auto_acmg_dir, "auto_acmg.log")

    try:
        # Ensure dependencies are installed
        subprocess.run(["pipenv", "install"], cwd=auto_acmg_dir, env=env, check=True)

        # Start server in a new terminal and log output
        command = (
            "nohup pipenv run uvicorn src.main:app --host 0.0.0.0 --port 8080 --reload > auto_acmg.log 2>&1 &"
        )
        subprocess.run(command, shell=True, cwd=auto_acmg_dir, env=env, check=True)
        print("Auto-ACMG Server started. Waiting for confirmation...")

        startup_success = False

        for _ in range(30):  # Check for 30 seconds
            with open(log_path, "r", errors="ignore") as log_file:
                logs = log_file.read()

                if "Application startup complete." in logs:
                    print("Auto-ACMG Server is running.")
                    startup_success = True
                    break

                if "[Errno 98] Address already in use" in logs:
                    print("Error: Port 8080 is already in use. Resolving...")

                    # Find and kill the process using port 8080
                    try:
                        result = subprocess.run(
                            "lsof -i :8080 | awk 'NR>1 {print $2}'",
                            shell=True,
                            capture_output=True,
                            text=True,
                            check=True,
                        )
                        pids = result.stdout.strip().split("\n")
                        for pid in pids:
                            if pid.isdigit():
                                print(f"Killing process {pid} on port 8080...")
                                subprocess.run(["kill", "-9", pid], check=True)
                        print("Restarting Auto-ACMG Server...")
                        return start_auto_acmg_server()  # Restart function after killing processes

                    except subprocess.CalledProcessError as e:
                        print(f"Failed to resolve port issue: {e}")
                        sys.exit(1)

            time.sleep(1)

        if not startup_success:
            print("Error: Auto-ACMG Server did not start properly. Check auto_acmg.log for details.")
            sys.exit(1)

    except subprocess.CalledProcessError as e:
        print(f"Error starting Auto-ACMG: {e}")
        sys.exit(1)


def ensure_file_exists(filepath, command, cwd=None):
    """Checks if a file exists, and runs the provided command if it's missing."""
    if not os.path.exists(filepath):
        print(f"{filepath} missing, running command to generate it...")
        run_command(command, cwd)
    else:
        print(f"{filepath} already exists, skipping command.")

def main(input_vcf, final_output, annotated_vcf=None):
    print("Starting pipeline...")

    # Estimate and display runtime
    num_rows = count_rows_in_file(input_vcf)
    estimated_time = estimate_runtime(num_rows)
    print(f"Estimated runtime: {estimated_time:.2f} seconds")

    base_name = os.path.splitext(os.path.basename(input_vcf))[0]
    annotated_diablo = os.path.join(TEST_DIR, f"{base_name}_diablo.tsv")

    start_time = time.time()

    ensure_file_exists(annotated_diablo, f"time python Diablo_annotate.py -i {input_vcf} -o {annotated_diablo}")

    merged_variants = os.path.join(TEST_DIR, f"{base_name}_merged_set1.tsv")
    pathogenic_variants = os.path.join(TEST_DIR, f"{base_name}_pathogenic_set2.tsv")

    if annotated_vcf:
        ensure_file_exists(merged_variants, f"python merge_files.py {annotated_vcf} {annotated_diablo} {pathogenic_variants} {merged_variants}")
    else:
        ensure_file_exists(pathogenic_variants, f"python merge_files.py {annotated_diablo} {pathogenic_variants}")

    wintervar_set1_json = os.path.join(TEST_DIR, f"{base_name}_wintervar_set1.json")
    wintervar_set2_json = os.path.join(TEST_DIR, f"{base_name}_wintervar_set2.json")

    if annotated_vcf:
        ensure_file_exists(wintervar_set1_json, f"python intervar.py {merged_variants} {wintervar_set1_json}")
    ensure_file_exists(wintervar_set2_json, f"python intervar.py {pathogenic_variants} {wintervar_set2_json}")

    intervar_set1_csv = os.path.join(TEST_DIR, f"{base_name}_intervar_set1.tsv")
    intervar_set2_csv = os.path.join(TEST_DIR, f"{base_name}_intervar_set2.tsv")
    merged_set1_intervar = os.path.join(TEST_DIR, f"{base_name}_merged_set1_intervar.tsv")
    merged_set2_intervar = os.path.join(TEST_DIR, f"{base_name}_merged_set2_intervar.tsv")

    if annotated_vcf:
        ensure_file_exists(merged_set1_intervar, f"python json_to_csv_intervar.py {wintervar_set1_json} {intervar_set1_csv} {merged_variants} {merged_set1_intervar} set1")
    ensure_file_exists(merged_set2_intervar, f"python json_to_csv_intervar.py {wintervar_set2_json} {intervar_set2_csv} {pathogenic_variants} {merged_set2_intervar} set2")

    # Start Auto-ACMG server
    start_auto_acmg_server()

    auto_acmg_set1_json = os.path.join(TEST_DIR, f"{base_name}_auto_acmg_set1.json")
    auto_acmg_set2_json = os.path.join(TEST_DIR, f"{base_name}_auto_acmg_set2.json")
    auto_acmg_set1_csv = os.path.join(TEST_DIR, f"{base_name}_auto_acmg_set1.tsv")
    auto_acmg_set2_csv = os.path.join(TEST_DIR, f"{base_name}_auto_acmg_set2.tsv")

    if annotated_vcf:
        ensure_file_exists(auto_acmg_set1_json, f"pipenv run python auto-acmg-query.py ../{merged_set1_intervar} ../{auto_acmg_set1_json}", cwd="auto-acmg")
    ensure_file_exists(auto_acmg_set2_json, f"pipenv run python auto-acmg-query.py ../{merged_set2_intervar} ../{auto_acmg_set2_json}", cwd="auto-acmg")

    if annotated_vcf:
        ensure_file_exists(auto_acmg_set1_csv, f"python json_csv_auto_cmg.py {auto_acmg_set1_json} {auto_acmg_set1_csv}")
    ensure_file_exists(auto_acmg_set2_csv, f"python json_csv_auto_cmg.py {auto_acmg_set2_json} {auto_acmg_set2_csv}")

    if not annotated_vcf:
        run_command(f"python final_acmg_classifier.py DUMMY {auto_acmg_set2_csv} {final_output}")
    else:
        ensure_file_exists(final_output, f"python final_acmg_classifier.py {auto_acmg_set1_csv} {auto_acmg_set2_csv} {final_output}")

    end_time = time.time()
    elapsed_time = end_time - start_time
    log_execution_time(num_rows, elapsed_time)
    print(f"Pipeline execution completed in {elapsed_time:.2f} seconds! Final output: {final_output}")

if __name__ == "__main__":
    if len(sys.argv) not in [3, 4]:
        print("Usage: python pipeline.py <input_vcf> [<annotated_vcf>] <final_output>")
        sys.exit(1)

    input_vcf = sys.argv[1]
    final_output = sys.argv[-1]
    annotated_vcf = sys.argv[2] if len(sys.argv) == 4 else None

    main(input_vcf, final_output, annotated_vcf)
