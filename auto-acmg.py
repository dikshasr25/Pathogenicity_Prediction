import subprocess
import os
import shutil

# Set up base directory
base_dir = "/mnt/c/Users/blood/Documents/test_sample/jsons_nutrition/Diablo_ACMG"

# Set up environment variables
os.environ["AUTO_ACMG_SEQREPO_DATA_DIR"] = os.path.join(base_dir, "seqrepo/auto-acmg")

def run_command(command, shell=True, check=True):
    """Helper function to run shell commands safely."""
    print(f"🔹 Running: {command}")
    try:
        subprocess.run(command, shell=shell, check=check)
    except subprocess.CalledProcessError as e:
        print(f"Command failed: {e}")
        exit(1)

try:
    # Update and install necessary packages
    run_command("sudo apt-get update")
    run_command("sudo apt-get install -y make build-essential libssl-dev zlib1g-dev "
                "libbz2-dev libreadline-dev libsqlite3-dev wget curl llvm "
                "libncursesw5-dev xz-utils tk-dev libxml2-dev libxmlsec1-dev "
                "libffi-dev liblzma-dev")

    # Remove unused packages
    run_command("sudo apt autoremove -y")
    run_command("sudo apt-get install -y pipenv")

    # Check if pyenv is installed
    pyenv_dir = os.path.expanduser("~/.pyenv")
    if not os.path.exists(pyenv_dir):
        run_command("curl https://pyenv.run | bash")

    # Clone auto-acmg repository if not exists
    if not os.path.exists("auto-acmg"):
        run_command("git clone https://github.com/bihealth/auto-acmg.git")
    os.chdir("auto-acmg")

    # Install Git LFS and pull LFS files
    run_command("git lfs install")
    run_command("git lfs pull")

    seqrepo_dir = os.path.join(base_dir, "seqrepo/auto-acmg")
    if not os.path.exists(seqrepo_dir):
        print(f"Creating seqrepo directory: {seqrepo_dir}")
        os.makedirs(seqrepo_dir, exist_ok=True)

    # Initialize seqrepo only if necessary
    if not os.path.exists(os.path.join(seqrepo_dir, "aliases.sqlite")):
        run_command("pipenv run seqrepo init -i auto-acmg")
    else:
        print("Seqrepo already initialized. Skipping initialization.")

    # Copy environment file and update seqrepo directory
    run_command("cp .env.dev .env")
    run_command(f"sed -i 's|^AUTO_ACMG_SEQREPO_DATA_DIR=.*|AUTO_ACMG_SEQREPO_DATA_DIR={seqrepo_dir}|' .env")

    # Install dependencies using Pipenv
    run_command("pipenv install")

    # Fix biocommons/seqrepo fetchone() issue
    try:
        seqrepo_cli_path = os.path.join(
            subprocess.check_output("pipenv --venv", shell=True, text=True).strip(),
            "lib/python3.12/site-packages/biocommons/seqrepo/cli.py"
        )
        run_command(f"sed -i -e 's/if aliases_cur.fetchone() is not None/if next(aliases_cur, None) is not None/' {seqrepo_cli_path}")
        print("Fix applied to seqrepo/cli.py successfully.")
    except Exception as e:
        print(f"Unable to apply fix for fetchone() issue: {e}")

    # Fetch and load sequences
    sequences = [
        "NC_000001.10", "NC_000002.11", "NC_000003.11", "NC_000004.11", "NC_000005.9",
        "NC_000006.11", "NC_000007.13", "NC_000008.10", "NC_000009.11", "NC_000010.10",
        "NC_000011.9", "NC_000012.11", "NC_000013.10", "NC_000014.8", "NC_000015.9",
        "NC_000016.9", "NC_000017.10", "NC_000018.9", "NC_000019.9", "NC_000020.10",
        "NC_000021.8", "NC_000022.10", "NC_000023.10", "NC_000024.9", "NC_012920.1",
        "NC_000001.11", "NC_000002.12", "NC_000003.12", "NC_000004.12", "NC_000005.10",
        "NC_000006.12", "NC_000007.14", "NC_000008.11", "NC_000009.12", "NC_000010.11",
        "NC_000011.10", "NC_000012.12", "NC_000013.11", "NC_000014.9", "NC_000015.10",
        "NC_000016.10", "NC_000017.11", "NC_000018.10", "NC_000019.10", "NC_000020.11",
        "NC_000021.9", "NC_000022.11", "NC_000023.11", "NC_000024.10", "NC_012920.1"
    ]
    run_command(f"pipenv run seqrepo fetch-load -i auto-acmg -n RefSeq {' '.join(sequences)}")

    # Run the server
    run_command("pipenv run uvicorn src.main:app --host 0.0.0.0 --port 8080 --reload --workers 8")

except subprocess.CalledProcessError as e:
    print(f"Command failed with error: {e}")
except Exception as e:
    print(f"An unexpected error occurred: {e}")
