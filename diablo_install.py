import subprocess
import sys
import os

def run_command(command):
    """Runs a shell command and prints its output."""
    try:
        result = subprocess.run(command, shell=True, check=True, capture_output=True, text=True)
        print(result.stdout)
        if result.stderr:
            print(result.stderr)
    except subprocess.CalledProcessError as e:
        print(f"Command failed: {command}")
        print(e.stderr)

def install_opencravat():
    """Installs OpenCravat and required modules."""
    print("Installing OpenCravat...")
    run_command("pip install open-cravat")
    
    print("Installing required OpenCravat modules...")
    modules = [
        "clinvar", "dbsnp", "fathmm", "fathmm_mkl", "genocanyon", "gerp", "gnomad3",
        "interpro", "lrt", "metalr", "metasvm", "mutation_assessor", "mutationtaster",
        "omim", "polyphen2", "provean", "sift", "siphy", "spliceai", "hpo"
    ]
    run_command(f"oc module install {' '.join(modules)}")
    run_command("oc module install-base")

def clone_diablo_acmg():
    """Clones the Diablo ACMG repository."""
    repo_url = "https://github.com/TheSergeyPixel/Diablo_ACMG.git"
    print("Cloning Diablo ACMG...")
    run_command(f"git clone {repo_url}")

def setup_conda_environment():
    """Sets up the Conda environment using the provided .yml file."""
    repo_path = "Diablo_ACMG"
    yml_path = os.path.join(repo_path, "diablo_annotate.yml")
    
    if not os.path.exists(yml_path):
        print(f"Error: {yml_path} not found.")
        sys.exit(1)

    print("Creating Conda environment from YAML file...")
    run_command(f"conda env create -f {yml_path}")

def main():
    print("Setting up Diablo ACMG...")
    
    install_opencravat()
    clone_diablo_acmg()
    setup_conda_environment()

    print("Diablo ACMG installation complete.")

if __name__ == "__main__":
    main()
