import pandas as pd
import requests
import json
import sys
import time
import re
from concurrent.futures import ThreadPoolExecutor, as_completed
from requests.adapters import HTTPAdapter
from requests.packages.urllib3.util.retry import Retry

# Detect dataset type based on filename
def detect_dataset_from_filename(filename):
    match = re.search(r'set[12]', filename, re.IGNORECASE)
    return match.group(0).lower() if match else "set1"

# Function to query WinterVar API
def get_variant_json(row, dataset="set1", max_retries=3, backoff_factor=0.3, timeout=5):
    if dataset == "set1":
        chromosome = str(row.get('CHROMOSOME', '')).strip()
        position = str(row.get('CHROMOSOME_POSITION_HG38', '')).strip()
        ref_allele = str(row.get('REFERENCE_ALLELE', '')).strip()
        alt_allele = str(row.get('RISK_ALLELE', '')).strip()
    else:  # Assume dataset is "set2"
        chromosome = str(row.get('chrom', '')).strip()
        position = str(row.get('pos', '')).strip()
        ref_allele = str(row.get('ref_base', '')).strip()
        alt_allele = str(row.get('alt_base', '')).strip()

    # Skip if any required field is missing
    if not all([chromosome, position, ref_allele, alt_allele]):
        return {}

    # Format chromosome
    chromosome = chromosome.replace('chr', '')

    # Construct API URL
    url = f"http://wintervar.wglab.org/api_new.php?queryType=position&chr={chromosome}&pos={position}&ref={ref_allele}&alt={alt_allele}&build=hg38"

    # Setup a session with retry strategy
    session = requests.Session()
    retry_strategy = Retry(
        total=max_retries,
        backoff_factor=backoff_factor,
        status_forcelist=[429, 500, 502, 503, 504],
    )
    adapter = HTTPAdapter(max_retries=retry_strategy)
    session.mount("http://", adapter)
    session.mount("https://", adapter)

    try:
        response = session.get(url, timeout=timeout)
        response.raise_for_status()  
        
        if not response.text.strip():
            return {}

        json_data = response.json()
        return json_data if json_data else {}
    except (requests.exceptions.JSONDecodeError, requests.exceptions.RequestException):
        return {}

# Function to run API queries in parallel
def run_wintervar(input_csv, output_json, max_workers=10):
    print(f"Reading input file: {input_csv}")

    # Detect dataset type from filename
    dataset = detect_dataset_from_filename(input_csv)
    print(f"Detected dataset type: {dataset}")

    try:
        df = pd.read_csv(input_csv, sep='\t', dtype=str)
        df.replace(["nan", "NaN"], "", inplace=True)

        if df.empty:
            print(f"Skipping {input_csv}: File is empty.")
            return
    except pd.errors.EmptyDataError:
        print(f"Skipping {input_csv}: File contains no data.")
        return

    print("Querying WinterVar API using multi-threading...")
    start_time = time.time()

    results = []
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        future_to_row = {executor.submit(get_variant_json, row, dataset): row for _, row in df.iterrows()}
        for future in as_completed(future_to_row):
            result = future.result()
            if result:
                results.append(result)

    # Save JSON output
    with open(output_json, 'w') as json_file:
        json.dump(results, json_file, indent=4)

    elapsed_time = time.time() - start_time
    print(f"WinterVar processing complete. JSON saved to: {output_json}")
    print(f"Execution Time: {elapsed_time:.2f} seconds")

# Main execution
if __name__ == "__main__":
    if len(sys.argv) != 3:
        print("Usage: python intervar.py <input_csv> <output_json>")
        sys.exit(1)

    input_csv = sys.argv[1]
    output_json = sys.argv[2]

    run_wintervar(input_csv, output_json)
