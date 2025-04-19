import csv
import json
import os
import sys
import subprocess
import time

# Function to ensure 'chr' prefix in chromosome notation
def format_chromosome(chrom):
    return chrom if chrom.startswith("chr") else f"chr{chrom}"

# Function to generate HGVS notation
def generate_hgvs(chromosome, position, reference_allele, risk_allele):
    formatted_chromosome = format_chromosome(chromosome)
    return f"{formatted_chromosome}:{position}:{reference_allele}:{risk_allele}"

# Function to fetch JSON data for a batch of HGVS notations
def fetch_json_batch(hgvs_list):
    results = {}
    for hgvs in hgvs_list:
        command = f"curl -X GET 'http://localhost:8080/api/v1/predict/seqvar?variant_name={hgvs}'"
        try:
            result = subprocess.run(command, shell=True, check=True, text=True, capture_output=True)

            if not result.stdout.strip():
                print(f"Warning: Empty response for HGVS {hgvs}")
                results[hgvs] = None
                continue

            results[hgvs] = json.loads(result.stdout)

        except subprocess.CalledProcessError as e:
            print(f"Error: Failed to fetch data for {hgvs}: {e}")
            results[hgvs] = None
        except json.JSONDecodeError as e:
            print(f"Error: JSON decoding failed for {hgvs}: {e}")
            print("Response received:", result.stdout)
            results[hgvs] = None

    return results

# Function to flatten JSON for CSV writing
def flatten_json(json_obj, parent_key='', sep='_'):
    items = []
    for k, v in json_obj.items():
        new_key = f"{parent_key}{sep}{k}" if parent_key else k
        if isinstance(v, dict):
            items.extend(flatten_json(v, new_key, sep=sep).items())
        else:
            items.append((new_key, v))
    return dict(items)

# Function to save results incrementally
def save_results(file_name, data):
    with open(file_name, 'w') as json_file:
        json.dump(data, json_file, indent=4)

# Function to determine set type and extract appropriate columns
def determine_set_type(header):
    if {'CHROMOSOME', 'CHROMOSOME_POSITION_HG38', 'REFERENCE_ALLELE', 'RISK_ALLELE'}.issubset(header):
        return "set1"
    elif {'chrom', 'pos', 'ref_base', 'alt_base'}.issubset(header):
        return "set2"
    else:
        return None

# Main function to process TSV, fetch JSON in batches, and save incrementally
import os
import csv
import json
import sys

def process_tsv(input_tsv, output_json):
    # Check if input file exists and is not empty
    if not os.path.exists(input_tsv) or os.stat(input_tsv).st_size == 0:
        print(f"Input file {input_tsv} is empty or missing. Creating an empty output JSON file.")
        with open(output_json, 'w') as json_file:
            json.dump([], json_file, indent=4)  # Write empty JSON list
        return  # Exit function early

    # Load existing data if present
    existing_data = {}
    if os.path.exists(output_json):
        with open(output_json, 'r') as json_file:
            try:
                existing_data = {row['HGVS']: row for row in json.load(json_file)}
            except json.JSONDecodeError:
                print("Warning: Corrupted JSON detected. Starting fresh.")

    # Step 1: Read TSV file and extract unique HGVS notations
    hgvs_to_row = {}  # Map HGVS to row data
    with open(input_tsv, 'r') as tsv_file:
        reader = csv.DictReader(tsv_file, delimiter='\t')
        header = reader.fieldnames

        if not header:  # Handle files with no header row
            print(f"Error: Input TSV {input_tsv} has no headers. Creating an empty output JSON file.")
            with open(output_json, 'w') as json_file:
                json.dump([], json_file, indent=4)
            return  # Exit early

        set_type = determine_set_type(header)
        if not set_type:
            print("Error: Input TSV does not match expected column names for Set 1 or Set 2.")
            print(f"Found headers: {header}")  # Debugging info
            with open(output_json, 'w') as json_file:
                json.dump([], json_file, indent=4)
            return  # Exit early

        for row in reader:
            if set_type == "set1":
                hgvs = generate_hgvs(row['CHROMOSOME'], row['CHROMOSOME_POSITION_HG38'], row['REFERENCE_ALLELE'], row['RISK_ALLELE'])
            else:  # set2
                hgvs = generate_hgvs(row['chrom'], row['pos'], row['ref_base'], row['alt_base'])

            row['HGVS'] = hgvs
            hgvs_to_row[hgvs] = row

    if not hgvs_to_row:  # If there are no valid rows, create an empty JSON file
        print(f"No valid variants found in {input_tsv}. Creating an empty output JSON file.")
        with open(output_json, 'w') as json_file:
            json.dump([], json_file, indent=4)
        return  # Exit early

    unique_hgvs = list(set(hgvs_to_row.keys()))  # Get unique HGVS

    # Step 2: Remove already processed HGVS
    pending_hgvs = [hgvs for hgvs in unique_hgvs if hgvs not in existing_data]

    print(f"Total unique HGVS: {len(unique_hgvs)}, Pending queries: {len(pending_hgvs)}")

    # Step 3: Process HGVS in batches of 100
    batch_size = 100
    for i in range(0, len(pending_hgvs), batch_size):
        batch = pending_hgvs[i:i + batch_size]
        print(f"Processing batch {i // batch_size + 1}/{(len(pending_hgvs) + batch_size - 1) // batch_size}...")

        # Fetch JSON data for batch
        json_results = fetch_json_batch(batch)

        # Update results and existing data
        for hgvs, json_data in json_results.items():
            row_data = hgvs_to_row[hgvs]
            if json_data:
                flattened_json = flatten_json(json_data)
                row_data.update(flattened_json)

            existing_data[hgvs] = row_data

        # Save progress after every batch
        save_results(output_json, list(existing_data.values()))

        print(f"Saved {len(existing_data)} entries to {output_json}")

    print(f"Processing complete. All data saved as {output_json}.")

# Entry point for running the script
if __name__ == "__main__":
    start_time = time.time()

    if len(sys.argv) != 3:
        print("Usage: python auto-acmg-query.py <input_tsv> <output_json>")
        sys.exit(1)

    input_tsv = sys.argv[1]  # Get input file name from command line
    output_json = sys.argv[2]  # Get output file name from command line

    if not os.path.exists(input_tsv):
        print(f"Error: Input file {input_tsv} not found. Please provide a valid TSV file.")
        sys.exit(1)

    process_tsv(input_tsv, output_json)

    end_time = time.time()
    elapsed_time = end_time - start_time
    print(f"Execution Time: {elapsed_time:.2f} seconds")
