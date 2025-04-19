import json
import csv
import os
import sys

def json_to_csv(input_file_path, output_file_path):
    """Converts a JSON file containing a list of dictionaries into a CSV file."""
    
    # Ensure the input JSON file exists and is not empty
    if not os.path.exists(input_file_path) or os.stat(input_file_path).st_size == 0:
        print(f"Warning: Input JSON file '{input_file_path}' is empty or missing. Creating an empty CSV file.")
        with open(output_file_path, 'w', newline='', encoding='utf-8') as tsv_file:
            writer = csv.writer(tsv_file, delimiter='\t')
            writer.writerow([])  # Write an empty file with no headers
        return  # Exit normally

    # Read the JSON file
    try:
        with open(input_file_path, 'r', encoding='utf-8') as json_file:
            data = json.load(json_file)
    except json.JSONDecodeError:
        print(f"Error: Unable to parse JSON file '{input_file_path}'. Creating an empty CSV file.")
        with open(output_file_path, 'w', newline='', encoding='utf-8') as tsv_file:
            writer = csv.writer(tsv_file, delimiter='\t')
            writer.writerow([])  # Write empty file
        return  # Exit normally

    # Ensure the JSON data is a list of dictionaries
    if not isinstance(data, list) or not all(isinstance(item, dict) for item in data):
        print(f"Error: JSON file '{input_file_path}' is not a valid list of dictionaries. Creating an empty CSV file.")
        with open(output_file_path, 'w', newline='', encoding='utf-8') as tsv_file:
            writer = csv.writer(tsv_file, delimiter='\t')
            writer.writerow([])  # Write empty file
        return  # Exit normally

    # Handle empty JSON lists
    if not data:
        print(f"Warning: JSON file '{input_file_path}' is empty. Creating an empty CSV file.")
        with open(output_file_path, 'w', newline='', encoding='utf-8') as tsv_file:
            writer = csv.writer(tsv_file, delimiter='\t')
            writer.writerow([])  # Write empty file
        return  # Exit normally

    # Extract all unique keys from the data (ensuring all columns are included)
    unique_headers = set()
    for item in data:
        unique_headers.update(item.keys())

    # Convert to a sorted list for consistency
    final_headers = sorted(unique_headers)

    # Write to the output CSV file
    with open(output_file_path, 'w', newline='', encoding='utf-8') as tsv_file:
        writer = csv.DictWriter(tsv_file, fieldnames=final_headers, delimiter='\t')
        
        # Write the header row
        writer.writeheader()
        
        # Write each row of data
        for item in data:
            writer.writerow({key: item.get(key, '') for key in final_headers})

    print(f"CSV file saved successfully: {output_file_path}")

if __name__ == "__main__":
    if len(sys.argv) != 3:
        print("Usage: python json_to_csv_auto_acmg.py <input_json_file> <output_csv_file>")
        sys.exit(1)

    input_json = sys.argv[1]
    output_csv = sys.argv[2]

    json_to_csv(input_json, output_csv)
