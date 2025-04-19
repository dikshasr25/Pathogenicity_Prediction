import json
import pandas as pd
import sys

import pandas as pd
import json
import os

def json_to_csv(json_file, output_csv):
    """
    Converts a JSON file to a CSV file while flattening the data.
    If the JSON file is empty or missing, an empty CSV file is created.
    """
    # Check if JSON file is empty or missing
    if not os.path.exists(json_file) or os.stat(json_file).st_size == 0:
        print(f"Warning: {json_file} is empty or missing. Creating an empty CSV file.")
        pd.DataFrame().to_csv(output_csv, sep="\t", index=False)
        return

    # Read the JSON file
    with open(json_file, 'r') as file:
        try:
            data = json.load(file)
        except json.JSONDecodeError:
            print(f"Error: {json_file} is not a valid JSON file. Creating an empty CSV file.")
            pd.DataFrame().to_csv(output_csv, sep="\t", index=False)
            return

    # If JSON data is empty, save an empty CSV
    if not data:
        print(f"Warning: {json_file} contains no data. Creating an empty CSV file.")
        pd.DataFrame().to_csv(output_csv, sep="\t", index=False)
        return

    # Flatten the JSON and extract the relevant columns for each row
    flat_data = []
    for item in data:
        row = {
            'Chromosome': item.get('Chromosome', ''),
            'Position': item.get('Position', ''),
            'Ref_allele': item.get('Ref_allele', ''),
            'Risk_allele': item.get('Alt_allele', ''),
            'Build': item.get('Build', ''),
            'Gene': item.get('Gene', ''),
        }
        # Flatten the rest of the fields
        for key, value in item.items():
            if key not in row:
                row[key] = value
        flat_data.append(row)

    # Create a pandas DataFrame
    df = pd.DataFrame(flat_data)

    # Add suffix to InterVar classification columns
    suffix = '_intervar'
    classification_columns = [
        'PVS1', 'PS1', 'PS2', 'PS3', 'PS4', 'PM1', 'PM2', 'PM3', 'PM4', 'PM5', 'PM6',
        'PP1', 'PP2', 'PP3', 'PP4', 'PP5', 'BA1', 'BP1', 'BP2', 'BP3', 'BP4', 'BP5',
        'BP6', 'BP7', 'BS1', 'BS2', 'BS3', 'BS4'
    ]
    df = df.rename(columns={col: col + suffix for col in classification_columns if col in df.columns})

    # Save to CSV
    df.to_csv(output_csv, sep="\t", index=False)
    print(f"CSV file saved as {output_csv}")


def merge_csv_files(intervar_csv, original_set_csv, output_csv, merge_type):
    """
    Merges the InterVar CSV with the original dataset (Set 1 or Set 2).
    If either file is empty, it saves the non-empty file as the output.
    If both files are empty, an empty file is created.
    """
    try:
        intervar_df = pd.read_csv(intervar_csv, sep="\t", low_memory=False)
    except (pd.errors.EmptyDataError, FileNotFoundError):
        print(f"Warning: {intervar_csv} is empty or missing. Proceeding with available data.")
        intervar_df = pd.DataFrame()  # Create an empty dataframe

    try:
        original_df = pd.read_csv(original_set_csv, sep="\t", low_memory=False)
    except (pd.errors.EmptyDataError, FileNotFoundError):
        print(f"Warning: {original_set_csv} is empty or missing. Proceeding with available data.")
        original_df = pd.DataFrame()  # Create an empty dataframe

    # If both files are empty, save an empty file
    if intervar_df.empty and original_df.empty:
        print(f"Both {intervar_csv} and {original_set_csv} are empty. Saving an empty file.")
        pd.DataFrame().to_csv(output_csv, sep="\t", index=False)
        return

    # Convert all columns to string to avoid type mismatches
    intervar_df = intervar_df.astype(str)
    original_df = original_df.astype(str)

    if merge_type == "set1":
        # Ensure 'CHROMOSOME_POSITION_HG38' is a string (fixing the merge issue)
        if 'CHROMOSOME_POSITION_HG38' in original_df.columns:
            original_df['CHROMOSOME_POSITION_HG38'] = original_df['CHROMOSOME_POSITION_HG38'].astype(str)
        
        # Merge if both files are not empty
        if not intervar_df.empty and not original_df.empty:
            merged_df = pd.merge(
                original_df,
                intervar_df,
                left_on=['CHROMOSOME', 'CHROMOSOME_POSITION_HG38', 'REFERENCE_ALLELE', 'RISK_ALLELE'],
                right_on=['Chromosome', 'Position', 'Ref_allele', 'Risk_allele'],
                how="left"
            )
        else:
            merged_df = original_df if not original_df.empty else intervar_df

    elif merge_type == "set2":
        # Ensure required columns exist
        required_columns = ['chrom', 'pos', 'ref_base', 'alt_base']
        missing_cols = [col for col in required_columns if col not in original_df.columns]
        if missing_cols:
            print(f"Warning: Missing required columns in original CSV: {missing_cols}. Proceeding with available data.")

        # Convert 'chrom' column and remove 'chr' prefix if present
        if 'chrom' in original_df.columns:
            original_df['chrom'] = original_df['chrom'].astype(str).str.replace(r'^(chr)', '', regex=True)

        # Merge if both files are not empty
        if not intervar_df.empty and not original_df.empty:
            merged_df = pd.merge(
                original_df,
                intervar_df,
                left_on=['chrom', 'pos', 'ref_base', 'alt_base'],
                right_on=['Chromosome', 'Position', 'Ref_allele', 'Risk_allele'],
                how="left"
            )
        else:
            merged_df = original_df if not original_df.empty else intervar_df

    else:
        raise ValueError("Invalid merge type. Use 'set1' or 'set2'.")

    # Save final merged output
    merged_df.to_csv(output_csv, sep='\t', index=False)
    print(f"Merged CSV file saved as {output_csv}")


if __name__ == "__main__":
    if len(sys.argv) != 6:
        print("Usage: python json_to_csv_intervar.py <json_file> <intervar_csv> <original_set_csv> <output_csv> <merge_type>")
        sys.exit(1)

    json_file = sys.argv[1]
    intervar_csv = sys.argv[2]
    original_set_csv = sys.argv[3]
    output_csv = sys.argv[4]
    merge_type = sys.argv[5]

    # Convert JSON to CSV
    json_to_csv(json_file, intervar_csv)

    # Merge with original set
    merge_csv_files(intervar_csv, original_set_csv, output_csv, merge_type)
