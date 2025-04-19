import pandas as pd
import sys
import os

def merge_files(file1_path, file2_path, output_merged=None, output_pathogenic=None):
    """
    Merges two variant annotation files while keeping common and pathogenic variants separate.

    If file1 (Annotated VCF) is missing, only pathogenic variants from file2 (Diablo output) are processed.

    :param file1_path: Path to the first input file (Annotated VCF) or None if missing.
    :param file2_path: Path to the second input file (Diablo output).
    :param output_merged: Output file for merged common variants (only created if file1 is present).
    :param output_pathogenic: Output file for pathogenic variants.
    """

    if not os.path.exists(file2_path):
        print(f"Error: Diablo output file {file2_path} is missing. Cannot proceed.")
        sys.exit(1)

    # Read Diablo file (file2) - Required
    print(f"Reading {file2_path} (Diablo output)...")
    df2 = pd.read_csv(file2_path, sep='\t', low_memory=False)
    df2.columns = df2.columns.str.strip()

    # Debugging: Print available columns in Diablo output
    print(f"Available columns in {file2_path}: {df2.columns.tolist()}")

    # Check for ACMG column in Diablo output
    if "ACMG" in df2.columns:
        df2["ACMG"] = df2["ACMG"].astype(str).str.lower()
        pathogenic_df = df2[
            (df2["ACMG"] == "pathogenic") | 
            (df2["ACMG"] == "likely pathogenic")
        ]
        pathogenic_df.to_csv(output_pathogenic, sep='\t', index=False)
        print(f"Set 2 (Pathogenic Variants) saved as {output_pathogenic}")
    else:
        print("Error: 'ACMG' column is missing in the Diablo output file!")
        print("Available columns:", df2.columns.tolist())
        sys.exit(1)

    # If file1 (Annotated VCF) is missing, stop here
    if not file1_path or not os.path.exists(file1_path):
        print("Annotated VCF file is missing. Only Set 2 (Pathogenic Variants) was processed.")
        return

    # Read file1 (Annotated VCF) if it exists
    print(f"Reading {file1_path} (Annotated VCF)...")
    df1 = pd.read_csv(file1_path, sep='\t', low_memory=False)

# Debugging: Print available columns in Annotated VCF
    print(f"Available columns in {file1_path}: {df1.columns.tolist()}")

# Ensure required columns exist before merging
    required_columns = ["CHROMOSOME", "CHROMOSOME_POSITION_HG38", "REFERENCE_ALLELE", "RISK_ALLELE"]
    if not all(col in df1.columns for col in required_columns):
     print(f"Error: Annotated VCF file {file1_path} is missing required columns {required_columns}.")
     sys.exit(1)

    required_columns_diablo = ["chrom", "pos", "ref_base", "alt_base"]
    if not all(col in df2.columns for col in required_columns_diablo):
     print(f"Error: Diablo output file {file2_path} is missing required columns {required_columns_diablo}.")
     sys.exit(1)

# Merge Set 1 (Common Variants) using filtered dataframe
    merged_df = df1.merge(
    df2,
    how="inner",
    left_on=["CHROMOSOME", "CHROMOSOME_POSITION_HG38", "REFERENCE_ALLELE", "RISK_ALLELE"],
    right_on=["chrom", "pos", "ref_base", "alt_base"]
)

    # Save merged file
    if output_merged:
        merged_df.to_csv(output_merged, sep='\t', index=False)
        print(f"Set 1 (Common Variants) saved as {output_merged}")

import sys
import os

if __name__ == "__main__":
    if len(sys.argv) < 3:
        print("Usage: python merge_files.py [<file1>] <file2> <output_pathogenic> [<output_merged>]")
        sys.exit(1)

    # If only three arguments are provided, assume no annotated VCF file
    if len(sys.argv) == 3:
        file1 = None  # No Annotated VCF provided
        file2 = sys.argv[1]  # Diablo output file
        output_pathogenic = sys.argv[2]  # Pathogenic variants output
        output_merged = None  # No merged output required
    else:
        file1 = sys.argv[1]  # Annotated VCF file (Optional)
        file2 = sys.argv[2]  # Diablo output file (Required)
        output_pathogenic = sys.argv[3]  # Pathogenic variants output (Required)
        output_merged = sys.argv[4] if len(sys.argv) > 4 else None  # Optional merged output

    merge_files(file1, file2, output_merged, output_pathogenic)

