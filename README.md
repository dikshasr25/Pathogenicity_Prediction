# Pathogenic Prediction Pipeline

## Overview
This pipeline automates the annotation, classification, and prioritization of genetic variants using multiple tools, including Diablo, InterVar, and Auto-ACMG. It takes a VCF file as input and produces a final output with clinically relevant variant classifications.

## Prerequisites
Before running the pipeline, ensure you have installed the necessary dependencies and set up the required tools.

### Install Diablo and Auto-ACMG
Run the following scripts to install Diablo and Auto-ACMG:
```sh
python diablo_install.py
python auto-acmg.py
```
These scripts will set up the required dependencies and databases for each tool.

## Folder Structure
```
project-folder/
│── pipeline.py
│── Diablo_annotate.py
│── merge_files.py
│── intervar.py
│── json_to_csv_intervar.py
│── final_acmg_classifier.py
│── test/ (for storing temporary files)
│── auto-acmg/ (contains Auto-ACMG server files)
  |── auto-acmg-query.py (inside auto-acmg folder)
│── input.vcf (example input file)
│── output.tsv (example output file)
```

## Usage
Run the pipeline using the following command:
```sh
python pipeline.py <input_vcf> [<annotated_vcf>] <final_output>
```
- `<input_vcf>`: Path to the input VCF file.
- `[<annotated_vcf>]`: (Optional) Path to a pre-annotated VCF file.
- `<final_output>`: Path to save the final output file.

### Example
```sh
python pipeline.py sample.vcf output.tsv
```
or
```sh
python pipeline.py sample.vcf annotated_sample.vcf output.tsv
```

## Features
- Estimates runtime based on previous execution history.
- Handles missing files by running necessary steps.
- Integrates with Auto-ACMG for classification.
- Automatically starts the Auto-ACMG server and resolves port conflicts.

## Notes
- Ensure that the `auto-acmg-query.py` script is located inside the `auto-acmg` directory.
- The `test/` folder is required to store temporary files.
- All scripts and input files should be placed in the main project folder.

## Running the Pipeline
```sh
python pipeline.py <input_vcf> [<annotated_vcf>] <final_output>
```
- `<input_vcf>`: The input VCF file containing variants.
- `<annotated_vcf>` (optional): Pre-annotated VCF file.
- `<final_output>`: The final output file with ACMG classifications.

### Example Run
```sh
python pipeline.py sample.vcf annotated_sample.tsv final_output.tsv
```

## Pipeline Steps
### 1. Annotate Variants with Diablo
- Runs `Diablo_annotate.py` to annotate the input VCF.
- **Output:** `<sample>_diablo.tsv`

### 2. Merge Variants with Additional Annotations
- Runs `merge_files.py` to integrate annotations from different sources.
- **Outputs:**
  - `<sample>_merged_set1.tsv` (common variants with phenotype)
  - `<sample>_pathogenic_set2.tsv` (pathogenic variants without phenotype)

### 3. Run InterVar for Variant Classification
- Runs `intervar.py` on both sets.
- **Outputs:**
  - JSON files (`intervar_set1.json`, `intervar_set2.json`)
  - TSV conversions (`intervar_set1.tsv`, `intervar_set2.tsv`)

### 4. Start Auto-ACMG Server and Query Variants
- Runs `auto-acmg-query.py` to classify variants.
- **Outputs:**
  - JSON files (`auto_acmg_set1.json`, `auto_acmg_set2.json`)
  - TSV conversions (`auto_acmg_set1.tsv`, `auto_acmg_set2.tsv`)

### 5. Generate Final ACMG Classification
- Runs `final_acmg_classifier.py` to generate the final output file.

## Troubleshooting
### Auto-ACMG Server Not Starting?
Check if the port 8080 is in use:
```sh
lsof -i :8080
```
Kill any existing processes on that port:
```sh
kill -9 <PID>
```
Restart the server using:
```sh
pipenv run uvicorn src.main:app --host 0.0.0.0 --port 8080 --reload &
```

### Missing Required Files?
- Ensure all dependencies are installed using `pipenv install`.
- Run `python diablo_install.py` and `python auto-acmg.py` before starting.
- Check file paths and ensure the correct order of arguments when running the pipeline.

## Output
The final output file (`<final_output>`) contains:
- Merged variant annotations
- Pathogenicity classifications
- ACMG-based prioritization
