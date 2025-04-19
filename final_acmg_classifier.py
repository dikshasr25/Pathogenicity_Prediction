
import pandas as pd
import os
import sys

def process_acmg_classifier(file_path):
    """Process the Auto ACMG classifier output file and return a cleaned dataframe."""
    print(f"Processing file: {file_path}")
    
    df = pd.read_csv(file_path, sep='\t',low_memory=False)
    
    df.replace(["nan", "NaN"], "", inplace=True)
    # Ensure case-insensitive column matching
    df_columns_lower = df.columns.str.casefold()

    # Define ACMG criteria
    acmg_criteria = ["pvs1", "ps1", "ps3", "pm1", "pm2", "pm4", "bp3", "pm5", 
                     "pp2", "pp3", "bp4", "pp5", "ba1", "bs2", "bs3", "bp1", 
                     "bp6", "bp7", "bs1"]

    # Identify relevant base columns
    base_columns_raw = [
    "chrom",
    "pos",
    "ref_base",
    "alt_base",
    "hugo",
    "PHENOTYPEIDS",
    "PHENOTYPELIST",
    "so",
    "cchange",
    "achange",
    "clinvar.sig",
    "clinvar.disease_refs",
    "clinvar.disease_names",
    "clinvar.hgvs",
    "clinvar.rev_stat",
    "clinvar.id",
    "clinvar.af_go_esp",
    "clinvar.af_exac",
    "clinvar.af_tgp",
    "clinvar.clinvar_allele_id",
    "clinvar.variant_type",
    "clinvar.variant_clinical_sources",
    "clinvar.dbsnp_id",
    "extra_vcf_info.AC",
    "extra_vcf_info.AF",
    "extra_vcf_info.AN",
    "extra_vcf_info.DP",
    "fathmm_mkl.fathmm_mkl_coding_score",
    "fathmm_mkl.fathmm_mkl_coding_rankscore",
    "fathmm_mkl.fathmm_mkl_coding_pred",
    "fathmm_mkl.fathmm_mkl_group",
    "gerp.bp4_benign",
    "gerp.pp3_pathogenic",
    "hpo.id",
    "hpo.term",
    "hpo.all",
    "lrt.lrt_score",
    "lrt.lrt_converted_rankscore",
    "lrt.lrt_pred",
    "metalr.score",
    "metalr.rankscore",
    "metalr.pred",
    "metasvm.score",
    "metasvm.rankscore",
    "metasvm.pred",
    "mutation_assessor.transcript",
    "mutation_assessor.score",
    "mutation_assessor.rankscore",
    "mutation_assessor.impact",
    "mutation_assessor.all",
    "mutationtaster.rankscore",
    "mutationtaster.prediction",
    "provean.score",
    "provean.rankscore",
    "provean.prediction",
    "polyphen2.hdiv_pred",
    "polyphen2.hvar_pred",
    "polyphen2.hdiv_rank",
    "sift.prediction",
    "sift.confidence",
    "sift.score",
    "sift.rankscore",
    "sift.bp4_benign",
    "sift.pp3_pathogenic",
    "dbsnp.rsid",
    "gnomad3.af",
    "spliceai.ds_ag",
    "spliceai.ds_al",
    "spliceai.ds_dg",
    "spliceai.ds_dl",
    "spliceai.dp_ag",
    "spliceai.dp_al",
    "spliceai.dp_dg",
    "spliceai.dp_dl",
    "vcfinfo.phred",
    "vcfinfo.zygosity",
    "vcfinfo.alt_reads",
    "vcfinfo.tot_reads",
    "vcfinfo.af",
    "dbsnp.rsid",
    "gnomad3.af",
    "gnomad3.af_afr",
    "gnomad3.af_asj",
    "gnomad3.af_eas",
    "gnomad3.af_fin",
    "gnomad3.af_lat",
    "gnomad3.af_nfe",
    "gnomad3.af_oth",
    "gnomad3.af_sas",
    "prediction_data_consequence_mehari",
    "prediction_data_consequence_cadd",
    "prediction_data_consequence_cadd_consequence",
    "prediction_data_gene_symbol",
    "prediction_data_hgnc_id",
    "prediction_data_transcript_id",
    "prediction_data_transcript_tags",
    "prediction_data_tx_pos_utr",
    "prediction_data_cds_pos",
    "prediction_data_prot_pos",
    "prediction_data_prot_length",
    "prediction_data_pHGVS",
    "prediction_data_cds_start",
    "prediction_data_cds_end",
    "prediction_data_strand",
    "prediction_data_scores_cadd_phyloP100",
    "prediction_data_scores_cadd_gerp",
    "prediction_data_scores_cadd_spliceAI_acceptor_gain",
    "prediction_data_scores_cadd_spliceAI_acceptor_loss",
    "prediction_data_scores_cadd_spliceAI_donor_gain",
    "prediction_data_scores_cadd_spliceAI_donor_loss",
    "prediction_data_scores_cadd_ada",
    "prediction_data_scores_cadd_rf",
    "prediction_data_scores_dbnsfp_alpha_missense",
    "prediction_data_scores_dbnsfp_metaRNN",
    "prediction_data_scores_dbnsfp_bayesDel_noAF",
    "prediction_data_scores_dbnsfp_revel",
    "prediction_data_scores_dbnsfp_phyloP100",
    "prediction_data_scores_dbnsfp_sift",
    "prediction_data_scores_dbnsfp_polyphen2",
    "prediction_data_scores_dbnsfp_mutationTaster",
    "prediction_data_scores_dbnsfp_fathmm",
    "prediction_data_scores_dbnsfp_provean",
    "prediction_data_scores_dbnsfp_vest4",
    "prediction_data_scores_dbnsfp_mutpred",
    "prediction_data_scores_dbnsfp_primateAI",
    "prediction_data_scores_dbscsnv_ada",
    "prediction_data_scores_dbscsnv_rf",
    "prediction_data_scores_misZ",
    "prediction_data_thresholds_phyloP100",
    "prediction_data_thresholds_gerp",
    "prediction_data_thresholds_spliceAI_acceptor_gain",
    "prediction_data_thresholds_spliceAI_acceptor_loss",
    "prediction_data_thresholds_spliceAI_donor_gain",
    "prediction_data_thresholds_spliceAI_donor_loss",
    "prediction_data_thresholds_ada",
    "prediction_data_thresholds_rf",
    "prediction_data_thresholds_metaRNN_pathogenic",
    "prediction_data_thresholds_bayesDel_noAF_pathogenic",
    "prediction_data_thresholds_revel_pathogenic",
    "prediction_data_thresholds_cadd_pathogenic",
    "prediction_data_thresholds_metaRNN_benign",
    "prediction_data_thresholds_bayesDel_noAF_benign",
    "prediction_data_thresholds_revel_benign",
    "prediction_data_thresholds_cadd_benign",
    "Score", "Intervar",
    "ACMG"
]

    # Ensure all base columns are present, even if they are empty
    for col in base_columns_raw:
     if col.casefold() not in df_columns_lower:
        df[col] = ""

# Match base columns case-insensitively
    base_columns = [col for col in base_columns_raw if col in df.columns]
# Strictly filter prediction columns
    prediction_columns = [col for col in df.columns if col.lower().startswith("prediction_criteria_") and col.lower().endswith("_prediction")]

# Rename prediction columns
    renamed_columns = {col: f"{col.split('_')[2]}_auto_acmg" for col in prediction_columns if len(col.split('_')) > 2}
    df.rename(columns=renamed_columns, inplace=True)

# Convert applicable Auto ACMG prediction columns to binary
    for col in renamed_columns.values():
     df[col] = df[col].apply(lambda x: 1 if isinstance(x, str) and x.strip().casefold() == "applicable" else 0)

# Ensure all required Diablo ACMG columns exist
    for criteria in acmg_criteria:
     col_name = f"{criteria}_diablo_acmg"
     if col_name.casefold() not in df_columns_lower:
        df[col_name] = 0  # Default to 0 if the column does not exist

# Identify InterVar and Diablo columns case-insensitively
    intervar_columns = {col.casefold(): col for col in df.columns if col.lower().endswith("_intervar")}
    diablo_columns = {col.casefold(): col for col in df.columns if col.lower().endswith("_diablo_acmg")}

# Create final ACMG classifier columns
    final_acmg_columns = {}
    for criteria in acmg_criteria:
        auto_col = f"{criteria}_auto_acmg"
        intervar_col = intervar_columns.get(f"{criteria}_intervar")
        diablo_col = diablo_columns.get(f"{criteria}_diablo_acmg")
    
        valid_columns = [col for col in [auto_col, intervar_col, diablo_col] if col in df.columns]
    
        if valid_columns:
            final_acmg_columns[f"Final_{criteria.upper()}"] = df[valid_columns].max(axis=1)

# Convert dictionary to DataFrame and merge
    df_final_classifiers = pd.DataFrame(final_acmg_columns)
    df_filtered = pd.concat([df[base_columns], df_final_classifiers], axis=1)
    
# Add Flag_Pathogenicity column
    df_filtered["Flag_Pathogenicity"] = df_filtered.get("ACMG", "").apply(
    lambda x: "0" if pd.isna(x) or str(x).strip() == "" or "benign" in str(x).casefold() or "vus" in str(x).casefold() 
    else "1"
)

# Add Flag_Phenotype column
    # Identify the 'PhenotypeIDs' column (case insensitive)
    phenotype_col = next((col for col in df_filtered.columns if col.casefold() == "phenotypeids"), None)

# Add 'Flag_Phenotype' column and ensure empty values default to 0
    if phenotype_col:
     df_filtered["Flag_Phenotype"] = df_filtered[phenotype_col].fillna("").apply(lambda x: 1 if str(x).strip() not in ["", "-"] else 0)
    else:
     df_filtered["Flag_Phenotype"] = 0  # If column is missing, assign 0 to all rows

    # Create Final_ACMG column by combining ACMG and Intervar
    df_filtered["Final_ACMG"] = df_filtered[["ACMG", "Intervar"]].apply(
    lambda x: "/".join(x.dropna().astype(str)).strip("/") if x.notna().any() else "", axis=1
)

    # Remove the word "auto" from Final_ACMG column values
    df_filtered["Final_ACMG"] = df_filtered["Final_ACMG"].str.replace("auto", "", case=False)

# Drop ACMG, Intervar, Chromosome, and Risk_allele columns
    df_filtered.drop(columns=[col for col in ["ACMG", "Intervar", "Chromosome", "Risk_allele", "PHENOTYPEIDS",
    "PHENOTYPELIST"] if col in df_filtered.columns], inplace=True)


    column_renaming = {
        "so": "MC",
        "extra_vcf_info.AC": "AlleleCount",
        "extra_vcf_info.AF": "AlleleFrequency",
        "extra_vcf_info.AN": "AlleleNumber",
        "extra_vcf_info.DP": "Depth",
        "chrom": "CHROMOSOME",
        "pos": "CHROMOSOME_POSITION_HG38",
        "ref_base": "REFERENCE_ALLELE",
        "alt_base": "RISK_ALLELE",
        "dbsnp.rsid": "DBSNP.RSID",
        "hugo": "GENESYMBOL",
        "clinvar.sig": "CLINVAR.CLINICAL_SIGNIFICANCE",
        "Final_ACMG" : "ACMG"    }

    df_filtered.rename(columns=column_renaming, inplace=True)
    
    return df_filtered

def is_file_empty(file_path):
    """Returns True if the file is empty or does not contain valid data."""
    if not os.path.exists(file_path):
        return True  # File doesn't exist
    if os.stat(file_path).st_size == 0:
        return True  # File is empty
    try:
        df = pd.read_csv(file_path, sep='\t', nrows=5, low_memory=False)
        return df.empty  # No actual data
    except pd.errors.EmptyDataError:
        return True  # No columns detected (corrupt or missing header)
    except pd.errors.ParserError:
        print(f"Warning: Unable to parse {file_path}. Possible corruption.")
        return True  # Parsing error

def merge_sets(set1_file, set2_file, output_file):
    """Vertically merge Set 1 and Set 2 and save the final output."""
    
    # Load datasets if they exist and are not empty
    df_set1 = process_acmg_classifier(set1_file) if not is_file_empty(set1_file) else pd.DataFrame()
    df_set2 = process_acmg_classifier(set2_file) if not is_file_empty(set2_file) else pd.DataFrame()

    if df_set1.empty and df_set2.empty:
        print("Warning: Both Set 1 and Set 2 are empty. Creating an empty output file.")
        pd.DataFrame().to_csv(output_file, sep='\t', index=False)  # Create empty file
        return

    print("Concatenating datasets vertically...")
    df_merged = pd.concat([df_set1, df_set2], ignore_index=True)  # Merge sets

    # Replace NaN with empty string
    df_merged.fillna("", inplace=True)
    df_merged.replace(["nan", "NaN"], "", inplace=True)

    print("Saving merged dataset...")
    df_merged.to_csv(output_file, sep='\t', index=False)
    print(f"Final merged file saved as {output_file}")

def main(set1_file, set2_file, final_output_file):
    """Main function to process ACMG classification and merge sets."""

    set1_empty = is_file_empty(set1_file)
    set2_empty = is_file_empty(set2_file)

    if set1_empty and set2_empty:
        print("Warning: Both Set 1 and Set 2 are empty. Creating an empty output file.")
        pd.DataFrame().to_csv(final_output_file, sep='\t', index=False)  # Create empty file
        return

    if not set1_empty and set2_empty:
        print("Set 2 is empty. Processing only Set 1.")
        df_set1 = process_acmg_classifier(set1_file)
        df_set1.to_csv(final_output_file, sep='\t', index=False)
        print(f"Final output saved as {final_output_file}")
        return

    if not set2_empty and set1_empty:
        print("Set 1 is empty. Processing only Set 2.")
        df_set2 = process_acmg_classifier(set2_file)
        df_set2.to_csv(final_output_file, sep='\t', index=False)
        print(f"Final output saved as {final_output_file}")
        return

    print("Both Set 1 and Set 2 contain data. Proceeding with merging.")
    merge_sets(set1_file, set2_file, final_output_file)

if __name__ == "__main__":
    if len(sys.argv) != 4:
        print("Usage: python final_classifier.py <set1_file> <set2_file> <final_output_file>")
        sys.exit(1)

    main(sys.argv[1], sys.argv[2], sys.argv[3])
