import subprocess

def run_diablo_annotation(input_vcf, output_tsv):
    """
    Runs the Diablo annotation script on the given VCF file.
    
    :param input_vcf: Path to the input VCF file.
    :param output_tsv: Path to the output TSV file.
    """
    command = f"python Diablo_annotate.py -i {input_vcf} -o {output_tsv}"
    print(f"Executing: {command}")
    
    try:
        subprocess.run(command, shell=True, check=True)
        print(f"Diablo annotation completed. Output saved to {output_tsv}")
    except subprocess.CalledProcessError as e:
        print(f"Error running Diablo annotation: {e}")
