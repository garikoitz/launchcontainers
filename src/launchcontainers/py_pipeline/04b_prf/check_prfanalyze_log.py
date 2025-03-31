import os

def check_estimates_lines(folder_path):
    # Find all .out files in the folder
    out_files = [f for f in os.listdir(folder_path) if f.endswith('.out')]
    if not out_files:
        print("No .out files found in the folder.")
        return

    for out_file in out_files:
        out_file_path = os.path.join(folder_path, out_file)
        with open(out_file_path, 'r') as f:
            lines = f.readlines()
            count = sum(1 for line in lines if line.strip().startswith("Writing the estimates"))
            if count != 6:
                print(f"{out_file} has {count} lines starting with 'Writing the estimates' (expected 6).")

# Example usage:
folder = "/scratch/tlei/VOTCLOC/dipc_slurm_prfanalyze-vista_logs/march29"
check_estimates_lines(folder)
