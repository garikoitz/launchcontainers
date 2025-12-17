#!/usr/bin/env python3
"""
Simple check: Are RWvsLEX and RWvsPER stat-t files present for sub-02 all sessions?
"""

from pathlib import Path

# Configuration
base_dir = "/bcbl/home/public/Gari/VOTCLOC/main_exp/BIDS/derivatives/l1_surface/analysis-final_v2"
subject = "02"
sessions = ["01","02", "03","04", "05", "06"]  # Add all your sessions here
contrasts = ["RWvsLEX", "RWvsPER"]
hemis = ["L"]

print(f"Checking sub-{subject} for contrasts: {contrasts}")
print(f"Sessions: {sessions}")
print("=" * 80)
print()

total_expected = 0
total_found = 0
missing_list = []

# Loop through run configurations (1 to 10)
for num_runs in range(1, 11):
    
    # Loop through iterations (1 to 10)
        
        # Loop through sessions
        for ses in sessions:
            
            output_dir = Path(base_dir) / f"power_analysis_{num_runs}_run" / f"sub-{subject}" / f"ses-{ses}"
            
            # Check for each contrast and hemisphere (stat-t only)
            for contrast in contrasts:
                for hemi in hemis:
                    
                    total_expected += 1
                    
                    # Pattern for stat-t files
                    pattern = f"sub-{subject}_ses-{ses}_*_hemi-{hemi}_*_contrast-{contrast}_stat-t_*_statmap.func.gii"
                    
                    matching_files = list(output_dir.glob(pattern))
                    
                    if matching_files:
                        total_found += 1
                    else:
                        missing_list.append(f"{num_runs}run ses-{ses} hemi-{hemi} {contrast}")

print(f"Total expected: {total_expected}")
print(f"Total found: {total_found}")
print(f"Total missing: {total_expected - total_found}")
print(f"Completion: {(total_found/total_expected)*100:.1f}%")
print()

if missing_list:
    print(f"Missing {len(missing_list)} files:")
    for item in missing_list[:20]:  # Show first 20
        print(f"  - {item}")
    if len(missing_list) > 20:
        print(f"  ... and {len(missing_list) - 20} more")
else:
    print("✓ All files present!")