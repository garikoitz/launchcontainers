import os
import re
from datetime import datetime, timedelta
from pathlib import Path
from typing import Tuple, List

import pandas as pd
import typer

app = typer.Typer()    
# start to getting the mapping info
# only get the both
merged = pd.read_csv('/bcbl/home/public/Gari/VOTCLOC/main_exp/dcm_bids_mapping_summary.csv')
merged_both= merged[merged['_merge']=='both']

sub_ses_group = merged.groupby(['sub', 'ses_from_note'])
# create symlink
force = True
# after everything, rerun all the dcm conversion
errors = []
for sub in range(1, 12):
    for ses in range(1, 11):
        sub = f'{sub:02}'
        ses = f'{ses:02}'
        target_dir = os.path.join(
            output_dir,
            f'sub-{sub}',
            f'ses-{ses}',
        )
        try:
            info = sub_ses_group.get_group((sub, ses))
            # clean the duplicated, drop the manual part 
            info=info.sort_values('origin')
            info=info.drop_duplicates(subset=['sub','ses_from_note','date', 'note','number_of_protocal'], keep = 'first')
            # drop the wrong upload sub-01_ses-01 with autoupload
            idx_to_drop = info.index[
                    (info['number_of_protocal']==26) ]
            info = info.drop(idx_to_drop)

            if len(info) > 1 or 'wrong' in info['note'].values:
                # if there are multiple dcm folder for the same session:
                if len(info) == 1 and info['note'].item() == 'wrong':
                    errors.append((sub, ses))
                elif len(info) > 1:
                    idx_to_drop = info.index[
                            (info['note']=='wrong') ]
                    info = info.drop(idx_to_drop)
                    errors.append((sub, ses))
                
            if len(info) > 1:
                print(f'sub-{sub}_ses-{ses}')
                print(info[['number_of_protocal', 'origin','note','dir_name']])

                # 1 if info is wrong, append it to error
                if info['note'].item() == 'wrong'
                    errors.append((sub, ses))

                # 1. auto upload and the manual mixed we drop the manual part and check
                if len(info['lab_project_dir'].unique()) > 1:
                    idx_to_drop = info.index[
                            (info['number_of_protocal']<50)
                        )
                    ]
                    info = info.drop(idx_to_drop)
                # after drop the manual and autp upload part
                if len(info) > 1:
                    print(f'$$need manual correction sub={sub},ses={ses}')
                    print(
                        info[[
                            'sub', 'ses', 'date', 'ses_label',
                            'ses_suffix', 'session_correct',
                        ]],
                    )
                    errors.append((sub, ses))
                else:
                    if info['session_correct'].item() == 0:
                        print(f'*session have mismatch functional run sub={sub},ses={ses}')
                        errors.append((sub, ses))
            else:
                # now the series only have 1 item so we can use item to get the value
                # we can simply create symlink
                lab_project_dir = info['lab_project_dir'].item()
                dir_name = info['dir_name'].item()
                levels_from_top = info['levels_from_top'].item()
                session_correct = info['session_correct'].item()

                if session_correct != 0:
                    src_dir = os.path.join(lab_project_dir, dir_name)
                    # print(f'going to create symlink from \n {src_dir} to {target_dir}')
                else:
                    print(f'*session have mismatch functional run sub={sub},ses={ses}')
                    # force_symlink(src_dir, target_dir,force)
                    errors.append((sub, ses))
            # now we need to parse the dir name to sub -xx ses-xx format
        except:
            print(f'*session have mismatch functional run sub={sub},ses={ses}')
            errors.append((sub, ses))

print(errors)

# if __name__ == "__main__":
#     app()


# Example usage
if __name__ == '__main__':
# Parse DICOM metadata
dcm_expand = parse_dicom_metadata(
    base_csv_path='/path/to/base_dicom_check_Nov-05.csv',
    manual_csv_path='/path/to/manual_dicom_check_Nov-05.csv',
)

# Validate and get summary
n_total, n_bad, bad_subs = validate_dicom_metadata(dcm_expand)

print(f"Total sessions: {n_total}")
print(f"Problematic sessions: {n_bad}")
print(f"Affected subjects: {bad_subs}")

# Export clean sessions for heudiconv processing
clean_sessions = dcm_expand[dcm_expand['note'] != 'wrong']
clean_sessions.to_csv('dicom_sessions_for_conversion.csv', index=False)