# %%
# generate sub ses list
from __future__ import annotations

data = []
for sub in sub_list:
    for ses in ses_list:
        data.append([sub, ses, True, True, True, True])

# Create the DataFrame
df = pd.DataFrame(data, columns=['sub', 'ses', 'RUN', 'anat', 'dwi', 'func'])
output_file = path.join(basedir, 'code', 'lc_subseslist.txt')
df.to_csv(output_file, sep=',', index=False)
