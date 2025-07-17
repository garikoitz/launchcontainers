from launchcontainers.utils import read_df, force_symlink, check_symlink
from bids import BIDSLayout
import errno

'''
This is a helper function to rename the WC ret

so normally, the ret we won't do GLM, but for the word center case, we will use it

This function will take sub and ses as input, get the BIDS and fmriprep Layout, rename:

create symlink:
retRW --> retfixRW
retFF --> retfixFF
retCB --> retfixRWblock01  (dependes on the language)

this script will work with Joana's script, at somepoint, we will merge it
'''

sub='11'
ses='02'
fp_ana_name='afterJuly09'

basedir = '/bcbl/home/public/Gari/VOTCLOC/main_exp'
fmriprep_dir = f'{basedir}/BIDS/derivatives/fmriprep-{fp_ana_name}'
bids_dir = f'{basedir}/BIDS'

bids_layout=BIDSLayout(bids_dir, derivatives=False, validate=False)
fmriprep_layout=BIDSLayout(fmriprep_dir,validate=False)

# use the BIDS to get the list that contains the old name
all_ret_fmriprep=fmriprep_layout.get(subject=sub, session=ses,
task=['retfixRW','retfixFF','retfixRWblock01'],datatype='func')

all_ret_bids=bids_layout.get(subject=sub, session=ses,
task=['retfixRW','retfixFF','retfixRWblock01'],datatype='func')

# define a dict and replace the key with the value
rename_dict={
    "retRW":"retfixRW",
    "retFF":"retfixFF",
    "retCB":"retfixRWblock01" }

def replace_all(s, repl):
    for old, new in repl.items():
        s = s.replace(new, old)
    return s

for i in all_ret_bids:
    fname=i.path
    force_symlink(fname, replace_all(fname,rename_dict), False)


# it works