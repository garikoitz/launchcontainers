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

sub='02'
ses='10'


fp_ana_name='afterJuly09'

basedir = '/bcbl/home/public/Gari/VOTCLOC/main_exp'
fmriprep_dir = f'{basedir}/BIDS/derivatives/fmriprep-{fp_ana_name}'
prfanalyze_dir= f'{basedir}/BIDS/derivatives/prfanalyze-vista/analysis-01'
bids_dir = f'{basedir}/BIDS'
use_bids_query=True

if use_bids_query:
    bids_layout=BIDSLayout(bids_dir, derivatives=False, validate=False)
    fmriprep_layout=BIDSLayout(fmriprep_dir,validate=False)
    prfanalyze_layout= BIDSLayout(prfanalyze_dir,validate=False)

    # use the BIDS to get the list that contains the old name
    all_ret_fmriprep=fmriprep_layout.get(subject=sub, session=ses,
    task=['retfixRW','retfixFF','retfixRWblock01'],datatype='func')

    all_ret_bids=bids_layout.get(subject=sub, session=ses,
    task=['retfixRW','retfixFF','retfixRWblock01'],datatype='func')

    all_ret_prfanalyze=prfanalyze_layout.get(subject=sub, session=ses,
    task=['retfixRW','retfixFF','retfixRWblock01'])

else:
    #use glob, not implemented
    task=['retfixRW','retfixFF','retfixRWblock01']

# define a dict and replace the key with the value
rename_dict={
    "retRW":"retfixRW",
    "retFF":"retfixFF",
    "retCB":"retfixRWblock01" }

def add_fix_to_name(s, repl):
    for old, new in repl.items():
        s = s.replace(old, new)
    return s

def remove_fix_from_name(s, repl):
    for old, new in repl.items():
        s = s.replace(new, old)
    return s


for i in all_ret_prfanalyze:
    fname=i.path
    force_symlink(fname, remove_fix_from_name(fname,rename_dict), False)


# it works