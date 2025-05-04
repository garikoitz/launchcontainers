# this is the notes about how to rename the filenames of the fixRW fixFF

# 1. cd to BIDS dir sub/ses/func
ls *ret*

for i in *ret* ; do mv $i ${i//retFF/retfixFF} ; done

# 2. cd to fmriprep dir sub/ses/func
ls *ret*

for i in *ret* ; do mv $i ${i//retRW/retfixRW} ; done

# there could be the case that we need to rename the retCB_run-01
for i in *ret* ; do mv $i ${i//retCB_run-01/retfixRWblock01_run-01} ; done
for i in *ret* ; do mv $i ${i//retCB_run-02/retfixRWblock02_run-01} ; done
