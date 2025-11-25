

# 1. cd to BIDS dir sub/ses/func
ls *ret*

for i in *ret* ; do mv $i ${i//retFF/retfixFF} ; done
for i in *ret* ; do mv $i ${i//retCB/retfixRWblock} ; done
for i in *ret* ; do mv $i ${i//retRW/retfixRW} ; done

or we can do
for i in *ret* ; do mv $i ${i//ret/retfix} ; done
for i in *ret* ; do mv $i ${i//retfixCB/retfixRWblock01} ; done


# 2. cd to fmriprep dir sub/ses/func
ls *ret*

for i in *ret* ; do mv $i ${i//retFF/retfixFF} ; done
for i in *ret* ; do mv $i ${i//retCB/retfixRWblock} ; done
for i in *ret* ; do mv $i ${i//retRW/retfixRW} ; done

# there could be the case that we need to rename the retCB_run-01
for i in *ret* ; do mv $i ${i//retCB_run-01/retfixRWblock01_run-01} ; done
for i in *ret* ; do mv $i ${i//retCB_run-02/retfixRWblock02_run-01} ; done
