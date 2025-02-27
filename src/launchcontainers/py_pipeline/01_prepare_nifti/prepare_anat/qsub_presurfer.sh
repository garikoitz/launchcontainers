step=presurfer
basedir=/bcbl/home/public/Gari/VOTCLOC/fLoc_pilot
bids_dirname=BIDS

src_dir=$basedir/$bids_dirname
analysis_name=MP2RAGE_and_NORDIC
outputdir=${basedir}/${bids_dirname}/derivatives/process_nifti/analysis-${analysis_name}

# sub='02'
# ses='day5BCBL'
force=false # if overwrite exsting file

codedir=/export/home/tlei/tlei/soft/MRIworkflow/01_prepare_nifti
subseslist_path=$codedir/subses_List.txt
logdir=${outputdir}/run_${step}
mkdir -p $logdir

echo "reading the subses"
# Initialize a line counter
line_number=0
# Read the file line by line
while IFS=$'\t' read -r sub ses
do
    echo "line number is $line_number sub is $sub ses is $ses"
    # Increment line counter
    ((line_number++))

    # Skip the first line which is the header
    if [ $line_number -eq 1 ]; then
        continue
    fi

	echo this is line number $line_number 
	echo "### CONVERTING TO NIFTI OF SUBJECT: $sub $ses SESSION ###"
	cmd="qsub -q matlab.q \
	    -S /bin/bash
		-M tlei@bcbl.eu
		-N ${step}_s-${sub}_s-${ses} \
		-o $logdir/${step}_sub-${sub}_ses-${ses}.o \
    	-e $logdir/${step}_sub-${sub}_ses-${ses}.e \
		-l mem_free=16G \
		-v src_dir=${src_dir} \
		-v outputdir=${outputdir} \
		-v sub=${sub} \
		-v ses=${ses} \
		-v force=$force \
		$codedir/run_${step}.sh "

	echo $cmd
	eval $cmd
done < "$subseslist_path"
