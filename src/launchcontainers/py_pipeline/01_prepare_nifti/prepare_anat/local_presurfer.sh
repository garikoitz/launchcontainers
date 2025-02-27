step=presurfer
basedir=/bcbl/home/public/Gari/VOTCLOC/main_exp
bids_dirname=BIDS

src_dir=$basedir/raw_nifti
analysis_name=wordcenter_test
outputdir=${basedir}/${bids_dirname}
force=false # if overwrite exsting file

codedir=/export/home/tlei/tlei/soft/MRIworkflow/01_prepare_nifti/prepare_anat
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
	# Export variables for use in the called script
    export src_dir
    export outputdir
    export sub
    export ses
    export force
    export codedir
    # Command to execute locally
    cmd="bash $codedir/run_${step}.sh "
    
    echo $cmd
    eval $cmd
    unset sub
    unset ses
done < "$subseslist_path"
