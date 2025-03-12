step=presurfer
basedir=/bcbl/home/public/Gari/VOTCLOC/main_exp
bids_dirname=BIDS

src_dir=$basedir/raw_nifti
analysis_name=beforeMar05
outputdir=${basedir}/${bids_dirname}
force=false # if overwrite exsting file

codedir=/export/home/tlei/tlei/soft/launchcontainers/src/launchcontainers/py_pipeline/01_prepare_nifti/prepare_anat
subseslist_path=$codedir/subseslist_votcloc.txt
logdir=${outputdir}/log_${step}/analysis_$analysis_name
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

    now=$(date +"%Y-%m-%dT%H-%M")
    log_file="${logdir}/presurfer_${sub}_${ses}_${step}_${now}.log"
    error_file="${logdir}/presurfer_${sub}_${ses}_${step}_${now}.err"
    # Run the command in the background
    echo $cmd
    eval $cmd > ${log_file} 2> ${error_file}
    unset sub
    unset ses
done < "$subseslist_path"
