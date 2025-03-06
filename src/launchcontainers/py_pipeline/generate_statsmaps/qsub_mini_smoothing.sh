logdir=/bcbl/home/public/Gari/MINI/BIDS_BLQfunc_T1/l1_surfaces_log/
codedir=/bcbl/home/public/Gari/MINI/BIDS_BLQfunc_T1/code/generate_fig
subseslist_path=/bcbl/home/public/Gari/MINI/BIDS_BLQfunc_T1/code/l1_glm/subseslist.tsv

mkdir -p $logdir
# Initialize a line counter
line_number=0

# Read the file line by line
while IFS=$'\t' read -r sub ses
do
    # Increment line counter
    ((line_number++))

    # Skip the first line which is the header
    if [ $line_number -eq 1 ]; then
        continue
    fi

	
	echo "### Runing SURFACE_glm on SUBJECT: $sub $ses SESSION ###"	
	cmd="qsub -q long.q \
		-N Smooth_mini-${sub}_s-${ses} \
		-o $logdir/Smooth_mini-${sub}-${ses}.o \
    	-e $logdir/Smooth_mini-${sub}-${ses}.e \
		-l mem_free=16G \
		-v sub=${sub} \
		-v ses=${ses} \
		$codedir/batch_smooth_surface.sh "

	echo $cmd
	eval $cmd
done < "$subseslist_path"
