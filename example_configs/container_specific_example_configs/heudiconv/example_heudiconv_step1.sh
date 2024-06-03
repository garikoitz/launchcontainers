basedir=/export/home/glerma/public/Exchange/LMC_DWI_course
sing_path=${basedir}/containers
#sub=$(cat $basedir/subSesList.txt | awk 'NR >1 {print $(1)}' | tr ',\n' ' ')
# define an array of subjects
sub=("BHpilot_IT") 
#cmd="ls ${basedir}/dicom/${subj}"
#echo $cmd
#eval $cmd

module load apptainer/latest

for sub in "${sub[@]}";do 
	echo "Working with  sub: $sub "
	cmd="apptainer shell \
                        --bind ${basedir}:/base \
                        ${sing_path}/heudiconv_1.1.0.sif \
                                                        -d /base/DATA/Project/dicom/sub-{subject}/*/*/* \
                                                        --subjects ${sub} \
                                                        -o /base/BIDS/ \
                                                        -f convertall \
                                                        -c none \
                                                        -g all \
                                                        --overwrite \
        "

    
	echo $cmd
	eval $cmd	
done
