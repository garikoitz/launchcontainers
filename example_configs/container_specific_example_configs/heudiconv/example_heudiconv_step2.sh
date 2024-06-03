basedir=/export/home/glerma/public/Exchange/LMC_DWI_course
sing_path=${basedir}/containers
#sub=$(cat $basedir/subSesList.txt | awk 'NR >1 {print $(1)}' | tr ',\n' ' ')
sub=("BHpilot_IT") 
ses=001

module load apptainer/latest

cmd="apptainer run \
                    --bind ${basedir}:/base \
                    ${sing_path}/heudiconv_1.1.0.sif \
                                                        -d /base/DATA/Project/dicom/sub-{subject}/*/*/* \
                                                        -s ${sub} \
                                                        --ses ${ses} \
                                                        -o /base/BIDS/ \
                                                        --overwrite \
                                                        -f /base/BIDS/heudiconv/heudiconv_heuristics.py \
                                                        -c dcm2niix \
                                                        -b \
                                                        --grouping all \
    "

echo $cmd
eval $cmd
