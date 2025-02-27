# ADD FSL TO THE PATH BEFORE LAUNCHING MATLAB
# then do 
# tbUse BCBLViennaSoft;  
# this step is to add pressurfer and NORDIC_RAW into the path so that you
# can use it
#module load gcc/7.3.0
module load afni
module load fsl
module load matlab/R2021B
# VIENNA
# baseP = '/ceph/mri.meduniwien.ac.at/projects/physics/fmri/data/bcblvie22/BIDS';

# BCBL
# basedir=/bcbl/home/public/Gari/VOTCLOC/main_exp
# bids_dirname=BIDS

# src_dir=$basedir/$bids_dirname
# analysis_name=week1
# outputdir=${basedir}/${bids_dirname}/derivatives/process_nifti/analysis-${analysis_name}

# subs=('03' '06' '08')
# sess=('01')
# force=false # if overwrite exsting file
# for sub in "$subs[@]" ; do
# for ses in $sess[@]; do

    echo "Doing PRESURFER for sub: ${sub}, and ses: ${ses}"
    matlab -nosplash -nodesktop -r "tbUse BCBLViennaSoft; \
    addpath('$codedir'); \
    src_dir='$src_dir'; \
    outputdir='$outputdir'; \
    sub='$sub'; \
    ses='$ses'; \
    force=$force; \
    presurferT1(src_dir, outputdir, sub, ses, force); exit" 
    
# done done