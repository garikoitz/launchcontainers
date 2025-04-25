# """
# MIT License

# Copyright (c) 2024-2025 Yongning Lei

# Permission is hereby granted, free of charge, to any person obtaining a copy of this software
# and associated documentation files (the "Software"), to deal in the Software without restriction,
# including without limitation the rights to use, copy, modify, merge, publish, distribute, sublicense,
# and/or sell copies of the Software, and to permit persons to whom the Software is furnished to do so,
# subject to the following conditions:

# The above copyright notice and this permission notice shall be included in all copies or substantial
# portions of the Software.
# """


# This is the code for the first step of heudiconv
# you can run this one by itself by uncomment the variables
# or you can also use the qsub code to run them by inputing variables


# basedir=/bcbl/home/public/Gari/VOTCLOC/VSS
# dicom_dirname=dicom
# outputdir=$basedir/BIDS
# sing_path=/bcbl/home/public/Gari/singularity_images

# sample code to run without ses
#singularity run --bind /bcbl/home/public/Gari/VOTCLOC/main_exp:/base --bind /bcbl:/bcbl --bind /export:/export /bcbl/home/public/Gari/singularity_images/heudiconv_1.3.2.sif -d /base/dicom/sub-{subject}/*/*/*/*/* -s 01 02 03 04 05 06 08 -o /bcbl/home/public/Gari/VOTCLOC/main_exp/raw_nifti -f convertall -c none -g all --overwrite > ~/public/Gari/VOTCLOC/main_exp/raw_nifti/log_heudiconv/redo_heudiconv-and-using-subonly/all.log 2> ~/public/Gari/VOTCLOC/main_exp/raw_nifti/log_heudiconv/redo_heudiconv-and-using-subonly/all.err


module load apptainer/latest
echo "Now the singularity is loaded, it is: "
module list


echo "Subject: ${sub} "
echo "Session: ${ses} "
cmd="singularity run \
        	--bind ${basedir}:/base \
	    	--bind /bcbl:/bcbl \
			--bind /export:/export \
        	${sing_path} \
			-d /base/${dicom_dirname}/sub-{subject}/ses-{session}/*/*/*/*.dcm \
	    	-s ${sub} \
			-ss ${ses} \
			-o ${outputdir} \
	    	-f convertall \
	    	-c none \
        	-g all \
        	--overwrite "
			# -ss ${ses} \
echo $cmd
eval $cmd

module unload apptainer


# I added this 			-d /base/${dicom_dirname}/sub-{subject}/ses-{session}/*/*.dcm \ is because some of the directory will be read and being processed
