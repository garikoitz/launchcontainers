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


# basedir=/bcbl/home/public/Gari/VOTCLOC/VSS
# dicom_dirname=dicom
# outputdir=$basedir/BIDS
# sing_path=/bcbl/home/public/Gari/singularity_images
# sub='05'
# ses='day6BCBL'
# heuristicfile=$basedir/code/00_dicom_to_nifti/heuristic/heuristic_BCBL.py

module load apptainer/latest
echo "Now the singularity is loaded, it is: "
module list


echo "Subject: ${sub} "
# try the no session thing Feb 09 2025
echo "Session: ${ses} "
cmd="singularity run \
        	--bind ${basedir}:/base \
	    	--bind /bcbl:/bcbl \
			--bind /export:/export \
        	${sing_path} \
			-d /base/${dicom_dirname}/sub-{subject}/ses-{session}/*/*/*/*.dcm \
	    	--subjects ${sub} \
			--ses ${ses} \
			-o ${outputdir} \
        	--overwrite \
	    	-f ${heuristicfile} \
	    	-c dcm2niix \
	    	-b \
        	--grouping all "
			# try the no sesion
			#--ses ${ses} \
echo $cmd
eval $cmd

module unload apptainer
