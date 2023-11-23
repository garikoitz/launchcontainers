# basedir="/bcbl/home/public/KSHIPRA/dwibygari"
basedir="/export/home/glerma/TESTDATA/heuditest/"
subj="S001"


# First run it empty
singularity run --bind /bcbl:/bcbl \
	        --bind ${basedir}:/base \
		~/glerma/software/heudiconv \
		-d /base/dicom/{subject}/*.dcm \
                -o /base/Nifti/ \
                -f convertall \
                -s ${subj} \
		--grouping all \
                -c none


