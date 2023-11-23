# export basedir=/export/home/glerma/public/Gari/MAGNO2
# export basedir="/export/home/glerma/glerma/00local/PROYECTOS/MAGNO2/"
# export subj=10_MAGNO_6979
export sess=T01
export basedir="/export/home/glerma/TESTDATA/heuditest/"
export subj="S001"

module load singularity/3.5.2
# Then run it after we create the Nifti/code/convertall.py file
singularity run --bind /bcbl:/bcbl \
	        --bind  ${basedir}:/base \
		~/glerma/software/heudiconv \
		--dicom_dir_template /base/dicom/{subject}/*.dcm \
		--outdir /base/Nifti/ \
		--heuristic /base/Nifti/code/convertall.py \
		--subjects ${subj} \
		--ses ${sess} \
		--converter dcm2niix \
		--bids \
		--minmeta \
		--grouping all \
		--overwrite
