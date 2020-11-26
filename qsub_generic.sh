#!/bin/bash
#-c cwd
#-m be
# Loading modules:
# Tasks of the job

# Get all users in python to generate the file
# with open('allsubs2.txt','a+') as myfile:
# 	myfile.write(" ".join(map(str,A)))

printf "Calling the tool $1, with sub $2, ses $3, running analysis $4 (mem=$5, var 6=$6)\n\n"
# export basedir=/export/home/glerma/public/Gari/MAGNO2
export basedir="/export/home/glerma/glerma/00local/PROYECTOS/MAGNO2/"
export basedir="/bcbl/home/public/Gari/MINI/"
export tool=$1
export sub=$2
export ses=$3
export analysis=$4
export mem=$5
export noqsub=$6

export subjbids="$sub"
export path2subderivatives="${basedir}/Nifti/derivatives/${tool}/analysis-${analysis}/sub-${subjbids}/ses-${ses}"
export path2config="${basedir}/Nifti/derivatives/${tool}/analysis-${analysis}/config.json"
	
printf "\n\n It will use basedir:$basedir and tool:$tool \n\n"

	if [ "$noqsub" = "run" ]; then
		printf "\n\nNO-QSUB MODE DETECTED\n\n"
		module load singularity/3.5.2
		printf "Starting singularity, using:"
		printf "Tool: ~/glerma/containers/${tool}.sif"
		printf "Path: ${path2subderivatives}"
		printf "Config: ${path2config}"
		set -x
		# singularity ${noqsub} -e --no-home \
		# env SINGULARITYENV_MAXMEM=60000000 
		singularity ${noqsub} --cleanenv --containall \
		        --bind /bcbl:/bcbl \
		        --bind /tmp:/tmp \
		        --bind /scratch:/scratch \
		        --bind ${path2subderivatives}/input:/flywheel/v0/input:ro \
                        --bind ${path2subderivatives}/output:/flywheel/v0/output \
		        --bind ${path2config}:/flywheel/v0/config.json \
		        ~/glerma/containers/${tool}.sif

	fi
	if [${noqsub} = ""]; then
		printf "#########################################"
    		printf "############## $sub_$ses ################"
    		printf "#########################################"
    		qsub    -q all.q \
			-N t-${tool}_a-${analysis}_s-${sub}_s-${ses} \
			-l mem_free=$mem \
			-v tool=$tool \
			-v path2subderivatives=$path2subderivatives \
			-v path2config=$path2config \
			$pM/runSingularity.sh 
	fi
