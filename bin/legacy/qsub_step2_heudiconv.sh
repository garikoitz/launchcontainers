#!/bin/bash
#-c cwd
#-m be
# Loading modules:
# Tasks of the job

# Get all users in python to generate the file
# with open('allsubs2.txt','a+') as myfile:
# 	myfile.write(" ".join(map(str,A)))

# export basedir=/export/home/glerma/public/Gari/MAGNO2
# export subj=T1_7851
export sess=$2
# export basedir=/export/home/glerma/public/Gari/MAGNO2
basedir="/export/home/glerma/glerma/00local/PROYECTOS/MAGNO2/"
basedir="/bcbl/home/public/KSHIPRA/dwibygari"

read SUBJECTS < $1
for sub in ${SUBJECTS};
    do
	export subj="$sub"
	
	printf "#########################################"
	printf "############## $sub_$sess ###############"
	printf "#########################################"
	qsub    -q veryshort.q \
		-N t-heudiconv_s-${sub}_s-${sess} \
		-l mem_free=16G \
		-v subj=$subj \
		-v sess=$sess \
		-v basedir=$basedir \
		$pM/step2_heudiconv.sh 
done
