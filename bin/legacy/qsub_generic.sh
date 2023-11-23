#!/bin/bash
printf "[qsub_generic.sh] "
# get the arguments from the command line
while getopts "t:s:e:a:b:o:m:q:c:p:g:i:n:u:h:d:f:j:" opt; do
    case $opt in
        t) tool="$OPTARG";;
        s) sub="$OPTARG";;
        e) ses="$OPTARG";;
        a) analysis="$OPTARG";;
        b) basedir="$OPTARG";;
        o) codedir="$OPTARG";;
        m) mem="$OPTARG";;
        q) que="$OPTARG";;
        c) core="$OPTARG";;
        p) tmpdir="$OPTARG";;
	    g) logdir="$OPTARG";;
        i) sin_ver="$OPTARG";;
        n) container="$OPTARG";;
        u) qsb="$OPTARG";;
        h) host="$OPTARG";;
        d) manager="$OPTARG";;
        f) system="$OPTARG";;
        j) maxwall="$OPTARG";;
    esac
done

printf "Calling the tool $tool, with sub $sub, ses $ses, running analysis $analysis"

export subjbids="$sub"
export path2subderivatives="${basedir}/Nifti/derivatives/${tool}/analysis-${analysis}/sub-${subjbids}/ses-${ses}"
export path2config="${basedir}/Nifti/derivatives/${tool}/analysis-${analysis}/config.json"

printf "\n\n [qsub_generic.sh] It will use basedir:$basedir and tool:$tool \n\n"

if [ "$qsb" == "true" ];then
	printf "#########################################\n"
 	printf "######### $sub, session $ses ##########\n"
   	printf "#########################################\n"

    printf "#### running subject $sub, session $ses, analysis $analysis\n"
    printf "#### host: $host\n"
    printf "#### que: $que\n"
    printf "#### mem: $mem\n"
    printf "#### tool: $tool\n"
    printf "#### path2subderivatives: $path2subderivatives\n"
    printf "#### config: $path2config\n"
    printf "#### singularity version: $sin_ver\n"
    printf "#### container: $container\n"
    printf "#### temporal directory: $tmpdir\n"
    printf "#### log directory: $logdir\n"
    printf "#### coding directory: $codedir\n"
    printf "#### workload manager to submit: $manager\n"
    printf "#### maxwall: computaiton duration: $maxwall\n"

    # -N t-${tool}_a-${analysis}_s-${sub}_s-${ses} \
    # # THIS IS FOR BCBL
    if [ "$host" == "BCBL" ]; then
            cmd="qsub \
            -q $que \
            -N ${sub}_${ses}_${tool}_${analysis} \
            -o ${logdir}/t-${tool}_a-${analysis}_s-${sub}_s-${ses}.o \
            -e ${logdir}/t-${tool}_a-${analysis}_s-${sub}_s-${ses}.e \
            -l mem_free=$mem \
            -v
	    tool=${tool},path2subderivatives=${path2subderivatives},host=${host},path2config=${path2config},sin_ver=${sin_ver},container=${container},tmpdir=${tmpdir},manager=${manager},system=${system} \
    	    ${codedir}/runSingularity.sh"
    elif [ "$host" == "DIPC" ]; then
        if [ "$manager" == "qsub" ] ; then
            cmd="qsub \
            -q $que -l mem=$mem,nodes=1:ppn=$core \
            -N ${sub}-${ses}-${tool}-${analysis} \
            -o ${logdir}/s-${sub}_s-${ses}_t-${tool}_a-${analysis}.o \
            -e ${logdir}/s-${sub}_s-${ses}_t-${tool}_a-${analysis}.e \
            -v
	    tool=${tool},path2subderivatives=${path2subderivatives},host=${host},path2config=${path2config},sin_ver=${sin_ver},container=${container},tmpdir=${tmpdir},manager=${manager},system=${system} \
            ${codedir}/runSingularity.sh"
        elif [ "$manager" == "slurm" ]; then
            cmd="sbatch \
            -q $que --partition=${que} --mem=$mem --nodes=1 --cpus-per-task=$core 
            --time=00-${maxwall}:00:00 \
            --ntasks-per-node=1 \
            --job-name=${sub}_${ses}_${tool}_${analysis} \
            -o ${logdir}/t-${tool}_a-${analysis}_s-${sub}_s-${ses}.o \
            -e ${logdir}/t-${tool}_a-${analysis}_s-${sub}_s-${ses}.e \
            --export=ALL,tool=${tool},path2subderivatives=${path2subderivatives},host=${host},path2config=${path2config},sin_ver=${sin_ver},container=${container},tmpdir=${tmpdir},manager=${manager},system=${system} \
            ${codedir}/runSingularity.sh"
        fi
   fi
   printf "#### runnig $manager in $host server:\n"
   echo $cmd
   eval $cmd
fi

if [ "$qsb" == "false" ];then
  printf "\n\nNO-QSUB MODE DETECTED\n\n"
  printf "\n\nLoading Module Singularity\n\n"
  printf "Starting singularity, using ${sin_ver}:\n"
  

  printf "Tool: ~/containers/${tool}.sif\n"
  printf "Path: ${path2subderivatives}\n"
  printf "Config: ${path2config}\n"
  set -x
  
  module load ${sin_ver}  
  singularity run -e --no-home \
          --bind /scratch:/scratch \
          --bind ${path2subderivatives}/input:/flywheel/v0/input:ro \
                      --bind ${path2subderivatives}/output:/flywheel/v0/output \
          --bind ${path2config}:/flywheel/v0/config.json \
          ${container}
fi
