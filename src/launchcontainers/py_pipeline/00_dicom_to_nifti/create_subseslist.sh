#!/bin/bash
# Place this file in your BIDS-Nifti folder
# this code is the legacy code from Gari, Tiger updated it

basedir=$1
output_list_dir=$2
output_name=$3

echo "sub,ses,RUN,anat,dwi,func" > $output_list_dir/${output_name}.txt;
for sub in $(ls -d $basedir/*sub-*/);
do
    for ses in "$sub"/*/
    do
        if [ -d "$ses/anat" ];then
            anat="True"
    	else
	    anat="False"
        fi

        if [ -d "$ses/dwi" ];then
            dwi="True"
	else
	    dwi="False"
        fi

        if [ -d "$ses/func" ];then
            fmri="True"
        else
	    fmri="False"
        fi

        sub=$(basename $sub)
        ses=$(basename $ses)

        echo "${sub##*-},${ses##*-},True,${anat},${dwi},${fmri}" >> $output_list_dir/${output_name}.txt ;
    done
done
