#!/bin/bash
basedir=/export/home/llecca/public/DB/devtrajtract/DATA/SWAHILI

echo "sub,ses,RUN" > $basedir/Dicoms/subSesList_Dicoms.csv
for sub in $(ls -d $basedir/Dicoms/*sub-*/);
do
    for ses in "$sub"/*/
    do
        sub=$(basename $sub)
        ses=$(basename $ses)
        echo "${sub##*-},${ses##*-},True" >> $basedir/Dicoms/subSesList_Dicoms.csv
    done
done
