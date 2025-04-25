# """
# MIT License
# Copyright (c) 2020-2025 Garikoitz Lerma
# Copyright (c) 2024-2025 Yongning Lei

# Permission is hereby granted, free of charge, to any person obtaining a copy of this software
# and associated documentation files (the "Software"), to deal in the Software without restriction,
# including without limitation the rights to use, copy, modify, merge, publish, distribute, sublicense,
# and/or sell copies of the Software, and to permit persons to whom the Software is furnished to do so,
# subject to the following conditions:

# The above copyright notice and this permission notice shall be included in all copies or substantial
# portions of the Software.
# """

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
