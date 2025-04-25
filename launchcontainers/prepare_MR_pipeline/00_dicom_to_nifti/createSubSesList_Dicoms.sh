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
