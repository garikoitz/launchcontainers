import argparse
import os
import shutil as sh
import glob
import sys

parser = argparse.ArgumentParser(description='''zipDicoms.py 'sublist.txt' srcdir dstdir 'remove if str'  :::::
zip the dicoms in the folder and move to the dest dir''')

# Required positional argument
parser.add_argument('sublist', type=str, help='list of subs')
parser.add_argument('srcdir', type=str, help='srcdst')
parser.add_argument('dstdir', type=str, help='path and file name to write')
parser.add_argument('intsrcpath', type=str, help='internal src path')
parser.add_argument('intdstpath', type=str, help='internal dst path')

# Optional positional argument
parser.add_argument('remove', type=str, nargs='?', help='if contains string do not use')
# parser.add_argument('opt_pos_arg', type=int, nargs='?',
#                             help='An optional integer positional argument')

# Optional argument
# parser.add_argument('--opt_arg', type=int,
#                             help='An optional integer argument')

# Switch
# parser.add_argument('--switch', action='store_true',
#                             help='A boolean switch')

args = parser.parse_args()


subs = open(args.sublist,'r').readlines()
if len(subs)>1: 
    print("There are more than one row, it will only work with the first row")


    


A = sorted(glob.glob(args.wildcard))
if os.path.isfile(args.fullPathFile):
    print(args.fullPathFile + ' already exists.\n ')
else:    
    with open(args.fullPathFile,'a+') as myfile: 
        myfile.write(args.separator.join(map(str,A)))


# Example
$GIT/paper-MAGNO/shpython/zipDicom.py $HOME/lab/MRI/MAGNO/DATA/images
