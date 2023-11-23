import argparse
import os
import shutil as sh
import glob
import sys

parser = argparse.ArgumentParser(description='''createListOfSubjects.py 'subdir' 'wildcard' 'fullPathFile' 'separator'  :::::
it will create fullPathFile  with the subject list names separated with spaces, detected by wildcard, separated by separator''')

# Required positional argument
parser.add_argument('subdir', type=str, help='subdir where the data is')
parser.add_argument('wildcard', type=str, help='wildcard to select folders')
parser.add_argument('fullPathFile', type=str, help='path and file name to write')
parser.add_argument('separator', type=str, help='separator to use')

# Optional positional argument
# parser.add_argument('opt_pos_arg', type=int, nargs='?',
#                             help='An optional integer positional argument')

# Optional argument
# parser.add_argument('--opt_arg', type=int,
#                             help='An optional integer argument')

# Switch
# parser.add_argument('--switch', action='store_true',
#                             help='A boolean switch')

args = parser.parse_args()


print('Create file'+args.fullPathFile+' using '+args.wildcard+' in '+args.subdir+', use "'+args.separator+'" as separator.\n')


os.chdir(args.subdir)


A = sorted(glob.glob(args.wildcard))
if os.path.isfile(args.fullPathFile):
    print(args.fullPathFile + ' already exists.\n ')
else:    
    with open(args.fullPathFile,'a+') as myfile: 
        myfile.write(args.separator.join(map(str,A)))



