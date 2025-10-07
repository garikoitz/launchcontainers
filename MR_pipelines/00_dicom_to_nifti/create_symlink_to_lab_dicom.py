import os
import pandas as pd
import pydicom
from datetime import datetime

def build_depth_report(base_dir, exclude=()):
    rows = []

    # Get only first-level dirs under base_dir
    top_level_dirs = [
        d for d in os.listdir(base_dir)
        if os.path.isdir(os.path.join(base_dir, d)) and d not in exclude
    ]
    # get all the dir under the basedir, for every dir we will walk the dir
    # the walk dir looping is at the subdir level (i.e., there are 10 subdirs, it will do 10 iter)
    for topdir in top_level_dirs:
        top_path = os.path.join(base_dir, topdir)
        print(topdir)
        # get the number of subdirs under images/xx/
        subdir=0
        # get if the session is having correct number of functional dcms
        func_correct = 1
        acq_dates = []
        if not any(x in topdir for x in ["test", "multisite", "pilot"]):
            for root, dirs, files in os.walk(top_path):
                if not dirs and files:
                    # get the depth for heudiconv
                    rel_from_top = os.path.relpath(root, top_path)
                    if rel_from_top == ".":
                        depth = 1   # file directly in the top-level dir
                    else:
                        depth = len(rel_from_top.split(os.sep)) + 1
                    subdir+=1
                    # filter if the dcm transfer is correct
                    if len(files) > 250:
                        print(f"WARNING !!! the number of files of this ses is not correct {topdir}")
                        func_correct = 0
                    # get the acq date   
                    if not "Phoenix" in root:
                        dcm=pydicom.dcmread(os.path.join(root, files[0]))
                        fmt = "%Y%m%d%H%M%S.%f"
                        dt=datetime.strptime(dcm.AcquisitionDateTime,fmt)
                        acq_date= dt.strftime("%Y-%m-%d")
                        #print(f'acquition date if {acq_date}')
                        acq_dates.append(acq_date)
            if len(set(acq_dates))>1:
                print(f"WARNING different date time in one dir, error {topdir}")
                func_corr=0
                acq_date='ERR'

            rows.append({
                    "base_dir": base_dir,
                    "dir_name": topdir,
                    "levels_from_top": depth,
                    "number_of_protocal": subdir,
                    "example_file_name": files[0],
                    "func_correct": func_correct,
                    "acq_date":acq_date
                })

    return pd.DataFrame(rows, columns=[
                                        "base_dir",
                                        "dir_name",  
                                        "levels_from_top", 
                                        "number_of_protocal", 
                                        "example_file_name",
                                        "func_correct",
                                        "acq_date"])

if __name__ == "__main__":
    base_dir = "/export/home/tlei/lab/MRI/VOTCLOC_22324/DATA/images"
    exclude = ("manual")
    df = build_depth_report(base_dir,exclude)
    print(df.head())        
    output_dir = "/bcbl/home/public/Gari/VOTCLOC/main_exp/dicom"
    df.to_csv(os.path.join(output_dir,"base_dicom_check.csv"), index=False)

    # then do the samething for manual
    base_dir2 = "/export/home/tlei/lab/MRI/VOTCLOC_22324/DATA/images/manual"
    exclude2 = ()
    df2 = build_depth_report(base_dir2,exclude2)
    print(df2.head())        
    df2.to_csv(os.path.join(output_dir,"manuel-dir_dicom_check.csv"), index=False)


###now read the xlsx file and get the unique date and time
### merge the date and time so that we know what dcm folder we should use

# 🔹 Path to the downloaded Excel file
# Replace with your actual file path
xlsx_file = '/bcbl/home/public/Gari/VOTCLOC/main_exp/BIDS/sourcedata/VOTCLOC_subses_list.xlsx'
# 🔹 Load the Excel file
xls = pd.ExcelFile(xlsx_file)

# 🔹 Find all sheets that match "sub-xx"
all_df = []
for sheet_name in xls.sheet_names:
    if sheet_name.startswith('sub-'):  # Process sheets named sub-xx
        df = pd.read_excel(xls, sheet_name=sheet_name, header=0)
        df = df.loc[:, ['sub', 'ses', 'date']]
        # ✅ Make 'sub' column string type
        df["sub"] = df["sub"].astype(str)
        # ✅ Convert 'date' to datetime, then format to YYYY-MM-DD
        df["date"] = pd.to_datetime(df["date"]).dt.strftime("%Y-%m-%d")
    all_df.append(df)

big_df = pd.concat(all_df)

subses_date=big_df[['sub','ses','date']].drop_duplicates()
subses_date.to_csv(os.path.join(output_dir,"subses_date_summary.csv"), index=False)
# load the 
