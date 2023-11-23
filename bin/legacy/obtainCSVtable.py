dt   = pd.read_csv(subseslist, sep=",", header=0)
rowind = 0
for index in dt.index:
  sub = dt.loc[index, 'sub']
  ses = dt.loc[index, 'ses']
  dwi = dt.loc[index, 'dwi']
  for meas in meass:
    if dwi:
      tmpfname    = os.path.join('sub-'+sub,'ses-'+ses,'output','RTP_'+meas+'.csv')
      tmp         = pd.read_csv(tmpfname, sep=",", header=0)
      tmp.columns = ['Left_Thalamic_Radiation','Right_Thalamic_Radiation','Left_Corticospinal','Right_Corticospinal','Left_Cingulum_Cingulate','Right_Cingulum_Cingulate','Left_Cingulum_Hippocampus','Right_Cingulum_Hippocampus','Callosum_Forceps_Major','Callosum_Forceps_Minor','Left_IFOF','Right_IFOF','Left_ILF','Right_ILF','Left_SLF','Right_SLF','Left_Uncinate','Right_Uncinate','Left_Arcuate','Right_Arcuate','Left_Optic_Radiation','Right_Optic_radiation']
      tmpfnameC2R    = os.path.join('sub-'+sub,'ses-'+ses,'output','RTP_C2ROI'+meas+'.csv')
      tmpC2R         = pd.read_csv(tmpfnameC2R, sep=",", header=0)
      tmpC2R.columns = ['Left_Thalamic_Radiation','Right_Thalamic_Radiation','Left_Corticospinal','Right_Corticospinal','Left_Cingulum_Cingulate','Right_Cingulum_Cingulate','Left_Cingulum_Hippocampus','Right_Cingulum_Hippocampus','Callosum_Forceps_Major','Callosum_Forceps_Minor','Left_IFOF','Right_IFOF','Left_ILF','Right_ILF','Left_SLF','Right_SLF','Left_Uncinate','Right_Uncinate','Left_Arcuate','Right_Arcuate','Left_Optic_Radiation','Right_Optic_radiation']
      for t in tmp.columns:
          # Full tract first
          tmpt = tmp[t]
          tt   = pd.DataFrame(columns=['sub', 'ses','struct','meas','val'])
          tt.at[0,'sub']    = sub
          tt.at[0,'ses']    = ses
          tt.at[0,'struct'] = t
          tt.at[0,'meas']   = meas
          tt.at[0,'val']    = tmpt.values
          # Clip2ROI tract second
          tmptC2R              = tmpC2R[t]
          ttC2R                = pd.DataFrame(columns=['sub', 'ses','struct','meas','val'])
          ttC2R.at[0,'sub']    = sub
          ttC2R.at[0,'ses']    = ses
          ttC2R.at[0,'struct'] = t
          ttC2R.at[0,'meas']   = meas
          ttC2R.at[0,'val']    = tmptC2R.values
          if rowind == 0:
            vals      = tt
            valsC2ROI = ttC2R
          else:
            vals      = pd.concat([vals, tt],ignore_index=True)
            valsC2ROI = pd.concat([valsC2ROI, ttC2R],ignore_index=True)
          # Increase index by one
          rowind += 1
vals.to_csv('MAGNO_Full_Tract.csv', index=False)
valsC2ROI.to_csv('MAGNO_C2ROI_Tract.csv', index=False)

