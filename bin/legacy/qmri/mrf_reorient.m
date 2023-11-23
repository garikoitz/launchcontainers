function mrf_reorient(niifilename,refname)
% mrf_reorient(niifilename,[refantname])
% adjust the origin and orientation of 3D-MRF-derived nifti file, 
% by changing the v.mat of its header,
% output: renamed with a prefix of 'r'. It will be ready for
% further coregistration process.
% 
% called SPM functions, should be compatiable of all versions.
%
% Author: Hongjian He, CBIST@ZJU.

if nargin>=2
    vref = spm_vol(refname);    % if there is a reference T1w
    matref = eye(4);
    matref(:,4) = vref.mat(:,4); 
else
    matref = [1,0,0,-100; 0,1,0,-100; 0,0,1,-140; 0,0,0,1];
    % this new mat is simply from a 3D-T1w, since it is not necessary to be accurate 
end

v = spm_vol(niifilename);
data = spm_read_vols(v);

% reverse the y-aixs, need to change y-origin accordingly
matref(2,2) = -1; 
matref(2,4) = matref(2,4)+v.dim(2);  
v.mat = matref; 

% write the output files
[~,fname,fext] = fileparts(v.fname); %2021.11.01, get rid of dir information
v.fname = ['r',fname,fext];
spm_write_vol(v,data);