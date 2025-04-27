% need to module load freesurfer/7.3.2



% conver the t-map from fsnative to fsaverage
basedir = '/bcbl/home/public/Gari/VOTCLOC/';
freesurfer_dir = fullfile(basedir, "derivatives/fmriprep/analysis-okazaki_ST05/sourcedata/freesurfer");
setenv('SUBJECTS_DIR', freesurfer_dir);
%{
    % test the number of vertex in fsaverage
    lh_inflated=read_surf(fullfile(freesurfer_dir, "fsaverage","surf","lh.inflated"));

    %add path of SPM
    addpath(genpath(fullfile(toolboxRoot,'spm12')))

    %fmriprep dir
    fmriprep_dir='/bcbl/home/public/Gari/VOTCLOC/VSS/derivatives/fmriprep/analysis-okazaki_ST05';
    sub='05';
    ses='day1VA';

    sub05_fsaverage=gifti(fullfile(fmriprep_dir,['sub-' sub], ['ses-' ses], 'func', 'sub-05_ses-day1VA_task-fLoc_run-02_hemi-L_space-fsaverage_bold.func.gii'));
%}

% for sub and session, get all the t-map files under the folder, and them
% use mri_surf2surf

input_dir='/bcbl/home/public/Gari/VOTCLOC/VSS/derivatives/l1_surface/analysis-s134tama_fwhm02';
subs={'01','03','04'};%'01','02','03','04'
sess={'day3PF'};
 %'day3PF','day5BCBL','day6BCBL'sub='05';
 % 'day1VA', 'day1VB', 'day2VA', 'day2VB','day3PF','day5BCBL','day6BCBL'
outputdir=[input_dir '_fsaverage_surf2surf'];

% all the output should be put in the output_sesdir
for subI=1:length(subs)
    sub=subs(subI);
for sesI = 1: length(sess)
    ses=sess(sesI);
    cd (fullfile(input_dir,['sub-' sub{1}],['ses-' ses{1}])); %or,['sub-' sub{1}]
    tmapdir=dir('*.gii');  %('*_stat-t*');
    output_sesdir=fullfile(outputdir,['sub-' sub{1}],['ses-' ses{1}]); % or outputdir,['sub-' sub{1}],['ses-' ses{1}]

    if (exist(output_sesdir,'dir')==0)
        mkdir(output_sesdir)
    end

parfor I=1:length(tmapdir)
    sprintf(tmapdir(I).name);
    srcfile=fullfile(tmapdir(I).folder,tmapdir(I).name);
    targfile=fullfile(output_sesdir,strrep(tmapdir(I).name,'fsnative', 'fsaverage'));
    cmd=['mri_surf2surf --srcsubject sub-',sub{1},' --srcsurfval ' ,srcfile ,' --trgsubject fsaverage --trgsurfval ' ,targfile ,' --hemi lh'];
    system(cmd)
end
end
end
