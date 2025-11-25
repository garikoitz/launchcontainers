function nordic_dwi(src_dir, output_dir, analysis_name, sub, ses, nordic_scans_end, doNORDIC, force, log_fname)
% MIT License

% Copyright (c) 2024-2025 Yongning Lei

% Permission is hereby granted, free of charge, to any person obtaining a copy of this software
% and associated documentation files (the "Software"), to deal in the Software without restriction,
% including without limitation the rights to use, copy, modify, merge, publish, distribute, sublicense,
% and/or sell copies of the Software, and to permit persons to whom the Software is furnished to do so,
% subject to the following conditions:

% The above copyright notice and this permission notice shall be included in all copies or substantial
% portions of the Software.

    % ADD FSL TO THE PATH BEFORE LAUNCHING MATLAB
    % then do
    tbUse BCBLViennaSoft;
    % this step is to add pressurfer and NORDIC_RAW into the path so that you
    % can use it


    %if system('fslroi')==127
    %    error("didn't load fsl");
    %end

    %if system('3dTstat')==127
    %    error("didn't load afni");
    %end
    %%%%%%%%%% EDIT THIS %%%%%%%%%%
    %clc;
    %clear all;
    % VIENNA
    % baseP = '/ceph/mri.meduniwien.ac.at/projects/physics/fmri/data/bcblvie22/BIDS';

    % BCBL

    %{
    src_dir = fullfile('/bcbl/home/public/Gari/VOTCLOC/main_exp','BIDS');
    analysis_name = 'dwi_nonordic';
    output_dir = fullfile('/bcbl/home/public/Gari/VOTCLOC/main_exp','BIDS', 'derivatives', 'processed_nifti');
    if ~exist(output_dir, 'dir')
       mkdir(output_dir)
    end
    subs = {'03','06','08'} ;
    ses = '01';
    nordic_scans_end = 0;
    force = false;
    doNORDIC = false;
    log_fname = ['dwi_noNordic_log.txt'];

    to run it
    addpath '/bcbl/home/home_n-z/tlei/soft/MRIworkflow/01_prepare_nifti'
    for subI=1:length(subs)
     sub=subs{subI}
     nordic_dwi(src_dir, output_dir, analysis_name, sub, ses, nordic_scans_end, doNORDIC, force, log_fname)
    end
    %}

    sub=['sub-' sub];
    ses=['ses-' ses];
    if ~exist(output_dir, 'dir')
       mkdir(output_dir)
    end
    analysis_dir = fullfile(output_dir, ['analysis-' analysis_name]);
    if ~exist(analysis_dir, 'dir')
       mkdir(analysis_dir)
    end
    tbPath = fullfile(bvRP,'..');
    spm12Path = fullfile(tbPath, 'spm12');
    bidsmatlab_path=fullfile(tbPath,'bids-matlab');
    addpath(bidsmatlab_path);
    addpath(spm12Path);
    fmamtPath = fullfile(tbPath, 'freesurfer_mrtrix_afni_matlab_tools'); % tbUse if not installed
    addpath(genpath(fmamtPath));
    addpath(genpath(fullfile(src_dir,'..','code')));
    addpath('/bcbl/home/home_n-z/tlei/soft/MRIworkflow/01_prepare_nifti');
    nordicpath=fullfile(tbPath,'NORDIC_Raw');
    addpath(genpath(nordicpath));
    setenv('FSLOUTPUTTYPE', 'NIFTI_GZ')

    log=fullfile(analysis_dir, log_fname);
    % start the diary, disp, fprintf , sprintf will go to diary and will be
    % captured by .e and .o
    diary(log);
    src_sesP = fullfile(src_dir, sub, ses,'dwi');
    out_sesP = fullfile(analysis_dir, sub, ses, 'dwi');
    system(['chmod 755 ', src_sesP]);
    system(['chmod 755 ', analysis_dir])
    fprintf('The input dir is: %s, and the output dir is %s \n', src_sesP, out_sesP);

    % Detect all T1w.nii.gz files
    dwimag_pattern = fullfile(src_sesP, ['*_magnitude.nii.gz']);
    src_mags = dir(dwimag_pattern);


    % nordic
    %% Step 1, check if the BIDS has been processed, if not, create backups
    % loop over all the mag files in src_filder
    % if there are no mag back up files, create backups, delete the noise
    % scans to only 1 left
    disp('### Starting step 1, checking if the nordic has been run before \n')
    time_start=datetime('now');
    parfor src_magI=1:length(src_mags) % src_magI=1
        try
            % define file names
            fn_magn_in  = fullfile(src_mags(src_magI).folder, src_mags(src_magI).name);
            fn_phase_in = strrep(fn_magn_in, '_magnitude', '_phase');
            system(['chmod 777 ', fn_phase_in, ' ', fn_magn_in, ' ']);

            fn_magn_backup = strrep(fn_magn_in, '.nii.gz', '_orig.nii.gz');
            fn_phase_backup = strrep(fn_phase_in, '.nii.gz', '_orig.nii.gz');
            % might have filetype problems, so needs to check here
            system(['fslmaths ', fn_magn_in,  ' ', fn_magn_in,  ' -odt short']);
            system(['fslmaths ', fn_phase_in, ' ', fn_phase_in, ' -odt short']);
            disp('** Change data format to float of src mag and src phase .nii.gz \n');


            if ~exist(fn_magn_backup, 'file')
                disp('The magnitude_orig.nii.gz are not there, creating backups and remove extra noise scans \n');
                info = niftiinfo(fn_magn_in);
                system(['cp ', fn_magn_in, ' ', fn_magn_backup]);
                system(['cp ', fn_phase_in, ' ', fn_phase_backup]);

                disp('** backups for mag and phase created \n');
                % maintain 1 volumns for nordic and remove the extra
                if nordic_scans_end > 1
                    system(['fslroi ', fn_magn_in, ' ', fn_magn_in, ' 0 -1 0 -1 0 -1 0 ', num2str(info.ImageSize(end)-(nordic_scans_end-1))]);
                    system(['fslroi ', fn_phase_in, ' ', fn_phase_in, ' 0 -1 0 -1 0 -1 0 ', num2str(info.ImageSize(end)-(nordic_scans_end-1))]);
                    disp('** Extra noise scans removed \n');
                end
                system(['fslmaths ', fn_magn_in,  ' ', fn_magn_in,  ' -odt float']);
                system(['fslmaths ', fn_phase_in, ' ', fn_phase_in, ' -odt float']);
                disp('** Change data format to float of src mag and src phase .nii.gz \n');
            else
                disp(' The magnitude_orig.nii.gz are there, do nothing \n');
            end

        end

    end

    %% Step 2, prepare ARG struct for each run of the the magnitude.nii.gz
    disp('Starting step2, preparing the ARG and file struct storing the input and output file info \n')
    clear ARG
    I = 1; %ARG file index
    for src_magI=1:length(src_mags)
        % define file names
        fn_magn_in  = fullfile(src_mags(src_magI).folder, src_mags(src_magI).name);
        fn_phase_in = strrep(fn_magn_in, '_magnitude', '_phase');
        fn_out      = fullfile(strrep(src_mags(src_magI).folder, '/BIDS',['/BIDS/derivatives/processed_nifti/analysis-' analysis_name] ), strrep(src_mags(src_magI).name, '_magnitude', '_dwi'));

        if ~(exist(strrep(fn_out, '.nii.gz', 'magn.nii'), 'file') || exist(fn_out,'file')) && doNORDIC

            ARG(I).temporal_phase = 1;
            ARG(I).phase_filter_width = 3;
            ARG(I).noise_volume_last = 0;
            [ARG(I).DIROUT,fn_out_name,~] =fileparts(fn_out);
            ARG(I).DIROUT = [ARG(I).DIROUT, '/'];
            if ~exist(ARG(I).DIROUT, 'dir')
                mkdir(ARG(I).DIROUT)
            end
            ARG(I).make_complex_nii = 1;
            ARG(I).save_gfactor_map = 1;
            % the number P >= cubic root of 11*num of direction
            ARG(I).kernel_size_PCA = [11,11,11];
            file(I).phase = fn_phase_in;
            file(I).magni = fn_magn_in;
            %file.out has no .gz only nii
            file(I).out   = strrep(fn_out_name, '.nii', '');

            I = I + 1;
        else
            disp('Step 2 will not crete ARG and file Struct, because nordic might be run before ')
        end

    end

    %% Step 3 Call NORDIC_RAW Do nordic on all functional runs under this session using parfor
    if exist('ARG', 'var')

        disp ('Step 3, the NORDIC using parfor \n')
        disp(['the length of ARG is ' length(ARG)]);
        parfor i=1:length(ARG)

            sprintf("Processing Nordic of dwi on dir- 0%s", i);
            NIFTI_NORDIC(file(i).magni, file(i).phase,file(i).out,ARG(i));

        end
        clear ARG file
        disp('This step will create 3 files: gfactorxx_dwi.nii ; dwimagn.nii ; dwiphase.nii \n');
    end
    % output of step 3 will be under output dir
    % 1. gfactor_sub-03_ses-01_task-fLoc_run-01_dwi.nii
    % 2. sub-03_ses-01_task-fLoc_run-01_dwimagn.nii
    % 3. sub-03_ses-01_task-fLoc_run-01_dwiphase.nii
    %% Step 4, wrap up nodric output to make BIDS nifti
    disp('Starting step 4, rename and gzip files as well as move json, bvec, bval files\n');
    fprintf('Do nordic is: %d,  ', doNORDIC)
    parfor src_magI=1:length(src_mags)
        %             try
        % define file names
        fn_magn_in  = fullfile(src_mags(src_magI).folder, src_mags(src_magI).name);
        fn_phase_in = strrep(fn_magn_in, '_magnitude', '_phase');
        fn_out      = fullfile(strrep(src_mags(src_magI).folder, '/BIDS',['/BIDS/derivatives/processed_nifti/analysis-' analysis_name] ),...
            strrep(src_mags(src_magI).name, '_magnitude', '_dwi'));
        gfactorFile = strrep(strrep(fn_out, '.nii.gz', '.nii'),[sub '_ses'],['gfactor_' sub '_ses']);

        if exist(gfactorFile, 'file') && doNORDIC

            disp('Gfactor orig file is here, and going to gzip the gfactor');
            % clean up
            info = niftiinfo(strrep(fn_out, '.nii.gz', 'magn.nii'));
            % remove the last one
            system(['fslroi ', strrep(fn_out, '.nii.gz', 'magn.nii'), ' ', fn_out, ' 0 -1 0 -1 0 -1 0 ', num2str(info.ImageSize(end))]);

            gzip(gfactorFile);
            % there will be a file called _dwiphase.nii, we didn't
            % remove it
            system(['rm ', strrep(fn_out, '.nii.gz', 'magn.nii'), ' ', gfactorFile, ' ' , strrep(fn_out, '.nii.gz', 'phase.nii')]);
            system(['mv ', strrep(gfactorFile, '.nii', '.nii.gz'), ' ', strrep(strrep(strrep(gfactorFile, '.nii', '.nii.gz'), '_dwi', '_gfactor'), 'gfactor_', '')]);
            fprintf (' Phase file removed, gfactor file zipped, dwi.nii.gz created for mag file %s \n', src_mags(src_magI).name);
        end

        if ~doNORDIC && ~exist(fn_out,'file')
            disp('NOT doing nordic, but we need edit the magfile')
            info = niftiinfo(fn_magn_in);
            system(['cp ',fn_magn_in, ' ',  fn_out]);
            system(['chmod 755 ', fn_out]);
            system(['fslroi ', fn_out, ' ', ...
               fn_out, ' 0 -1 0 -1 0 -1 0 ', num2str(info.ImageSize(end)-nordic_scans_end)]);
            fprintf(' No NORDIC, copied mag file and rename as dwi, also removed the last noise scan for %s\n', src_mags(src_magI).name);
        elseif doNORDIC
            disp('We need do NORDIC, but the targ file exist, we overwritte it')
        elseif exist(fn_out,'file')
            disp('Dont do NORDIC, but the fn_out file are here, do nothing')

        end
        % copy the json file
        if ~exist(strrep(fn_out, '_dwi.nii.gz', '_dwi.json'), 'file')
            system(['cp ', strrep(fn_magn_in, '_magnitude.nii.gz', '_magnitude.json'), ' ', ...
                strrep(fn_out, '_dwi.nii.gz', '_dwi.json')]);

            system(['chmod 755 ', strrep(fn_out, '_dwi.nii.gz', '_dwi.json'), ' ']);    %strrep(fn_out, '_dwi.nii.gz', '_dwi.json')
            fprintf (' json sidecar copied for dwi file %s\n', strrep(src_mags(src_magI).name, '_magnitude', '_dwi'));

        end
        % copy the bvec file
        if ~exist(strrep(fn_out, '_dwi.nii.gz', '_dwi.bvec'), 'file')
            system(['cp ', strrep(fn_magn_in, '_magnitude.nii.gz', '_magnitude.bvec'), ' ', ...
                strrep(fn_out, '_dwi.nii.gz', '_dwi.bvec')]);
            fprintf (' bvec sidecar copied for dwi file %s\n', strrep(src_mags(src_magI).name, '_magnitude', '_dwi'));

        end
        % copy the bval file
        if ~exist(strrep(fn_out, '_dwi.nii.gz', '_dwi.bval'), 'file')
            system(['cp ', strrep(fn_magn_in, '_magnitude.nii.gz', '_magnitude.bval'), ' ', ...
                strrep(fn_out, '_dwi.nii.gz', '_dwi.bval')]);
            fprintf (' bval sidecar copied for dwi file %s\n', strrep(src_mags(src_magI).name, '_magnitude', '_dwi'));

        end

    end

    time_end=datetime('now');
    fprintf('The total time for sub: %s, ses: %s, are %s\n', sub, ses, time_end-time_start);
    disp('NORDIC finished!!')
    diary off;
end
