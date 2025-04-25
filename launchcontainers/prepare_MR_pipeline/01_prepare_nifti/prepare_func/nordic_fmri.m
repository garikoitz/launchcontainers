function nordic_fmri(src_dir, output_dir, sub, ses, nordic_scans_end, doNORDIC, dotsnr, force)
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
    % module load fsl
    % module load afni
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
    %%%%%%%%%% Note to run the code currently only on BCBL local
        %%%%%%%%%% copy below to the MATLAB command line
    %%%%%%%%%% remember to edi the src dir and subseslist
    src_dir = fullfile('/bcbl/home/public/Gari/VOTCLOC/main_exp','raw_nifti');
    output_dir = fullfile('/bcbl/home/public/Gari/VOTCLOC/main_exp','BIDS');
    if ~exist(output_dir, 'dir')
       mkdir(output_dir)
    end

    nordic_scans_end = 1;
    force = false;
    doNORDIC = true;
    dotsnr = false;
    %to run it
    code_dir='/bcbl/home/public/Gari/VOTCLOC/main_exp/code/01_prepare_nifti';
    subses=importdata(fullfile(code_dir,'subseslist_nordic.txt'));

    for I=1:length(subses.data)
     sub=subses.data(I,1);
     ses=subses.data(I,2);
     if ~ isa(sub,'str')
        sub=sprintf('%02d',sub);
     end
     if ~ isa(ses,'str')
        ses=sprintf('%02d',ses);
     end

     nordic_fmri(src_dir, output_dir, sub, ses, nordic_scans_end, doNORDIC, dotsnr, force);
    end
    %}
    disp('################### \n')
    fprintf('this is sub, %s \n', sub);
    fprintf('this is ses, %s \n', ses);
    fprintf('%s \n',class(sub));

    sub=['sub-' sub];
    ses=['ses-' ses];
    if ~exist(output_dir, 'dir')
       mkdir(output_dir)
    end

    tbPath = fullfile(bvRP,'..');
    spm12Path = fullfile(tbPath, 'spm12');
    bidsmatlab_path=fullfile(tbPath,'bids-matlab');
    addpath(bidsmatlab_path);
    addpath(spm12Path);
    fmamtPath = fullfile(tbPath, 'freesurfer_mrtrix_afni_matlab_tools'); % tbUse if not installed
    addpath(genpath(fmamtPath));
    addpath(genpath(fullfile(src_dir,'..','code')));
    addpath(genpath('/bcbl/home/home_n-z/tlei/soft/launchcontainers/src/launchcontainers/py_pipeline'));
    nordicpath=fullfile(tbPath,'NORDIC_Raw');
    addpath(genpath(nordicpath));
    setenv('FSLOUTPUTTYPE', 'NIFTI_GZ');

    % start the diary, disp, fprintf , sprintf will go to diary and will be
    % captured by .e and .o
    % diary(log);
    src_sesP = fullfile(src_dir, sub, ses,'func');
    out_sesP = fullfile(output_dir, sub, ses, 'func');
    % change permission to src_sesP, output_dir all and out_sesP
    system(['chmod -R 777 ', src_sesP]);
    system(['chmod -R 777 ', out_sesP]);
    fprintf('The input dir is: %s, and the output dir is %s \n', src_sesP, out_sesP);
    if ~exist(out_sesP, 'dir')
       mkdir(out_sesP);
    end
    system(['chmod -R 777 ', out_sesP]);

    % Detect all T1w.nii.gz files
    funcmag_pattern = fullfile(src_sesP, ['*_magnitude.nii.gz']);
    src_mags = dir(funcmag_pattern);
    % Get the number of runs
    num_runs = length(src_mags);
    runs = arrayfun(@(x) sprintf('%02d', x), 1:num_runs, 'UniformOutput', false);
    fprintf('Number of runs for are %i \n', num_runs);


    % nordic
    %% Step 1, check if the BIDS has been processed, if not, create backups
    % loop over all the mag files in src_filder
    % if there are no mag back up files, create backups, delete the noise
    % scans to only 1 left
    disp('### Starting step 1, preaparing the  mag and phase for nordic \n')
    time_start=datetime('now');
    parfor src_magI=1:length(src_mags) % src_magI=1
        prepare_nordic_bold_nifti(fullfile(src_mags(src_magI).folder, src_mags(src_magI).name),nordic_scans_end ,force)
    end

    %% Step 2, prepare ARG struct for each run of the the magnitude.nii.gz
    disp('### Starting step2, preparing the ARG and file struct storing the input and output file info \n')
    clear ARG

    I = 1; %ARG file index
    % update src_mags
    funcmag_pattern = fullfile(src_sesP, ['*_magnitude.nii.gz']);
    src_mags = dir(funcmag_pattern);
    num_runs = length(src_mags);
    runs = arrayfun(@(x) sprintf('%02d', x), 1:num_runs, 'UniformOutput', false);
    fprintf('Number of runs after prepare for are %i \n', num_runs);
    for src_magI=1:length(src_mags)
        % define file names
        fn_magn_in  = fullfile(src_mags(src_magI).folder, src_mags(src_magI).name);
        fn_phase_in = strrep(fn_magn_in, '_magnitude', '_phase');
        fn_out      = fullfile(out_sesP, strrep(src_mags(src_magI).name, '_magnitude', '_bold'));

        if ~(exist(strrep(fn_out, '.nii.gz', 'magn.nii'), 'file') || exist(fn_out,'file')) && doNORDIC

            ARG(I).temporal_phase = 1;
            ARG(I).phase_filter_width = 10;
            ARG(I).noise_volume_last = 1;
            [ARG(I).DIROUT,fn_out_name,~] =fileparts(fn_out);
            ARG(I).DIROUT = [ARG(I).DIROUT, '/'];
            if ~exist(ARG(I).DIROUT, 'dir')
                mkdir(ARG(I).DIROUT)
            end
            ARG(I).make_complex_nii = 1;
            ARG(I).save_gfactor_map = 1;

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

            sprintf("Processing Nordic on run- 0%s", i);
            NIFTI_NORDIC(file(i).magni, file(i).phase,file(i).out,ARG(i));

        end
        clear ARG file
        disp('This step will create 3 files: gfactorxx_bold.nii ; boldmagn.nii ; boldphase.nii \n');
    end
    % output of step 3 will be under output dir
    % 1. gfactor_sub-03_ses-01_task-fLoc_run-01_bold.nii
    % 2. sub-03_ses-01_task-fLoc_run-01_boldmagn.nii
    % 3. sub-03_ses-01_task-fLoc_run-01_boldphase.nii
    %% Step 4, wrap up nodric output to make BIDS nifti
    disp('### Starting step 4, rename and gzip files as well as move json file \n');
    fprintf('Do nordic is: %d, dotsnr is %d\n', doNORDIC, dotsnr)
    parfor src_magI=1:length(src_mags)
        %             try
        % define file names
        fn_magn_in  = fullfile(src_mags(src_magI).folder, src_mags(src_magI).name);
        fn_phase_in = strrep(fn_magn_in, '_magnitude', '_phase');
        fn_out      = fullfile(out_sesP,strrep(src_mags(src_magI).name, '_magnitude', '_bold'));
        gfactorFile = strrep(strrep(fn_out, '.nii.gz', '.nii'),[sub '_ses'],['gfactor_' sub '_ses']);

        if exist(gfactorFile, 'file') && doNORDIC

            disp('Gfactor orig file is here, and going to gzip the gfactor');
            % clean up
            info = niftiinfo(strrep(fn_out, '.nii.gz', 'magn.nii'));
            % remove the last one
            system(['fslroi ', strrep(fn_out, '.nii.gz', 'magn.nii'), ' ', fn_out, ' 0 -1 0 -1 0 -1 0 ', num2str(info.ImageSize(end)-1)]);

            gzip(gfactorFile);
            % there will be a file called _boldphase.nii, we didn't
            % remove it
            system(['rm ', strrep(fn_out, '.nii.gz', 'magn.nii'), ' ', gfactorFile, ' ' , strrep(fn_out, '.nii.gz', 'phase.nii')]);
            system(['mv ', strrep(gfactorFile, '.nii', '.nii.gz'), ' ', strrep(strrep(strrep(gfactorFile, '.nii', '.nii.gz'), '_bold', '_gfactor'), 'gfactor_', '')]);
            fprintf (' Phase file removed, gfactor file zipped, bold.nii.gz created for mag file %s \n', src_mags(src_magI).name);
        end

        if ~doNORDIC && ~exist(fn_out,'file')
            disp('NOT doing nordic, but we need edit the magfile')
            info = niftiinfo(fn_magn_in);
            system(['cp ',fn_magn_in, ' ', fn_out]);
            system(['chmod 755 ', fn_out]);
            system(['fslroi ', fn_out, ' ', ...
               fn_out, ' 0 -1 0 -1 0 -1 0 ', num2str(info.ImageSize(end)-nordic_scans_end)]);
            fprintf(' No NORDIC, copied mag file and rename as bold, also removed the last noise scan for %s\n', src_mags(src_magI).name);
        elseif doNORDIC
            disp('We need do NORDIC, so not just simply edit mag')
        elseif exist(fn_out,'file')
            disp('Dont do NORDIC, but the fn_out file are here, do nothing')

        end
        % copy the json file
        if ~exist(strrep(fn_out, '_bold.nii.gz', '_bold.json'), 'file')
            system(['cp ', strrep(fn_magn_in, '_magnitude.nii.gz', '_magnitude.json'), ' ', ...
                strrep(fn_out, '_bold.nii.gz', '_bold.json')]);

            system(['chmod 755 ', strrep(fn_out, '_bold.nii.gz', '_bold.json'), ' ']);    %strrep(fn_out, '_bold.nii.gz', '_bold.json')
            fprintf (' json sidecar copied for bold file %s\n', strrep(src_mags(src_magI).name, '_magnitude', '_bold'));

        end


        % copy the sbref
        % the sbref here is only converted the mag, and it is called
        % _sbref.nii.gz, didn't add the _part-mag
        src_sbref = strrep(fn_magn_in, '_magnitude.nii.gz', '_sbref.nii.gz');
        src_sbref_json = strrep(fn_magn_in, '_magnitude.nii.gz', '_sbref.json');
        dst_sbref = strrep(fn_out, '_bold.nii.gz', '_sbref.nii.gz');
        dst_sbref_json = strrep(fn_out, '_bold.nii.gz', '_sbref.json');

        if ~(exist(dst_sbref, 'file') && (exist(dst_sbref_json, 'file')))
            system(['cp ', src_sbref, ' ', dst_sbref]);
            system(['cp ', src_sbref_json, ' ', dst_sbref_json]);
            fprintf('sbref copied to %s\n', dst_sbref(end-20:end));
            system(['chmod 755 ', dst_sbref_json, ' ']);
        end

    end


    if dotsnr
        bolds = dir(fullfile(out_sesP, ['*_bold.nii.gz']));
        src_mags  =dir(funcmag_pattern);
        bolds(contains({bolds.name}, 'gfactor')) = [];


        parfor nb=1:length(bolds)

            % Define file names
            magFile  = fullfile(src_mags(nb).folder, src_mags(nb).name);
            boldFile = fullfile(bolds(nb).folder, bolds(nb).name);


            tsnrFile = strrep(boldFile,'bold','tsnr_postNordic');
            magtsnrFile = strrep(boldFile,'bold','tsnr_preNordic');
            gfactorFile = strrep(boldFile,'bold','gfactor');
            tsnrGfactorFile = strrep(gfactorFile,'gfactor','gfactorSameSpace');

            % pre NORDIC tSNR
            magHeader = niftiinfo(magFile);
            magData = single(niftiread(magHeader));
            magtsnrData = mean(magData,4) ./ std(magData,1,4);
            magtsnrData(isnan(magtsnrData)) = 0;
            magHeader.ImageSize = size(magtsnrData);
            magHeader.PixelDimensions=magHeader.PixelDimensions(1:3);
            magHeader.Datatype = 'single' ;
            niftiwrite(magtsnrData, strrep(magtsnrFile, '.nii', ''), magHeader,'compressed',true)

            % post NORDIC tSNR
            boldHeader = niftiinfo(boldFile);
            boldData = single(niftiread(boldHeader));

            tsnrData = mean(boldData,4) ./ std(boldData,1,4);
            boldHeader.ImageSize = size(tsnrData);
            boldHeader.PixelDimensions=boldHeader.PixelDimensions(1:3);
            boldHeader.Datatype = 'single' ;
            niftiwrite(tsnrData, strrep(tsnrFile, '.nii', ''),boldHeader,'compressed',true)

            % Write g factor in same space

            gHeader =  niftiinfo(gfactorFile);
            gfactorData = single(niftiread(gHeader));
            gHeader.ImageSize=size(gfactorData);
            gHeader.PixelDimensions=gHeader.PixelDimensions(1:3);
            gHeader.Datatype = 'single' ;
            niftiwrite(gfactorData, strrep(tsnrGfactorFile, '.nii', ''), gHeader,'compressed',true)
            fprintf('TSNR map created for %s\n', bolds(nb).name);


        end
    end
    time_end=datetime('now');
    fprintf('The total time for sub: %s, ses: %s, and %s of runs are %s\n', sub, ses, num_runs, time_end-time_start);
    disp('NORDIC finished!!')
    % diary off;
end
