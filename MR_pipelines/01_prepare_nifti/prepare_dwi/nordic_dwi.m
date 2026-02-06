function nordic_dwi(tbPath, src_dir, output_dir, sub, ses, nordic_scans_end, doNORDIC, dotsnr, force)
% MIT License
% Copyright (c) 2024-2025 Yongning Lei
% Modified for DWI processing with NORDIC

% Permission is hereby granted, free of charge, to any person obtaining a copy of this software
% and associated documentation files (the "Software"), to deal in the Software without restriction,
% including without limitation the rights to use, copy, modify, merge, publish, distribute, sublicense,
% and/or sell copies of the Software, and to permit persons to whom the Software is furnished to do so,
% subject to the following conditions:

% The above copyright notice and this permission notice shall be included in all copies or substantial
% portions of the Software.

    disp('################### DWI NORDIC Processing \n');
    fprintf('this is sub, %s \n', sub);
    fprintf('this is ses, %s \n', ses);
    fprintf('%s \n',class(sub));

    sub=['sub-' sub];
    ses=['ses-' ses];
    if ~exist(output_dir, 'dir')
       mkdir(output_dir);
    end
    
    spm12Path = fullfile(tbPath, 'spm12');
    bidsmatlab_path=fullfile(tbPath,'bids-matlab');
    addpath(bidsmatlab_path);
    addpath(spm12Path);
    fmamtPath = fullfile(tbPath, 'freesurfer_mrtrix_afni_matlab_tools');
    addpath(genpath(fmamtPath));
    addpath(genpath(fullfile(src_dir,'..','code')));
    addpath(genpath('/bcbl/home/home_n-z/tlei/soft/launchcontainers/src/launchcontainers/MR_pipelines'));
    nordicpath=fullfile(tbPath,'NORDIC_Raw');
    addpath(genpath(nordicpath));
    setenv('FSLOUTPUTTYPE', 'NIFTI_GZ');

    % Setup directories - DWI uses 'dwi' folder instead of 'func'
    src_sesP = fullfile(src_dir, sub, ses, 'dwi');
    out_sesP = fullfile(output_dir, sub, ses, 'dwi');
    
    % Change permissions
    system(['chmod -R 777 ', src_sesP]);
    fprintf('The input dir is: %s, and the output dir is %s \n', src_sesP, out_sesP);
    if ~exist(out_sesP, 'dir')
       mkdir(out_sesP);
    end
    system(['chmod -R 777 ', out_sesP]);

    % Detect all DWI magnitude files
    dwimag_pattern = fullfile(src_sesP, ['*_magnitude.nii.gz']);
    src_mags = dir(dwimag_pattern);
    
    % Get the number of runs
    num_runs = length(src_mags);
    runs = arrayfun(@(x) sprintf('%02d', x), 1:num_runs, 'UniformOutput', false);
    fprintf('Number of DWI runs: %i \n', num_runs);

    %% Step 1: Prepare magnitude and phase for NORDIC
    disp('### Starting step 1, preparing the mag and phase for NORDIC DWI \n')
    time_start=datetime('now');
    
    parfor src_magI=1:length(src_mags)
        prepare_nordic_dwi_nifti(fullfile(src_mags(src_magI).folder, src_mags(src_magI).name), nordic_scans_end, force);
    end

    %% Step 2: Prepare ARG struct for each DWI run
    disp('### Starting step 2, preparing the ARG and file struct for DWI \n')
    clear ARG

    I = 1; % ARG file index
    
    % Update src_mags after preparation
    dwimag_pattern = fullfile(src_sesP, ['*_magnitude.nii.gz']);
    src_mags = dir(dwimag_pattern);
    num_runs = length(src_mags);
    fprintf('Number of runs after preparation: %i \n', num_runs);
    
    for src_magI=1:length(src_mags)
        % Define file names
        fn_magn_in  = fullfile(src_mags(src_magI).folder, src_mags(src_magI).name);
        fn_phase_in = strrep(fn_magn_in, '_magnitude', '_phase');
        fn_out      = fullfile(out_sesP, strrep(src_mags(src_magI).name, '_magnitude', '_dwi'));

        if ~(exist(strrep(fn_out, '.nii.gz', 'magn.nii'), 'file') || exist(fn_out,'file')) && doNORDIC

            % DWI-specific NORDIC parameters
            ARG(I).temporal_phase = 3;              % DWI uses 3 (not 1)
            ARG(I).phase_filter_width = 3;          % Conservative for DWI
            ARG(I).noise_volume_last = 0;           % No noise volumes (use MPPCA estimation)
            
            [ARG(I).DIROUT, fn_out_name, ~] = fileparts(fn_out);
            ARG(I).DIROUT = [ARG(I).DIROUT, '/'];
            if ~exist(ARG(I).DIROUT, 'dir')
                mkdir(ARG(I).DIROUT)
            end
            
            ARG(I).make_complex_nii = 1;
            ARG(I).save_gfactor_map = 1;
            
            % DWI-specific: kernel size is 11x11x11 (11:1 ratio is for spatial:temporal)
            ARG(I).kernel_size_PCA = [11, 11, 11];  
            
            file(I).phase = fn_phase_in;
            file(I).magni = fn_magn_in;
            file(I).out   = strrep(fn_out_name, '.nii', '');  % No .gz, only .nii

            I = I + 1;
        else
            disp('Step 2 will not create ARG and file Struct, because NORDIC might have been run before')
        end
    end

    %% Step 3: Call NORDIC_RAW - Do NORDIC on all DWI runs using parfor
    if exist('ARG', 'var')
        disp('Step 3: Running NORDIC using parfor \n')
        fprintf('The length of ARG is %d\n', length(ARG));
        
        parfor i=1:length(ARG)
            fprintf("Processing NORDIC on DWI run %d\n", i);
            NIFTI_NORDIC(file(i).magni, file(i).phase, file(i).out, ARG(i));
        end
        
        clear ARG file
        disp('This step creates 3 files: gfactor_*_dwi.nii ; *_dwimagn.nii ; *_dwiphase.nii \n');
    end

    %% Step 4: Wrap up NORDIC output to make BIDS-compliant nifti
    disp('### Starting step 4, rename and gzip files, move json and bvec/bval files \n');
    fprintf('doNORDIC is: %d, dotsnr is %d\n', doNORDIC, dotsnr)
    
    parfor src_magI=1:length(src_mags)
        % Define file names
        fn_magn_in  = fullfile(src_mags(src_magI).folder, src_mags(src_magI).name);
        fn_phase_in = strrep(fn_magn_in, '_magnitude', '_phase');
        fn_out      = fullfile(out_sesP, strrep(src_mags(src_magI).name, '_magnitude', '_dwi'));
        gfactorFile = strrep(strrep(fn_out, '.nii.gz', '.nii'), [sub '_ses'], ['gfactor_' sub '_ses']);

        if exist(gfactorFile, 'file') && doNORDIC
            disp('Gfactor file exists, processing NORDIC outputs');
            
            % Clean up - remove noise volumes if any
            info = niftiinfo(strrep(fn_out, '.nii.gz', 'magn.nii'));
            
            if nordic_scans_end > 0
                % Remove last volumes (noise scans)
                system(['fslroi ', strrep(fn_out, '.nii.gz', 'magn.nii'), ' ', fn_out, ...
                       ' 0 -1 0 -1 0 -1 0 ', num2str(info.ImageSize(end) - nordic_scans_end)]);
            else
                % No noise volumes to remove, just rename
                system(['mv ', strrep(fn_out, '.nii.gz', 'magn.nii'), ' ', ...
                       strrep(fn_out, '.nii.gz', '_temp.nii')]);
                gzip(strrep(fn_out, '.nii.gz', '_temp.nii'));
                system(['mv ', strrep(fn_out, '.nii.gz', '_temp.nii.gz'), ' ', fn_out]);
                system(['rm ', strrep(fn_out, '.nii.gz', '_temp.nii')]);
            end
            
            % Gzip gfactor file
            gzip(gfactorFile);
            
            % Remove intermediate files
            system(['rm ', gfactorFile, ' ', strrep(fn_out, '.nii.gz', 'phase.nii')]);
            
            % Rename gfactor file to proper BIDS format
            system(['mv ', strrep(gfactorFile, '.nii', '.nii.gz'), ' ', ...
                   strrep(strrep(strrep(gfactorFile, '.nii', '.nii.gz'), '_dwi', '_gfactor'), 'gfactor_', '')]);
            
            fprintf('Phase file removed, gfactor file zipped, dwi.nii.gz created for %s \n', src_mags(src_magI).name);
        end

        if ~doNORDIC && ~exist(fn_out, 'file')
            disp('NOT doing NORDIC, but copying and editing mag file')
            info = niftiinfo(fn_magn_in);
            system(['cp ', fn_magn_in, ' ', fn_out]);
            system(['chmod 755 ', fn_out]);
            
            if nordic_scans_end > 0
                system(['fslroi ', fn_out, ' ', fn_out, ...
                       ' 0 -1 0 -1 0 -1 0 ', num2str(info.ImageSize(end) - nordic_scans_end)]);
            end
            
            fprintf('No NORDIC, copied mag file and renamed as dwi, removed last noise scans for %s\n', src_mags(src_magI).name);
        elseif doNORDIC
            disp('Doing NORDIC, not just editing mag file')
        elseif exist(fn_out, 'file')
            disp('Not doing NORDIC, but fn_out file exists, do nothing')
        end
        
        % Copy JSON sidecar
        if ~exist(strrep(fn_out, '_dwi.nii.gz', '_dwi.json'), 'file')
            system(['cp ', strrep(fn_magn_in, '_magnitude.nii.gz', '_magnitude.json'), ' ', ...
                   strrep(fn_out, '_dwi.nii.gz', '_dwi.json')]);
            system(['chmod 755 ', strrep(fn_out, '_dwi.nii.gz', '_dwi.json')]);
            fprintf('JSON sidecar copied for dwi file %s\n', strrep(src_mags(src_magI).name, '_magnitude', '_dwi'));
        end
        
        % Copy bvec and bval files (critical for DWI!)
        src_bvec = strrep(fn_magn_in, '_magnitude.nii.gz', '_magnitude.bvec');
        src_bval = strrep(fn_magn_in, '_magnitude.nii.gz', '_magnitude.bval');
        dst_bvec = strrep(fn_out, '_dwi.nii.gz', '_dwi.bvec');
        dst_bval = strrep(fn_out, '_dwi.nii.gz', '_dwi.bval');
        
        if ~exist(dst_bvec, 'file')
            system(['cp ', src_bvec, ' ', dst_bvec]);
            system(['chmod 755 ', dst_bvec]);
            fprintf('bvec copied to %s\n', dst_bvec);
        end
        
        if ~exist(dst_bval, 'file')
            system(['cp ', src_bval, ' ', dst_bval]);
            system(['chmod 755 ', dst_bval]);
            fprintf('bval copied to %s\n', dst_bval);
        end
    end

    %% Step 5: Calculate SNR/tSNR maps for DWI (b0 volumes only)
    if dotsnr
        dwis = dir(fullfile(out_sesP, ['*_dwi.nii.gz']));
        src_mags = dir(dwimag_pattern);
        dwis(contains({dwis.name}, 'gfactor')) = [];

        parfor nd=1:length(dwis)
            try
                % Define file names
                magFile  = fullfile(src_mags(nd).folder, src_mags(nd).name);
                dwiFile  = fullfile(dwis(nd).folder, dwis(nd).name);
                
                % Find corresponding bval file (corrected path)
                bvalFile = strrep(magFile, '_magnitude.nii.gz', '_magnitude.bval');
                
                % Output file names
                tsnrFile_postNordic = strrep(dwiFile, '_dwi', '_desc-b0_tsnr_postNordic');
                tsnrFile_preNordic  = strrep(dwiFile, '_dwi', '_desc-b0_tsnr_preNordic');
                snrFile_postNordic  = strrep(dwiFile, '_dwi', '_desc-b0_snr_postNordic');
                snrFile_preNordic   = strrep(dwiFile, '_dwi', '_desc-b0_snr_preNordic');
                gfactorFile = strrep(dwiFile, '_dwi', '_gfactor');
                tsnrGfactorFile = strrep(gfactorFile, '_gfactor', '_gfactorSameSpace');

                % Read bval file to identify b0 volumes
                if exist(bvalFile, 'file')
                    bvals = load(bvalFile);
                    
                    % Handle both row and column formats
                    if size(bvals, 1) > 1
                        bvals = bvals';
                    end
                    
                    % Find b0 volumes (b-value < 50)
                    b0_indices = find(bvals < 50);
                    
                    if ~isempty(b0_indices)
                        fprintf('Processing %s: Found %d b0 volumes out of %d total volumes\n', ...
                            dwis(nd).name, length(b0_indices), length(bvals));
                        
                        %% Pre-NORDIC Processing
                        magHeader = niftiinfo(magFile);
                        magData = single(niftiread(magHeader));
                        magData_b0 = magData(:, :, :, b0_indices);
                        
                        % Calculate mean b0 image
                        meanb0_pre = mean(magData_b0, 4);
                        
                        % Create brain mask using Otsu's method on mean b0
                        threshold_pre = graythresh(meanb0_pre(:)) * max(meanb0_pre(:));
                        brainMask_pre = meanb0_pre > threshold_pre;
                        
                        % Morphological operations to clean up mask
                        se = strel('sphere', 3);
                        brainMask_pre = imopen(brainMask_pre, se);
                        brainMask_pre = imclose(brainMask_pre, se);
                        brainMask_pre = imfill(brainMask_pre, 'holes');
                        
                        % Calculate temporal std across b0 volumes
                        stdb0_pre = std(magData_b0, 1, 4);
                        
                        % Method 1: Temporal SNR (tSNR) - masked
                        % tSNR = mean / std (temporal)
                        tsnrData_pre = meanb0_pre ./ stdb0_pre;
                        
                        % Apply brain mask and handle invalid values
                        tsnrData_pre(~brainMask_pre) = 0;
                        tsnrData_pre(isnan(tsnrData_pre)) = 0;
                        tsnrData_pre(isinf(tsnrData_pre)) = 0;
                        tsnrData_pre(tsnrData_pre > 500) = 0;  % Cap unrealistic values
                        
                        % Method 2: SNR using background noise
                        % Create background mask (inverse of brain, eroded)
                        se_erode = strel('sphere', 5);
                        backgroundMask_pre = ~imdilate(brainMask_pre, se_erode);
                        backgroundMask_pre = backgroundMask_pre & (meanb0_pre > 0);
                        
                        % Calculate noise from background
                        background_std_pre = std(meanb0_pre(backgroundMask_pre));
                        
                        % SNR = signal / background_noise
                        snrData_pre = meanb0_pre / background_std_pre;
                        snrData_pre(~brainMask_pre) = 0;
                        snrData_pre(isnan(snrData_pre)) = 0;
                        snrData_pre(isinf(snrData_pre)) = 0;
                        
                        % Update header for 3D output
                        magHeader.ImageSize = size(tsnrData_pre);
                        magHeader.PixelDimensions = magHeader.PixelDimensions(1:3);
                        magHeader.Datatype = 'single';
                        
                        % Write pre-NORDIC tSNR map
                        niftiwrite(tsnrData_pre, strrep(tsnrFile_preNordic, '.nii', ''), ...
                                magHeader, 'compressed', true);
                        fprintf('  Pre-NORDIC b0 tSNR map saved: %s\n', tsnrFile_preNordic);
                        
                        % Write pre-NORDIC SNR map
                        niftiwrite(snrData_pre, strrep(snrFile_preNordic, '.nii', ''), ...
                                magHeader, 'compressed', true);
                        fprintf('  Pre-NORDIC b0 SNR map saved: %s\n', snrFile_preNordic);
                        
                        % Report pre-NORDIC statistics
                        median_tsnr_pre = median(tsnrData_pre(brainMask_pre));
                        mean_snr_pre = mean(snrData_pre(brainMask_pre));
                        fprintf('  Pre-NORDIC - Median tSNR: %.2f, Mean SNR: %.2f\n', ...
                            median_tsnr_pre, mean_snr_pre);

                        %% Post-NORDIC Processing
                        dwiHeader = niftiinfo(dwiFile);
                        dwiData = single(niftiread(dwiHeader));
                        dwiData_b0 = dwiData(:, :, :, b0_indices);
                        
                        % Calculate mean b0 image
                        meanb0_post = mean(dwiData_b0, 4);
                        
                        % Use same brain mask for fair comparison (based on post-NORDIC data)
                        threshold_post = graythresh(meanb0_post(:)) * max(meanb0_post(:));
                        brainMask_post = meanb0_post > threshold_post;
                        brainMask_post = imopen(brainMask_post, se);
                        brainMask_post = imclose(brainMask_post, se);
                        brainMask_post = imfill(brainMask_post, 'holes');
                        
                        % Calculate temporal std across b0 volumes
                        stdb0_post = std(dwiData_b0, 1, 4);
                        
                        % Method 1: Temporal SNR (tSNR) - masked
                        tsnrData_post = meanb0_post ./ stdb0_post;
                        
                        % Apply brain mask and handle invalid values
                        tsnrData_post(~brainMask_post) = 0;
                        tsnrData_post(isnan(tsnrData_post)) = 0;
                        tsnrData_post(isinf(tsnrData_post)) = 0;
                        tsnrData_post(tsnrData_post > 500) = 0;  % Cap unrealistic values
                        
                        % Method 2: SNR using background noise
                        backgroundMask_post = ~imdilate(brainMask_post, se_erode);
                        backgroundMask_post = backgroundMask_post & (meanb0_post > 0);
                        
                        background_std_post = std(meanb0_post(backgroundMask_post));
                        
                        snrData_post = meanb0_post / background_std_post;
                        snrData_post(~brainMask_post) = 0;
                        snrData_post(isnan(snrData_post)) = 0;
                        snrData_post(isinf(snrData_post)) = 0;
                        
                        % Update header for 3D output
                        dwiHeader.ImageSize = size(tsnrData_post);
                        dwiHeader.PixelDimensions = dwiHeader.PixelDimensions(1:3);
                        dwiHeader.Datatype = 'single';
                        
                        % Write post-NORDIC tSNR map
                        niftiwrite(tsnrData_post, strrep(tsnrFile_postNordic, '.nii', ''), ...
                                dwiHeader, 'compressed', true);
                        fprintf('  Post-NORDIC b0 tSNR map saved: %s\n', tsnrFile_postNordic);
                        
                        % Write post-NORDIC SNR map
                        niftiwrite(snrData_post, strrep(snrFile_postNordic, '.nii', ''), ...
                                dwiHeader, 'compressed', true);
                        fprintf('  Post-NORDIC b0 SNR map saved: %s\n', snrFile_postNordic);
                        
                        % Report post-NORDIC statistics
                        median_tsnr_post = median(tsnrData_post(brainMask_post));
                        mean_snr_post = mean(snrData_post(brainMask_post));
                        fprintf('  Post-NORDIC - Median tSNR: %.2f, Mean SNR: %.2f\n', ...
                            median_tsnr_post, mean_snr_post);
                        
                        % Calculate improvements
                        tsnr_improvement = median_tsnr_post / median_tsnr_pre;
                        snr_improvement = mean_snr_post / mean_snr_pre;
                        
                        fprintf('  *** tSNR improvement: %.2fx ***\n', tsnr_improvement);
                        fprintf('  *** SNR improvement: %.2fx ***\n', snr_improvement);
                        
                    else
                        warning('No b0 volumes found in %s (all bvals >= 50)', dwis(nd).name);
                    end
                else
                    warning('bval file not found: %s', bvalFile);
                end

                %% Write g-factor in same space
                if exist(gfactorFile, 'file')
                    gHeader = niftiinfo(gfactorFile);
                    gfactorData = single(niftiread(gHeader));
                    gHeader.ImageSize = size(gfactorData);
                    gHeader.PixelDimensions = gHeader.PixelDimensions(1:3);
                    gHeader.Datatype = 'single';
                    niftiwrite(gfactorData, strrep(tsnrGfactorFile, '.nii', ''), ...
                            gHeader, 'compressed', true);
                    fprintf('  G-factor map saved: %s\n', tsnrGfactorFile);
                end
                
            catch ME
                warning('Error processing SNR/tSNR for %s: %s', dwis(nd).name, ME.message);
                fprintf('  Stack trace:\n');
                for k = 1:length(ME.stack)
                    fprintf('    %s (line %d)\n', ME.stack(k).name, ME.stack(k).line);
                end
            end
        end
        
        fprintf('\n=== SNR/tSNR calculation complete for all runs ===\n');
    end

    time_end = datetime('now');
    fprintf('Total time for sub: %s, ses: %s, with %d runs: %s\n', ...
        sub, ses, num_runs, time_end - time_start);
    disp('DWI NORDIC finished!!')
end