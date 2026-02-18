function nordic_dwi(tbPath, src_dir, output_dir, sub, ses, nordic_scans_end, doNORDIC, force)
% MIT License
% Copyright (c) 2024-2026 Yongning Lei
% Modified for DWI processing with NORDIC

% Permission is hereby granted, free of charge, to any person obtaining a copy of this software
% and associated documentation files (the "Software"), to deal in the Software without restriction,
% including without limitation the rights to use, copy, modify, merge, publish, distribute, sublicense,
% and/or sell copies of the Software, and to permit persons to whom the Software is furnished to do so,
% subject to the following conditions:

% The above copyright notice and this permission notice shall be included in all copies or substantial
% portions of the Software.

    disp('################### DWI NORDIC Processing');
    fprintf('this is sub: %s\n', sub);
    fprintf('this is ses: %s\n', ses);

    sub = ['sub-' sub];
    ses = ['ses-' ses];
    if ~exist(output_dir, 'dir')
       mkdir(output_dir);
    end
    
    spm12Path = fullfile(tbPath, 'spm12');
    bidsmatlab_path = fullfile(tbPath, 'bids-matlab');
    addpath(bidsmatlab_path);
    addpath(spm12Path);
    fmamtPath = fullfile(tbPath, 'freesurfer_mrtrix_afni_matlab_tools');
    addpath(genpath(fmamtPath));
    addpath(genpath(fullfile(src_dir, '..', 'code')));
    addpath(genpath('/bcbl/home/home_n-z/tlei/soft/launchcontainers/src/launchcontainers/MR_pipelines'));
    nordicpath = fullfile(tbPath, 'NORDIC_Raw');
    addpath(genpath(nordicpath));
    setenv('FSLOUTPUTTYPE', 'NIFTI_GZ');

    % Dynamic acq label based on doNORDIC
    % CHANGED: acq-* replacement instead of desc- prefix
    if doNORDIC
        acq_label = 'nordic';
    else
        acq_label = 'magonly';
    end
    fprintf('Output acq label: acq-%s\n', acq_label);

    % Setup directories
    src_sesP = fullfile(src_dir, sub, ses, 'dwi');
    out_sesP = fullfile(output_dir, sub, ses, 'dwi');
    
    system(['chmod -R 777 ', src_sesP]);
    fprintf('Input dir: %s\nOutput dir: %s\n', src_sesP, out_sesP);
    if ~exist(out_sesP, 'dir')
       mkdir(out_sesP);
    end
    system(['chmod -R 777 ', out_sesP]);

    % Detect all DWI magnitude files (exclude _orig files)
    dwimag_pattern = fullfile(src_sesP, '*_magnitude.nii.gz');
    src_mags = dir(dwimag_pattern);
    src_mags(contains({src_mags.name}, '_orig')) = [];
    
    num_runs = length(src_mags);
    fprintf('Number of DWI runs: %i\n', num_runs);

    %% Step 1: Prepare magnitude and phase for NORDIC
    disp('### Step 1: Preparing mag and phase files');
    time_start = datetime('now');
    
    parfor src_magI = 1:length(src_mags)
        prepare_nordic_dwi_nifti( ...
            fullfile(src_mags(src_magI).folder, src_mags(src_magI).name), ...
            nordic_scans_end, doNORDIC, force);
    end

    %% Step 2 & 3: Prepare ARG struct and run NORDIC (only when doNORDIC)
    if doNORDIC
        disp('### Step 2: Preparing ARG and file struct for NORDIC');
        clear ARG

        I = 1;
        
        % Re-read src_mags after preparation
        src_mags = dir(dwimag_pattern);
        src_mags(contains({src_mags.name}, '_orig')) = [];
        num_runs = length(src_mags);
        fprintf('Number of runs after preparation: %i\n', num_runs);
        
        for src_magI = 1:length(src_mags)
            fn_magn_in  = fullfile(src_mags(src_magI).folder, src_mags(src_magI).name);
            fn_phase_in = strrep(fn_magn_in, '_magnitude', '_phase');
            % CHANGED: output name uses rename helper (acq-* -> acq-nordic, _magnitude -> _dwi)
            fn_out      = fullfile(out_sesP, rename_mag_to_output(src_mags(src_magI).name, acq_label));

            if ~(exist(strrep(fn_out, '.nii.gz', 'magn.nii'), 'file') || exist(fn_out, 'file')) || force

                ARG(I).temporal_phase = 3;
                ARG(I).phase_filter_width = 3;
                ARG(I).noise_volume_last = 0;
                
                [ARG(I).DIROUT, fn_out_name, ~] = fileparts(fn_out);
                ARG(I).DIROUT = [ARG(I).DIROUT, '/'];
                if ~exist(ARG(I).DIROUT, 'dir')
                    mkdir(ARG(I).DIROUT)
                end
                
                ARG(I).make_complex_nii = 1;
                ARG(I).save_gfactor_map = 1;
                ARG(I).kernel_size_PCA = [11, 11, 11];
                
                file(I).phase = fn_phase_in;
                file(I).magni = fn_magn_in;
                file(I).out   = strrep(fn_out_name, '.nii', '');

                I = I + 1;
            else
                fprintf('Step 2: Skipping %s - output already exists and force=0\n', fn_magn_in);
            end
        end

        %% Step 3: Run NORDIC
        if exist('ARG', 'var')
            fprintf('### Step 3: Running NORDIC on %d runs\n', length(ARG));
            
            parfor i = 1:length(ARG)
                fprintf('Processing NORDIC on DWI run %d\n', i);
                NIFTI_NORDIC(file(i).magni, file(i).phase, file(i).out, ARG(i));
            end
            
            clear ARG file
            disp('Step 3 done: created gfactor, magn, phase .nii files');
        else
            disp('Step 3: No ARG struct created, skipping NORDIC.');
        end
    else
        disp('### Skipping steps 2-3 (doNORDIC=0)');
    end

    %% Step 4: Create BIDS-compliant output files
    disp('### Step 4: Creating BIDS-compliant output files');
    fprintf('doNORDIC=%d, force=%d, acq_label=acq-%s\n', doNORDIC, force, acq_label);
    
    % Re-read src_mags
    src_mags = dir(dwimag_pattern);
    src_mags(contains({src_mags.name}, '_orig')) = [];
    
    parfor src_magI = 1:length(src_mags)
        fn_magn_in  = fullfile(src_mags(src_magI).folder, src_mags(src_magI).name);
        fn_out      = fullfile(out_sesP, rename_mag_to_output(src_mags(src_magI).name, acq_label));
        gfactorFile = strrep(strrep(fn_out, '.nii.gz', '.nii'), ...
                      [sub '_ses'], ['gfactor_' sub '_ses']);

        % Check if this file has associated bvec/bval (AP has them, PA does not)
        src_bvec_local = strrep(fn_magn_in, '_magnitude.nii.gz', '_magnitude.bvec');
        src_bval_local = strrep(fn_magn_in, '_magnitude.nii.gz', '_magnitude.bval');
        bvec_orig = strrep(src_bvec_local, '.bvec', '_orig.bvec');
        bval_orig = strrep(src_bval_local, '.bval', '_orig.bval');
        has_bvec_bval = exist(src_bvec_local, 'file') || exist(bvec_orig, 'file');

        % Check if orig files exist for this specific file
        mag_orig_local = strrep(fn_magn_in, '.nii.gz', '_orig.nii.gz');
        has_orig = exist(mag_orig_local, 'file');

        %% --- doNORDIC: process NORDIC outputs ---
        if doNORDIC && exist(gfactorFile, 'file')
            disp('Processing NORDIC outputs (gfactor found)');
            
            info = niftiinfo(strrep(fn_out, '.nii.gz', 'magn.nii'));
            
            if nordic_scans_end > 0
                system(['fslroi ', strrep(fn_out, '.nii.gz', 'magn.nii'), ' ', fn_out, ...
                       ' 0 -1 0 -1 0 -1 0 ', num2str(info.ImageSize(end) - nordic_scans_end)]);
            else
                system(['mv ', strrep(fn_out, '.nii.gz', 'magn.nii'), ' ', ...
                       strrep(fn_out, '.nii.gz', '_temp.nii')]);
                gzip(strrep(fn_out, '.nii.gz', '_temp.nii'));
                system(['mv ', strrep(fn_out, '.nii.gz', '_temp.nii.gz'), ' ', fn_out]);
                system(['rm -f ', strrep(fn_out, '.nii.gz', '_temp.nii')]);
            end
            
            gzip(gfactorFile);
            system(['rm -f ', gfactorFile, ' ', strrep(fn_out, '.nii.gz', 'phase.nii')]);
            % CHANGED: gfactor rename uses _dwi -> _gfactor with new acq label
            system(['mv ', strrep(gfactorFile, '.nii', '.nii.gz'), ' ', ...
                   strrep(strrep(strrep(gfactorFile, '.nii', '.nii.gz'), ...
                   '_dwi', '_gfactor'), 'gfactor_', '')]);
            
            fprintf('NORDIC output finalized for %s\n', src_mags(src_magI).name);

        %% --- !doNORDIC: copy source to target ---
        elseif ~doNORDIC
            if ~exist(fn_out, 'file') || force
                if has_orig
                    copy_src = mag_orig_local;
                    fprintf('Case 4: Copying orig -> target for %s\n', src_mags(src_magI).name);
                else
                    copy_src = fn_magn_in;
                    fprintf('Case 3: Copying magnitude -> target for %s\n', src_mags(src_magI).name);
                end

                system(['cp ', copy_src, ' ', fn_out]);
                system(['chmod 755 ', fn_out]);

                if nordic_scans_end > 0
                    info = niftiinfo(fn_out);
                    system(['fslroi ', fn_out, ' ', fn_out, ...
                           ' 0 -1 0 -1 0 -1 0 ', num2str(info.ImageSize(end) - nordic_scans_end)]);
                end

                fprintf('Output created: %s\n', fn_out);
            else
                fprintf('Output already exists and force=0, skipping: %s\n', fn_out);
            end

        elseif doNORDIC && ~exist(gfactorFile, 'file') && exist(fn_out, 'file')
            fprintf('NORDIC output already finalized, skipping: %s\n', fn_out);
        end
        
        %% --- Copy sidecar files (JSON, bvec, bval) to output ---
        dst_json = strrep(fn_out, '.nii.gz', '.json');
        if ~exist(dst_json, 'file') || force
            src_json_local = strrep(fn_magn_in, '_magnitude.nii.gz', '_magnitude.json');
            src_json_orig  = strrep(src_json_local, '.json', '_orig.json');
            if has_orig && exist(src_json_orig, 'file')
                system(['cp ', src_json_orig, ' ', dst_json]);
            else
                system(['cp ', src_json_local, ' ', dst_json]);
            end
            system(['chmod 755 ', dst_json]);
            fprintf('JSON copied: %s\n', dst_json);
        end
        
        % bvec and bval (only for files that have them, i.e. AP)
        if has_bvec_bval
            dst_bvec = strrep(fn_out, '.nii.gz', '.bvec');
            dst_bval = strrep(fn_out, '.nii.gz', '.bval');
            
            if ~exist(dst_bvec, 'file') || force
                if doNORDIC
                    system(['cp ', src_bvec_local, ' ', dst_bvec]);
                elseif has_orig && exist(bvec_orig, 'file')
                    system(['cp ', bvec_orig, ' ', dst_bvec]);
                else
                    system(['cp ', src_bvec_local, ' ', dst_bvec]);
                end
                system(['chmod 755 ', dst_bvec]);
                fprintf('bvec copied: %s\n', dst_bvec);
            end
            
            if ~exist(dst_bval, 'file') || force
                if doNORDIC
                    system(['cp ', src_bval_local, ' ', dst_bval]);
                elseif has_orig && exist(bval_orig, 'file')
                    system(['cp ', bval_orig, ' ', dst_bval]);
                else
                    system(['cp ', src_bval_local, ' ', dst_bval]);
                end
                system(['chmod 755 ', dst_bval]);
                fprintf('bval copied: %s\n', dst_bval);
            end
            
            % For non-NORDIC with noise scan removal, trim bvec/bval in output
            if ~doNORDIC && nordic_scans_end > 0 && exist(fn_out, 'file')
                out_info = niftiinfo(fn_out);
                out_nvols = out_info.ImageSize(end);
                trim_bvec_bval(dst_bvec, dst_bval, out_nvols);
            end
        end
    end

    time_end = datetime('now');
    fprintf('Total time for %s, %s, %d runs: %s\n', sub, ses, num_runs, time_end - time_start);
    disp('DWI NORDIC finished!!');
end


%% ========== NEW: Rename magnitude filename to output filename ==========
% Replaces acq-<anything> with acq-<acq_label> and _magnitude with _dwi
%
% Example:
%   rename_mag_to_output('sub-07_ses-01_acq-votcloc1d5_dir-PA_run-01_magnitude.nii.gz', 'nordic')
%   -> 'sub-07_ses-01_acq-nordic_dir-PA_run-01_dwi.nii.gz'
%
%   rename_mag_to_output('sub-07_ses-01_acq-votcloc1d5_dir-AP_run-01_magnitude.bvec', 'magonly')
%   -> 'sub-07_ses-01_acq-magonly_dir-AP_run-01_dwi.bvec'
function out_name = rename_mag_to_output(mag_name, acq_label)
    % Step 1: Replace acq-<anything>_ with acq-<acq_label>_
    out_name = regexprep(mag_name, 'acq-[^_]+', ['acq-' acq_label]);
    % Step 2: Replace _magnitude with _dwi
    out_name = strrep(out_name, '_magnitude', '_dwi');
end


%% ========== Trim bvec/bval in output dir to match volume count ==========
function trim_bvec_bval(bvec_file, bval_file, new_volume_count)
    if exist(bvec_file, 'file')
        bvec = load(bvec_file);
        if size(bvec, 1) == 3 && size(bvec, 2) > new_volume_count
            bvec_new = bvec(:, 1:new_volume_count);
            fid = fopen(bvec_file, 'w');
            fprintf(fid, '%.6f ', bvec_new(1, :)); fprintf(fid, '\n');
            fprintf(fid, '%.6f ', bvec_new(2, :)); fprintf(fid, '\n');
            fprintf(fid, '%.6f ', bvec_new(3, :)); fprintf(fid, '\n');
            fclose(fid);
            fprintf('  Output bvec trimmed: %d -> %d volumes\n', size(bvec, 2), new_volume_count);
        end
    end

    if exist(bval_file, 'file')
        bval = load(bval_file);
        if size(bval, 1) > 1
            bval = bval';
        end
        if size(bval, 2) > new_volume_count
            bval_new = bval(1:new_volume_count);
            fid = fopen(bval_file, 'w');
            fprintf(fid, '%.1f ', bval_new);
            fprintf(fid, '\n');
            fclose(fid);
            fprintf('  Output bval trimmed: %d -> %d volumes\n', length(bval), new_volume_count);
        end
    end
end