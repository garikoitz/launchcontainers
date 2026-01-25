function prepare_nordic_dwi_nifti(src_magnitude, nordic_scans_end, force)
% MIT License
% Copyright (c) 2024-2025 Yongning Lei

% Permission is hereby granted, free of charge, to any person obtaining a copy of this software
% and associated documentation files (the "Software"), to deal in the Software without restriction,
% including without limitation the rights to use, copy, modify, merge, publish, distribute, sublicense,
% and/or sell copies of the Software, and to permit persons to whom the Software is furnished to do so,
% subject to the following conditions:

% The above copyright notice and this permission notice shall be included in all copies or substantial
% portions of the Software.

% This function prepares DWI data for NORDIC processing
% It has 3 jobs:
%   1. Create backup files (magnitude, phase, bvec, bval)
%   2. Change dtype of source files
%   3. Remove noise scans if present
%   4. Update bvec/bval files to match volume count

    % Define names of the backup files
    src_phase = strrep(src_magnitude, '_magnitude', '_phase');
    src_bvec = strrep(src_magnitude, '_magnitude.nii.gz', '_magnitude.bvec');
    src_bval = strrep(src_magnitude, '_magnitude.nii.gz', '_magnitude.bval');
    
    mag_backup = strrep(src_magnitude, '.nii.gz', '_orig.nii.gz');
    phase_backup = strrep(src_phase, '.nii.gz', '_orig.nii.gz');
    bvec_backup = strrep(src_bvec, '.bvec', '_orig.bvec');
    bval_backup = strrep(src_bval, '.bval', '_orig.bval');

    disp('Change the src_mag, src_phase, bvec, bval file permissions to 777 \n');
    system(['chmod 777 ', src_phase, ' ', src_magnitude, ' ', src_bvec, ' ', src_bval]);

    %% Create or restore backups
    if ~(exist(mag_backup, 'file') && exist(phase_backup, 'file') && ...
         exist(bvec_backup, 'file') && exist(bval_backup, 'file'))
        % If backup files don't exist, create them
        system(['cp ', src_magnitude, ' ', mag_backup]);
        system(['cp ', src_phase, ' ', phase_backup]);
        system(['cp ', src_bvec, ' ', bvec_backup]);
        system(['cp ', src_bval, ' ', bval_backup]);
        disp('** Backups for mag, phase, bvec, and bval created \n');
        
    elseif (exist(mag_backup, 'file') && exist(phase_backup, 'file') && ...
            exist(bvec_backup, 'file') && exist(bval_backup, 'file')) && force
        % If backup files exist and force overwrite is requested
        disp('Backup files found, overwriting...... \n');
        delete(src_magnitude);
        delete(src_phase);
        delete(src_bvec);
        delete(src_bval);
        
        system(['cp ', mag_backup, ' ', src_magnitude]);
        system(['cp ', phase_backup, ' ', src_phase]);
        system(['cp ', bvec_backup, ' ', src_bvec]);
        system(['cp ', bval_backup, ' ', src_bval]);
        
        disp('** Source files restored from backups \n');
    else
        disp('Backup files found, do nothing...... \n');
    end
    
    % After the above, there will always be 1 mag, 1 phase, 1 bvec, 1 bval, 
    % and their backups

    %% Process magnitude and phase files
    % 1. Change dtype to float
    % 2. Remove noise scans if present
    % 3. Update bvec/bval to match new volume count
    
    mag_info = niftiinfo(src_magnitude);
    fprintf('The dtype of magnitude is %s \n', mag_info.Datatype);
    mag_backup_info = niftiinfo(mag_backup);
    fprintf('The dtype of magnitude_orig is %s \n', mag_backup_info.Datatype);
    
    if strcmp(mag_backup_info.Datatype, 'uint16')
        fprintf('Backup file of %s is in original datatype, we are safe \n', src_magnitude)
        
        % Remove extra noise scans if present (keep only 1 or 0 based on nordic_scans_end)
        if nordic_scans_end > 1
            new_volume_count = mag_info.ImageSize(end) - (nordic_scans_end - 1);
            system(['fslroi ', src_magnitude, ' ', src_magnitude, ...
                   ' 0 -1 0 -1 0 -1 0 ', num2str(new_volume_count)]);
            system(['fslroi ', src_phase, ' ', src_phase, ...
                   ' 0 -1 0 -1 0 -1 0 ', num2str(new_volume_count)]);
            fprintf('** Extra noise scans removed, keeping %d volumes \n', new_volume_count);
            
            % Update bvec and bval files to match new volume count
            update_bvec_bval(src_bvec, src_bval, new_volume_count);
            
        elseif nordic_scans_end == 1
            % Keep all volumes including 1 noise scan
            fprintf('** Keeping all volumes including 1 noise scan \n');
        else
            % nordic_scans_end == 0, no noise scans
            fprintf('** No noise scans to remove \n');
        end
        
        % Change datatype to float if not already
        if ~(strcmp(mag_info.Datatype, 'single') && mag_info.BitsPerPixel == 32) || force
            system(['fslmaths ', src_magnitude, ' ', src_magnitude, ' -odt float']);
            system(['fslmaths ', src_phase, ' ', src_phase, ' -odt float']);
            
            after_change_mag_info = niftiinfo(src_magnitude);
            fprintf('The dtype of magnitude file %s after fslmaths is %s \n', ...
                   src_magnitude, after_change_mag_info.Datatype);
            disp('** Changed data format to float for src mag and src phase .nii.gz \n');
        else
            disp('The dtype of mag and phase input is already float, do nothing \n');
        end
        
    else
        warning('The backup magnitude file is not uint16 dtype, we might have an issue!');
    end
end


function update_bvec_bval(bvec_file, bval_file, new_volume_count)
    % Update bvec and bval files to match the new volume count after 
    % removing noise scans
    
    % Read bvec file (3 rows x N columns)
    bvec = load(bvec_file);
    if size(bvec, 1) == 3
        % Standard format: 3 rows (x, y, z directions)
        if size(bvec, 2) > new_volume_count
            bvec_new = bvec(:, 1:new_volume_count);
            
            % Write updated bvec
            fid = fopen(bvec_file, 'w');
            fprintf(fid, '%.6f ', bvec_new(1, :));
            fprintf(fid, '\n');
            fprintf(fid, '%.6f ', bvec_new(2, :));
            fprintf(fid, '\n');
            fprintf(fid, '%.6f ', bvec_new(3, :));
            fprintf(fid, '\n');
            fclose(fid);
            
            fprintf('** bvec file updated: %d -> %d volumes \n', ...
                   size(bvec, 2), new_volume_count);
        end
    else
        warning('bvec file format unexpected, skipping update');
    end
    
    % Read bval file (1 row x N columns)
    bval = load(bval_file);
    if size(bval, 1) == 1 && size(bval, 2) > new_volume_count
        bval_new = bval(1:new_volume_count);
        
        % Write updated bval
        fid = fopen(bval_file, 'w');
        fprintf(fid, '%.1f ', bval_new);
        fprintf(fid, '\n');
        fclose(fid);
        
        fprintf('** bval file updated: %d -> %d volumes \n', ...
               length(bval), new_volume_count);
    elseif size(bval, 1) > 1
        % Sometimes bval is in column format
        bval = bval';
        if size(bval, 2) > new_volume_count
            bval_new = bval(1:new_volume_count);
            
            % Write updated bval
            fid = fopen(bval_file, 'w');
            fprintf(fid, '%.1f ', bval_new);
            fprintf(fid, '\n');
            fclose(fid);
            
            fprintf('** bval file updated (transposed): %d -> %d volumes \n', ...
                   length(bval), new_volume_count);
        end
    end
end