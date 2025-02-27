% ADD FSL TO THE PATH BEFORE LAUNCHING MATLAB
% then do 
% tbUse BCBLViennaSoft;  
% this step is to add preserger and NORDIC_RAW into the path so that you
% can use it


%if system('fslroi')==127
%    error("didn't load fsl");
%end

%if system('3dTstat')==127
%    error("didn't load afni");
%end
%%%%%%%%%% EDIT THIS %%%%%%%%%%
% VIENNA
% baseP = '/ceph/mri.meduniwien.ac.at/projects/physics/fmri/data/bcblvie22/BIDS';

% BCBL
baseP = fullfile('/bcbl/home/public/Gari/VOTCLOC','BIDS');

% GENERIC
subs = {'01','02','03','04','05'}; %{'bt001','bt002'};
% subs = {}; %{'bt001','bt002'};
sess = {'day3PF','day4PF'};
acqs = {'104','68'};
 
nordic_scans_end = 0;
force = false;  % do i overwrite exsting file? 
doNORDIC = true;
%%%%%%%%%%%


tbPath = fullfile(bvRP,'..');
spm12Path = fullfile(tbPath, 'spm12');
addpath(spm12Path); 
fmamtPath = fullfile(tbPath, 'freesurfer_mrtrix_afni_matlab_tools'); % tbUse if not installed
addpath(genpath(fmamtPath));
addpath(genpath(fullfile(baseP,'..','code')))

nordicpath=fullfile(tbPath,'NORDIC_Raw');
addpath(genpath(nordicpath));

presurferpath=fullfile(tbPath,'presurfer');
addpath(genpath(presurferpath));
setenv('FSLOUTPUTTYPE', 'NIFTI_GZ')


%%
% nordic + detrend + tsnr
for subI=1:length(subs)
    sub = ['sub-',subs{subI}];
    for sesI=1:length(sess)
        ses = ['ses-',sess{sesI}];
        sesP = fullfile(baseP, sub, ses);
        %% perform nordic on all the funtional files
        mags = dir(fullfile(sesP, 'dwi', '*_magnitude.nii.gz'));
        
        % backup mag  and phase
        for magI=1:length(mags)
            try
                % define file names
                fn_magn_in  = fullfile(mags(magI).folder, mags(magI).name);
                fn_phase_in = strrep(fn_magn_in, '_magnitude', '_phase');

                if ~exist(strrep(fn_magn_in, '.nii.gz', '_orig.nii.gz'), 'file') 

                    info = niftiinfo(fn_magn_in);
                    system(['cp ', fn_magn_in, ' ', strrep(fn_magn_in, '.nii.gz', '_orig.nii.gz')]);
                    system(['cp ', fn_phase_in, ' ', strrep(fn_phase_in, '.nii.gz', '_orig.nii.gz')]);
                    system(['chmod 755 ', fn_phase_in, ' ', fn_magn_in]);
                    system(['chmod 755 ', fullfile(baseP, sub)])
                    % maintain 1 volumns for nordic and remove the extra,
                    % but for DWI there is no extra so we don't remove
%                     if nordic_scans_end > 1
%                         system(['fslroi ', fn_magn_in, ' ', fn_magn_in, ' 0 -1 0 -1 0 -1 0 ', num2str(info.ImageSize(end)-(nordic_scans_end-1))]);
%                         system(['fslroi ', fn_phase_in, ' ', fn_phase_in, ' 0 -1 0 -1 0 -1 0 ', num2str(info.ImageSize(end)-(nordic_scans_end-1))]);
%                     end
                    system(['fslmaths ', fn_magn_in,  ' ', fn_magn_in,  ' -odt float']);
                    system(['fslmaths ', fn_phase_in, ' ', fn_phase_in, ' -odt float']);
                end
            end
        end
        clear magI
        %% perform nordic on all the DWI files
        I = 1;
        
        for magI=1:length(mags)
            % define file names
            fn_magn_in  = fullfile(mags(magI).folder, mags(magI).name);
            fn_phase_in = strrep(fn_magn_in,  '_magnitude', '_phase');
            fn_out      = strrep(fn_magn_in,  '_magnitude', '_dwi');

            if ~(exist(strrep(fn_out, '.nii.gz', 'magn.nii'), 'file') || exist(fn_out,'file')) && doNORDIC

                ARG(I).temporal_phase = 1;
                ARG(I).phase_filter_width = 3;
                ARG(I).noise_volume_last = 0;
                [ARG(I).DIROUT,fn_out_name,~] = fileparts(fn_out);
                ARG(I).DIROUT = [ARG(I).DIROUT, '/'];
                ARG(I).make_complex_nii = 1;
                ARG(I).save_gfactor_map = 1;
                % the number P >= cubic root of 11*num of direction
                ARG(I).kernel_size_PCA = [11,11,11];
                
                file(I).phase = fn_phase_in;
                file(I).magni = fn_magn_in;
                file(I).out   = strrep(fn_out_name, '.nii', '');

                I = I + 1;
            end
        end
        if exist('ARG', 'var')
            parfor i=1:length(ARG)
                %              try
                NIFTI_NORDIC(file(i).magni, file(i).phase,file(i).out,ARG(i));
                %              end
            end
            clear ARG file
        end
        clear magI

        for magI=1:length(mags)
            %             try
            % define file names
            fn_magn_in  = fullfile(mags(magI).folder, mags(magI).name);
            fn_phase_in = strrep(fn_magn_in,  '_magnitude', '_phase');
            fn_out      = strrep(fn_magn_in,  '_magnitude', '_dwi');
            gfactorFile = strrep(strrep(fn_out, '.nii.gz', '.nii'), [sub '_ses'] ,['gfactor_' sub '_ses']);

            if exist(gfactorFile, 'file') && doNORDIC
                % clean up
                info = niftiinfo(fn_magn_in);
                % remove the last one 
               
                % system(['fslroi ', strrep(fn_out, '.nii.gz', 'magn.nii'), ' ', fn_out, ' 0 -1 0 -1 0 -1 0 ', num2str(info.ImageSize(end))]);
                gzip(gfactorFile);
                system(['rm ', strrep(fn_out, '.nii.gz', 'magn.nii'), ' ', gfactorFile]);
                system(['mv ', strrep(gfactorFile, '.nii', '.nii.gz'), ' ', strrep(strrep(strrep(gfactorFile, '.nii', '.nii.gz'), '_dwi', '_gfactor'), 'gfactor_', '')]);
            end


            % rename the mag json, 
            if ~exist(strrep(fn_magn_in, '_magnitude.nii.gz', '_dwi.json'), 'file')
                system(['cp ', strrep(fn_magn_in, '_magnitude.nii.gz', '_magnitude.json'), ' ', ...
                    strrep(fn_magn_in, '_magnitude.nii.gz', '_dwi.json')]);
            end
            if ~exist(strrep(fn_magn_in, '_magnitude.nii.gz', '_dwi.bvec'), 'file')
                system(['cp ', strrep(fn_magn_in, '_magnitude.nii.gz', '_magnitude.bvec'), ' ', ...
                    strrep(fn_magn_in, '_magnitude.nii.gz', '_dwi.bvec')]);
            end
            if ~exist(strrep(fn_magn_in, '_magnitude.nii.gz', '_dwi.bval'), 'file')
                system(['cp ', strrep(fn_magn_in, '_magnitude.nii.gz', '_magnitude.bval'), ' ', ...
                    strrep(fn_magn_in, '_magnitude.nii.gz', '_dwi.bval')]);
            end            
            %             end

          
        end
            

    end
end
