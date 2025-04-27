function masked_surface = mask_surf_use_label(surface_array,label,num_of_vertices)
%mask_surf_use_label Use freesurfer label to mask the surface for GLM or
%visualization
%  surface_array: 3D or 4D arry stores values on each vertices
%   label: get from freesurfer/label dir, it can be a file if you set
%   usepath to 0, it can also be a path and this function will read the
%   path
%   num_of_vertices: number of total vertices on the surface
%   usepath: 1 or 0, of 1, this function will read the path, else this
%   function will take the input label file

if size(surface_array,1) > size(surface_array,2)
    surface_array=surface_array';
end


value_within_label=surface_array(:,label);
% create empty array
masked_surface=zeros(1,num_of_vertices);

% put the value within label to the empty array
masked_surface(label)=value_within_label;

end
