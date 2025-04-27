from __future__ import annotations

import os

import nibabel.freesurfer as fs
import nilearn.surface as surf
import numpy as np
from nilearn import plotting
from nilearn.surface import load_surf_data


def plot_on_surface(output_dict, name, k, figure_dir):

    # %%
    # difine the location of output file

    if not os.path.isdir(figure_dir):
        os.mkdir(figure_dir)

    figure_name = f'kmeans_{k}_clusters_{name}.png'
    output_file = os.path.join(figure_dir, figure_name)
    title = f'kmeans clustering result on {name} with {k} clusters'
    # get surface file as background
    fsaverage = '/bcbl/home/public/Gari/MINI/ANALYSIS/freesurferacpc/fsaverage/surf/lh.inflated'
    curv_left = fs.read_morph_data(
        '/bcbl/home/public/Gari/MINI/ANALYSIS/freesurferacpc/fsaverage/surf/lh.curv',
    )
    curv_left_sign = np.sign(curv_left)

    #  get overlay file
    sulc_left = fs.read_morph_data(
        '/bcbl/home/public/Gari/MINI/ANALYSIS/freesurferacpc/fsaverage/surf/lh.sulc',
    )

    # load ROI to plot as contours
    motspots = fs.read_annot(
        '/bcbl/home/public/Gari/MINI/ANALYSIS/freesurferacpc/fsaverage/label/lh.motspots.annot',
    )
    # get the label
    clusters = output_dict['map']

# figure_l= plotting.plot_surf_roi(fsaverage, roi_map=cluster_after_kmean[k][dataset_setting]['map'],
#                        hemi='left', view='lateral',
#                        bg_map=curv_left_sign, bg_on_data=True,
#                        darkness=.5, engine='matplotlib')

    figure_v = plotting.plot_surf_roi(
        fsaverage, roi_map=clusters,
        hemi='left', view=(-135, 90),
        bg_map=curv_left_sign, bg_on_data=True,
        darkness=.5, engine='matplotlib',
        title=title, output_file=output_file,
    )

    # fsaverage = '/bcbl/home/public/Gari/MINI/ANALYSIS/freesurferacpc/fsaverage/surf/lh.inflated'
    # curv_left= fs.read_morph_data('/bcbl/home/public/Gari/MINI/ANALYSIS/freesurferacpc/fsaverage/surf/lh.curv')
    # curv_left_sign = np.sign(curv_left)
    # fig= plotting.plot_surf_stat_map(fsaverage, mean,
    #                     hemi='left', view=(-135,90),
    #                     bg_map=curv_left_sign, bg_on_data=True,
    #                     darkness=.5, engine='plotly',
    #                    )
    # fig.show()


# plotting.plot_surf_contours(fsaverage, motspots[0],
#                             legend=True, view='ventral', labels=motspots[-1], levels=[-1,1],
#                             figure=figure_v, colors=['r','b'],engine='matplotlib')

    return figure_v


def get_threshT_binary_map(sub, ses, contrast, tmap_dir, threshold):
    allmaps_dir = op.join(tmap_dir, f'sub-{sub}', f'ses-{ses}')

    t_valmaps = [i for i in os.listdir(allmaps_dir) if (f'{contrast}_stat-t' in i)]
    tmap = t_valmaps[0]

    wp_tmap1 = load_surf_data(op.join(allmaps_dir, tmap))
    th_tmap = np.where(wp_tmap1 > threshold, 1, 0)
    number_of_vertices = len(wp_tmap1)
    return number_of_vertices, th_tmap


fsdir = '/home/tlei/tlei/MINI/derivatives/freesurfer'
sub_id = 'S042'
surf_dir = f'{fsdir}/sub-{sub_id}/surf'
label_dir = f'{fsdir}/sub-{sub_id}/label'
fs_inflated = f'{surf_dir}/lh.inflated'

curv_left = fs.read_morph_data(f'{surf_dir}/lh.curv')
curv_left_sign = np.sign(curv_left)

biglots_path = f'{label_dir}/lh.bigLOTS.label'
biglots_label = surf.load_surf_data(biglots)

contrast = 'FacesvsPER'
gii = f'/home/tlei/tlei/MINI/derivatives/vertex_wisecount/analysis-MINI_test-retest/tmap/sub-{sub_id}_space-fsnative_contrast-{contrast}_desc-thresh1.3sm00_vertexwisecount.gii'


elva, azimuth = -20, 30

fig = plotting.plot_surf_stat_map(
    fs_inflated,
    stat_map=load_surf_data(gii),
    hemi='left',
    view=(elva, azimuth),
    bg_map=curv_left, bg_on_data=True,
    darkness=.5, engine='plotly',
)
fig.show()
