import os
import sys
import yaml
import matplotlib.pyplot as plt
import numpy as np
from common import *

def draw_insert_only(folder: str, db: str, figname: str):
    backend = ['sync', 'cephfs', 'sync_ncl']
    th_range: 'dict[str, tuple]' = {'ncl':(1,16), 'cephfs':(1,16), 'cephfs_sync':(2,32), 'ext4':(1,16), 'sync':(2,24), 'sync_ncl':(1,16)}
    threads = [1, 2, 4, 8, 12, 16, 20, 24, 32, 48]
    # ncl strong, cephfs weak, cephfs strong
    # threads = range(1, 21)
    all_data: 'dict[str, dict[str, list[float]]]' = {}
    iter = 3
    for b in backend:
        all_data[b] = {'lat':[], 'tput':[]}
        for t in threads:
            if t < th_range[b][0] or t > th_range[b][1]:
                continue
            lats: list[float] = []
            tputs: list[float] = []
            for i in range(1, iter+1):
                sample_name = '{}_load_{}_th{}_trail{}.yml'.format(db, b, t, i)
                try:
                    with open(os.path.join(folder, db, 'load', b, sample_name), 'r') as f:
                        data = yaml.load(f)
                        lats.append(data['INSERT']['avg'])
                        tputs.append(data['throughput'])
                except:
                    pass
            try:
                all_data[b]['lat'].append(np.average(lats))
                all_data[b]['tput'].append(np.average(tputs)/1000)
            except:
                pass
    
    fig, (ax1, ax2) = plt.subplots(2, 1, sharex=True, figsize=(4, 7))
    fig.subplots_adjust(hspace=-0.8)  
    #ax2 = ax #.twinx()
    for b in backend:
        if side[b] == 0:
            if b == 'ext4': 
                continue
            ax1.plot(all_data[b]['tput'], all_data[b]['lat'], label=name[b], color=color[b], linewidth=3, linestyle=linestyles[b], marker=markers[b], markersize=9)
            ax2.plot(all_data[b]['tput'], all_data[b]['lat'], label=name[b], color=color[b], linewidth=3, linestyle=linestyles[b], marker=markers[b], markersize=9)
        # else:
        #     ax2.plot(all_data[b]['tput'], all_data[b]['lat'], label=name[b], marker=markers[b], color=color[b])
        print(b+' '+str((all_data[b]['tput']))+str(all_data[b]['lat']))
    #ax.set_xlim(left=1)
    #ax1.set_xscale('log')
    #ax2.set_xscale('log')
    #ax.set_yscale('log')
    #ax.set_ylim(bottom=1)
    #ax2.set_ylim(top = 10**4)
    ax2.set_xlabel('Throughput (KOps/s)')
    ax2.yaxis.set_label_coords(-.22, .1)
    ax2.set_ylabel('          Latency (' + u"\u03bcs)")
    #if db == 'rocksdb':
    ax1.legend(frameon=False, fontsize=16, loc='upper left', bbox_to_anchor=(-0.35, 2.4), ncol = 2, columnspacing=0.9)
    # ax2.legend(frameon=False, fontsize='small', loc='upper right')
    ax1.set_box_aspect(0.2)
    ax2.set_box_aspect(0.2)
    
    # zoom-in / limit the view to different portions of the data
    if db == 'rocksdb':
        ax2.set_ylim(1, 100)  # outliers only
    else:
        ax2.set_ylim(1, 200)
    ax1.set_ylim(4000,5200)  # most of the data

    # hide the spines between ax and ax2
    ax1.spines.bottom.set_visible(False)
    ax2.spines.top.set_visible(False)
    ax1.xaxis.tick_top()
    ax1.tick_params(labeltop=False)  # don't put tick labels at the top
    ax2.xaxis.tick_bottom()
    d = .5  # proportion of vertical to horizontal extent of the slanted line
    kwargs = dict(marker=[(-1, -d), (1, d)], markersize=12,
                  linestyle="none", color='k', mec='k', mew=1, clip_on=False)
    ax1.plot([0, 1], [0, 0], transform=ax1.transAxes, **kwargs)
    ax2.plot([0, 1], [1, 1], transform=ax2.transAxes, **kwargs)
    fig.savefig(os.path.join(folder, 'fig', db+figname), bbox_inches='tight')


def draw_insert_only_sqlite(folder: str):
    backend = ['sync', 'cephfs', 'sync_ncl']
    all_data: dict[str, dict[str, list[float]]] = {}
    iter = 3
    for b in backend:
        all_data[b] = {'lat':[], 'tput':[]}
        for i in range(1, iter+1):
            sample_name = 'sqlite_load_{}_th1_trail{}.yml'.format(b, i)
            try:
                with open(os.path.join(folder, 'sqlite', 'load', b, sample_name), 'r') as f:
                    data = yaml.load(f)
                    all_data[b]['lat'].append(data['INSERT']['avg'])
                    all_data[b]['tput'].append(data['throughput'])
            except:
                continue
    
    fig, (ax1, ax2)= plt.subplots(1, 2)
    fig.subplots_adjust(wspace=0.3)  
    x_pos = [1,3,5]
    for x, b in enumerate(backend):
        ax1.bar(x_pos[x], np.average(all_data[b]['tput'])/1000, color=color[b], width=0.8)
        ax2.bar(x_pos[x], np.average(all_data[b]['lat']), color=color[b], width=0.8)
        print('{} {} {}us'.format(b, np.average(all_data[b]['tput']), np.average(all_data[b]['lat'])))
    ax1.set_ylabel('Throughput (KOps/s)', fontsize=11)
    ax2.set_ylabel('Latency (' + u"\u03bcs)", fontsize=11)
    ax2.set_yscale('log')
    ax2.set_ylim(bottom=1, top=10**4)
    ax1.set_xlim([0,6])
    ax2.set_xlim([0,6])
   
    ax2.set_xticks(x_pos) #, np.arange(len(backend)))
    ax2.set_xticklabels([name_bars[b] for b in backend], fontsize=10)
    ax1.set_xticks(x_pos) #, np.arange(len(backend)))
    ax1.set_xticklabels([name_bars[b] for b in backend], fontsize=10)
    ax1.tick_params(axis='y', labelsize=11)
    ax2.tick_params(axis='y', labelsize=11)
    ax1.set_box_aspect(0.7)
    ax2.set_box_aspect(0.7)
    fig.savefig(os.path.join(folder, 'fig/sqlite_lattput.pdf'), bbox_inches='tight')


if __name__ == '__main__':
    plt.rcParams['font.size'] = 16
    folder = 'data/result'
    if len(sys.argv) > 1:
        folder = sys.argv[1]

    draw_insert_only(folder, 'rocksdb', '_lattput.pdf')
    draw_insert_only(folder, 'redis', '_lattput.pdf')
    draw_insert_only_sqlite(folder)
