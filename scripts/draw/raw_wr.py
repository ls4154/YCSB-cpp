import os
import sys
import csv
import matplotlib.pyplot as plt
import numpy as np
from common import *


def draw_write(folder: str, type: str):
    size: 'dict[str, list[int]]' = {}
    lat: 'dict[str, list[float]]' = {}
    one_lat: 'dict[str, list[float]]' = {}
    backend = ['sync', 'cephfs', 'sync_ncl']

    for b in backend:
        filename: str = os.path.join(folder, 'raw', '{}_write.csv'.format(b))
        with open(filename, 'r') as f:
            csv_reader = csv.reader(f)
            cur_size: int = 0
            size[b] = []
            lat[b] = []
            one_lat[b] = []

            for row in csv_reader:
                s: int = int(row[0])
                if s != cur_size:
                    if len(one_lat[b]) != 0:
                        lat[b].append(np.average(one_lat[b]))
                    cur_size = s
                    size[b].append(s)
                    one_lat[b].clear()
                
                one_lat[b].append(float(row[1]))
            lat[b].append(np.average(one_lat[b]))
        
    fig, (ax1, ax2) = plt.subplots(2, 1, sharex=True)
    fig.subplots_adjust(hspace=-0.2)  
    for b in backend:
        print(b, glibcnames[b], linestyles[b])
        
        ax1.plot(size[b], lat[b], marker=markers[b], color=color[b], label=glibcnames[b], linewidth=3, markersize=10, linestyle=linestyles[b])
        ax2.plot(size[b], lat[b], marker=markers[b], color=color[b], label=glibcnames[b], linewidth=3, markersize=10, linestyle=linestyles[b])
        print(b, size, lat[b])

    ax1.set_xscale('log')
    ax2.set_xscale('log')
    
    #ax.set_yscale('log')
    ax2.set_xlabel('Size (bytes)')
    ax2.yaxis.set_label_coords(-.17, .9)
    ax2.set_ylabel('                    Latency (' + u"\u03bcs)")
    ax1.set_xlim(left=10**2, right=10**4)
    ax2.set_xlim(left=10**2, right=10**4)
    #ax.set_ylim(bottom=0)
    ax1.set_box_aspect(0.25)
    ax2.set_box_aspect(0.25)
    
    ax2.set_ylim(0, 20)
    ax1.set_ylim(1800,2200)  # most of the data

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
    
    ax1.legend(frameon=False, ncol=3, fontsize=17, loc='upper left', bbox_to_anchor=(-0.32, 1.6), columnspacing=0.9)
    
    fig.savefig(os.path.join(folder, 'fig', type+'_lat.pdf'), bbox_inches='tight')


if __name__ == '__main__':
    plt.rcParams['font.size'] = 19
    folder = '/data/result'
    if len(sys.argv) > 1:
        folder = sys.argv[1]
    draw_write(folder, 'write')
