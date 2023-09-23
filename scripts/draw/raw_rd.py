import os
import sys
import csv
import matplotlib.pyplot as plt
import numpy as np

glibcnames = {'sync_ncl': 'NCL no prefetch', 'cephfs': 'DFS', 'cephfs_sync': 'DFS direct IO', 'sync' : 'strong DFS', 'ncl': 'NCL'}
color = {'cephfs':'tab:orange', 'cephfs_sync':'tab:green', 'ncl':'tab:brown', 'sync':'tab:purple', 'sync_ncl':'tab:blue','nclwseq':'tab:pink'}
linestyles = {'cephfs': 'dotted', 'ncl': 'dashed', 'cephfs_sync':'dashdot', 'sync_ncl': (5, (10, 3))}

markers = {'cephfs':'o', 'cephfs_sync':'^', 'ncl':'P', 'sync':'v', 'sync_ncl':'*', 'nclwseq':'X'}
plt.rcParams['font.size'] = 18

def draw_read(folder: str, type: str):
    size: 'dict[str, list[int]]' = {}
    lat: 'dict[str, list[float]]' = {}
    one_lat: 'dict[str, list[float]]' = {}
    backend = ['cephfs', 'sync_ncl', 'ncl', 'cephfs_sync']

    for b in backend:
        filename: str = os.path.join(folder, 'raw', '{}_read.csv'.format(b))
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
                
                if b == 'ncl':
                    recover = int(row[1])
                    num = int(row[2])
                    read_lat = float(row[3])
                    one_lat[b].append(read_lat + recover / num)
                else:
                    one_lat[b].append(float(row[1]))
            lat[b].append(np.average(one_lat[b]))
        
    fig, ax = plt.subplots()
    for b in backend:
        print(b, glibcnames[b], color[b], linestyles[b], markers[b])
        
        ax.plot(size[b], lat[b], marker=markers[b], color=color[b], label=glibcnames[b], linewidth=3, markersize=10, linestyle=linestyles[b])
        print(b, size, lat[b])

    ax.set_xscale('log')
    ax.set_yscale('log')
    ax.set_xlabel('Size (bytes)')
    ax.set_ylabel('Latency (' + u"\u03bcs)")
    ax.set_xlim(left=10**2, right=10**4)
    #ax.set_ylim(bottom=0)
    ax.set_box_aspect(0.6)
    

    # hide the spines between ax and ax2
    
    ax.legend(frameon=False, ncol=2, fontsize=18, loc='upper left', bbox_to_anchor=(-0.2, 1.3))
    
    fig.savefig(os.path.join(folder, 'fig', type+'_lat.pdf'), bbox_inches='tight')


if __name__ == '__main__':
    plt.rcParams['font.size'] = 17
    folder = '/data/result'
    if len(sys.argv) > 1:
        folder = sys.argv[1]
    draw_read(folder, 'read')
