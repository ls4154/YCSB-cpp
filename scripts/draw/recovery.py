import os
import sys
import csv
import matplotlib.pyplot as plt
import numpy as np
from common import *

phases = ['get peer', 'connect', 'rdma read', 'sync peer', 'parse']

def draw_recovery(folder: str):
    data: 'dict[str, dict[str, dict[str, int]]]' = {}
    lat: 'dict[str, dict[str, list[float]]]' = {}
    databases = ['rocksdb', 'redis', 'sqlite']
    backends = ['ncl', 'cephfs', 'ext4']
    hatch = ['\\\\\\', '///', 'xxx', '||', '']

    for b in backends:
        for db in databases:
            try:
                with open(os.path.join(folder, 'recover', db, '{}.csv'.format(b)), 'r') as f:
                    if b not in data:
                        data[b] = {}
                    if db not in data[b]:
                        data[b][db] = {}
                    lat_brkd: 'dict[str, int]' = {}
                    csv_reader = csv.reader(f, delimiter=',')
                    for row in csv_reader:
                        lat_brkd[row[0]] = int(row[1])
                    calculate_lat_phase_accumulated(lat_brkd, data[b][db])
            except:
                continue

        # metric2idx: dict = {}
        # for i, metric in enumerate(title):
        #     metric2idx[metric] = i
        #     if i == 0:
        #         continue
        #     b = metric.split(' ')[0]
        #     db = metric.split(' ')[1]
        #     if b not in data:
        #         data[b] = {}
        #     if db not in data[b]:
        #         data[b][db] = {}

        # for row in csv_reader:
        #     ph = row[0]
        #     for m in title[1:]:
        #         b = m.split(' ')[0]
        #         db = m.split(' ')[1]
        #         try:
        #             l = int(row[metric2idx[m]])
        #             data[b][db][ph]=l
        #         except:
        #             pass
        
        for b in backends:
            lat[b] = {}
            for ph in phases:
                for db in databases:
                    try:
                        if ph not in data[b][db]:
                            continue
                        if ph not in lat[b]:
                            lat[b][ph] = []
                        lat[b][ph].append(data[b][db][ph]/1000000)
                    except:
                        continue

        x = np.arange(len(databases))
        print (x)
        width = 0.2
        fig, (ax1, ax2)  = plt.subplots(2, 1, sharex=True, figsize=(7, 2.8), gridspec_kw={'height_ratios': [0.5, 2]})
        fig.subplots_adjust(hspace=-0.2)  
    
        i = -1
        
        
        lines = []
        # for h, b in enumerate(phases):
        #     lines.append(ax.bar(x, np.zeros(len(databases)), label=phases, hatch=hatch[h], color='white', edgecolor='black'))
        # fig.legend(lines, labels=phases, loc='upper center', ncol=len(phases), frameon=False, fontsize=12, handletextpad=0.1, columnspacing=0.3)

        hatch.reverse()
        phases.reverse()
        lines.clear()
        for b in backends:
            lines.append(ax2.bar(x + i*width, np.zeros(len(databases)), label=None, color=color[b], edgecolor='black'))
            lines.append(ax1.bar(x + i*width, np.zeros(len(databases)), label=name[b], color=color[b], edgecolor='white'))
            i += 1
        
        #fig.legend(lines, labels=backends, loc='upper center', ncol=len(backends), frameon=False, fontsize=12, bbox_to_anchor=(0.5, 0.97))
        i = -1
        for b in backends:
            # lines.append(ax.bar(x + i*width, np.zeros(len(databases)), label=name[b], color=color[b]))
            for h, ph in enumerate(phases):
                
                if ph not in lat[b]:
                    continue
                print(b, h, ph)
                ax2.bar(x + i*width, lat[b][ph], width, align='center', label=None, color=color[b], hatch=hatch[h], edgecolor='black')
                if b == 'ncl':
                    ax2.bar(0, 0, width, align='center', label=phases[h], color = 'white', hatch=hatch[h], edgecolor='black')
            i += 1
            
        ax2.legend(frameon=False, ncol=5, fontsize=12, loc='upper left', bbox_to_anchor=(-0.15, 1.2), columnspacing=0.5)
        ax1.legend(frameon=False, ncol=3, fontsize=12, loc='upper left', bbox_to_anchor=(0.1, 1.8))
        #h, l = plt.gca().get_legend_handles_labels()
        #print(h,l)
        #plt.legend(l)
        #ax2.set_box_aspect(2)
        #ax.set_xlabel('Database')
        ax2.set_ylabel('Recovery time (s)')
        ax1.spines.bottom.set_visible(False)
        ax1.spines.top.set_visible(False)
        ax1.spines.left.set_visible(False)
        ax1.spines.right.set_visible(False)
        ax1.set_xticks([])
        ax1.set_yticks([])
        ax1.tick_params(labeltop=False)  # don't put tick labels at the top
        ax2.set_xticks(x)
        ax2.set_xticklabels(databases)
        fig.savefig(os.path.join(folder, 'fig', 'recovery-time.pdf'), bbox_inches='tight')


def calculate_lat_phase_accumulated(lat_brkd:'dict[str, int]', data):
    try:
        data['get peer'] = lat_brkd['get peer']
        data['connect'] = data['get peer'] + lat_brkd['connect']
        data['rdma read'] = data['connect'] + lat_brkd['recover']
        data['sync peer'] = data['rdma read'] + lat_brkd['sync peers']
    except:
        pass
    data['parse'] = lat_brkd['end recover']


if __name__ == '__main__':
    folder = '/data/result'
    if len(sys.argv) > 1:
        folder = sys.argv[1]
    draw_recovery(folder)
