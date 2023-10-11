import os
import sys
import yaml
import matplotlib.pyplot as plt
import numpy as np
from common import *

def draw_ycsb(folder: str, db: str, figname: str, pos: int):
    leg_pos = [0.76, 0.5, 0.1]
    leg_posy = [1.3, 1.28, 1.27]
    backend = ['sync', 'cephfs', 'sync_ncl']
    if db == 'sqlite':
        workloads = ['a', 'b', 'c', 'd', 'f']
    else:
        workloads = ['a', 'b', 'c', 'd', 'f']
    all_data: 'dict[str, dict[str, list[float]]]' = {}
    iter = 3
    for b in backend:
        all_data[b] = {'data':[], 'err':[]}
        for w in workloads:
            tputs = []
            for i in range(1, iter+1):
                try:
                    sample_name = '{}_workload{}_{}_trail{}.yml'.format(db, w, b, i)
                    with open(os.path.join(folder, db, 'run', b, sample_name), 'r') as f:
                        data = yaml.load(f)
                        tputs.append(data['throughput'])
                except:
                    pass
            try:
                all_data[b]['data'].append(np.average(tputs)/1000)
                all_data[b]['err'].append(np.std(tputs)/1000)
            except:
                pass
    
    x = np.arange(len(workloads))
    width = 0.2
    if db =='sqlite':
        plt.rcParams['font.size'] = 18
    #plt.figure().set_figheight(0.755-0.05*pos)
    fig, ax = plt.subplots()
    i = -1
    bars = []
    for b in backend:
        if (i+1) == pos :
            label_name = name[b]
        else:
            label_name = None
        bars.append(ax.bar(x + i*width, all_data[b]['data'], width, yerr=all_data[b]['err'], align='center', capsize=1.5, label=label_name, color=color[b], hatch=hat[b], edgecolor='black'))
        print(b+' '+str(np.array(all_data[b]['data'])/np.array(all_data['sync']['data'])))
        i += 1
    
    ax.set_xticks(x)
    ax.set_xticklabels(workloads)
    ax.set_box_aspect(0.5-0.02*pos)
    if db =='sqlite':
        ax.set_box_aspect(0.5-0.025*pos)
    
    ax.set_xlabel('Workload')
    ax.set_ylabel('Throughput (KOps/s)')
    ax.legend(loc='upper center', ncol=len(backend), frameon=False, bbox_to_anchor=(leg_pos[pos], leg_posy[pos]), fontsize=18, columnspacing=0.5)
    fig.savefig(os.path.join(folder, 'fig', db+figname), bbox_inches='tight')


if __name__ == '__main__':
    folder = '/data/result'
    if len(sys.argv) > 1:
        folder = sys.argv[1]

    draw_ycsb(folder, 'rocksdb', '_ycsb.pdf', 0)
    draw_ycsb(folder, 'redis', '_ycsb.pdf', 1)
    draw_ycsb(folder, 'sqlite', "_ycsb.pdf", 2)
