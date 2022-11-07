import sys
import os
import time
import signal
import yaml
import numpy
import matplotlib.pyplot as plt

stop: bool = False

def sig_handle(sig, frame):
    global stop
    stop = True

def mon(dir: str, output: str):
    global stop
    mon_record: 'dict[str, dict[str, list]]' = {}

    start = round(time.time() * 1000)
    while not stop:
        try:
            logs = os.listdir(dir)
        except FileNotFoundError:
            continue

        elapse = round(time.time() * 1000) - start
        for log in logs:
            if log not in mon_record:
                mon_record[log] = {'time': [], 'size': []}
            
            try:
                status = os.stat(os.path.join(dir, log))
            except:
                continue
            if status.st_size > 0:
                mon_record[log]['time'].append(elapse)
                mon_record[log]['size'].append(status.st_size)
        
        time.sleep(0.1)
    
    with open(output, 'w') as yml:
        yaml.dump(mon_record, yml)


def draw(file, output):
    with open(file, 'r') as yml:
        mon_record = yaml.load(yml)

        fig, ax = plt.subplots(1,1)
        for log in mon_record:
            ax.plot(numpy.array(mon_record[log]['time']) / 1000, numpy.array(mon_record[log]['size']) / 1024 / 1024)
        
        ax.set_aspect(0.5)
        ax.set_xlabel('time (s)')
        ax.set_ylabel('size (MB)')
        fig.savefig(output, bbox_inches='tight')

if __name__ == '__main__':
    mode: str = sys.argv[1]
    output: str = sys.argv[3]

    if mode == 'mon':
        dir: str = sys.argv[2]
        signal.signal(signal.SIGINT, sig_handle)
        mon(dir, output)
    elif mode == 'draw':
        file: str = sys.argv[2]
        draw(file, output)
