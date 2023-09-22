markers = {'cephfs':'o', 'cephfs_sync':'^', 'ext4':'s', 'ncl':'*', 'sync':'v', 'sync_ncl':'P', 'nclwseq':'X'}
color = {'cephfs':'tab:orange', 'cephfs_sync':'tab:green', 'ext4':'tab:red', 'ncl':'tab:blue', 'sync':'tab:purple', 'sync_ncl':'tab:brown','nclwseq':'tab:pink'}
side = {'cephfs':0, 'cephfs_sync':0, 'sync':0, 'ext4':0, 'ncl':0, 'sync_ncl':0, 'nclwseq':0}
name = {'cephfs': 'weak-app DFT', 'cephfs_sync': 'CephFS Direct I/O', 'ext4': 'ext4', 'ncl': 'SplitFT', 'sync': 'strong-app DFT', 'sync_ncl': 'SplitFT', 'nclwseq': 'NCL w/ Seq'}
hat = {'cephfs': '//', 'ext4': '*', 'ncl': '.', 'sync': '\\', 'sync_ncl': ''}
name_bars = {'cephfs': 'weak-app \n DFT', 'cephfs_sync': 'CephFS Direct I/O', 'ext4': 'ext4', 'ncl': 'NCL Weak', 'sync': 'strong-app \n DFT', 'sync_ncl': 'SplitFT', 'nclwseq': 'NCL w/ Seq'}
glibcnames = {'sync_ncl': 'NCL', 'cephfs': 'weak-bench DFS', 'cephfs_sync': 'CephFS Direct I/O', 'sync' : 'strong-bench DFS'}
linestyles = {'cephfs': 'dotted', 'sync_ncl': 'dashed', 'sync': 'solid', 'cephfs_sync':'dashdot', 'ncl': 'dashed'}
