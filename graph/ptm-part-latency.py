import matplotlib.pyplot as plt
import pandas as pd

from graph.table import *
from graph.plot import *

plt.rcParams['figure.figsize']=(5, 2.9)

total_table = pd.read_csv("data/PTM-transaction.csv")

ycsb_type_array = ['YCSB_A', 'YCSB_B', 'YCSB_C']
cc_type_array = ['SP', 'Timestone', 'Trinity', 'Zen-TICTOC', 'Zen-MVCC', 'Zen-SILO']

#63b2ee - 76da91 - f8cb7f - f89588 - 7cd6cf - 9192ab - 7898e1 - efa666 - eddd86 - 9987ce - 63b2ee - 76da91
bar_graph_properties = {'OCC': BarGraphProperty('OCC', '#3b6291', '/'),
                        'Courier': BarGraphProperty('XXX', '#388498', '-'),
                        'Courier-Save': BarGraphProperty('XXX-Save', '#bf7334', '-')
                        }

throughput_map = {
    'Total': [6792, 2736, 3152],
    'Prepare': [3720, 2584, 3336],
    'Persist Data': [680, 32, 24],
    'Persist Log': [1600, 32, 32],

}
xticks = list(throughput_map.keys())

fig = plt.figure()
width = 0.25

for offset, cc in enumerate(xticks):
    throughput_map[cc] = np.array(throughput_map[cc]) / max(throughput_map[cc]) * 100

    ax1 = plt.bar(
        offset - width,
        throughput_map[cc][0],
        width,
        color='#1f77b4',
        edgecolor='black',
        linewidth=1
    )
    ax2 = plt.bar(
        offset,
        throughput_map[cc][1],
        width,
        color='#ff7f0e',
        edgecolor='black',
        linewidth=1
    )
    ax3 = plt.bar(
        offset + width,
        throughput_map[cc][2],
        width,
        color='#a7a532',
        edgecolor='black',
        linewidth=1
    )

# Axis Label
plt.ylabel('Proportion(%)', fontsize=17)
plt.yticks(fontsize=14)
plt.xticks(np.arange(0, len(xticks)), xticks, rotation=30, fontsize=14)
plt.savefig('fig/ptm-part-latency-48.pdf', bbox_inches='tight')

throughput_map = {
    'Total': [2720, 1976, 2304],
    'Prepare': [1448, 1928, 3128],
    'Persist Data': [592, 32, 24],
    'Persist Log': [216, 32, 32],

}
xticks = list(throughput_map.keys())

fig = plt.figure()
width = 0.2

for offset, cc in enumerate(xticks):
    throughput_map[cc] = np.array(throughput_map[cc]) / max(throughput_map[cc]) * 100

    ax1 = plt.bar(
        offset - width,
        throughput_map[cc][0],
        width,
        color='#1f77b4',
        edgecolor='black',
        linewidth=1
    )
    ax2 = plt.bar(
        offset,
        throughput_map[cc][1],
        width,
        color='#ff7f0e',
        edgecolor='black',
        linewidth=1
    )
    ax3 = plt.bar(
        offset + width,
        throughput_map[cc][2],
        width,
        color='#a7a532',
        edgecolor='black',
        linewidth=1
    )

# Axis Label
plt.ylabel('Proportion(%)', fontsize=17)
plt.yticks(fontsize=14)
plt.xticks(np.arange(0, len(xticks)), xticks, rotation=30, fontsize=14)
plt.savefig('fig/ptm-part-latency-24.pdf', bbox_inches='tight')

legend = fig.legend([ax1, ax2, ax3], ['OCC', 'Courier', 'Courier-Save'], fontsize=12, bbox_to_anchor=(0.9, 1.1), ncol=8,
                    framealpha=0)
export_legend(legend, 'fig/ptm-part-latency-legend.pdf')
