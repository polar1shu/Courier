import matplotlib.pyplot as plt
import pandas as pd

from graph.table import *
from graph.plot import *

total_table = pd.read_csv("data/PTM-transaction.csv")
ycsb_a_table = get_workload_table(total_table, 'YCSB_A')

ycsb_type_array = ['YCSB_A', 'YCSB_B', 'YCSB_C']
cc_type_array = ['SP', 'Timestone', 'Trinity', 'Zen-TICTOC', 'Zen-MVCC', 'Zen-SILO']

#63b2ee - 76da91 - f8cb7f - f89588 - 7cd6cf - 9192ab - 7898e1 - efa666 - eddd86 - 9987ce - 63b2ee - 76da91
bar_graph_properties = {'Timestone': BarGraphProperty('Timestone', '#3b6291', '/'),
                        'SP': BarGraphProperty('SP3', '#779043', '.'),
                        'Trinity': BarGraphProperty('Trinity', '#779042', '+'),
                        'Zen-TICTOC': BarGraphProperty('Zen-Tictoc', '#624c7c', '-'),
                        'Zen-MVCC': BarGraphProperty('Zen-MVCC', '#388498', '-'),
                        'Zen-SILO': BarGraphProperty('Zen-SILO', '#bf7334', '-')
                        }

throughput_map = {
    'Timestone': [(24, 9413541), (24, 9413541)],
    'SP': [(24, 22424970), (24, 22424970)],
    'Trinity': [(24, 22733473), (24, 22733473)],
    'Zen-TICTOC': [(16, 8069133), (24, 8007559)],
    'Zen-MVCC': [(8, 5033688), (24, 2517676)],
    'Zen-SILO': [(16, 8238093), (24, 7929662)]
}

fig = plt.figure(figsize=(4.5, 3.2))
width = 0.4

for offset, cc in enumerate(cc_type_array):
    cc_table = get_cc_table(ycsb_a_table, cc)
    bar_property = bar_graph_properties[cc]

    throughput = throughput_map[cc][0]
    ax1 = plt.bar(
        offset - width / 2,
        throughput[1],
        width,
        color='#8c564b'
    )
    plt.bar_label(ax1, labels=[throughput[0]], label_type='edge', fontsize=12)
    throughput = throughput_map[cc][1]
    throughput
    ax2 = plt.bar(
        offset + width / 2,
        throughput[1],
        width,
        color='#2ca02c'
    )

# Axis Label
plt.ylabel('Throughput(txn/s)', fontsize=17)
plt.ylim(0, 2.5 * 1e7)
xticks = ['SP3', 'Timestone', 'Trinity', 'Zen-Tictoc', 'Zen-MVCC', 'Zen-SILO']
plt.xticks(np.arange(0, len(xticks)), xticks, rotation=30, fontsize=14)
plt.yticks(np.arange(0, 2.5 * 1e7, 0.5 * 1e7), fontsize=14)
plt.savefig('fig/ptm-write-intensive-50.pdf', bbox_inches='tight')


throughput_map = {
    'Timestone': [9413541, 7759293, 7491313],
    'SP': [22424970, 19421108, 19405616],
    'Trinity': [22733473, 21191169, 17039654],
    'Zen-TICTOC': [8147559, 8004951, 8094215],
    'Zen-MVCC': [2970358, 2299808, 2886528],
    'Zen-SILO': [9299954, 8270757, 8293165]
}

fig = plt.figure(figsize=(4.5, 3.2))
width = 0.25

for offset, cc in enumerate(cc_type_array):
    cc_table = get_cc_table(ycsb_a_table, cc)
    bar_property = bar_graph_properties[cc]

    ax3 = plt.bar(
        offset - width,
        throughput_map[cc][0],
        width,
        color='#1f77b4'
    )
    ax4 = plt.bar(
        offset,
        throughput_map[cc][1],
        width,
        color='#ff7f0e'
    )
    ax5 = plt.bar(
        offset + width,
        throughput_map[cc][2],
        width,
        color='#a7a532'
    )

# Axis Label
plt.ylabel('Throughput(txn/s)', fontsize=17)
plt.ylim(0, 2.5 * 1e7)
xticks = ['SP3', 'Timestone', 'Trinity', 'Zen-Tictoc', 'Zen-MVCC', 'Zen-SILO']
plt.xticks(np.arange(0, len(xticks)), xticks, rotation=30, fontsize=14)
plt.yticks(np.arange(0, 2.5 * 1e7, 0.5 * 1e7), fontsize=14)
plt.savefig('fig/ptm-write-intensive-cross-NUMA.pdf', bbox_inches='tight')


legend = plt.legend([ax1, ax2, ax3, ax4, ax5], ['Peak', '24 threads', 'NUMA-local', 'NUMA remote', 'NUMA hybrid'],
                    fontsize=14, bbox_to_anchor=(1.4, 2), ncol=5, framealpha=0)
export_legend(legend, 'fig/ptm-write-intensive-legend.pdf')
