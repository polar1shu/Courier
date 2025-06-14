import matplotlib.pyplot as plt
import pandas as pd

from graph.table import *
from graph.plot import *

plt.rcParams['figure.figsize']=(5, 2.2)

total_table = pd.read_csv("data/PTM-transaction.csv")
ycsb_type_array = ['ReadMostly', 'WriteIntensive', 'WriteMostly']
cc_type_array = ['Courier', 'Courier_SAVE', 'SP', 'Timestone', 'Trinity', 'Zen-TICTOC', 'Zen-MVCC', 'Zen-SILO']
cc_latency_type_array = ['Courier', 'Courier_SAVE', 'SP', 'Timestone', 'Trinity']

#63b2ee - 76da91 - f8cb7f - f89588 - 7cd6cf - 9192ab - 7898e1 - efa666 - eddd86 - 9987ce - 63b2ee - 76da91
line_graph_properties = {'Courier': LineGraphProperty('Courier', 'o', '#63b2ee', '-'),
                         'Courier_SAVE': LineGraphProperty('Courier-Save', 'o', '#76da91', '-'),
                         'SP': LineGraphProperty('SP3', 's', '#f8cb7f', '-'),
                         'Trinity': LineGraphProperty('Trinity', '^', '#f89588', '-'),
                         'Timestone': LineGraphProperty('Timestone', 'v', '#9192ab', '-'),
                         'Zen-TICTOC': LineGraphProperty('Zen-Tictoc', 'd', '#9987ce', '--'),
                         'Zen-MVCC': LineGraphProperty('Zen-MVCC', 'd', '#efa666', '--'),
                         'Zen-SILO': LineGraphProperty('Zen-SILO', 'd', '#7898e1', '--')}


for idx, ycsb_type in enumerate(ycsb_type_array):
    fig = plt.figure()

    ycsb_a_table = get_workload_table(total_table, ycsb_type)

    for cc in cc_type_array:
        cc_table = get_cc_table(ycsb_a_table, cc)
        line_property = line_graph_properties[cc]

        thread_nums = cc_table['[Task Summary]Total Thread Num()']
        throughputs = cc_table['[Task Summary]Speed(txn/s)'] / 1e6
        plt.plot(
            thread_nums,
            throughputs,
            label=line_property.label,
            marker=line_property.marker,
            # color=line_property.color,
            linestyle=line_property.line_style
        )

    # Axis Label
    plt.xlabel('The number of threads', fontsize=14)
    plt.ylabel('Throughput(Mtxn/s)', fontsize=14)

    plt.xticks(thread_nums, fontsize=12)
    plt.yticks(fontsize=12)

    plt.savefig('fig/ptm-gen-' + ycsb_type + '.pdf', bbox_inches = 'tight')

# Legend
lines, labels = fig.axes[-1].get_legend_handles_labels()
legend = fig.legend(lines, labels, bbox_to_anchor=(0.9, 1.1), fontsize=14, ncol=8, framealpha=0)
export_legend(legend, 'fig/ptm-gen-legend.pdf')