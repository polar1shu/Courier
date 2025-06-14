import matplotlib.pyplot as plt
import pandas as pd

from graph.table import *
from graph.plot import *

plt.rcParams['figure.figsize']=(5, 2.9)

total_table = pd.read_csv("data/PTM-transaction.csv")
ycsb_type_array = ['YCSB_A', 'WriteMostly']
cc_type_array = ['Courier', 'Courier_SAVE', 'SP', 'Timestone', 'Trinity']

#63b2ee - 76da91 - f8cb7f - f89588 - 7cd6cf - 9192ab - 7898e1 - efa666 - eddd86 - 9987ce
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
        abort_ratio = cc_table['Abort ratio(%)']
        plt.plot(
            thread_nums,
            abort_ratio,
            label=line_property.label,
            marker=line_property.marker,
            # color=line_property.color,
            linestyle=line_property.line_style
        )

    # Axis Label
    plt.xlabel('The number of threads', fontsize=17)
    plt.ylabel('Abort rate(%)', fontsize=17)

    plt.xticks(thread_nums, fontsize=14)
    plt.yticks(fontsize=14)

    plt.savefig('fig/ptm-abort-' + ycsb_type + '.pdf', bbox_inches = 'tight')

# Legend
lines, labels = fig.axes[-1].get_legend_handles_labels()
legend = fig.legend(lines, labels, bbox_to_anchor=(0.9, 1.1), fontsize=14, ncol=8, framealpha=0)
export_legend(legend, 'fig/ptm-abort-legend.pdf')