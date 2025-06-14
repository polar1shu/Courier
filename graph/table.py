def get_workload_table(table, workload_type):
    return table[table['[Global Config]Workload()'] == "WorkloadType::" + workload_type]


def get_cc_table(table, cc_type):
    return table[table['[Global Config]ConcurrentControl()'] == "CCKind::" + cc_type]
