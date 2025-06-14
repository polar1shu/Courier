import os
import getopt
import re
import sys


def get_sys_command_result(command):
    result = os.popen(command)
    res = result.read()
    result.close()
    return res


CPU = {
    "CORE_NUM": ("The number of cores in one cpu",
                 get_sys_command_result(
                     "cat /proc/cpuinfo| grep \"physical id\"| sort| uniq| wc -l | tr -cd \"[0-9]\"")),
    "PHYSICAL_NUM": ("The number of available physical cpu",
                     get_sys_command_result("cat /proc/cpuinfo| grep \"cpu cores\"| uniq | tr -cd \"[0-9]\"")),
    "LOGICAL_NUM": ("The number of available logical cpu",
                    os.sysconf("SC_NPROCESSORS_ONLN")),
    "FREQUENCY": ("The frequency of cpu",
                  get_sys_command_result(
                      "cat /sys/devices/system/cpu/cpu1/cpufreq/scaling_cur_freq | tr -cd \"[0-9]\""))
}

CACHE = {
    "CACHE_LINE_SIZE_L1": ("The size of L1 cache line",
                           get_sys_command_result(
                               'getconf -a | grep \'LEVEL1_DCACHE_SIZE\' | sed \'s/LEVEL1_DCACHE_SIZE//g\' | tr -cd \"[0-9]\"')),
    "CACHE_LINE_SIZE_L2": ("The size of L2 cache line",
                           get_sys_command_result(
                               'getconf -a | grep \'LEVEL2_CACHE_SIZE\' | sed \'s/LEVEL2_CACHE_SIZE//g\' | tr -cd \"[0-9]\"')),
    "CACHE_LINE_SIZE_L3": ("The size of L3 cache line",
                           get_sys_command_result(
                               'getconf -a | grep \'LEVEL3_CACHE_SIZE\' | sed \'s/LEVEL3_CACHE_SIZE//g\' | tr -cd \"[0-9]\"')),

    "CACHE_LINE_L1": ("The size of L1 cache",
                      get_sys_command_result(
                          'getconf -a | grep \'LEVEL1_DCACHE_LINESIZE\' | sed \'s/LEVEL1_DCACHE_LINESIZE//g\' | tr -cd \"[0-9]\"')),
    "CACHE_LINE_L2": ("The size of L2 cache",
                      get_sys_command_result(
                          'getconf -a | grep \'LEVEL2_CACHE_LINESIZE\' | sed \'s/LEVEL2_CACHE_LINESIZE//g\' | tr -cd \"[0-9]\"')),
    "CACHE_LINE_L3": ("The size of L3 cache",
                      get_sys_command_result(
                          'getconf -a | grep \'LEVEL3_CACHE_LINESIZE\' | sed \'s/LEVEL3_CACHE_LINESIZE//g\' | tr -cd \"[0-9]\"'))
}

NUMA = {
    "NODE_NUM": ("The number of numa nodes",
                 get_sys_command_result('lscpu | grep \"NUMA node(s)\" | tr -cd \"[0-9]\"'))
}
base = get_sys_command_result('numactl --hardware | grep \'cpus\'')
numa_node_res = []
pattern = 'node \d+ cpus: (.*)'
mode = re.compile(pattern)
res = re.findall(mode, base)
for sub_res in res:
    numa_node_res.append('{ ' + ', '.join(sub_res.split(' ')) + ' }')
NUMA['NODE_CPUS'] = ('The allocation of CPUs on each NUMA node',
                     '{ ' + ', '.join(numa_node_res) + ' }')

MEMORY = {
    "PAGE_SIZE": ("The size of memory page",
                  os.sysconf('SC_PAGESIZE')),
    "MAX_MEMORY_SIZE": ("The maximum size of memory",
                        get_sys_command_result('cat /proc/meminfo | grep \"MemTotal\" | tr -cd \"[0-9]\"'))
}


def note_connection(note):
    return '/// ' + note


def macro_connection(domain, struct, name):
    return domain + '_' + struct + '_' + name


def make_initiative_list(array):
    res = '{ '

    first = True
    for item in array:
        if not first:
            res += ', '
        res += '\"' + str(item) + '\"'
        first = False

    res += ' }'
    return res


def definition_connection(variant, content):
    return '#define ' + variant + '\t' + content


def add_def_announcement(note, variant, default_value, ifdef_value):

    if note is not None:
        res = str(note) + '\n'
    else:
        res = ""

    res += '#ifndef ' + variant + '\n'
    if ifdef_value is not None:
        res += '\t#ifndef ' + ifdef_value + '\n'
        res += '\t\t' + definition_connection(variant, str(default_value)) + '\n'
        res += '\t#else\n'
        res += '\t\t' + definition_connection(variant, str(ifdef_value)) + '\n'
        res += '\t#endif\n'
    else:
        res += '\t#ifndef ' + variant + '\n'
        res += '\t\t' + definition_connection(variant, str(default_value)) + '\n'
        res += '\t#endif\n'
    res += '#endif\n'

    return res + '\n'


if __name__ == '__main__':

    with open('util/include/arch/arch.h', 'w') as f:

        f.write("\n/// ARCHITECTURE MACRO\n\n")

        for key, value in CPU.items():
            actual_variant = macro_connection('ARCH', 'CPU', key)
            defined_variant = actual_variant + '_DEFINED'

            f.write(
                add_def_announcement(
                    note_connection(value[0]), actual_variant, value[1], defined_variant
                )
            )

        for key, value in CACHE.items():
            actual_variant = macro_connection('ARCH', 'CACHE', key)
            defined_variant = actual_variant + '_DEFINED'

            f.write(
                add_def_announcement(
                    note_connection(value[0]), actual_variant, value[1], defined_variant
                )
            )

        for key, value in MEMORY.items():
            actual_variant = macro_connection('ARCH', 'MEMORY', key)
            defined_variant = actual_variant + '_DEFINED'

            f.write(
                add_def_announcement(
                    note_connection(value[0]), actual_variant, value[1], defined_variant
                )
            )

        for key, value in NUMA.items():
            actual_variant = macro_connection('ARCH', 'NUMA', key)
            defined_variant = actual_variant + '_DEFINED'

            f.write(
                add_def_announcement(
                    note_connection(value[0]), actual_variant, value[1], defined_variant
                )
            )

        f.write("\n/// VARIANT DEFINITION\n\n")

        opts, args = getopt.getopt(sys.argv[1:], 'd:', ['pmem_dir='])
        for opt_name, opt_value in opts:
            if opt_name in ('-d', '--pmem_dir'):
                pmem_dir_array = opt_value.split(',')
                for dir_name in pmem_dir_array:
                    if not os.path.exists(dir_name):
                        os.mkdir(dir_name)

                actual_variant = macro_connection('ARCH', 'PMEM', 'DIR_NAME')
                defined_variant = actual_variant + '_DEFINED'
                f.write(add_def_announcement(
                    note_connection('The names of directory where pmem is mounted at'),
                    actual_variant,
                    make_initiative_list(pmem_dir_array), defined_variant))

            else:
                print("Unknown operator: " + opt_name)
