import os
import sys
import itertools

import pandas as pd

BUILD_DIR = './build'

WORKLOAD_TYPE = [
    # 'YCSB_A',
    # 'YCSB_B',
    # 'YCSB_C',
    # 'YCSB_D',
    # 'YCSB_E',
    # 'YCSB_F',
    # 'ReadMostly',
    # 'WriteIntensive',
    # 'WriteMostly',
    # 'TPCC',
    'TPCC_Light'
]

DATA_INDEX_TYPE = [
    'BPTree',
    # 'HashMap'
]

CONCURRENT_CONTROL_TYPE = [
    # 'TICTOC',
    # 'OCC',
    # 'TPL',
    # 'MVCC',
    # 'ROMULUS',
    # 'COURIER',
    'COURIER_SAVE',
    'SP',
]

TRANSACTION_MANAGER_TYPE = [
    'StdTransactionManager',
    # 'PreloadStdTransactionManager'
]

STORAGE_MANAGER_TYPE = [
    # 'Random_DRAM',
    # 'Simple_PMEMDATA_DRAMLOG',
    'Simple_PMEMDATA_PMEMLOG',
]

THREAD_BIND_TYPE = [
    # "NUMABind",
    "CPUBind",
    # "HybridBind"
]

THREAD_AMOUNT = [
    1, 8, 16, 24, 32, 48
]


PWB_TYPE = [
    'CLWB',
    # 'CLFLUSH',
    # 'CLFLUSHOPT'
]

exe_status_table = pd.DataFrame(columns=['Workload', 'Index',
                                         'ConcurrentControl',
                                         'TransactionManager',
                                         'StorageManager',
                                         'PWB',
                                         'Exit Code', 'Process'])

if __name__ == '__main__':
    # Start test
    cmake_flags  = " -DCMAKE_TOOLCHAIN_FILE=/home/zxr/lib/vcpkg/scripts/buildsystems/vcpkg.cmake"
    cmake_flags += " -DAUTO_TEST=true"
    cmake_flags += " -DCMAKE_BUILD_TYPE=Release"

    for concurrent_control, \
        workload, \
        index, \
        transaction_manager, \
        storage_manager, \
        thread_bind, \
        pwb_type in itertools.product(CONCURRENT_CONTROL_TYPE,
                                                WORKLOAD_TYPE,
                                                DATA_INDEX_TYPE,
                                                TRANSACTION_MANAGER_TYPE,
                                                STORAGE_MANAGER_TYPE,
                                                THREAD_BIND_TYPE,
                                                PWB_TYPE
                                                ):
        cmake_flags += " -DCONCURRENT_CONTROL_DEFINED=" + concurrent_control
        cmake_flags += " -DWORKLOAD_DEFINED=" + workload
        cmake_flags += " -DINDEX_DEFINED=" + index
        cmake_flags += " -DTRANSACTION_MANAGER_DEFINED=" + transaction_manager
        cmake_flags += " -DSTORAGE_MANAGER_DEFINED=" + storage_manager
        cmake_flags += " -DTHREAD_BIND_DEFINED=" + thread_bind
        cmake_flags += " -DPWB_DEFINED=" + pwb_type

        THREAD_AMOUNT_TYPE = THREAD_AMOUNT

        for thread_amount in THREAD_AMOUNT_TYPE:
            cmake_flags += " -DTHREAD_AMOUNT_DEFINED=" + thread_amount.__str__()

            ret = 0
            process = ""

            # Make option file
            if ret == 0:
                ret = os.system('cd ' + BUILD_DIR + ' && ' +
                                'cmake .. ' + cmake_flags)
                process = "CMake"

            # Compile
            print("Compile")
            if ret == 0:
                ret = os.system('cd ' + BUILD_DIR + ' && make PTM -j')
                process = "Make"

            # Execute
            print("Execution")
            if ret == 0:
                system_sentence = 'cd ' + BUILD_DIR + ' && '

                system_sentence += './PTM'
                ret = os.system(system_sentence)

                process = "Execute"

            if ret == 0:
                process = "Done"

            # exe_status_table = exe_status_table.append(
            #     {
            #         "Workload": workload,
            #         "Index": index,
            #         "ConcurrentControl": concurrent_control,
            #         "TransactionManager": transaction_manager,
            #         "StorageManager": storage_manager,
            #         "ThreadBind": thread_bind,
            #         "PWB": pwb_type,
            #         "Exit Code": ret,
            #         "Process": process
            #     },
            #     ignore_index=True
            # )

    #
    # print("Output table: AutoTest.csv")
    # exe_status_table.to_csv("AutoTest.csv")
