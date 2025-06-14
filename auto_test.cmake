# Necessary Configuration about Components

if (${AUTO_TEST})
    add_definitions(
            -DAUTO_TEST

            # Component of transaction memory system.

            -DWORKLOAD_DEFINED=${WORKLOAD_DEFINED}

            -DINDEX_DEFINED=${INDEX_DEFINED}

            -DCONCURRENT_CONTROL_DEFINED=${CONCURRENT_CONTROL_DEFINED}

            -DTRANSACTION_MANAGER_DEFINED=${TRANSACTION_MANAGER_DEFINED}

            -DSTORAGE_MANAGER_DEFINED=${STORAGE_MANAGER_DEFINED}

            # Fence instruction

            -DPWB_DEFINED=${PWB_DEFINED}

            # The amount and bind strategy of threads

            -DTHREAD_AMOUNT_DEFINED=${THREAD_AMOUNT_DEFINED}

            -DTHREAD_BIND_DEFINED=${THREAD_BIND_DEFINED}

            -DMAX_THREAD_NUM_DEFINED=${THREAD_AMOUNT_DEFINED}

    )
endif ()