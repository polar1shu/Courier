
/// ARCHITECTURE MACRO

/// The number of cores in one cpu
#ifndef ARCH_CPU_CORE_NUM
	#ifndef ARCH_CPU_CORE_NUM_DEFINED
		#define ARCH_CPU_CORE_NUM	2
	#else
		#define ARCH_CPU_CORE_NUM	ARCH_CPU_CORE_NUM_DEFINED
	#endif
#endif

/// The number of available physical cpu
#ifndef ARCH_CPU_PHYSICAL_NUM
	#ifndef ARCH_CPU_PHYSICAL_NUM_DEFINED
		#define ARCH_CPU_PHYSICAL_NUM	28
	#else
		#define ARCH_CPU_PHYSICAL_NUM	ARCH_CPU_PHYSICAL_NUM_DEFINED
	#endif
#endif

/// The number of available logical cpu
#ifndef ARCH_CPU_LOGICAL_NUM
	#ifndef ARCH_CPU_LOGICAL_NUM_DEFINED
		#define ARCH_CPU_LOGICAL_NUM	56
	#else
		#define ARCH_CPU_LOGICAL_NUM	ARCH_CPU_LOGICAL_NUM_DEFINED
	#endif
#endif

/// The frequency of cpu
#ifndef ARCH_CPU_FREQUENCY
	#ifndef ARCH_CPU_FREQUENCY_DEFINED
		#define ARCH_CPU_FREQUENCY	3400000
	#else
		#define ARCH_CPU_FREQUENCY	ARCH_CPU_FREQUENCY_DEFINED
	#endif
#endif

/// The size of L1 cache line
#ifndef ARCH_CACHE_CACHE_LINE_SIZE_L1
	#ifndef ARCH_CACHE_CACHE_LINE_SIZE_L1_DEFINED
		#define ARCH_CACHE_CACHE_LINE_SIZE_L1	49152
	#else
		#define ARCH_CACHE_CACHE_LINE_SIZE_L1	ARCH_CACHE_CACHE_LINE_SIZE_L1_DEFINED
	#endif
#endif

/// The size of L2 cache line
#ifndef ARCH_CACHE_CACHE_LINE_SIZE_L2
	#ifndef ARCH_CACHE_CACHE_LINE_SIZE_L2_DEFINED
		#define ARCH_CACHE_CACHE_LINE_SIZE_L2	1310720
	#else
		#define ARCH_CACHE_CACHE_LINE_SIZE_L2	ARCH_CACHE_CACHE_LINE_SIZE_L2_DEFINED
	#endif
#endif

/// The size of L3 cache line
#ifndef ARCH_CACHE_CACHE_LINE_SIZE_L3
	#ifndef ARCH_CACHE_CACHE_LINE_SIZE_L3_DEFINED
		#define ARCH_CACHE_CACHE_LINE_SIZE_L3	44040192
	#else
		#define ARCH_CACHE_CACHE_LINE_SIZE_L3	ARCH_CACHE_CACHE_LINE_SIZE_L3_DEFINED
	#endif
#endif

/// The size of L1 cache
#ifndef ARCH_CACHE_CACHE_LINE_L1
	#ifndef ARCH_CACHE_CACHE_LINE_L1_DEFINED
		#define ARCH_CACHE_CACHE_LINE_L1	64
	#else
		#define ARCH_CACHE_CACHE_LINE_L1	ARCH_CACHE_CACHE_LINE_L1_DEFINED
	#endif
#endif

/// The size of L2 cache
#ifndef ARCH_CACHE_CACHE_LINE_L2
	#ifndef ARCH_CACHE_CACHE_LINE_L2_DEFINED
		#define ARCH_CACHE_CACHE_LINE_L2	64
	#else
		#define ARCH_CACHE_CACHE_LINE_L2	ARCH_CACHE_CACHE_LINE_L2_DEFINED
	#endif
#endif

/// The size of L3 cache
#ifndef ARCH_CACHE_CACHE_LINE_L3
	#ifndef ARCH_CACHE_CACHE_LINE_L3_DEFINED
		#define ARCH_CACHE_CACHE_LINE_L3	64
	#else
		#define ARCH_CACHE_CACHE_LINE_L3	ARCH_CACHE_CACHE_LINE_L3_DEFINED
	#endif
#endif

/// The size of memory page
#ifndef ARCH_MEMORY_PAGE_SIZE
	#ifndef ARCH_MEMORY_PAGE_SIZE_DEFINED
		#define ARCH_MEMORY_PAGE_SIZE	4096
	#else
		#define ARCH_MEMORY_PAGE_SIZE	ARCH_MEMORY_PAGE_SIZE_DEFINED
	#endif
#endif

/// The maximum size of memory
#ifndef ARCH_MEMORY_MAX_MEMORY_SIZE
	#ifndef ARCH_MEMORY_MAX_MEMORY_SIZE_DEFINED
		#define ARCH_MEMORY_MAX_MEMORY_SIZE	527737508
	#else
		#define ARCH_MEMORY_MAX_MEMORY_SIZE	ARCH_MEMORY_MAX_MEMORY_SIZE_DEFINED
	#endif
#endif

/// The number of numa nodes
#ifndef ARCH_NUMA_NODE_NUM
	#ifndef ARCH_NUMA_NODE_NUM_DEFINED
		#define ARCH_NUMA_NODE_NUM	2
	#else
		#define ARCH_NUMA_NODE_NUM	ARCH_NUMA_NODE_NUM_DEFINED
	#endif
#endif

/// The allocation of CPUs on each NUMA node
#ifndef ARCH_NUMA_NODE_CPUS
	#ifndef ARCH_NUMA_NODE_CPUS_DEFINED
		#define ARCH_NUMA_NODE_CPUS	{{ 0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21, 22, 23, 24, 25, 26, 27 }, { 28, 29, 30, 31, 32, 33, 34, 35, 36, 37, 38, 39, 40, 41, 42, 43, 44, 45, 46, 47, 48, 49, 50, 51, 52, 53, 54, 55 }}
	#else
		#define ARCH_NUMA_NODE_CPUS	ARCH_NUMA_NODE_CPUS_DEFINED
	#endif
#endif


/// VARIANT DEFINITION

/// The names of directory where pmem is mounted at
#ifndef ARCH_PMEM_DIR_NAME
	#ifndef ARCH_PMEM_DIR_NAME_DEFINED
		#define ARCH_PMEM_DIR_NAME	{ "/mnt/pmem0/data_dir", "/mnt/pmem1/data_dir" }
	#else
		#define ARCH_PMEM_DIR_NAME	ARCH_PMEM_DIR_NAME_DEFINED
	#endif
#endif

