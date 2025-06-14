#include <string>
#include <PTM/bench.h>

#include <global_config/global_config.h>

using namespace ptm;


/*!
 * @brief For auto testing, a macro 'AUTO_TEST' must be defined as 'true', and macros
 * as followed should be explicitly defined.
 * For macro conducted from cmake to process, please refer to 'auto_test.cmake'
 * For supported macro contents, please refer to global_config and resort to corresponding enum class
 * @tparam WORKLOAD_DEFINED
 * @tparam INDEX_DEFINED
 * @tparam CONCURRENT_CONTROL_DEFINED
 * @tparam TRANSACTION_MANAGER_DEFINED
 * @tparam STORAGE_MANAGER_DEFINED
 * @tparam BindStrategy
 */
#ifdef AUTO_TEST

	/// Workload Configuration
	#define WORKLOAD_MACRO(type) WorkloadType::type
	constexpr auto Workload = WORKLOAD_MACRO(WORKLOAD_DEFINED);
	#undef WORKLOAD_MACRO

	/// Index Configuration
	#define INDEX_MACRO(type) IndexType::type
	constexpr auto DataIndex = INDEX_MACRO(INDEX_DEFINED);
	#undef INDEX_MACRO

	/// Concurrent control Configuration
	#define CONCURRENT_CONTROL_MACRO(type) ConcurrentControlType::type
	constexpr auto ConcurrentControl = CONCURRENT_CONTROL_MACRO(CONCURRENT_CONTROL_DEFINED);
	#undef CONCURRENT_CONTROL_MACRO

	/// Transaction Configuration
	#define TRANSACTION_MANAGER_MACRO(type) TransactionManagerType::type
	constexpr auto TransactionManager = TRANSACTION_MANAGER_MACRO(TRANSACTION_MANAGER_DEFINED);
	#undef TRANSACTION_MANAGER_MACRO

	/// Storage Manager Configuration
	#define STORAGE_MANAGER_MACRO(type) StorageManagerType::type
	constexpr auto StorageManager = STORAGE_MANAGER_MACRO(STORAGE_MANAGER_DEFINED);
	#undef STORAGE_MANAGER_MACRO

	/// Storage Manager Configuration
	#define THREAD_BIND_MACRO(type) ThreadBindStrategyType::type
	constexpr auto BindStrategy = THREAD_BIND_MACRO(THREAD_BIND_DEFINED);
	#undef STORAGE_MANAGER_MACRO

	/// Number of threads to be test
	const std::vector<uint32_t> test_thread_num{THREAD_AMOUNT_DEFINED};

#else
	/*!
	 * @brief Configuration for manual defined, don't forget to rerun cmake.
	 */

	/// Workload Configuration
	/// When generating non-reentrant transaction, DO NOT warm up and DO NOT preload
	constexpr auto Workload = WorkloadType::TPCC_Light;

	/// Index Configuration
	constexpr auto DataIndex = IndexType::HashMap;

	/// Concurrent control Configuration
	constexpr auto ConcurrentControl = ConcurrentControlType::COURIER_SAVE;

	/// Transaction Configuration
	constexpr auto TransactionManager = TransactionManagerType::StdTransactionManager;

	/// Storage Manager Configuration
	constexpr auto StorageManager = StorageManagerType::Simple_PMEMDATA_PMEMLOG;

	/// Strategy of thread binding
	constexpr ThreadBindStrategyType BindStrategy = ThreadBindStrategyType::NUMABind;

	/// Number of threads to be test
	const std::vector<uint32_t> test_thread_num = std::vector<uint32_t>({48});
#endif

// Global Configuration
using GlobalConfig = GlobalConfigManager<
				Workload,
				DataIndex,
				ConcurrentControl,
				TransactionManager,
				BindStrategy,
				StorageManager>;

int main() {

	/// Duration in millisecond for each test.
	constexpr uint32_t test_time_millisecond = 60'000;

	// Test bench
	ptm::bench<GlobalConfig> bench;
	// Start bench
	bench.run(test_thread_num, test_time_millisecond, true);

	return 0;
}