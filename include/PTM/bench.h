/*
 * @author: BL-GS 
 * @date:   2022/12/31
 */

#pragma once

#include <cstdint>

#include <thread/thread.h>
#include <spdlog/spdlog.h>
#include <listener/listener.h>
#include <global_config/global_config.h>

namespace ptm {

	template<class GlobalConfig>
	class bench {
	public:
		using ConcurrentControl  = GlobalConfig::ConcurrentControl;

		using TransactionManager = GlobalConfig::TransactionManager;

		using StorageManager     = GlobalConfig::StorageManager;

		using WorkloadType       = GlobalConfig::WorkloadType;

		static constexpr auto ThreadBindStrategy = GlobalConfig::ThreadBindStrategy;

	public:
		bench() {}

		//! @brief Turn bench on
		void run(const auto &TEST_THREAD_NUM_ARRAY, uint32_t TEST_TIME_MILLISECOND, bool do_warm_up = false) {
			// Do test on each kind of thread_num
			for (uint32_t thread_num: TEST_THREAD_NUM_ARRAY) {
				// ------ Initialize basic components.

				// Initialize storage manager and transaction manager
				StorageManager storage_manager;
				for (TableScheme table_scheme: WorkloadType::TableSchemeSizeDefinition) {
					storage_manager.add_table(table_scheme.tuple_size, table_scheme.max_tuple_num);
				}

				TransactionManager transaction_manager(ThreadBindStrategy, &storage_manager);
				// Initialize concurrent control class
				cc::ConcurrentControlMessage::disable_record();

				spdlog::info("Start initialization");
				transaction_manager.init();

				// ------- Warming up
				if (do_warm_up) {
					spdlog::info("Start warming up");
					// For warming up, we just insert all base data into system.
					transaction_manager.warm_up(thread_num);
				}

				spdlog::info("Set Listener");

				util::listener::ListenerArray listener_array;
//				listener_array.add_listener(new util::listener::PMMListener);
//				listener_array.add_listener(new util::listener::NUMAWatcher);
//				listener_array.add_listener(new util::listener::TimeListener{});
//				listener_array.add_listener(new util::listener::PAPIListener);

				// ------ Test
				spdlog::info("Start tests");

				// Running transactions
				cc::ConcurrentControlMessage::enable_record();
				listener_array.start_record();

				// ProfilerStart("PTM.prof");
				transaction_manager.run(thread_num, std::chrono::milliseconds(TEST_TIME_MILLISECOND));
				// ProfilerStop();

				listener_array.end_record();
				// Get a relative message about transaction running.
				transaction::TransactionManagerInfo manager_info = transaction_manager.get_manager_info();
				cc::ConcurrentControlMessage::disable_record();

				// ------ Print summary
				// print global property
				GlobalConfig::print_property();
				// print information about task execution
				util::print_property("Task Summary",
									  std::make_tuple("Test time", manager_info.total_running_time_, "ms"),
				                      std::make_tuple("Total Thread Num", manager_info.total_thread_num_, ""),
				                      std::make_tuple("Worker Thread Num", manager_info.worker_thread_num_, ""),
									  std::make_tuple("Total tx", manager_info.total_tx_, ""),
									  std::make_tuple("Abort tx", manager_info.abort_tx_, ""),
									  std::make_tuple("Speed", (manager_info.total_tx_ - manager_info.abort_tx_) * 1000 / manager_info.total_running_time_, "txn/s"),
									  std::make_tuple("Abort rate", static_cast<double>(manager_info.abort_tx_) * 100.0 / manager_info.total_tx_, "%"),
									  std::make_tuple("Running Latency(99%)", manager_info.running_latency_, "ns"),
									  std::make_tuple("Commit Latency(99%)", manager_info.commit_latency_, "ns"),
									  std::make_tuple("Index Latency(99%)", manager_info.index_latency_, "ns"),
									  std::make_tuple("Validate Latency(99%)", manager_info.validate_latency_, "ns"),
									  std::make_tuple("Persist Log Latency(99%)", manager_info.persist_log_latency_, "ns"),
									  std::make_tuple("Persist Data Latency(99%)", manager_info.persist_data_latency_, "ns"),
									  std::make_tuple("Total Latency(99%)", manager_info.total_transaction_latency_, "ns"),
									  std::make_tuple("Running Time", manager_info.running_time_, "ns"),
									  std::make_tuple("Commit Time", manager_info.commit_time_, "ns"),
									  std::make_tuple("Index Time", manager_info.index_time_, "ns"),
									  std::make_tuple("Transaction Interface Time", manager_info.transaction_interface_time_, "ns"),
									  std::make_tuple("Validate Time", manager_info.validate_time_, "ns"),
									  std::make_tuple("Persist Log Time", manager_info.persist_log_time_, "ns"),
									  std::make_tuple("Persist Data Time", manager_info.persist_data_time_, "ns")
									  );
			}
		}
	};
}
