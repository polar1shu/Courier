/*
 * @author: BL-GS 
 * @date:   2023/1/28
 */

#pragma once

#include <cstdint>
#include <chrono>

#include <thread_allocator/abstract_thread_allocator.h>
#include <transaction_manager/task_error.h>

namespace transaction {

	using ThreadBindStrategy     = allocator::ThreadBindStrategy;

	//! @brief Information about transaction execution.
	struct TransactionManagerInfo {
	public:
		uint64_t total_running_time_;

		uint64_t total_thread_num_;

		uint64_t worker_thread_num_;

		uint64_t total_tx_;

		uint64_t abort_tx_;

		uint64_t running_latency_;

		uint64_t commit_latency_;

		uint64_t index_latency_;

		uint64_t validate_latency_;

		uint64_t persist_log_latency_;

		uint64_t persist_data_latency_;

		uint64_t total_transaction_latency_;

		uint64_t running_time_;

		uint64_t commit_time_;

		uint64_t index_time_;

		uint64_t transaction_interface_time_;

		uint64_t validate_time_;

		uint64_t persist_log_time_;

		uint64_t persist_data_time_;

	public:
		TransactionManagerInfo():   total_running_time_(1),
									total_thread_num_(0),
									worker_thread_num_(0),
									total_tx_(0),
									abort_tx_(0),
									running_latency_(0),
									commit_latency_(0),
									index_latency_(0),
									validate_latency_(0),
									persist_log_latency_(0),
									persist_data_latency_(0),
									total_transaction_latency_(0),
									running_time_(0),
									commit_time_(0),
									index_time_(0),
									transaction_interface_time_(0),
									validate_time_(0),
									persist_log_time_(0),
									persist_data_time_(0) {}

		TransactionManagerInfo(std::chrono::milliseconds running_time,
		                       uint64_t total_thread_num,
		                       uint64_t worker_thread_num,
		                       cc::ConcurrentControlMessage cc_message):
				total_running_time_(running_time.count()),
				total_thread_num_(total_thread_num),
				worker_thread_num_(worker_thread_num),
				total_tx_(cc_message.total_tx),
				abort_tx_(cc_message.abort_tx),
				running_latency_(cc_message.get_total_running_latency(99)),
				commit_latency_(cc_message.get_total_commit_latency(99)),
				index_latency_(cc_message.get_total_index_latency(99)),
				validate_latency_(cc_message.get_total_validate_latency(99)),
				persist_log_latency_(cc_message.get_total_persist_log_latency(99)),
				persist_data_latency_(cc_message.get_total_persist_data_latency(99)),
				total_transaction_latency_(cc_message.get_total_total_latency(99)),
				running_time_(cc_message.get_total_running_time()),
				commit_time_(cc_message.get_total_commit_time()),
				index_time_(cc_message.get_total_index_time()),
				validate_time_(cc_message.get_total_validate_time()),
				persist_log_time_(cc_message.get_total_persist_log_time()),
				persist_data_time_(cc_message.get_total_persist_data_time()) {

			transaction_interface_time_    = running_time_ - index_time_;
		}

		TransactionManagerInfo(const TransactionManagerInfo &other) = default;

		TransactionManagerInfo &operator= (const TransactionManagerInfo &other) = default;
	};

	//! @brief Concept about concurrent control
	//! @tparam ConcurrentControl
	template<class ConcurrentControl>
	concept ConcurrentControlConcept = requires(ConcurrentControl cc,
			ConcurrentControl::ExecutorType executor) {

		{ cc.get_executor() } -> std::same_as<typename ConcurrentControl::ExecutorType>;

		{ executor.commit() } -> std::same_as<bool>;
	};


	//! @brief Concept about transaction manager
	//! @tparam TxManager
	template<class TxManager>
	concept TransactionManagerConcept = requires(TxManager manager,
												 TxManager::CCType cc,
	                                             uint32_t num_thread,
												 std::chrono::milliseconds run_time) {

		// ------ Message getter needed to get information about transaction execution.

		//! @brief Message getter function required
		{ manager.get_manager_info() } -> std::same_as<TransactionManagerInfo>;

		// ------ Warming up needed to build a basic system for test

		//! @brief Warming up function required
		{ manager.init() } -> std::same_as<void>;

		// ------ Running needed to begin transaction execution.

		//! @brief Transaction running function required
		{ manager.warm_up(num_thread) } -> std::same_as<void>;

		//! @brief Transaction running function required
		{ manager.run(num_thread, run_time) } -> std::same_as<void>;
	};
}
