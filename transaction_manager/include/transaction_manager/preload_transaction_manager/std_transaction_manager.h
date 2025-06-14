/*
 * @author: BL-GS 
 * @date:   2023/1/28
 */

#pragma once

#include <spdlog/spdlog.h>

#include <workload/abstract_workload.h>

#include <thread_allocator/thread_allocator.h>
#include <transaction_manager/abstract_transaction_manager.h>

namespace transaction {

	template<class Workload, class CC>
			requires workload::WorkloadConcept<Workload>
			        && ConcurrentControlConcept<CC>
	class PreloadStdTransactionManager {
	public:
		using Self               = PreloadStdTransactionManager<Workload, CC>;

		using ThreadAllocator    = allocator::StdThreadAllocator;

		using WorkloadType       = Workload;

		using TransactionType    = WorkloadType::Transaction;

		using CCType             = CC;

		using ExecutorType       = CC::ExecutorType;

		static constexpr uint32_t INIT_THREAD_NUM = std::min(24U, thread::get_max_tid());

		static constexpr uint32_t PRELOAD_NUMBER_PER_THREAD = 5'000'000;

		static constexpr uint32_t DEFAULT_WARN_UP_MILLI_SEC = 3'000;

	private:
		TransactionManagerInfo info_;

		ThreadAllocator thread_allocator_;

		WorkloadType workload_;

		CCType concurrent_control_;

	public:
		template<class ...Args>
		explicit PreloadStdTransactionManager(ThreadBindStrategy bind_strategy, Args &&...args):
				info_(), thread_allocator_(bind_strategy),
				concurrent_control_(std::forward<Args>(args)...) {};

		TransactionManagerInfo get_manager_info() const {
			return info_;
		}

	public:
		void init() {
			// Get new transaction from workload
			auto init_tx_list = workload_.initialize_insert();

			thread_allocator_.reserve(INIT_THREAD_NUM)
			                 .run_tasks(
									 [this, &init_tx_list](int id) {
										 init_work(id, init_tx_list);
							        }
							)
			                 .wait_all_tasks()
							 .clear_all_tasks();
		}

		void warm_up(const uint32_t num_thread) {
			auto run_time = std::chrono::milliseconds{DEFAULT_WARN_UP_MILLI_SEC};
			std::barrier barrier{num_thread + 1};
			std::atomic_flag stop_flag{false};

			thread_allocator_.reserve(num_thread)
							 .run_tasks([&](int id) { exec_work(id, barrier, stop_flag); });

			barrier.arrive_and_wait();
			auto start_time = std::chrono::steady_clock::now();
			auto end_time = start_time;
			while (!stop_flag.test()) {
				thread::pause();
				end_time = std::chrono::steady_clock::now();
				if (end_time - start_time >= run_time) { break; }
			}
			stop_flag.test_and_set();

			thread_allocator_.wait_all_tasks();
		}

		void run(const uint32_t num_thread, const std::chrono::milliseconds run_time) {
			std::barrier barrier{num_thread + 1};
			std::atomic_flag stop_flag{false};

			thread_allocator_.reserve(num_thread)
			                 .run_tasks([&](int id) { exec_work(id, barrier, stop_flag); });

			barrier.arrive_and_wait();

			auto start_time = std::chrono::steady_clock::now();
			auto end_time = start_time;
			while (!stop_flag.test()) {
				thread::pause();
				end_time = std::chrono::steady_clock::now();
				if (end_time - start_time >= run_time) { break; }
			}
			stop_flag.test_and_set();

			thread_allocator_.clear_all_tasks();

			info_ = TransactionManagerInfo(std::chrono::duration_cast<std::chrono::milliseconds>(end_time - start_time), num_thread, num_thread,
			                               concurrent_control_.get_concurrent_control_message());
		}

	private:
		void init_work(int id, auto &init_tx_list) {
			auto  handle = inner_init_work(id, init_tx_list).handle;
			auto &handle_promise = handle.promise();

			// Start execute function
			while (!handle.done()) {
				handle();
				// Get task error code and deal with it
				TaskError &error = handle_promise.task_error;
				running_task_error_handler(error);
				error = TaskError::None;
			}

			// Get final error code and deal with it
			TaskError &final_error = handle_promise.task_error;
			final_task_error_handler(final_error);
			final_error = TaskError::None;
		}

		void exec_work(int id, std::barrier<> &barrier, std::atomic_flag &stop_flag) {
			std::vector<TransactionType> preload_transactions;
			preload_transactions.reserve(PRELOAD_NUMBER_PER_THREAD);
			for (uint64_t i = 0; i < PRELOAD_NUMBER_PER_THREAD; ++i) {
				preload_transactions.emplace_back(workload_.generate_transaction());
			}

			barrier.arrive_and_wait();

			auto  handle = inner_exec_work(preload_transactions).handle;
			auto &handle_promise = handle.promise();

			// Start execute function
			while (!stop_flag.test() && !handle.done()) {
				handle();
				// Get task error code and deal with it
				TaskError &error = handle_promise.task_error;
				running_task_error_handler(error, stop_flag);
				error = TaskError::None;
			}

			// Get final error code and deal with it
			TaskError &final_error = handle_promise.task_error;
			final_task_error_handler(final_error, stop_flag);
			final_error = TaskError::None;
		}

	private:
		ThreadTaskReturnObject inner_init_work(int id, auto &init_tx_list) {
			for (uint64_t i = id; i < init_tx_list.size(); i += INIT_THREAD_NUM) {
				// Get new transaction from workload
				auto transaction = init_tx_list[i];
				// Get new context for tx execution
				auto executor = concurrent_control_.get_executor();
				// Run transaction.
				while (true) {
					if (transaction.run(executor) && executor.commit()) { break; }

					co_yield TaskError::AssertFault;

					executor.abort();
					executor.reset();
				}
				// Expect not to suspend after a init transaction
				co_await std::suspend_never{};
			}
			// Finish all uncompleted works.
			concurrent_control_.flush_all_works();
			// Exit
			co_return TaskError::None;
		}

		ThreadTaskReturnObject inner_exec_work(std::vector<TransactionType> &preload_transaction_vector) {
			for (TransactionType &transaction: preload_transaction_vector) {
				// Get new context for tx execution
				auto executor = concurrent_control_.get_executor();
				// Run transaction.
				while (true) {
					if (transaction.run(executor) && executor.commit()) { break; }
					// Suspend and return error after abort a transaction.
					co_yield TaskError::Retry;
					// Abort the transaction and retry.
					executor.abort();
					executor.reset();
				}
				// Suspend after finishing a transaction
				co_await std::suspend_always{};
			}
			// Never stop for anything util counter alarming
			co_return TaskError::PreStop;
		}

	private:
		//! @brief Error handler during running
		static void running_task_error_handler(const TaskError &error) {
			if (error != TaskError::None && error != TaskError::Retry) {
				spdlog::error("Error unexpected: ", static_cast<uint32_t>(error));
			}
		};

		//! @brief Error handler during running
		static void running_task_error_handler(const TaskError &error, std::atomic_flag &stop_flag) {
			if (error == TaskError::PreStop) {
				stop_flag.test_and_set();
			}
			else if (error != TaskError::None && error != TaskError::Retry) {
				spdlog::error("Error unexpected: ", static_cast<uint32_t>(error));
			}
		};

		//! @brief Error handler after everything
		static void final_task_error_handler(const TaskError &error) {
			if (error != TaskError::None) {
				spdlog::error("Error unexpected: ", static_cast<uint32_t>(error));
			}
		};

		//! @brief Error handler after everything
		static void final_task_error_handler(const TaskError &error, std::atomic_flag &stop_flag) {
			if (error == TaskError::PreStop) {
				stop_flag.test_and_set();
			}
			else if (error != TaskError::None) {
				spdlog::error("Error unexpected: ", static_cast<uint32_t>(error));
			}
		};
	};

}
