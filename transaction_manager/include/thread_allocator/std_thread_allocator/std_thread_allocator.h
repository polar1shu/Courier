/*
 * @author: BL-GS 
 * @date:   2023/1/2
 */

#pragma once

#include <cstdint>
#include <utility>
#include <list>
#include <thread>
#include <future>
#include <numeric>
#include <latch>
#include <barrier>
#include <spdlog/spdlog.h>

#include <thread/thread.h>
#include <thread_allocator/abstract_thread_allocator.h>


namespace allocator {

	inline namespace sstd {

		inline void thread_bind(ThreadBindStrategy thread_bind_strategy, int id) {
			if (thread_bind_strategy == ThreadBindStrategy::NUMABind) {
				int numa_node_num = thread::ThreadConfig::get_num_numa_node();
				int numa_id;

				int temp_id = id;
				for (numa_id = 0; numa_id < numa_node_num; ++numa_id) {
					temp_id -= thread::ThreadInfo::get_num_cpu_on_node(numa_id);
					if (temp_id < 0) { break; }
				}

				if (!thread::THREAD_CONTEXT.bind_tid(id)) {
					spdlog::error("Unable to bind tid ", id);
				}

				int cpu_id = thread::THREAD_CONTEXT.bind_cpu_on_node(numa_id);
				if (cpu_id == -1) {
					spdlog::error("Unable to bind cpu on node {}", numa_id);
				}
			}
			else if (thread_bind_strategy == ThreadBindStrategy::HybridBind) {
				int numa_id = id % thread::ThreadInfo::get_num_numa_node();
				if (!thread::THREAD_CONTEXT.bind_tid(id)) {
					spdlog::error("Unable to bind tid {}", id);
				}

				int cpu_id = thread::THREAD_CONTEXT.bind_cpu_on_node(numa_id);
				if (cpu_id == -1) {
					spdlog::error("Unable to bind cpu on node {}", numa_id);
				}
			}
			else if (thread_bind_strategy == ThreadBindStrategy::CPUBind) {
				if (!thread::THREAD_CONTEXT.bind_tid(id)) {
					spdlog::error("Unable to bind tid {}", id);
				}

				auto [numa_id, cpu_id] = thread::THREAD_CONTEXT.bind_cpu();
				if (cpu_id == -1) {
					spdlog::error("Unable to bind cpu");
				}
			}
			else if (thread_bind_strategy == ThreadBindStrategy::RemoteNUMABind) {
				int numa_node_num = thread::ThreadConfig::get_num_numa_node();
				int numa_id;

				int temp_id = id;
				for (numa_id = numa_node_num - 1; numa_id >= 0; --numa_id) {
					temp_id -= thread::ThreadInfo::get_num_cpu_on_node(numa_id);
					if (temp_id < 0) { break; }
				}

				if (!thread::THREAD_CONTEXT.bind_tid(id)) {
					spdlog::error("Unable to bind tid {}", id);
				}

				int cpu_id = thread::THREAD_CONTEXT.bind_cpu_on_node(numa_id);
				if (cpu_id == -1) {
					spdlog::error("Unable to bind cpu on node {}", numa_id);
				}
			}
		}

		inline void thread_unbind() {
			thread::THREAD_CONTEXT.deallocate_tid();
		}

		enum class ThreadStatus {
			Exec,
			Stop,
			Idle
		};

		struct ThreadHandle {
			std::atomic<ThreadStatus> status_;

			std::function<void(int)> execute_func_;

			std::thread thread_handle_;

			ThreadHandle(ThreadBindStrategy bind_strategy, int id):
					status_(ThreadStatus::Idle),
					thread_handle_([this, bind_strategy, id](){
						while (true) {
							switch (ThreadStatus exec_status = ThreadStatus::Exec; status_.load(std::memory_order::acquire)) {
								case ThreadStatus::Idle:
									thread::pause();
									break;

								case ThreadStatus::Exec:
									thread_bind(bind_strategy, id);
									execute_func_(id);
									thread_unbind();
									status_.compare_exchange_strong(exec_status, ThreadStatus::Idle);
									break;

								case ThreadStatus::Stop:
									return;
							}
						}
					}) {}

			ThreadHandle(ThreadHandle &&other) noexcept = default;

			~ThreadHandle() {
				status_.store(ThreadStatus::Stop);
				thread_handle_.join();
			}

		public:
			template<class Func>
			bool set_func_ptr(Func &new_func) {
				if (status_ == ThreadStatus::Idle) {
					execute_func_ = new_func;
					return true;
				}
				return false;
			}

			bool start_exec() {
				if (status_ == ThreadStatus::Idle) {
					status_.store(ThreadStatus::Exec);
					return true;
				}
				return false;
			}

			void wait() const {
				while (status_.load(std::memory_order::acquire) == ThreadStatus::Exec) {
					thread::pause();
				}
			}
		};

		//! @brief An thread allocator implemented with std::thread, which is also simple
		//! because there is no complex management but creating threads and run the task.
		class StdThreadAllocator {
		public:
			using Self = StdThreadAllocator;

		private:
			ThreadBindStrategy thread_bind_strategy_;

			std::list<ThreadHandle> thread_handle_array_;

		public:
			explicit StdThreadAllocator(ThreadBindStrategy bind_strategy):
					thread_bind_strategy_(bind_strategy) { }

		public:
			/*!
			 * @brief Run tasks in specific number of threads.
			 * @tparam TaskFunc
			 * @param func The task to execute.
			 */
			template<class TaskFunc>
			Self &run_tasks(TaskFunc &&func) {
				for (ThreadHandle &handle: thread_handle_array_) {
					handle.set_func_ptr(func);
				}
				for (ThreadHandle &handle: thread_handle_array_) {
					handle.start_exec();
				}
				return *this;
			}

		public:
			Self &reserve(size_t num) {
				size_t size = thread_handle_array_.size();
				for (size_t i = num; i < size; ++i) {
					thread_handle_array_.pop_back();
				}
				for (size_t i = size; i < num; ++i) {
					thread_handle_array_.emplace_back(thread_bind_strategy_, i);
				}
				return *this;
			}

			Self &wait_all_tasks() {
				for (ThreadHandle &handle: thread_handle_array_) {
					handle.wait();
				}
				return *this;
			}

			Self &clear_all_tasks() {
				thread_handle_array_.clear();
				return *this;
			}

		public:
			size_t get_thread_num() const {
				return thread_handle_array_.size();
			}
		};

	}

}
