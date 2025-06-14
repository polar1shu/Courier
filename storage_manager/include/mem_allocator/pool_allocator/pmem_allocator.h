/*
 * @author: BL-GS 
 * @date:   2023/1/9
 */

#pragma once

#include <cstdint>
#include <stack>
#include <cassert>
#include <spdlog/spdlog.h>
#include <tbb/concurrent_queue.h>

#include <util/utility_macro.h>
#include <thread/thread.h>
#include <mem_allocator/abstract_mem_allocator.h>

namespace allocator {
	class SimplePmemAllocator {
	public:
		static constexpr size_t ALLOC_ALIGN_SIZE = CACHE_LINE_SIZE;

		inline static const char *PMEM_ROOT_DIR_NAME = GLOBAL_DATA_MEM_DIR_PATH[0];

		static constexpr size_t BLOCK_ALIGN_SIZE = CACHE_LINE_SIZE;

		static constexpr size_t THREAD_LOCAL_BUFFER_NUM = 32;

	private:
		tbb::concurrent_queue<void *> blocks_array;

		size_t alloc_size_;

		FileDescriptor pmem_descriptor_;

		std::stack<void *> thread_local_storage_[thread::get_max_tid()];

	public:
		SimplePmemAllocator(size_t tuple_size, size_t expected_amount):
			alloc_size_(align_block_size(tuple_size)),
			pmem_descriptor_(PMEM_ROOT_DIR_NAME,
			                 allocate_file_name(),
							 std::max(alloc_size_ * expected_amount * 2, 128UL * 1024 * 1024)) {

			for (uint8_t *cur_ptr = pmem_descriptor_.start_ptr;
						cur_ptr < pmem_descriptor_.start_ptr + pmem_descriptor_.total_size; cur_ptr += alloc_size_) {
				blocks_array.push(cur_ptr);
			}

			spdlog::info("PMEM File Path: {}", pmem_descriptor_.file_path.string());
			spdlog::info("PMEM Map Size: {}", pmem_descriptor_.total_size);
			spdlog::info("Aligned Size of Data Block: {}", alloc_size_);
			spdlog::info("Amount of Data Blocks: {}", blocks_array.unsafe_size());
			spdlog::info("Address range: 0x{} - 0x{}",
			                   static_cast<void *>(pmem_descriptor_.start_ptr),
							   static_cast<void *>(pmem_descriptor_.start_ptr + pmem_descriptor_.total_size));
		}

		constexpr static MemAllocatorControlHeader get_header() {
			return {
					.allocate_order = MemAllocatorOrder::Random,
					.mem_type       = MemMedia::PMEM
			};
		}

	public:
		void *allocate(size_t size) {
			const uint32_t tid = thread::get_tid();
			void *res = nullptr;
			if (thread_local_storage_[tid].empty()) {
				while (!blocks_array.try_pop(res)) {
					if (blocks_array.unsafe_size() == 0) [[unlikely]] {
						spdlog::error(__FILE__, __LINE__, ": Running out of memory");
						exit(-1);
					}
				}
				DEBUG_ASSERT(res != nullptr);
			}
			else {
				res = thread_local_storage_[tid].top();
				thread_local_storage_[tid].pop();
				DEBUG_ASSERT(res != nullptr);
			}

			return res;
		}
		
		void deallocate(void *ptr, [[maybe_unused]] size_t size) {
			if (thread::is_registered()) [[likely]] {
				const uint32_t tid = thread::get_tid();
				if (thread_local_storage_[tid].size() >= THREAD_LOCAL_BUFFER_NUM) {
					blocks_array.push(ptr);
				}
				else {
					thread_local_storage_[tid].push(ptr);
				}
			}
			else {
				blocks_array.push(ptr);
			}
		}

		void recovery_iteration(std::function<bool(void *data_ptr)> &callback_func) {
			uint32_t iter_num = blocks_array.unsafe_size();
			const uint32_t thread_num = 24;
			const uint32_t avg_num = iter_num / thread_num;

			auto task_func = [this, &callback_func](uint32_t task_num){
				void *data_ptr = nullptr;
				while (task_num != 0 && blocks_array.try_pop(data_ptr)) {
					if (!callback_func(data_ptr)) {
						blocks_array.push(data_ptr);
					}
					--task_num;
				}
			};

			std::vector<std::thread> thread_vec;
			for (uint32_t i = 0; i < thread_num; ++i) {
				thread_vec.emplace_back(task_func, avg_num);
			}
			task_func(iter_num - thread_num * avg_num);
			for (auto &t: thread_vec) { t.join(); }
		}

	private:
		static size_t align_block_size(size_t origin_size) {
			return (origin_size + (BLOCK_ALIGN_SIZE - 1)) & (~(BLOCK_ALIGN_SIZE - 1));
		}
	};
}
