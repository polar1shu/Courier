/*
 * @author: BL-GS 
 * @date:   2023/1/1
 */

#pragma once

#include <cstdint>
#include <memory>
#include <stack>
#include <tbb/concurrent_queue.h>

#include <spdlog/spdlog.h>
#include <thread/thread.h>
#include <memory/memory_config.h>
#include <mem_allocator/abstract_mem_allocator.h>

namespace allocator {

	class DRAMPoolAllocator {
	public:
		static constexpr size_t ALLOC_ALIGN_SIZE = CACHE_LINE_SIZE;

		inline static const char *DRAM_ROOT_DIR_NAME = "/dev/shm";

		static constexpr size_t BLOCK_ALIGN_SIZE = CACHE_LINE_SIZE;

		static constexpr size_t THREAD_LOCAL_BUFFER_NUM = 32;

	private:
		tbb::concurrent_queue<void *> blocks_array;

		size_t alloc_size_;

		FileDescriptor pmem_descriptor_;

		std::stack<void *> thread_local_storage_[thread::get_max_tid()];

	public:
		DRAMPoolAllocator(size_t tuple_size, size_t expected_amount):
				alloc_size_(align_block_size(tuple_size)),
				pmem_descriptor_(DRAM_ROOT_DIR_NAME,
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
		}

		DRAMPoolAllocator(std::string_view dir_name, std::string_view file_name, size_t tuple_size, size_t expected_amount):
				alloc_size_(align_block_size(tuple_size)),
				pmem_descriptor_(dir_name,
								 file_name.data() + allocate_file_name(),
				                 std::max(alloc_size_ * expected_amount * 2, 128UL * 1024 * 1024)) {

			for (uint8_t *cur_ptr = pmem_descriptor_.start_ptr;
			     cur_ptr < pmem_descriptor_.start_ptr + pmem_descriptor_.total_size; cur_ptr += alloc_size_) {
				blocks_array.push(cur_ptr);
			}

			spdlog::info("PMEM File Path: {}", pmem_descriptor_.file_path.string());
			spdlog::info("PMEM Map Size: {}", pmem_descriptor_.total_size);
			spdlog::info("Aligned Size of Data Block: {}", alloc_size_);
			spdlog::info("Amount of Data Blocks: {}", blocks_array.unsafe_size());
		}

		constexpr static MemAllocatorControlHeader get_header() {
			return {
					.allocate_order = MemAllocatorOrder::Random,
					.mem_type       = MemMedia::DRAM
			};
		}

		FileDescriptor &get_descriptor() {
			return pmem_descriptor_;
		}

	public:
		void *allocate(size_t size) {
			uint32_t tid = thread::get_tid();
			void *res = nullptr;
			if (thread_local_storage_[tid].empty()) {
				while (!blocks_array.try_pop(res)) {
					if (blocks_array.unsafe_size() == 0) [[unlikely]] {
						spdlog::error(__FILE__, __LINE__, ": Running out of memory");
						exit(-1);
					}
				}
			}
			else {
				res = thread_local_storage_[tid].top();
				thread_local_storage_[tid].pop();
			}
			return res;
		}

		void deallocate(void *ptr, [[maybe_unused]] size_t size) {
			if (thread_local_storage_->size() >= THREAD_LOCAL_BUFFER_NUM) {
				blocks_array.push(ptr);
			}
			else {
				thread_local_storage_->push(ptr);
			}
		}

		void recovery_iteration(std::function<bool(void *data_ptr)> &callback_func) {
			spdlog::error("Not implemented");
			exit(-1);
		}

	private:
		static size_t align_block_size(size_t origin_size) {
			return (origin_size + (BLOCK_ALIGN_SIZE - 1)) & (~(BLOCK_ALIGN_SIZE - 1));
		}
	};
}
