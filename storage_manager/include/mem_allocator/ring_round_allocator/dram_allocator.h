/*
 * @author: BL-GS 
 * @date:   2023/3/23
 */

#pragma once

#include <cstdint>
#include <cassert>
#include <span>

#include <spdlog/spdlog.h>
#include <thread/thread.h>
#include <mem_allocator/abstract_mem_allocator.h>

namespace allocator {

	class RindRoundDRAMAllocator {
	public:
		static constexpr size_t ALLOC_ALIGN_SIZE = CACHE_LINE_SIZE;

		inline static const char *DRAM_ROOT_DIR_NAME = "/dev/shm/temp_log";

	private:

		FileDescriptor pmem_descriptor_;

		std::atomic<uint8_t *> cur_bound_;

	public:
		RindRoundDRAMAllocator(size_t tuple_size, size_t expected_amount):
				pmem_descriptor_(DRAM_ROOT_DIR_NAME,
				                 allocate_file_name(),
				                 std::max(align_size(tuple_size) * expected_amount * 2, 1024UL * 1024 * 1024)),
				cur_bound_(pmem_descriptor_.start_ptr){

			spdlog::info("DRAM Map File Path: {}", pmem_descriptor_.file_path.string());
			spdlog::info("DRAM Map Size: {}", pmem_descriptor_.total_size);
		}

		~RindRoundDRAMAllocator() = default;

		constexpr static MemAllocatorControlHeader get_header() {
			return {
					.allocate_order = MemAllocatorOrder::Sequential,
					.mem_type = MemMedia::DRAM
			};
		}

	public:
		void *allocate(size_t size) {
			size = align_size(size);

			while (true) {
				uint8_t *ptr      = cur_bound_.load(std::memory_order::acquire);
				uint8_t *old_ptr  = ptr;
				uint8_t *next_ptr = ptr + size;

				if (next_ptr >= pmem_descriptor_.start_ptr + pmem_descriptor_.total_size) [[unlikely]] {
					// Relocate the space to the start
					ptr = pmem_descriptor_.start_ptr;
					next_ptr = ptr + size;
				}

				// Try to allocate a new space
				if (cur_bound_.compare_exchange_strong(old_ptr, next_ptr)) {
					return ptr;
				}
			}
		}

		void deallocate(void *ptr, [[maybe_unused]] size_t size) {}

		std::span<uint8_t> get_space_range() const {
			return { pmem_descriptor_.start_ptr, pmem_descriptor_.total_size };
		}
		
	private:
		static size_t align_size(size_t size) {
			constexpr size_t ALIGNED_SIZE = CACHE_LINE_SIZE;
			return (size + ALIGNED_SIZE - 1) & (~(ALIGNED_SIZE - 1));
		}

	};
}
