/*
 * @author: BL-GS 
 * @date:   2023/1/1
 */

#pragma once

#include <cstdint>
#include <stack>
#include <tbb/concurrent_queue.h>

#include <spdlog/spdlog.h>
#include <thread/thread.h>
#include <memory/memory_config.h>
#include <mem_allocator/abstract_mem_allocator.h>

namespace allocator {

	class DRAMAllocator {
	public:
		static constexpr size_t ALLOC_ALIGN_SIZE = CACHE_LINE_SIZE;

	public:
		DRAMAllocator(size_t tuple_size, size_t expected_amount = 0) {}

		constexpr static MemAllocatorControlHeader get_header() {
			return {
				.allocate_order = MemAllocatorOrder::Random,
				.mem_type = MemMedia::DRAM,
			};
		}

	public:
		static void *allocate(size_t size) {
			return operator new(size, static_cast<std::align_val_t>(ALLOC_ALIGN_SIZE));
		}

		static void deallocate(void *ptr, [[maybe_unused]]size_t size) {
			operator delete(ptr, static_cast<std::align_val_t>(ALLOC_ALIGN_SIZE));
		}

		void recovery_iteration(std::function<bool(void *data_ptr)> &callback_func) {
			spdlog::error("Not implemented");
			exit(-1);
		}
	};

}
