/*
 * @author: BL-GS 
 * @date:   2023/3/1
 */

#pragma once

#include <cstdint>
#include <cstring>
#include <vector>
#include <tuple>

#include <thread/thread.h>
#include <memory/memory_config.h>
#include <storage_manager/abstract_storage_manager.h>
#include <mem_allocator/mem_allocator.h>
#include <log_manager/abstract_log_manager.h>

namespace logm {

	template<class LogAllocator>
		requires allocator::SequenceMemAllocatorConcept<LogAllocator>
	class SimpleLogManagerTemplate {
	public:
		static constexpr uint32_t MAX_THREAD = thread::get_max_tid();

		static constexpr uint32_t MAX_LOG_SIZE = 1024 * 128;

		static constexpr size_t LOG_ALLOC_ALIGN_SIZE = LogAllocator::ALLOC_ALIGN_SIZE;

	public:
		using Self         = SimpleLogManagerTemplate;

	public:
		LogAllocator allocator_;

	public:
		SimpleLogManagerTemplate(): allocator_(MAX_LOG_SIZE, MAX_THREAD) {}

		~SimpleLogManagerTemplate() = default;

	public:
		void *allocate(size_t size) {
			return allocator_.allocate(size);
		}

		void deallocate(void *ptr, size_t size) {
			allocator_.deallocate(ptr, size);
		}

		std::span<uint8_t> get_space_range() const {
			return allocator_.get_space_range();
		}
	};
}
