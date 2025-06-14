/*
 * @author: BL-GS 
 * @date:   2023/1/14
 */

#pragma once

#include <cstdint>
#include <cassert>
#include <concepts>
#include <span>

#include <memory/file_descriptor.h>

namespace allocator {

	//! @brief Order of allocated space
	enum class MemAllocatorOrder {
		/// Sequential allocation
		Sequential,
		/// Thread-local sequential allocation
		ThreadSequential,
		/// Random allocation
		Random
	};

	//! @brief Storage media
	enum class MemMedia {
		PMEM,
		DRAM
	};

	//! @brief Metadata about memory allocator
	struct MemAllocatorControlHeader {
		/// Order of allocated space
		MemAllocatorOrder allocate_order;
		/// Storage media supported by allocator
		MemMedia mem_type;
	};

	static constexpr const char * GLOBAL_DATA_MEM_DIR_PATH[] = ARCH_PMEM_DIR_NAME;

	//! @brief Concept about memory allocator
	//! @tparam MemAllocator
	template<class MemAllocator>
	concept MemAllocatorConcept = requires(
			MemAllocator allocator,
			size_t size,
			void *ptr) {

		// Statement of how is pointers aligned
		{ MemAllocator::ALLOC_ALIGN_SIZE };

		// Require control header getter
		{ MemAllocator::get_header() } -> std::same_as<MemAllocatorControlHeader>;

		// Require allocator interface
		{ allocator.allocate(size) } -> std::convertible_to<void *>;
		{ allocator.deallocate(ptr, size) } -> std::same_as<void>;
	};

	template<class MemAllocator>
	concept SequenceMemAllocatorConcept = requires(
			MemAllocator allocator) {

		{ allocator.get_space_range() } -> std::same_as<std::span<uint8_t>>;
	};
}
