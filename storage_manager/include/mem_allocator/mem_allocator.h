/*
 * @author: BL-GS 
 * @date:   2023/1/2
 */

#pragma once

#include <mem_allocator/abstract_mem_allocator.h>

#include <mem_allocator/dram_allocator/dram_allocator.h>

#include <mem_allocator/pool_allocator/dram_allocator.h>
#include <mem_allocator/pool_allocator/pmem_allocator.h>

#include <mem_allocator/ring_round_allocator/pmem_allocator.h>
#include <mem_allocator/ring_round_allocator/dram_allocator.h>

namespace allocator {

	enum class MemAllocatorType {
		DramAllocator,
		DramPoolAllocator,
		SimplePmemAllocator,
		RingRoundPMEMAllocator,
		RingRoundDRAMAllocator
	};

	template<MemAllocatorType Type>
	struct MemAllocatorManager {
		using AllocatorType = void;
	};

	template<>
	struct MemAllocatorManager<MemAllocatorType::DramAllocator> {
		using AllocatorType = DRAMAllocator;

		static_assert(MemAllocatorConcept<AllocatorType>);
	};

	template<>
	struct MemAllocatorManager<MemAllocatorType::DramPoolAllocator> {
		using AllocatorType = DRAMPoolAllocator;

		static_assert(MemAllocatorConcept<AllocatorType>);
	};

	template<>
	struct MemAllocatorManager<MemAllocatorType::SimplePmemAllocator> {
		using AllocatorType = SimplePmemAllocator;

		static_assert(MemAllocatorConcept<AllocatorType>);
	};

	template<>
	struct MemAllocatorManager<MemAllocatorType::RingRoundPMEMAllocator> {
		using AllocatorType = RindRoundPMEMAllocator;

		static_assert(MemAllocatorConcept<AllocatorType>);
	};

	template<>
	struct MemAllocatorManager<MemAllocatorType::RingRoundDRAMAllocator> {
		using AllocatorType = RindRoundDRAMAllocator;

		static_assert(MemAllocatorConcept<AllocatorType>);
	};

}
