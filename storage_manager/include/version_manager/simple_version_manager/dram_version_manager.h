/*
 * @author: BL-GS 
 * @date:   2023/2/26
 */

#pragma once
#ifndef PTM_VERSION_MANAGER_SIMPLE_VERSION_MANAGER_DRAM_VERSION_MANAGER_H
#define PTM_VERSION_MANAGER_SIMPLE_VERSION_MANAGER_DRAM_VERSION_MANAGER_H

#include <mem_allocator/mem_allocator.h>

#include <version_manager/abstract_version_manager.h>
#include <version_manager/simple_version_manager/simple_version_manager.h>

namespace versionm {

	template<class VersionHeader>
	struct DRAMVersionManagerBasic {
		using VersionHeaderType = VersionHeader;

		using VersionAllocator = allocator::DRAMAllocator;
	};

	template<class VersionHeader>
	class DRAMVersionManager : public SimpleVersionManagerTemplate<
			typename DRAMVersionManagerBasic<VersionHeader>::VersionHeaderType,
			typename DRAMVersionManagerBasic<VersionHeader>::VersionAllocator
			> {
	public:
		using BaseType = SimpleVersionManagerTemplate<
				typename DRAMVersionManagerBasic<VersionHeader>::VersionHeaderType,
				typename DRAMVersionManagerBasic<VersionHeader>::VersionAllocator
		>;

		static constexpr StorageOrder VERSION_STORAGE_ORDER = StorageOrder::Random;

		static constexpr StorageOrder get_version_storage_order() { return VERSION_STORAGE_ORDER; };

		static constexpr StorageMemType VERSION_STORAGE_MEM_TYPE = StorageMemType::DRAM;

		static constexpr StorageMemType get_version_storage_memtype() { return VERSION_STORAGE_MEM_TYPE; };

		using VersionAllocator = allocator::DRAMAllocator;
		static_assert(allocator::MemAllocatorConcept<VersionAllocator>);

		static constexpr StorageControlHeader get_version_storage_control_header() { return VersionAllocator::get_header(); }

	public:
		DRAMVersionManager(size_t tuple_size, size_t expected_amount): BaseType(tuple_size, expected_amount) {}
	};
}


#endif //PTM_VERSION_MANAGER_SIMPLE_VERSION_MANAGER_DRAM_VERSION_MANAGER_H
