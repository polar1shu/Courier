/*
 * @author: BL-GS 
 * @date:   2023/2/17
 */

#pragma once

#include <cstdint>
#include <utility>
#include <concepts>

#include <mem_allocator/abstract_mem_allocator.h>

namespace versionm {

	using StorageMemType        = allocator::MemMedia;

	using StorageOrder          = allocator::MemAllocatorOrder;

	using StorageControlHeader  = allocator::MemAllocatorControlHeader;

	//! @brief Concept about concurrent control.
	//! @tparam VersionManager
	template<class VersionManager>
	concept VersionManagerConcept = requires(
			VersionManager manager,
			VersionManager::VersionHeaderType *version_tuple_ptr) {

		/*
		 * Function for version interface
		 */

		// Require allocate interface
		{ manager.allocate_version() } -> std::convertible_to<void *>;
		// Require deallocate interface
		{ manager.deallocate_version(version_tuple_ptr) } -> std::same_as<bool>;
	};
}
