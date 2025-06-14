/*
 * @author: BL-GS 
 * @date:   2023/3/2
 */

#pragma once
#ifndef PTM_VERSION_MANAGER_SIMPLE_VERSION_MANAGER_SIMPLE_VERSION_MANAGER_H
#define PTM_VERSION_MANAGER_SIMPLE_VERSION_MANAGER_SIMPLE_VERSION_MANAGER_H

#include <version_manager/abstract_version_manager.h>
#include <mem_allocator/mem_allocator.h>

namespace versionm {

	template<class VersionHeader, class VersionAllocator>
	class SimpleVersionManagerTemplate {
	public:
		using VersionHeaderType = VersionHeader;

		using VersionAllocatorType = VersionAllocator;

		static constexpr uint32_t VERSION_STORAGE_EXPAND_FACTOR = 4;

	private:
		size_t alloc_size_;

		VersionAllocator version_allocator_;

	public:
		SimpleVersionManagerTemplate(size_t version_size, size_t expected_amount):
				alloc_size_(version_size + sizeof(VersionHeader)),
				version_allocator_(alloc_size_, expected_amount * VERSION_STORAGE_EXPAND_FACTOR) {}

		~SimpleVersionManagerTemplate() = default;

	public:
		VersionHeaderType *allocate_version() {
			VersionHeaderType *res = (VersionHeaderType *)version_allocator_.allocate(alloc_size_);
			return res;
		}

		bool deallocate_version(VersionHeaderType *target) {
			version_allocator_.deallocate(target, alloc_size_);
			return true;
		}

	};
}

#endif //PTM_VERSION_MANAGER_SIMPLE_VERSION_MANAGER_SIMPLE_VERSION_MANAGER_H
