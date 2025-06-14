/*
 * @author: BL-GS 
 * @date:   2023/2/26
 */

#pragma once

#include <version_manager/abstract_version_manager.h>

#include <version_manager/simple_version_manager/dram_version_manager.h>
#include <version_manager/simple_version_manager/pmem_version_manager.h>

namespace versionm {

	enum class VersionManagerKind {
		DRAM,
		PMEM,
	};

	template<VersionManagerKind Type, class VersionHeader>
	struct VersionManagerManager {
		using VersionManager = void;
	};

	template<class VersionHeader>
	struct VersionManagerManager<VersionManagerKind::DRAM, VersionHeader> {
		using VersionManager = DRAMVersionManager<VersionHeader>;

		static_assert(VersionManagerConcept<VersionManager>);
	};

	template<class VersionHeader>
	struct VersionManagerManager<VersionManagerKind::PMEM, VersionHeader> {
		using VersionManager = PMEMVersionManager<VersionHeader>;

		static_assert(VersionManagerConcept<VersionManager>);
	};

}
