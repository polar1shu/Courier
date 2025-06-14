/*
 * @author: BL-GS 
 * @date:   2023/2/17
 */

#pragma once

#include <thread/thread.h>
#include <storage_manager/abstract_storage_manager.h>
#include <mem_allocator/mem_allocator.h>

#include <log_manager/abstract_log_manager.h>
#include <log_manager/simple_log_manager/simple_log_manager.h>

namespace logm {

	class PMEMLogManager : public SimpleLogManagerTemplate<
			allocator::RindRoundPMEMAllocator
	> {

	public:
		static constexpr StorageOrder LOG_STORAGE_ORDER = StorageOrder::ThreadSequential;
		static constexpr StorageOrder get_log_storage_order() { return LOG_STORAGE_ORDER; };

		static constexpr StorageMemType LOG_STORAGE_MEM_TYPE = StorageMemType::PMEM;
		static constexpr StorageMemType get_log_storage_memtype() { return LOG_STORAGE_MEM_TYPE; };

		static constexpr auto LOG_MEM_DIR_PATH = GLOBAL_LOG_MEM_DIR_PATH;

		using LogAllocator = allocator::RindRoundPMEMAllocator;

		static constexpr StorageControlHeader get_log_storage_control_header() { return LogAllocator::get_header(); }

	public:
		using Self         = PMEMLogManager;
		using Base         = SimpleLogManagerTemplate<LogAllocator>;

		static constexpr size_t LOG_ALLOC_ALIGN_SIZE = LogAllocator::ALLOC_ALIGN_SIZE;
	};
}
