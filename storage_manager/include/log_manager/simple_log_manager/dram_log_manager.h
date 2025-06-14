/*
 * @author: BL-GS 
 * @date:   2023/2/20
 */

#pragma once

#include <thread/thread.h>
#include <mem_allocator/mem_allocator.h>

#include <log_manager/abstract_log_manager.h>
#include <log_manager/simple_log_manager/simple_log_manager.h>

namespace logm {

	class DRAMTLLogManager: public SimpleLogManagerTemplate<
			allocator::RindRoundDRAMAllocator
		> {

	public:
		static constexpr StorageOrder LOG_STORAGE_ORDER = StorageOrder::Random;
		static constexpr StorageOrder get_log_storage_order() { return LOG_STORAGE_ORDER; };

		static constexpr StorageMemType LOG_STORAGE_MEM_TYPE = StorageMemType::DRAM;
		static constexpr StorageMemType get_log_storage_memtype() { return LOG_STORAGE_MEM_TYPE; };

		static constexpr auto LOG_MEM_DIR_PATH = GLOBAL_LOG_MEM_DIR_PATH;

		using LogAllocator = allocator::RindRoundDRAMAllocator;

		static constexpr StorageControlHeader get_log_storage_control_header() { return LogAllocator::get_header(); }

	public:
		using Self         = DRAMTLLogManager;
		using Base         = SimpleLogManagerTemplate<LogAllocator>;

		static constexpr size_t LOG_ALLOC_ALIGN_SIZE = LogAllocator::ALLOC_ALIGN_SIZE;
	};
}
