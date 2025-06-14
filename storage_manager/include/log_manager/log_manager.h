/*
 * @author: BL-GS 
 * @date:   2023/2/17
 */

#pragma once

#include <log_manager/simple_log_manager/pmem_log_manager.h>
#include <log_manager/simple_log_manager/dram_log_manager.h>

namespace logm {

	enum class LogManagerKind {
		PMEM_TL,
		DRAM_TL
	};

	template<LogManagerKind Type>
	struct LogManagerManager {
		using LogManager = void;
	};

	template<>
	struct LogManagerManager<LogManagerKind::PMEM_TL> {
		using LogManager = PMEMLogManager;

		static_assert(LogManagerConcept<LogManager>);
	};

	template<>
	struct LogManagerManager<LogManagerKind::DRAM_TL> {
		using LogManager = DRAMTLLogManager;

		static_assert(LogManagerConcept<LogManager>);
	};

}
