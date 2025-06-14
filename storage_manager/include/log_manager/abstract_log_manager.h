/*
 * @author: BL-GS 
 * @date:   2023/2/17
 */

#pragma once

#include <cstdint>
#include <utility>
#include <concepts>
#include <span>

namespace logm {

	using StorageMemType        = allocator::MemMedia;

	using StorageOrder          = allocator::MemAllocatorOrder;

	using StorageControlHeader  = allocator::MemAllocatorControlHeader;

	static constexpr const char * GLOBAL_LOG_MEM_DIR_PATH[] = {
			"/mnt/pmem0/log_dir/",
			"/mnt/pmem1/log_dir/",
			"/mnt/pmem2/log_dir/",
			"/mnt/pmem3/log_dir/"
	};


	//! @brief Concept about concurrent control.
	//! @tparam LogManager
	template<class LogManager>
	concept LogManagerConcept = requires(LogManager log_manager,
			void *src, size_t src_length) {

		{ LogManager::LOG_ALLOC_ALIGN_SIZE };

		// Require static description about storage order
		std::is_function_v<decltype(&LogManager::get_log_storage_order)>;
		{ LogManager::get_log_storage_order() } -> std::same_as<StorageOrder>;
		// Require static description about storage memory type
		std::is_function_v<decltype(&LogManager::get_log_storage_memtype)>;
		{ LogManager::get_log_storage_memtype() } -> std::same_as<StorageMemType>;
		// Require static description about storage allocator control header
		std::is_function_v<decltype(&LogManager::get_log_storage_control_header)>;
		{ LogManager::get_log_storage_control_header() } -> std::same_as<StorageControlHeader>;

		/*
		 * Function for log append and flush
		 */

		{ log_manager.get_space_range() } -> std::same_as<std::span<uint8_t>>;
	};
}
