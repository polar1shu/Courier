/*
 * @author: BL-GS 
 * @date:   2023/2/17
 */

#pragma once

#include <cstdint>
#include <utility>
#include <concepts>

#include <index/index.h>
#include <mem_allocator/abstract_mem_allocator.h>

namespace datam {

	using IndexType              = ix::IndexType;

	using StorageMemType        = allocator::MemMedia;

	using StorageOrder          = allocator::MemAllocatorOrder;

	using StorageControlHeader  = allocator::MemAllocatorControlHeader;




	//! @brief Concept about concurrent control.
	//! @tparam DataManager
	template<class DataManager>
	concept DataManagerConcept = requires(
			DataManager manager,
			DataManager::DataKeyType data_key,
			DataManager::IndexTupleType &index_tuple,
			DataManager::DataTupleHeaderType *data_tuple_header_ptr,
			void *data_ptr,
			std::function<void(const typename DataManager::IndexTupleType &)> deallocate_func) {

		/*
		 * Align Assumed
		 */
		{ DataManager::VHEADER_ALLOC_ALIGN_SIZE };
		{ DataManager::DATA_ALLOC_ALIGN_SIZE };

		// Require static description about storage order
		std::is_function_v<decltype(&DataManager::get_data_storage_order)>;
		{ DataManager::get_data_storage_order() } -> std::same_as<StorageOrder>;
		// Require static description about storage memory type
		std::is_function_v<decltype(&DataManager::get_data_storage_memtype)>;
		{ DataManager::get_data_storage_memtype() } -> std::same_as<StorageMemType>;
		// Require static description about storage allocator control header
		std::is_function_v<decltype(&DataManager::get_data_storage_control_header)>;
		{ DataManager::get_data_storage_control_header() } -> std::same_as<StorageControlHeader>;

		/*
		 * Function for data interface
		 */

		// Require allocate interface
		{ manager.add_data_index_tuple(data_key, index_tuple) } -> std::same_as<bool>;
		// Require deallocate interface
		{ manager.delete_data_index_tuple(data_key) } -> std::same_as<bool>;
		// Require read interface
		{ manager.read_data_index_tuple(data_key, index_tuple) } -> std::same_as<bool>;

		// Require allocate interface
		{ manager.allocate_data_and_header() } -> std::same_as<std::pair<decltype(data_tuple_header_ptr), decltype(data_ptr)>>;
		{ manager.allocate_data() } -> std::same_as<decltype(data_ptr)>;
		{ manager.allocate_header() } -> std::same_as<decltype(data_tuple_header_ptr)>;
		// Require deallocate interface
		{ manager.deallocate_data_and_header(data_ptr) } -> std::same_as<bool>;
		{ manager.deallocate_data(data_ptr) } -> std::same_as<bool>;
		{ manager.deallocate_header(data_tuple_header_ptr) } -> std::same_as<bool>;

		{ manager.register_deallocate_data_func(deallocate_func) } -> std::same_as<void>;
	};
}
