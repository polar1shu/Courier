/*
 * @author: BL-GS 
 * @date:   2023/1/24
 */

#pragma once

#include <concepts>

#include <index/abstract_index.h>
#include <mem_allocator/abstract_mem_allocator.h>

namespace storage {

	using IndexType             = ix::IndexType;

	using StorageMemType        = allocator::MemMedia;

	using StorageOrder          = allocator::MemAllocatorOrder;

	using StorageControlHeader  = allocator::MemAllocatorControlHeader;

	//! @brief Concept about basic storage manager
	//! @tparam ManagerClass
	template<class ManagerClass>
	concept StorageManagerConcept = requires(ManagerClass manager,
			ManagerClass::DataKeyType data_key,
			ManagerClass::IndexTupleType log_tuple,
			ManagerClass::DataTupleHeaderType *data_tuple_header_ptr,
			void *data_ptr,
			uint32_t data_type_ino,
			const void *src_ptr, void *dst_ptr,
			size_t offset, size_t size
			) {

		/*
		 * Align Assumed
		 */
		{ ManagerClass::DATA_ALLOC_ALIGN_SIZE };
		{ ManagerClass::VHEADER_ALLOC_ALIGN_SIZE };
		{ ManagerClass::LOG_ALLOC_ALIGN_SIZE };

		/*
		 * Data Index interface
		 */

		// Add data index tuple.
		{ manager.add_data_index_tuple(data_type_ino, data_key, log_tuple) } -> std::same_as<bool>;

		// Delete data index tuple.
		{ manager.delete_data_index_tuple(data_type_ino, data_key) } -> std::same_as<bool>;

		// Require read interface
		{ manager.read_data_index_tuple(data_type_ino, data_key, log_tuple) } -> std::same_as<bool>;

		/*
		 * Data interface.
		 * Dividing data allocation and index is intended to leaf option of flushing to upper level.
		 */

		// Allocate space for data
		{ manager.allocate_data_and_header(data_type_ino) } -> std::same_as<std::pair<decltype(data_tuple_header_ptr), decltype(data_ptr)>>;

		{ manager.allocate_data(data_type_ino) } -> std::same_as<decltype(data_ptr)>;

		{ manager.allocate_header(data_type_ino) } -> std::same_as<decltype(data_tuple_header_ptr)>;


		// Require deallocate interface
		{ manager.deallocate_data_and_header(data_type_ino, data_ptr) } -> std::same_as<void>;

		{ manager.deallocate_data(data_type_ino, data_ptr) } -> std::same_as<void>;

		{ manager.deallocate_header(data_type_ino, data_tuple_header_ptr) } -> std::same_as<void>;

		/*
		 * Log interface
		 */

		{ manager.get_log_space_range() } -> std::same_as<std::span<uint8_t>>;

		// Require register for deallocate func
		{ manager.register_data_deallocate_func(std::function<void(void *)>(), data_type_ino)  } -> std::same_as<void>;

		{ manager.pwb_range(dst_ptr, size) } -> std::same_as<void>;

		{ manager.fence() } -> std::same_as<void>;

	};

	//! @brief Concept about storage manager supporting multiple versions.
	//! @tparam MVManagerClass
	template<class MVManagerClass>
	concept MVStorageManagerConcept = requires(MVManagerClass manager,
			void *dst_ptr,
			size_t data_type_ino
			) {
		requires StorageManagerConcept<MVManagerClass>;

		// Require allocate interface
		{ manager.allocate_version(data_type_ino) } -> std::convertible_to<void *>;

		// Require deallocate interface
		{ manager.deallocate_version(data_type_ino, dst_ptr) } -> std::same_as<bool>;
	};

	//! @brief Concept about data tuple
	//! @tparam DataTuple
	template<class DataTuple>
	concept DataTupleConcept = requires {
		std::is_default_constructible_v<DataTuple>;
		std::is_copy_constructible_v<DataTuple>;
		std::is_copy_assignable_v<DataTuple>;
	};

	//! @brief Concept about data's key
	//! @tparam KeyType
	template<class KeyType>
	concept DataKeyTypeConcept = requires {
		std::is_default_constructible_v<KeyType>;
		std::is_copy_constructible_v<KeyType>;
		std::is_copy_assignable_v<KeyType>;
	};

	//! @brief Concept about log item
	//! @tparam LogItem
	template<class LogItem>
	concept LogItemConcept = requires {
		std::is_default_constructible_v<LogItem>;
		std::is_copy_constructible_v<LogItem>;
		std::is_copy_assignable_v<LogItem>;
	};

	//! @brief Concept about version tuple
	//! @tparam LogTuple
	template<class VersionTuple>
	concept VersionTupleConcept = requires {
		std::is_default_constructible_v<VersionTuple>;
		std::is_copy_constructible_v<VersionTuple>;
		std::is_copy_assignable_v<VersionTuple>;
	};


	template<class IndexType>
	concept IndexConcept = requires(
			IndexType index,
			IndexType::KeyType key,
			IndexType::ValueType value,
		std::function<void(const typename IndexType::ValueType &)> clear_func) {

		{ index.insert(key, value) } -> std::same_as<bool>;
		{ index.remove(key) } -> std::same_as<bool>;
		{ index.read(key, value) } -> std::same_as<bool>;
		{ index.update(key) } -> std::same_as<bool>;
		{ index.read_and_modify(key, [](){}) } -> std::same_as<bool>;
		{ index.contain(key) } -> std::same_as<bool>;
		{ index.clear(clear_func) } -> std::same_as<void>;
		{ index.size() } -> std::convertible_to<uint32_t>;
	};
}
