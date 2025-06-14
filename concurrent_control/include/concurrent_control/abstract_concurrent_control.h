/*
 * @author: BL-GS 
 * @date:   2023/1/28
 */

#pragma once

#include <concepts>
#include <index/index.h>
#include <concurrent_control/config.h>

namespace cc {

	template<class AbKey>
	concept AbstractKeyConcept = requires(
			AbKey key,
			AbKey::TypeInoType type_info,
			AbKey::MainKeyType main_key
			) {

		typename AbKey::TypeInoType;

		typename AbKey::MainKeyType;

		requires std::is_copy_assignable_v<AbKey>;
		requires std::is_copy_assignable_v<typename AbKey::TypeInoType>;
		requires std::is_copy_assignable_v<typename AbKey::MainKeyType>;

		{ key.get_type_ino() } -> std::same_as<typename AbKey::TypeInoType>;

		{ key.get_main_key() } -> std::same_as<typename AbKey::MainKeyType>;

		{ key.set_type_ino(type_info) };

		{ key.set_main_key(main_key) };
	};

	template<class Executor>
	concept ExecutorConcept = requires(
			Executor executor,
			Executor::AbKeyType key,
			const void *src_ptr,
			void *dst_ptr,
			uint32_t offset,
			uint32_t size) {

		{ executor.template read<uint32_t>(key) } -> std::convertible_to<const void *>;

		{ executor.template update<uint32_t>(key) } -> std::convertible_to<void *>;

		{ executor.template update<uint32_t>(key, size, offset) } -> std::convertible_to<void *>;

		{ executor.read(key, dst_ptr, size, offset) } -> std::convertible_to<bool>;

		{ executor.update(key, src_ptr, size, offset) } -> std::convertible_to<bool>;

		{ executor.insert(key, src_ptr, size) } -> std::same_as<bool>;

		{ executor.commit() } -> std::same_as<bool>;

		{ executor.abort() } -> std::same_as<bool>;

		{ executor.reset() } -> std::same_as<bool>;

	};

	//! @brief Concept about concurrent control.
	//! @tparam CC
	template<class CC>
	concept CCConcept = requires(CC cc,
			CC::KeyType key,
			CC::Context context) {

		// ------ Flush all uncompleted works
		{ cc.flush_all_works() } -> std::same_as<void>;

		// ------ Running tasks functions needed to execute.
		{ cc.get_executor() };

		// ------ Information getter function needed to get relative message.

		//! @brief Get relative information about execution in this concurrent control.
		{ cc.get_concurrent_control_message() } -> std::same_as<ConcurrentControlMessage &>;
	};

	//! @brief Concept about basic storage manager
	//! @tparam ManagerClass
	template<class ManagerClass>
	concept StorageManagerConcept = requires(ManagerClass manager,
			ManagerClass::DataKeyType data_key,
			ManagerClass::IndexTupleType log_tuple,
			ManagerClass::DataTupleHeaderType *data_tuple_header_ptr,
			void *data_ptr,
			uint32_t data_type_ino,
			size_t offset, size_t size
	) {

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

		// Require register for deallocate func
		{ manager.register_data_deallocate_func(std::function<void(void *)>(), data_type_ino)  } -> std::same_as<void>;

		{ manager.pwb_range(data_ptr, size) } -> std::same_as<void>;

		{ manager.fence() } -> std::same_as<void>;
	};

	//! @brief Concept about storage manager supporting multiple versions.
	//! @tparam MVManagerClass
	template<class MVManagerClass>
	concept MVStorageManagerConcept = requires(MVManagerClass manager,
		MVManagerClass::VersionHeaderType *dst_ptr,
		size_t data_type_ino
	) {
		requires StorageManagerConcept<MVManagerClass>;

			// Require allocate interface
			{ manager.allocate_version(data_type_ino) } -> std::convertible_to<void *>;

			// Require deallocate interface
			{ manager.deallocate_version(data_type_ino, dst_ptr) } -> std::same_as<bool>;
	};

}
