/*
 * @author: BL-GS 
 * @date:   2023/2/26
 */

#pragma once

#include <data_manager/abstract_data_manager.h>
#include <log_manager/abstract_log_manager.h>
#include <version_manager/abstract_version_manager.h>

#include <storage_manager/abstract_storage_manager.h>

namespace storage {

	template<class DataManager, class LogManager, class VersionManager>
	requires datam::DataManagerConcept<DataManager>
	         && logm::LogManagerConcept<LogManager>
	         && versionm::VersionManagerConcept<VersionManager>
	class SimpleMVStorageManagerTemplate {

	public:
		using Self          = SimpleMVStorageManagerTemplate<DataManager, LogManager, VersionManager>;

		/*
		 * Data manager parameters
		 */
		using DataKeyType         = DataManager::DataKeyType;
		using DataTupleHeaderType = DataManager::DataTupleHeaderType;
		using IndexTupleType      = DataManager::IndexTupleType;

		/*
		 * Version storage management ---------------------------
		 */
		using VersionHeaderType = VersionManager::VersionHeaderType;

		/*
		 * Align Assumed
		 */
		static constexpr size_t DATA_ALLOC_ALIGN_SIZE    = DataManager::DATA_ALLOC_ALIGN_SIZE;
		static constexpr size_t VHEADER_ALLOC_ALIGN_SIZE = DataManager::VHEADER_ALLOC_ALIGN_SIZE;
		static constexpr size_t LOG_ALLOC_ALIGN_SIZE     = LogManager::LOG_ALLOC_ALIGN_SIZE;

	private:

		LogManager  log_manager_;

		std::vector<DataManager *> data_manager_array_;

		std::vector<VersionManager *> version_manager_array_;

	public:
		SimpleMVStorageManagerTemplate() = default;

		~SimpleMVStorageManagerTemplate() {
			for (DataManager *data_manager_ptr: data_manager_array_) {
				delete data_manager_ptr;
			}
			for (VersionManager *version_manager_ptr: version_manager_array_) {
				delete version_manager_ptr;
			}
		}

	public:
		void add_table(size_t tuple_size, size_t expected_amount) {
			// For data tuple, assert that it should be without any variant data
			DataManager *data_manager_ptr = new DataManager{sizeof(DataTupleHeaderType), expected_amount};
			data_manager_array_.emplace_back(data_manager_ptr);
			// For version tuple, assert that all data are in that
			VersionManager *version_manager_ptr = new VersionManager{tuple_size, expected_amount};
			version_manager_array_.emplace_back(version_manager_ptr);
		}

		/*
		 * Data index interface
		 */

		bool add_data_index_tuple(uint32_t data_type_ino, DataKeyType data_key, const IndexTupleType &index_tuple) {
			return data_manager_array_[data_type_ino]->add_data_index_tuple(data_key, index_tuple);
		}

		bool delete_data_index_tuple(uint32_t data_type_ino, DataKeyType data_key) {
			return data_manager_array_[data_type_ino]->delete_data_index_tuple(data_key);
		}

		bool read_data_index_tuple(uint32_t data_type_ino, DataKeyType data_key, IndexTupleType &index_tuple) {
			return data_manager_array_[data_type_ino]->read_data_index_tuple(data_key, index_tuple);
		}

		/*
		 * Data interface
		 */

		std::pair<DataTupleHeaderType *, void *> allocate_data_and_header(uint32_t data_type_ino) {
			return data_manager_array_[data_type_ino]->allocate_data_and_header();
		}

		void *allocate_data(uint32_t data_type_ino) {
			return data_manager_array_[data_type_ino]->allocate_data();
		}

		DataTupleHeaderType *allocate_header(uint32_t data_type_ino) {
			return data_manager_array_[data_type_ino]->allocate_header();
		}

		void deallocate_data_and_header(uint32_t data_type_ino, DataTupleHeaderType *header_ptr, void *data_ptr) {
			data_manager_array_[data_type_ino]->deallocate_data_and_header(header_ptr, data_ptr);
		}

		void deallocate_data(uint32_t data_type_ino, void *data_ptr) {
			data_manager_array_[data_type_ino]->deallocate_data(data_ptr);
		}

		void deallocate_header(uint32_t data_type_ino,DataTupleHeaderType *header_ptr) {
			data_manager_array_[data_type_ino]->deallocate_header(header_ptr);
		}


		template<class Func>
		void register_data_deallocate_func(Func &&func, uint32_t data_type = std::numeric_limits<uint32_t>::max()) {
			if (data_type == std::numeric_limits<uint32_t>::max()) {
				for (DataManager *data_manager_ptr: data_manager_array_) {
					data_manager_ptr->register_deallocate_data_func(std::forward<Func>(func));
				}
			}
			else {
				data_manager_array_[data_type]->register_deallocate_data_func(std::forward<Func>(func));
			}
		}

		/*
		 * Log interface
		 */

		std::span<uint8_t> get_log_space_range() const {
			return log_manager_.get_space_range();
		}

		/*
		 * Version interface
		 */

		VersionHeaderType * allocate_version(uint32_t data_type_ino) {
			return version_manager_array_[data_type_ino]->allocate_version();
		}

		bool deallocate_version(uint32_t data_type_ino, VersionHeaderType *target) {
			return version_manager_array_[data_type_ino]->deallocate_version(target);
		}

		/*
		 * Other interface
		 */

		template<class T>
		void pwb_range(T *target, uint32_t size = sizeof(T)) {
			NVM::pwb_range(target, size);
		}

		void fence() {
			std::atomic_thread_fence(std::memory_order_acquire);
		}
	};

	template<class DataManager, class LogManager>
	requires datam::DataManagerConcept<DataManager>
	         && logm::LogManagerConcept<LogManager>
	class SimpleStorageManagerTemplate {

	public:
		using Self          = SimpleStorageManagerTemplate<DataManager, LogManager>;

		/*
		 * Data manager parameters
		 */
		using DataKeyType         = DataManager::DataKeyType;
		using DataTupleHeaderType = DataManager::DataTupleHeaderType;
		using IndexTupleType      = DataManager::IndexTupleType;

		/*
		 * Align Assumed
		 */
		static constexpr size_t DATA_ALLOC_ALIGN_SIZE    = DataManager::DATA_ALLOC_ALIGN_SIZE;
		static constexpr size_t VHEADER_ALLOC_ALIGN_SIZE = DataManager::VHEADER_ALLOC_ALIGN_SIZE;
		static constexpr size_t LOG_ALLOC_ALIGN_SIZE     = LogManager::LOG_ALLOC_ALIGN_SIZE;

	private:
		LogManager log_manager_;

		std::vector<size_t> table_size_array_;

		std::vector<DataManager *> data_manager_array_;

	public:
		SimpleStorageManagerTemplate() = default;

		~SimpleStorageManagerTemplate() {
			for (DataManager *data_manager_ptr: data_manager_array_) {
				delete data_manager_ptr;
			}
		}

	public:
		void add_table(size_t tuple_size, size_t expected_amount) {
			DataManager *data_manager_ptr = new DataManager{tuple_size, expected_amount};
			table_size_array_.emplace_back(tuple_size);
			data_manager_array_.emplace_back(data_manager_ptr);
		}

		auto get_table_size_info() const { return table_size_array_; }

		/*
		 * Data index interface
		 */

		bool add_data_index_tuple(uint32_t data_type_ino, DataKeyType data_key, const IndexTupleType &index_tuple) {
			return data_manager_array_[data_type_ino]->add_data_index_tuple(data_key, index_tuple);
		}

		bool delete_data_index_tuple(uint32_t data_type_ino, DataKeyType data_key) {
			return data_manager_array_[data_type_ino]->delete_data_index_tuple(data_key);
		}

		bool read_data_index_tuple(uint32_t data_type_ino, DataKeyType data_key, IndexTupleType &index_tuple) {
			return data_manager_array_[data_type_ino]->read_data_index_tuple(data_key, index_tuple);
		}

		/*
		 * Data interface
		 */

		std::pair<DataTupleHeaderType *, void *> allocate_data_and_header(uint32_t data_type_ino) {
			auto [header_ptr, data_ptr] = data_manager_array_[data_type_ino]->allocate_data_and_header();
			return {
				std::assume_aligned<VHEADER_ALLOC_ALIGN_SIZE>(header_ptr),
				std::assume_aligned<DATA_ALLOC_ALIGN_SIZE>(data_ptr)
			};
		}

		void *allocate_data(uint32_t data_type_ino) {
			return std::assume_aligned<DATA_ALLOC_ALIGN_SIZE>(data_manager_array_[data_type_ino]->allocate_data());
		}

		DataTupleHeaderType *allocate_header(uint32_t data_type_ino) {
			return std::assume_aligned<VHEADER_ALLOC_ALIGN_SIZE>(data_manager_array_[data_type_ino]->allocate_header());
		}

		void deallocate_data_and_header(uint32_t data_type_ino, void *data_ptr) {
			data_manager_array_[data_type_ino]->deallocate_data_and_header(data_ptr);
		}

		void deallocate_data(uint32_t data_type_ino, void *data_ptr) {
			data_manager_array_[data_type_ino]->deallocate_data(data_ptr);
		}

		void deallocate_header(uint32_t data_type_ino,DataTupleHeaderType *header_ptr) {
			data_manager_array_[data_type_ino]->deallocate_header(header_ptr);
		}

		template<class Func>
		void register_data_deallocate_func(Func &&func, uint32_t data_type = std::numeric_limits<uint32_t>::max()) {
			if (data_type == std::numeric_limits<uint32_t>::max()) {
				for (DataManager *data_manager_ptr: data_manager_array_) {
					data_manager_ptr->register_deallocate_data_func(std::forward<Func>(func));
				}
			}
			else {
				data_manager_array_[data_type]->register_deallocate_data_func(std::forward<Func>(func));
			}
		}

		/*
		 * Log interface
		 */

		std::span<uint8_t> get_log_space_range() const {
			return log_manager_.get_space_range();
		}

		/*
		 * Recovery
		 */
		void recovery_iteration(uint32_t data_type, std::function<bool(void *data_ptr)> &callback_func) {
			data_manager_array_[data_type]->recovery_iteration(callback_func);
		}

		/*
		 * Other interface
		 */

		template<class T>
		void pwb_range(T *target, uint32_t size = sizeof(T)) {
			NVM::pwb_range(target, size);
		}

		void fence() {
			NVM::fence();
		}
	};
}
