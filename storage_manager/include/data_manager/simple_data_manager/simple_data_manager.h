/*
 * @author: BL-GS 
 * @date:   2023/3/1
 */

#pragma once

#include <data_manager/abstract_data_manager.h>
#include <mem_allocator/mem_allocator.h>

namespace datam {

	template<class DataKey, class DataTupleHeader, class IndexTuple, class DataIndex, class DataAllocator>
	class SimpleDataManagerTemplate {
	public:
		using DataKeyType         = DataKey;

		using DataTupleHeaderType = DataTupleHeader;

		using IndexTupleType      = IndexTuple;

		static constexpr size_t VHEADER_ALLOC_ALIGN_SIZE = DataAllocator::ALLOC_ALIGN_SIZE;

		static constexpr size_t DATA_ALLOC_ALIGN_SIZE    = std::min(VHEADER_ALLOC_ALIGN_SIZE, 16UL);

	private:
		size_t data_size_;

		DataIndex data_index_;

		DataAllocator data_allocator_;

		std::function<void(const IndexTupleType &)> data_deallocate_func = [](const IndexTupleType &){};


	public:
		SimpleDataManagerTemplate(size_t tuple_size, size_t expected_amount):
			data_size_(tuple_size +  align_ceil(sizeof(DataTupleHeaderType), DATA_ALLOC_ALIGN_SIZE)),
			data_index_(sizeof(IndexTupleType), expected_amount),
			data_allocator_(data_size_, expected_amount) {}

		~SimpleDataManagerTemplate() {
			data_index_.clear(data_deallocate_func);
		}

		void recovery_iteration(std::function<bool(void *data_ptr)> &callback_func) {
			data_allocator_.recovery_iteration(callback_func);
		}

	public:
		bool add_data_index_tuple(DataKeyType data_key, const IndexTupleType &src_index_tuple) {
			return data_index_.insert(data_key, src_index_tuple);
		}

		bool delete_data_index_tuple(DataKeyType data_key) {
			return data_index_.remove(data_key);
		}

		bool read_data_index_tuple(DataKeyType data_key, IndexTupleType &dst_index_tuple) {
			return data_index_.read(data_key, dst_index_tuple);
		}

		std::pair<DataTupleHeaderType *, void *> allocate_data_and_header() {
			auto *res = static_cast<DataTupleHeaderType *>(data_allocator_.allocate(data_size_));
			return {res, res + 1};
		}

		bool deallocate_data_and_header(void *data_ptr) {
			data_allocator_.deallocate(static_cast<DataTupleHeaderType *>(data_ptr) - 1, data_size_);
			return true;
		}

		void *allocate_data() {
			spdlog::error("This Data Manager DO NOT support (de)allocate data tuple and header separately.");
			return nullptr;
		}

		bool deallocate_data(void *data_ptr) {
			spdlog::error("This Data Manager DO NOT support (de)allocate data tuple and header separately.");
			return false;
		}

		DataTupleHeaderType *allocate_header() {
			spdlog::error("This Data Manager DO NOT support (de)allocate data tuple and header separately.");
			return nullptr;
		}

		bool deallocate_header(void *header_ptr) {
			spdlog::error("This Data Manager DO NOT support (de)allocate data tuple and header separately.");
			return false;
		}

		void register_deallocate_data_func(const std::function<void(const IndexTupleType &)> &deallocate_func) {
			data_deallocate_func = deallocate_func;
		}
	};
}

