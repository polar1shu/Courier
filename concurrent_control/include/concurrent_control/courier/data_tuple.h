/*
 * @author: BL-GS 
 * @date:   2023/1/2
 */

#pragma once

#include <cstdint>
#include <cstring>
#include <atomic>

#include <thread/rwlock.h>

namespace cc::courier {

	struct DataTupleVirtualHeader {
	private:
		using Self = DataTupleVirtualHeader;

	private:
		thread::RWLock lock_;

		uint64_t wts_;

		void *virtual_data_ptr_;

	public:
		DataTupleVirtualHeader():
				wts_(0),
				virtual_data_ptr_(nullptr) {}

		DataTupleVirtualHeader(uint64_t wts, void *virtual_data_ptr):
				wts_(wts),
				virtual_data_ptr_(virtual_data_ptr){}

		uint64_t get_wts() const { return wts_; }

		void set_wts(uint64_t new_wts) { wts_ = new_wts; }

		void *get_virtual_data_ptr() const { return virtual_data_ptr_; }

	public:
		bool try_lock_write() { return lock_.try_lock_write(); }

		void lock_write() { lock_.lock_write(); }

		void unlock_write() { lock_.unlock_write(); }

		bool is_locked_write() { return lock_.is_locked_write(); }

		bool try_lock_read() { return lock_.try_lock_read(); }

		void lock_read() { lock_.lock_read(); }

		void unlock_read() { lock_.unlock_read(); }

	public:
		void set_virtual_data(void *new_data_ptr, uint32_t size, uint32_t offset) const {
			std::memcpy(static_cast<uint8_t *>(virtual_data_ptr_) + offset,
						static_cast<uint8_t *>(new_data_ptr) + offset,
						size);
		}
	};

	template<class AbKey>
	struct DataTupleHeader {
		AbKey key_;
		bool valid_;

	public:
		DataTupleHeader(): key_(), valid_(false) {}
		explicit DataTupleHeader(AbKey key): key_(key), valid_(true) {}
	};

	struct IndexTuple {
	private:
		using Self = IndexTuple;

		using DataTupleVirtualHeaderType = DataTupleVirtualHeader;

	public:
		uint32_t data_type_;

		uint32_t data_size_;

		DataTupleVirtualHeaderType *data_header_ptr_;

		void *data_ptr_;

	public:
		IndexTuple(): data_type_(-1), data_size_(0), data_header_ptr_(nullptr), data_ptr_(nullptr) {}

		IndexTuple(uint32_t data_type, uint32_t data_size, DataTupleVirtualHeaderType *data_header_ptr, void *data_ptr) :
				data_type_(data_type), data_size_(data_size), data_header_ptr_(data_header_ptr), data_ptr_(data_ptr) {}

		IndexTuple(const Self &other) = default;

		Self &operator=(const Self &other) = default;

	public:

		uint64_t get_wts() const { return data_header_ptr_->get_wts(); }

		uint32_t get_data_type() const { return data_type_; }

		uint32_t get_data_size() const { return data_size_; }

		DataTupleVirtualHeaderType *get_data_header_ptr() const { return data_header_ptr_; }

		void set_wts(uint64_t new_wts) { data_header_ptr_->set_wts(new_wts); }

	public:
		void lock_write() const { data_header_ptr_->lock_write(); }

		void unlock_write() const { data_header_ptr_->unlock_write(); }

		bool try_lock_write() const { return data_header_ptr_->try_lock_write(); }

		bool is_locked_write() const { return data_header_ptr_->is_locked_write(); }

		void lock_read() const { data_header_ptr_->lock_read(); }

		void unlock_read() const { data_header_ptr_->unlock_read(); }

		bool try_lock_read() const { return data_header_ptr_->try_lock_read(); }

	public:
		void *get_virtual_data_ptr() const { return data_header_ptr_->get_virtual_data_ptr(); }

		void *get_origin_data_ptr() const { return data_ptr_; }

		void set_virtual_data(void *new_data_ptr, uint32_t size, uint32_t offset) const {
			data_header_ptr_->set_virtual_data(new_data_ptr, size, offset);
		}

		void set_origin_data(void *new_data_ptr, uint32_t size, uint32_t offset) const {
			std::memcpy(static_cast<uint8_t *>(data_ptr_) + offset,
						static_cast<uint8_t *>(new_data_ptr) + offset,
						size);
		}
	};

}
