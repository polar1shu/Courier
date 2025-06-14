/*
 * @author: BL-GS 
 * @date:   2023/1/2
 */

#pragma once

#include <cstdint>
#include <cstring>
#include <atomic>

#include <thread/rwlock.h>

namespace cc::courier_save {

	struct DataTupleVirtualHeader {
	private:
		using Self = DataTupleVirtualHeader;

	public:
		/// Lock
		thread::RWLock lock_;
		/// Write timestamp
		uint64_t wts_;
		/// The area of virtual data, which may be pointed to origin data
		std::atomic<void *> virtual_data_ptr_;
		/// The area of origin data
		std::span<uint8_t> data_ptr_;
		///
		uint32_t data_type_;

	public:
		DataTupleVirtualHeader():
				wts_(0),
				virtual_data_ptr_(nullptr) {}

		DataTupleVirtualHeader(uint64_t wts, void *virtual_data_ptr, void *data_ptr, size_t data_size, uint32_t data_type):
				wts_(wts),
				virtual_data_ptr_(virtual_data_ptr),
				data_ptr_(static_cast<uint8_t *>(data_ptr), data_size),
				data_type_(data_type) {}

		uint64_t get_wts() const { return wts_; }

		void set_wts(uint64_t new_wts) { wts_ = new_wts; }

		void *get_virtual_data_ptr() const { return virtual_data_ptr_.load(std::memory_order::acquire); }

		void *get_origin_data_ptr() const { return data_ptr_.data(); }

		size_t get_data_size() const { return data_ptr_.size(); }

		uint32_t get_data_type() const { return data_type_; }

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
			std::memcpy(static_cast<uint8_t *>(virtual_data_ptr_.load(std::memory_order::acquire)) + offset,
			            static_cast<uint8_t *>(new_data_ptr) + offset,
			            size);
		}

		void reset_data_ptr() {
			virtual_data_ptr_.store(get_origin_data_ptr(), std::memory_order::release);
		}
	};

	/*!
	 * @brief The header of data (in PM), indicating the status of data tuple.
	 * @tparam AbKey The type of abstract key
	 */
	template<class AbKey>
	struct DataTupleHeader {
		/// The key value
		AbKey key_;
		/// Indicating whether this tuple is valid
		bool valid_;

	public:
		DataTupleHeader(): key_(), valid_(false) {}
		explicit DataTupleHeader(AbKey key): key_(key), valid_(true) {}
	};

	/*!
	 * @brief The tuple located in index structure to look up the virtual header and some info about data
	 */
	struct IndexTuple {
	private:
		using Self = IndexTuple;

		using DataTupleVirtualHeaderType = DataTupleVirtualHeader;

	public:
		/// The type of data table
		uint32_t data_type_;
		/// The size of data
		uint32_t data_size_;
		/// The pointer to the virtual header
		DataTupleVirtualHeaderType *data_header_ptr_;

	public:
		IndexTuple(): data_type_(-1), data_size_(0), data_header_ptr_(nullptr) {}

		IndexTuple(uint32_t data_type, uint32_t data_size, DataTupleVirtualHeaderType *data_header_ptr, void *data_ptr) :
				data_type_(data_type), data_size_(data_size), data_header_ptr_(data_header_ptr) {}

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

		void *get_origin_data_ptr() const { return data_header_ptr_->get_origin_data_ptr(); }

		void set_virtual_data(void *new_data_ptr, uint32_t size, uint32_t offset) const {
			data_header_ptr_->set_virtual_data(new_data_ptr, size, offset);
		}

		void set_origin_data(void *new_data_ptr, uint32_t size, uint32_t offset) const {
			std::memcpy(static_cast<uint8_t *>(data_header_ptr_->get_origin_data_ptr()) + offset,
			            static_cast<uint8_t *>(new_data_ptr) + offset,
			            size);
		}
	};

}
