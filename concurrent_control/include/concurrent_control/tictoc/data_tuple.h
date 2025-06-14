/*
 * @author: BL-GS 
 * @date:   2023/1/2
 */

#pragma once

#include <cstdint>
#include <cstring>
#include <atomic>

#include <thread/rwlock.h>

namespace cc::tictoc {

	struct alignas(32) DataTupleHeader {
	private:
		using Self = DataTupleHeader;

	public:
		std::atomic<uint64_t> rts_;

		std::atomic<uint64_t> wts_;

		thread::RWLock lock_;

	public:
		DataTupleHeader(): rts_(0), wts_(0), lock_() {}

		DataTupleHeader(uint64_t rts, uint64_t wts): rts_(rts), wts_(wts), lock_() {}

		std::atomic<uint64_t> &get_rts() { return rts_; }

		std::atomic<uint64_t> &get_wts() { return wts_; }

	public:
		bool try_lock_write() { return lock_.try_lock_write(); }

		void lock_write() { lock_.lock_write(); }

		void unlock_write() { lock_.unlock_write(); }

		bool is_locked_write() { return lock_.is_locked_write(); }

		bool try_lock_read() { return lock_.try_lock_read(); }

		void lock_read() { lock_.lock_read(); }

		void unlock_read() { lock_.unlock_read(); }

	};

	struct alignas(32) IndexTuple {
	private:
		using Self = IndexTuple;

		using DataTupleHeaderType = DataTupleHeader;

	public:
		uint32_t data_type_;

		uint32_t data_size_;

		DataTupleHeaderType *data_header_ptr_;

		void *data_ptr_;

	public:
		IndexTuple(): data_type_(-1), data_size_(0), data_header_ptr_(nullptr), data_ptr_(nullptr) {}

		IndexTuple(uint32_t data_type, uint32_t data_size, DataTupleHeaderType *data_header_ptr, void *data_ptr) :
				data_type_(data_type), data_size_(data_size), data_header_ptr_(data_header_ptr), data_ptr_(data_ptr) {}

		IndexTuple(const Self &other) = default;

		Self &operator=(const Self &other) = default;

	public:
		std::atomic<uint64_t> &get_rts() const { return data_header_ptr_->get_rts(); }

		std::atomic<uint64_t> &get_wts() const { return data_header_ptr_->get_wts(); }

		uint32_t get_data_type() const { return data_type_; }

		uint32_t get_data_size() const { return data_size_; }

		DataTupleHeaderType *get_data_header_ptr() const { return data_header_ptr_; }

	public:
		void lock_write() const { data_header_ptr_->lock_write(); }

		void unlock_write() const { data_header_ptr_->unlock_write(); }

		bool try_lock_write() const { return data_header_ptr_->try_lock_write(); }

		bool is_locked_write() const { return data_header_ptr_->is_locked_write(); }

		void lock_read() const { data_header_ptr_->lock_read(); }

		void unlock_read() const { data_header_ptr_->unlock_read(); }

		bool try_lock_read() const { return data_header_ptr_->try_lock_read(); }

	public:
		void *get_data_ptr() const { return data_ptr_; }

	public:
		void set_data(void *new_data_ptr, uint32_t size, uint32_t offset) const {
			std::memcpy(static_cast<uint32_t *>(data_ptr_) + offset,
						static_cast<uint32_t *>(new_data_ptr) + offset,
						size);
		}
	};
} // namespace cc::tictoc

