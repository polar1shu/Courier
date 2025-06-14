/*
 * @author: BL-GS 
 * @date:   2023/1/2
 */

#pragma once

#include <cstdint>
#include <cstring>

#include <thread/rwlock.h>

namespace cc::sp {

	inline constexpr uint32_t NO_LOCK_THREAD = 1001;

	struct alignas(32) DataTupleVirtualHeader {
	private:
		using Self = DataTupleVirtualHeader;

	public:
		std::atomic<uint64_t> wts_;

		thread::RWLock lock_;

		std::atomic<void *> latest_data_ptr_;

		uint32_t data_size_;

		uint32_t data_type_;

		uint32_t thid_;

		uint32_t last_tid_;

		uint32_t seq_num_;

		void *data_ptr_;

	public:
		DataTupleVirtualHeader():
				wts_(0),
				latest_data_ptr_(nullptr),
				data_size_(0),
				thid_(NO_LOCK_THREAD),
				last_tid_(0),
				seq_num_(0),
				data_ptr_(nullptr) {}

		DataTupleVirtualHeader(uint64_t wts, void *data_ptr, uint32_t data_size, uint32_t data_type):
				wts_(wts),
				latest_data_ptr_(data_ptr),
				data_size_(data_size),
				data_type_(data_type),
				thid_(NO_LOCK_THREAD),
				last_tid_(0),
				seq_num_(0),
				data_ptr_(data_ptr) {}

		uint32_t get_data_type() const { return data_type_; }

		std::atomic<uint64_t> &get_wts_ref() { return wts_; }

	public:
		void lock_write() {
			uint32_t tid = thread::get_tid();
			if (thid_ != tid) {
				lock_.lock_write();
				thid_ = tid;
			}
		}

		void unlock_write() {
			if (thid_ == NO_LOCK_THREAD) { return; }
			thid_ = NO_LOCK_THREAD;
			lock_.unlock_write();
		}

		bool is_locked_write() { return lock_.is_locked_write(); }

	public:
		void *get_data_ptr() const { return data_ptr_; }

		void *get_latest_data_ptr() const { return latest_data_ptr_; }

		uint32_t get_data_size() const { return data_size_; }
	};

	template<class AbKey>
	struct DataTupleHeader {
		AbKey key_;
		bool valid_;

	public:
		DataTupleHeader(): key_(), valid_(false) {}
		explicit DataTupleHeader(AbKey key): key_(key), valid_(true) {}
	};

	struct alignas(32) IndexTuple {
	private:
		using Self = IndexTuple;

		using DataTupleVirtualHeaderType = DataTupleVirtualHeader;

	public:
		uint32_t data_type_;

		DataTupleVirtualHeaderType *data_header_ptr_;

	public:
		IndexTuple(): data_type_(-1), data_header_ptr_(nullptr) {}

		IndexTuple(uint32_t data_type, DataTupleVirtualHeaderType *data_header_ptr) :
				data_type_(data_type), data_header_ptr_(data_header_ptr) {}

		IndexTuple(const Self &other) = default;

		Self &operator=(const Self &other) = default;

	public:

		std::atomic<uint64_t> &get_wts_ref() const { return data_header_ptr_->get_wts_ref(); }

		uint32_t get_data_type() const { return data_type_; }

		uint32_t get_data_size() const { return data_header_ptr_->get_data_size(); }

		DataTupleVirtualHeaderType *get_data_header_ptr() const { return data_header_ptr_; }

	public:
		void lock_write() const { data_header_ptr_->lock_write(); }

		void unlock_write() const { data_header_ptr_->unlock_write(); }

		bool is_locked_write() const { return data_header_ptr_->is_locked_write(); }

	public:
		void *get_data_ptr() const { return data_header_ptr_->get_data_ptr(); }

		void *get_latest_data_ptr() const { return data_header_ptr_->get_latest_data_ptr(); }

	public:
		void set_data(void *new_data_ptr, uint32_t size, uint32_t offset) const {
			std::memcpy(static_cast<uint8_t *>(get_latest_data_ptr()) + offset,
						static_cast<uint8_t *>(new_data_ptr) + offset,
						size);
		}
	};

}
