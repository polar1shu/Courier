/*
 * @author: BL-GS 
 * @date:   2023/1/2
 */

#pragma once

#include <cstdint>
#include <optional>
#include <atomic>
#include <shared_mutex>

#include <memory/memory.h>
#include <thread/thread.h>

namespace cc {

	inline namespace mvcc {

		struct WriteHis {
		public:
			static constexpr uint64_t DEFAULT_COMMIT_TS = std::numeric_limits<uint64_t>::max();

		public:
			WriteHis *next_ptr_;

			alignas(32) uint8_t data_ptr_[];

			WriteHis(): next_ptr_(nullptr) {}

		public:
			void *get_data_ptr() const { return (void *)data_ptr_; }
		};

		struct alignas(32) DataHeader {
		public:
			static constexpr uint64_t DEFAULT_COMMIT_TS = std::numeric_limits<uint64_t>::max();

			static constexpr uint64_t DIRECT_VISIT_VERSION_NUM = 32;

		public:
			using Self        = DataHeader;

			using VersionType = WriteHis;

			struct VersionInfo {
				std::atomic<uint64_t> rts_;
				std::atomic<uint64_t> wts_;
				std::atomic<VersionType *> version_ptr_;

				VersionInfo(): version_ptr_(nullptr) {}
			};

			struct VersionIter {
				VersionInfo *info_;
				uint32_t idx_;

				VersionIter(VersionInfo *info, uint32_t idx): info_(info), idx_(idx) {}

				void operator++() {
					++idx_;
					if (idx_ == DIRECT_VISIT_VERSION_NUM) { idx_ = 0; }
				}

				void operator--() {
					if (idx_ == 0) { idx_ = DIRECT_VISIT_VERSION_NUM; }
					--idx_;
				}

				VersionInfo *operator->() const {
					return info_ + idx_;
				}

				VersionInfo &operator* () const {
					return info_[idx_];
				}

				bool operator == (const VersionIter &other) const {
					return idx_ == other.idx_;
				}
			};

		public:
			thread::RWLock lock_;

			uint64_t insert_ts_;

			uint64_t delete_ts_;

			size_t data_size_;

			std::atomic<uint32_t> oldest_version_idx_;
			std::atomic<uint32_t> next_version_idx_;

			VersionInfo version_info_array_[DIRECT_VISIT_VERSION_NUM];

		public:
			DataHeader():
					insert_ts_(DEFAULT_COMMIT_TS),
					delete_ts_(DEFAULT_COMMIT_TS),
					data_size_(0),
					oldest_version_idx_(0),
					next_version_idx_(0) {}

			DataHeader(uint64_t insert_ts, size_t data_size, VersionType *version_ptr):
					insert_ts_(insert_ts),
					delete_ts_(DEFAULT_COMMIT_TS),
					data_size_(data_size),
					oldest_version_idx_(0),
					next_version_idx_(1) {
				version_info_array_[0].wts_.store(insert_ts, std::memory_order::relaxed);
				version_info_array_[0].version_ptr_.store(version_ptr, std::memory_order::relaxed);
			}

		public:
			bool try_lock_read() { return lock_.try_lock_read(); }

			void lock_read() { lock_.lock_read(); }

			void unlock_read() { lock_.unlock_read(); }

			bool try_lock_write() { return lock_.try_lock_write(); }

			void lock_write() { lock_.lock_write(); }

			void unlock_write() { lock_.unlock_write(); }

		public:
			uint64_t get_insert_ts() const { return insert_ts_; }

			uint64_t get_delete_ts() const { return delete_ts_; }

			size_t get_data_size() const { return data_size_; }

		public:
			void add_version(VersionType *new_version_ptr, uint64_t ts) {
				uint64_t next_version_idx = next_version_idx_.load(std::memory_order::acquire);
				VersionInfo &version_info = version_info_array_[next_version_idx];
				version_info.version_ptr_.store(new_version_ptr, std::memory_order::release);
				version_info.wts_.store(ts, std::memory_order::release);
				version_info.rts_.store(ts, std::memory_order::release);
				if (next_version_idx == DIRECT_VISIT_VERSION_NUM - 1) {
					next_version_idx_.store(0, std::memory_order::release);
				}
				else {
					next_version_idx_.store(next_version_idx + 1, std::memory_order::release);
				}
			}

			VersionType *get_version(uint64_t ts) const {
				VersionIter start_iter = get_start_iter();
				VersionIter end_iter = get_end_iter();
				--end_iter;

				while (true) {
					const VersionInfo &version_info = *end_iter;
					if (version_info.wts_ <= ts) {
						return version_info.version_ptr_.load(std::memory_order::acquire);
					}

					if (end_iter == start_iter) { break; }
					--end_iter;
				}
				return nullptr;
			}

			bool validate_update(uint64_t ts) const {
				VersionIter end_iter = get_end_iter();
				--end_iter;
				return end_iter->rts_.load(std::memory_order::acquire) <= ts;
			}

			bool need_reclaim(uint64_t min_tx_ts) const {
				VersionIter start_iter = get_start_iter();
				++start_iter;
				VersionIter end_iter = get_end_iter();

				if (start_iter == end_iter) { return false; }

				const VersionInfo &second_version_info = *start_iter;
				return second_version_info.wts_.load(std::memory_order::relaxed) < min_tx_ts;
			}

			void clear(uint64_t ts, auto &&clear_func) {
				VersionIter start_iter{get_start_iter()};
				VersionIter second_iter = start_iter;
				++second_iter;
				VersionIter end_iter{get_end_iter()};

				while (second_iter != end_iter) {
					VersionInfo &version_info        = *start_iter;
					VersionInfo &second_version_info = *second_iter;
					uint64_t version_wts = second_version_info.wts_.load(std::memory_order::relaxed);
					if (version_wts >= ts) { break; }

					VersionType *version_ptr = version_info.version_ptr_.load(std::memory_order::relaxed);
					clear_func(version_ptr);

					++start_iter; ++second_iter;
				}
				oldest_version_idx_.store(start_iter.idx_, std::memory_order::release);
			}

		private:
			VersionIter get_start_iter() const {
				return {
					const_cast<VersionInfo *>(version_info_array_),
					oldest_version_idx_.load(std::memory_order::acquire)
				};
			}

			VersionIter get_end_iter() const {
				return {
						const_cast<VersionInfo *>(version_info_array_),
						next_version_idx_.load(std::memory_order::acquire)
				};
			}

		};

		struct alignas(32) IndexTuple {
		public:
			using Self           = IndexTuple;

			using DataHeaderType = DataHeader;

			using VersionType    = WriteHis;

		public:
			uint32_t data_type_;

			DataHeaderType *data_header_ptr_;

		public:
			uint32_t get_data_type() const { return data_type_; }

			uint32_t get_data_size() const { return data_header_ptr_->get_data_size(); }

			DataHeaderType *get_data_header_ptr() const { return data_header_ptr_; }

		};

	}
}
