/*
 * @author: BL-GS 
 * @date:   2023/3/22
 */

#pragma once

#include <cstdint>
#include <cstring>
#include <atomic>

#include <recovery/abstract_recovery.h>

namespace recovery {

	template<class StorageManager, class Key>
		requires StorageManagerConcept<StorageManager>
	class ReserveRecovery {
	public:
		enum class LogLabel {
			Start,
			Insert,
			Update,
			Delete,
			Commit,
			Abort
		};

		struct LogTuple {
		public:
			/// The type of operation
			LogLabel label_;
			/// The timestamp of transaction
			uint64_t ts_;
			/// The key of target
			Key      key_;
			/// The length of content
			uint32_t size_;
			/// Extra information attached to log tuple.
			uint8_t extra_info_[];

		public:
			LogTuple(LogLabel label, uint64_t ts) : label_(label), ts_(ts), size_(0) {}

			LogTuple(LogLabel label, uint64_t ts, const Key &key) :
					label_(label), ts_(ts), key_(key), size_(0) {}

			LogTuple(LogLabel label, uint64_t ts, const Key &key, const void *src, uint32_t size) :
					label_(label), ts_(ts), key_(key), size_(size) {
				std::memcpy(extra_info_, src, size);
			}
		};

		struct OccupiedSpace {
			uint8_t *start_ptr;
			uint8_t *end_ptr;
		};

	public:
		using Self         = ReserveRecovery<StorageManager, Key>;

		using KeyType      = Key;

		using LogTupleType = LogTuple;

	private:
		StorageManager *storage_manager_ptr_;

		alignas(64) std::atomic<uint8_t *> free_bound_;

		uint8_t *reset_point_;

	public:
		explicit ReserveRecovery(StorageManager *storage_manager_ptr):
				storage_manager_ptr_(storage_manager_ptr),
				free_bound_(nullptr) {

			// Warn up
			auto [success, space] = allocate_log_space(1, 0);
			assert(success);
			reset_point_ = space.end_ptr;
			free_bound_.store(space.end_ptr, std::memory_order_release);
			deallocate_log_space(space);
		}

	public:
		static constexpr uint32_t get_log_header_size() {
			return sizeof(LogTupleType);
		}

		static constexpr uint32_t get_log_header_size(size_t info_size) {
			return sizeof(LogTupleType) + info_size;
		}

		void generate_log_header(void *dst, LogLabel label, uint64_t ts) {
			new (dst) LogTupleType(label, ts);
		}

		void generate_log_header(void *dst, LogLabel label, uint64_t ts, const KeyType &key) {
			new (dst) LogTupleType(label, ts, key);
		}

		void generate_log_header(void *dst, LogLabel label, uint64_t ts, const KeyType &key, const void *info_src, size_t info_size) {
			new (dst) LogTupleType(label, ts, key, info_src, info_size);
		}

	public: // Inferior interface

		std::pair<bool, LogSpace> allocate_log_space(size_t log_amount, size_t expected_size) {
			size_t total_size  = log_amount * sizeof(LogTupleType) + expected_size;
			uint8_t *start_ptr = (uint8_t *)storage_manager_ptr_->allocate_log(total_size);
			uint8_t *end_ptr   = start_ptr + total_size;

			_mm_prefetch(start_ptr, _MM_HINT_T0);
			return std::make_pair(
					// Whether exceeding the bound
					start_ptr >= free_bound_.load(std::memory_order_acquire),
					LogSpace {
							.start_ptr = start_ptr,
							.cur_ptr   = start_ptr,
							.end_ptr   = end_ptr
					}
			);
		}

		void deallocate_log_space(LogSpace &log_space) {
			free_bound_.store(log_space.end_ptr, std::memory_order_release);
			storage_manager_ptr_->deallocate_log(log_space.start_ptr, log_space.end_ptr - log_space.start_ptr);
		}

		void reset_log_space() {
			free_bound_.store(reset_point_);
		}

		void write_log(LogSpace &log_space, const void *src, size_t log_size) {
			std::memcpy(log_space.cur_ptr, src, log_size);
			log_space.cur_ptr += log_size;
			_mm_prefetch(log_space.cur_ptr, _MM_HINT_T0);
		}

	public: // superior interface
		/*!
		 * @brief Append a new log entry indicating starting
		 * @param log_space The allocated space of log
		 * @param ts The unique id of a transaction's logs
		 */
		void start(LogSpace &log_space, uint64_t ts) { // I'm considering to abandon it.
			generate_log_header(log_space.cur_ptr, LogLabel::Start, ts);
			log_space.cur_ptr += get_log_header_size();
			_mm_prefetch(log_space.cur_ptr, _MM_HINT_T0);
		}

		/*!
		 * @brief Append a new log entry indicating updating
		 * @param log_space The allocated space of log
		 * @param ts The unique id of a transaction's logs
		 * @param key The key of logged data tuple
		 * @param src The start address of extra info
		 * @param size The size of extra info
		 */
		void update_item(LogSpace &log_space, uint64_t ts, const KeyType &key, const void *src, uint32_t size) {
			generate_log_header(log_space.cur_ptr, LogLabel::Update, ts, key, src, size);
			log_space.cur_ptr += get_log_header_size(size);
			_mm_prefetch(log_space.cur_ptr, _MM_HINT_T0);
		}

		/*!
		 * @brief Append a new log entry indicating inserting
		 * @param log_space The allocated space of log
		 * @param ts The unique id of a transaction's logs
		 * @param key The key of logged data tuple
		 * @param src The start address of extra info
		 * @param size The size of extra info
		 */
		void insert_item(LogSpace &log_space, uint64_t ts, const KeyType &key, const void *src, uint32_t size) {
			generate_log_header(log_space.cur_ptr, LogLabel::Insert, ts, key, src, size);
			log_space.cur_ptr += get_log_header_size(size);
			_mm_prefetch(log_space.cur_ptr, _MM_HINT_T0);
		}

		/*!
		 * @brief Append a new log entry indicating deleting
		 * @param log_space The allocated space of log
		 * @param ts The unique id of a transaction's logs
		 * @param key The key of logged data tuple
		 */
		void delete_item(LogSpace &log_space, uint64_t ts, const KeyType &key) {
			generate_log_header(log_space.cur_ptr, LogLabel::Delete, ts, key);
			log_space.cur_ptr += get_log_header_size();
			_mm_prefetch(log_space.cur_ptr, _MM_HINT_T0);
		}

		/*!
		 * @brief Append a new log entry indicating committing
		 * @param log_space The allocated space of log
		 * @param ts The unique id of a transaction's logs
		 */
		void commit(LogSpace &log_space, uint64_t ts) {
			generate_log_header(log_space.cur_ptr, LogLabel::Commit, ts);
			log_space.cur_ptr += get_log_header_size();

			storage_manager_ptr_->pwb_range(log_space.start_ptr, log_space.cur_ptr - log_space.start_ptr);
		}

		/*!
		 * @brief Append a new log entry indicating aborting
		 * @param log_space The allocated space of log
		 * @param ts The unique id of a transaction's logs
		 */
		void abort(LogSpace &log_space, uint64_t ts) {
			generate_log_header(log_space.cur_ptr, LogLabel::Abort, ts);
			log_space.cur_ptr += get_log_header_size();

			storage_manager_ptr_->pwb_range(log_space.start_ptr, log_space.cur_ptr - log_space.start_ptr);
		}

	};
}
