/*
 * @author: BL-GS 
 * @date:   2023/6/6
 */

#pragma once

#include <cstdint>
#include <cstring>
#include <atomic>
#include <span>
#include <optional>

#include <memory/flush.h>

namespace cc::occ_numa {

	enum class LogLabel {
		Start,
		Insert,
		Update,
		Delete,
		Commit
	};

	template<LogLabel Label, class AbKey>
	struct LogTuple {
	public:
		/// The type of operation
		LogLabel label_;
		/// The timestamp of transaction
		uint64_t ts_;
		/// The key of target
		AbKey    key_;
		/// The length of content
		uint32_t size_;
		uint32_t offset_;
		/// Extra information attached to log tuple.
		uint8_t extra_info_[];
	};

	template<class AbKey>
	struct LogTuple<LogLabel::Start, AbKey> {
		/// The type of operation
		LogLabel label_;
		/// The timestamp of transaction
		uint64_t ts_;
	};

	template<class AbKey>
	struct LogTuple<LogLabel::Commit, AbKey> {
		/// The type of operation
		LogLabel label_;
		/// The timestamp of transaction
		uint64_t ts_;
	};

	template<class AbKey>
	struct LogTuple<LogLabel::Delete, AbKey> {
		/// The type of operation
		LogLabel label_;
		/// The timestamp of transaction
		uint64_t ts_;
		/// The key of target
		AbKey    key_;
	};

	template<class AbKey>
	struct LogTuple<LogLabel::Insert, AbKey> {
	public:
		/// The type of operation
		LogLabel label_;
		/// The timestamp of transaction
		uint64_t ts_;
		/// The key of target
		AbKey    key_;
		/// The length of content
		uint32_t size_;
		/// Extra information attached to log tuple.
		uint8_t extra_info_[];
	};

	template<class AbKey>
	class LogPersist {
	public:
		using AbKeyType = AbKey;

	private:
		// Assume the space is large enough to contain all logs.
		std::span<uint8_t> log_space_range_;

		std::atomic<uint8_t *> cur_bound_;

	public:
		LogPersist(std::span<uint8_t> log_space_range): log_space_range_(log_space_range), cur_bound_(log_space_range.data()) {}

		~LogPersist() = default;

	private:
		uint8_t *allocate_log_space(size_t expected_size) {
			uint8_t *start_ptr = log_space_range_.data();
			while (true) {
				uint8_t *log_ptr = cur_bound_.load(std::memory_order::relaxed);
				if (log_ptr + expected_size < std::addressof(log_space_range_.back())) {
					start_ptr = log_ptr;
				}
				if (cur_bound_.compare_exchange_weak(log_ptr, start_ptr + expected_size)) { break; }
			}
			return start_ptr;
		}

	public:
		void add_start_log(uint64_t ts) {
			uint8_t *start_ptr = allocate_log_space(sizeof(LogTuple<LogLabel::Start, AbKeyType>));
			new (start_ptr) LogTuple<LogLabel::Start, AbKeyType>{
					.label_ = LogLabel::Start,
					.ts_ = ts
			};
			clflushopt_range(start_ptr, sizeof(LogTuple<LogLabel::Start, AbKeyType>));
			sfence();
		}

		void add_commit_log(uint64_t ts) {
			uint8_t *start_ptr = allocate_log_space(sizeof(LogTuple<LogLabel::Commit, AbKeyType>));
			new (start_ptr) LogTuple<LogLabel::Commit, AbKeyType>{
					.label_ = LogLabel::Commit,
					.ts_ = ts
			};
			clflushopt_range(start_ptr, sizeof(LogTuple<LogLabel::Commit, AbKeyType>));
			sfence();
		}

		void add_delete_log(uint64_t ts, AbKey key) {
			uint8_t *start_ptr = allocate_log_space(sizeof(LogTuple<LogLabel::Delete, AbKeyType>));
			new (start_ptr) LogTuple<LogLabel::Delete, AbKeyType>{
					.label_ = LogLabel::Delete,
					.ts_ = ts,
					.key_ = key
			};
			clflushopt_range(start_ptr, sizeof(LogTuple<LogLabel::Delete, AbKeyType>));
			sfence();
		}

		void add_update_log(uint64_t ts, AbKey key, const void *src, uint32_t size, uint32_t offset) {
			auto *log_tuple_ptr = (LogTuple<LogLabel::Update, AbKeyType> *)allocate_log_space(sizeof(LogTuple<LogLabel::Update, AbKeyType>) + size);
			new (log_tuple_ptr) LogTuple<LogLabel::Update, AbKeyType>{
					.label_ = LogLabel::Update,
					.ts_ = ts,
					.key_ = key,
					.size_ = size,
					.offset_ = offset
			};
			std::memcpy(log_tuple_ptr->extra_info_ + offset, static_cast<const uint8_t *>(src) + offset, size);
			clflushopt_range(log_tuple_ptr, sizeof(LogTuple<LogLabel::Update, AbKeyType>) + size);
			sfence();
		}

		void add_insert_log(uint64_t ts, AbKey key, const void *src, uint32_t size) {
			auto *log_tuple_ptr = (LogTuple<LogLabel::Insert, AbKeyType> *)allocate_log_space(sizeof(LogTuple<LogLabel::Insert, AbKeyType>) + size);
			new (log_tuple_ptr) LogTuple<LogLabel::Insert, AbKeyType>{
					.label_ = LogLabel::Insert,
					.ts_ = ts,
					.key_ = key,
					.size_ = size
			};
			std::memcpy(log_tuple_ptr->extra_info_, src, size);
			clflushopt_range(log_tuple_ptr, sizeof(LogTuple<LogLabel::Insert, AbKeyType>) + size);
			sfence();
		}
	};

} // namespace cc::occ
