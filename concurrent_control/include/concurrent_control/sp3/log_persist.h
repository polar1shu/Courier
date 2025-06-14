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

namespace cc::sp {

	enum class LogLabel {
		Start,
		Update,
		Commit
	};

	struct LogSpace {
		uint8_t *start_ptr;
		uint8_t *cur_ptr;
		uint8_t *end_ptr;
	};

	template<LogLabel Label, class AbKey>
	struct LogTuple {
	public:
		/// The type of operation
		LogLabel label_;
		/// The length of content
		uint32_t size_;
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
	class LogPersist {
	public:
		using AbKeyType = AbKey;

		static constexpr size_t LOG_PAGE_SIZE = 2048;

	private:
		// Assume the space is large enough to contain all logs.
		std::span<uint8_t> log_space_range_;

		std::atomic<uint8_t *> cur_bound_;

	public:
		LogPersist(std::span<uint8_t> log_space_range): log_space_range_(log_space_range), cur_bound_(log_space_range.data()) {}

		~LogPersist() = default;

	public:
		std::pair<bool, LogSpace> allocate_log_space(size_t update_amount, size_t expected_size) {
			size_t size = expected_size +
					sizeof(LogTuple<LogLabel::Commit, AbKeyType>) +
					sizeof(LogTuple<LogLabel::Update, AbKeyType>) +
					sizeof(LogTuple<LogLabel::Start, AbKeyType>);
			LogSpace res_space;
			while (true) {
				uint8_t *log_ptr = res_space.start_ptr = cur_bound_.load(std::memory_order::relaxed);
				if (log_ptr + size >= std::addressof(log_space_range_.back())) {
					return { false, {} };
				}
				if (cur_bound_.compare_exchange_weak(log_ptr, res_space.start_ptr + size)) { break; }
			}
			res_space.cur_ptr = res_space.start_ptr;
			res_space.end_ptr = res_space.start_ptr + size;

			assert(res_space.cur_ptr != nullptr);
			return { true, res_space };
		}

		void reset_log_space() {
			cur_bound_.store(log_space_range_.data());
		}

	public:
		void add_start_log(LogSpace &log_space, uint64_t ts) {
			new (log_space.cur_ptr) LogTuple<LogLabel::Start, AbKeyType>{
					.label_ = LogLabel::Start,
					.ts_ = ts
			};
			log_space.cur_ptr += sizeof(LogTuple<LogLabel::Start, AbKeyType>);
		}

		void add_commit_log(LogSpace &log_space, uint64_t ts) {
			new (log_space.cur_ptr) LogTuple<LogLabel::Commit, AbKeyType>{
					.label_ = LogLabel::Commit,
					.ts_ = ts
			};
			log_space.cur_ptr += sizeof(LogTuple<LogLabel::Commit, AbKeyType>);
		}

		void add_update_log(LogSpace &log_space, const void *src, uint32_t size) {
			auto *log_tuple_ptr = reinterpret_cast<LogTuple<LogLabel::Update, AbKeyType> *>(log_space.cur_ptr);
			new (log_tuple_ptr) LogTuple<LogLabel::Update, AbKeyType>{
					.label_ = LogLabel::Update,
					.size_ = size
			};
			std::memcpy(log_tuple_ptr->extra_info_, src, size);
			log_space.cur_ptr += sizeof(LogTuple<LogLabel::Update, AbKeyType>) + size;
		}
	};

}
