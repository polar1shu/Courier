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

#include <util/utility_macro.h>
#include <memory/cache_config.h>

#include <concurrent_control/courier/log.h>
#include <concurrent_control/courier/thread_context.h>

namespace cc::courier {

	struct LogMetadata {
		void *allocate_bitmap_;
	};

	template<class AbKey>
	class LogPersist {
	public:
		using AbKeyType = AbKey;

		static constexpr size_t LOG_PAGE_SIZE = 48_KB;

	private:
		std::span<uint8_t> log_space_range_;

		std::atomic<uint8_t> *log_space_available_;

		LogMetadata log_metadata_;

	public:
		LogPersist(std::span<uint8_t> log_space_range) {
			size_t page_num    = log_space_range.size() / LOG_PAGE_SIZE;
			size_t bitmap_size = align_to_cache_line((page_num + 7) / 8);
			log_space_available_           = new std::atomic<uint8_t>[bitmap_size]{0};
			log_metadata_.allocate_bitmap_ = log_space_range.data();
			log_space_range_               = log_space_range.subspan(bitmap_size);
		}

		~LogPersist() {
			delete[] log_space_available_;
		}

		size_t get_page_num() const {
			return log_space_range_.size() / LOG_PAGE_SIZE;
		}

	public:
		std::optional<LogSpace> allocate_log_space() {
			thread_local_context.page_idx = (thread_local_context.page_idx + 1) % get_page_num();
			uint32_t page_idx = thread_local_context.page_idx;

			size_t bit_mask_idx       = page_idx / 8;
			size_t bit_mask_inner_idx = page_idx % 8;

			uint8_t bit_mask = log_space_available_[bit_mask_idx].load(std::memory_order::acquire);
			uint8_t bit_and = bit_mask | (1 << bit_mask_inner_idx);

			if (bit_mask != bit_and && log_space_available_[bit_mask_idx].compare_exchange_weak(bit_mask, bit_and)) {

				LogSpace res_space {
					.start_ptr = log_space_range_.data() + page_idx * LOG_PAGE_SIZE
				};
				res_space.cur_ptr = res_space.start_ptr;
				res_space.end_ptr = res_space.start_ptr + LOG_PAGE_SIZE;

				log_space_assert(res_space);
				return res_space;
			}
			return std::nullopt;
		}

		void deallocate_log_space(LogSpace log_space) {
			size_t page_idx = (log_space.start_ptr - log_space_range_.data()) / LOG_PAGE_SIZE;

			size_t bit_mask_idx       = page_idx / 8;
			size_t bit_mask_inner_idx = page_idx % 8;

			uint8_t bit_and = ~(1 << bit_mask_inner_idx);
			log_space_available_[bit_mask_idx] &= bit_and;
		}

	public:
		void add_commit_log(LogSpace &log_space, uint64_t ts) {
			log_space_assert(log_space);
			new (log_space.cur_ptr) LogTuple<LogLabel::Commit, AbKeyType>{
					.label_ = LogLabel::Commit,
					.ts_ = ts
			};
			log_space.cur_ptr += sizeof(LogTuple<LogLabel::Commit, AbKeyType>);
			log_space_assert(log_space);
		}

		void add_delete_log(LogSpace &log_space, uint64_t ts, AbKey key) {
			log_space_assert(log_space);
			new (log_space.cur_ptr) LogTuple<LogLabel::Delete, AbKeyType>{
					.label_ = LogLabel::Delete,
					.ts_ = ts,
					.key_ = key
			};
			log_space.cur_ptr += sizeof(LogTuple<LogLabel::Delete, AbKeyType>);
			log_space_assert(log_space);
		}

		void add_update_log(LogSpace &log_space, uint64_t ts, AbKey key, const void *src, uint32_t size, uint32_t offset) {
			auto *log_tuple_ptr = reinterpret_cast<LogTuple<LogLabel::Update, AbKeyType> *>(log_space.cur_ptr);
			log_space_assert(log_space);
			new (log_tuple_ptr) LogTuple<LogLabel::Update, AbKeyType>{
					.label_ = LogLabel::Update,
					.ts_ = ts,
					.key_ = key,
					.size_ = size,
					.offset_ = offset
			};
			std::memcpy(log_tuple_ptr->extra_info_, static_cast<const uint8_t *>(src) + offset, size);
			log_space.cur_ptr += sizeof(LogTuple<LogLabel::Update, AbKeyType>) + size;
			log_space_assert(log_space);
		}

		void add_insert_log(LogSpace &log_space, uint64_t ts, AbKey key, const void *src, uint32_t size) {
			auto *log_tuple_ptr = reinterpret_cast<LogTuple<LogLabel::Insert, AbKeyType> *>(log_space.cur_ptr);
			log_space_assert(log_space);
			new (log_tuple_ptr) LogTuple<LogLabel::Insert, AbKeyType>{
					.label_ = LogLabel::Insert,
					.ts_ = ts,
					.key_ = key,
					.size_ = size
			};
			std::memcpy(log_tuple_ptr->extra_info_, src, size);
			log_space.cur_ptr += sizeof(LogTuple<LogLabel::Insert, AbKeyType>) + size;
			log_space_assert(log_space);
		}

	public:
		static bool log_space_enough(const LogSpace &log_space, const uint32_t log_size) {
			const uint32_t total_size = log_size + sizeof(LogTuple<LogLabel::Commit, AbKeyType>);
			if (total_size > LOG_PAGE_SIZE) [[unlikely]] {
				spdlog::warn("The size of log page (size: {}) is smaller than log request (size: {}) of one transaction",
				             LOG_PAGE_SIZE, total_size);
			}
			return (log_space.end_ptr - log_space.cur_ptr) >= total_size;
		}

		void log_space_assert(const LogSpace &log_space) const {
			// global range
			DEBUG_ASSERT(log_space.start_ptr >= log_space_range_.data());
			DEBUG_ASSERT(log_space.end_ptr <= log_space_range_.data() + log_space_range_.size());
			// space range
			DEBUG_ASSERT(log_space.cur_ptr >= log_space.start_ptr);
			DEBUG_ASSERT(log_space.end_ptr >= log_space.cur_ptr);
			DEBUG_ASSERT(log_space.end_ptr > log_space.start_ptr);
		}

	};

} // namespace cc::courier
