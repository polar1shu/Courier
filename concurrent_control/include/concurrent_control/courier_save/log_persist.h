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
#include <spdlog/spdlog.h>

#include <util/utility_macro.h>
#include <memory/cache_config.h>

#include <concurrent_control/courier_save/log.h>
#include <concurrent_control/courier_save/thread_context.h>

namespace cc::courier_save {

	template<class AbKey>
	class LogPersist {
	public:
		using AbKeyType = AbKey;

		static constexpr size_t LOG_PAGE_SIZE = MEM_PAGE_SIZE * 32;

	private:
		std::span<uint8_t> log_space_range_;

		std::span<uint8_t> log_space_available_;

	public:
		LogPersist(std::span<uint8_t> log_space_range) {
			size_t page_num    = log_space_range.size() / LOG_PAGE_SIZE;
			size_t bitmap_size = align_to_cache_line((page_num + 7) / 8);
			log_space_available_           = log_space_range.subspan(0, bitmap_size);
			log_space_range_               = log_space_range.subspan(bitmap_size);

			std::memset(log_space_available_.data(), 0, bitmap_size);
			clwb_range(log_space_available_.data(), bitmap_size);
			sfence();

			spdlog::info("Log range: 0x{} - 0x{}",
			             static_cast<void *>(log_space_range.data()), static_cast<void *>(log_space_range.data() + log_space_range.size()));
			spdlog::info("Log content range: 0x{} - 0x{}",
			             static_cast<void *>(log_space_range_.data()), static_cast<void *>(log_space_range_.data() + log_space_range_.size()));
			spdlog::info("Log bitmap range: 0x{} - 0x{}",
			             static_cast<void *>(log_space_available_.data()), static_cast<void *>(log_space_available_.data() + log_space_available_.size()));
		}

		size_t get_page_num() const {
			return log_space_range_.size() / LOG_PAGE_SIZE;
		}

		std::span<uint8_t> get_bitmap_ref() const {
			return log_space_available_;
		}

		std::span<uint8_t> get_pool_ref() const {
			return log_space_range_;
		}

	public:
		std::optional<LogSpace> allocate_log_space() {
			auto &page_idx = thread_local_context.page_idx;

			page_idx = (page_idx + 1) % get_page_num();

			const size_t bit_mask_idx       = page_idx / 8;
			const size_t bit_mask_inner_idx = page_idx % 8;

			const auto &log_pool_mask = std::atomic_ref<uint8_t>(log_space_available_[bit_mask_idx]);

			uint8_t bit_mask = log_pool_mask.load(std::memory_order::acquire);
			const uint8_t bit_and = bit_mask | (1 << bit_mask_inner_idx);

			if (bit_mask != bit_and && log_pool_mask.compare_exchange_strong(bit_mask, bit_and)) {

				LogSpace res_space {
					.start_ptr = log_space_range_.data() + page_idx * LOG_PAGE_SIZE
				};
				res_space.cur_ptr = res_space.start_ptr;
				res_space.end_ptr = res_space.start_ptr + LOG_PAGE_SIZE;

				return res_space;
			}
			return std::nullopt;
		}

		void deallocate_log_space(LogSpace log_space) {
			size_t page_idx = (log_space.start_ptr - log_space_range_.data()) / LOG_PAGE_SIZE;

			size_t bit_mask_idx       = page_idx / 8;
			size_t bit_mask_inner_idx = page_idx % 8;

			uint8_t bit_and = ~(1 << bit_mask_inner_idx);
			const auto &log_pool_mask = std::atomic_ref<uint8_t>(log_space_available_[bit_mask_idx]);
			log_pool_mask.fetch_and(bit_and);
		}

	public:
		void add_commit_log(LogSpace &log_space, uint64_t ts) {
			new (log_space.cur_ptr) LogTuple<LogLabel::Commit, AbKeyType>{
					.label_ = LogLabel::Commit,
					.ts_ = ts
			};
			log_space.cur_ptr += sizeof(LogTuple<LogLabel::Commit, AbKeyType>);
			log_space_assert(log_space);
		}

		void add_delete_log(LogSpace &log_space, uint64_t ts, AbKey key) {
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
		static bool log_space_enough(const LogSpace &log_space, const size_t log_size) {
			const uint32_t total_size = log_size + sizeof(LogTuple<LogLabel::Commit, AbKeyType>);
			if (total_size > LOG_PAGE_SIZE) [[unlikely]] {
				spdlog::error("The size of log for one transaction is larger than log page, require: {}", total_size);
				exit(-1);
			}

			const auto left_size = static_cast<size_t>((log_space.end_ptr - log_space.cur_ptr));
			return total_size <= left_size;
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

}
