/*
 * @author: BL-GS 
 * @date:   2022/12/12
 */

#pragma once

#include <cstdint>
#include <algorithm>
#include <atomic>
#include <bitset>

#include <util/utility_macro.h>

namespace workload {

	template<class IntType,
			IntType UPPER_BOUND,
			IntType LOWER_BOUND = 0>
	requires std::is_integral_v<IntType>
	class AcknowledgedCounterGenerator {
	public:
		static_assert(UPPER_BOUND > LOWER_BOUND);

		static constexpr IntType ITEM_NUMBER = UPPER_BOUND - LOWER_BOUND;

	private:
		std::atomic<IntType> limit_;

		std::atomic<IntType> index_;

		std::mutex window_lock_;

		std::atomic<uint8_t> windows_[ITEM_NUMBER / 8 + 1];

	public:
		AcknowledgedCounterGenerator(): limit_{LOWER_BOUND}, index_{LOWER_BOUND} {
			for (auto &window: windows_) { window.store(0, std::memory_order::release); }
		}

		IntType get_next() {
			DEBUG_ASSERT(index_ <= UPPER_BOUND);
			return index_++;
		}

		void acknowledge(IntType key) {
			IntType byte_pos    = (key - LOWER_BOUND) / 8;
			IntType byte_offset = (key - LOWER_BOUND) % 8;

			DEBUG_ASSERT(index_ >= limit_.load());

			windows_[byte_pos] |= (1 << byte_offset);
			// Move forward limit by 8 once.
			if (window_lock_.try_lock()) {
				IntType limit_pos = limit_.load(std::memory_order::relaxed) / 8;
				while (windows_[limit_pos].load(std::memory_order::relaxed) == 0xFF) { ++limit_pos; }
				limit_.store(limit_pos * 8);
				window_lock_.unlock();
			}
		}

		inline IntType get_limit() { return limit_.load(std::memory_order::acquire); }
	};


	template<class IntType,
			IntType LOWER_BOUND = 0>
	requires std::is_integral_v<IntType>
	class UnlimitedAcknowledgedCounterGenerator {
	private:
		std::atomic<IntType> index_;

	public:
		UnlimitedAcknowledgedCounterGenerator(): index_{LOWER_BOUND} {}

		IntType get_next() {
			return index_++;
		}

		void acknowledge(IntType key) {}

		inline IntType get_limit() { return index_.load(std::memory_order::acquire); }
	};
}
