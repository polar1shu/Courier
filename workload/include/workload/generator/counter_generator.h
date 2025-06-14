/*
 * @author: BL-GS 
 * @date:   2022/12/13
 */

#pragma once

#include <cstdint>
#include <atomic>

namespace workload {
	template<
	        class IntType,
			IntType UPPER_BOUND,
			IntType LOWER_BOUND = 0>
		requires std::is_integral_v<IntType>
	class CounterGenerator {
	public:
		static_assert(UPPER_BOUND > LOWER_BOUND);

		static constexpr IntType ITEM_NUMBER = UPPER_BOUND - LOWER_BOUND;

	private:
		std::atomic<IntType> limit_ = LOWER_BOUND;

	public:
		CounterGenerator() = default;

		IntType get_next() {
			return limit_++;
		}

		inline IntType get_limit() { return limit_.load(); }
	};
}
