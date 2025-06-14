/*
 * @author: BL-GS 
 * @date:   2022/12/2
 */

#pragma once

#include <cstdint>

namespace workload {

	template<class IntType,
			IntType UPPER_BOUND,
			IntType LOWER_BOUND = 0>
		requires std::is_integral_v<IntType>
	class ConstantGenerator {
	public:
		static_assert(UPPER_BOUND >= LOWER_BOUND);

	public:
		ConstantGenerator() = default;

		inline IntType get_next() {
			return (UPPER_BOUND + LOWER_BOUND) / 2;
		}

	};
}
