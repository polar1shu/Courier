/*
 * @author: BL-GS 
 * @date:   2022/12/14
 */

#pragma once

#include <cstdint>

#include <workload/generator/zipfian_generator.h>

namespace workload {
	template<class IntType,
	        IntType UPPER_BOUND,
			IntType LOWER_BOUND = 0,
			IntType ZIPFIAN_CONSTANT = 99>
		requires std::is_integral_v<IntType>
	class SkewLatestGenerator {
	private:
		ZipfianGenerator<IntType, UPPER_BOUND, 0, ZIPFIAN_CONSTANT> zipfian_generator;

	public:
		SkewLatestGenerator() : zipfian_generator() {}

		inline uint32_t get_next(uint32_t limit = UPPER_BOUND) {
			uint32_t res = zipfian_generator.get_next_with_iter_number(limit);
			return limit - res;
		}
	};
}
