/*
 * @author: BL-GS 
 * @date:   2022/12/13
 */

#pragma once

#include <cstdint>

#include <util/simple_hash.h>

#include <workload/generator/zipfian_generator.h>

namespace workload {

	template<
	        class IntType,
			IntType UPPER_BOUND,
			IntType LOWER_BOUND = 0,
			IntType ZIPFIAN_CONSTANT = 99>
		requires std::is_integral_v<IntType>
	class ScrambleZipfianGenerator {
	private:
		static constexpr IntType ITEM_NUMBER = UPPER_BOUND - LOWER_BOUND;

		ZipfianGenerator<IntType, UPPER_BOUND, 0, ZIPFIAN_CONSTANT> zipfian_generator;

	public:
		ScrambleZipfianGenerator() : zipfian_generator() {}

		IntType get_next(IntType limit = UPPER_BOUND) {
			IntType res = zipfian_generator.get_next();
			res = LOWER_BOUND + util::hash(res) % ITEM_NUMBER;
			return res;
		}
	};
}
