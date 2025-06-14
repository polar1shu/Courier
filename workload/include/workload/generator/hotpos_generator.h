/*
 * @author: BL-GS 
 * @date:   2022/11/27
 */

#pragma once

#include <cstdint>
#include <cmath>
#include <random>
#include <workload/generator/uniform_generator.h>

namespace workload {

	template<class IntType,
			IntType UPPER_BOUND,
			IntType LOWER_BOUND     = 0,
			uint32_t HOTSET_FRACTION = 0,
			uint32_t HOTOPN_FRACTION = 0>
		requires std::is_integral_v<IntType>
	class HotposGenerator {

	public:
		static_assert(UPPER_BOUND > LOWER_BOUND);
		static_assert(HOTSET_FRACTION <= 100);
		static_assert(HOTOPN_FRACTION <= 100);

		static constexpr IntType ITEM_NUMBER      = UPPER_BOUND - LOWER_BOUND;
		static constexpr IntType HOT_ITEM_NUMBER  = ITEM_NUMBER * HOTSET_FRACTION / 100;
		static constexpr IntType COLD_ITEM_NUMBER = ITEM_NUMBER - HOT_ITEM_NUMBER;

	private:
		UniformGenerator<IntType, LOWER_BOUND + HOT_ITEM_NUMBER, LOWER_BOUND + HOT_ITEM_NUMBER + COLD_ITEM_NUMBER>
		        uniform_generator_;

	public:
		HotposGenerator() = default;

	    IntType get_next() {
			if (util::rander.rand_percentage() < HOTOPN_FRACTION) {
				return LOWER_BOUND + std::default_random_engine::max() % HOT_ITEM_NUMBER;
			}
			else {
				return uniform_generator_.get_next();
			}
		}

		inline IntType get_next(IntType limit) {
			IntType item_number      = limit - LOWER_BOUND;
			IntType hot_item_number  = item_number * HOTSET_FRACTION / 100;

			if (util::rander.rand_percentage() < HOTOPN_FRACTION) {
				return LOWER_BOUND + std::default_random_engine::max() % hot_item_number;
			}
			else {
				uint32_t cold_item_number = item_number - hot_item_number;
				return util::rander.rand_range(LOWER_BOUND + hot_item_number,
				                               LOWER_BOUND + hot_item_number + cold_item_number);
			}
		}
	};
}
