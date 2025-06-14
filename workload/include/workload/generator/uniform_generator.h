/*
 * @author: BL-GS 
 * @date:   2022/11/27
 */

#pragma once

#include <cstdint>
#include <random>

#include <util/random_generator.h>

namespace workload {

	template<class IntType,
			IntType UPPER_BOUND,
			IntType LOWER_BOUND = 0>
		requires std::is_integral_v<IntType>
	class UniformGenerator {
	public:
		static_assert(UPPER_BOUND >= LOWER_BOUND);

		static constexpr uint32_t ITEM_NUMBER = UPPER_BOUND - LOWER_BOUND;

	private:
		std::uniform_int_distribution<IntType> uniform_dist_;

	public:
		UniformGenerator(): uniform_dist_(LOWER_BOUND, UPPER_BOUND) {}

		IntType get_next() {
			if constexpr (LOWER_BOUND == UPPER_BOUND) { return LOWER_BOUND; }
			return uniform_dist_(util::rander.get_engine());
		}

		IntType get_next(IntType limit) {
			if constexpr (LOWER_BOUND == UPPER_BOUND) { return LOWER_BOUND; }
			return uniform_dist_(util::rander.get_engine()) % limit;
		}
	};

	template<class FloatType>
		requires std::is_floating_point_v<FloatType>
	class UniformFloatGenerator {
	private:
		std::uniform_real_distribution<FloatType> uniform_dist_;

	public:
		UniformFloatGenerator(FloatType upper_bound, FloatType lower_bound): uniform_dist_(lower_bound, upper_bound) {}

		FloatType get_next() {
			return uniform_dist_(util::rander.get_engine());
		}
	};
}
