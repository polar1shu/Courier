/*
 * @author: BL-GS 
 * @date:   2023/3/3
 */

#pragma once

#include <cstdint>

#include <util/random_generator.h>

namespace workload {

	template<class CharType,
	        CharType Base,
			CharType Range>
		requires std::is_integral_v<CharType>
	class RandomStringGenerator {
	public:
		RandomStringGenerator() = default;

	public:
		void get_next (CharType *target, uint32_t length) {
			char rand_char = Base + util::rander.rand_range(static_cast<CharType>(0), Range);
			std::memset(target, rand_char, length - 1);
			*(target + length - 1) = static_cast<CharType>(0);
		}

		template<class IntType>
		void get_next (CharType *target, IntType min_length, IntType max_length) {
			IntType length = util::rander.rand_range(min_length, max_length);
			assert(max_length >= length && length >= min_length);
			char rand_char = Base + util::rander.rand_range(static_cast<CharType>(0), Range);
			std::memset(target, rand_char, length - 1);
			*(target + length - 1) = static_cast<CharType>(0);
		}
	};

}
