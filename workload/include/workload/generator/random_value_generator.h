/*
 * @author: BL-GS 
 * @date:   2022/12/2
 */

#pragma once

#include <cstdint>
#include <workload/generator/string_generator.h>

namespace workload {

	template<uint32_t Length>
	class RandomValueGenerator {
	public:
		RandomStringGenerator<uint8_t, 0, 0xFF> string_generator_;

		RandomValueGenerator() = default;

		void get_next (void *dst, uint32_t length = Length) {
			string_generator_.get_next((uint8_t *)dst, length);
		}
	};
}
