/*
 * @author: BL-GS 
 * @date:   2023/3/3
 */

#pragma once

#include <cstdint>
#include <cassert>
#include <cstring>

namespace workload {

	class RandomLastNameGenerator {
	public:
		static constexpr const char* SYLLABLES[] = {
				"BAR", "OUGHT", "ABLE", "PRI", "PRES", "ESE", "ANTI", "CALLY", "ATION", "EING", };

		static constexpr uint32_t LENGTHS[] = { 3, 5, 4, 3, 4, 3, 4, 5, 5, 4, };

	public:
		RandomLastNameGenerator() = default;

		void get_last_name(int num, char* name) {
			assert(0 <= num && num <= 999);
			int indicies[] = { num / 100, (num / 10) % 10, num % 10 };

			int offset = 0;
			for (auto indicy: indicies) {
				memcpy(name + offset, SYLLABLES[indicy], LENGTHS[indicy]);
				offset += LENGTHS[indicy];
			}
			name[offset] = '\0';
		}
	};

}
