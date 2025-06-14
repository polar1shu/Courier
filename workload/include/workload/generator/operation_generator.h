/*
 * @author: BL-GS 
 * @date:   2022/12/1
 */

#pragma once

#include <cstdint>
#include <cstring>
#include <array>
#include <random>

#include <util/utility_macro.h>

namespace workload {

	template<class OpeType>
	class OperationGenerator {
	private:
		std::vector<uint32_t> percentages_;

	public:
		OperationGenerator(std::initializer_list<uint32_t> percentages): percentages_(percentages) {
			DEBUG_ASSERT(std::accumulate(percentages.begin(), percentages.end(), 0U) >= 100);
		}

		OpeType get_next() {
			uint32_t rand_num = util::rander.rand_percentage();
			uint32_t res = 0;
			while (percentages_[res] < rand_num) {
				rand_num -= percentages_[res];
				++res;
			}
			return static_cast<OpeType>(res);
		}

	};
}
