/*
 * @author: BL-GS 
 * @date:   2023/3/3
 */

#pragma once

#include <cstdint>
#include <cassert>

#include <workload/generator/uniform_generator.h>

namespace workload {

	class NURandGenerator {

		struct NURandC {
			int c_last_;
			int c_id_;
			int ol_i_id_;
		};

	private:
		UniformGenerator<int, 256, 0>    low_rand_generator_;
		UniformGenerator<int, 1024, 0>   mid_rand_generator_;
		UniformGenerator<int, 8192, 0>   high_rand_generator_;

		NURandC c_values_;

	public:
		NURandGenerator(): c_values_(get_random_constant()) {}

		inline void update_constant() {
			NURandC c_new = get_random_constant();
			while (!valid(c_new.c_last_, c_values_.c_last_)) {
				c_new.c_last_ = static_cast<int>(low_rand_generator_.get_next());
			}
			c_values_ = c_new;
		}

		template<int Constant>
		inline int get_next(int x, int y) {
			if constexpr (Constant == 255) {
				return ((low_rand_generator_.get_next() | util::rander.rand_range(x, y)) +
						c_values_.c_last_) % (y - x + 1) + x;
			}
			else if (Constant == 1023) {
				return ((mid_rand_generator_.get_next() | util::rander.rand_range(x, y)) +
				        c_values_.c_last_) % (y - x + 1) + x;
			}
			else if (Constant == 8191) {
				return ((high_rand_generator_.get_next() | util::rander.rand_range(x, y)) +
				        c_values_.c_last_) % (y - x + 1) + x;
			}
			else {
				assert(false);
			}
		}

	private:
		NURandC get_random_constant() {
			return {
					static_cast<int>(low_rand_generator_.get_next()),
					static_cast<int>(mid_rand_generator_.get_next()),
					static_cast<int>(high_rand_generator_.get_next())
			};
		}

		// Returns true if the C-Run value is valid. See TPC-C 2.1.6.1 (page 20).
		static bool valid(int cRun, int cLoad) {
			int cDelta = std::abs(cRun - cLoad);
			return 65 <= cDelta && cDelta <= 119 && cDelta != 96 && cDelta != 112;
		}
	};

}
