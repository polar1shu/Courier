/*
 * @author: BL-GS 
 * @date:   2022/11/27
 */

#pragma once

#include <cstdint>
#include <cmath>

#include <util/random_generator.h>

namespace workload {

	template<
	        class IntType,
			IntType UPPER_BOUND,
			IntType LOWER_BOUND = 0,
			IntType ZIPFIAN_CONSTANT = 99>
		requires std::is_integral_v<IntType>
	class ZipfianGenerator {
		static_assert(UPPER_BOUND > LOWER_BOUND);
		static_assert(ZIPFIAN_CONSTANT < 100);

	private:
		static constexpr IntType  ITEM_NUMBER = UPPER_BOUND - LOWER_BOUND;

		static constexpr double THETA = static_cast<double>(ZIPFIAN_CONSTANT) / 100.0;

		static constexpr double ALPHA = 1.0 / (1.0 - THETA);

		double zeta_n_;
		double zeta_to_theta_;
		IntType  count_for_zeta_;
		double eta_;

	public:
		ZipfianGenerator() :
			zeta_n_{zeta_static(ITEM_NUMBER)},
			zeta_to_theta_{zeta(2)},
			count_for_zeta_{ITEM_NUMBER},
			eta_{(1 - std::pow(2.0 / static_cast<double>(ITEM_NUMBER), 1 - THETA)) / (1 - zeta_to_theta_ / zeta_n_)}
		{}

		IntType  get_next() {
			if (ITEM_NUMBER != count_for_zeta_) {
				if (ITEM_NUMBER > count_for_zeta_) {
					zeta_n_ = zeta(count_for_zeta_, ITEM_NUMBER, zeta_n_);
				}
				else {
					zeta_n_ = zeta(ITEM_NUMBER);
				}
				eta_ = (1 - std::pow(2.0 / ITEM_NUMBER, 1 - THETA)) / (1 - zeta_to_theta_ / zeta_n_);
			}

			double u = util::rander.rand_double();
			double uz = u * zeta_n_;

			if (uz < 1.0) { return LOWER_BOUND; }

			if (uz < 1.0 + std::pow(0.5, THETA)) { return LOWER_BOUND + 1; }

			return LOWER_BOUND + (ITEM_NUMBER * std::pow(eta_ * u - eta_ + 1, ALPHA));
		}

		IntType  get_next(IntType  limit) {
			IntType  item_number = limit - LOWER_BOUND;
			if (item_number != count_for_zeta_) {
				if (item_number > count_for_zeta_) {
					zeta_n_ = zeta(count_for_zeta_, item_number, zeta_n_);
				}
				else {
					zeta_n_ = zeta(item_number);
				}
				eta_ = (1 - std::pow(2.0 / item_number, 1 - THETA)) / (1 - zeta_to_theta_ / zeta_n_);
			}

			double u = util::rander.rand_double();
			double uz = u * zeta_n_;

			if (uz < 1.0) { return LOWER_BOUND; }

			if (uz < 1.0 + std::pow(0.5, THETA)) { return LOWER_BOUND + 1; }

			return LOWER_BOUND + (item_number * std::pow(eta_ * u - eta_ + 1, ALPHA));
		}

		IntType  get_next_with_iter_number(IntType  item_number) {
			if (item_number != count_for_zeta_) {
				if (item_number > count_for_zeta_) {
					zeta_n_ = zeta(count_for_zeta_, ITEM_NUMBER, zeta_n_);
				}
				else {
					zeta_n_ = zeta(item_number);
				}
				eta_ = (1 - std::pow(2.0 / item_number, 1 - THETA)) / (1 - zeta_to_theta_ / zeta_n_);
			}

			double u = util::rander.rand_double();
			double uz = u * zeta_n_;

			if (uz < 1.0) { return LOWER_BOUND; }

			if (uz < 1.0 + std::pow(0.5, THETA)) { return LOWER_BOUND + 1; }

			return LOWER_BOUND + (item_number * std::pow(eta_ * u - eta_ + 1, ALPHA));
		}

	private:
		static double zeta_static(IntType  n, IntType  st = 0, double initial_sum = 0.0) {
			double sum = initial_sum;
			for (IntType  i = st; i < n; ++i) {
				sum += 1 / (std::pow(i + 1, THETA));
			}
			return sum;
		}

		double zeta(IntType  n, IntType  st = 0, double initial_sum = 0) {
			count_for_zeta_ = n;
			return zeta_static(n, st, initial_sum);
		}
	};
}
