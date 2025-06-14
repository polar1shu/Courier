//
// Created by BL-GS on 2022/10/2.
//

#pragma once

#include <cstdint>
#include <random>
#include <cstring>

namespace util {

    class RandomGenerator {

    private:
		std::random_device hardware_rand_;

        std::default_random_engine random_engine_;

    public:
	    RandomGenerator():
			    hardware_rand_(),
			    random_engine_(hardware_rand_()) {}

		auto &get_engine() {
			return random_engine_;
		}

    public:
		//! Get a random num ranging from lower to upper
		//! \param lower
		//! \param upper
		//! \return
		template<class IntType>
			requires std::is_integral_v<IntType>
	    IntType rand_range(IntType lower, IntType upper) {
			std::uniform_int_distribution<IntType> random_dis(lower, upper);
			return random_dis(random_engine_);
	    }

		//! Get a random num ranging from lower to upper
		//! And exclude a specific number
		//! \param lower
		//! \param upper
		//! \param exclude
		//! \return
		template<class IntType>
			requires std::is_integral_v<IntType>
		IntType rand_range(IntType lower, IntType upper, IntType exclude) {
			std::uniform_int_distribution<IntType> random_dis(lower, upper);
			while (true) {
				IntType res = random_dis(random_engine_);
				if (res != exclude) { return res; }
			}
		}

	    //! \param lower
	    //! \param upper
	    //! \return
	    template<class FloatType>
	        requires std::is_floating_point_v<FloatType>
	    FloatType rand_range(FloatType lower, FloatType upper) {
		    std::uniform_real_distribution<FloatType> random_dis(lower, upper);
		    return random_dis(random_engine_);
	    }

	    //! Get a random num ranging from 0 to 99
	    //! \return
	    uint32_t rand_percentage() { return rand_range(0, 100); }

	    //! Get a random num ranging from 0 to 1 in double type
	    //! \return
	    double rand_double() {
			std::uniform_real_distribution random_dis;
			return random_dis(random_engine_);
		}

	    //! Get a random num ranging from 0 to RAND_MAX
	    //! \return
	    uint32_t rand_long() { return random_engine_(); }

    };

	//! @brief Thread local random data generator
	inline static thread_local util::RandomGenerator rander;
}

