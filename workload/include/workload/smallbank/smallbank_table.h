/*
 * @author: BL-GS 
 * @date:   2023/10/6
 */

#pragma once

#include <cstdint>

namespace workload {

	inline namespace smallbank {

		struct Account {
			static constexpr uint32_t CUSTOMER_NAME_MAX_LENGTH = 64;

			uint64_t customer_id;
			char customer_name[CUSTOMER_NAME_MAX_LENGTH];
		};

		struct Saving {
			static constexpr float MIN_BALANCE              = 0.01f;
			static constexpr float MAX_BALANCE              = 9999.99f;

			uint64_t customer_id;
			float saving_balance;
		};

		struct Checking {
			static constexpr float MIN_BALANCE              = 0.01f;
			static constexpr float MAX_BALANCE              = 9999.99f;

			uint64_t customer_id;
			float checking_balance;
		};

		enum class DurableTable: uint32_t {
			Account  = 0,
			Saving   = 1,
			Checking = 2
		};
	}
}
