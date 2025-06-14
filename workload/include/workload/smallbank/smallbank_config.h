/*
 * @author: BL-GS 
 * @date:   2023/10/6
 */

#pragma once

#include <cstdint>

namespace workload {

	inline namespace smallbank {

		enum class SmallBankTransactionType: int {
			/// Add some balance to a saving
			TransactSavings = 0,
			/// Add some balance to a checking
			DepositChecking = 1,
			/// Transmit balance between two accounts
			SendPayment     = 2,
			/// Remove an checking account
			WriteCheck      = 3,
			/// Transmit all balance in an saving account to an checking account
			Amalgamate      = 4,
			/// Read the balance of a user's saving account and checking account
			Query           = 5,
			/// Initialize environment for benchmark
			Initialize      = 6
		};

		enum class SmallBankConfigType {
			Light,
			Normal
		};

		template<SmallBankConfigType Type>
		struct SmallBankConfig {};

		template<>
		struct SmallBankConfig<SmallBankConfigType::Normal> {
			/// The number of customers
			static constexpr uint32_t NUM_CUSTOMER     = 100000;
			/// The number of customers which is frequently operated
			static constexpr uint32_t NUM_HOT_CUSTOMER = 4000;
			/// The percentage of operating a hot account
			static constexpr uint32_t HOT_PERCENTAGE   = 10;

			static constexpr uint32_t TRANSACT_SAVING_PERCENTAGE  = 15;
			static constexpr uint32_t DEPOSIT_CHECKING_PERCENTAGE = 15;
			static constexpr uint32_t SEND_PAYMENT_PERCENTAGE     = 15;
			static constexpr uint32_t WRITE_CHECK_PERCENTAGE      = 25;
			static constexpr uint32_t AMALGAMATE_PERCENTAGE       = 15;
			static constexpr uint32_t QUERY_PERCENTAGE            = 15;
		};
	}
}
