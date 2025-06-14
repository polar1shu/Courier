/*
 * @author: BL-GS 
 * @date:   2023/10/6
 */

#pragma once

#include <cstdint>

#include <workload/generator/operation_generator.h>

#include <workload/smallbank/smallbank_config.h>

namespace workload {

	inline namespace smallbank {

		template<class OpeType>
		struct RequestDistributionGeneratorTrait {
			using Generator = OperationGenerator<OpeType>;
		};

		template<class Config>
		class SmallBankConfigManager {
		public:
			static constexpr std::initializer_list<uint32_t> Percentages{
					Config::TRANSACT_SAVING_PERCENTAGE,
					Config::DEPOSIT_CHECKING_PERCENTAGE,
					Config::SEND_PAYMENT_PERCENTAGE,
					Config::WRITE_CHECK_PERCENTAGE,
					Config::AMALGAMATE_PERCENTAGE,
					Config::QUERY_PERCENTAGE
			};

			using KeyType = uint64_t;

			using RequestGenerator =
					typename RequestDistributionGeneratorTrait<SmallBankTransactionType>::Generator;
		};
	}
}
