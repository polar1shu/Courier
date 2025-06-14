/*
 * @author: BL-GS 
 * @date:   2023/2/28
 */

#pragma once

#include <cstdint>

#include <workload/generator/operation_generator.h>
#include <workload/tpcc/tpcc_config.h>

namespace workload {

	inline namespace tpcc {

		template<class OpeType>
		struct RequestDistributionGeneratorTrait {
			using Generator = OperationGenerator<OpeType>;
		};

		template<class Config>
		struct TPCCConfigManager {
			using KeyType = int64_t;

			static constexpr bool COPY_STRING      = Config::COPY_STRING;

			static constexpr bool FORMAL_OUTPUT    = Config::FORMAL_OUTPUT;

			static constexpr bool DO_INSERT_REMOVE = Config::DO_INSERT_REMOVE;

			static constexpr std::initializer_list<uint32_t> Percentages {
					Config::OPE_STOCK_LEVEL_PERCENTAGE,
					Config::OPE_DELIVERY_PERCENTAGE,
					Config::OPE_ORDER_STATUS_PERCENTAGE,
					Config::OPE_PAYMENT_PERCENTAGE,
					Config::OPE_NEW_ORDER_PERCENTAGE
			};

			using RequestGenerator =
					typename RequestDistributionGeneratorTrait<TPCCTransactionType>::Generator;
		};

	}
}
