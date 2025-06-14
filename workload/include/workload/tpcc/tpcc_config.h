/*
 * @author: BL-GS 
 * @date:   2023/2/28
 */

#pragma once

#include <cstdint>

namespace workload {

	inline namespace tpcc {

		enum class TPCCTransactionType: int {
			StockLevel  = 0,
			Delivery    = 1,
			OrderStatus = 2,
			Payment     = 3,
			NewOrder    = 4,
			Initialize  = 5
		};

		enum class TPCCConfigType {
			Light,
			Fat,
			Normal
		};

		template<TPCCConfigType Type>
		struct TPCCConfig { };

		template<>
		struct TPCCConfig<TPCCConfigType::Normal> {

			static constexpr uint32_t NUM_ITEMS                   = 100000;

			static constexpr uint32_t NUM_WAREHOUSES              = 100;

			static constexpr uint32_t DISTRICTS_PER_WAREHOUSE     = 10;

			static constexpr uint32_t CUSTOMERS_PER_DISTRICT      = 3000;

			static constexpr uint32_t NEW_ORDER_PER_DISTRICT      = 900;

			static constexpr uint32_t REMOTE_PROBABILITY          = 1;

			static constexpr uint32_t OPE_STOCK_LEVEL_PERCENTAGE  = 4;

			static constexpr uint32_t OPE_DELIVERY_PERCENTAGE     = 4;

			static constexpr uint32_t OPE_ORDER_STATUS_PERCENTAGE = 4;

			static constexpr uint32_t OPE_PAYMENT_PERCENTAGE      = 43;

			static constexpr uint32_t OPE_NEW_ORDER_PERCENTAGE =
					100 - OPE_STOCK_LEVEL_PERCENTAGE - OPE_DELIVERY_PERCENTAGE - OPE_ORDER_STATUS_PERCENTAGE - OPE_PAYMENT_PERCENTAGE;

			static constexpr bool COPY_STRING      = true;

			static constexpr bool FORMAL_OUTPUT    = true;

			static constexpr bool DO_INSERT_REMOVE = true;
		};

		template<>
		struct TPCCConfig<TPCCConfigType::Light> {

			static constexpr uint32_t NUM_ITEMS                   = 100000;

			static constexpr uint32_t NUM_WAREHOUSES              = 100;

			static constexpr uint32_t DISTRICTS_PER_WAREHOUSE     = 10;

			static constexpr uint32_t CUSTOMERS_PER_DISTRICT      = 3000;

			static constexpr uint32_t NEW_ORDER_PER_DISTRICT      = 900;

			static constexpr uint32_t REMOTE_PROBABILITY          = 1;

			static constexpr uint32_t OPE_STOCK_LEVEL_PERCENTAGE  = 4;

			static constexpr uint32_t OPE_DELIVERY_PERCENTAGE     = 4;

			static constexpr uint32_t OPE_ORDER_STATUS_PERCENTAGE = 4;

			static constexpr uint32_t OPE_PAYMENT_PERCENTAGE      = 43;

			static constexpr uint32_t OPE_NEW_ORDER_PERCENTAGE =
					100 - OPE_STOCK_LEVEL_PERCENTAGE - OPE_DELIVERY_PERCENTAGE - OPE_ORDER_STATUS_PERCENTAGE - OPE_PAYMENT_PERCENTAGE;

			static constexpr bool COPY_STRING      = false;

			static constexpr bool FORMAL_OUTPUT    = false;

			static constexpr bool DO_INSERT_REMOVE = false;
		};

		template<>
		struct TPCCConfig<TPCCConfigType::Fat> {

			static constexpr uint32_t NUM_ITEMS                   = 100000;

			static constexpr uint32_t NUM_WAREHOUSES              = 1024;

			static constexpr uint32_t DISTRICTS_PER_WAREHOUSE     = 10;

			static constexpr uint32_t CUSTOMERS_PER_DISTRICT      = 3000;

			static constexpr uint32_t NEW_ORDER_PER_DISTRICT      = 900;

			static constexpr uint32_t REMOTE_PROBABILITY          = 1;

			static constexpr uint32_t OPE_STOCK_LEVEL_PERCENTAGE  = 4;

			static constexpr uint32_t OPE_DELIVERY_PERCENTAGE     = 4;

			static constexpr uint32_t OPE_ORDER_STATUS_PERCENTAGE = 4;

			static constexpr uint32_t OPE_PAYMENT_PERCENTAGE      = 43;

			static constexpr uint32_t OPE_NEW_ORDER_PERCENTAGE =
					100 - OPE_STOCK_LEVEL_PERCENTAGE - OPE_DELIVERY_PERCENTAGE - OPE_ORDER_STATUS_PERCENTAGE - OPE_PAYMENT_PERCENTAGE;

			static constexpr bool COPY_STRING      = false;

			static constexpr bool FORMAL_OUTPUT    = false;

			static constexpr bool DO_INSERT_REMOVE = false;
		};
	}
}
