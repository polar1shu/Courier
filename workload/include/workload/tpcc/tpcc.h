/*
 * @author: BL-GS 
 * @date:   2023/2/28
 */

#pragma once

#include <cstdint>
#include <vector>
#include <util/random_generator.h>

#include <workload/abstract_workload.h>
#include <workload/tpcc/tpcc_config.h>
#include <workload/tpcc/tpcc_config_manager.h>
#include <workload/tpcc/tpcc_executor.h>
#include <workload/tpcc/tpcc_transaction.h>

namespace workload {

	inline namespace tpcc {

		template<class Config>
		class TPCC {
		public:
			using ConfigManager    = TPCCConfigManager<Config>;

			using KeyType          = ConfigManager::KeyType;

			using AbKeyType        = AbstractKey<KeyType>;

			using BenchExecutor    = TPCCExecutor<Config>;

			using Transaction      = TPCCTransaction<BenchExecutor>;

			using RequestGenerator = ConfigManager::RequestGenerator;
			static_assert(OperationGeneratorConcept<RequestGenerator>);

			// Aware: The order of table should be the same Enumeration of that.
			static constexpr std::initializer_list<TableScheme> TableSchemeSizeDefinition = {
							TableScheme { sizeof(Item),         Config::NUM_ITEMS  },
							TableScheme { sizeof(Warehouse),    Config::NUM_WAREHOUSES },
							TableScheme { sizeof(District),     Config::NUM_WAREHOUSES * Config::DISTRICTS_PER_WAREHOUSE },
							TableScheme { sizeof(Stock),        Config::NUM_WAREHOUSES * Config::NUM_ITEMS },
							TableScheme { sizeof(Customer),     Config::NUM_WAREHOUSES * Config::DISTRICTS_PER_WAREHOUSE * Config::CUSTOMERS_PER_DISTRICT },
							TableScheme { sizeof(Order),        Config::NUM_WAREHOUSES * Config::DISTRICTS_PER_WAREHOUSE * Config::CUSTOMERS_PER_DISTRICT },
							TableScheme { sizeof(OrderLine),    Config::NUM_WAREHOUSES * Config::DISTRICTS_PER_WAREHOUSE * Config::CUSTOMERS_PER_DISTRICT * Order::MAX_OL_CNT },
							TableScheme { sizeof(NewOrder),     Config::NUM_WAREHOUSES * Config::DISTRICTS_PER_WAREHOUSE * Config::NEW_ORDER_PER_DISTRICT },
							TableScheme { sizeof(History),      Config::NUM_WAREHOUSES * Config::DISTRICTS_PER_WAREHOUSE * Config::CUSTOMERS_PER_DISTRICT },
							TableScheme { sizeof(NewOrderID),   Config::NUM_WAREHOUSES * Config::DISTRICTS_PER_WAREHOUSE }
			};

			static constexpr bool COPY_STRING = ConfigManager::COPY_STRING;

		private:
			RequestGenerator request_generator_;

			BenchExecutor bench_executor_;

		public:
			TPCC(): request_generator_(ConfigManager::Percentages) {}

			/*!
			 * @brief Generate a transaction randomly
			 * @return TPCC transaction
			 */
			Transaction generate_transaction() {
				// Get the operation randomly.
				TPCCTransactionType ope_type = request_generator_.get_next();
				return Transaction {
					ope_type,
					&bench_executor_
				};
			}

			/*!
			 * @brief Get transactions for initialization
			 * @return An array of transactions.
			 */
			std::vector<Transaction> initialize_insert() {
				// Get the number of phases for initialization.
				auto initialize_phase_num = BenchExecutor::total_initialize_phase_num();
				// Make a transaction list.
				std::vector<Transaction> res_vec(initialize_phase_num, Transaction{
						TPCCTransactionType::Initialize,
						&bench_executor_
				});

				for (uint64_t i = 0; i < res_vec.size(); ++i) {
					res_vec[i].initialization_phase_ = i;
				}
				// Small vector, just return it.
				return res_vec;
			}
		};
	}

}
