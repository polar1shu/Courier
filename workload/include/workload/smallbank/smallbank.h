/*
 * @author: BL-GS 
 * @date:   2023/10/6
 */

#pragma once

#include <util/random_generator.h>

#include <workload/abstract_key.h>
#include <workload/abstract_workload.h>
#include <workload/generator/abstract_generator.h>
#include <workload/smallbank/smallbank_config_manager.h>
#include <workload/smallbank/smallbank_table.h>
#include <workload/smallbank/smallbank_transaction.h>
#include <workload/smallbank/smallbank_executor.h>
#include <workload/smallbank/smallbank_engine.h>

namespace workload {

	template<class Config>
	class SmallBank {
	public:
		using ConfigManager = SmallBankConfigManager<Config>;

		using KeyType       = ConfigManager::KeyType;

		using AbKeyType     = AbstractKey<KeyType>;

		using BenchExecutor = SmallBankExecutor<Config>;

		using Transaction   = SmallBankTransaction<BenchExecutor, KeyType>;

		using RequestGenerator = ConfigManager::RequestGenerator;
		static_assert(OperationGeneratorConcept<RequestGenerator>);

		static constexpr uint32_t NUM_CUSTOMER      = Config::NUM_CUSTOMER;

		static constexpr uint32_t NUM_HOT_CUSTOMER  = Config::NUM_HOT_CUSTOMER;

		static constexpr uint32_t HOT_PERCENTAGE    = Config::HOT_PERCENTAGE;

		static constexpr std::initializer_list<TableScheme> TableSchemeSizeDefinition = {
				TableScheme { sizeof(Account),  Config::NUM_CUSTOMER },
				TableScheme { sizeof(Saving),   Config::NUM_CUSTOMER },
				TableScheme { sizeof(Checking), Config::NUM_CUSTOMER }
		};

	private:
		RequestGenerator request_generator_;

		BenchExecutor bench_executor_;

		UniformGenerator<KeyType, NUM_CUSTOMER - 1> customer_id_generator_;

		UniformGenerator<KeyType, NUM_HOT_CUSTOMER - 1> hot_customer_id_generator_;

		UniformFloatGenerator<float> balance_generator_{0.0f, 9999.9f};

	public:
		SmallBank(): request_generator_(ConfigManager::Percentages) {}

	public:
		/*!
		 * @brief Generate a transaction randomly
		 * @return TPCC transaction
		 */
		Transaction generate_transaction() {
			// Get the operation randomly.
			SmallBankTransactionType ope_type = request_generator_.get_next();

			if (util::rander.rand_percentage() < HOT_PERCENTAGE) {
				auto customer_id1 = hot_customer_id_generator_.get_next();
				auto customer_id2 = hot_customer_id_generator_.get_next();
				while (customer_id2 == customer_id1) {
					customer_id2 = hot_customer_id_generator_.get_next();
				}
				return {
						ope_type,
						&bench_executor_,
						customer_id1,
						customer_id2,
						balance_generator_.get_next()
				};
			}

			auto customer_id1 = customer_id_generator_.get_next();
			auto customer_id2 = customer_id_generator_.get_next();
			while (customer_id2 == customer_id1) {
				customer_id2 = customer_id_generator_.get_next();
			}

			return {
					ope_type,
					&bench_executor_,
					customer_id1,
					customer_id2,
					balance_generator_.get_next()
			};
		}

		/*!
		 * @brief Get transactions for initialization
		 * @return An array of transactions.
		 */
		std::vector<Transaction> initialize_insert() {
			std::vector<Transaction> res_tx;
			for (uint32_t i = 0; i < Config::NUM_CUSTOMER; ++i) {
				res_tx.emplace_back(SmallBankTransactionType::Initialize,
									&bench_executor_,
									i,
									0,
									balance_generator_.get_next()
				);
			}
			return res_tx;
		}
	};
}
