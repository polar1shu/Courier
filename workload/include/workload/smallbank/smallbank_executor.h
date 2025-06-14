/*
 * @author: BL-GS 
 * @date:   2023/10/8
 */

#pragma once

#include <util/random_generator.h>

#include <workload/generator/uniform_generator.h>
#include <workload/smallbank/smallbank_config_manager.h>
#include <workload/smallbank/smallbank_generator.h>

namespace workload {

	inline namespace smallbank {

		template<class Config>
		class SmallBankExecutor {
		public:
			using ConfigManager               = SmallBankConfigManager<Config>;

			using KeyType                     = ConfigManager::KeyType;

		private:
			SmallBankGenerator generator_;

		public:
			template<class Executor>
			bool do_transact_savings(Executor &executor, KeyType customer_id, float amount) {
				SmallBankWorkloadEngine<Config, Executor> engine(&executor);

				return engine.transact_savings(executor, customer_id, amount);
			}

			template<class Executor>
			bool do_deposit_checking(Executor &executor, KeyType customer_id, float amount) {
				SmallBankWorkloadEngine<Config, Executor> engine(&executor);

				return engine.deposit_checking(executor, customer_id, amount);
			}

			template<class Executor>
			bool do_send_payment(Executor &executor, KeyType customer_id1, KeyType customer_id2, float amount) {
				SmallBankWorkloadEngine<Config, Executor> engine(&executor);

				return engine.send_payment(executor, customer_id1, customer_id2, amount);
			}

			template<class Executor>
			bool do_write_check(Executor &executor, KeyType customer_id, float amount) {
				SmallBankWorkloadEngine<Config, Executor> engine(&executor);

				return engine.write_check(executor, customer_id, amount);
			}

			template<class Executor>
			bool do_amalgamate(Executor &executor, KeyType customer_id1, KeyType customer_id2, float amount) {
				SmallBankWorkloadEngine<Config, Executor> engine(&executor);

				return engine.amalgamate(executor, customer_id1, customer_id2, amount);
			}

			template<class Executor>
			bool do_query(Executor &executor, KeyType customer_id) {
				SmallBankWorkloadEngine<Config, Executor> engine(&executor);

				return engine.query(executor, customer_id);
			}

			template<class Executor>
			bool do_initialize(Executor &executor, KeyType customer_id, float amount) {
				SmallBankWorkloadEngine<Config, Executor> engine(&executor);

				return generator_.generate_account(engine, customer_id) &&
				generator_.generate_saving(engine, customer_id, amount) &&
				generator_.generate_checking(engine, customer_id, amount);
			}
		};
	}
}
