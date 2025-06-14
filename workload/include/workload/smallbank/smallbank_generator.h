/*
 * @author: BL-GS 
 * @date:   2023/10/8
 */

#pragma once

#include <cstdint>

#include <workload/generator/random_value_generator.h>
#include <workload/generator/uniform_generator.h>
#include <workload/smallbank/smallbank_table.h>
#include <workload/smallbank/smallbank_engine.h>

namespace workload {

	inline namespace smallbank {

		class SmallBankGenerator {
		private:
			RandomStringGenerator<char, 'a', 26> string_generator_;

		public:
			template<class Config, class Executor>
			bool generate_account(SmallBankWorkloadEngine<Config, Executor> &engine, uint64_t customer_id) {
				Account account;
				account.customer_id = customer_id;
				string_generator_.get_next(account.customer_name, account.CUSTOMER_NAME_MAX_LENGTH - 1);

				return engine.insert_account(account);
			}

			template<class Config, class Executor>
			bool generate_saving(SmallBankWorkloadEngine<Config, Executor> &engine, uint64_t customer_id, float amount) {
				Saving saving {
					customer_id,
					amount
				};

				return engine.insert_saving(saving);
			}

			template<class Config, class Executor>
			bool generate_checking(SmallBankWorkloadEngine<Config, Executor> &engine, uint64_t customer_id, float amount) {
				Checking checking{
					customer_id,
					amount
				};

				return engine.insert_checking(checking);
			}

		};
	}
}
