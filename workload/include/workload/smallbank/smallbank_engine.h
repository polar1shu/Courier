/*
 * @author: BL-GS 
 * @date:   2023/10/8
 */

#pragma once

#include <workload/abstract_key.h>
#include <workload/abstract_transaction.h>
#include <workload/smallbank/smallbank_config_manager.h>

namespace workload {

	inline namespace smallbank {

		template<class Config, class Executor>
		class SmallBankWorkloadEngine {};

		template<class Config, class Executor>
			requires FineGranularityExecutorConcept<Executor, AbstractKey<typename SmallBankConfigManager<Config>::KeyType>>
		class SmallBankWorkloadEngine<Config, Executor> {
		public:
			using ConfigManager = SmallBankConfigManager<Config>;

			using KeyType       = ConfigManager::KeyType;

			using SmallBankKey  = AbstractKey<KeyType>;

		private:
			Executor *executor_ptr_;

		public:
			explicit SmallBankWorkloadEngine(Executor *executor_ptr): executor_ptr_(executor_ptr) {}

		public:
			bool transact_savings(Executor &executor, KeyType customer_id, float amount) {
				Saving *saving = update_saving(customer_id);
				if (saving == nullptr) { return false; }

				saving->saving_balance += amount;
				return true;
			}

			bool deposit_checking(Executor &executor, KeyType customer_id, float amount) {
				Checking *checking = update_checking(customer_id);
				if (checking == nullptr) { return false; }

				checking->checking_balance += amount;
				return true;
			}

			bool send_payment(Executor &executor, KeyType customer_id1, KeyType customer_id2, float amount) {
				Checking *checking1 = update_checking(customer_id1);
				if (checking1 == nullptr) { return false; }
				Checking *checking2 = update_checking(customer_id2);
				if (checking2 == nullptr) { return false; }

				checking1->checking_balance -= amount;
				checking2->checking_balance += amount;
				return true;
			}

			bool write_check(Executor &executor, KeyType customer_id, float amount) {
				Checking *checking = update_checking(customer_id);
				if (checking == nullptr) { return false; }
				Saving *saving = update_saving(customer_id);
				if (saving == nullptr) { return false; }

				float total = saving->saving_balance + checking->checking_balance;
				if (total < amount) {
					checking->checking_balance -= (amount - 1);
				}
				else {
					checking->checking_balance -= amount;
				}
				return true;
			}

			bool amalgamate(Executor &executor, KeyType customer_id1, KeyType customer_id2, float amount) {
				Saving *saving1 = update_saving(customer_id1);
				if (saving1 == nullptr) { return false; }
				Checking *checking1 = update_checking(customer_id1);
				if (checking1 == nullptr) { return false; }
				Checking *checking2 = update_checking(customer_id2);
				if (checking2 == nullptr) { return false; }

				checking2->checking_balance += saving1->saving_balance + checking1->checking_balance;
				saving1->saving_balance = 0;
				checking1->checking_balance = 0;

				return true;
			}

			bool query(Executor &executor, KeyType customer_id) {
				const Saving *saving = find_saving(customer_id);
				if (saving == nullptr) { return false; }
				const Checking *checking = find_checking(customer_id);
				if (checking == nullptr) { return false; }

				__attribute__((unused)) float res = saving->saving_balance + checking->checking_balance;
				return true;
			}

		public:
			const Account *find_account(KeyType customer_id) {
				SmallBankKey key{DurableTable::Account, customer_id};
				return executor_ptr_->template read<Account>(key);
			}

			const Saving *find_saving(KeyType customer_id) {
				SmallBankKey key{DurableTable::Saving, customer_id};
				return executor_ptr_->template read<Saving>(key);
			}

			const Checking *find_checking(KeyType customer_id) {
				SmallBankKey key{DurableTable::Checking, customer_id};
				return executor_ptr_->template read<Checking>(key);
			}

		public:
			Account *update_account(KeyType customer_id) {
				SmallBankKey key{DurableTable::Account, customer_id};
				return executor_ptr_->template update<Account>(key);
			}

			Saving *update_saving(KeyType customer_id) {
				SmallBankKey key{DurableTable::Saving, customer_id};
				return executor_ptr_->template update<Saving>(key);
			}

			Checking *update_checking(KeyType customer_id) {
				SmallBankKey key{DurableTable::Checking, customer_id};
				return executor_ptr_->template update<Checking>(key);
			}

		public:
			bool insert_account(const Account &account) {
				return executor_ptr_->insert(
						SmallBankKey{DurableTable::Account, account.customer_id},
						&account,
						sizeof(Account)
				);
			}

			bool insert_saving(const Saving &saving) {
				return executor_ptr_->insert(
						SmallBankKey{DurableTable::Saving, saving.customer_id},
						&saving,
						sizeof(Saving)
				);
			}

			bool insert_checking(const Checking &checking) {
				return executor_ptr_->insert(
						SmallBankKey{DurableTable::Checking, checking.customer_id},
						&checking,
						sizeof(Checking)
				);
			}

		};

		template<class Config, class Executor>
			requires ExecutorConcept<Executor, AbstractKey<typename SmallBankConfigManager<Config>::KeyType>>
		class SmallBankWorkloadEngine<Config, Executor> {
		public:
			using ConfigManager = SmallBankConfigManager<Config>;

			using KeyType       = ConfigManager::KeyType;

			using SmallBankKey  = AbstractKey<KeyType>;

		private:
			Executor *executor_ptr_;

		public:
			explicit SmallBankWorkloadEngine(Executor *executor_ptr): executor_ptr_(executor_ptr) {}

		public:
			bool transact_savings(Executor &executor, KeyType customer_id, float amount) {
				Saving saving;
				if (!find_saving(customer_id, saving)) { return false; }
				saving.saving_balance += amount;
				if (!update_saving(customer_id, saving)) { return false; }

				return true;
			}

			bool deposit_checking(Executor &executor, KeyType customer_id, float amount) {
				Checking checking;
				if (!find_checking(customer_id, checking)) { return false; }
				checking.checking_balance += amount;
				if (!update_checking(customer_id, checking)) { return false; }

				return true;
			}

			bool send_payment(Executor &executor, KeyType customer_id1, KeyType customer_id2, float amount) {
				Checking checking1;
				if (!find_checking(customer_id1, checking1)) { return false; }
				Checking checking2;
				if (!find_checking(customer_id2, checking2)) { return false; }

				checking1.checking_balance -= amount;
				checking2.checking_balance += amount;

				if (!update_checking(customer_id1, checking1) && !update_checking(customer_id2, checking2)) { return false; }
				return true;
			}

			bool write_check(Executor &executor, KeyType customer_id, float amount) {
				Checking checking;
				if (!find_checking(customer_id, checking)) { return false; }
				Saving saving;
				if (!find_saving(customer_id, saving)) { return false; }

				float total = saving.saving_balance + checking.checking_balance;
				if (total < amount) {
					checking.checking_balance -= (amount - 1);
				}
				else {
					checking.checking_balance -= amount;
				}
				if (!update_checking(customer_id, checking)) { return false; }

				return true;
			}

			bool amalgamate(Executor &executor, KeyType customer_id1, KeyType customer_id2, float amount) {
				Saving saving1;
				if (!find_saving(customer_id1, saving1)) { return false; }
				Checking checking1;
				if (!find_checking(customer_id1, checking1)) { return false; }
				Checking checking2;
				if (!find_checking(customer_id2, checking2)) { return false; }

				checking2.checking_balance += saving1.saving_balance + checking1.checking_balance;
				saving1.saving_balance = 0;
				checking1.checking_balance = 0;

				if (!update_checking(customer_id1, &checking1) && !update_checking(customer_id2, checking2) && !update_saving(customer_id1, saving1)) {
					return false;
				}

				return true;
			}

			bool query(Executor &executor, KeyType customer_id) {
				Checking checking;
				if (!find_checking(customer_id, &checking)) { return false; }
				Saving saving;
				if (!find_saving(customer_id, &saving)) { return false; }

				__attribute__((unused)) float res = saving.saving_balance + checking.checking_balance;
				return true;
			}

		public:
			bool find_account(KeyType customer_id, Account &account) {
				SmallBankKey key{DurableTable::Account, customer_id};
				return executor_ptr_->read(key, &account, sizeof(Account), 0);
			}

			bool find_saving(KeyType customer_id, Saving &saving) {
				SmallBankKey key{DurableTable::Saving, customer_id};
				return executor_ptr_->read(key, &saving, sizeof(Saving), 0);
			}

			bool find_checking(KeyType customer_id, Checking &checking) {
				SmallBankKey key{DurableTable::Checking, customer_id};
				return executor_ptr_->read(key, &checking, sizeof(Checking), 0);
			}

		public:
			bool update_account(KeyType customer_id, const Account &account) {
				SmallBankKey key{DurableTable::Account, customer_id};
				return executor_ptr_->update(key, &account, sizeof(Account), 0);
			}

			bool update_saving(KeyType customer_id, const Saving &saving) {
				SmallBankKey key{DurableTable::Saving, customer_id};
				return executor_ptr_->update(key, &saving, sizeof(Saving), 0);
			}

			bool update_checking(KeyType customer_id, const Checking &checking) {
				SmallBankKey key{DurableTable::Checking, customer_id};
				return executor_ptr_->update(key, &checking, sizeof(Checking), 0);
			}

		public:
			bool insert_account(const Account &account) {
				return executor_ptr_->insert(
						SmallBankKey{DurableTable::Account, account.customer_id},
						&account,
						sizeof(Account)
				);
			}

			bool insert_saving(const Saving &saving) {
				return executor_ptr_->insert(
						SmallBankKey{DurableTable::Saving, saving.customer_id},
						&saving,
						sizeof(Saving)
				);
			}

			bool insert_checking(const Checking &checking) {
				return executor_ptr_->insert(
						SmallBankKey{DurableTable::Checking, checking.customer_id},
						&checking,
						sizeof(Checking)
				);
			}


		};
	}
}
