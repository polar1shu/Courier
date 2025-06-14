/*
 * @author: BL-GS 
 * @date:   2023/10/6
 */

#pragma once

#include <workload/abstract_transaction.h>
#include <workload/smallbank/smallbank_config.h>

namespace workload {

	inline namespace smallbank {

		template<class BenchExecutor, class Key>
		class SmallBankTransaction : public AbstractTransaction<SmallBankTransaction<BenchExecutor, Key>> {
		public:
			using KeyType = Key;

		public:
			SmallBankTransactionType type_;

			BenchExecutor *bench_executor_ptr_;

			KeyType customer_id_;

			KeyType customer_id_2_;

			float amount_;

		public:
			SmallBankTransaction() = default;

			SmallBankTransaction(SmallBankTransactionType type, BenchExecutor *bench_executor_ptr, KeyType customer_id, KeyType customer_id_2, float amount):
					type_(type), bench_executor_ptr_(bench_executor_ptr), customer_id_(customer_id), customer_id_2_(customer_id_2), amount_(amount) {}

			SmallBankTransaction(const SmallBankTransaction &other) = default;

		public:
			template<class Executor>
			bool run_impl(Executor &executor) {
				switch (type_) {
					case SmallBankTransactionType::TransactSavings:
						return bench_executor_ptr_->do_transact_savings(executor, customer_id_, amount_);

					case SmallBankTransactionType::DepositChecking:
						return bench_executor_ptr_->do_deposit_checking(executor, customer_id_, amount_);

					case SmallBankTransactionType::SendPayment:
						return bench_executor_ptr_->do_send_payment(executor, customer_id_, customer_id_2_, amount_);

					case SmallBankTransactionType::WriteCheck:
						return bench_executor_ptr_->do_write_check(executor, customer_id_, amount_);

					case SmallBankTransactionType::Amalgamate:
						return bench_executor_ptr_->do_amalgamate(executor, customer_id_, customer_id_2_, amount_);

					case SmallBankTransactionType::Query:
						return bench_executor_ptr_->do_query(executor, customer_id_);

					case SmallBankTransactionType::Initialize:
						return bench_executor_ptr_->do_initialize(executor, customer_id_, amount_);

				}

				return true;
			}

		public:
			[[nodiscard]] bool is_only_read_impl() const {
				switch (type_) {
					case SmallBankTransactionType::Query:
						return true;
					default:
						return false;
				}
			}

		};

	}
}
