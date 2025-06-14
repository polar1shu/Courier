/*
 * @author: BL-GS 
 * @date:   2023/2/28
 */

#pragma once

#include <workload/abstract_transaction.h>
#include <workload/tpcc/tpcc_transaction.h>
#include <workload/tpcc/tpcc_config.h>
#include <workload/tpcc/tpcc_engine.h>

namespace workload {

	inline namespace tpcc {

		template<class BenchExecutor>
		class TPCCTransaction : public AbstractTransaction<TPCCTransaction<BenchExecutor>> {
		public:
			TPCCTransactionType type_;

			BenchExecutor *bench_executor_ptr_;

			uint64_t initialization_phase_;

		public:
			TPCCTransaction() = default;

			TPCCTransaction(TPCCTransactionType type, BenchExecutor *bench_executor_ptr):
					type_(type), bench_executor_ptr_(bench_executor_ptr) {}

			TPCCTransaction(TPCCTransactionType type, BenchExecutor *bench_executor_ptr, uint64_t initialization_phase):
					type_(type), bench_executor_ptr_(bench_executor_ptr), initialization_phase_(initialization_phase) {}

			TPCCTransaction(const TPCCTransaction &other) = default;

		public:
			template<class Executor>
			bool run_impl(Executor &executor) {
				switch (type_) {
					case TPCCTransactionType::Payment:
						return bench_executor_ptr_->do_payment(executor);

					case TPCCTransactionType::NewOrder:
						return bench_executor_ptr_->do_new_order(executor);

					case TPCCTransactionType::StockLevel:
						return bench_executor_ptr_->do_stock_level(executor);

					case TPCCTransactionType::Delivery:
						return bench_executor_ptr_->do_delivery(executor);

					case TPCCTransactionType::OrderStatus:
						return bench_executor_ptr_->do_order_status(executor);

					case TPCCTransactionType::Initialize:
						return bench_executor_ptr_->do_initialize(executor, initialization_phase_);

				}

				return true;
			}

		public:
			[[nodiscard]] bool is_only_read_impl() const {
				switch (type_) {
					case TPCCTransactionType::StockLevel:
						return true;
					default:
						return false;
				}
			}
		};
	}

}
