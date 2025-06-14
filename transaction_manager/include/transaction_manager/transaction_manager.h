/*
 * @author: BL-GS 
 * @date:   2023/1/28
 */

#pragma once

#include <transaction_manager/abstract_transaction_manager.h>

#include <transaction_manager/simple_transaction_manager/std_transaction_manager.h>
#include <transaction_manager/preload_transaction_manager/std_transaction_manager.h>

namespace transaction {

	enum class TransactionManagerType {
		StdTransactionManager,
		PreloadStdTransactionManager
	};

	// ------ Register transaction manager public.

	template<TransactionManagerType TMT, class Workload, class CC>
	struct TransactionManagerManager {
		using TransactionManager = void;
	};

	template<class Workload, class CC>
	struct TransactionManagerManager<TransactionManagerType::StdTransactionManager, Workload, CC> {
		using TransactionManager = StdTransactionManager<Workload, CC>;

		static_assert(TransactionManagerConcept<TransactionManager>);
	};

	template<class Workload, class CC>
	struct TransactionManagerManager<TransactionManagerType::PreloadStdTransactionManager, Workload, CC> {
		using TransactionManager = PreloadStdTransactionManager<Workload, CC>;

		static_assert(TransactionManagerConcept<TransactionManager>);
	};
}
