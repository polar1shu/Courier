/*
 * @author: BL-GS 
 * @date:   2023/1/28
 */

#pragma once

#include <cstdint>
#include <concepts>

#include <workload/abstract_key.h>
#include <workload/abstract_transaction.h>

namespace workload {

	template<class Workload>
	concept WorkloadConcept = requires(Workload wl,
			Workload::KeyType key,
			Workload::Transaction tx) {

		typename Workload::KeyType;

		typename Workload::AbKeyType;

		typename Workload::Transaction;

		Workload::TableSchemeSizeDefinition;

		/*!
		 * @brief Function for initializing workload.
		 */
		{ wl.initialize_insert() } -> std::same_as<std::vector<typename Workload::Transaction>>;

		/*!
		 * @brief Function for generating new transaction
		 */
		{ wl.generate_transaction() } -> std::same_as<typename Workload::Transaction>;
	};


	/*!
	 * @brief The information about size and number of tuples.
	 */
	struct TableScheme {
		/// Size of tuple
		size_t tuple_size;
		/// The maximal number of this tuple
		size_t max_tuple_num;
	};

}
