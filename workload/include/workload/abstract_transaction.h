/*
 * @author: BL-GS 
 * @date:   2022/11/30
 */

#pragma once

#include <cstdint>
#include <concepts>

#include <workload/abstract_key.h>

namespace workload {

	template<class Executor, class AbKeyType>
	concept AbstractExecutorConcept = requires(
			Executor executor,
			AbKeyType abstract_key,
			size_t size,
			size_t offset,
			void *output_ptr,
			void *input_ptr
			) {

		{ executor.read(abstract_key, output_ptr, size, offset) } -> std::same_as<bool>;

		{ executor.update(abstract_key, input_ptr, size, offset) } -> std::same_as<bool>;

		{ executor.insert(abstract_key, input_ptr, size) } -> std::same_as<bool>;

		{ executor.remove(abstract_key) } -> std::same_as<bool>;
	};

	template<class Executor, class AbKeyType>
	concept FineGranularityExecutorConcept = requires(
			Executor executor,
			AbKeyType abstract_key,
			size_t size,
			size_t offset,
			void *output_ptr,
			void *input_ptr
	) {

		requires AbstractExecutorConcept<Executor, AbKeyType>;

		{ executor.template read<int>(abstract_key) } -> std::same_as<const int *>;

		{ executor.template update<int>(abstract_key) } -> std::same_as<int *>;

		{ executor.template update<int>(abstract_key, size, offset) } -> std::same_as<int *>;
	};

	template<class Executor, class AbKeyType>
	concept ExecutorConcept = requires(
			Executor executor,
			AbKeyType abstract_key,
			size_t size,
			size_t offset,
			void *output_ptr,
			void *input_ptr
	) {
		requires AbstractExecutorConcept<Executor, AbKeyType>;
		requires !FineGranularityExecutorConcept<Executor, AbKeyType>;
	};

	/*!
	 * @brief All transaction are required to inherit this class
	 * @tparam TransactionImpl
	 */
	template<class TransactionImpl>
	class AbstractTransaction {
	public:
		AbstractTransaction() = default;

		AbstractTransaction(const AbstractTransaction &other) = default;

		AbstractTransaction(AbstractTransaction &&other) noexcept = default;

	public:
		template<class ExecutorType>
		bool run(ExecutorType &executor) {
			return static_cast<TransactionImpl *>(this)->run_impl(executor);
		}

	public:
		[[nodiscard]] bool is_only_read() const {
			return static_cast<const TransactionImpl *>(this)->is_only_read_impl();
		}
	};
}
