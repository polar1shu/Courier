/*
 * @author: BL-GS 
 * @date:   2023/1/2
 */

#pragma once

#include <cstdint>
#include <atomic>
#include <array>
#include <util/latency_counter.h>
#include <spdlog/spdlog.h>
#include <thread/thread.h>

#include <concurrent_control/abstract_concurrent_control.h>

#include <concurrent_control/occ_numa/data_tuple.h>
#include <concurrent_control/occ_numa/tx_context.h>
#include <concurrent_control/occ_numa/executor.h>
#include <concurrent_control/occ_numa/log_persist.h>

namespace cc::occ_numa {

	template<class WorkloadType>
	struct OCCNUMABasic {
	public:
		using AbKeyType           = WorkloadType::AbKeyType;
		static_assert(AbstractKeyConcept<AbKeyType>);

		using KeyType             = AbKeyType::MainKeyType;
		using DataTupleHeaderType = DataTupleHeader;
		using IndexTupleType      = IndexTuple;
	};


	template<class WorkloadType, class StorageManager>
		requires StorageManagerConcept<StorageManager>
	class OCCNUMA {
	public:
		using Self                = OCCNUMA<WorkloadType, StorageManager>;

		using AbKeyType           = WorkloadType::AbKeyType;
		using KeyType             = AbKeyType::MainKeyType;
		using Transaction         = WorkloadType::Transaction;

		using DataTupleHeaderType = DataTupleHeader;
		using IndexTupleType      = IndexTuple;

		using Context             = TxContext<AbKeyType>;
		using ReadEntryType       = Context::ReadEntry;
		using WriteEntryType      = Context::WriteEntry;
		using InsertEntryType     = Context::InsertEntry;

		using ExecutorType        = Executor<Self, AbKeyType>;

		static_assert(AbstractKeyConcept<AbKeyType>);
		static_assert(ExecutorConcept<ExecutorType>);

		friend Context;

		friend ExecutorType;

	public:
		/// Global time counter
		std::atomic<uint64_t> time_counter;

		StorageManager *storage_manager_ptr_;

		LogPersist<AbKeyType> log_persist_;

		std::array<std::atomic<Context *>, thread::get_max_tid()> delegate_context_array_;

		std::array<uint32_t, thread::get_max_tid()> delegate_idx_array_;

	public: // Class Property

		explicit OCCNUMA(StorageManager *storage_manager_ptr): time_counter(0),
				storage_manager_ptr_(storage_manager_ptr), log_persist_(storage_manager_ptr->get_log_space_range()) {
			spdlog::warn("Atomic version of OCC can't make sure that data read is integral when running transaction, "
						"but this inconsistency will be detected when validating.");

			storage_manager_ptr_->register_data_deallocate_func([&](const IndexTupleType &index_tuple) {
				void *data_ptr = index_tuple.get_data_ptr();
				storage_manager_ptr_->deallocate_data_and_header(index_tuple.get_data_type(), data_ptr);
			});

			std::fill_n(delegate_context_array_.begin(), thread::get_max_tid(), nullptr);

			constexpr uint32_t cpu_num_in_node = thread::get_max_tid() / thread::get_num_nodes();
			static_assert(thread::get_max_tid() % thread::get_num_nodes() == 0, "Acquire average distribution of CPU on NUMA node");
			constexpr uint32_t cpu_arrays[thread::get_num_nodes()][cpu_num_in_node] = ARCH_NUMA_NODE_CPUS;

			spdlog::warn("Assume numa node 0 as local node");
			for (const auto & cpu_array : cpu_arrays) {
				for (uint32_t cpu_idx = 0; cpu_idx < cpu_num_in_node; ++cpu_idx) {
					uint32_t tid = cpu_array[cpu_idx];
					delegate_idx_array_[tid] = cpu_arrays[0][cpu_idx];
				}
			}
		}

		~OCCNUMA() {
			get_concurrent_control_message().clear_up();
		}

		void flush_all_works() {
			storage_manager_ptr_->fence();
		}

		void flush_thread_work(uint32_t tid) {}

		/*!
		 * @brief Get summary of all threads, requesting no threads participating in transaction execution having exited.
		 * @return
		 */
		ConcurrentControlMessage &get_concurrent_control_message() {
			return ConcurrentControlMessage::get_summary_message();
		}

	private:

		/*!
		 * @brief Get thread local message recorder
		 * @return
		 */
		ConcurrentControlMessage &get_thread_message() {
			return ConcurrentControlMessage::get_thread_message();
		}

	public: // Outer interface

		/*!
		 * @brief Get executor for transaction execution
		 * @return Executor with initialized context of transaction
		 */
		ExecutorType get_executor() {
			ExecutorType executor(this);
			init_tx(executor.get_context());
			return executor;
		}

	private:
		uint64_t get_new_wts() {
			return ++time_counter;
		}

		/*!
		 * @brief Initialize context of transaction
		 * @param tx_context Context of transaction
		 * @return Always true
		 */
		bool init_tx(Context &tx_context) {
			get_thread_message().start_transaction();
			tx_context.message_.start_total();
			tx_context.message_.start_running();
			tx_context.status_ = Context::Status::Running;
			return true;
		}

		/*!
		 * @brief Clear all temp data left by 'run_transaction'
		 * @param tx_context Context of transaction
		 * @return Always true
		 */
		bool clean_up(Context &tx_context) {
			switch (tx_context.status_) {
				case Context::Status::Running:
					tx_context.message_.end_running();
					break;

				case Context::Status::Validating:
					tx_context.message_.end_validate();
					break;

				default:
					break;
			}
			return true;
		}

	private: // Transaction Deal

		/*
		 * Interface for executor when running workload
		 */

		/*!
		 * @brief Read operation
		 * @param tx_context Context of transaction
		 * @param key Abstract key of data tuple
		 * @return Whether reading is successful
		 */
		void *read(Context &tx_context, const AbKeyType &key) {
			// Look up write set
			void *data_ptr = tx_context.look_up_write_set(key);
			if (data_ptr != nullptr) { return data_ptr; }
			// Index reading
			IndexTupleType temp_index_tuple;
			if (!read_index(tx_context, key, temp_index_tuple)) { return nullptr; }
			// Read timestamp before copying data.
			// Correspondingly, we will write data before write timestamp in write phase.
			// Then, if we read the old data, the timestamp should be old, too, which will incur validation failure.
			data_ptr = tx_context.access_read(key, temp_index_tuple);
			return data_ptr;
		}


		/*!
		 * @brief Update operation
		 * @param tx_context Context of transaction
		 * @param key Abstract key of data tuple
		 * @return Whether the tuple exists
		 */
		void *update(Context &tx_context, const AbKeyType &key) {
			// Index reading
			IndexTupleType temp_index_tuple;
			if (!read_index(tx_context, key, temp_index_tuple)) { return nullptr; }
			// Read timestamp before copying data.
			// Correspondingly, we will write data before write timestamp in write phase.
			// Then, if we read the old data, the timestamp should be old, too, which will incur validation failure.
			void *data_ptr = tx_context.access_write(key, temp_index_tuple, temp_index_tuple.get_data_size(), 0);
			return data_ptr;
		}

		/*!
		 * @brief Update operation
		 * @param tx_context Context of transaction
		 * @param key Abstract key of data tuple
		 * @param size Size to update
		 * @param offset Offset of start position to data tuple
		 * @return Whether the tuple exists
		 */
		void *update(Context &tx_context, const AbKeyType &key, uint32_t size, uint32_t offset) {
			// Index reading
			IndexTupleType temp_index_tuple;
			if (!read_index(tx_context, key, temp_index_tuple)) { return nullptr; }
			// Read timestamp before copying data.
			// Correspondingly, we will write data before write timestamp in write phase.
			// Then, if we read the old data, the timestamp should be old, too, which will incur validation failure.
			void *data_ptr = tx_context.access_write(key, temp_index_tuple, size, offset);
			return data_ptr;
		}

		/*!
		 * @brief Insert operation
		 * @param tx_context Context of transaction
		 * @param key Abstract key of data tuple
		 * @param row Data to overwrite(update), which should have been moved by corresponding offset
		 * @param size Size to update
		 * @return If there has been a tuple with the same key, return false
		 */
		bool insert(Context &tx_context, const AbKeyType &key, const void *row, uint32_t size) {
			// Index reading
			IndexTupleType temp_index_tuple;
			if (read_index(tx_context, key, temp_index_tuple)) { return false; }
			// There is no such tuple reference to get from index
			return tx_context.access_insert(key, row, size);
		}

		/*!
		 * @brief Remove operation
		 * @param tx_context Context of transaction
		 * @param key Abstract key of data tuple
		 * @return If there isn't a tuple with the same key, return falseI
		 */
		bool remove(Context &tx_context, const AbKeyType &key) {
			// Index reading
			IndexTupleType temp_index_tuple;
			if (!read_index(tx_context, key, temp_index_tuple)) { return false; }
			// There is no such tuple reference to get from index
			return tx_context.access_delete(key, temp_index_tuple);
		}

	public:
		/*
		 * Interface for transaction manager
		 */

		std::tuple<bool, bool, int> validate(Context &tx_context) {
			tx_context.message_.start_validate();
			tx_context.status_ = Context::Status::Validating;

			auto &read_set   = tx_context.read_set_;
			auto &write_set  = tx_context.write_set_;
			auto &insert_set = tx_context.insert_set_;

			bool has_update = !write_set.empty();
			bool has_insert = !insert_set.empty();
			bool has_write  = has_update || has_insert;

			bool success_validate = true;
			int lock_num = 0;

			if (has_write) {
				// According to the comparison rule, write tx should be ahead of read tx with the same key
				std::sort(write_set.begin(), write_set.end());

				// Validate write set
				for (auto &entry: write_set) {
					IndexTupleType &origin_tuple = entry.tuple;
					// Avoid locking the same tuple repeatedly
					++lock_num;
					while (!origin_tuple.try_lock_write()) {
						assist();
						std::this_thread::yield();
					}
					if (entry.wts != origin_tuple.get_wts_ref().load(std::memory_order_acquire)) {
						success_validate = false;
						break;
					}
				}

				// There is possibly the same tuple in read and write set if read set occur before write set.
				// But it is all right because we deal with read set firstly.
				// Validate read set
				if (success_validate) {
					for (auto &entry: read_set) {
						IndexTupleType &origin_tuple = entry.tuple;
						if (!origin_tuple.try_lock_write()) {
							// Avoid repeated tuple in a read set and a write set
							if (!tx_context.look_up_write_set(entry.key)) {
								success_validate = false;
								break;
							}
						}
						else {
							origin_tuple.unlock_write();
						}


						uint64_t wts = origin_tuple.get_wts_ref().load(std::memory_order::acquire);
						if (entry.wts != wts) {
							success_validate = false;
							break;
						}
					}
				}
			}

			tx_context.message_.end_validate();
			return { success_validate, has_write, lock_num };
		}

		void persist(Context &tx_context) {
			tx_context.commit_ts_ = get_new_wts();
			// Persist logs
			write_log(tx_context);
			// Update index
			update_index(tx_context);
		}

		void assist() {
			// Check whether there is any delegated task.
			Context *delegate_context_ptr_ = delegate_context_array_[thread::get_tid()].load(std::memory_order_acquire);
			if (delegate_context_ptr_ != nullptr) { // Has delegated task
				persist(*delegate_context_ptr_);
				delegate_context_array_[thread::get_tid()].store(nullptr, std::memory_order_release);
			}
		}

		/*!
		 * @brief Validation and commitment
		 * @param tx_context Context of transaction
		 * @return s validation and commitment successful.
		 */
		bool commit(Context &tx_context) {
			tx_context.message_.end_running();
			tx_context.message_.start_commit();

			tx_context.status_ = Context::Status::Committing;

			auto [success_validate, has_write, lock_num] = validate(tx_context);

			if (has_write) {
				if (success_validate) {
					uint32_t delegate_context_idx = delegate_idx_array_[thread::get_tid()];
					if (delegate_context_idx != thread::get_tid()) {
						// Delegate context to local node
						delegate_context_array_[delegate_context_idx].store(&tx_context);
						auto start_time = std::chrono::steady_clock::now();
						do {
							auto end_time = std::chrono::steady_clock::now();
							if (end_time - start_time > std::chrono::milliseconds(200)) { break; }

							thread::pause();
						} while (delegate_context_array_[delegate_context_idx].load(std::memory_order_acquire) != nullptr);
					}
					else {
						persist(tx_context);
					}
				}

				// Unlock all locked tuples.
				for (int i = 0; i < lock_num; ++i) {
					tx_context.write_set_[i].tuple.unlock_write();
				}
			}

			tx_context.message_.end_commit();
			tx_context.message_.end_total();

			assist();

			return success_validate;
		}

		/*!
		 * @brief Abortion and clean temp info
		 * @param tx_context Context of transaction
		 * @return Always true
		 */
		bool abort(Context &tx_context) {
			get_thread_message().abort_transaction();
			return clean_up(tx_context);
		}

		/*!
		 * @brief Reset context for another execution
		 * @param tx_context Context of transaction
		 * @return Always true
		 */
		bool reset(Context &tx_context) {
			tx_context.clear();
			init_tx(tx_context);
			return true;
		}

	private: // Assist Function

		/*!
		 * @brief Write log
		 * @param tx_context Context of transaction
		 */
		void write_log(Context &tx_context) {
			tx_context.message_.start_persist_log();

			auto &write_set    = tx_context.write_set_;
			auto &insert_set   = tx_context.insert_set_;
			uint64_t commit_ts = tx_context.commit_ts_;

			// ---- Write log
			// Start
			log_persist_.add_start_log(commit_ts);
			// Insert
			for (const InsertEntryType &entry: insert_set) {
				log_persist_.add_insert_log(commit_ts, entry.key, entry.data_ptr, entry.size);
			}
			// Write
			for (const WriteEntryType &entry: write_set) {
				if (entry.type == TxType::Write) {
					log_persist_.add_update_log(commit_ts, entry.key, entry.data_ptr, entry.size, entry.offset);
				}
				else {
					log_persist_.add_delete_log(commit_ts, entry.key);
				}
			}
			// Commit
			log_persist_.add_commit_log(commit_ts);

			tx_context.message_.end_persist_log();
		}

		/*!
		 * @brief Read content from index
		 * @param tx_context Context of transaction
		 * @param key Abstract key of data tuple
		 * @param index_tuple Index tuple read
		 * @return Whether reading successes
		 */
		bool read_index(Context &tx_context, const AbKeyType &key, IndexTupleType &index_tuple) {
			tx_context.message_.start_index();
			bool res = storage_manager_ptr_->read_data_index_tuple(key.type_, key.logic_key_, index_tuple);
			tx_context.message_.end_index();
			return res;
		}

		//! @brief Update index including Update/Insert/Delete
		void update_index(Context &tx_context) {
			tx_context.message_.start_persist_data();

			auto &write_set    = tx_context.write_set_;
			auto &insert_set   = tx_context.insert_set_;
			uint64_t commit_ts = tx_context.commit_ts_;

			// For every entry in insert set, insert tuple
			for (const InsertEntryType &entry: insert_set) {
				auto key           = entry.key;
				auto size          = entry.size;

				// Make header of data
				auto [header_ptr, data_ptr] = storage_manager_ptr_->allocate_data_and_header(key.type_);
				new(header_ptr) DataTupleHeaderType(commit_ts);
				// Make index tuple of data and connect it with data header.
				IndexTupleType index_tuple(key.type_, entry.size, header_ptr, data_ptr);
				// Copy data
				index_tuple.set_data(entry.data_ptr, size, 0);
				// Insert index tuple
				storage_manager_ptr_->add_data_index_tuple(key.type_, key.logic_key_, index_tuple);
				storage_manager_ptr_->pwb_range(header_ptr, sizeof(DataTupleHeaderType));
				storage_manager_ptr_->pwb_range(data_ptr, size);
			}

			// For every entry in write set, update or delete tuple
			for (const WriteEntryType &entry: write_set) {
				auto &origin_tuple = entry.tuple;

				if (entry.type == TxType::Write) [[likely]] {
					auto size          = entry.size;
					auto offset        = entry.offset;
					// Write data before write timestamp in write phase.
					// Correspondingly, we will read timestamp before copying data.
					// Then, if we read the old data, the timestamp should be old, too, which will incur validation failure.
					origin_tuple.set_data(entry.data_ptr, size, offset);
					storage_manager_ptr_->pwb_range(
							static_cast<uint8_t *>(origin_tuple.get_data_ptr()) + offset,
							size
					);
					storage_manager_ptr_->fence();
					origin_tuple.get_wts_ref().store(commit_ts, std::memory_order_release);

					break;
				}
				else if (entry.type == TxType::Delete) {
					const AbKeyType &key = entry.key;

					storage_manager_ptr_->deallocate_data_and_header(key.type_, origin_tuple.get_data_ptr());
					storage_manager_ptr_->delete_data_index_tuple(key.type_, key.logic_key_);
					break;
				}
			}

			storage_manager_ptr_->fence();

			tx_context.message_.end_persist_data();
		}
	};
}
