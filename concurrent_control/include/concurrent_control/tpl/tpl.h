/*
 * @author: BL-GS 
 * @date:   2023/1/2
 */

#pragma once

#include <util/latency_counter.h>
#include <thread/thread.h>

#include <recovery/recovery.h>
#include <concurrent_control/abstract_concurrent_control.h>

#include <concurrent_control/tpl/data_tuple.h>
#include <concurrent_control/tpl/tx_context.h>
#include <concurrent_control/tpl/executor.h>

namespace cc {

	namespace tpl {

		template<class WorkloadType>
		struct TPLBasic {
		public:
			using AbKeyType           = WorkloadType::AbKeyType;
			static_assert(AbstractKeyConcept<AbKeyType>);

			using KeyType             = AbKeyType::MainKeyType;
			using DataTupleHeaderType = DataTupleHeader;
			using IndexTupleType      = IndexTuple;
		};


		template<class WorkloadType, class StorageManager>
			requires StorageManagerConcept<StorageManager>
		class TPL {
		public:
			using Self                = TPL<WorkloadType, StorageManager>;

			using AbKeyType           = WorkloadType::AbKeyType;
			using KeyType             = AbKeyType::MainKeyType;
			using Transaction         = WorkloadType::Transaction;

			using DataTupleHeaderType = DataTupleHeader;
			using IndexTupleType      = IndexTuple;

			using Context             = TxContext<AbKeyType>;
			using AccessEntryType     = Context::Entry;

			using ExecutorType        = Executor<Self, AbKeyType>;
			using RecoveryType        = recovery::RecoveryManager<recovery::RecoveryType::NoReserve, StorageManager, AbKeyType>::Recovery;

			static_assert(AbstractKeyConcept<AbKeyType>);
			static_assert(ExecutorConcept<ExecutorType>);

			friend Context;

			friend ExecutorType;

		public:
			StorageManager *storage_manager_ptr_;

			RecoveryType recovery_manager_;

		public: // Class Property

			explicit TPL(StorageManager *storage_manager_ptr):
					storage_manager_ptr_(storage_manager_ptr),
					recovery_manager_(storage_manager_ptr_) {
				storage_manager_ptr_->register_data_deallocate_func([&](const IndexTupleType &index_tuple) {
					storage_manager_ptr_->deallocate_data_and_header(index_tuple.get_data_type(),
													  index_tuple.get_data_header_ptr(),
													  index_tuple.get_data_ptr());
				});
			}

			~TPL() {
				get_concurrent_control_message().clear_up();
			}

			void flush_all_works() {
				storage_manager_ptr_->fence();
			}

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

			/*!
			 * @brief Initialize context of transaction
			 * @param tx_context Context of transaction
			 * @return Always true
			 */
			bool init_tx(Context &tx_context) {
				get_thread_message().start_transaction();
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
				tx_context.clear();
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

			/*!
			 * @brief Validation and commitment
			 * @param tx_context Context of transaction
			 * @return s validation and commitment successful.
			 */
			bool commit(Context &tx_context) {
				tx_context.message_.end_running();
				tx_context.message_.start_commit();
				tx_context.status_ = Context::Status::Committing;

				// Persist logs
				if (tx_context.log_amount_ > 0) {
					auto log_space = write_log(tx_context);
					// Update index
					update_index(tx_context);
					recovery_manager_.deallocate_log_space(log_space);
				}

				tx_context.message_.end_commit();

				return true;
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
			auto write_log(Context &tx_context) {
				tx_context.message_.start_persist_log();

				auto &write_set    = tx_context.write_set_;
				auto &insert_set   = tx_context.insert_set_;

				auto [log_amount, log_size] = tx_context.get_log_amount_size();
				auto log_space = recovery_manager_.allocate_log_space(log_amount, log_size);

				// ---- Write log
				// Start
				recovery_manager_.start(log_space, 0);
				// Insert
				for (AccessEntryType &entry: insert_set) {
					recovery_manager_.insert_item(log_space, 0, entry.key, entry.data_ptr, entry.size);
				}
				// Write
				for (AccessEntryType &entry: write_set) {
					if (entry.type == TxType::Write) {
						recovery_manager_.update_item(log_space, 0,
													  entry.key,
													  static_cast<uint8_t *>(entry.data_ptr) + entry.offset,
													  entry.size);
					}
					else if (entry.type == TxType::Delete) {
						recovery_manager_.delete_item(log_space, 0, entry.key);
					}
				}
				// Commit
				recovery_manager_.commit(log_space, 0);
				storage_manager_ptr_->fence();

				tx_context.message_.end_persist_log();

				return log_space;
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

				// For every entry in insert set, insert tuple
				for (AccessEntryType &entry: insert_set) {
					auto key           = entry.key;
					auto size          = entry.size;

					// Make header of data
					auto [header_ptr, data_ptr] = storage_manager_ptr_->allocate_data_and_header(key.type_);
					new(header_ptr) DataTupleHeaderType();
					// Make index tuple of data and connect it with data header.
					IndexTupleType index_tuple(key.type_, size, header_ptr, data_ptr);
					// Copy data
					index_tuple.set_data(entry.data_ptr, size, 0);
					// Insert index tuple
					storage_manager_ptr_->add_data_index_tuple(key.type_, key.logic_key_, index_tuple);
					storage_manager_ptr_->pwb_range(header_ptr, sizeof(DataTupleHeaderType));
					storage_manager_ptr_->pwb_range(data_ptr, size);
				}

				// For every entry in write set, update or delete tuple
				for (AccessEntryType &entry: write_set) {
					auto key           = entry.key;
					auto size          = entry.size;
					auto offset        = entry.offset;
					auto &origin_tuple = entry.tuple;

					switch (entry.type) {
						case TxType::Write:
							origin_tuple.set_data(entry.data_ptr, size, offset);
							storage_manager_ptr_->pwb_range(static_cast<uint8_t *>(origin_tuple.get_data_ptr()) + offset, size);
							break;

						case TxType::Delete:
							storage_manager_ptr_->deallocate_data_and_header(key.type_, origin_tuple.get_data_header_ptr(), origin_tuple.get_data_ptr());
							storage_manager_ptr_->delete_data_index_tuple(key.type_, key.logic_key_);
							break;

						default:
							break;
					}
				}

				storage_manager_ptr_->fence();
				tx_context.message_.end_persist_data();
			}
		};
	}
}
