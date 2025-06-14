/*
 * @author: BL-GS 
 * @date:   2023/1/2
 */

#pragma once

#include <util/latency_counter.h>
#include <spdlog/spdlog.h>
#include <thread/thread.h>

#include <concurrent_control/abstract_concurrent_control.h>

#include <concurrent_control/courier/data_tuple.h>
#include <concurrent_control/courier/tx_context.h>
#include <concurrent_control/courier/executor.h>
#include <concurrent_control/courier/data_persist.h>
#include <concurrent_control/courier/log_persist.h>
#include <concurrent_control/courier/thread_context.h>

namespace cc::courier {

	template<class WorkloadType>
	struct CourierBasic {
	public:
		using AbKeyType           = WorkloadType::AbKeyType;
		static_assert(AbstractKeyConcept<AbKeyType>);

		using KeyType             = AbKeyType::MainKeyType;
		using DataTupleHeaderType = DataTupleHeader<AbKeyType>;
		using IndexTupleType      = IndexTuple;
	};


	template<class WorkloadType, class StorageManager>
		requires StorageManagerConcept<StorageManager>
	class Courier {
	public:
		using Self                          = Courier<WorkloadType, StorageManager>;

		using AbKeyType                     = WorkloadType::AbKeyType;
		using KeyType                       = AbKeyType::MainKeyType;

		using DataTupleHeaderType        = DataTupleHeader<AbKeyType>;
		using DataTupleVirtualHeaderType = DataTupleVirtualHeader;
		using IndexTupleType             = IndexTuple;

		using Context                       = TxContext<AbKeyType>;
		using ReadEntryType                 = Context::ReadEntry;
		using WriteEntryType                = Context::WriteEntry;

		using ExecutorType        = Executor<Self, AbKeyType>;
		using DataPersistType     = DataPersist<StorageManager, AbKeyType>;
		using LogPersistType      = LogPersist<AbKeyType>;

		static_assert(ExecutorConcept<ExecutorType>);
		static_assert(AbstractKeyConcept<AbKeyType>);

		friend Context;
		friend ExecutorType;

	private:
		static constexpr bool LOCK_RETRY_LIMIT          = true;
		static constexpr uint32_t LOCK_RETRY_LIMIT_NUM  = 2;

	public:
		StorageManager *storage_manager_ptr_;

		// The first to deconstruct because it needs recovery manager to have data written.
		DataPersistType data_persist_;

		LogPersistType log_persist_;

	public: // Class Property

		explicit Courier(StorageManager *storage_manager_ptr):
				storage_manager_ptr_(storage_manager_ptr),
				data_persist_(storage_manager_ptr, &log_persist_),
				log_persist_(storage_manager_ptr->get_log_space_range()) {
			spdlog::warn("Experiment can't make sure that data read is integral when running transaction, "
						"but this inconsistency will be detected when validating.");

			storage_manager_ptr_->register_data_deallocate_func([&](const IndexTupleType &index_tuple) {
				DataTupleVirtualHeaderType *virtual_header_ptr = index_tuple.get_data_header_ptr();
//				Context::deallocate_data_buffer(virtual_header_ptr->get_virtual_data_ptr());

				void *data_ptr = index_tuple.get_origin_data_ptr();
				storage_manager_ptr_->deallocate_data_and_header(index_tuple.get_data_type(), data_ptr);
				// Deallocate virtual header
				delete virtual_header_ptr;
			});
		}

		~Courier() {
			get_concurrent_control_message().clear_up();
		}

		void flush_all_works() {
			data_persist_.flush_all_work();
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
			auto &private_log_space = thread_local_context.log_space;
			if (!private_log_space.has_value()) [[unlikely]] { // If the current thread does not have log space, allocate one
				while (true) {
					private_log_space = log_persist_.allocate_log_space();
					if (private_log_space.has_value()) { break; }
					data_persist_.aid(thread::get_tid());
				}
			}

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
			// Look up the write set
			void *data_ptr = tx_context.look_up_write_set(key);
			if (data_ptr == nullptr) {
				// Index reading
				IndexTupleType temp_index_tuple;
				if (!read_index(tx_context, key, temp_index_tuple)) { return nullptr; }
				// Read timestamp before copying data.
				// Correspondingly, we will write data before write timestamp in write phase.
				// Then, if we read the old data, the timestamp should be old, too, which will incur validation failure.
				data_ptr = tx_context.access_read(key, temp_index_tuple);
			}
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
			if (!read_index(tx_context, key, temp_index_tuple)) [[unlikely]] { return nullptr; }
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
			if (!read_index(tx_context, key, temp_index_tuple)) [[unlikely]] { return nullptr; }
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
			if (read_index(tx_context, key, temp_index_tuple)) [[unlikely]] { return false; }
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
			if (!read_index(tx_context, key, temp_index_tuple)) [[unlikely]] { return false; }
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

			auto &write_set  = tx_context.write_set_;

			bool only_read   = write_set.empty();
			if (only_read) {
				return read_only_commit(tx_context);
			}

			return normal_commit(tx_context);
		}

		bool read_only_commit(Context &tx_context) {
			uint32_t tid = thread::get_tid();
			data_persist_.aid(tid);

			return true;
		}

		bool normal_commit(Context &tx_context) {
			tx_context.message_.start_commit();
			tx_context.message_.start_validate();
			tx_context.status_ = Context::Status::Validating;

			uint32_t tid = thread::get_tid();

			auto &read_set   = tx_context.read_set_;
			auto &write_set  = tx_context.write_set_;

			uint32_t log_size = tx_context.get_log_amount_size();

			auto &private_log_space = thread_local_context.log_space;
			if (!log_persist_.log_space_enough(private_log_space.value(), log_size)) {
				// Submit space of log
				data_persist_.persist_data(tid, private_log_space.value());
				while (true) {
					// Try to allocate log space
					private_log_space = log_persist_.allocate_log_space();
					if (private_log_space.has_value()) { break; }
					// If the current thread is needed for aid, then participate in that.
					data_persist_.aid(tid);
				}
			}

			// According to the comparison rule, write tx should be ahead of read tx with the same key
			std::sort(write_set.begin(), write_set.end());

			bool success_validate = true;
			int lock_num = 0;

			// Validate write set
			for (auto &entry: write_set) {
				if (entry.type == TxType::Insert) [[unlikely]] { continue; }

				IndexTupleType &origin_tuple = entry.tuple;

				if constexpr (LOCK_RETRY_LIMIT) {
					uint32_t retry_times = 0;
					while (origin_tuple.try_lock_write() && ++retry_times != LOCK_RETRY_LIMIT_NUM) {
						thread::pause();
					}

					if (retry_times == LOCK_RETRY_LIMIT_NUM) {
						success_validate = false;
						break;
					}
				}
				else {
					while (origin_tuple.try_lock_write() ) {
						thread::pause();
					}
				}


				// Avoid locking the same tuple repeatedly
				++lock_num;

				if (entry.wts != origin_tuple.get_wts()) {
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
					if (origin_tuple.is_locked_write()) [[unlikely]] {
						// Avoid repeated tuple in a read set and write set
						if (!tx_context.look_up_write_set(entry.key)) {
							success_validate = false;
							break;
						}
					}
					uint64_t wts = origin_tuple.get_wts();
					if (entry.wts != wts) {
						success_validate = false;
						break;
					}
				}
			}

			tx_context.message_.end_validate();
			tx_context.status_ = Context::Status::Committing;

			if (success_validate) {
				// Persist logs
				write_log(tx_context);
				// Update index
				update_index(tx_context);
			}
			// Unlock all locked tuples.
			for (int i = 0; i < lock_num; ++i) { write_set[i].tuple.unlock_write(); }

			// Delay update event and insert event
			if (success_validate) {
				// Commit task of persisting data
				data_persist_.push_context(tid, tx_context);
			}

			data_persist_.aid(tid);

			tx_context.message_.end_commit();
			tx_context.message_.end_total();

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
			tx_context.clear_abort();
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

			LogSpace &log_space = thread_local_context.log_space.value();
			uint8_t *start_ptr = log_space.cur_ptr;
			// ---- Write log
			// Write
			for (const WriteEntryType &entry: write_set) {
				if (entry.type == TxType::Write) [[likely]] {
					log_persist_.add_update_log(log_space, entry.wts,
					                            entry.key,
					                            static_cast<uint8_t *>(entry.data_ptr),
					                            entry.size,
					                            entry.offset);
				}
				else if (entry.type == TxType::Insert) {
					log_persist_.add_insert_log(log_space, entry.wts,
					                            entry.key,
					                            static_cast<uint8_t *>(entry.data_ptr),
					                            entry.size);
				}
				else if (entry.type == TxType::Delete) {
					log_persist_.add_delete_log(log_space, entry.wts,
					                            entry.key);
				}
			}
			// Commit
			log_persist_.add_commit_log(log_space, 0);

			util_mem::clflushopt_range(start_ptr, log_space.cur_ptr - start_ptr);
			storage_manager_ptr_->fence();

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

			// For every entry in write set, update or delete tuple
			for (WriteEntryType &entry: write_set) {
				auto &origin_tuple = entry.tuple;

				if (entry.type == TxType::Write) [[likely]] {
					auto size          = entry.size;
					auto offset        = entry.offset;

					// Write data before write timestamp in write phase.
					// Correspondingly, we will read timestamp before copying data.
					// Then, if we read the old data, the timestamp should be old, too, which will incur validation failure.
					origin_tuple.set_virtual_data(entry.data_ptr, size, offset);
					storage_manager_ptr_->fence();
					origin_tuple.set_wts(origin_tuple.get_wts() + 1);
				}
				else if (entry.type == TxType::Insert) {
					auto key           = entry.key;
					auto size          = entry.size;

					// Make header of data
					auto [header_ptr, data_ptr] = storage_manager_ptr_->allocate_data_and_header(key.type_);
					// Set the data pointer of header as entry in an insert set.
					new(header_ptr) DataTupleHeaderType(key);
					// Allocate new header for data
					auto *virtual_header_ptr = new DataTupleVirtualHeaderType(0, entry.data_ptr);
					// Make index tuple of data and connect it with data header.
					new (&entry.tuple) IndexTupleType(key.type_, size, virtual_header_ptr, data_ptr);
					// Insert index tuple
					storage_manager_ptr_->add_data_index_tuple(key.type_, key.logic_key_, entry.tuple);
					storage_manager_ptr_->pwb_range(header_ptr, sizeof(DataTupleHeaderType));
				}
				else {
					auto key           = entry.key;
//					Context::deallocate_data_buffer(origin_tuple.get_virtual_data_ptr());
					storage_manager_ptr_->deallocate_data_and_header(key.type_, origin_tuple.get_origin_data_ptr());
					storage_manager_ptr_->delete_data_index_tuple(key.type_, key.logic_key_);
				}
			}

			storage_manager_ptr_->fence();

			tx_context.message_.end_persist_data();
		}

	};
}
