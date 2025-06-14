/*
 * @author: BL-GS 
 * @date:   2023/1/2
 */

#pragma once

#include <util/latency_counter.h>
#include <spdlog/spdlog.h>
#include <thread/thread.h>

#include <concurrent_control/abstract_concurrent_control.h>

#include <concurrent_control/mvcc/data_tuple.h>
#include <concurrent_control/mvcc/tx_context.h>
#include <concurrent_control/mvcc/executor.h>

namespace cc {

	namespace mvcc {

		template<class WorkloadType>
		struct MVCCBasic {
		public:
			using AbKeyType           = WorkloadType::AbKeyType;
			static_assert(AbstractKeyConcept<AbKeyType>);

			using KeyType             = AbKeyType::MainKeyType;
			using DataTupleHeaderType = DataHeader;
			using IndexTupleType      = IndexTuple;
			using VersionType         = WriteHis;
		};

		template<class WorkloadType, class StorageManager>
			requires MVStorageManagerConcept<StorageManager>
		class MVCC {
		public:
			static constexpr uint32_t ALL_FIELD  = WorkloadType::ALL_FIELD;
			static constexpr uint32_t MAX_THREAD = thread::get_max_tid();

		public:
			using Self        = MVCC<WorkloadType, StorageManager>;

			using AbKeyType           = WorkloadType::AbKeyType;
			static_assert(AbstractKeyConcept<AbKeyType>);

			using KeyType             = AbKeyType::MainKeyType;
			using Transaction = WorkloadType::Transaction;

			using DataTupleHeaderType = DataHeader;
			using IndexTupleType      = IndexTuple;
			using VersionType         = WriteHis;

			using Context             = TxContext<KeyType>;
			using AccessEntryType     = Context::Entry;
			using ExecutorType        = Executor<Self, KeyType>;
			static_assert(ExecutorConcept<ExecutorType>);

			using RecoveryType = recovery::RecoveryManager<recovery::RecoveryType::NoReserve, StorageManager, AbKeyType>::Recovery;

			friend Context;

			friend ExecutorType;

		public:
			/// Global time counter
			std::atomic<uint64_t> time_counter;

			StorageManager *storage_manager_;

			RecoveryType recovery_manager_;

			/// Register for threads' active transaction ID, which will be multi-read and one-write.
			std::atomic<uint64_t> min_ts_register_[MAX_THREAD];

			struct ThreadTsOwner {
				std::function<void()> final_work_;
				~ThreadTsOwner() { final_work_(); }
			};

		public: // Class Property

			explicit MVCC(StorageManager *storage_manager_ptr) :
					time_counter{0}, storage_manager_(storage_manager_ptr), recovery_manager_(storage_manager_ptr) {
				spdlog::warn("MVCC doesn't support multi-transaction to single-thread binding.");

				storage_manager_->register_data_deallocate_func([&](const IndexTupleType &index_tuple){
					DataTupleHeaderType *header_ptr = index_tuple.get_data_header_ptr();
					// Clear all versions
					header_ptr->clear(0, [&, data_type = index_tuple.get_data_type()](VersionType *version_ptr){
						storage_manager_->deallocate_version(data_type, version_ptr);
					});
					// Deallocate data tuple
					DataTupleHeaderType *data_header_ptr = index_tuple.get_data_header_ptr();
					storage_manager_->deallocate_header(index_tuple.get_data_type(), data_header_ptr);
				});

				for (auto &ts: min_ts_register_) {
					ts.store(std::numeric_limits<uint64_t>::max());
				}

			}

			~MVCC() {
				get_concurrent_control_message().clear_up();
			}

			void flush_all_works() {
				storage_manager_->fence();
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
				thread_local ThreadTsOwner ts_owner_([&](){
					min_ts_register_[thread::get_tid()].store(std::numeric_limits<uint64_t>::max());
				});

				get_thread_message().start_transaction();
				tx_context.start_ts_ = ++time_counter;

				// TODO: It is not good for group transaction process
				min_ts_register_[thread::get_tid()].store(tx_context.start_ts_, std::memory_order::release);

				tx_context.message_.start_running();
				return true;
			}

			/*!
			 * @brief Clear all temp data left by 'run_transaction'
			 * @param tx_context Context of transaction
			 * @return Always true
			 */
			bool clean_up(Context &tx_context) {
				get_thread_message().submit_time(tx_context.message_);
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
				DataTupleHeaderType *header_ptr = temp_index_tuple.get_data_header_ptr();
				VersionType *version_ptr = header_ptr->get_version(tx_context.start_ts_);
				data_ptr = version_ptr->get_data_ptr();
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
				tx_context.message_.start_validate();

				auto &access_set = tx_context.access_set_;
				auto &insert_set = tx_context.insert_set_;

				bool success_validate = true;
				int lock_num = 0;

				bool has_write = (!insert_set.empty()) || (!access_set.empty());

				// Validate all time stamp
				if (has_write) {
					std::sort(access_set.begin(), access_set.end());

					for (auto &entry: access_set) {
						IndexTupleType &origin_tuple = entry.tuple;
						DataTupleHeaderType *data_header_ptr = origin_tuple.get_data_header_ptr();

						if (!data_header_ptr->validate_update(tx_context.start_ts_)) {
							success_validate = false;
							break;
						}

						data_header_ptr->lock_write();
						++lock_num;

						if (!data_header_ptr->validate_update(tx_context.start_ts_)) {
							success_validate = false;
							break;
						}
					}
				}

				tx_context.message_.end_validate();

				if (success_validate && has_write) {
					// If success validating, then update all time stamp and update index.
					// Persist logs
					auto log_space = write_log(tx_context);
					// Update index
					update_index(tx_context);

					recovery_manager_.deallocate_log_space(log_space);
				}

				// Unlock all locked tuples.
				if (has_write) {
					for (int i = 0; i < lock_num; ++i) {
						IndexTupleType &origin_tuple = access_set[i].tuple;
						DataTupleHeaderType *data_header_ptr = origin_tuple.get_data_header_ptr();
						data_header_ptr->unlock_write();
					}

					uint64_t min_ts = get_min_ts();
					for (auto &entry: access_set) {
						IndexTupleType &origin_tuple = entry.tuple;
						reclaim_versions(min_ts, origin_tuple);
					}
				}

				tx_context.message_.end_commit();

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
			auto write_log(Context &tx_context) {
				tx_context.message_.start_persist_log();

				auto &access_set = tx_context.access_set_;
				auto &insert_set = tx_context.insert_set_;

				auto [log_amount, log_size] = tx_context.get_log_amount_size();
				auto log_space = recovery_manager_.allocate_log_space(log_amount, log_size);

				// ---- Write log
				// Start
				recovery_manager_.start(log_space, tx_context.start_ts_);
				// Insert
				for (AccessEntryType &entry: insert_set) {
					recovery_manager_.insert_item(log_space, tx_context.start_ts_, entry.key, entry.data_ptr, entry.size);
				}
				// Write
				for (AccessEntryType &entry: access_set) {
					if (entry.type == TxType::Write) {
						recovery_manager_.update_item(log_space, tx_context.start_ts_,
						                              entry.key,
						                              static_cast<uint8_t *>(entry.data_ptr) + entry.offset,
						                              entry.size);
					}
					else if (entry.type == TxType::Delete) {
						recovery_manager_.delete_item(log_space, tx_context.start_ts_, entry.key);
					}
				}
				// Commit
				recovery_manager_.commit(log_space, tx_context.start_ts_);
				storage_manager_->fence();

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
				bool res = storage_manager_->read_data_index_tuple(key.type_, key.logic_key_, index_tuple);
				tx_context.message_.end_index();
				return res;
			}

			//! @brief Update index including Update/Insert/Delete
			void update_index(Context &tx_context) {
				tx_context.message_.start_persist_data();

				auto &access_set   = tx_context.access_set_;
				auto &insert_set   = tx_context.insert_set_;
				uint64_t commit_ts = tx_context.start_ts_;

				// For every entry in insert set, insert tuple
				for (AccessEntryType &entry: insert_set) {
					auto key           = entry.key;
					auto size          = entry.size;

					// Create a new version
					VersionType *new_version_ptr = storage_manager_->allocate_version(key.type_);
					new(new_version_ptr) VersionType();
					std::memcpy(new_version_ptr->get_data_ptr(), entry.data_ptr, size);

					// Make header of data
					auto *header_ptr = storage_manager_->allocate_header(key.type_);
					new(header_ptr) DataTupleHeaderType(commit_ts, size, new_version_ptr);

					// Make index tuple of data and connect it with data header.
					IndexTupleType index_tuple {
						.data_type_       = key.type_,
						.data_header_ptr_ = header_ptr
					};

					// Insert index tuple
					storage_manager_->add_data_index_tuple(key.type_, key.logic_key_, index_tuple);
					storage_manager_->pwb_range(header_ptr, sizeof(DataTupleHeaderType));
					storage_manager_->pwb_range(new_version_ptr, sizeof(VersionType) + size);
				}

				// For every entry in write set, update or delete tuple
				for (AccessEntryType &entry: access_set) {
					auto key           = entry.key;
					auto &origin_tuple = entry.tuple;
					auto entire_size   = entry.tuple.get_data_size();

					if (entry.type == TxType::Write) {
						// Create a new version
						VersionType *new_version_ptr = storage_manager_->allocate_version(key.type_);
						new (new_version_ptr) VersionType();
						std::memcpy(new_version_ptr->get_data_ptr(), entry.data_ptr, entire_size);

						DataTupleHeaderType *data_header_ptr = entry.tuple.get_data_header_ptr();
						data_header_ptr->add_version(new_version_ptr, commit_ts);

						storage_manager_->pwb_range(new_version_ptr, sizeof(VersionType) + entire_size);
					}
					else if (entry.type == TxType::Delete) {
						DataTupleHeaderType *data_header_ptr = entry.tuple.get_data_header_ptr();
						data_header_ptr->delete_ts_ = tx_context.start_ts_;
						storage_manager_->pwb_range(&(data_header_ptr->delete_ts_));
					}
				}

				storage_manager_->fence();

				tx_context.message_.end_persist_data();
			}

			uint64_t get_min_ts() {
				// Get min ts of all live threads
				uint64_t min_ts = std::numeric_limits<uint64_t>::max();
				for (auto &ts: min_ts_register_) {
					auto temp_ts = ts.load(std::memory_order::acquire);
					if (temp_ts < min_ts) { min_ts = temp_ts; }
				}
				return (min_ts == std::numeric_limits<uint64_t>::max()) ? 0 : min_ts;
			}

			void reclaim_versions(int64_t min_ts, IndexTupleType &index_tuple) {
				DataTupleHeaderType *header_ptr = index_tuple.get_data_header_ptr();

				if (!header_ptr->need_reclaim(min_ts)) { return; }

				if (header_ptr->try_lock_write()) {
					header_ptr->clear(min_ts, [&, data_type = index_tuple.get_data_type()](VersionType *version_ptr){
						storage_manager_->deallocate_version(data_type, version_ptr);
					});
					header_ptr->unlock_write();
				}
			}
		};

	}
}
