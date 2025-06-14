/*
 * @author: BL-GS 
 * @date:   2023/2/18
 */

#pragma once

#include <util/latency_counter.h>
#include <thread/thread.h>
#include <log_manager/abstract_log_manager.h>

#include <concurrent_control/abstract_concurrent_control.h>
#include <concurrent_control/romulus_log/rwlock.h>
#include <concurrent_control/romulus_log/log_tuple.h>
#include <concurrent_control/romulus_log/data_tuple.h>
#include <concurrent_control/romulus_log/tx_context.h>
#include <concurrent_control/romulus_log/executor.h>

namespace cc {

	namespace romulus {

		template<class WorkloadType>
		struct RomulusLogBasic {
		public:
			using KeyType   = WorkloadType::KeyType;
			using FieldType = WorkloadType::FieldType;
			using ValueType = WorkloadType::ValueType;
			using RowType   = WorkloadType::RowType;

			using TupleType = DataTuple<RowType>;

			using DataType       = TupleType;
			using LogType        = TupleType;
			using IndexTupleType = IndexTuple<RowType>;
			using VersionType    = TupleType;
		};


		template<class WorkloadType, class StorageManager>
			requires StorageManagerConcept<StorageManager>
		class RomulusLog {
		public:
			using Self        = RomulusLog<WorkloadType, StorageManager>;

			using KeyType     = WorkloadType::KeyType;
			using FieldType   = WorkloadType::FieldType;
			using ValueType   = WorkloadType::ValueType;
			using RowType     = WorkloadType::RowType;
			using Transaction = WorkloadType::Transaction;

			using TupleType      = DataTuple<RowType>;
			using LogType        = DataTuple<RowType>;
			using IndexTupleType = IndexTuple<RowType>;

			using Context      = TxContext;
			using ExecutorType = Executor<Self, KeyType, FieldType, RowType>;

			static constexpr uint32_t ALL_FIELD = WorkloadType::ALL_FIELD;

			static constexpr uint32_t CHUNK_SIZE = 1024;

			static constexpr uint32_t MAX_THREAD_NUM = thread::get_max_tid();

			enum class State {
				IDLE,
				MUTATING,
				COPYING
			};

		public:
			StorageManager *storage_manager_;

		private:
			// Stuff used by the Flat Combining mechanism
			static constexpr uint32_t CLPAD = 128 / sizeof(uintptr_t);

			alignas(128) std::atomic<std::function<void()>*> fc[MAX_THREAD_NUM * CLPAD]; // array of atomic pointers to functions

			std::function<void()>* lfc[MAX_THREAD_NUM];

			CRWWPSpinLock rwlock;

			// There is always at least one (empty) chunk in the log, it's the head
			std::atomic<State> state;

			LogChunk* log_head;

			LogChunk* log_tail;

		public: // Class Property

			explicit RomulusLog(StorageManager *storage_manager):
				storage_manager_ (storage_manager),
				state(State::IDLE),
				log_head(new LogChunk), log_tail(log_head) {
				for (uint32_t i = 0; i < MAX_THREAD_NUM; ++i) {
					fc[i * CLPAD].store(nullptr, std::memory_order::relaxed);
					lfc[i] = nullptr;
				}

				storage_manager_->register_data_deallocate_func([&](const IndexTupleType &index_tuple) {
					storage_manager_->deallocate_data(index_tuple.get_data_ptr());
				});
			}

			~RomulusLog() {
				get_concurrent_control_message().clear_up();
			}

			ConcurrentControlMessage &get_concurrent_control_message() {
				return ConcurrentControlMessage::get_summary_message();
			}

		private:
			ConcurrentControlMessage &get_thread_message() {
				return ConcurrentControlMessage::get_thread_message();
			}

		public:

			//! @brief Run transaction
			//! @param tx Transaction
			//! @return Is running successful
			bool run_transaction(auto &&tx) {
				Context thread_context{};

				get_thread_message().start_transaction();

				// Transaction running
				thread_context.message_.start_running();
				bool need_commit = run_tx(tx, thread_context);
				thread_context.message_.end_running();
				if (need_commit) {
					commit(thread_context);
				}
				clear(thread_context);
				return true;
			}

		public:
			//! @brief Run transaction
			//! @param tx Transaction to execute
			//! @return
			bool run_tx(auto &tx, Context &thread_context) {
				// For transaction, we should build four lambda function for it.
				// As for the reason to take it apart, it is my intention to simplify the callback function,
				// which will be done once the lambada ended.

				auto read_func = [&](KeyType key, FieldType field, void *output_ptr) -> bool {
					IndexTupleType temp_index_tuple;
					// Get tuple reference from index
					if (!read_index(thread_context, key, temp_index_tuple)) { return false; }
					copy_data_from_index_tuple(thread_context, temp_index_tuple, output_ptr);
					// Add it into read set
					return true;
				};

				auto write_func = [&](KeyType key, FieldType field, RowType &&value) -> bool {
					IndexTupleType temp_index_tuple;
					// Get tuple reference from index
					if (!read_index(thread_context, key, temp_index_tuple)) { return false; }

					// Write directly
					if (field == ALL_FIELD) {
						temp_index_tuple.set_data(value);
					}
					else {
						temp_index_tuple.get_data().set_element(field, value.get_element(field));
					}

					add_to_log(temp_index_tuple.get_data_ptr());
					return true;
				};

				auto scan_func = [&](KeyType key, FieldType field, uint32_t scan_length, void *output_ptr) -> bool {
					IndexTupleType temp_index_tuple;

					for (uint32_t i = 0; i < scan_length; ++i) {
						// Get tuple reference from index
						// Get tuple reference from index
						if (!read_index(thread_context, key, temp_index_tuple)) { return false; }
						copy_data_from_index_tuple(thread_context, temp_index_tuple, output_ptr);
						output_ptr = reinterpret_cast<char *>(output_ptr) + sizeof(RowType);
					}
					return true;
				};

				auto insert_func = [&](KeyType key, FieldType field, RowType &&value) -> bool {
					// There is no such tuple reference to get from index
					TupleType *new_data = storage_manager_->allocate_data(value);
					bool res = storage_manager_->add_data(key, IndexTupleType{new_data});
					if (!res) { assert(false); }
					return true;
				};

				uint32_t tid = thread::get_tid();

				if (tx.is_only_read()) {
					rwlock.shared_lock(tid);
					tx.run(read_func, write_func, scan_func, insert_func);
					rwlock.shared_unlock(tid);

					return false;
				}
				else {
					std::function<void()> myfunc = [&](){ tx.run(read_func, write_func, scan_func, insert_func); };

					// Add our mutation to the array of flat combining
					fc[tid * CLPAD].store(&myfunc, std::memory_order_release);
					// Lock writersMutex
					while (true) {
						if (rwlock.try_exclusive_lock()) break;
						// Check if another thread executed my mutation
						if (fc[tid * CLPAD].load(std::memory_order_acquire) == nullptr) { return false; }
						std::this_thread::yield();
					}

					bool something_to_do = false;
					// Save a local copy of the flat combining array

					for (uint32_t i = 0; i < MAX_THREAD_NUM; i++) {
						lfc[i] = fc[i * CLPAD].load(std::memory_order_acquire);
						if (lfc[i] != nullptr) { something_to_do = true; }
					}
					// Check if there is at least one operation to apply
					if (!something_to_do) {
						rwlock.exclusive_unlock();
						return false;
					}

					state.store(State::MUTATING, std::memory_order_relaxed);
					storage_manager_->pwb_range(&state, sizeof(state));
					storage_manager_->fence();

					rwlock.wait_for_readers();

					thread_context.message_.start_persist_data();
					// Apply all mutativeFunc
					for (auto & f : lfc) {
						if (f != nullptr) { (*f)(); }
					}
					apply_main_pwb();
					thread_context.message_.end_persist_data();
				}

				return true;
			}

			bool commit(Context &thread_context) {
				thread_context.message_.start_commit();

				state.store(State::COPYING, std::memory_order_relaxed);
				storage_manager_->pwb_range(&state, sizeof(state));
				storage_manager_->fence();

				// After changing state to COPYING all applied mutativeFunc are visible and persisted
				for (uint32_t i = 0; i < MAX_THREAD_NUM; i++) {
					if (lfc[i] == nullptr) continue;
					fc[i * CLPAD].store(nullptr, std::memory_order_release);
				}

				apply_log_pwb();

				clear_log();
				storage_manager_->fence();
				state.store(State::IDLE, std::memory_order_relaxed);
				rwlock.exclusive_unlock();

				thread_context.message_.end_commit();
				return true;
			}

			bool clear(Context &thread_context) {
				get_thread_message().submit_time(thread_context.message_);
				return true;
			}


		private:
			/*
	         * Called to make every store persistent on main and back region
	         */
			void apply_main_pwb() {
				// Apply the log to the instance on 'to_addr', copying data from the instance at 'from_addr'
				LogChunk* chunk = log_head;
				while (chunk != nullptr) {
					for (uint32_t i = 0; i < chunk->num_entries; i++) {
						LogEntry& e = chunk->entries[i];
						storage_manager_->pwb_range(static_cast<TupleType *>(e.tuple_ptr)->get_main_data_ptr(), sizeof(RowType));
					}
					chunk = chunk->next;
				}
			}

			void apply_back_pwb() {
				// Apply the log to the instance on 'to_addr', copying data from the instance at 'from_addr'
				LogChunk* chunk = log_head;
				while (chunk != nullptr) {
					for (uint32_t i = 0; i < chunk->num_entries; i++) {
						LogEntry& e = chunk->entries[i];
						storage_manager_->pwb_range(static_cast<TupleType *>(e.tuple_ptr)->get_back_data_ptr(), sizeof(RowType));
					}
					chunk = chunk->next;
				}
			}

			/*
		     * Called at the end of a transaction to replicate the mutations on "back",
		     * or when abort_transaction() is called by the user, to rollback the
		     * mutations on "main".
		     * Deletes the log as it is being applied.
		     */
			void apply_log_pwb(Context &thread_context) {
				thread_context.message_.start_persist_log();
				// Apply the log to the instance on 'to_addr', copying data from the instance at 'from_addr'
				LogChunk* chunk = log_head;
				while (chunk != nullptr) {
					for (uint32_t i = 0; i < chunk->num_entries; i++) {
						LogEntry& e = chunk->entries[i];
						static_cast<TupleType *>(e.tuple_ptr)->back_up();
						storage_manager_->pwb_range(static_cast<TupleType *>(e.tuple_ptr)->get_back_data_ptr(), sizeof(RowType));
					}
					chunk = chunk->next;
				}
				storage_manager_->fence();
				thread_context.message_.end_persist_log();
			}

			void clear_log() {
				LogChunk* chunk = log_head->next;
				while (chunk != nullptr) {
					LogChunk* next = chunk->next;
					delete chunk;
					chunk = next;
				}
				// Clear the log, leaving one chunk for next transaction, with zero'ed entries
				log_tail = log_head;
				log_head->num_entries = 0;
				log_head->next = nullptr;
			}


			/*
			* Adds to the log the current contents of the memory location starting at
			* 'addr' with a certain 'length' in bytes
			*/
			void add_to_log(TupleType* addr) {
				// Get the current chunk of log and if it is already full then create a new chunk and add the entry there.
				LogChunk* chunk = log_tail;

				if (chunk->num_entries == CHUNK_SIZE) {
					chunk = new LogChunk();
					log_tail->next = chunk;
					log_tail = chunk;
				}
				LogEntry& e = chunk->entries[chunk->num_entries];

				e.tuple_ptr = addr;

				chunk->num_entries++;
			}

			//! @brief Read content from index
			//! @param thread_context
			//! @param key
			//! @param index_tuple Output tuple
			//! @return Is reading successful.
			bool read_index(Context &thread_context, const KeyType &key, IndexTupleType &index_tuple) {
				thread_context.message_.start_index();
				bool res = storage_manager_->read_data(key, index_tuple);
				thread_context.message_.end_index();
				return res;
			}

		};
	}
}
