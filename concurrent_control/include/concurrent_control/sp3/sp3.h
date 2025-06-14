/*
 * @author: BL-GS 
 * @date:   2023/1/2
 */

#pragma once

#include <tbb/concurrent_unordered_set.h>

#include <util/latency_counter.h>
#include <spdlog/spdlog.h>
#include <thread/thread.h>

#include <concurrent_control/abstract_concurrent_control.h>
#include <concurrent_control/sp3/data_tuple.h>
#include <concurrent_control/sp3/tx_context.h>
#include <concurrent_control/sp3/executor.h>
#include <concurrent_control/sp3/log_persist.h>

namespace cc::sp {

	template<class WorkloadType>
	struct SPBasic {
	public:
		using AbKeyType           = WorkloadType::AbKeyType;
		static_assert(AbstractKeyConcept<AbKeyType>);

		using KeyType             = AbKeyType::MainKeyType;
		using DataTupleHeaderType = DataTupleHeader<AbKeyType>;
		using IndexTupleType      = IndexTuple;
	};

	template<class WorkloadType, class StorageManager>
		requires StorageManagerConcept<StorageManager>
	class SP {
	public:
		using Self                = SP<WorkloadType, StorageManager>;

		using AbKeyType           = WorkloadType::AbKeyType;
		using KeyType             = AbKeyType::MainKeyType;
		using Transaction         = WorkloadType::Transaction;

		using DataTupleHeaderType = DataTupleHeader<AbKeyType>;
		using IndexTupleType      = IndexTuple;

		using Context             = TxContext<AbKeyType>;
		using AccessEntryType     = Context::Entry;
		using ExecutorType        = Executor<Self, AbKeyType>;

		using EventMapType        = tbb::concurrent_unordered_set<DataTupleVirtualHeader *>;

		static_assert(AbstractKeyConcept<AbKeyType>);
		static_assert(ExecutorConcept<ExecutorType>);

		friend Context;

		friend ExecutorType;

		enum class TxStatus {
			Normal,
			Running,
			NoAlive
		};

		struct alignas(CACHE_LINE_SIZE) TxidEntry {
			std::atomic<TxStatus> state;        // 用来指明 state
			std::atomic<uint32_t> seq_num;    // 用来指明该线程的事务ID

			TxidEntry(): state(TxStatus::Normal), seq_num(0) {}
		};

	public:
		StorageManager *storage_manager_ptr_;

		LogPersist<AbKeyType> log_persist_;

		alignas(CACHE_LINE_SIZE) std::atomic<bool> stop_flag_;

		alignas(CACHE_LINE_SIZE) TxidEntry global_tx_id_table[thread::get_max_tid()];

		alignas(CACHE_LINE_SIZE) std::array<uint32_t, thread::get_max_tid()> *dep_list;

		alignas(CACHE_LINE_SIZE) std::array<uint32_t, thread::get_max_tid()> *localLP;

		EventMapType update_event_set_[2];

		uint32_t update_event_set_idx_;


	public: // Class Property

		explicit SP(StorageManager *storage_manager_ptr):
				storage_manager_ptr_(storage_manager_ptr),
				log_persist_(storage_manager_ptr->get_log_space_range()),
				dep_list(new std::array<uint32_t, thread::get_max_tid()>[thread::get_max_tid(), CACHE_LINE_SIZE]),
				localLP(new std::array<uint32_t, thread::get_max_tid()>[thread::get_max_tid(), CACHE_LINE_SIZE]),
				update_event_set_idx_(0) {

			spdlog::warn("Atomic version of SP can't make sure that data read is integral when running transaction, "
						"but this inconsistency will be detected when validating.");

			storage_manager_ptr_->register_data_deallocate_func([&](const IndexTupleType &index_tuple) {
				DataTupleVirtualHeader *header_ptr = index_tuple.get_data_header_ptr();
				void *data_ptr = index_tuple.get_data_ptr();
				delete header_ptr;
				storage_manager_ptr_->deallocate_data_and_header(index_tuple.get_data_type(), data_ptr);
			});

			for(uint32_t i = 0; i < thread::get_max_tid(); ++i) {
				dep_list[i].fill(0);
				localLP[i].fill(0);
			}

			for (auto &item: global_tx_id_table) {
				item.state.store(TxStatus::NoAlive, std::memory_order::relaxed);
			}
		}

		~SP() {
			get_concurrent_control_message().clear_up();

			delete[] dep_list;
			delete[] localLP;
		}

		void flush_all_works() {
			storage_manager_ptr_->fence();

			for (auto &item: global_tx_id_table) {
				item.state.store(TxStatus::NoAlive, std::memory_order::relaxed);
			}
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
		/*!
		 * @brief Initialize context of transaction
		 * @param tx_context Context of transaction
		 * @return Always true
		 */
		bool init_tx(Context &tx_context) {
			get_thread_message().start_transaction();

			uint32_t tid = thread::get_tid();
			if (stop_flag_.load(std::memory_order::acquire)) {
				if (tid == 0) {
					// Wait for all threads blocked
					for (uint32_t i = 1; i < thread::get_max_tid(); ++i) {
						if (global_tx_id_table[i].state.load(std::memory_order::relaxed) == TxStatus::NoAlive) [[unlikely]] { continue; }
						while (global_tx_id_table[i].state.load(std::memory_order::acquire) != TxStatus::Normal) {
							std::this_thread::yield();
						}
					}

					// Get unprocessed context and copy delayed data
					for (DataTupleVirtualHeader *header_ptr : update_event_set_[update_event_set_idx_]) {
						void *origin_data_ptr = header_ptr->get_data_ptr();
						void *temp_data_ptr = header_ptr->latest_data_ptr_.load(std::memory_order::relaxed);

						std::memcpy(origin_data_ptr,
						            temp_data_ptr,
						            header_ptr->data_size_);
						storage_manager_ptr_->pwb_range(origin_data_ptr, header_ptr->data_size_);

						storage_manager_ptr_->deallocate_data_and_header(header_ptr->data_type_, temp_data_ptr);
						header_ptr->latest_data_ptr_.store(origin_data_ptr, std::memory_order::relaxed);
					}
					update_event_set_idx_ = 1 - update_event_set_idx_;

					storage_manager_ptr_->fence();
					log_persist_.reset_log_space();

					// Restart all threads
					for (auto & table : global_tx_id_table) {
						if (table.state.load(std::memory_order::relaxed) == TxStatus::NoAlive) { continue; }
						table.state.store(TxStatus::Normal, std::memory_order::relaxed);
					}
					storage_manager_ptr_->fence();
					stop_flag_.store(false, std::memory_order::release);

					update_event_set_[1 - update_event_set_idx_].clear();
				}
				else {
					while (stop_flag_.load(std::memory_order::acquire)) { std::this_thread::yield(); }
				}
			}

			global_tx_id_table[tid].state.store(TxStatus::Running, std::memory_order::release);

			global_tx_id_table[tid].seq_num++;

			dep_list[tid].fill(0);

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

			// Clear all back up data in context
			tx_context.clear([this](const AbKeyType &key, void *data_ptr) {
				storage_manager_ptr_->deallocate_data_and_header(key.type_, data_ptr);
			});

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

			uint32_t tid = thread::get_tid();
			DataTupleVirtualHeader *header_ptr = temp_index_tuple.get_data_header_ptr();
			uint32_t thid       = header_ptr->thid_;
			uint32_t seq_num    = header_ptr->seq_num_;
			dep_list[tid][thid] = std::max(dep_list[tid][thid], seq_num);

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

			uint32_t tid = thread::get_tid();

			DataTupleVirtualHeader *vheader_ptr = temp_index_tuple.get_data_header_ptr();
			auto [header_ptr, back_up_ptr]      = storage_manager_ptr_->allocate_data_and_header(key.type_);
			uint32_t entire_size                = vheader_ptr->get_data_size();

			new (header_ptr) DataTupleHeaderType(key);
			std::memcpy(back_up_ptr, vheader_ptr->latest_data_ptr_, entire_size);
			// Read timestamp before copying data.
			// Correspondingly, we will write data before write timestamp in write phase.
			// Then, if we read the old data, the timestamp should be old, too, which will incur validation failure.
			tx_context.access_write(key, temp_index_tuple, back_up_ptr, temp_index_tuple.get_data_size(), 0);

			dep_list[tid][vheader_ptr->last_tid_] = std::max(dep_list[tid][vheader_ptr->last_tid_], vheader_ptr->seq_num_);

			return back_up_ptr;
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

			uint32_t tid = thread::get_tid();

			DataTupleVirtualHeader *vheader_ptr = temp_index_tuple.get_data_header_ptr();
			auto [header_ptr, back_up_ptr]      = storage_manager_ptr_->allocate_data_and_header(key.type_);
			uint32_t entire_size                = vheader_ptr->get_data_size();

			new (header_ptr) DataTupleHeaderType(key);
			std::memcpy(back_up_ptr, vheader_ptr->latest_data_ptr_, entire_size);
			// Read timestamp before copying data.
			// Correspondingly, we will write data before write timestamp in write phase.
			// Then, if we read the old data, the timestamp should be old, too, which will incur validation failure.
			tx_context.access_write(key, temp_index_tuple, back_up_ptr, size, offset);

			dep_list[tid][vheader_ptr->last_tid_] = std::max(dep_list[tid][vheader_ptr->last_tid_], vheader_ptr->seq_num_);

			return back_up_ptr;
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
			return tx_context.access_insert(key, row, size) != nullptr;
		}

		/*!
		 * @brief Remove operation
		 * @param tx_context Context of transaction
		 * @param key Abstract key of data tuple
		 * @return If there isn't a tuple with the same key, return false
		 */
		bool remove(Context &tx_context, const AbKeyType &key) {
			spdlog::error("Haven't implemented remove for sp3.");
			return false;
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
			tx_context.status_ = Context::Status::Validating;

			auto &write_set  = tx_context.write_set_;
			auto &insert_set = tx_context.insert_set_;

			bool has_update = !write_set.empty();
			bool has_insert = !insert_set.empty();
			bool has_write  = has_update || has_insert;

			// Commit read only transaction
			if (!has_write) { return read_only_commit(tx_context); }

			return normal_commit(tx_context);
		}

		bool read_only_commit(Context &tx_context) {
			tx_context.message_.end_running();
			tx_context.status_ = Context::Status::Committing;

			uint32_t tid = thread::get_tid();

			auto &thread_dep_list    = dep_list[tid];
			auto &thread_local_lp    = localLP[tid];
			auto &thread_tx_id_table = global_tx_id_table[tid];

			for (uint32_t i = 0; i < thread::get_max_tid() && i != tid && dep_list[tid][i] != 0; ++i) {

				if (thread_dep_list[i] < thread_local_lp[i]) { continue; }

				while (true) {
					thread_local_lp[i] = global_tx_id_table[i].seq_num;
					if (thread_local_lp[i] != thread_dep_list[i] ||
								global_tx_id_table[i].state.load(std::memory_order::acquire) != TxStatus::Running) { break; }

					std::this_thread::yield();
				}
			}
			thread_tx_id_table.state.store(TxStatus::Normal);

			return true;
		}

		bool normal_commit(Context &tx_context) {
			tx_context.message_.start_commit();
			tx_context.message_.start_validate();
			tx_context.status_ = Context::Status::Validating;

			uint32_t tid = thread::get_tid();

			auto &read_set   = tx_context.read_set_;
			auto &write_set  = tx_context.write_set_;

			// Commit normal transaction

			// Allocate space for logging
			auto [success_log, log_space] = allocate_log_space(tx_context);
			if (!success_log) { // If there isn't any spare space for logging, stop all threads to persist data
				if (write_set.empty()) [[unlikely]] {// TODO: Hack
					log_persist_.reset_log_space();
					success_log = true;
				}
				else {
					stop_flag_.store(true);

					tx_context.message_.end_validate();
					return false;
				}
			}

			// If no log, abort directly
			bool success_validate = true;

			if (success_validate) {
				// According to the comparison rule, write tx should be ahead of read tx with the same key
				std::sort(write_set.begin(), write_set.end());

				// Validate write set
				for (auto &entry: write_set) {
					IndexTupleType &origin_tuple = entry.tuple;
					// Avoid locking the same tuple repeatedly
					origin_tuple.lock_write();
				}

				// There is possibly the same tuple in read and write set if read set spur before write set.
				// But it is all right because we deal with read set firstly.
				// Validate read set
				for (auto &entry: read_set) {
					IndexTupleType &origin_tuple = entry.tuple;
					uint64_t wts   = origin_tuple.get_wts_ref().load(std::memory_order::acquire);
					uint32_t lthid = origin_tuple.get_data_header_ptr()->thid_;

					if (entry.wts != wts || (lthid != NO_LOCK_THREAD && lthid != tid)) {
						success_validate = false;
						break;
					}
				}
			}

			tx_context.message_.end_validate();
			tx_context.status_ = Context::Status::Committing;

			if (success_validate) {
				tx_context.message_.start_persist_data();
				do_insert(tx_context);
				do_update(tx_context);
				tx_context.message_.end_persist_data();
			}

			// Unlock all locked tuples.
			for (auto &entry: write_set) {
				IndexTupleType &origin_tuple = entry.tuple;
				origin_tuple.unlock_write();
			}

			auto &thread_dep_list = dep_list[tid];
			auto &thread_local_lp = localLP[tid];

			for (uint32_t i = 0; i < thread::get_max_tid() && i != tid && thread_dep_list[i]; ++i) {
				if (thread_dep_list[i] < thread_local_lp[i]) { continue; }

				while (true) {
					thread_local_lp[i] = global_tx_id_table[i].seq_num;
					if (thread_local_lp[i] != thread_dep_list[i] ||
					    global_tx_id_table[i].state.load(std::memory_order::acquire) != TxStatus::Running) { break; }
					std::this_thread::yield();
				}
			}

			if (success_validate) {
				// Persist logs
//				write_log(log_space, tx_context);

				auto &thread_tx_id_table = global_tx_id_table[tid];
				thread_tx_id_table.state.store(TxStatus::Normal);
			}

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

			uint32_t tid = thread::get_tid();
			global_tx_id_table[tid].state.store(TxStatus::Normal);
			global_tx_id_table[tid].seq_num--;

			return clean_up(tx_context);
		}

		/*!
		 * @brief Reset context for another execution
		 * @param tx_context Context of transaction
		 * @return Always true
		 */
		bool reset(Context &tx_context) {
			init_tx(tx_context);
			return true;
		}

	private: // Assist Function

		auto allocate_log_space(Context &tx_context) {
			auto [log_amount, log_size] = tx_context.get_log_amount_size();
			// Append the size of dependency list
			log_size += thread::get_max_tid() * sizeof(uint32_t);
			// Allocate space for logs
			return log_persist_.allocate_log_space(log_amount, log_size);
		}

		/*!
		 * @brief Write log
		 * @param tx_context Context of transaction
		 */
		auto write_log(auto log_space, Context &tx_context) {
			tx_context.message_.start_persist_log();

			uint32_t tid = thread::get_tid();

			auto &write_set    = tx_context.write_set_;
			auto &insert_set   = tx_context.insert_set_;

			// ---- Write log
			// Start
			log_persist_.add_start_log(log_space, 0);

			// Write all dependent relationships
			auto &thread_dep_list = dep_list[tid];
			log_persist_.add_update_log(log_space, thread_dep_list.data(), thread_dep_list.size() * sizeof(uint32_t));

			// Insert
			for (AccessEntryType &entry: insert_set) {
				auto [content_ptr, content_size] = tx_context.get_log(entry);
				log_persist_.add_update_log(log_space, content_ptr, content_size);
			}
			// Write
			for (AccessEntryType &entry: write_set) {
				auto [content_ptr, content_size] = tx_context.get_log(entry);
				log_persist_.add_update_log(log_space, content_ptr, content_size);
			}
			// Commit
			log_persist_.add_commit_log(log_space, 0);
			storage_manager_ptr_->pwb_range(log_space.start_ptr, log_space.cur_ptr - log_space.start_ptr);
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

		/// @brief As for inserting, we need to allocate data and header and insert it.
		/// @param tx_context
		void do_insert(Context &tx_context) {
			auto &insert_set   = tx_context.insert_set_;

			// For every entry in insert set, insert tuple
			for (AccessEntryType &entry: insert_set) {
				auto key           = entry.key;
				auto size          = entry.size;

				// Make header of data
				auto [header_ptr, data_ptr] = storage_manager_ptr_->allocate_data_and_header(key.type_);
				new(header_ptr) DataTupleHeaderType(key);
				// Make index tuple of data and connect it with data header.
				IndexTupleType index_tuple(key.type_, new DataTupleVirtualHeader{ 0, data_ptr, entry.size, key.type_ });
				// Copy data
				index_tuple.set_data(entry.data_ptr, size, 0);
				// Insert index tuple
				storage_manager_ptr_->add_data_index_tuple(key.type_, key.logic_key_, index_tuple);
				storage_manager_ptr_->pwb_range(header_ptr, sizeof(DataTupleHeaderType));
				storage_manager_ptr_->pwb_range(index_tuple.get_data_header_ptr(), sizeof(DataTupleVirtualHeader));
				storage_manager_ptr_->pwb_range(data_ptr, size);
			}

			storage_manager_ptr_->fence();

			tx_context.message_.end_persist_data();
		}


		/// @brief As for updating data, we just need to change the pointer and the metadata.
		/// @param tx_context
		void do_update(Context &tx_context) {
			uint32_t tid = thread::get_tid();
			auto &write_set = tx_context.write_set_;

			for (AccessEntryType &entry: write_set) {
				DataTupleVirtualHeader *vheader_ptr  = entry.tuple.get_data_header_ptr();
				void *temp_ptr = entry.data_ptr;

				temp_ptr = vheader_ptr->latest_data_ptr_.exchange(temp_ptr, std::memory_order::release);

				vheader_ptr->last_tid_            = tid;
				vheader_ptr->seq_num_             = global_tx_id_table[tid].seq_num;
				vheader_ptr->wts_                += 1;

				update_event_set_[update_event_set_idx_].insert(vheader_ptr);
				if (temp_ptr != vheader_ptr->get_data_ptr()) {
					storage_manager_ptr_->deallocate_data_and_header(vheader_ptr->get_data_type(), temp_ptr);
				}
			}
		}

	};
}
