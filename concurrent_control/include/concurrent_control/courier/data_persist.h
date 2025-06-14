/*
 * @author: BL-GS 
 * @date:   2023/3/23
 */

#pragma once

#include <cstdint>
#include <atomic>
#include <utility>
#include <unordered_map>
#include <concurrentqueue/concurrentqueue.h>

#include <concurrent_control/abstract_concurrent_control.h>
#include <concurrent_control/courier/data_tuple.h>
#include <concurrent_control/courier/tx_context.h>
#include <concurrent_control/courier/log_persist.h>
#include <concurrent_control/courier/thread_context.h>

namespace cc::courier {

	//! @brief Component responsible for delayed persisting tasks
	//! @tparam StorageManager Type of storage manager
	//! @tparam Recovery Type of recovery component
	//! @tparam AbKey Type of abstract key
	template<class StorageManager, class AbKey>
		requires AbstractKeyConcept<AbKey>
	class DataPersist {
	public:
		using AbKeyType       = AbKey;
		using KeyType         = AbKeyType::MainKeyType;

		using IndexTupleType  = IndexTuple;
		using Context         = TxContext<AbKeyType>;

		using WriteEntryType  = Context::WriteEntry;

		using LogSpaceType    = LogSpace;

		//! @brief The maximal number of tasks in a batch
		static constexpr uint32_t MAX_BATCH_NUM         = 64;
		//! @brief When amount of tasks exceed this level, ask more threads for aid.
		static constexpr uint32_t TASK_HIGH_LEVEL       = 48;
		//! @brief The number of tasks which local thread acquire for once.
		static constexpr uint32_t ACQUIRE_TASK_NUM_ONCE = 4;

	private:
		StorageManager *storage_manager_ptr_;

		LogPersist<AbKeyType> *recovery_ptr_;

		//! @brief The queue of combined delayed tasks
		moodycamel::ConcurrentQueue<ThreadBuffer *> thread_buffer_queue_;
		// alignas(64) tbb::concurrent_queue<ThreadBuffer *> thread_buffer_queue_;
		//! @brief Max tid of threads who need to aid persisting data
		alignas(64) volatile uint32_t persist_aid_max_tid_;

		//! @brief The max tid of tid historically, just for record.
		uint32_t his_max_aid_tid_;
		//! @brief The max amount of tasks delayed historically.
		uint32_t his_max_task_num_;

	public:
		DataPersist(StorageManager *storage_manager_ptr, LogPersist<AbKeyType> *recovery_ptr):
				storage_manager_ptr_(storage_manager_ptr),
				recovery_ptr_(recovery_ptr),
				persist_aid_max_tid_(0),
				his_max_aid_tid_(0),
				his_max_task_num_(0) {
		}

		~DataPersist() {
			std::cout << "His max tid: " << his_max_aid_tid_ << std::endl;
			std::cout << "His max task num: " << his_max_task_num_ << std::endl;
			// It is no need to write all entries because the log will record the left things.
		}

		//! @brief Function for coordinating thread.
		void persist_thread_work() {
			// Coordinate worker threads.
			const uint32_t aid_max_tid         = persist_aid_max_tid_;
			const uint32_t task_num            = thread_buffer_queue_.size_approx();
			const uint32_t low_level_task_num  = aid_max_tid * MAX_BATCH_NUM;
			const uint32_t high_level_task_num = low_level_task_num + TASK_HIGH_LEVEL;

			if (ConcurrentControlMessage::record) {
				his_max_task_num_ = std::max(his_max_task_num_, task_num);
			}

			if (task_num > high_level_task_num) {
				persist_aid_max_tid_ = aid_max_tid + 1;
				if (ConcurrentControlMessage::record) {
					his_max_aid_tid_ = std::max(his_max_aid_tid_, aid_max_tid + 1);
				}
			}
			else if (task_num < low_level_task_num) {
				persist_aid_max_tid_ = aid_max_tid - 1;
			}
		}

		//! @brief Finish all work undone
		void flush_all_work() {
			while (thread_buffer_queue_.size_approx() > 0) { do_batch(); }
		}

	public:
		/*!
			* @brief Try to enqueue delayed write events
			* @param tid ID of the current thread
			* @param tx_context Context of transaction
			*/
		void push_context(uint32_t tid, Context &tx_context) {
			ThreadBuffer *thread_buffer_ptr = thread_local_context.thread_buffer_ptr;

			auto &entry_map       = thread_buffer_ptr->entry_map;

			auto &write_set       = tx_context.write_set_;

			// For all write events, there may have been an event with the same key, just to combine them
			// The detail of combination is in construction of DelayUpdateEvent
			for (const WriteEntryType &entry: write_set) {
				if (entry.type != TxType::Delete) [[likely]] {
					// Because of the certain order of context pushed, we can update it directly.
					entry_map.insert_or_assign(entry.tuple.get_data_header_ptr(), generate_event(entry));
				}
			}
		}

		/*!
			* @brief Try to enqueue data
			* @param tid ID of the current thread
			*/
		void persist_data(uint32_t tid, const LogSpaceType &log_space) {
			ThreadBuffer *thread_buffer_ptr = thread_local_context.thread_buffer_ptr;
			thread_local_context.thread_buffer_ptr = new ThreadBuffer;

			thread_buffer_ptr->log_space    = log_space;
			thread_buffer_queue_.enqueue(thread_buffer_ptr);
		}

		/*!
			* @brief Aid if needed
			* @param tid ID of the current thread
			*/
		void aid(uint32_t tid) {
			if (tid == 0) { persist_thread_work(); }
			// Try to aid processing tasks.
			if (need_aid(tid)) [[unlikely]] { do_batch(); }
		}

		/*!
			* @brief Try to get a batch of task and process it.
			*/
		void do_batch() {
			for (uint32_t i = 0; i < ACQUIRE_TASK_NUM_ONCE; ++i) {
				ThreadBuffer *batch_ptr = nullptr;
				if (thread_buffer_queue_.try_dequeue(batch_ptr)) {
					process_batch(batch_ptr);
					delete batch_ptr;
				}
			}
		}

		/*!
			* @brief Process a batch of tasks
			* @param batch_ptr The pointer to the batch
			*/
		void process_batch(ThreadBuffer *batch_ptr) {
			auto &entry_map       = batch_ptr->entry_map;
			auto &log_space       = batch_ptr->log_space;

			// Process all entries in the batch
			for (auto &item: entry_map) {
				process_event(item.first, item.second);
			}
			util_mem::sfence();

			// Deallocate all space for log
			deallocate_log_space(log_space);
		}

		/*!
			* @brief Process a delayed update event
			* @param event The task to process
			*/
		static void process_event(DataTupleVirtualHeader *header_ptr, const DelayUpdateEvent &event) {
			const uint8_t *source_data_ptr = static_cast<uint8_t *>(header_ptr->get_virtual_data_ptr());
			if (source_data_ptr == event.target_ptr_) { return; }

			// Copy the content from DRAM pointed by vHeader to PM
			std::memcpy(
				event.target_ptr_ + event.offset_,
				source_data_ptr + event.offset_,
				event.size_
			);
			util_mem::clflushopt_range(event.target_ptr_ + event.offset_, event.size_);
		}

	private:
		/*!
			* @brief Generate a new event according to the entry in context
			* @param entry the entry in context(write set / insert set)
			* @return The corresponding delayed update event
			*/
		static DelayUpdateEvent generate_event(const WriteEntryType &entry) {
			const IndexTupleType &index_tuple = entry.tuple;
			return DelayUpdateEvent {
				index_tuple.get_origin_data_ptr(),
				entry.size,
				entry.offset // Offset of insert event is assured to be 0
			};
		}

		/*!
			* @brief Deallocate the space of log
			* @param log_space
			*/
		void deallocate_log_space(LogSpaceType &log_space) {
			recovery_ptr_->deallocate_log_space(log_space);
		}

		/*!
			* @brief Whether the current thread need to aid processing tasks
			* @param tid The id of thread
			*/
		bool need_aid(uint32_t tid) const {
			return persist_aid_max_tid_ >= tid;
		}
	};

}
