/*
 * @author: BL-GS 
 * @date:   2024/1/17
 */

#pragma once

#include <cstddef>
#include <optional>

#include <util/random_generator.h>

namespace cc::courier_save {

	struct LogSpace {
		uint8_t *start_ptr;
		uint8_t *cur_ptr;
		uint8_t *end_ptr;
	};

	//! @brief Information about delayed data persisting.
	struct DelayUpdateEvent {
	public:
		//! @brief Pointer of destination, which has been offset
		uint8_t *target_ptr_;
		//! @brief Start pointer of source data
		uint32_t start_idx_;
		//! @brief End pointer of source data
		uint32_t end_idx_;
		//! @brief The pointer to the shared_mutex of data cache object, acting as reference count handler
		std::shared_mutex *shared_handler_ptr_;

	public:
		DelayUpdateEvent(void *target_ptr, void *start_ptr, uint32_t size, uint32_t offset, std::shared_mutex *shared_handler_ptr):
              target_ptr_((uint8_t *)target_ptr + offset),
              start_idx_(offset),
              end_idx_(offset + size),
			  shared_handler_ptr_(shared_handler_ptr) {
			// Courier gets a deferred write task only if it locates in a cache object.
			DEBUG_ASSERT(shared_handler_ptr_ != nullptr,
			             "Generate delay update event with null shared handler");
			// shared_handler_ptr_->lock_shared();
		}

		DelayUpdateEvent(DelayUpdateEvent &&other) noexcept :
              target_ptr_(other.target_ptr_),
              start_idx_(other.start_idx_),
              end_idx_(other.end_idx_),
			  shared_handler_ptr_(other.shared_handler_ptr_) {
			other.shared_handler_ptr_ = nullptr;
		}

		/*!
		* @brief Combine the region presented by this event with another one.
		* @param other Another event to combine with
		*/
		DelayUpdateEvent &operator= (const DelayUpdateEvent &other) {
			target_ptr_         = std::min(target_ptr_      , other.target_ptr_);
			start_idx_          = std::min(start_idx_       , other.start_idx_);
			end_idx_            = std::max(end_idx_         , other.end_idx_);

			DEBUG_ASSERT(shared_handler_ptr_ == other.shared_handler_ptr_,
			             "Combine two entries with different shared handler");
			shared_handler_ptr_->unlock_shared();
			return *this;
		}
	};

	//! @brief Temporary buffer for delayed persisting data
	struct ThreadBuffer {
	public:
		//! @brief Map storing the delayed event and the pointer to tuple's header
		std::unordered_map<DataTupleVirtualHeader *, DelayUpdateEvent> entry_map_;
		//! @brief The array storing log spaces of delayed events
		LogSpace log_space_;
	};


	//! @brief A context for each worker thread, containing thread-private variables.
	struct ThreadContext {
		/// The pointer to a buffer which contains delay writes
		ThreadBuffer *thread_buffer_ptr;
		/// The iterator of log page
		size_t page_idx;
		/// The thread-private space for logging
		std::optional<LogSpace> log_space;

		ThreadContext(): thread_buffer_ptr(nullptr), page_idx(util::rander.rand_long()) {
			thread_buffer_ptr = new ThreadBuffer;
		}
		~ThreadContext() { delete thread_buffer_ptr; }
	};

	inline thread_local ThreadContext thread_local_context;

}