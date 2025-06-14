/*
 * @author: BL-GS 
 * @date:   2024/1/18
 */

#pragma once

#include <cstdint>
#include <unordered_map>

#include <util/random_generator.h>

namespace cc::courier {

	struct DataTupleVirtualHeader;

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
		uint32_t size_;
		uint32_t offset_;

	public:
		DelayUpdateEvent(void *target_ptr, uint32_t size, uint32_t offset):
                   target_ptr_(static_cast<uint8_t *>(target_ptr) + offset),
                   size_(size),
                   offset_(offset) {}

		DelayUpdateEvent(DelayUpdateEvent &&other) noexcept :
				      target_ptr_(other.target_ptr_),
				      size_(other.size_),
				      offset_(other.offset_){}

		DelayUpdateEvent &operator= (const DelayUpdateEvent &other) {
			combine(other);
			return *this;
		}

	public:
		/*!
		* @brief Combine the region presented by this event with another one.
		* @param other Another event to combine with
		*/
		void combine(const DelayUpdateEvent &other) {
			DEBUG_ASSERT(target_ptr_ == other.target_ptr_, "Unequal pointer when combining delay update event");

			const uint32_t new_offset = std::min(offset_, other.offset_);
			const uint32_t new_size   = std::max(
                offset_ + size_,
                other.offset_ + other.size_
            ) - new_offset;
			size_   = new_size;
			offset_ = new_offset;
		}
	};

	//! @brief Temporary buffer for delayed persisting data
	struct ThreadBuffer {
	public:
		//! @brief Map storing the delayed event and the pointer to tuple's header
		std::unordered_map<DataTupleVirtualHeader *, DelayUpdateEvent> entry_map;
		//! @brief The array storing log spaces of delayed events
		LogSpace log_space;
	};

	struct ThreadContext {

		ThreadBuffer *thread_buffer_ptr;

		size_t page_idx;
		std::optional<LogSpace> log_space;

		ThreadContext(): thread_buffer_ptr(nullptr), page_idx(util::rander.rand_long()) {
			thread_buffer_ptr = new ThreadBuffer;
		}

		~ThreadContext() noexcept {
			delete thread_buffer_ptr;
		}
	};

	inline thread_local ThreadContext thread_local_context;

}