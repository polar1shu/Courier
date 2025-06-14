/*
 * @author: BL-GS 
 * @date:   2023/6/11
 */

#pragma once

#include <cstdint>
#include <cstddef>
#include <span>
#include <vector>
#include <mutex>

#include <memory/memory.h>
#include <concurrent_control/courier_save/data_tuple.h>
#include <concurrent_control/courier_save/log_persist.h>

namespace cc::courier_save {

	struct VHeaderCacheTuple {
		std::shared_mutex       shared_handler_;
		DataTupleVirtualHeader *vheader_ptr_;
		alignas(16) uint8_t		data_[];
	};

	class VHeaderCache {
	private:
		static constexpr size_t VHEADER_CACHE_SIZE	= 64_MB;

		static constexpr size_t CACHE_TUPLE_ALIGN	= 16;

		struct CacheHeader {
			uint8_t *start_ptr_;
			uint32_t size_;
			uint8_t *last_time_ptr_;

			CacheHeader():
                start_ptr_(nullptr),
                size_(0),
				last_time_ptr_(nullptr) {}

			CacheHeader(CacheHeader &&other) noexcept :
                start_ptr_(other.start_ptr_),
                size_(other.size_),
				last_time_ptr_(other.last_time_ptr_) {

				other.start_ptr_     = nullptr;
				other.size_          = 0;
				other.last_time_ptr_ = nullptr;
			}

			~CacheHeader() {
				delete[] start_ptr_;
			}

			void init() {
				start_ptr_      = new uint8_t[VHEADER_CACHE_SIZE];
				size_           = VHEADER_CACHE_SIZE;
				last_time_ptr_  = start_ptr_;
			}
		};

	private:
		std::vector<CacheHeader> cache_array_;

	public:
		VHeaderCache() = default;

		~VHeaderCache() {
			for (CacheHeader &header: cache_array_) {
				uint32_t aligned_data_size = align_cache_tuple(header.size_);

				uint8_t *start_ptr = header.start_ptr_;
				uint8_t *end_ptr = header.start_ptr_ + VHEADER_CACHE_SIZE;

				if (start_ptr == nullptr) { continue; }

				for (uint8_t *ptr = start_ptr; ptr < end_ptr; ptr += aligned_data_size) {
					auto *cache_tuple_ptr = reinterpret_cast<VHeaderCacheTuple *>(ptr);
					cache_tuple_ptr->vheader_ptr_ = nullptr;
				}
			}
		}

	public:
		void init_cache_tuple(uint32_t data_type, uint32_t data_size) {
			uint32_t aligned_data_size = align_cache_tuple(data_size);

			if (cache_array_.size() <= data_type) [[unlikely]] {// Allocate a new cache for this type of data, thus a warming up is necessary.
				// It is quite slow to use a map, we just assume that the `data_type` is sequential and begin from 0
				cache_array_.resize(data_type + 1);
			}

			auto &cur_cache = cache_array_[data_type];

			if (cur_cache.start_ptr_ == nullptr) {
				cur_cache.init();
				// Initialize the cache space
				uint8_t *start_ptr = cur_cache.start_ptr_;
				uint8_t *end_ptr   = cur_cache.start_ptr_ + VHEADER_CACHE_SIZE - aligned_data_size;
				for (uint8_t *ptr = start_ptr; ptr <= end_ptr; ptr += aligned_data_size) {
					auto *cache_tuple_ptr = reinterpret_cast<VHeaderCacheTuple *>(ptr);
					new(cache_tuple_ptr) VHeaderCacheTuple {
						.vheader_ptr_ = nullptr
					};
				}
			}
		}

		/*!
		 * @brief Allocate a new cache tuple for specific data tuple
		 * @details To Allocate a new cache tuple, it first find the corresponding cache by `data_type`,
		 * and then move forward the scanner to get a cache tuple. If the tuple contains any valid data,
		 * we shall persist it into `origin_data_ptr`.
		 * @note To make this cache tuple globally visible, you need to use `construct_data_cache_link` to record it in virtual header.
		 * @param data_type The type id of data
		 * @param data_size The size of data
		 * @param update_size The actually updated size [for optimization]
		 * @param update_offset The actually updated offset [for optimization]
		 * @return The pointer to available cache
		 */
		uint8_t *allocate_cache_tuple(uint32_t data_type, uint32_t data_size) {
			uint32_t aligned_data_size = align_cache_tuple(data_size);

			// Initialize cache tuple if not exist
			init_cache_tuple(data_type, data_size);

			auto &cur_cache = cache_array_[data_type];

			// Ring-round allocation
			uint8_t *cur_cache_ptr = cur_cache.last_time_ptr_;
			if (cur_cache_ptr >= cur_cache.start_ptr_ + cur_cache.size_ - aligned_data_size) {
				cur_cache_ptr = cur_cache.start_ptr_;
			}
			cur_cache.last_time_ptr_ = cur_cache_ptr + aligned_data_size;

			auto *cache_tuple_ptr = reinterpret_cast<VHeaderCacheTuple *>(cur_cache_ptr);
			if (cache_tuple_ptr->vheader_ptr_ != nullptr) {
				return nullptr;
			}
			return cache_tuple_ptr->data_;
		}

		/*!
		 * @brief Deallocate a cache tuple
		 * @details It won't do anything if this cache haven't been linked to any valid data tuple,
		 * but deconstruct the link if a relationship exists.
		 * Therefore, it is ok to use it when aborting before actually updating data
		 * @param data_type The type id of data
		 * @param cache_data_ptr
		 */
		static void deallocate_cache_tuple(uint32_t data_type, void *cache_data_ptr) {
			VHeaderCacheTuple *cache_tuple_ptr = get_cache_tuple_from_data_ptr(cache_data_ptr);
			if (cache_tuple_ptr->vheader_ptr_ != nullptr) { // If it is related to a data tuple, deconstruct this relationship.
				cache_tuple_ptr->vheader_ptr_->virtual_data_ptr_.store(cache_tuple_ptr->vheader_ptr_->get_origin_data_ptr());
				cache_tuple_ptr->vheader_ptr_ = nullptr;
			}
		}

	public:
		static VHeaderCacheTuple *construct_data_cache_link(void *cache_data_ptr, DataTupleVirtualHeader *vheader_ptr) {
			VHeaderCacheTuple *cache_tuple_ptr = get_cache_tuple_from_data_ptr(cache_data_ptr);
			cache_tuple_ptr->vheader_ptr_ = vheader_ptr;

			vheader_ptr->virtual_data_ptr_.store(cache_data_ptr, std::memory_order::release);
			return cache_tuple_ptr;
		}

		void abort_reclaim(uint32_t data_type, void *cache_data_ptr) {
			VHeaderCacheTuple *cache_tuple_ptr = get_cache_tuple_from_data_ptr(cache_data_ptr);
			void *allocate_ptr = cache_array_[data_type].last_time_ptr_;
			cache_array_[data_type].last_time_ptr_ = reinterpret_cast<uint8_t *>(
					std::min(static_cast<VHeaderCacheTuple *>(allocate_ptr), cache_tuple_ptr)
			);
		}

		/*!
		 * @brief Get pointer to struct `VHeaderCacheTuple` by offset
		 * @param data_ptr The pointer to data in cache tuple
		 */
		static VHeaderCacheTuple *get_cache_tuple_from_data_ptr(void *data_ptr) {
			return reinterpret_cast<VHeaderCacheTuple *>(static_cast<uint8_t *>(data_ptr) - offsetof(VHeaderCacheTuple, data_));
		}

	private:
		/*!
		 * @brief Align the data size up to `CACHE_TUPLE_ALIGN`
		 */
		static constexpr uint32_t align_cache_tuple(uint32_t data_size) {
			 static_assert(is_2pow(CACHE_TUPLE_ALIGN), "Expect cache tuple aligned to the power of 2");
			 return ceil_2pow(data_size + sizeof(VHeaderCacheTuple), CACHE_TUPLE_ALIGN);
		}

	};
}
