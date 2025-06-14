/*
 * @author: BL-GS 
 * @date:   2023/1/2
 */

#pragma once

#include <cstdint>
#include <mutex>
#include <vector>
#include <utility>

#include <concurrent_control/config.h>
#include <concurrent_control/abstract_concurrent_control.h>
#include <concurrent_control/mvcc/data_tuple.h>

namespace cc {

	inline namespace mvcc {

		enum class TxType {
			Read,
			Write,
			Insert,
			Delete
		};

		template<class AbKey>
		struct AccessEntry {
		public:
			using AbKeyType      = AbKey;
			using IndexTupleType = IndexTuple;

		public:
			/// Type of operation
			TxType              type;
			/// Key of object
			AbKeyType           key;
			/// Size and ptr as file description
			uint32_t            offset;
			uint32_t            size;
			void *              data_ptr;
			/// Index tuple
			IndexTupleType      tuple;

			auto operator <=> (const AccessEntry &other) const {
				return key <=> other.key;
			}

			bool operator == (const AccessEntry &other) const {
				return key == other.key;
			}
		};

		template<class AbKey>
		struct TxContext {
		public:
			using AbKeyType      = AbKey;
			using IndexTupleType = IndexTuple;
			using Entry          = AccessEntry<AbKeyType>;

			static constexpr size_t TEMP_DATA_ALIGN_SIZE = 16;

		public:
			ConcurrentControlPortableMessage message_;
			/// Start ts
			uint64_t start_ts_;
			/// For all update/delete elements
			std::vector<Entry> access_set_;
			/// For all insert elements
			std::vector<Entry> insert_set_;
			/// The total size of log info
			uint64_t log_info_size_;
			/// The total amount of log tuple
			uint64_t log_amount_;

		public:
			TxContext(): start_ts_(0), log_amount_(0), log_info_size_(0) {}

			TxContext(TxContext &&other) noexcept = default;

			~TxContext() { clear(); }

			void clear() {
				for (auto &write_event: access_set_) {
					if (write_event.type == TxType::Write) {
						deallocate_data_buffer(write_event.data_ptr, write_event.tuple.get_data_size());
					}
				}
				for (auto &insert_event: insert_set_) {
					deallocate_data_buffer(insert_event.data_ptr, insert_event.size);
				}
				access_set_.clear();
				insert_set_.clear();
				log_amount_ = log_info_size_ = 0;
			}

			static uint8_t *allocate_data_buffer(size_t size) {
				return static_cast<uint8_t *>(std::aligned_alloc(TEMP_DATA_ALIGN_SIZE, size));
			}

			static void deallocate_data_buffer(void *ptr, [[maybe_unused]]size_t size) {
				std::free(ptr);
			}

		public:
			void *look_up_write_set(const AbKeyType &key) {
				// According to partial principle
				for (auto it = access_set_.crbegin(); it != access_set_.crend(); ++it) {
					if (it->key == key) {
						return it->data_ptr;
					}
				}
				return nullptr;
			}

			void *access_write(const AbKeyType &key, IndexTupleType &tuple, uint32_t size, uint32_t offset) {
				DataHeader *data_header_ptr = tuple.get_data_header_ptr();

				if (!data_header_ptr->validate_update(start_ts_)) { return nullptr; }

				auto *version_ptr = data_header_ptr->get_version(start_ts_);
				if (version_ptr == nullptr) { return nullptr; }

				uint32_t total_size = data_header_ptr->get_data_size();
				// Allocate a temp space storing data
				void *data_buffer = allocate_data_buffer(total_size);

				std::memcpy(data_buffer, version_ptr->get_data_ptr(), total_size);

				access_set_.emplace_back(
						Entry {
								.type     = TxType::Write,
								.key      = key,
								.offset   = offset,
								.size     = size,
								.data_ptr = data_buffer,
								.tuple    = tuple
						}
				);
				log_amount_++;
				log_info_size_ += size;
				return data_buffer;
			}

			bool access_insert(const AbKeyType &key, const void *src_ptr, uint32_t size) {
				// Allocate a temp space storing data
				void *data_buffer = allocate_data_buffer(size);
				std::memcpy(data_buffer, src_ptr, size);

				insert_set_.emplace_back(
						Entry {
								.type     = TxType::Insert,
								.key      = key,
								.size     = size,
								.data_ptr = data_buffer
						}
				);
				log_amount_++;
				log_info_size_ += size;
				return true;
			}

			bool access_delete(const AbKeyType &key, IndexTupleType &tuple) {
				{
					access_set_.emplace_back(
							Entry {
									.type  = TxType::Delete,
									.key   = key,
									.tuple = tuple
							}
					);
				}
				log_amount_++;
				return true;
			}

			std::pair<uint64_t, uint64_t> get_log_amount_size() const {
				return {
						log_info_size_,
						log_amount_
				};
			}
		};
	}
}
