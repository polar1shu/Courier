/*
 * @author: BL-GS 
 * @date:   2023/1/2
 */

#pragma once

#include <cstdint>
#include <mutex>
#include <vector>
#include <queue>
#include <utility>

#include <concurrent_control/config.h>
#include <concurrent_control/tpl/data_tuple.h>

namespace cc {

	namespace tpl {

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
			TxType          type;
			/// Key of object
			AbKeyType       key;
			/// Size and ptr as file description
			uint32_t        offset;
			uint32_t        size;
			void *          data_ptr;
			/// Index tuple
			IndexTupleType       tuple;

			auto operator <=> (const AccessEntry &other) const {
				return key <=> other.key;
			}

			bool operator == (const AccessEntry &other) const {
				return key == other.key;
			}
		};

		template<class AbKey>
			requires AbstractKeyConcept<AbKey>
		struct TxContext {
		public:
			using AbKeyType      = AbKey;
			using IndexTupleType = IndexTuple;
			using Entry          = AccessEntry<AbKeyType>;

			static constexpr size_t TEMP_DATA_ALIGN_SIZE = 16;

		public:
			enum class Status {
				Running,
				Validating,
				Committing
			};
			Status status_;

			ConcurrentControlPortableMessage message_;
			/// The total size of log info
			uint64_t log_info_size_;
			/// The total amount of log tuple
			uint64_t log_amount_;
			/// For all read elements
			std::vector<Entry> read_set_;
			/// For all update/delete elements
			std::vector<Entry> write_set_;
			/// For all insert elements
			std::vector<Entry> insert_set_;

		public:
			TxContext() {
				read_set_.reserve(64);
				write_set_.reserve(64);
				insert_set_.reserve(64);
			}

			TxContext(TxContext &&other) noexcept = default;

			~TxContext() {
				clear();
				auto &summary_message = ConcurrentControlMessage::get_thread_message();
				summary_message.submit_time(message_);
			}

			static uint8_t *allocate_data_buffer(size_t size) {
				return static_cast<uint8_t *>(std::aligned_alloc(TEMP_DATA_ALIGN_SIZE, size));
			}

			static void deallocate_data_buffer(void *ptr, [[maybe_unused]]size_t size) {
				std::free(ptr);
			}

			void clear() {
				for (auto &read_event: read_set_) {
					read_event.tuple.unlock_read();
				}
				for (auto &write_event: write_set_) {
					write_event.tuple.unlock_write();
					if (write_event.type == TxType::Write) {
						deallocate_data_buffer(write_event.data_ptr, write_event.tuple.get_data_size());
					}
				}
				for (auto &insert_event: insert_set_) {
					deallocate_data_buffer(insert_event.data_ptr, insert_event.size);
				}
				read_set_.clear();
				write_set_.clear();
				insert_set_.clear();
			}

		public:
			void *look_up_write_set(const AbKeyType &key) {
				// According to partial principle
				for (auto it = write_set_.crbegin(); it != write_set_.crend(); ++it) {
					if (it->key == key) {
						return it->data_ptr;
					}
				}
				return nullptr;
			}

			void *access_read(const AbKeyType &key, IndexTupleType &tuple) {
				if (!tuple.try_lock_read()) { return nullptr; }
				// Add it into read set.
				read_set_.emplace_back(
						Entry {
								.type  = TxType::Read,
								.key   = key,
								.tuple = tuple
						}
				);
				return tuple.get_data_ptr();
			}

			void *access_write(const AbKeyType &key, IndexTupleType &tuple, uint32_t size, uint32_t offset) {
				if (!tuple.try_lock_write()) { return nullptr; }
				// Allocate a temp space storing data
				uint32_t data_size = tuple.get_data_size();
				uint8_t *data_buffer = allocate_data_buffer(data_size);
				// Read data
				std::memcpy(data_buffer, tuple.get_data_ptr(), data_size);

				write_set_.emplace_back(
						Entry {
								.type     = TxType::Write,
								.key      = key,
								.offset   = offset,
								.size     = size,
								.data_ptr = data_buffer,
								.tuple    = tuple
						}
				);
				++log_amount_;
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
				++log_amount_;
				log_info_size_ += size;
				return true;
			}

			bool access_delete(const AbKeyType &key, IndexTupleType &tuple) {
				if (!tuple.try_lock_write()) { return false; }
				{
					write_set_.emplace_back(
							Entry {
								.type  = TxType::Delete,
								.key   = key,
								.tuple = tuple
							}
					);
				}
				++log_amount_;
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
