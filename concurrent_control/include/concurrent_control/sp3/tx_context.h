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
#include <concurrent_control/sp3/data_tuple.h>

namespace cc {

	namespace sp {

		enum class TxType {
			Write,
			Insert,
			Delete,
			Read
		};

		template<class AbKey>
			requires AbstractKeyConcept<AbKey>
		struct AccessEntry {
		public:
			using AbKeyType      = AbKey;
			using IndexTupleType = IndexTuple;

		public:
			/// Type of operation
			TxType          type;
			/// Timestamp of operation
			uint64_t        wts;
			/// Key of object
			AbKeyType       key;
			/// Size and ptr as file description
			uint32_t        offset;
			uint32_t        size;
			void *          data_ptr;
			/// Index tuple
			IndexTupleType       tuple;

			auto operator <=> (const AccessEntry &other) const {
				if (key == other.key) { return type <=> other.type; }
				return key <=> other.key;
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

			static constexpr uint64_t LOG_SIZE_PER_ENTRY = offsetof(Entry, data_ptr) + sizeof(Entry::data_ptr) -
															offsetof(Entry, key);

		public:
			enum class Status {
				Running,
				Validating,
				Committing
			};
			Status status_;

			ConcurrentControlPortableMessage message_;
			/// Commit ts
			uint64_t commit_ts_;
			/// The total amount of log tuple
			uint64_t log_amount_;
			/// For all read elements
			std::vector<Entry> read_set_;
			/// For all update/delete elements
			std::vector<Entry> write_set_;
			/// For all insert elements
			std::vector<Entry> insert_set_;

		public:
			TxContext(): commit_ts_(0), log_amount_(0) {}

			TxContext(TxContext &&other) noexcept = default;

			~TxContext() {
				auto &summary_message = ConcurrentControlMessage::get_thread_message();
				summary_message.submit_time(message_);
			}

			static uint8_t *allocate_data_buffer(size_t size) {
				return static_cast<uint8_t *>(std::aligned_alloc(TEMP_DATA_ALIGN_SIZE, size));
			}

			static void deallocate_data_buffer(void *ptr, [[maybe_unused]]size_t size) {
				std::free(ptr);
			}

			void clear(auto &&back_up_clear_func) {
				for (auto &write_event: write_set_) {
					if (write_event.type == TxType::Write && write_event.data_ptr) {
						back_up_clear_func(write_event.key, write_event.data_ptr);
					}
				}
				for (auto &insert_event: insert_set_) {
					deallocate_data_buffer(insert_event.data_ptr, insert_event.size);
				}
				read_set_.clear();
				write_set_.clear();
				insert_set_.clear();

				log_amount_ = 0;
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
				// Read timestamp before copying
				uint64_t wts = tuple.get_wts_ref().load(std::memory_order_acquire);
				// Add it into read set.
				read_set_.emplace_back(
						Entry {
								.type  = TxType::Read,
								.wts   = wts,
								.key   = key,
								.tuple = tuple
						}
				);
				return tuple.get_latest_data_ptr();
			}

			void *access_write(const AbKeyType &key, IndexTupleType &tuple, void *back_up_ptr, uint32_t size, uint32_t offset) {
				// Get wts from header of data tuple, read timestamp before data pointer
				uint64_t wts = tuple.get_wts_ref().load(std::memory_order_acquire);

				write_set_.emplace_back(
					Entry {
						.type     = TxType::Write,
						.wts      = wts,
						.key      = key,
						.offset   = offset,
						.size     = size,
						.data_ptr = back_up_ptr,
						.tuple    = tuple
					}
				);
				++log_amount_;

				return back_up_ptr;
			}

			void *access_insert(const AbKeyType &key, const void *src_ptr, uint32_t size) {
				// Allocate a temp space storing data
				void *data_buffer = allocate_data_buffer(size);
				std::memcpy(data_buffer, src_ptr, size);

				insert_set_.emplace_back(
						Entry {
							.type     = TxType::Insert,
							.wts      = 0,
							.key      = key,
							.size     = size,
							.data_ptr = data_buffer
						}
				);
				++log_amount_;

				return data_buffer;
			}

			bool access_delete(const AbKeyType &key, IndexTupleType &tuple) {
				uint64_t wts = tuple.get_wts_ref().load(std::memory_order_acquire);

				write_set_.emplace_back(
						Entry {
							.type  = TxType::Delete,
							.wts   = wts,
							.key   = key,
							.tuple = tuple
						}
				);
				++log_amount_;
				return true;
			}

			std::pair<uint64_t, uint64_t> get_log_amount_size() const {
				return {
						log_amount_,
						LOG_SIZE_PER_ENTRY * log_amount_
				};
			}

			std::pair<void *, uint64_t> get_log(const Entry &entry) const {
				return {
						(void *)&entry.key,
					LOG_SIZE_PER_ENTRY
				};
			}

		};
	}
}
