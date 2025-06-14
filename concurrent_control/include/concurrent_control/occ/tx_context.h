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
#include <concurrent_control/occ/data_tuple.h>

namespace cc::occ {

	enum class TxType {
		Write,
		Insert,
		Delete,
		Read
	};

	template<class AbKey, TxType Type>
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
			return key <=> other.key;
		}
	};

	template<class AbKey>
		requires AbstractKeyConcept<AbKey>
	struct AccessEntry<AbKey, TxType::Read> {
	public:
		using AbKeyType      = AbKey;
		using IndexTupleType = IndexTuple;

	public:
		/// Timestamp of operation
		uint64_t        wts;
		/// Key of object
		AbKeyType       key;
		/// Index tuple
		IndexTupleType       tuple;

		auto operator <=> (const AccessEntry &other) const {
			return key <=> other.key;
		}
	};

	template<class AbKey>
		requires AbstractKeyConcept<AbKey>
	struct AccessEntry<AbKey, TxType::Insert> {
	public:
		using AbKeyType      = AbKey;
		using IndexTupleType = IndexTuple;

	public:
		/// Timestamp of operation
		uint64_t        wts;
		/// Key of object
		AbKeyType       key;
		uint32_t        size;
		void *          data_ptr;

		auto operator <=> (const AccessEntry &other) const {
			return key <=> other.key;
		}
	};


	template<class AbKey>
		requires AbstractKeyConcept<AbKey>
	struct TxContext {
	public:
		using AbKeyType      = AbKey;
		using KeyType        = AbKeyType::MainKeyType;

		using IndexTupleType = IndexTuple;
		using ReadEntry      = AccessEntry<AbKeyType, TxType::Read>;
		using WriteEntry     = AccessEntry<AbKeyType, TxType::Write>;
		using InsertEntry    = AccessEntry<AbKeyType, TxType::Insert>;

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
		/// The total size of log info
		uint64_t log_info_size_;
		/// The total amount of log tuple
		uint64_t log_amount_;
		/// For all read elements
		std::vector<ReadEntry> read_set_;
		/// For all update/delete elements
		std::vector<WriteEntry> write_set_;
		/// For all insert elements
		std::vector<InsertEntry> insert_set_;

	public:
		TxContext(): commit_ts_(0), log_info_size_(0), log_amount_(0) {
			read_set_.reserve(64);
			write_set_.reserve(16);
		}

		TxContext(TxContext &&other) noexcept = default;

		~TxContext() {
			for (auto &write_event: write_set_) {
				if (write_event.type == TxType::Write) {
					deallocate_data_buffer(write_event.data_ptr, write_event.tuple.get_data_size());
				}
			}
			for (auto &insert_event: insert_set_) {
				deallocate_data_buffer(insert_event.data_ptr, insert_event.size);
			}

			auto &summary_message = ConcurrentControlMessage::get_thread_message();
			summary_message.submit_time(message_);
		}

		uint8_t *allocate_data_buffer(size_t size) {
			return (uint8_t *)std::malloc(size);
		}

		void deallocate_data_buffer(void *ptr, size_t size) {
			std::free(ptr);
		}

		void clear() {
			for (auto &write_event: write_set_) {
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

			log_amount_ = log_info_size_ = 0;
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
			tuple.lock_read();
			uint64_t wts = tuple.get_wts_ref().load(std::memory_order_acquire);
			tuple.unlock_read();
			// Add it into read set.
			read_set_.emplace_back(
					ReadEntry {
							.wts   = wts,
							.key   = key,
							.tuple = tuple
					}
			);
			return tuple.get_data_ptr();
		}

		void *access_write(const AbKeyType &key, IndexTupleType &tuple, uint32_t size, uint32_t offset) {
			// Allocate a temp space storing data
			uint8_t *data_buffer = allocate_data_buffer(tuple.get_data_size());
			// Get wts from header of data tuple, read timestamp before data pointer
			tuple.lock_read();
			uint64_t wts = tuple.get_wts_ref().load(std::memory_order_acquire);
			tuple.unlock_read();
			// Read data
			std::memcpy(data_buffer, tuple.get_data_ptr(), tuple.data_size_);

			write_set_.emplace_back(
				WriteEntry {
					.type     = TxType::Write,
					.wts      = wts,
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

		void *access_insert(const AbKeyType &key, const void *src_ptr, uint32_t size) {
			// Allocate a temp space storing data
			void *data_buffer = allocate_data_buffer(size);
			std::memcpy(data_buffer, src_ptr, size);

			insert_set_.emplace_back(
					InsertEntry {
						.key      = key,
						.size     = size,
						.data_ptr = data_buffer
					}
			);
			++log_amount_;
			log_info_size_ += size;

			return data_buffer;
		}

		bool access_delete(const AbKeyType &key, IndexTupleType &tuple) {
			uint64_t wts = tuple.get_wts_ref().load(std::memory_order_acquire);

			write_set_.emplace_back(
					WriteEntry {
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
					log_info_size_
			};
		}

	};
}
