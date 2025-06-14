/*
 * @author: BL-GS 
 * @date:   2023/1/2
 */

#pragma once

#include <cstdint>
#include <vector>
#include <utility>

#include <concurrent_control/abstract_concurrent_control.h>
#include <concurrent_control/config.h>
#include <concurrent_control/courier_save/data_tuple.h>
#include <concurrent_control/courier_save/vheader_cache.h>
#include <concurrent_control/courier_save/log.h>

namespace cc::courier_save {

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
		using KeyType        = AbKeyType::MainKeyType;
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

		std::shared_mutex *shared_handler_ptr = nullptr;

		auto operator <=> (const AccessEntry &other) const {
			return key <=> other.key;
		}
	};

	template<class AbKey>
		requires AbstractKeyConcept<AbKey>
	struct AccessEntry<AbKey, TxType::Read> {
	public:
		using AbKeyType      = AbKey;
		using KeyType        = AbKeyType::MainKeyType;
		using IndexTupleType = IndexTuple;

	public:
		/// Timestamp of operation
		uint64_t        wts;
		/// Key of object
		AbKeyType       key;
		/// Index tuple
		IndexTupleType  tuple;

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

		static constexpr size_t TEMP_DATA_ALIGN_SIZE = 16;

	public: // Declare static thread local parameter here for no initialization when no use this concurrent control

		enum class Status {
			Running,
			Validating,
			Committing,
			Abort
		};
		Status status_;

		ConcurrentControlPortableMessage message_;
		/// The total size of log info
		uint32_t log_info_size_;
		/// For all read elements
		std::vector<ReadEntry> read_set_;
		/// For all update/insert/delete elements
		std::vector<WriteEntry> write_set_;

	public:
		TxContext(): log_info_size_(0) { }

		TxContext(TxContext &&other) noexcept = default;

		~TxContext() {
			for (auto &write_event: write_set_) {
				if (write_event.type == TxType::Write || write_event.type == TxType::Insert) [[likely]] {
					delete[] static_cast<uint8_t *>(write_event.data_ptr);
				}
			}
			// Do not deallocate buffer in insert set, which is reserved for virtual data.
			submit_message();
		}

		void submit_message() {
			auto &summary_message = ConcurrentControlMessage::get_thread_message();
			summary_message.submit_time(message_);
		}

		void clear_abort() {
			for (auto &write_event: write_set_) {
				if (write_event.type == TxType::Write || write_event.type == TxType::Insert) [[likely]] {
					delete[] static_cast<uint8_t *>(write_event.data_ptr);
				}
			}
			read_set_.clear();
			write_set_.clear();

			log_info_size_ = 0;
		}

	public:
		void *look_up_write_set(const AbKeyType &key) {
			for (const WriteEntry &entry: write_set_) {
				if (entry.key == key) {
					return entry.data_ptr;
				}
			}
			return nullptr;
		}

		void *access_read(const AbKeyType &key, IndexTupleType &tuple) {
			// Get wts from header of data tuple
			uint64_t wts = tuple.get_wts();
			// Add it into read set
			read_set_.emplace_back(
					ReadEntry {
							.wts   = wts,
							.key   = key,
							.tuple = tuple
					}
			);
			return tuple.get_virtual_data_ptr();
		}

		void *access_write(const AbKeyType &key, IndexTupleType &tuple, uint32_t size, uint32_t offset) {
			uint32_t data_size = tuple.get_data_size();
			// Allocate a temp space storing data
			uint8_t *data_buffer = new uint8_t[data_size];
			// Get wts from header of data tuple, read timestamp before data pointer
			DataTupleVirtualHeader *header_ptr = tuple.get_data_header_ptr();
			uint64_t wts = header_ptr->get_wts();
			// Read data
			std::memcpy(
					data_buffer,
					tuple.get_virtual_data_ptr(),
					data_size
			);

			// Add it into write set
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
			// Register log
			log_info_size_ += sizeof(LogTuple<LogLabel::Update, AbKeyType>) + size;

			return data_buffer;
		}

		bool access_insert(const AbKeyType &key, const void *src_ptr, uint32_t size) {
			// Allocate a temp space storing data
			void *data_buffer = new uint8_t[size];
			std::memcpy(data_buffer, src_ptr, size);
			// Add it into insert set
			write_set_.emplace_back(
					WriteEntry {
						.type     = TxType::Insert,
						.key      = key,
						.size     = size,
						.data_ptr = data_buffer
					}
			);
			// Register log
			log_info_size_ += sizeof(LogTuple<LogLabel::Insert, AbKeyType>) + size;
			return true;
		}

		bool access_delete(const AbKeyType &key, IndexTupleType &tuple) {
			// Get wts from header of data tuple
			uint64_t wts = tuple.get_wts();
			// Add it into write set
			write_set_.emplace_back(
					WriteEntry {
						.type  = TxType::Delete,
						.wts   = wts,
						.key   = key,
						.tuple = tuple
					}
			);
			// Register log
			log_info_size_ += sizeof(LogTuple<LogLabel::Delete, AbKeyType>);
			return true;
		}

		uint32_t get_log_amount_size() const {
			return log_info_size_;
		}
	};

}
