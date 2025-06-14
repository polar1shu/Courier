/*
 * @author: BL-GS 
 * @date:   2023/12/30
 */

#pragma once

#include <span>
#include <queue>
#include <functional>

#include <concurrent_control/courier_save/log_persist.h>

namespace cc::courier_save {

	template<class AbKey>
	class RecoveryManager {
	public:
		using LogPersistType = LogPersist<AbKey>;

		static constexpr auto LOG_PAGE_SIZE = LogPersistType::LOG_PAGE_SIZE;

	private:
		std::function<void(uint64_t, AbKey, uint32_t, uint32_t, const void *)> update_func_;
		std::function<void(uint64_t, AbKey, uint32_t, const void *)> insert_func_;

	public:
		RecoveryManager(std::function<void(uint64_t , AbKey, uint32_t, uint32_t, const void *)> update_func,
				std::function<void(uint64_t, AbKey, uint32_t, const void *)> insert_func):
			update_func_(update_func), insert_func_(insert_func) {}

	public:
		void recovery(uint64_t page_num, std::span<uint8_t> log_bitmap, std::span<uint8_t> log_pool) {
			for (uint64_t page_idx = 0; page_idx < page_num; ++page_idx) {
				const uint64_t bitmask_uint8_idx = page_idx / 8;
				const uint64_t bitmask_bit_idx   = page_idx % 8;
				if ((log_bitmap[bitmask_uint8_idx] & (1 << bitmask_bit_idx)) != 0) {
					recovery_page_iteration(log_pool.data() + page_idx * LOG_PAGE_SIZE);
				}
			}
		}

	private:
		void recovery_page_iteration(const void *start_ptr) {
			const void *end_ptr = static_cast<const uint8_t *>(start_ptr) + LOG_PAGE_SIZE;

			const void *iter = start_ptr;

			std::queue<std::function<void()>> recovery_queue;

			while (iter < end_ptr) {
				auto abstract_log_tuple_ptr = static_cast<const LogTuple<LogLabel::None, AbKey> *>(iter);

				switch (abstract_log_tuple_ptr->label_) {
					case LogLabel::Commit:
						while (!recovery_queue.empty()) {
							recovery_queue.front()();
							recovery_queue.pop();
						}

						iter = static_cast<const uint8_t *>(iter) + sizeof(LogTuple<LogLabel::Commit, AbKey>);
						break;
					case LogLabel::Update: {
						auto log_tuple_ptr = static_cast<const LogTuple<LogLabel::Update, AbKey> *>(iter);
						auto size = log_tuple_ptr->size_ + sizeof(LogTuple<LogLabel::Update, AbKey>);

						recovery_queue.push([=, this]{
							update_func_(log_tuple_ptr->ts_, log_tuple_ptr->key_, log_tuple_ptr->size_, log_tuple_ptr->offset_, log_tuple_ptr->extra_info_);
						});

						iter = static_cast<const uint8_t *>(iter) + size;
						break;
					}
					case LogLabel::Insert: {
						auto log_tuple_ptr = static_cast<const LogTuple<LogLabel::Insert, AbKey> *>(iter);
						auto size = log_tuple_ptr->size_ + sizeof(LogTuple<LogLabel::Insert, AbKey>);

						recovery_queue.push([=, this]{
							insert_func_(log_tuple_ptr->ts_, log_tuple_ptr->key_, log_tuple_ptr->size_, log_tuple_ptr->extra_info_);
						});

						iter = static_cast<const uint8_t *>(iter) + size;
						break;
					}

					default: // Unrecognized Label
						iter = end_ptr;
				}
			}

		}

	};

	template<class AbKey>
	struct RecoveryTransaction {
	private:
		AbKey       key_;
		uint32_t    size_;
		uint32_t    offset_;
		uint64_t    ts_;
		uint8_t *   data_ptr_;

	public:
		RecoveryTransaction(): size_(0), offset_(0), ts_(0), data_ptr_(nullptr) {}

		RecoveryTransaction(AbKey key, uint32_t size, uint32_t offset, uint32_t ts, uint8_t *data_ptr):
			key_(key), size_(size), offset_(offset), ts_(ts), data_ptr_(data_ptr) {}

		RecoveryTransaction(const RecoveryTransaction &other) noexcept :
			key_(other.key_), size_(other.size_), offset_(other.offset_), ts_(other.ts_), data_ptr_(other.data_ptr_) {
		}

		RecoveryTransaction &operator= (const RecoveryTransaction &other) noexcept {
			key_            = other.key_;
			size_           = other.size_;
			offset_         = other.offset_;
			ts_             = other.ts_;
			data_ptr_       = other.data_ptr_;
			return *this;
		}

	public:
		template<class Executor>
		bool run(Executor &executor) {
			return executor.update(key_, data_ptr_ - offset_, size_, offset_);
		}
	};
}