/*
 * @author: BL-GS 
 * @date:   2023/2/18
 */

#pragma once

#include <atomic>
#include <thread>

#include <thread/thread.h>

namespace cc::romulus {
	class CRWWPSpinLock {

	private:
		class SpinLock {
			alignas(128) std::atomic<int> writers {0};
		public:
			inline bool is_locked() { return (writers.load() != 0); }
			inline void lock() {
				while (!try_lock()) { std::this_thread::yield(); }
			}
			inline bool try_lock() {
				if (writers.load() != 0) return false;
				int tmp = 0;
				return writers.compare_exchange_strong(tmp,2);
			}
			inline void unlock() {
				writers.store(0, std::memory_order_release);
			}
		};

		class RIStaticPerThread {
		private:
			static constexpr uint32_t MAX_THREAD_NUM = thread::get_max_tid();

			static constexpr uint64_t NOT_READING = 0;
			static constexpr uint64_t READING = 1;

			static constexpr int CLPAD = 128 / sizeof(uint64_t);

		public:
			alignas(128) std::atomic<uint64_t>* states;

		public:
			RIStaticPerThread() {
				states = new std::atomic<uint64_t>[MAX_THREAD_NUM * CLPAD];
				for (uint32_t tid = 0; tid < MAX_THREAD_NUM; tid++) {
					states[tid*CLPAD].store(NOT_READING, std::memory_order_relaxed);
				}
			}

			~RIStaticPerThread() {
				delete[] states;
			}

			inline void arrive(const uint32_t tid) noexcept {
				states[tid*CLPAD].store(READING);
			}

			inline void depart(const uint32_t tid) noexcept {
				states[tid*CLPAD].store(NOT_READING, std::memory_order_release);
			}

			inline bool is_empty() noexcept {
				for (uint32_t tid = 0; tid < MAX_THREAD_NUM; tid++) {
					if (states[tid * CLPAD].load() != NOT_READING) return false;
				}
				return true;
			}
		};

		SpinLock splock {};
		RIStaticPerThread ri {};

	public:
		inline void exclusiveLock() {
			splock.lock();
			while (!ri.is_empty()) { std::this_thread::yield(); }
		}

		inline bool try_exclusive_lock() {
			return splock.try_lock();
		}

		inline void exclusive_unlock() {
			splock.unlock();
		}

		inline void shared_lock(const uint32_t tid) {
			while (true) {
				ri.arrive(tid);
				if (!splock.is_locked()) break;
				ri.depart(tid);
				while (splock.is_locked()) { std::this_thread::yield(); }
			}
		}

		inline void shared_unlock(const uint32_t tid) {
			ri.depart(tid);
		}

		inline void wait_for_readers(){
			while (!ri.is_empty()) {} // spin waiting for readers
		}
	};
} // namespace cc::romulus
