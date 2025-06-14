/*
 * @author: BL-GS 
 * @date:   2023/3/13
 */

#pragma once

#include <atomic>

namespace thread {

	class RWLock {
	public:
		static constexpr int W_LOCKED = -1;
		static constexpr int NOT_LOCK = 0;
	public:
		std::atomic<int> counter_;
		// counter == -1, write locked;
		// counter == 0, not locked;
		// counter > 0, there are $counter readers who acquires read-lock.

		RWLock(): counter_(0) {
		}

		void init() {
			counter_.store(0, std::memory_order_release);
		}

		void lock_read() {
			int expected, desired;
			expected = counter_.load(std::memory_order_acquire);
			while (true) {
				if (expected != -1) {
					desired = expected + 1;
				}
				else {
					// wait for writer lock release.
					expected = counter_.load(std::memory_order_acquire);
					continue;
				}
				if (counter_.compare_exchange_weak(expected, desired,
				                                     std::memory_order_acq_rel,
				                                     std::memory_order_acquire)) {
					break;
				}
			}
		}

		bool try_lock_read() {
			int expected, desired;
			expected = counter_.load(std::memory_order_acquire);
			while (true) {
				if (expected != -1) {
					desired = expected + 1;
				}
				else {
					return false;
				}

				if (counter_.compare_exchange_weak(expected, desired,
				                                     std::memory_order_acq_rel,
				                                     std::memory_order_acquire)) {
					return true;
				}
			}
		}

		void unlock_read() {
			counter_.fetch_sub(1, std::memory_order_acq_rel);
		}

		void lock_write() {
			int expected, desired(-1);
			expected = counter_.load(std::memory_order_acquire);
			while (true) {
				if (expected != 0) {
					expected = counter_.load(std::memory_order_acquire);
					continue;
				}
				if (counter_.compare_exchange_weak(expected, desired,
				                                     std::memory_order_acq_rel,
				                                     std::memory_order_acquire)) {
					break;
				}
			}
		}

		bool try_lock_write() {
			int expected, desired(-1);
			expected = counter_.load(std::memory_order_acquire);
			while (true) {
				if (expected != 0) { return false; }
				if (counter_.compare_exchange_weak(expected, desired,
				                                     std::memory_order_acq_rel,
				                                     std::memory_order_acquire)) {
					return true;
				}
			}
		}

		void unlock_write() {
			++counter_;
		}

		bool upgrade() {
			int expected = 1;
			return counter_.compare_exchange_weak(expected, -1,
			                                      std::memory_order_acq_rel);
		}

		int load_acq_counter() {
			return counter_.load(std::memory_order_acquire);
		}

		bool is_locked_write() {
			return load_acq_counter() == W_LOCKED;
		}

		bool is_locked_read() {
			return load_acq_counter() > 0;
		}

		bool is_locked() {
			return load_acq_counter() == NOT_LOCK;
		}
	};
}
