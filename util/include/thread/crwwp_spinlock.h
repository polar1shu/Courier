/******************************************************************************
 * Copyright (c) 2014-2017, Pedro Ramalhete, Andreia Correia
 * All rights reserved.
 *
 * Redistribution and use in source and binary forms, with or without
 * modification, are permitted provided that the following conditions are met:
 *     * Redistributions of source code must retain the above copyright
 *       notice, this list of conditions and the following disclaimer.
 *     * Redistributions in binary form must reproduce the above copyright
 *       notice, this list of conditions and the following disclaimer in the
 *       documentation and/or other materials provided with the distribution.
 *     * Neither the name of Concurrency Freaks nor the
 *       names of its contributors may be used to endorse or promote products
 *       derived from this software without specific prior written permission.
 *
 * THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS" AND
 * ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE IMPLIED
 * WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE ARE
 * DISCLAIMED. IN NO EVENT SHALL <COPYRIGHT HOLDER> BE LIABLE FOR ANY
 * DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES
 * (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES;
 * LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND
 * ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT
 * (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE OF THIS
 * SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
 ******************************************************************************
 */

#pragma once

#include <atomic>
#include <stdexcept>
#include <cstdint>
#include <thread>
#include <thread/thread_config.h>

namespace thread {

/**
 * <h1> C-RW-WP </h1>
 *
 * A C-RW-WP reader-writer lock with writer preference and using a
 * spin Lock as Cohort.
 *
 * C-RW-WP paper:         http://dl.acm.org/citation.cfm?id=2442532
 *
 * <p>
 * @author Pedro Ramalhete
 * @author Andreia Correia
 */
	class CRWWPSpinLock {
	public:
		static constexpr int MAX_THREAD_NUM = thread::MAX_TID;

	private:
		class SpinLock {
		private:
			alignas(128) std::atomic<int> writers {0};

		public:
			bool is_locked() {
				return (writers.load()==1);
			}

			void lock() {
				while (!try_lock()) pause();
			}

			bool try_lock() {
				if (writers.load() == 1) { return false; }
				int tmp = 0;
				return writers.compare_exchange_strong(tmp,1);
			}

			void unlock() {
				writers.store(0, std::memory_order_release);
			}
		};

		class RIStaticPerThread {
		private:
			static constexpr uint32_t NOT_READING = 0;
			static constexpr uint32_t READING     = 1;
			static constexpr uint32_t CLPAD       = 64 / sizeof(uint64_t);


			alignas(128) std::atomic<uint64_t>* states;

		public:
			RIStaticPerThread() {
				states = new std::atomic<uint64_t>[MAX_THREAD_NUM * CLPAD];
				for (int tid = 0; tid < MAX_THREAD_NUM; tid++) {
					states[tid * CLPAD].store(NOT_READING, std::memory_order_relaxed);
				}
			}

			~RIStaticPerThread() {
				delete[] states;
			}

			inline void arrive(const uint32_t tid) noexcept {
				states[tid * CLPAD].store(READING);
			}

			inline void depart(const uint32_t tid) noexcept {
				states[tid * CLPAD].store(NOT_READING, std::memory_order_release);
			}

			inline bool is_empty() noexcept {
				for (uint32_t tid = 0; tid < MAX_THREAD_NUM; tid++) {
					if (states[tid * CLPAD].load() != NOT_READING) return false;
				}
				return true;
			}
		};

	private:
		RIStaticPerThread ri {};
		//alignas(128) std::atomic<int> cohort { UNLOCKED };
		SpinLock splock {};

	public:
		CRWWPSpinLock() { }

		void exclusive_lock() {
			splock.lock();
			while (!ri.is_empty()) pause();
		}

		bool try_exclusive_lock() {
			return splock.try_lock();
		}

		void exclusive_unlock() {
			splock.unlock();
		}

		void shared_lock(const uint32_t tid) {
			while (true) {
				ri.arrive(tid);
				if (!splock.is_locked()) break;
				ri.depart(tid);
				while (splock.is_locked()) pause();
			}
		}

		void shared_unlock(const uint32_t tid) {
			ri.depart(tid);
		}

		void wait_for_readers(){
			while (!ri.is_empty()) {} // spin waiting for readers
		}
	};

}
