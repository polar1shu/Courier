/*
 * @author: BL-GS
 * @date:   2022/11/23
 */

#pragma once

#include <iostream>
#include <array>
#include <cassert>
#include <numeric>

#include <util/utility_macro.h>

#include <tbb/blocked_range.h>
#include <tbb/parallel_reduce.h>

namespace util {

	template<uint32_t Compress = 32, uint32_t UpperBound = 1024 * 64 - 1>
	class LatencyCounter {
	private:
		std::vector<uint64_t> latency;

	public:
		LatencyCounter(): latency(UpperBound + 1, 0) {}

		~LatencyCounter() = default;

		//! Add latency data into storage
		//! \param tid
		//! \param latency_time
		inline void add_latency(uint64_t latency_time) {
			uint64_t latency_bucket = latency_time / Compress;

			if (latency_bucket > UpperBound) [[unlikely]] {
				latency_bucket = UpperBound;
			}
			++latency[latency_bucket];
		}

		//! Get latency of all threads by tactile alpha
		//! \param num_thread
		//! \param alpha
		//! \return
		inline uint64_t get_latency_summary(int64_t alpha) const {
			int64_t total = 0;
			std::vector<uint64_t> latency_recorder(UpperBound + 1);
			for (int64_t i = 0; i <= UpperBound; ++i) {
				latency_recorder[i] = latency[i];
				total += latency[i];
			}
			return get_tactile(total, alpha, latency_recorder);
		}

		inline uint64_t get_time_summary() {
			return tbb::parallel_reduce(
					tbb::blocked_range<std::vector<uint64_t>::const_iterator>(latency.cbegin(), latency.cend()),
					static_cast<uint64_t>(0),
					[&](const tbb::blocked_range<std::vector<uint64_t>::const_iterator> &range, uint64_t sum){
						for (auto iter = range.begin(); iter != range.end(); ++iter) {
							sum += (*iter) * (iter - latency.begin()) * Compress;
						}
						return sum;
					},
					[](uint64_t a, uint64_t b) {
						return a + b;
					}
			);
		}

		inline void combine(const LatencyCounter &other) {
			for (int64_t i = UpperBound; i >= 0; --i) {
				latency[i] += other.latency[i];
			}
		}

		//! Clear all things in storage
		inline void clear() {
			std::fill(latency.begin(), latency.end(), 0);
		}

	private:
		//! Get latency by tactile
		//! \tparam V Type of Container to store result of tactile
		//! \param total_size Total size of storage
		//! \param alpha
		//! \param container Container to store result
		//! \return latency by tactile
		template<class V>
		uint64_t get_tactile(int64_t total_size, int64_t alpha, const V &container) const {
			DEBUG_ASSERT(alpha < 100 && alpha >= 0);
			int64_t level = (total_size * alpha + 99) / 100;
			DEBUG_ASSERT(level <= total_size);

			uint64_t res = 0;
			for (uint64_t i = 0; i <= UpperBound; ++i) {
				level -= container[i];
				if (level <= 0) {
					res = i + 1;
					break;
				}
			}
			return res * Compress;
		}
	};

}
