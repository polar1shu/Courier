/*
 * @author: BL-GS 
 * @date:   2023/6/17
 */

#pragma once

#include <cstddef>
#include <cstdint>

#include <memory/cache_config.h>

inline namespace util_mem {

	enum class PrefetchAim {
		Read  = 0,
		Write = 1
	};

	enum class PrefetchLocality {
		Ultra  = 3,
		High   = 2,
		Middle = 1,
		Low    = 0
	};

	template<PrefetchAim aim, PrefetchLocality locality>
	__attribute__((always_inline)) inline void prefetch(void *ptr) {
		__builtin_prefetch((void *)ptr, static_cast<int>(aim), static_cast<int>(locality));
	}

	__attribute__((always_inline)) inline void prefetch_read_ultra(void *ptr) {
		prefetch<PrefetchAim::Read, PrefetchLocality::Ultra>(ptr);
	}

	__attribute__((always_inline)) inline void prefetch_read_high(void *ptr) {
		prefetch<PrefetchAim::Read, PrefetchLocality::High>(ptr);
	}

	__attribute__((always_inline)) inline void prefetch_read_mid(void *ptr) {
		prefetch<PrefetchAim::Read, PrefetchLocality::Middle>(ptr);
	}

	__attribute__((always_inline)) inline void prefetch_read_low(void *ptr) {
		prefetch<PrefetchAim::Read, PrefetchLocality::Low>(ptr);
	}


	__attribute__((always_inline)) inline void prefetch_write_ultra(void *ptr) {
		prefetch<PrefetchAim::Write, PrefetchLocality::Ultra>(ptr);
	}

	__attribute__((always_inline)) inline void prefetch_write_high(void *ptr) {
		prefetch<PrefetchAim::Write, PrefetchLocality::High>(ptr);
	}

	__attribute__((always_inline)) inline void prefetch_write_mid(void *ptr) {
		prefetch<PrefetchAim::Write, PrefetchLocality::Middle>(ptr);
	}

	__attribute__((always_inline)) inline void prefetch_write_low(void *ptr) {
		prefetch<PrefetchAim::Write, PrefetchLocality::Low>(ptr);
	}



	/*
	 * SSE2/AVX1 only:
	 *
	 * How much data WC buffers can hold at the same time, after which sfence
	 * is needed to flush them.
	 *
	 * For some reason sfence affects performance of reading from DRAM, so we have
	 * to prefetch the source data earlier.
	 */
	constexpr size_t PERF_BARRIER_SIZE = 12 * CACHE_LINE_SIZE;

	/*
	 * How much to prefetch initially.
	 * Cannot be bigger than the size of L1 (32kB) - PERF_BARRIER_SIZE.
	 */
	constexpr size_t INI_PREFETCH_SIZE = 64 * CACHE_LINE_SIZE;

	static inline void prefetch(const uint8_t *addr) {
		_mm_prefetch(addr, _MM_HINT_T0);
	}

	static inline void prefetch_ini_fw(const uint8_t *src, size_t len) {
		size_t pref = std::min(len, INI_PREFETCH_SIZE);
		for (size_t i = 0; i < pref; i += CACHE_LINE_SIZE) {
			prefetch(src + i);
		}
	}

	static inline void prefetch_ini_bw(const uint8_t *src, size_t len) {
		size_t pref = std::min(len, INI_PREFETCH_SIZE);
		for (size_t i = 0; i < pref; i += CACHE_LINE_SIZE) {
			prefetch(src - i);
		}
	}

	static inline void prefetch_next_fw(const uint8_t *src, const uint8_t *srcend) {
		const uint8_t *begin = src + INI_PREFETCH_SIZE;
		const uint8_t *end = begin + PERF_BARRIER_SIZE;
		if (end > srcend) {
			end = srcend;
		}

		for (const uint8_t *addr = begin; addr < end; addr += CACHE_LINE_SIZE) {
			prefetch(addr);
		}
	}

	static inline void prefetch_next_bw(const uint8_t *src, const uint8_t *srcbegin) {
		const uint8_t *begin = src - INI_PREFETCH_SIZE;
		const uint8_t *end = begin - PERF_BARRIER_SIZE;
		if (end < srcbegin) {
			end = srcbegin;
		}

		for (const uint8_t *addr = begin; addr >= end; addr -= CACHE_LINE_SIZE) {
			prefetch(addr);
		}
	}
}
