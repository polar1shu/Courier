/*
 * @author: BL-GS 
 * @date:   2023/6/10
 */

#pragma once

#include <cstdint>
#include <cstring>
#include <immintrin.h>

#include <util/utility_macro.h>
#include <memory/cache_config.h>

namespace util_mem {
	static inline __m128i mm_loadu_si128(const uint8_t *src, unsigned idx) {
		return _mm_loadu_si128((const __m128i *)src + idx);
	}

	static inline void mm_stream_si128(uint8_t *dest, unsigned idx, __m128i src) {
		_mm_stream_si128((__m128i *)dest + idx, src);
		asm volatile("" ::: "memory");
	}

	static inline void memmove_movnt4x64b(uint8_t *dest, const uint8_t *src) {
		__m128i xmm0 = mm_loadu_si128(src, 0);
		__m128i xmm1 = mm_loadu_si128(src, 1);
		__m128i xmm2 = mm_loadu_si128(src, 2);
		__m128i xmm3 = mm_loadu_si128(src, 3);
		__m128i xmm4 = mm_loadu_si128(src, 4);
		__m128i xmm5 = mm_loadu_si128(src, 5);
		__m128i xmm6 = mm_loadu_si128(src, 6);
		__m128i xmm7 = mm_loadu_si128(src, 7);
		__m128i xmm8 = mm_loadu_si128(src, 8);
		__m128i xmm9 = mm_loadu_si128(src, 9);
		__m128i xmm10 = mm_loadu_si128(src, 10);
		__m128i xmm11 = mm_loadu_si128(src, 11);
		__m128i xmm12 = mm_loadu_si128(src, 12);
		__m128i xmm13 = mm_loadu_si128(src, 13);
		__m128i xmm14 = mm_loadu_si128(src, 14);
		__m128i xmm15 = mm_loadu_si128(src, 15);

		mm_stream_si128(dest, 0, xmm0);
		mm_stream_si128(dest, 1, xmm1);
		mm_stream_si128(dest, 2, xmm2);
		mm_stream_si128(dest, 3, xmm3);
		mm_stream_si128(dest, 4, xmm4);
		mm_stream_si128(dest, 5, xmm5);
		mm_stream_si128(dest, 6, xmm6);
		mm_stream_si128(dest, 7, xmm7);
		mm_stream_si128(dest, 8, xmm8);
		mm_stream_si128(dest, 9, xmm9);
		mm_stream_si128(dest, 10, xmm10);
		mm_stream_si128(dest, 11, xmm11);
		mm_stream_si128(dest, 12, xmm12);
		mm_stream_si128(dest, 13, xmm13);
		mm_stream_si128(dest, 14, xmm14);
		mm_stream_si128(dest, 15, xmm15);
	}

	static inline void memmove_movnt2x64b(uint8_t *dest, const uint8_t *src) {
		__m128i xmm0 = mm_loadu_si128(src, 0);
		__m128i xmm1 = mm_loadu_si128(src, 1);
		__m128i xmm2 = mm_loadu_si128(src, 2);
		__m128i xmm3 = mm_loadu_si128(src, 3);
		__m128i xmm4 = mm_loadu_si128(src, 4);
		__m128i xmm5 = mm_loadu_si128(src, 5);
		__m128i xmm6 = mm_loadu_si128(src, 6);
		__m128i xmm7 = mm_loadu_si128(src, 7);

		mm_stream_si128(dest, 0, xmm0);
		mm_stream_si128(dest, 1, xmm1);
		mm_stream_si128(dest, 2, xmm2);
		mm_stream_si128(dest, 3, xmm3);
		mm_stream_si128(dest, 4, xmm4);
		mm_stream_si128(dest, 5, xmm5);
		mm_stream_si128(dest, 6, xmm6);
		mm_stream_si128(dest, 7, xmm7);
	}

	static inline void memmove_movnt1x64b(uint8_t *dest, const uint8_t *src) {
		__m128i xmm0 = mm_loadu_si128(src, 0);
		__m128i xmm1 = mm_loadu_si128(src, 1);
		__m128i xmm2 = mm_loadu_si128(src, 2);
		__m128i xmm3 = mm_loadu_si128(src, 3);

		mm_stream_si128(dest, 0, xmm0);
		mm_stream_si128(dest, 1, xmm1);
		mm_stream_si128(dest, 2, xmm2);
		mm_stream_si128(dest, 3, xmm3);
	}

	static inline void memmove_movnt1x32b(uint8_t *dest, const uint8_t *src) {
		__m128i xmm0 = mm_loadu_si128(src, 0);
		__m128i xmm1 = mm_loadu_si128(src, 1);

		mm_stream_si128(dest, 0, xmm0);
		mm_stream_si128(dest, 1, xmm1);
	}

	static inline void memmove_movnt1x16b(uint8_t *dest, const uint8_t *src) {
		__m128i xmm0 = mm_loadu_si128(src, 0);

		mm_stream_si128(dest, 0, xmm0);
	}

	static inline void memmove_movnt1x8b(uint8_t *dest, const uint8_t *src) {
		_mm_stream_si64((long long *)dest, *(long long *)src);
	}

	static inline void memmove_movnt1x4b(uint8_t *dest, const uint8_t *src) {
		_mm_stream_si32((int *)dest, *(int *)src);
	}

	static inline void memmove_movnt_sse_fw(uint8_t *dest, const uint8_t *src, size_t len) {
		size_t cnt = (uint64_t)dest & 63;
		if (cnt > 0) {
			cnt = 64 - cnt;

			if (cnt > len)
				cnt = len;

			std::memmove(dest, src, len);
			for (size_t i = 0; i < len; i += CACHE_LINE_SIZE) {
				_mm_clflushopt(dest + i);
			}

			dest += cnt;
			src += cnt;
			len -= cnt;
		}

		const uint8_t *srcend = src + len;
		prefetch_ini_fw(src, len);

		while (len >= PERF_BARRIER_SIZE) {
			prefetch_next_fw(src, srcend);

			memmove_movnt4x64b(dest, src);
			dest += 4 * 64;
			src += 4 * 64;
			len -= 4 * 64;

			memmove_movnt4x64b(dest, src);
			dest += 4 * 64;
			src += 4 * 64;
			len -= 4 * 64;

			memmove_movnt4x64b(dest, src);
			dest += 4 * 64;
			src += 4 * 64;
			len -= 4 * 64;

			static_assert(PERF_BARRIER_SIZE == (4 + 4 + 4) * 64);
		}

		while (len >= 4 * 64) {
			memmove_movnt4x64b(dest, src);
			dest += 4 * 64;
			src += 4 * 64;
			len -= 4 * 64;
		}

		if (len >= 2 * 64) {
			memmove_movnt2x64b(dest, src);
			dest += 2 * 64;
			src += 2 * 64;
			len -= 2 * 64;
		}

		if (len >= 1 * 64) {
			memmove_movnt1x64b(dest, src);

			dest += 1 * 64;
			src += 1 * 64;
			len -= 1 * 64;
		}

		if (len == 0)
			return;

		/* There's no point in using more than 1 nt store for 1 cache line. */
		if (util_macro::is_2pow(len)) {
			if (len == 32)
				memmove_movnt1x32b(dest, src);
			else if (len == 16)
				memmove_movnt1x16b(dest, src);
			else if (len == 8)
				memmove_movnt1x8b(dest, src);
			else if (len == 4)
				memmove_movnt1x4b(dest, src);
			else
				goto nonnt;

			return;
		}

		nonnt:
			std::memcpy(dest, src, len);
			for (size_t i = 0; i < len; i += CACHE_LINE_SIZE) {
				_mm_clflushopt(dest + i);
			}
	}

	static inline void
	memmove_movnt_sse_bw(uint8_t *dest, const uint8_t *src, size_t len) {
		dest += len;
		src += len;

		size_t cnt = (uint64_t)dest & 63;
		if (cnt > 0) {
			if (cnt > len)
				cnt = len;

			dest -= cnt;
			src -= cnt;
			len -= cnt;

			std::memmove(dest, src, len);
			for (size_t i = 0; i < len; i += CACHE_LINE_SIZE) {
				_mm_clflushopt(dest + i);
			}
		}

		const uint8_t *srcbegin = src - len;
		prefetch_ini_bw(src, len);

		while (len >= PERF_BARRIER_SIZE) {
			prefetch_next_bw(src, srcbegin);

			dest -= 4 * 64;
			src -= 4 * 64;
			len -= 4 * 64;
			memmove_movnt4x64b(dest, src);

			dest -= 4 * 64;
			src -= 4 * 64;
			len -= 4 * 64;
			memmove_movnt4x64b(dest, src);

			dest -= 4 * 64;
			src -= 4 * 64;
			len -= 4 * 64;
			memmove_movnt4x64b(dest, src);

			static_assert(PERF_BARRIER_SIZE == (4 + 4 + 4) * 64);
		}

		while (len >= 4 * 64) {
			dest -= 4 * 64;
			src -= 4 * 64;
			len -= 4 * 64;
			memmove_movnt4x64b(dest, src);
		}

		if (len >= 2 * 64) {
			dest -= 2 * 64;
			src -= 2 * 64;
			len -= 2 * 64;
			memmove_movnt2x64b(dest, src);
		}

		if (len >= 1 * 64) {
			dest -= 1 * 64;
			src -= 1 * 64;
			len -= 1 * 64;
			memmove_movnt1x64b(dest, src);
		}

		if (len == 0)
			return;

		/* There's no point in using more than 1 nt store for 1 cache line. */
		if (util_macro::is_2pow(len)) {
			if (len == 32) {
				dest -= 32;
				src -= 32;
				memmove_movnt1x32b(dest, src);
			}
			else if (len == 16) {
				dest -= 16;
				src -= 16;
				memmove_movnt1x16b(dest, src);
			}
			else if (len == 8) {
				dest -= 8;
				src -= 8;
				memmove_movnt1x8b(dest, src);
			}
			else if (len == 4) {
				dest -= 4;
				src -= 4;
				memmove_movnt1x4b(dest, src);
			}
			else {
				goto nonnt;
			}

			return;
		}

		nonnt:
			dest -= len;
			src -= len;
			std::memcpy(dest, src, len);
			for (size_t i = 0; i < len; i += CACHE_LINE_SIZE) {
				_mm_clflushopt(dest + i);
			}
	}

	static inline void memmove_movnt_sse2(uint8_t *dest, const uint8_t *src, size_t len) {
		if ((uintptr_t)dest - (uintptr_t)src >= len) {
			memmove_movnt_sse_fw(dest, src, len);
		}
		else {
			memmove_movnt_sse_bw(dest, src, len);
		}
	}

	static inline void memcpy_movnt4x64b(uint8_t * __restrict dest, const uint8_t * __restrict src) {
		__m128i xmm0 = mm_loadu_si128(src, 0);
		__m128i xmm1 = mm_loadu_si128(src, 1);
		__m128i xmm2 = mm_loadu_si128(src, 2);
		__m128i xmm3 = mm_loadu_si128(src, 3);
		__m128i xmm4 = mm_loadu_si128(src, 4);
		__m128i xmm5 = mm_loadu_si128(src, 5);
		__m128i xmm6 = mm_loadu_si128(src, 6);
		__m128i xmm7 = mm_loadu_si128(src, 7);
		__m128i xmm8 = mm_loadu_si128(src, 8);
		__m128i xmm9 = mm_loadu_si128(src, 9);
		__m128i xmm10 = mm_loadu_si128(src, 10);
		__m128i xmm11 = mm_loadu_si128(src, 11);
		__m128i xmm12 = mm_loadu_si128(src, 12);
		__m128i xmm13 = mm_loadu_si128(src, 13);
		__m128i xmm14 = mm_loadu_si128(src, 14);
		__m128i xmm15 = mm_loadu_si128(src, 15);

		mm_stream_si128(dest, 0, xmm0);
		mm_stream_si128(dest, 1, xmm1);
		mm_stream_si128(dest, 2, xmm2);
		mm_stream_si128(dest, 3, xmm3);
		mm_stream_si128(dest, 4, xmm4);
		mm_stream_si128(dest, 5, xmm5);
		mm_stream_si128(dest, 6, xmm6);
		mm_stream_si128(dest, 7, xmm7);
		mm_stream_si128(dest, 8, xmm8);
		mm_stream_si128(dest, 9, xmm9);
		mm_stream_si128(dest, 10, xmm10);
		mm_stream_si128(dest, 11, xmm11);
		mm_stream_si128(dest, 12, xmm12);
		mm_stream_si128(dest, 13, xmm13);
		mm_stream_si128(dest, 14, xmm14);
		mm_stream_si128(dest, 15, xmm15);
	}

	static inline void memcpy_movnt2x64b(uint8_t * __restrict dest, const uint8_t * __restrict src) {
		__m128i xmm0 = mm_loadu_si128(src, 0);
		__m128i xmm1 = mm_loadu_si128(src, 1);
		__m128i xmm2 = mm_loadu_si128(src, 2);
		__m128i xmm3 = mm_loadu_si128(src, 3);
		__m128i xmm4 = mm_loadu_si128(src, 4);
		__m128i xmm5 = mm_loadu_si128(src, 5);
		__m128i xmm6 = mm_loadu_si128(src, 6);
		__m128i xmm7 = mm_loadu_si128(src, 7);

		mm_stream_si128(dest, 0, xmm0);
		mm_stream_si128(dest, 1, xmm1);
		mm_stream_si128(dest, 2, xmm2);
		mm_stream_si128(dest, 3, xmm3);
		mm_stream_si128(dest, 4, xmm4);
		mm_stream_si128(dest, 5, xmm5);
		mm_stream_si128(dest, 6, xmm6);
		mm_stream_si128(dest, 7, xmm7);
	}

	static inline void memcpy_movnt1x64b(uint8_t * __restrict dest, const uint8_t * __restrict src) {
		__m128i xmm0 = mm_loadu_si128(src, 0);
		__m128i xmm1 = mm_loadu_si128(src, 1);
		__m128i xmm2 = mm_loadu_si128(src, 2);
		__m128i xmm3 = mm_loadu_si128(src, 3);

		mm_stream_si128(dest, 0, xmm0);
		mm_stream_si128(dest, 1, xmm1);
		mm_stream_si128(dest, 2, xmm2);
		mm_stream_si128(dest, 3, xmm3);
	}

	static inline void memcpy_movnt1x32b(uint8_t * __restrict dest, const uint8_t * __restrict src) {
		__m128i xmm0 = mm_loadu_si128(src, 0);
		__m128i xmm1 = mm_loadu_si128(src, 1);

		mm_stream_si128(dest, 0, xmm0);
		mm_stream_si128(dest, 1, xmm1);
	}

	static inline void memcpy_movnt1x16b(uint8_t * __restrict dest, const uint8_t * __restrict src) {
		__m128i xmm0 = mm_loadu_si128(src, 0);

		mm_stream_si128(dest, 0, xmm0);
	}

	static inline void memcpy_movnt1x8b(uint8_t * __restrict dest, const uint8_t * __restrict src) {
		_mm_stream_si64((long long *)dest, *(long long *)src);
	}

	static inline void memcpy_movnt1x4b(uint8_t * __restrict dest, const uint8_t * __restrict src) {
		_mm_stream_si32((int *)dest, *(int *)src);
	}


	static inline void memcpy_movnt_sse_fw(uint8_t * __restrict dest, const uint8_t * __restrict src, size_t len) {
		size_t cnt = (uint64_t)dest & 63;
		if (cnt > 0) {
			cnt = 64 - cnt;

			if (cnt > len)
				cnt = len;

			std::memcpy(dest, src, len);
			for (size_t i = 0; i < len; i += CACHE_LINE_SIZE) {
				_mm_clflushopt(dest + i);
			}

			dest += cnt;
			src += cnt;
			len -= cnt;
		}

		const uint8_t *srcend = src + len;
		prefetch_ini_fw(src, len);

		while (len >= PERF_BARRIER_SIZE) {
			prefetch_next_fw(src, srcend);

			memcpy_movnt4x64b(dest, src);
			dest += 4 * 64;
			src += 4 * 64;
			len -= 4 * 64;

			memcpy_movnt4x64b(dest, src);
			dest += 4 * 64;
			src += 4 * 64;
			len -= 4 * 64;

			memcpy_movnt4x64b(dest, src);
			dest += 4 * 64;
			src += 4 * 64;
			len -= 4 * 64;

			static_assert(PERF_BARRIER_SIZE == (4 + 4 + 4) * 64);
		}

		while (len >= 4 * 64) {
			memcpy_movnt4x64b(dest, src);
			dest += 4 * 64;
			src += 4 * 64;
			len -= 4 * 64;
		}

		if (len >= 2 * 64) {
			memcpy_movnt2x64b(dest, src);
			dest += 2 * 64;
			src += 2 * 64;
			len -= 2 * 64;
		}

		if (len >= 1 * 64) {
			memcpy_movnt1x64b(dest, src);

			dest += 1 * 64;
			src += 1 * 64;
			len -= 1 * 64;
		}

		if (len == 0)
			return;

		/* There's no point in using more than 1 nt store for 1 cache line. */
		if (util_macro::is_2pow(len)) {
			if (len == 32)
				memcpy_movnt1x32b(dest, src);
			else if (len == 16)
				memcpy_movnt1x16b(dest, src);
			else if (len == 8)
				memcpy_movnt1x8b(dest, src);
			else if (len == 4)
				memcpy_movnt1x4b(dest, src);
			else
				goto nonnt;

			return;
		}

		nonnt:
		std::memcpy(dest, src, len);
		for (size_t i = 0; i < len; i += CACHE_LINE_SIZE) {
			_mm_clflushopt(dest + i);
		}
	}


	static inline void memcpy_movnt_sse2(uint8_t * __restrict dest, const uint8_t * __restrict src, size_t len) {
		memcpy_movnt_sse_fw(dest, src, len);
	}
}
