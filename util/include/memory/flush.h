/*
 * @author: BL-GS 
 * @date:   2023/6/17
 */

#pragma once

#include <cstdint>
#include <immintrin.h>

#include <memory/cache_config.h>

inline namespace util_mem {

	#ifndef PWB_DEFINED
		#define PWB_DEFINED CLWB
	#endif
	#define PWB_ENUM Flush::PWB_DEFINED

	enum class Flush {
		CLWB,
		CLFLUSH,
		CLFLUSHOPT
	};

	inline constexpr Flush get_current_pwb_type() {
		return PWB_ENUM;
	}

	__attribute__((always_inline)) inline void clwb(void *target) {
		_mm_clwb(target);
	}

	__attribute__((always_inline)) inline void clflush(void *target) {
		_mm_clflush(target);
	}

	__attribute__((always_inline)) inline void clflushopt(void *target) {
		_mm_clflushopt(target);
	}

	__attribute__((always_inline)) inline void clwb_range(void *start_ptr, uint32_t size) {
		uint8_t *target = static_cast<uint8_t *>(start_ptr);
		for (uint32_t i = 0; i < size; i += CACHE_LINE_SIZE) {
			_mm_clwb(target + i);
		}
	}

	__attribute__((always_inline)) inline void clflush_range(void *start_ptr, uint32_t size) {
		uint8_t *target = static_cast<uint8_t *>(start_ptr);
		for (uint32_t i = 0; i < size; i += CACHE_LINE_SIZE) {
			_mm_clflush(target + i);
		}
	}

	__attribute__((always_inline)) inline void clflushopt_range(void *start_ptr, uint32_t size) {
		uint8_t *target = static_cast<uint8_t *>(start_ptr);
		for (uint32_t i = 0; i < size; i += CACHE_LINE_SIZE) {
			_mm_clflushopt(target + i);
		}
	}

	__attribute__((always_inline)) inline void sfence() {
		asm volatile("sfence" ::: "memory");
	}

	__attribute__((always_inline)) inline void lfence() {
		asm volatile("lfence" ::: "memory");
	}

	__attribute__((always_inline)) inline void mfence() {
		asm volatile("mfence" ::: "memory");
	}

	__attribute__((always_inline)) inline void barrier() {
		asm volatile("" ::: "memory");
	}

}
