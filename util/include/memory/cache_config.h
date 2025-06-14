/*
 * @author: BL-GS 
 * @date:   2023/3/10
 */

#pragma once

#include <cstdint>
#include <immintrin.h>

#include <arch/arch.h>

inline namespace util_mem {

	constexpr uint32_t CACHE_LINE_SIZE = ARCH_CACHE_CACHE_LINE_L1;

	inline constexpr size_t align_to_cache_line(size_t size) {
		return (size + CACHE_LINE_SIZE - 1) / CACHE_LINE_SIZE * CACHE_LINE_SIZE;
	}

}
