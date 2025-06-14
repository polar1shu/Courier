/*
 * @author: BL-GS 
 * @date:   2023/3/10
 */

#pragma once

#include <memory/cache_config.h>
#include <memory/nvm_config.h>

inline namespace util_mem {

	inline static constexpr unsigned long long operator""_bits(unsigned long long num) {
		return (num + 7) / 8;
	}

	inline static constexpr unsigned long long operator""_B(unsigned long long num) {
		return num;
	}

	inline static constexpr unsigned long long operator""_KB(unsigned long long num) {
		return num * 1024;
	}

	inline static constexpr unsigned long long operator""_MB(unsigned long long num) {
		return num * 1024 * 1024;
	}

	inline static constexpr unsigned long long operator""_GB(unsigned long long num) {
		return num * 1024 * 1024 * 1024;
	}

	inline constexpr uint32_t MEM_PAGE_SIZE = ARCH_MEMORY_PAGE_SIZE;

}
