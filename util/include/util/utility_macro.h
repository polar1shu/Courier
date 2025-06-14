/*
 * @author: BL-GS
 * @date:   2022/11/23
 */
#pragma once

#include <concepts>
#include <cerrno>
#include <cmath>
#include <immintrin.h>
#include <spdlog/spdlog.h>

inline namespace util_macro {
	/// for bool expression which seems to be true mostly
	inline bool likely(bool input) {
		return __builtin_expect(input, 1);
	}

	/// for bool expression which seems to be false mostly
	inline bool unlikely(bool input) {
		return __builtin_expect(input, 0);
	}

	template<class IntType>
		requires std::is_integral_v<IntType> || std::is_floating_point_v<IntType>
	inline constexpr IntType get_min_among(IntType num) {
		return num;
	}

	template<class IntType, class ...Args>
		requires std::is_integral_v<IntType> || std::is_floating_point_v<IntType>
	inline constexpr IntType get_min_among(IntType num, Args &&...args) {
		return std::min(num, get_min_among(std::forward<Args>(args)...));
	}

	template<class IntType>
		requires std::is_integral_v<IntType> || std::is_floating_point_v<IntType>
	inline constexpr IntType get_max_among(IntType num) {
		return num;
	}

	template<class IntType, class ...Args>
		requires std::is_integral_v<IntType> || std::is_floating_point_v<IntType>
	inline constexpr IntType get_max_among(IntType num, Args &&...args) {
		return std::max(num, get_max_among(std::forward<Args>(args)...));
	}

	#ifndef NDEBUG // Debug
		#ifndef DEBUG_ASSERT
		#define DEBUG_ASSERT(expression, ...)           \
			do {                                        \
				if (!(expression)) {                    \
                    spdlog::error("[{}:{}] Assert fault: {}", __FILE_NAME__, __LINE__, "" __VA_ARGS__); \
                    spdlog::error("Errno: {}", errno);                                                  \
                    assert(0);                                  \
				}                                       \
			} while(0)
		#endif
	#else // Release
		#ifndef DEBUG_ASSERT
			#define DEBUG_ASSERT(expression, ...) static_cast<void>(0)
		#endif
	#endif

	template<class IntType>
	inline constexpr IntType align_floor(IntType obj, size_t aligned_size) {
		return obj / aligned_size * aligned_size;
	}

	template<class IntType>
	inline constexpr IntType align_ceil(IntType obj, size_t aligned_size) {
		return (obj + aligned_size - 1) / aligned_size * aligned_size;
	}

	template<class IntType>
	requires std::is_integral_v<IntType>
	inline constexpr IntType floor_2pow(IntType a, size_t align) {
		return a & (~(align - 1));
	}

	template<class IntType>
	requires std::is_integral_v<IntType>
	inline constexpr IntType ceil_2pow(IntType a, size_t align) {
		return (a + align - 1) & (~(align - 1));
	}

	template<class IntType>
	requires std::is_integral_v<IntType>
	inline constexpr bool is_align_to_2pow(IntType a, size_t align) {
		return a && (a & (align - 1)) == 0;
	}

	template<class IntType>
	requires std::is_integral_v<IntType>
	inline constexpr bool is_2pow(IntType a) {
		return a && (a & (a - 1)) == 0;
	}

	template<class T, class ...Args>
	inline T *construct(void *ptr, Args &&...args) {
		T *type_ptr = static_cast<T *>(ptr);
		new (type_ptr) T(std::forward<Args>(args)...);
		return type_ptr;
	}

	template<class T>
	inline T *deconstruct(T *ptr) {
		ptr->~T();
		return ptr;
	}

}  // namespace util_macro
