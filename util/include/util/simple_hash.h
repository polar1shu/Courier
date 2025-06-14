/*
 * @author: BL-GS 
 * @date: 2022/12/13
 * @ref: http://en.wikipedia.org/wiki/Fowler_Noll_Vo_hash
 */

#pragma once

#include <cstdint>

namespace util {

	/*!
	 * @brief Get quick hash from given value
	 * @param val[in] Value to process
	 * @return Hash of given value
	 * @note Specific for 64bits value
	 */
	inline uint64_t fnvhash(uint64_t val) {
		constexpr uint64_t FNV_OFFSET_BASIS_64 = 0xCBF29CE484222325L;
		constexpr uint64_t FNV_PRIME_64 = 1099511628211L;

		uint64_t hashval = FNV_OFFSET_BASIS_64;

		for (int i = 0; i < 8; i++) {
			uint64_t octet = val & 0x00ff;
			val = val >> 8;

			hashval = hashval ^ octet;
			hashval = hashval * FNV_PRIME_64;
		}
		return hashval;
	}

	/*!
	 * @brief Get quick hash from given value
	 * @param val[in] Value to process
	 * @return Hash of given value
	 * @note Specific for 64bits value
	 */
	inline uint32_t fnvhash(uint32_t val) {
		constexpr uint32_t FNV_OFFSET_BASIS_32 = 2166136261;
		constexpr uint32_t FNV_PRIME_32 = 16777619;

		uint32_t hashval = FNV_OFFSET_BASIS_32;

		for (int i = 0; i < 4; i++) {
			uint32_t octet = val & 0x00ff;
			val = val >> 8;

			hashval = hashval ^ octet;
			hashval = hashval * FNV_PRIME_32;
		}
		return hashval;
	}

	/*!
	 * @brief Get quick hash from given value
	 * @param val[in] Value to process
	 * @return Hash of given value
	 * @note Specific for 64bits value
	 */
	inline uint64_t hash(uint64_t val) {
		return fnvhash(val);
	}

	/*!
	 * @brief Get quick hash from given value
	 * @param val[in] Value to process
	 * @return Hash of given value
	 * @note Specific for 32bits value
	 */
	inline uint32_t hash(uint32_t val) {
		return fnvhash(val);
	}

}
