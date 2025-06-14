/*
 * @author: BL-GS 
 * @date:   2023/4/14
 */

#pragma once

#include <cstdint>
#include <limits>
#include <concepts>

#include <util/utility_macro.h>

namespace workload {

	template<class MainKey>
	requires std::is_copy_assignable_v<MainKey> &&
	         std::is_arithmetic_v<MainKey>
	struct AbstractKey {

	public:
		using TypeInoType = uint32_t;

		using MainKeyType = MainKey;

	public:
		TypeInoType type_;

		MainKey logic_key_;

	public:
		AbstractKey(): type_(std::numeric_limits<TypeInoType>::max()) {}

		template<class TypeIno>
		AbstractKey(const TypeIno &type_ino, MainKey logic_key):
				type_(static_cast<TypeInoType>(type_ino)), logic_key_(logic_key) {}

		AbstractKey(const AbstractKey &other) = default;

		AbstractKey &operator= (const AbstractKey &other) = default;

	public:
		TypeInoType get_type_ino() const {
			return type_;
		}

		MainKeyType get_main_key() const {
			return logic_key_;
		}

		void set_type_ino(TypeInoType new_type) {
			type_ = new_type;
		}

		void set_main_key(MainKeyType main_key) {
			logic_key_ = main_key;
		}

	public:
		AbstractKey operator+(const MainKey &num) const {
			return {
					type_,
					logic_key_ + num
			};
		}

		AbstractKey operator-(const MainKey &num) const {
			return {
					type_,
					logic_key_ - num
			};
		}

		void operator+=(const MainKey &num) {
			logic_key_ += num;
		}

		void operator-=(const MainKey &num) {
			logic_key_ -= num;
		}

	public:
		AbstractKey operator+(const AbstractKey &other) const {
			DEBUG_ASSERT(type_ == other.type_);
			return {
					type_,
					logic_key_ + other.logic_key_
			};
		}

		AbstractKey operator-(const AbstractKey &other) const {
			DEBUG_ASSERT(type_ == other.type_);
			return {
					type_,
					logic_key_ - other.logic_key_
			};
		}

		void operator+=(const AbstractKey &other) {
			DEBUG_ASSERT(type_ == other.type_);
			logic_key_ += other.logic_key_;
		}

		void operator-=(const AbstractKey &other) {
			DEBUG_ASSERT(type_ == other.type_);
			logic_key_ -= other.logic_key_;
		}

	public:
		auto operator<=>(const AbstractKey &other) const {
			if (type_ == other.type_) {
				return logic_key_ <=> other.logic_key_;
			}
			return type_ <=> other.type_;
		}

		bool operator==(const AbstractKey &other) const {
			return type_ == other.type_ && logic_key_ == other.logic_key_;
		}
	};
}
