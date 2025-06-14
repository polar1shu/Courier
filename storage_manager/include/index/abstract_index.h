/*
 * @author: BL-GS 
 * @date:   2023/1/26
 */

#pragma once

#include <cstdint>
#include <type_traits>
#include <functional>
#include <concepts>

namespace ix {

	template<class Key>
	concept KeyConcept = requires {
		std::is_integral_v<Key>;
	};

	template<class Value>
	concept ValueConcept = requires {
		std::is_copy_assignable_v<Value>;
		std::is_copy_constructible_v<Value>;
		std::is_default_constructible_v<Value>;
	};

	template<class IndexType>
	concept IndexConcept = requires(
			IndexType index,
			IndexType::KeyType key,
			IndexType::ValueType value,
			IndexType::ValueType &&moved_value,
			std::function<void(const typename IndexType::ValueType &)> clear_func) {
		{ index.insert(key, value) } -> std::same_as<bool>;
		{ index.remove(key) } -> std::same_as<bool>;
		{ index.read(key, value) } -> std::same_as<bool>;
		{ index.update(key, value) } -> std::same_as<bool>;
		{ index.contain(key) } -> std::same_as<bool>;
		{ index.clear(clear_func) } -> std::same_as<void>;
		{ index.size() } -> std::convertible_to<uint32_t>;
	};
}
