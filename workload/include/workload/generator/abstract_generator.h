/*
 * @author: BL-GS 
 * @date:   2023/1/26
 */

#pragma once

#include <cstdint>
#include <concepts>

namespace workload {

	template<class Generator>
	concept OperationGeneratorConcept = requires(Generator generator) {
		{ generator.get_next() };
	};

	template<class Generator>
	concept NumberGeneratorConcept = requires(Generator generator) {
		{ generator.get_next() } -> std::convertible_to<uint32_t>;
	};

	template<class Generator>
	concept KeyGeneratorConcept = requires(Generator generator) {
		{ generator.get_next(static_cast<uint32_t>(0)) } -> std::convertible_to<uint32_t>;
	};

	template<class Generator>
	concept StepNumberGeneratorConcept = requires(Generator generator) {
		{ generator.get_next() } -> std::convertible_to<uint32_t>;
		{ generator.acknowledge(static_cast<uint32_t>(0)) } -> std::same_as<void>;
		{ generator.get_limit() } -> std::convertible_to<uint32_t>;
	};

	template<class Generator>
	concept StringGeneratorConcept = requires(Generator generator) {
		{ generator.get_next(nullptr, static_cast<uint32_t>(0)) } -> std::convertible_to<void>;

	};
}
