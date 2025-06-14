/*
 * @author: BL-GS 
 * @date:   2023/3/11
 */

#pragma once

#include <chrono>
#include <concepts>
#include <coroutine>
#include <functional>

namespace allocator {

	enum class ThreadBindStrategy {
		None,
		NUMABind,
		RemoteNUMABind,
		CPUBind,
		HybridBind
	};

	template<class Allocator>
	concept ThreadAllocatorConcept = requires(Allocator allocator, int thread_num) {

		{ allocator.reserve(thread_num) };

		{ allocator.wait_all_tasks() };

		{ allocator.clear_all_tasks() };

		/// Task execution with self-control mechanism required
		{ allocator.run_tasks(static_cast<int>(0), [](){}) } -> std::convertible_to<uint64_t>;

	};

}
