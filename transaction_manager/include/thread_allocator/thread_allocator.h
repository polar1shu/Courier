/*
 * @author: BL-GS 
 * @date:   2023/1/2
 */

#pragma once

#include <iostream>

#include <thread_allocator/abstract_thread_allocator.h>

#include <thread_allocator/std_thread_allocator/std_thread_allocator.h>

namespace allocator {

	enum class ThreadAllocatorType {
		StdThreadAllocator
	};

	template<ThreadAllocatorType Type>
	struct ThreadAllocatorManager {
		using AllocatorType = void;
	};

	template<>
	struct ThreadAllocatorManager<ThreadAllocatorType::StdThreadAllocator> {
		using AllocatorType = StdThreadAllocator;
	};
}

