/*
 * @author: BL-GS 
 * @date:   2023/3/22
 */

#pragma once

#include <cstdint>
#include <cstddef>
#include <concepts>
#include <tuple>
#include <functional>

namespace recovery {

	//! @brief Concept about basic storage manager
	//! @tparam ManagerClass
	template<class ManagerClass>
	concept StorageManagerConcept = requires(ManagerClass manager,
			void *data_ptr,
			void *src_ptr,
			size_t size
		) {

		/*
		 * Log interface
		 */

		{ manager.allocate_log(size) } -> std::convertible_to<void *>;

		{ manager.deallocate_log(src_ptr, size) } -> std::same_as<void>;

		{ manager.pwb_range(data_ptr, size) } -> std::same_as<void>;

		{ manager.fence() } -> std::same_as<void>;
	};


	struct LogSpace {
		uint8_t *start_ptr;
		uint8_t *cur_ptr;
		uint8_t *end_ptr;
	};
}
