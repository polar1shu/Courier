/*
 * @author: BL-GS 
 * @date:   2023/3/10
 */

#pragma once

#include <memory/flush.h>

inline namespace util_mem {

	template<Flush FlushType = PWB_ENUM>
	struct NVMConfig {

		static inline void pwb(void *target) {
			clwb(target);
		}

		static inline void pwb_range(void *start_ptr, uint32_t size) {
			clwb_range(start_ptr, size);
		}

		static inline void fence() {
			std::atomic_thread_fence(std::memory_order::acq_rel);
		}
	};

	template<>
	struct NVMConfig<Flush::CLWB> {

		static inline void pwb(void *target) {
			clwb(target);
		}

		static inline void pwb_range(void *start_ptr, uint32_t size) {
			clwb_range(start_ptr, size);
		}

		static inline void fence() {
			asm volatile("sfence\n" : :);
		}
	};

	template<>
	struct NVMConfig<Flush::CLFLUSH> {

		static inline void pwb(void *target) {
			clflush(target);
		}

		static inline void pwb_range(void *start_ptr, uint32_t size) {
			clflush_range(start_ptr, size);
		}

		static inline void fence() {
		}
	};

	template<>
	struct NVMConfig<Flush::CLFLUSHOPT> {

		static inline void pwb(void *target) {
			clflushopt(target);
		}

		static inline void pwb_range(void *start_ptr, uint32_t size) {
			clflushopt_range(start_ptr, size);
		}

		static inline void fence() {
			asm volatile("sfence\n" : :);
		}
	};

	using NVM = NVMConfig<PWB_ENUM>;
}
