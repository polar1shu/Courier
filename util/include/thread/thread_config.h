/*
 * @author: BL-GS 
 * @date:   2023/2/22
 */

#pragma once

#include <arch/arch.h>

namespace thread {

	#ifndef MAX_THREAD_NUM_DEFINED
		static constexpr int MAX_TID = ARCH_CPU_LOGICAL_NUM;
	#else
		static constexpr int MAX_TID = MAX_THREAD_NUM_DEFINED;
	#endif


	/*!
	 * @brief Pause to prevent excess processor bus usage
	 */
	void pause() {
		#if defined( __sparc )
			__asm__ __volatile__ ( "rd %ccr,%g0" );
		#elif defined( __i386 ) || defined( __x86_64 )
			__asm__ __volatile__ ( "pause" : : : );
		#else
			std::this_thread::yield();
		#endif
	}
}
