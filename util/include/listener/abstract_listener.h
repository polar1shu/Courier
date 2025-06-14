/*
 * @author: BL-GS 
 * @date:   2023/6/21
 */

#pragma once

namespace util::listener {

	class AbstractListener {
	public:
		virtual ~AbstractListener() = default;

	public:
		/*!
		 * @brief Start recording
		 */
		virtual void start_record() = 0;

		/*!
		 * @brief End recording
		 */
		virtual void end_record() = 0;
	};

}

