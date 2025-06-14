/*
 * @author: BL-GS 
 * @date:   2023/6/23
 */

#pragma once

#include <coroutine>

namespace transaction {

	//! @brief Error or calling for main thread
	enum class TaskError {
		/// Everything normal
		None,
		/// Transaction abort and retry
		Retry,
		/// Assert false
		AssertFault,
		/// Acquire stopping earlier
		PreStop,
		/// Join a barrier with other worker threads(exclude main thread)
		Barrier,
		/// Join a barrier with other threads(include main thread who take charge of timing)
		TimeBarrier,
		/// Join a barrier with other threads(include main thread who take charge of ending timing)
		EndTimeBarrier,
		/// Join a barrier with other threads(include main thread who take charge of timing and interrupting)
		ClockBarrier
	};

	/*!
	 * @brief Return object of thread tasks to offer control information
	 */
	struct ThreadTaskReturnObject {
	public:
		// Although this is not the rule of me to name a structure.
		struct promise_type {
		public:
			TaskError task_error = TaskError::None;

		public:
			/// Interface for getting return object
			ThreadTaskReturnObject get_return_object() {
				return { .handle = std::coroutine_handle<promise_type>::from_promise(*this) };
			}

			/// Suspend before execution
			std::suspend_always	initial_suspend() { return {}; }

			/// Suspend after ending
			std::suspend_always final_suspend() noexcept { return {}; }

			/// Yield for error setting
			std::suspend_always yield_value(const TaskError &error) {
				task_error = error;
				return {};
			}

			void unhandled_exception() {}

			/// Return and set error
			void return_value(const TaskError &error) { task_error = error; }
		};

	public:
		std::coroutine_handle<promise_type> handle;
	};

}
