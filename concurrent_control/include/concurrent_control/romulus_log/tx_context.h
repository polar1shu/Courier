/*
 * @author: BL-GS 
 * @date:   2023/1/2
 */

#pragma once

#include <cstdint>
#include <mutex>
#include <vector>
#include <utility>

#include <concurrent_control/config.h>

namespace cc::romulus {

	enum class TxType {
		Read,
		Write,
		Insert,
		Delete
	};

	struct alignas(64) TxContext {
		ConcurrentControlPortableMessage message_;

		enum class Status {
			NeedWrite,
			Pass
		};
		Status status_;

		TxContext() = default;
	};
}
