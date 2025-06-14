/*
 * @author: BL-GS 
 * @date:   2023/2/18
 */

#pragma once

#include <cstdint>

namespace cc::romulus {

	// Each log entry is two words (8+8 = 16 bytes)
	struct LogEntry {
		void *tuple_ptr;

		LogEntry(): tuple_ptr(nullptr) {}
	};

	struct LogChunk {
		static constexpr uint32_t CHUNK_SIZE = 1024;

	public:
		LogEntry  entries[CHUNK_SIZE];
		uint64_t  num_entries;
		LogChunk* next;

	public:
		LogChunk(): num_entries(0), next(nullptr) {}
	};
}
