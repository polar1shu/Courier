/*
 * @author: BL-GS 
 * @date:   2024/1/19
 */

#pragma once

#include <cstdint>

namespace cc::courier {

	enum class LogLabel {
		Insert,
		Update,
		Delete,
		Commit
	};

	template<LogLabel Label, class AbKey>
	struct LogTuple {
	public:
		/// The type of operation
		LogLabel label_;
		/// The timestamp of transaction
		uint64_t ts_;
		/// The key of target
		AbKey    key_;
		/// The length of content
		uint32_t size_;
		uint32_t offset_;
		/// Extra information attached to log tuple.
		uint8_t extra_info_[];
	};

	template<class AbKey>
	struct LogTuple<LogLabel::Commit, AbKey> {
		/// The type of operation
		LogLabel label_;
		/// The timestamp of transaction
		uint64_t ts_;
	};

	template<class AbKey>
	struct LogTuple<LogLabel::Delete, AbKey> {
		/// The type of operation
		LogLabel label_;
		/// The timestamp of transaction
		uint64_t ts_;
		/// The key of target
		AbKey    key_;
	};

	template<class AbKey>
	struct LogTuple<LogLabel::Insert, AbKey> {
	public:
		/// The type of operation
		LogLabel label_;
		/// The timestamp of transaction
		uint64_t ts_;
		/// The key of target
		AbKey    key_;
		/// The length of content
		uint32_t size_;
		/// Extra information attached to log tuple.
		uint8_t extra_info_[];
	};

}