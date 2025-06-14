/*
 * @author: BL-GS 
 * @date:   2022/12/31
 */

#pragma once

#include <atomic>

#include <util/latency_counter.h>

namespace cc {

	constexpr uint32_t RECORD_GRANULARITY = 8;

	enum class RecordEvent : uint32_t {
		running      = 0,
		commit       = 1,
		index        = 2,
		validate     = 3,
		persist_log  = 4,
		persist_data = 5,
		total        = 6
	};
	constexpr bool GlobalRecordSwitch[] = {
			[(uint32_t)RecordEvent::running]      = true,
			[(uint32_t)RecordEvent::commit]       = true,
			[(uint32_t)RecordEvent::index]        = true,
			[(uint32_t)RecordEvent::validate]     = true,
			[(uint32_t)RecordEvent::persist_log]  = true,
			[(uint32_t)RecordEvent::persist_data] = true,
			[(uint32_t)RecordEvent::total]        = true
	};


	//! @brief Message recorder for every transaction
	//! Once a transaction ends, this struct need to be combined with thread-local
	//! message recorder (ConcurrentControlMessage).
	struct ConcurrentControlPortableMessage {

		#define MESSAGE_RECORDER(message_type) \
		private: \
			std::chrono::time_point<std::chrono::steady_clock> message_type##_time; \
            std::chrono::nanoseconds message_type##_duration{0}; \
		public: \
			void start_##message_type() { \
                if constexpr(GlobalRecordSwitch[static_cast<uint32_t>(RecordEvent::message_type)]) { \
					message_type##_time = std::chrono::steady_clock::now(); \
				} \
            } \
			void end_##message_type() { \
				if constexpr(GlobalRecordSwitch[static_cast<uint32_t>(RecordEvent::message_type)]) { \
					message_type##_duration += std::chrono::steady_clock::now() - message_type##_time; \
                } \
			} \
			auto get_##message_type##_duration() const { \
				return message_type##_duration;\
			}

		MESSAGE_RECORDER(running)

		MESSAGE_RECORDER(commit)

		MESSAGE_RECORDER(index)

		MESSAGE_RECORDER(validate)

		MESSAGE_RECORDER(persist_log)

		MESSAGE_RECORDER(persist_data)

		MESSAGE_RECORDER(total)

		#undef MESSAGE_RECORDER
	};

	//! @brief Global message recorder.
	//! Each thread keep a recorder. All message about transaction need to be combined once it ends,
	//! which is optimised as thread local to accelerate procession.
	//! Once the thread ends, this struct will be released,
	//! and relative information will be combined with global message recorder, which
	//! need locks to make sure consistence.
	struct ConcurrentControlMessage {
		#define LATENCY_RECORDER(message_type) \
			private:                              \
				util::LatencyCounter<RECORD_GRANULARITY> message_type##_latency; \
			public: \
				uint64_t get_total_##message_type##_latency(uint32_t alpha) {    \
					if constexpr(GlobalRecordSwitch[static_cast<uint32_t>(RecordEvent::message_type)]) { \
                        return message_type##_latency.get_latency_summary(alpha);         \
                    }                    \
                    return 0;            \
				}                        \
				uint64_t get_total_##message_type##_time() {                     \
                    if constexpr(GlobalRecordSwitch[static_cast<uint32_t>(RecordEvent::message_type)]) { \
						return message_type##_latency.get_time_summary();              \
                    }                    \
					return 0; \
				}

		LATENCY_RECORDER(running)

		LATENCY_RECORDER(commit)

		LATENCY_RECORDER(index)

		LATENCY_RECORDER(validate)

		LATENCY_RECORDER(persist_log)

		LATENCY_RECORDER(persist_data)

		LATENCY_RECORDER(total)

		#undef LATENCY_RECORDER

	public:
		/// Switch about whether records.
		inline static bool record{false};

		/// The number of transaction aborted.
		uint64_t abort_tx;
		/// The number of transaction in total.
		uint64_t total_tx;

	public:
		ConcurrentControlMessage(): abort_tx{0}, total_tx{0} { }

		//! @brief Once be released, accumulate data and combine it with global message recorder.
		~ConcurrentControlMessage() {
			if (record) {
				ConcurrentControlMessage &summary = get_summary_message();
				summary.combine(*this);
			}
		}

	public:
		static ConcurrentControlMessage &get_summary_message() {
			static ConcurrentControlMessage summary_message;
			return summary_message;
		}

		static ConcurrentControlMessage &get_thread_message() {
			static thread_local ConcurrentControlMessage thread_message;
			return thread_message;
		}

		//! @brief Turn on recording.
		static void enable_record() {
			std::atomic_ref{record}.store(true);
		}

		//! @brief Turn off recording.
		static void disable_record() {
			std::atomic_ref{record}.store(false);
		}


	public:
		void start_transaction() {
			if (record) {
				++total_tx;
			}
		}

		void abort_transaction() {
			if (record) {
				++abort_tx;
			}
		}


		//! @brief Submit message to global recorder
		//! @param message
		void submit_time(const ConcurrentControlPortableMessage &message) {
			#define SUBMIT_RECORD(message_type) \
                if constexpr(GlobalRecordSwitch[static_cast<uint32_t>(RecordEvent::message_type)]) { \
                    message_type##_latency.add_latency(message.get_##message_type##_duration().count());            \
                }

			if (record) {
				SUBMIT_RECORD(running)
				SUBMIT_RECORD(commit)
				SUBMIT_RECORD(index)
				SUBMIT_RECORD(validate)
				SUBMIT_RECORD(persist_log)
				SUBMIT_RECORD(persist_data)
				SUBMIT_RECORD(total)
			}

			#undef SUBMIT_RECORD
		}

		//! @brief
		//! @param other
		void combine(const ConcurrentControlMessage &other) {
			#define COMBINE_RECORD(message_type) \
                if constexpr(GlobalRecordSwitch[static_cast<uint32_t>(RecordEvent::message_type)]) { \
                    message_type##_latency.combine(other.message_type##_latency); \
				}

			static std::mutex lock_;
			lock_.lock();

//			uint32_t tid = thread::get_tid();
//			util::print_property("Thread Latency",
//			                                         std::make_tuple("tid", tid, ""),
//													 std::make_tuple("NUMA", thread::get_cpu_numa_id(), ""),
//													 std::make_tuple("Total Tx", other.total_tx, ""),
//													 std::make_tuple("Abort Tx", other.abort_tx, ""),
//													 std::make_tuple("Abort rate", static_cast<double>(other.abort_tx) /
//															 static_cast<double>(other.total_tx), ""),
//			                                         std::make_tuple("running", other.running_latency.get_latency_summary(99), ""),
//			                                         std::make_tuple("commit", other.commit_latency.get_latency_summary(99), ""),
//			                                         std::make_tuple("index", other.index_latency.get_latency_summary(99), ""),
//			                                         std::make_tuple("validate", other.validate_latency.get_latency_summary(99), ""),
//			                                         std::make_tuple("persist_log", other.persist_log_latency.get_latency_summary(99), ""),
//			                                         std::make_tuple("persist_data", other.persist_data_latency.get_latency_summary(99), "")
//													 );
//
			if (record) {
				COMBINE_RECORD(running)
				COMBINE_RECORD(commit)
				COMBINE_RECORD(index)
				COMBINE_RECORD(validate)
				COMBINE_RECORD(persist_log)
				COMBINE_RECORD(persist_data)
				COMBINE_RECORD(total)
			}

			total_tx += other.total_tx;
			abort_tx += other.abort_tx;
			lock_.unlock();

			#undef COMBINE_RECORD
		}

		/*!
		 * @brief Clear all records.
		 * It is ought to be called when concurrent control release.
		 */
		void clear_up() {
			#define CLEAR_RECORD(message_type) \
                if constexpr(GlobalRecordSwitch[static_cast<uint32_t>(RecordEvent::message_type)]) { \
                    message_type##_latency.clear();  \
				}

			total_tx = abort_tx = 0;
			CLEAR_RECORD(running)
			CLEAR_RECORD(commit)
			CLEAR_RECORD(index)
			CLEAR_RECORD(validate)
			CLEAR_RECORD(persist_log)
			CLEAR_RECORD(persist_data)
			CLEAR_RECORD(total)

			#undef CLEAR_RECORD
		}
	};

}
