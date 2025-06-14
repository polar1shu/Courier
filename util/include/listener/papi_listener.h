/*
 * @author: BL-GS 
 * @date:   2023/6/21
 */

#pragma once

#include <cstdint>
#include <cassert>
#include <papi.h>

#include <util/log_table.h>
#include <listener/abstract_listener.h>

namespace util::listener {

	class PAPIListener : public AbstractListener {
	private:
		std::vector<long long> start_value_;
		std::vector<long long> end_value_;

		int event_set;

	public:
		PAPIListener(): event_set(PAPI_NULL) {
			PAPI_library_init(PAPI_VER_CURRENT);
			PAPI_create_eventset(&event_set);

			PAPI_add_event(event_set, PAPI_TOT_INS);
			PAPI_add_event(event_set, PAPI_L1_DCM);
			PAPI_add_event(event_set, PAPI_L2_DCM);
			PAPI_add_event(event_set, PAPI_LD_INS);
			PAPI_add_event(event_set, PAPI_SR_INS);

			start_value_.resize(5, 0);
			end_value_.resize(5, 0);
		}

		~PAPIListener() override {
			PAPI_cleanup_eventset(event_set);
			PAPI_destroy_eventset(&event_set);
			PAPI_shutdown();

			util::print_property("PAPI Listener",
										std::make_tuple("PAPI_TOT_INS", end_value_[0] - start_value_[0], ""),
										std::make_tuple("PAPI_L1_DCM", end_value_[1] - start_value_[1], ""),
										std::make_tuple("PAPI_L2_DCM", end_value_[2] - start_value_[2], ""),
										std::make_tuple("PAPI_LD_INS", end_value_[3] - start_value_[3], ""),
										std::make_tuple("PAPI_SR_INS", end_value_[4] - start_value_[4], "")
			);
	    }

	public:
		void start_record() override {
			PAPI_start(event_set);
			PAPI_read(event_set, start_value_.data());
		}

		void end_record() override {
			PAPI_stop(event_set, end_value_.data());
		}
	};

}
