/*
 * @author: BL-GS
 * @date:   2022/11/23
 */

#pragma once

#include <chrono>
#include <ctime>
#include <iostream>
#include <utility>
#include <spdlog/spdlog.h>

#include <util/log_table.h>
#include <listener/abstract_listener.h>

namespace util::listener {

	class TimeListener: public AbstractListener {

	private:
		using system_clock = std::chrono::system_clock;
		using milliseconds = std::chrono::milliseconds;
		using time_point   = std::chrono::time_point<system_clock, milliseconds> ;

	private:
		time_point      start_time_;
		time_point      end_time_;

	public:
		TimeListener() = default;

		~TimeListener() override {
			std::string     start_;
			std::string     end_;
			long long       run_duration_;

			{
				time_t tt = std::chrono::system_clock::to_time_t(start_time_);
				start_ = std::ctime(&tt);
			}
			{
				time_t tt = std::chrono::system_clock::to_time_t(end_time_);
				end_ = std::ctime(&tt);
			}

			run_duration_ = std::chrono::duration_cast<milliseconds>(end_time_ - start_time_).count();
			util::print_property("Time Listener",
										std::make_tuple("Start Time", start_, ""),
										std::make_tuple("End Time", end_, ""),
										std::make_tuple("Run duration", run_duration_, "ms"));
		}

		void start_record() override {
			start_time_ = std::chrono::time_point_cast<milliseconds>(std::chrono::system_clock::now());
		}

		void end_record() override {
			end_time_ = std::chrono::time_point_cast<milliseconds>(std::chrono::system_clock::now());
		}

	public:
		static void print_time(char *buffer, uint32_t max_size = 20) {
			std::time_t tt = std::chrono::system_clock::to_time_t(std::chrono::system_clock::now());
			std::tm tm = *std::gmtime(&tt);
			std::strftime(buffer, max_size, "%Y-%m-%d %H:%M:%S", &tm);
		}

		static char *print_time_static(uint32_t max_size = 20) {
			static char buffer[64];
			print_time(buffer, max_size);
			return buffer;
		}
	};
}
