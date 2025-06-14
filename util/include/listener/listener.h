/*
 * @author: BL-GS 
 * @date:   2023/6/21
 */

#pragma once

#include <vector>

#include <listener/abstract_listener.h>
#include <listener/pmem_listener.h>
#include <listener/numa_listener.h>
#include <listener/time_listener.h>
#include <listener/papi_listener.h>

namespace util::listener {

	class ListenerArray {
	private:
		std::vector<std::unique_ptr<AbstractListener>> listener_array_;

	public:
		ListenerArray() = default;

		ListenerArray(const ListenerArray &other) = delete;

		ListenerArray(ListenerArray &&other) = delete;

		~ListenerArray() = default;

	public:
		void add_listener(std::unique_ptr<AbstractListener> &&listener_ptr) {
			listener_array_.emplace_back(std::move(listener_ptr));
		}

		void add_listener(AbstractListener *listener_ptr) {
			listener_array_.emplace_back(listener_ptr);
		}

		void clear_listener() {
			listener_array_.clear();
		}

	public:
		void start_record() {
			for (auto &listener_ptr: listener_array_) {
				listener_ptr->start_record();
			}
		}

		void end_record() {
			for (auto &listener_ptr: listener_array_) {
				listener_ptr->end_record();
			}
		}
	};

}
