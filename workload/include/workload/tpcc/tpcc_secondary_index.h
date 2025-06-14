/*
 * @author: BL-GS 
 * @date:   2023/3/11
 */

/*!
 * @brief Compromise to index searching which haven't been accomplished.
 */

#pragma once

#include <set>
#include <map>

#include <tbb/concurrent_queue.h>

#include <util/utility_macro.h>
#include <thread/thread.h>

#include <workload/tpcc/tpcc_table.h>

namespace workload {

	inline namespace tpcc {

		template<class Config>
		class TPCCSecondaryIndex {
		public:
			using ConfigManager               = TPCCConfigManager<Config>;

			using KeyType                     = ConfigManager::KeyType;

			static constexpr bool COPY_STRING = ConfigManager::COPY_STRING;

			class CustomerByNameOrdering {
			public:
				bool operator()(const CustomerNameIdentify& a, const CustomerNameIdentify& b) const {

					if (a.c_w_id < b.c_w_id) return true;
					if (a.c_w_id > b.c_w_id) return false;

					if (a.c_d_id < b.c_d_id) return true;
					if (a.c_d_id > b.c_d_id) return false;

					if constexpr (COPY_STRING) {
						int diff = strcmp(a.c_last, b.c_last);
						if (diff < 0) return true;
						if (diff > 0) return false;

						// Finally delegate to c_first
						return strcmp(a.c_first, b.c_first) < 0;
					}
					return false;
				}
			};

			using CustomerByNameSet  = std::set<CustomerNameIdentify, CustomerByNameOrdering>;

			using OrderByCustomerEle = std::atomic<int64_t>;

			using NewOrderCounter    = tbb::concurrent_queue<int64_t>; // Note that we should keep it insert by order

		private:

			uint32_t num_warehouse_;

			uint32_t num_district_per_warehouse_;

			uint32_t num_counter_per_district_;

			CustomerByNameSet *customers_by_name_ptr_;

			OrderByCustomerEle *orders_by_customer_array_;

		public:

			TPCCSecondaryIndex(uint32_t num_warehouse, uint32_t num_district_per_warehouse, uint32_t num_counter_per_district):
									num_warehouse_(num_warehouse),
									num_district_per_warehouse_(num_district_per_warehouse),
									customers_by_name_ptr_(new CustomerByNameSet{}),
									orders_by_customer_array_(new OrderByCustomerEle[num_warehouse * num_district_per_warehouse * num_counter_per_district]{0}) {
			}

			~TPCCSecondaryIndex() {
				delete customers_by_name_ptr_;
				delete[] orders_by_customer_array_;
			}

		public: // Customer by name

			/// There might be multi threads get in
			void insert_customer_by_name(const Customer &c) {
				static std::mutex lock;
				lock.lock();
				customers_by_name_ptr_->emplace(c);
				lock.unlock();
			}

			// There won't be any insert during running time, no need to make security.
			auto get_customer_indentify_by_name(const Customer &c) {
				CustomerNameIdentify c_iden(c);

				auto start_it = customers_by_name_ptr_->lower_bound(c_iden);

				if (start_it == customers_by_name_ptr_->end()) {
					return std::make_pair(false, start_it);
				}

				if constexpr (COPY_STRING) {
					auto stop_it = customers_by_name_ptr_->upper_bound(c_iden);

					auto middle_it = start_it;

					// Choose position n/2 rounded up (1 based addressing) = floor((n-1)/2)
					{
						int i = 0;
						while (start_it != stop_it) {
							++start_it;
							if (i % 2 == 1) { ++middle_it; }
							++i;
						}
					}
					DEBUG_ASSERT(strcmp(middle_it->c_last, c_iden.c_last) == 0);

					return std::make_pair(true, middle_it);
				}
				return std::make_pair(true, start_it);
			}

		public: // Order by customer

			auto get_order_by_customer(int32_t w_id, int32_t d_id, int32_t c_id) {
				auto &order_counter = orders_by_customer_array_[w_id * d_id * c_id - 1];
				return order_counter.load(std::memory_order::acquire);
			}

			void insert_order_by_customer(int32_t w_id, int32_t d_id, int32_t c_id, int64_t o_id) {
				auto &order_counter = orders_by_customer_array_[w_id * d_id * c_id - 1];
				while (true) {
					int64_t old_val = order_counter.load(std::memory_order::relaxed);
					if (old_val < o_id) {
						if (!order_counter.compare_exchange_strong(old_val, o_id)) { continue; }
					}
					break;
				}
			}

		};

	}
}
