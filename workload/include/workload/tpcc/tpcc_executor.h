/*
 * @author: BL-GS 
 * @date:   2023/3/1
 */

#pragma once

#include <cstdint>

#include <listener/time_listener.h>

#include <workload/tpcc/tpcc_generator.h>
#include <workload/tpcc/tpcc_engine.h>
#include <workload/tpcc/tpcc_secondary_index.h>

#include <workload/generator/nurand_generator.h>
#include <workload/generator/string_generator.h>
#include <workload/generator/last_name_generator.h>

namespace workload {

	inline namespace tpcc {

		template<class Config>
		class TPCCExecutor {
		public:
			static constexpr int32_t NUM_ITEMS                 = Config::NUM_ITEMS;

			static constexpr int32_t NUM_WAREHOUSES            = Config::NUM_WAREHOUSES;

			static constexpr int32_t DISTRICTS_PER_WAREHOUSE   = Config::DISTRICTS_PER_WAREHOUSE;

			static constexpr int32_t CUSTOMERS_PER_DISTRICT    = Config::CUSTOMERS_PER_DISTRICT;

			static constexpr int32_t REMOTE_PROBABILITY        = Config::REMOTE_PROBABILITY;

			static constexpr int32_t NEW_ORDER_PER_DISTRICT    = Config::NEW_ORDER_PER_DISTRICT;

		public:
			using ConfigManager               = TPCCConfigManager<Config>;

			using KeyType                     = ConfigManager::KeyType;

			using SecondaryIndexType            = TPCCSecondaryIndex<Config>;

			static constexpr bool COPY_STRING = ConfigManager::COPY_STRING;

		private:

			TPCCGenerator<
			        NUM_WAREHOUSES,
					NUM_ITEMS,
					DISTRICTS_PER_WAREHOUSE,
					CUSTOMERS_PER_DISTRICT,
					NEW_ORDER_PER_DISTRICT> bench_generator_;

			SecondaryIndexType secondary_index_;

			char time_string[DATETIME_SIZE];
		public:
			TPCCExecutor():
					bench_generator_(util::listener::TimeListener::print_time_static(DATETIME_SIZE)),
					secondary_index_(NUM_WAREHOUSES, DISTRICTS_PER_WAREHOUSE, CUSTOMERS_PER_DISTRICT) {

				util::listener::TimeListener::print_time(time_string, DATETIME_SIZE);
			}

			static constexpr uint64_t total_initialize_phase_num() {
				return (NUM_WAREHOUSES + 1) * NUM_ITEMS;
			}

		public:
			/*!
			 * @brief Initialize database
			 * @tparam Executor Class of executor from concurrent control
			 * @param executor Executor from concurrent control
			 * @return Always true.
			 * @table Items
			 * @table Stocks
			 * @table District
			 * @table Customer
			 * @table History
			 * @table Order
			 * @table OrderLine
			 * @table NewOrder
			 */
			template<class Executor>
			bool do_initialize(Executor &executor, uint64_t initialization_phase) {
				// Initialize database execution engine
				TPCCWorkloadEngine<Config, Executor> db_(&executor, &secondary_index_);

				assert(initialization_phase < (NUM_WAREHOUSES + 1) * NUM_ITEMS);
				auto idx = initialization_phase;

				auto w_id = idx / NUM_ITEMS;
				auto i_id = idx % NUM_ITEMS + 1;
				// Initialize Item table
				// After this, there are NUM_ITEMS items in table, whose id vary from 1 to NUM_ITEMS
				if (w_id == 0) {
					bench_generator_.make_items_table(db_, i_id);
				}
				else {
					// Initialize Warehouse table
					// After this, there are NUM_ITEMS stocks with w_id in table, a new warehouse,
					// DISTRICT_PER_WAREHOUSE district with w_id.
					// For each district, there are CUSTOMERS_PER_DISTRICT customers and histories
					// Besides, for each district, there are CUSTOMERS_PER_DISTRICT orders and orderlines,
					// among which there are NEW_ORDERS_PER_DISTRICT new orders.
					bench_generator_.make_warehouse(db_, w_id, i_id);
				}

				if (idx == total_initialize_phase_num() - 1) {
					// Update random engine after initialization.
					bench_generator_.update_rander_constant();
				}
				return true;
			}

			template<class Executor>
			bool do_stock_level(Executor &executor) {
				TPCCWorkloadEngine<Config, Executor> db_(&executor, &secondary_index_);

				int32_t threshold = generate_stock_level_threshold();

				return db_.stock_level(generate_warehouse_id(), generate_district_id(), threshold);
			}

			template<class Executor>
			bool do_order_status(Executor &executor) {
				TPCCWorkloadEngine<Config, Executor> db_(&executor, &secondary_index_);

				OrderStatusOutput output;
				if (util::rander.rand_percentage() <= 60) {
					// 60%: order status by last name
					char c_last[Customer::MAX_LAST + 1];
					if constexpr (COPY_STRING) {
						generate_last_name(c_last, CUSTOMERS_PER_DISTRICT);
					}
					return db_.order_status(generate_warehouse_id(), generate_district_id(), c_last, &output);
				}
				else {
					return db_.order_status(generate_warehouse_id(), generate_district_id(), generate_cid(), &output);
				}
			}

			template<class Executor>
			bool do_delivery(Executor &executor) {
				TPCCWorkloadEngine<Config, Executor> db_(&executor, &secondary_index_);

				int carrier = generate_order_carrier_id();

				std::vector<DeliveryOrderInfo> orders;
				return db_.delivery(generate_warehouse_id(), carrier, time_string, &orders);
			}

			template<class Executor>
			bool do_payment(Executor &executor) {
				TPCCWorkloadEngine<Config, Executor> db_(&executor, &secondary_index_);

				uint32_t x = util::rander.rand_percentage();
				uint32_t y = util::rander.rand_percentage();

				int32_t w_id = generate_warehouse_id();
				int32_t d_id = generate_district_id();

				int32_t c_w_id;
				int32_t c_d_id;
				if (NUM_WAREHOUSES == 1 || x <= 85) {
					// 85%: paying through own warehouse (or there is only 1 warehouse)
					c_w_id = w_id;
					c_d_id = d_id;
				}
				else {
					// 15%: paying through another warehouse:
					// select in range [1, num_warehouses] excluding w_id
					c_w_id = generate_warehouse_id(w_id);
					c_d_id = generate_district_id();
				}
				float h_amount = generate_payment_amount();

				PaymentOutput output;
				if (y <= 60) {
					// 60%: payment by last name
					char c_last[Customer::MAX_LAST + 1];
					if constexpr (COPY_STRING) {
						generate_last_name(c_last, CUSTOMERS_PER_DISTRICT);
					}
					return db_.payment(w_id, d_id, c_w_id, c_d_id, c_last, h_amount, time_string, &output);
				}
				else {
					// 40%: payment by id
					return db_.payment(w_id, d_id, c_w_id, c_d_id, generate_cid(), h_amount, time_string, &output);
				}
			}

			/*!
			 * @brief
			 * @tparam Executor
			 * @param executor
			 * @return
			 * @note Haven't implement rolling back of 1%
			 */
			template<class Executor>
			bool do_new_order(Executor &executor) {
				TPCCWorkloadEngine<Config, Executor> db_(&executor, &secondary_index_);

				int32_t w_id = generate_warehouse_id();
				int32_t d_id = generate_district_id();
				int32_t c_id = generate_cid();

				int ol_cnt = generate_orderline_cnt();

				std::array<NewOrderItem, Order::MAX_OL_CNT> items;

				bool do_remote = false;
				for (int i = 0; i < ol_cnt; ++i) {
					items[i].ol_quantity = generate_orderline_quantity();
					// TPC-C suggests generating a number in range (1, 100) and selecting remote on 1
					// This provides more variation, and lets us tune the fraction of "remote" transactions.
					bool remote = util::rander.rand_percentage() <= REMOTE_PROBABILITY;
					if (NUM_WAREHOUSES > 1 && remote) { // Remote supply
						do_remote = true;
						items[i].ol_supply_w_id = generate_warehouse_id(w_id);
					}
					else { // Local supply
						items[i].ol_supply_w_id = w_id;
					}

					// Select unique item id
					do {
						int id = generate_item_id();
						bool unique = true;
						for (int k = 0; k < i; ++k) {
							if (items[k].i_id == id) [[unlikely]] {
								unique = false;
								break;
							}
						}
						if (unique) [[likely]] {
							items[i].i_id = id;
							break;
						}

					} while (true);

				}

				NewOrderOutput output;
				bool result = db_.new_order(
						w_id, d_id, c_id,
						items.cbegin(), items.cbegin() + ol_cnt, time_string,
						do_remote, &output
				);
				return result;
			}

		private:
			int32_t generate_warehouse_id() {
				return bench_generator_.generate_warehouse_id();
			}

			int32_t generate_warehouse_id(int32_t exclude) {
				return bench_generator_.generate_warehouse_id_exclude(exclude);
			}

			int32_t generate_district_id() {
				return bench_generator_.generate_district_id();
			}

			int32_t generate_cid() {
				return bench_generator_.generate_cid();
			}

			int32_t generate_item_id() {
				return bench_generator_.generate_item_id();
			}

			int32_t generate_orderline_cnt() {
				return bench_generator_.generate_orderline_cnt();
			}

			int32_t generate_order_carrier_id() {
				return bench_generator_.generate_order_carrier_id();
			}

			float generate_payment_amount() {
				return bench_generator_.generate_payment_amount();
			}

			int32_t generate_orderline_quantity() {
				return bench_generator_.generate_orderline_quantity();
			}

			int32_t generate_stock_level_threshold() {
				return bench_generator_.generate_stock_level_threshold();
			}

			void generate_last_name(char *c_last, int max_cid) {
				return bench_generator_.generate_last_name(c_last, max_cid);
			}
		};

	}
}
