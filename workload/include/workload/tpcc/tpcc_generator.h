/*
 * @author: BL-GS 
 * @date:   2023/2/28
 */

#pragma once

#include <cstdint>
#include <cassert>
#include <set>

#include <util/utility_macro.h>

#include <workload/tpcc/tpcc_table.h>
#include <workload/tpcc/tpcc_engine.h>

#include <workload/generator/string_generator.h>
#include <workload/generator/nurand_generator.h>
#include <workload/generator/last_name_generator.h>

namespace workload {

	template<
	        int NUM_WAREHOUSES,
	        int NUM_ITEMS,
	        int DISTRICTS_PER_WAREHOUSE,
			int CUSTOMERS_PER_DISTRICT,
			int NEW_ORDERS_PER_DISTRICTS>
	class TPCCGenerator {

		static_assert(1 <= NUM_ITEMS && NUM_ITEMS <= Item::NUM_ITEMS);

		static_assert(1 <= DISTRICTS_PER_WAREHOUSE && DISTRICTS_PER_WAREHOUSE <= District::NUM_PER_WAREHOUSE);

		static_assert(1 <= CUSTOMERS_PER_DISTRICT && CUSTOMERS_PER_DISTRICT <= Customer::NUM_PER_DISTRICT);

		static_assert(1 <= NEW_ORDERS_PER_DISTRICTS && NEW_ORDERS_PER_DISTRICTS <= NewOrder::INITIAL_NUM_PER_DISTRICT);

		static_assert(NEW_ORDERS_PER_DISTRICTS <= CUSTOMERS_PER_DISTRICT);

		static constexpr int32_t MIN_STOCK_LEVEL_THRESHOLD = 10;

		static constexpr int32_t MAX_STOCK_LEVEL_THRESHOLD = 20;

		static constexpr float MIN_PAYMENT_AMOUNT          = 1.00;

		static constexpr float MAX_PAYMENT_AMOUNT          = 5000.00;

		static constexpr int32_t MAX_OL_QUANTITY           = 10;

	private:
		char now_[DATETIME_SIZE];

		NURandGenerator nurand_generator_;

		RandomLastNameGenerator last_name_generator_;

		RandomStringGenerator<char, 'a', 26> string_generator_;

		RandomStringGenerator<char, '0', 10> num_string_generator_;

		UniformGenerator<int, Item::MAX_IM, Item::MIN_IM> item_im_id_generator_;

		UniformFloatGenerator<float> item_price_generator_{Item::MAX_PRICE, Item::MIN_PRICE};

		UniformGenerator<int, Stock::MAX_QUANTITY, Stock::MIN_QUANTITY> stock_quantity_generator_;

		UniformFloatGenerator<float> customer_discount_generator_{Customer::MAX_DISCOUNT, Customer::MIN_DISCOUNT};

		UniformGenerator<int, Order::MAX_OL_CNT, Order::MIN_OL_CNT> order_ol_cnt_generator_;

		UniformGenerator<int, Order::MAX_CARRIER_ID, Order::MIN_CARRIER_ID> order_carrier_id_generator_;

		UniformGenerator<int, OrderLine::MAX_I_ID, OrderLine::MIN_I_ID> orderline_i_id_generator_;

		UniformFloatGenerator<float> orderline_amount_generator_{OrderLine::MAX_AMOUNT, OrderLine::MIN_AMOUNT };

		UniformFloatGenerator<float> warehouse_tax_generator_{Warehouse::MAX_TAX, Warehouse::MIN_TAX};

		UniformGenerator<int, NUM_WAREHOUSES, 1> warehouse_id_generator_;

		UniformGenerator<int, DISTRICTS_PER_WAREHOUSE, 1> district_id_generator_;

		UniformGenerator<int, MAX_STOCK_LEVEL_THRESHOLD, MIN_STOCK_LEVEL_THRESHOLD> stock_level_threshold_generator_;

		UniformFloatGenerator<float> payment_amount_generator_{MAX_PAYMENT_AMOUNT, MIN_PAYMENT_AMOUNT};

		UniformGenerator<int, MAX_OL_QUANTITY, 1> orderline_quantity_generator_;

		/*
		 * TODO: TPC-C 4.3.3.1. says that this should be a permutation of [1, 3000]. But since it is
		 * for a c_id field, it seems to make sense to have it be a permutation of the customers.
		 * For the "real" thing this will be equivalent
		 */
		std::vector<std::vector<int>> permutation;

	public:
		// Owns generator
		TPCCGenerator(const char* now): permutation(NUM_WAREHOUSES, std::vector<int>{}) {
			memcpy(now_, now, DATETIME_SIZE);

			for (int i = 0; i < NUM_WAREHOUSES; ++i) {
				permutation[i].resize(CUSTOMERS_PER_DISTRICT);
				make_permutation(permutation[i].data(), 1, CUSTOMERS_PER_DISTRICT);
			}
		}

		~TPCCGenerator() = default;

	public:

		/*!
		 * @brief Generates num_items items and inserts them into tables.
		 * @param db Execution engine of workload.
		 */
		template<class Config, class Executor>
		void make_items_table(TPCCWorkloadEngine<Config, Executor> &engine, int i_id) {
			// Select 10% of the rows to be marked "original"
			bool is_original = util::rander.rand_percentage() <= 10;
			// Generate item and insert into table
			Item item = generate_item(i_id, is_original);
			engine.insert_item(item);
		}

		/*!
		 * @brief Generates stock rows for w_id.
		 * @param engine Execution engine of workload
		 * @param w_id ID of warehouse
		 */
		template<class Config, class Executor>
		void make_stock(TPCCWorkloadEngine<Config, Executor> &engine, int32_t w_id, int32_t i_id) {
			// Select 10% of the stock to be marked "original"
			bool is_original = util::rander.rand_percentage() <= 10;
			// Generate stock and insert it into table.
			Stock s = generate_stock(i_id, w_id, is_original);
			engine.insert_stock(s);
		}

		/*!
		 * @brief Generates one warehouse and all related rows.
		 * @param engine Execution engine of workload
		 * @param w_id ID of warehouse
		 */
		template<class Config, class Executor>
		void make_warehouse(TPCCWorkloadEngine<Config, Executor> &engine, int32_t w_id, int32_t i_id) {
			// After this, there are NUM_ITEMS stocks with w_id in table
			make_stock(engine, w_id, i_id);
			// After this, there are a new warehouse, DISTRICT_PER_WAREHOUSE district with w_id.
			// For each district, there are CUSTOMERS_PER_DISTRICT customers and histories
			// Besides, for each district, there are CUSTOMERS_PER_DISTRICT orders and orderlines,
			// among which there are NEW_ORDERS_PER_DISTRICT new orders.
			make_warehouse_without_stock(engine, w_id, i_id);
		}

		/*!
		 * @brief Generates one warehouse and related rows, except stock.
		 * @param engine Execution engine of workload
		 * @param w_id ID of warehouse
		 */
		template<class Config, class Executor>
		void make_warehouse_without_stock(TPCCWorkloadEngine<Config, Executor> &engine, int32_t w_id, int32_t i_id) {
			if (i_id == 1) { // Only insert warehouse once
				// Generates a warehouse with w_id and inserts it into table.
				Warehouse w = generate_warehouse(w_id);
				engine.insert_warehouse(w);
			}

			static_assert(CUSTOMERS_PER_DISTRICT <= NUM_ITEMS);
			static_assert(CUSTOMERS_PER_DISTRICT > 0);
			int c_id = i_id;
			int o_id = i_id;
			if (c_id > CUSTOMERS_PER_DISTRICT) { return; }

			// Generate corresponding districts and customers
			for (int d_id = 1; d_id <= DISTRICTS_PER_WAREHOUSE; ++d_id) {
				// Generate district and insert it.
				District d = generate_district(d_id, w_id);
				engine.insert_district(d);

				// Generate customer and history.
				// Select 10% of the customers to have bad credit
				bool bad_credit = util::rander.rand_percentage() <= 10;
				Customer c = generate_customer(c_id, d_id, w_id, bad_credit);
				engine.insert_customer(c);
				// Generate history and insert.
				History h = generate_history(c_id, d_id, w_id);
				engine.insert_history(h);

				constexpr int new_orders_start_o_id = CUSTOMERS_PER_DISTRICT - NEW_ORDERS_PER_DISTRICTS + 1;

				if (o_id < new_orders_start_o_id) {
					Order o = generate_order(o_id, permutation[w_id - 1][o_id - 1], d_id, w_id, false);
					engine.insert_order(o);
					// Generate each OrderLine for the order
					for (int ol_number = 1; ol_number <= o.o_ol_cnt; ++ol_number) {
						OrderLine line = generate_order_line(ol_number, o_id, d_id, w_id, false);
						engine.insert_order_line(line);
					}
				}
				else if (o_id <= CUSTOMERS_PER_DISTRICT) { // The last NEW_ORDERS_PER_DISTRICTS orders are new
					Order o = generate_order(o_id, permutation[w_id - 1][o_id - 1], d_id, w_id, true);
					engine.insert_order(o);
					// Generate each OrderLine for the order
					for (int ol_number = 1; ol_number <= o.o_ol_cnt; ++ol_number) {
						OrderLine line = generate_order_line(ol_number, o_id, d_id, w_id, true);
						engine.insert_order_line(line);
					}
					NewOrder new_order = {
							.no_w_id = w_id,
							.no_d_id = d_id,
							.no_o_id = o_id
					};
					engine.insert_new_order(new_order);
				}

				if (i_id == 1) {
					NewOrderID new_order_id {
							.w_id = w_id,
							.d_id = d_id,
							.next_no_o_id = new_orders_start_o_id
					};
					engine.insert_new_order_id(new_order_id);
				}
			}
		}

	public:

		 /*!
		  * @brief Fills item with random data according to the TPC-C specification 4.3.3.1.
		  * @param id ID of item(i_id)
		  * @param original Whether the item is original.
		  * @return A tuple of item
		  */
		Item generate_item(int32_t id, bool original) {
			 DEBUG_ASSERT(1 <= id && id <= NUM_ITEMS);
			Item item {
					.i_id = id,
					.i_im_id = item_im_id_generator_.get_next(),
					.i_price = item_price_generator_.get_next()
			};

			string_generator_.get_next(item.i_name, Item::MIN_NAME, Item::MAX_NAME);
			string_generator_.get_next(item.i_data, Item::MIN_DATA, Item::MAX_DATA);

			if (original) { set_original(item.i_data); }
			return item;
		}

		/*!
		 * @brief Fills warehouse with random data according to the TPC-C specification 4.3.3.1.
		 * @param id ID of warehouse(w_id)
		 * @return A tuple of warehouse
		 */
		Warehouse generate_warehouse(int32_t id) {
			DEBUG_ASSERT(1 <= id && id <= Warehouse::MAX_WAREHOUSE_ID);
			Warehouse warehouse {
				.w_id = id,
				.w_tax = make_tax(),
				.w_ytd = Warehouse::INITIAL_YTD
			};
			string_generator_.get_next(warehouse.w_name, Warehouse::MIN_NAME, Warehouse::MAX_NAME);
			string_generator_.get_next(warehouse.w_street_1, Address::MIN_STREET, Address::MAX_STREET);
			string_generator_.get_next(warehouse.w_street_2, Address::MIN_STREET, Address::MAX_STREET);
			string_generator_.get_next(warehouse.w_city, Address::MIN_CITY, Address::MAX_CITY);
			string_generator_.get_next(warehouse.w_state, Address::STATE, Address::STATE);
			make_zip(warehouse.w_zip);
			return warehouse;
		}

		/*!
		 * @brief Fills stock with random data according to the TPC-C specification 4.3.3.1.
		 * @param id ID of stock(s_i_id)
		 * @param w_id ID of warehouse
		 * @param original Whether the stock is original.
		 * @return A tuple of stock
		 */
		Stock generate_stock(int32_t id, int32_t w_id, bool original) {
			DEBUG_ASSERT(1 <= id && id <= NUM_ITEMS);
			Stock stock{
					.s_i_id       = id,
					.s_w_id       = w_id,
					.s_quantity   = stock_quantity_generator_.get_next(),
					.s_ytd        = 0,
					.s_order_cnt  = 0,
					.s_remote_cnt = 0
			};

			for (int i = 0; i < DISTRICTS_PER_WAREHOUSE; ++i) {
				string_generator_.get_next(stock.s_dist[i], Stock::DIST, Stock::DIST);
			}
			string_generator_.get_next(stock.s_data, Stock::MIN_DATA, Stock::MAX_DATA);

			if (original) { set_original(stock.s_data); }
			return stock;
		}

		/*!
		 * @brief Fills district with random data according to the TPC-C specification 4.3.3.1.
		 * @param id ID of district(d_id)
		 * @param w_id ID of warehouse
		 * @return A tuple of district
		 */
		District generate_district(int32_t id, int32_t w_id) {
			DEBUG_ASSERT(1 <= id && id <= DISTRICTS_PER_WAREHOUSE);
			District district {
					.d_id        = id,
					.d_w_id      = w_id,
					.d_tax       = make_tax(),
					.d_ytd       = District::INITIAL_YTD,
					.d_next_o_id = CUSTOMERS_PER_DISTRICT + 1
			};

			string_generator_.get_next(district.d_name, District::MIN_NAME, District::MAX_NAME);
			string_generator_.get_next(district.d_street_1, Address::MIN_STREET, Address::MAX_STREET);
			string_generator_.get_next(district.d_street_2, Address::MIN_STREET, Address::MAX_STREET);
			string_generator_.get_next(district.d_city, Address::MIN_CITY, Address::MAX_CITY);
			string_generator_.get_next(district.d_state, Address::STATE, Address::STATE);
			make_zip(district.d_zip);
			return district;
		}

		/*!
		 * @brief Fill customer with random data
		 * @param id ID of customer
		 * @param d_id ID of district
		 * @param w_id ID of warehouse
		 * @param bad_credit Whether the customer has bad credit
		 * @return A tuple of customer
		 */
		Customer generate_customer(int32_t id, int32_t d_id, int32_t w_id, bool bad_credit) {
			DEBUG_ASSERT(1 <= id && id <= CUSTOMERS_PER_DISTRICT);
			Customer customer {
					.c_id           = id,
					.c_d_id         = d_id,
					.c_w_id         = w_id,
					.c_credit_lim   = Customer::INITIAL_CREDIT_LIM,
					.c_discount     = customer_discount_generator_.get_next(),
					.c_balance      = Customer::INITIAL_BALANCE,
					.c_ytd_payment  = Customer::INITIAL_YTD_PAYMENT,
					.c_payment_cnt  = Customer::INITIAL_PAYMENT_CNT,
					.c_delivery_cnt = Customer::INITIAL_DELIVERY_CNT
			};

			string_generator_.get_next(customer.c_first, Customer::MIN_FIRST, Customer::MAX_FIRST);
			strcpy(customer.c_middle, "OE");

			if (id <= 1000) {
				last_name_generator_.get_last_name(id - 1, customer.c_last);
			}
			else {
				generate_last_name(customer.c_last, CUSTOMERS_PER_DISTRICT);
			}

			string_generator_.get_next(customer.c_street_1, Address::MIN_STREET, Address::MAX_STREET);
			string_generator_.get_next(customer.c_street_2, Address::MIN_STREET, Address::MAX_STREET);
			string_generator_.get_next(customer.c_city, Address::MIN_CITY, Address::MAX_CITY);
			string_generator_.get_next(customer.c_state, Address::STATE, Address::STATE);
			make_zip(customer.c_zip);
			num_string_generator_.get_next(customer.c_phone, Customer::PHONE, Customer::PHONE);
			memcpy(customer.c_since, now_, DATETIME_SIZE);
			if (bad_credit) {
				strcpy(customer.c_credit, Customer::BAD_CREDIT);
			} else {
				strcpy(customer.c_credit, Customer::GOOD_CREDIT);
			}
			string_generator_.get_next(customer.c_data, Customer::MIN_DATA, Customer::MAX_DATA);

			return customer;
		}

		/*!
		 * @brief Fill order with random data
		 * @param id ID of order(o_id)
		 * @param c_id ID of customer
		 * @param d_id ID of district
		 * @param w_id ID of warehouse
		 * @param new_order Whether the order is new
		 * @return A tuple of order
		 */
		Order generate_order(int32_t id, int32_t c_id, int32_t d_id, int32_t w_id, bool new_order) {
			Order order {
					.o_id        = id,
					.o_c_id      = c_id,
					.o_d_id      = d_id,
					.o_w_id      = w_id,
					.o_ol_cnt    = order_ol_cnt_generator_.get_next(),
					.o_all_local = Order::INITIAL_ALL_LOCAL
			};

			if (!new_order) {
				order.o_carrier_id = order_carrier_id_generator_.get_next();
			}
			else {
				order.o_carrier_id = Order::NULL_CARRIER_ID;
			}

			memcpy(order.o_entry_d, now_, DATETIME_SIZE);
			return order;
		}

		/*!
		 * @brief Fill order line with random data
		 * @param number
		 * @param o_id ID of order
		 * @param d_id ID of district
		 * @param w_id ID of warehouse
		 * @param new_order Whether the order is new.
		 * @return A tuple of order line.
		 */
		OrderLine generate_order_line(int32_t number, int32_t o_id, int32_t d_id, int32_t w_id, bool new_order) {
			OrderLine orderline {
					.ol_o_id        = o_id,
					.ol_d_id        = d_id,
					.ol_w_id        = w_id,
					.ol_number      = number,
					.ol_i_id        = orderline_i_id_generator_.get_next(),
					.ol_supply_w_id = w_id,
					.ol_quantity    = OrderLine::INITIAL_QUANTITY
			};

			if (!new_order) {
				orderline.ol_amount = 0.00;
				memcpy(orderline.ol_delivery_d, now_, DATETIME_SIZE);
			} else {
				orderline.ol_amount = orderline_amount_generator_.get_next();
				// HACK: Empty delivery date == null
				orderline.ol_delivery_d[0] = '\0';
			}
			string_generator_.get_next(orderline.ol_dist_info, sizeof(orderline.ol_dist_info)-1,
			                 sizeof(orderline.ol_dist_info)-1);
			return orderline;
		}

		/*!
		 * @brief Fill history with random data
		 * @param c_id ID of customer
		 * @param d_id ID of district
		 * @param w_id ID of warehouse
		 * @return A tuple of history
		 */
		History generate_history(int32_t c_id, int32_t d_id, int32_t w_id) {
			History history {
					.h_c_id   = c_id,
					.h_c_d_id = d_id,
					.h_c_w_id = w_id,
					.h_d_id   = d_id,
					.h_w_id   = w_id,
					.h_amount = History::INITIAL_AMOUNT
			};
			memcpy(history.h_date, now_, DATETIME_SIZE);
			string_generator_.get_next(history.h_data, History::MIN_DATA, History::MAX_DATA);
			return history;
		}

	private:
		/*!
		 * @brief Insert origin mark into string
		 * @param s The address of string
		 */
		void set_original(char *s) {
			memcpy(s, "ORIGINAL", 8);
		}

		float make_tax() {
			static_assert(Warehouse::MIN_TAX == District::MIN_TAX);
			static_assert(Warehouse::MAX_TAX == District::MAX_TAX);
			return warehouse_tax_generator_.get_next();
		}

		void make_zip(char *zip) {
			num_string_generator_.get_next(zip, 4, 4);
			memcpy(zip + 4, "11111", 6);
		}

		template<class IntType>
		void make_permutation(IntType *array, IntType lower, IntType upper) {
			IntType len = upper - lower + 1;
			// initialize with consecutive values
			for (IntType i = 0; i < len; ++i) { array[i] = i + lower; }

			for (IntType i = 0; i < len; ++i) {
				// choose a value to go into this position, including this position
				int index = util::rander.rand_range(i, len - 1);
				std::swap(array[i], array[index]);
			}
		}

	public:

		void update_rander_constant() {
			nurand_generator_.update_constant();
		}

		int32_t generate_warehouse_id() {
			return warehouse_id_generator_.get_next();
		}

		int32_t generate_warehouse_id_exclude(int32_t exclude) {
			int32_t res;
			do {
				res = warehouse_id_generator_.get_next();
			} while(exclude == res);
			return res;
		}

		/*!
		 * @brief Generate ID of district
		 * @return
		 */
		int32_t generate_district_id() {
			return district_id_generator_.get_next();
		}

		/*!
		 * @brief Generate ID of customer
		 * @return
		 */
		int32_t generate_cid() {
			return nurand_generator_.get_next<1023>(1, CUSTOMERS_PER_DISTRICT);
		}

		/*!
		 * @brief Generate ID of item
		 * @return
		 */
		int32_t generate_item_id() {
			return nurand_generator_.get_next<8191>(1, NUM_ITEMS);
		}

		/*!
		 * @brief Generate number of orderline
		 * @return
		 */
		int32_t generate_orderline_cnt() {
			return order_ol_cnt_generator_.get_next();
		}

		/*!
		 * @brief Generate ID of order carrier
		 * @return
		 */
		int32_t generate_order_carrier_id() {
			return order_carrier_id_generator_.get_next();
		}

		/*!
		 * @brief Generate amount of payment
		 * @return
		 */
		float generate_payment_amount() {
			return payment_amount_generator_.get_next();
		}

		/*!
		 * @brief Generate quantity of orderline
		 * @return
		 */
		int32_t generate_orderline_quantity() {
			return orderline_quantity_generator_.get_next();
		}

		/*!
		 * @brief Generate threshold of stock level
		 * @return
		 */
		int32_t generate_stock_level_threshold() {
			return stock_level_threshold_generator_.get_next();
		}

		/*!
		 * @brief Generate a string of last name
		 * @param c_last
		 * @param max_cid
		 */
		void generate_last_name(char *c_last, int max_cid) {
			last_name_generator_.get_last_name(
					nurand_generator_.get_next<255>(0, std::min(999, max_cid - 1)), c_last);
		}

	};
}
