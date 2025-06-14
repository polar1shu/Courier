/*
 * @author: BL-GS 
 * @date:   2023/2/28
 */

#pragma once

#include <cstdio>
#include <cstdint>
#include <algorithm>
#include <vector>
#include <set>
#include <optional>

#include <util/utility_macro.h>
#include <workload/tpcc/tpcc_config_manager.h>
#include <workload/tpcc/tpcc_table.h>
#include <workload/tpcc/tpcc_secondary_index.h>

namespace workload {

	inline namespace tpcc {

		class TPCCKeyMaker {
		public:

			static int64_t make_stock_key(int32_t w_id, int32_t s_id) {
				DEBUG_ASSERT(1 <= w_id && w_id <= Warehouse::MAX_WAREHOUSE_ID);
				DEBUG_ASSERT(1 <= s_id && s_id <= Stock::NUM_STOCK_PER_WAREHOUSE);
				int64_t id = (int64_t)s_id + ((int64_t)w_id * Stock::NUM_STOCK_PER_WAREHOUSE);
				return id;
			}

			static int64_t make_district_key(int32_t w_id, int32_t d_id) {
				DEBUG_ASSERT(1 <= w_id && w_id <= Warehouse::MAX_WAREHOUSE_ID);
				DEBUG_ASSERT(1 <= d_id && d_id <= District::NUM_PER_WAREHOUSE);
				int64_t id = (int64_t)d_id + ((int64_t)w_id * District::NUM_PER_WAREHOUSE);
				return id;
			}

			static int64_t make_new_order_key(int32_t w_id, int32_t d_id, int32_t o_id) {
				DEBUG_ASSERT(1 <= w_id && w_id <= Warehouse::MAX_WAREHOUSE_ID);
				DEBUG_ASSERT(1 <= d_id && d_id <= District::NUM_PER_WAREHOUSE);
				DEBUG_ASSERT(1 <= o_id && o_id <= Order::MAX_ORDER_ID);
				int64_t upper_id = w_id * Warehouse::MAX_WAREHOUSE_ID + d_id;
				DEBUG_ASSERT(upper_id > 0);
				int64_t id = (upper_id << 32) | (int64_t)o_id;
				return id;
			}

			static int64_t make_customer_key(int32_t w_id, int32_t d_id, int32_t c_id) {
				DEBUG_ASSERT(1 <= w_id && w_id <= Warehouse::MAX_WAREHOUSE_ID);
				DEBUG_ASSERT(1 <= d_id && d_id <= District::NUM_PER_WAREHOUSE);
				DEBUG_ASSERT(1 <= c_id && c_id <= Customer::NUM_PER_DISTRICT);
				int64_t id = (int64_t)(w_id * District::NUM_PER_WAREHOUSE + d_id)
				             * Customer::NUM_PER_DISTRICT + c_id;
				return id;
			}

			static int64_t make_order_key(int32_t w_id, int32_t d_id, int32_t o_id) {
				DEBUG_ASSERT(1 <= w_id && w_id <= Warehouse::MAX_WAREHOUSE_ID);
				DEBUG_ASSERT(1 <= d_id && d_id <= District::NUM_PER_WAREHOUSE);
				DEBUG_ASSERT(1 <= o_id && o_id <= Order::MAX_ORDER_ID);
				// TODO: This is bad for locality since o_id is in the most significant position. Larger keys?
				int64_t id = ((int64_t)o_id * District::NUM_PER_WAREHOUSE + d_id)
				             * Warehouse::MAX_WAREHOUSE_ID + w_id;
				return id;
			}

			static int64_t make_order_by_customer_key(int32_t w_id, int32_t d_id, int32_t c_id, int32_t o_id) {
				DEBUG_ASSERT(1 <= w_id && w_id <= Warehouse::MAX_WAREHOUSE_ID);
				DEBUG_ASSERT(1 <= d_id && d_id <= District::NUM_PER_WAREHOUSE);
				DEBUG_ASSERT(1 <= c_id && c_id <= Customer::NUM_PER_DISTRICT);
				DEBUG_ASSERT(1 <= o_id && o_id <= Order::MAX_ORDER_ID);
				int32_t top_id = (w_id * District::NUM_PER_WAREHOUSE + d_id) * Customer::NUM_PER_DISTRICT
				                 + c_id;
				DEBUG_ASSERT(top_id >= 0);
				int64_t id = (((int64_t) top_id) << 32) | o_id;
				return id;
			}

			static int64_t make_order_line_key(int32_t w_id, int32_t d_id, int32_t o_id, int32_t number) {
				DEBUG_ASSERT(1 <= w_id && w_id <= Warehouse::MAX_WAREHOUSE_ID);
				DEBUG_ASSERT(1 <= d_id && d_id <= District::NUM_PER_WAREHOUSE);
				DEBUG_ASSERT(1 <= o_id && o_id <= Order::MAX_ORDER_ID);
				DEBUG_ASSERT(1 <= number && number <= Order::MAX_OL_CNT);
				// TODO: This may be bad for locality since o_id is in the most significant position. However,
				// Order status fetches all rows for one (w_id, d_id, o_id) tuple, so it may be fine,
				// but stock level fetches order lines for a range of (w_id, d_id, o_id) values
				int64_t id = ((int64_t)(o_id * District::NUM_PER_WAREHOUSE + d_id)
				              * Warehouse::MAX_WAREHOUSE_ID + w_id) * Order::MAX_OL_CNT + number;
				return id;
			}
		};

		template<class Config, class Executor>
		class TPCCWorkloadEngine {};

		template<class Config, class Executor>
			requires FineGranularityExecutorConcept<Executor, AbstractKey<typename TPCCConfigManager<Config>::KeyType>>
		class TPCCWorkloadEngine<Config, Executor> {
		public:
			using ConfigManager                 = TPCCConfigManager<Config>;

			using KeyType                       = ConfigManager::KeyType;

			using TPCCKey                       = AbstractKey<KeyType>;

			using SecondaryIndexType            = TPCCSecondaryIndex<Config>;

			static constexpr bool COPY_STRING      = ConfigManager::COPY_STRING;

			static constexpr bool FORMAL_OUTPUT    = ConfigManager::FORMAL_OUTPUT;

			static constexpr bool DO_INSERT_REMOVE = ConfigManager::DO_INSERT_REMOVE;

		private:
			Executor *executor_ptr_;

			SecondaryIndexType *second_index_ptr_;

		public:
			explicit TPCCWorkloadEngine(
					Executor *executor_ptr,
					SecondaryIndexType *second_index_ptr):
					executor_ptr_(executor_ptr),
					second_index_ptr_(second_index_ptr) {}

		public:

			int32_t stock_level(int32_t warehouse_id, int32_t district_id, int32_t threshold) {
				/* EXEC SQL SELECT d_next_o_id INTO :o_id FROM district
				WHERE d_w_id=:w_id AND d_id=:d_id; */
				const District *d_ptr = find_district(warehouse_id, district_id);
				if (d_ptr == nullptr) [[unlikely]] { return false; }
				const District &d = *d_ptr;

				int32_t o_id = d.d_next_o_id;

				/* EXEC SQL SELECT COUNT(DISTINCT (s_i_id)) INTO :stock_count FROM order_line, stock
					WHERE ol_w_id=:w_id AND ol_d_id=:d_id AND ol_o_id<:o_id AND ol_o_id>=:o_id-20
						AND s_w_id=:w_id AND s_i_id=ol_i_id AND s_quantity < :threshold;*/

				// with linear search, and std::vector with binary search using std::lower_bound. The best
				// seemed to be to simply save all the s_i_ids, then sort and eliminate duplicates at the end.
				std::vector<int32_t> s_i_ids;
				// Average size is more like ~30.
				s_i_ids.reserve(300);

				// Iterate over [o_id-20, o_id)
				DEBUG_ASSERT(o_id >= STOCK_LEVEL_ORDERS);
				for (int order_id = o_id - STOCK_LEVEL_ORDERS; order_id < o_id; ++order_id) {
					// HACK: We shouldn't rely on MAX_OL_CNT. See comment above.
					for (int line_number = 1; line_number <= Order::MAX_OL_CNT; ++line_number) {
						const OrderLine *line_ptr = find_order_line(warehouse_id, district_id, order_id, line_number);
						if (line_ptr == nullptr) [[unlikely]] { break; }
						const OrderLine &line = *line_ptr;

						// Check if s_quantity < threshold
						const Stock *stock_ptr = find_stock(warehouse_id, line.ol_i_id);
						if (stock_ptr == nullptr) [[unlikely]] { return false; }
						const Stock &stock = *stock_ptr;

						if (stock.s_quantity < threshold) {
							s_i_ids.push_back(line.ol_i_id);
						}
					}
				}

				// Filter out duplicate s_i_id: multiple order lines can have the same item
				if constexpr (FORMAL_OUTPUT) {
					std::sort(s_i_ids.begin(), s_i_ids.end());
					[[maybe_unused]]int num_distinct = 0;
					int32_t last = -1;  // NOTE: This relies on -1 being an invalid s_i_id
					for (int s_i_id : s_i_ids) {
						if (s_i_id != last) {
							last = s_i_id;
							++num_distinct;
						}
					}
				}

				return true;
			}


			bool order_status(int32_t warehouse_id, int32_t district_id, int32_t customer_id,
			                 OrderStatusOutput* output) {
				const Customer *c_ptr = find_customer(warehouse_id, district_id, customer_id);
				if (c_ptr == nullptr) [[unlikely]] { return false; }
				const Customer &c = *c_ptr;

				return internal_order_status(c, output);
			}

			bool order_status(int32_t warehouse_id, int32_t district_id, const char* c_last,
			                  OrderStatusOutput* output) {
				const CustomerNameIdentify c_iden = find_customer_by_name(warehouse_id, district_id, c_last);

				const Customer *c_ptr = find_customer(warehouse_id, district_id, c_iden.c_id);
				if (c_ptr == nullptr) [[unlikely]] { return false; }
				const Customer &c = *c_ptr;

				return internal_order_status(c, output);
			}

			bool new_order(int32_t warehouse_id, int32_t district_id, int32_t customer_id,
			               const auto item_iter_begin, const auto item_iter_end,
						   const char* now,
						   bool do_remote, NewOrderOutput* output) {
				// perform the home part
				bool result = new_order_local(warehouse_id, district_id, customer_id,
											  item_iter_begin, item_iter_end, now, do_remote, output);
				return result;
			}

			bool new_order_local(int32_t warehouse_id, int32_t district_id, int32_t customer_id,
			                     const auto item_iter_begin, const auto item_iter_end,
								 const char* now,
								 bool do_remote, NewOrderOutput* output) {
				{
					const Customer *c_ptr = find_customer(warehouse_id, district_id, customer_id);
					if (c_ptr == nullptr) [[unlikely]] { return false; }
					const Customer &c = *c_ptr;

					if constexpr (FORMAL_OUTPUT) {
						memcpy(output->c_last, c.c_last, sizeof(output->c_last));
						memcpy(output->c_credit, c.c_credit, sizeof(output->c_credit));
						output->c_discount = c.c_discount;
					}
				}

				// We will not abort: update the status and the database state, allocate an undo buffer
				if constexpr (COPY_STRING) {
					output->status[0] = '\0';
				}

				int32_t order_id;

				{
					constexpr uint32_t update_offset = offsetof(District, d_next_o_id);
					constexpr uint32_t update_size   = sizeof(District::d_next_o_id);

					District *d_ptr = update_district(warehouse_id, district_id, update_size, update_offset);
					if (d_ptr == nullptr) [[unlikely]] { return false; }
					District &d = *d_ptr;

					if constexpr (FORMAL_OUTPUT) {
						output->d_tax = d.d_tax;
						output->o_id  = d.d_next_o_id;
					}

					order_id = d.d_next_o_id;
					// Modify the order id to assign it
					d.d_next_o_id += 1;
				}

				{
					const Warehouse *w_ptr = find_warehouse(warehouse_id);
					if (w_ptr == nullptr) [[unlikely]] { return false; }
					const Warehouse &w = *w_ptr;

					if constexpr (FORMAL_OUTPUT) {
						output->w_tax = w.w_tax;
					}
				}

				{
					Order order {
							.o_id         = order_id,
							.o_c_id       = customer_id,
							.o_d_id       = district_id,
							.o_w_id       = warehouse_id,
							.o_carrier_id = Order::NULL_CARRIER_ID,
							.o_ol_cnt     = static_cast<int>(item_iter_end - item_iter_begin),
							.o_all_local  = do_remote ? 0 : 1
					};

					if constexpr (FORMAL_OUTPUT) {
						memcpy(order.o_entry_d, now, DATETIME_SIZE);
					}

					if constexpr (DO_INSERT_REMOVE) {
						if (!insert_order(order)) [[unlikely]] { return false; }
					}
				}

				{
					NewOrder new_order = {
							.no_w_id = warehouse_id,
							.no_d_id = district_id,
							.no_o_id = order_id
					};
					if constexpr (DO_INSERT_REMOVE) {
						if (!insert_new_order(new_order)) [[unlikely]] { return false; }
					}
				}

				// CHEAT: Validate all items to see if we will need to abort
				std::array<const Item *, Order::MAX_OL_CNT> item_tuples;
				if (!find_and_validate_items(item_iter_begin, item_iter_end, item_tuples.begin())) [[unlikely]] {
					if constexpr (FORMAL_OUTPUT) {
						strcpy(output->status, NewOrderOutput::INVALID_ITEM_STATUS);
					}
					return false;
				}

				OrderLine line {
						.ol_o_id = order_id,
						.ol_d_id = district_id,
						.ol_w_id = warehouse_id
				};
				if constexpr (COPY_STRING) {
					memset(line.ol_delivery_d, 0, DATETIME_SIZE);
				}

				if constexpr (FORMAL_OUTPUT) {
					output->items.resize(item_iter_end - item_iter_begin);
					output->total = 0;
				}

				int ol_number_count = 0;
				for (auto item_iter = item_iter_begin; item_iter != item_iter_end; ++item_iter) {
					const auto &item             = *item_iter;
					const auto &item_tuple       = *item_tuples[ol_number_count];

					constexpr uint32_t stock_update_offset = util_macro::get_min_among(
							offsetof(Stock, s_quantity),
							offsetof(Stock, s_ytd),
							offsetof(Stock, s_order_cnt),
							offsetof(Stock, s_remote_cnt)
					);
					constexpr uint32_t stock_update_size = util_macro::get_max_among(
							sizeof(Stock::s_quantity)   + offsetof(Stock, s_quantity),
							sizeof(Stock::s_ytd)        + offsetof(Stock, s_ytd),
							sizeof(Stock::s_order_cnt)  + offsetof(Stock, s_order_cnt),
							sizeof(Stock::s_remote_cnt) + offsetof(Stock, s_remote_cnt)
					) - stock_update_offset;
					// Vertical Partitioning HACK: We read s_dist_xx from our local replica, assuming that
					// these columns are replicated everywhere.
					// replicas. Try the "two round" version in the future.
					Stock *stock_ptr = update_stock(item.ol_supply_w_id, item.i_id, stock_update_size, stock_update_offset);
					if (stock_ptr == nullptr) [[unlikely]] { return false; }
					Stock &stock = *stock_ptr;

					// update stock

					if (stock.s_quantity >= item.ol_quantity + 10) {
						stock.s_quantity -= item.ol_quantity;
					}
					else {
						stock.s_quantity = stock.s_quantity - item.ol_quantity + 91;
					}

					auto &output_item = output->items[ol_number_count];
					if constexpr (FORMAL_OUTPUT) {
						output_item.s_quantity = stock.s_quantity;
					}

					stock.s_ytd       += item.ol_quantity;
					stock.s_order_cnt += 1;
					// new_order_local calls new_order_remote, so this is needed
					if (item.ol_supply_w_id != warehouse_id) {
						// remote order
						stock.s_remote_cnt += 1;
					}

					// Make order line

					// Since we *need* to replicate s_dist_xx columns, might as well replicate s_data
					// Makes it 290 bytes per tuple, or ~28 MB per warehouse.

					if constexpr (FORMAL_OUTPUT) {
						bool stock_is_original = (strstr(stock.s_data, "ORIGINAL") != nullptr);
						if (stock_is_original && strstr(item_tuples[item_iter - item_iter_begin]->i_data, "ORIGINAL") != nullptr) {
							output_item.brand_generic = NewOrderOutput::ItemInfo::BRAND;
						}
						else {
							output_item.brand_generic = NewOrderOutput::ItemInfo::GENERIC;
						}
					}

					if constexpr (FORMAL_OUTPUT) {
						static_assert(sizeof(output_item.i_name) == sizeof(item_tuple.i_name));
						memcpy(output_item.i_name, item_tuple.i_name, sizeof(output_item.i_name));
						output_item.i_price    = item_tuple.i_price;
						output_item.ol_amount  = static_cast<float>(item.ol_quantity) * item_tuple.i_price;
						output->total         += output_item.ol_amount;
					}

					line.ol_number      = ++ol_number_count;
					line.ol_i_id        = item.i_id;
					line.ol_supply_w_id = item.ol_supply_w_id;
					line.ol_quantity    = item.ol_quantity;
					line.ol_amount      = static_cast<float>(item.ol_quantity) * item_tuple.i_price;

					if constexpr (COPY_STRING) {
						memcpy(line.ol_dist_info, stock.s_dist[district_id], sizeof(line.ol_dist_info));
					}

					if constexpr (DO_INSERT_REMOVE) {
						if (!insert_order_line(line)) [[unlikely]] { return false; }
					}
				}

				return true;
			}

			bool payment(int32_t warehouse_id, int32_t district_id, int32_t c_warehouse_id,
			             int32_t c_district_id, int32_t customer_id, float h_amount, const char* now,
			             PaymentOutput* output) {
				constexpr uint32_t customer_update_offset = std::min(offsetof(Customer, c_balance), std::min(offsetof(Customer, c_ytd_payment), offsetof(Customer, c_payment_cnt)));
				constexpr uint32_t customer_update_size   = std::max(
						sizeof(Customer::c_balance) + offsetof(Customer, c_balance),
						sizeof(Customer::c_ytd_payment) + std::max(offsetof(Customer, c_ytd_payment),
					    sizeof(Customer::c_payment_cnt) + offsetof(Customer, c_payment_cnt))) - customer_update_offset;

				Customer *customer_ptr = update_customer(c_warehouse_id, c_district_id, customer_id, customer_update_size, customer_update_offset);
				if (customer_ptr == nullptr) [[unlikely]] { return false; }
				Customer &customer = *customer_ptr;

				if (!payment_local(
						warehouse_id, district_id, c_warehouse_id, c_district_id, customer_id, h_amount,
						now, output
				)) [[unlikely]] { return false; }
				internal_payment_remote(warehouse_id, district_id, customer, h_amount, output);
				return true;
			}

			bool payment(int32_t warehouse_id, int32_t district_id, int32_t c_warehouse_id,
			                     int32_t c_district_id, const char* c_last, float h_amount, const char* now,
			                     PaymentOutput* output) {
				const CustomerNameIdentify temp_customer = find_customer_by_name(c_warehouse_id, c_district_id, c_last);

				constexpr uint32_t customer_update_offset = util_macro::get_min_among(
						offsetof(Customer, c_balance),
						offsetof(Customer, c_ytd_payment),
						offsetof(Customer, c_payment_cnt)
				);
				constexpr uint32_t customer_update_size   = util_macro::get_max_among(
						offsetof(Customer, c_balance),
						offsetof(Customer, c_ytd_payment),
						offsetof(Customer, c_payment_cnt)
				) - customer_update_offset;
				Customer *customer_ptr = update_customer(temp_customer.c_w_id, temp_customer.c_d_id, temp_customer.c_id, customer_update_size, customer_update_offset);
				if (customer_ptr == nullptr) [[unlikely]] { return false; }
				Customer &customer = *customer_ptr;

				if (!payment_local(
						warehouse_id, district_id, c_warehouse_id, c_district_id, customer.c_id, h_amount,
						now, output
				)) [[unlikely]] { return false; }
				internal_payment_remote(warehouse_id, district_id, customer, h_amount, output);

				return true;
			}

			bool payment_local(int32_t warehouse_id, int32_t district_id, int32_t c_warehouse_id,
			                   int32_t c_district_id, int32_t customer_id, float h_amount, const char* now,
			                   PaymentOutput* output) {
				Warehouse *w_ptr = update_warehouse(warehouse_id);
				if (w_ptr == nullptr) [[unlikely]] { return false; }
				Warehouse &w = *w_ptr;

				w.w_ytd += h_amount;
				if constexpr (FORMAL_OUTPUT) {
					Address::copy(
							output->w_street_1, output->w_street_2, output->w_city,
							output->w_state, output->w_zip,
							w.w_street_1, w.w_street_2, w.w_city,
							w.w_state, w.w_zip);
				}


				District *d_ptr = update_district(warehouse_id, district_id);
				if (d_ptr == nullptr) [[unlikely]] { return false; }
				District &d = *d_ptr;

				d.d_ytd += h_amount;
				if constexpr (FORMAL_OUTPUT) {
					Address::copy(
							output->d_street_1, output->d_street_2, output->d_city,
							output->d_state, output->d_zip,
							d.d_street_1, d.d_street_2, d.d_city,d.d_state, d.d_zip);
				}

				// Insert the line into the history table
				History h {
						.h_c_id   = customer_id,
						.h_c_d_id = c_district_id,
						.h_c_w_id = c_warehouse_id,
						.h_d_id   = district_id,
						.h_w_id   = warehouse_id,
						.h_amount = h_amount
				};
				if constexpr (COPY_STRING) {
					memcpy(h.h_date, now, DATETIME_SIZE);
					strcpy(h.h_data, w.w_name);
					strcat(h.h_data, "    ");
					strcat(h.h_data, d.d_name);
				}

				if constexpr (DO_INSERT_REMOVE) {
					if (!insert_history(h)) [[unlikely]] { return false; }
				}

				// Zero all the customer fields: avoid uninitialized data for serialization
				if constexpr (FORMAL_OUTPUT) {
					output->c_credit_lim  = 0;
					output->c_discount    = 0;
					output->c_balance     = 0;
					output->c_first[0]    = '\0';
					output->c_middle[0]   = '\0';
					output->c_last[0]     = '\0';
					output->c_phone[0]    = '\0';
					output->c_since[0]    = '\0';
					output->c_credit[0]   = '\0';
					output->c_data[0]     = '\0';
					output->c_street_1[0] = '\0';
					output->c_street_2[0] = '\0';
					output->c_city[0]     = '\0';
					output->c_state[0]    = '\0';
					output->c_zip[0]      = '\0';
				}

				return true;
			}

			bool delivery(int32_t warehouse_id, int32_t carrier_id, const char* now,
			              std::vector<DeliveryOrderInfo>* orders) {
				orders->clear();
				for (int32_t d_id = 1; d_id <= District::NUM_PER_WAREHOUSE; ++d_id) {
					constexpr uint32_t new_order_id_update_size   = sizeof(NewOrderID::next_no_o_id);
					constexpr uint32_t new_order_id_update_offset = offsetof(NewOrderID, next_no_o_id);

					NewOrderID *new_order_id_ptr = update_new_order_id(warehouse_id, d_id, new_order_id_update_size, new_order_id_update_offset);
					if (new_order_id_ptr == nullptr) [[unlikely]] { return false; }
					NewOrderID &new_order_id_tuple = *new_order_id_ptr;

					int32_t new_order_key = new_order_id_tuple.next_no_o_id;
					int32_t o_id = new_order_key;

					if constexpr (DO_INSERT_REMOVE) {
						++new_order_id_tuple.next_no_o_id;
					}
					if constexpr (DO_INSERT_REMOVE) {
						if (!delete_new_order(TPCCKeyMaker::make_new_order_key(warehouse_id, d_id, o_id))) [[unlikely]] { return false; }
					}

					orders->emplace_back(
							DeliveryOrderInfo {
									.d_id = d_id,
									.o_id = o_id
							}
					);

					constexpr uint32_t order_update_size   = sizeof(Order::o_carrier_id);
					constexpr uint32_t order_update_offset = offsetof(Order, o_carrier_id);

					Order *o_ptr = update_order(warehouse_id, d_id, o_id, order_update_size, order_update_offset);
					if (o_ptr == nullptr) [[unlikely]] { return false; }
					Order &o = *o_ptr;

					if constexpr (DO_INSERT_REMOVE) {
						if (o.o_carrier_id != Order::NULL_CARRIER_ID) { return false; }
						o.o_carrier_id = carrier_id;
					}

					float total = 0;

					if constexpr (DO_INSERT_REMOVE) {
						for (int32_t i = 1; i <= o.o_ol_cnt; ++i) {
							constexpr uint32_t order_line_update_size   = sizeof(OrderLine::ol_delivery_d);
							constexpr uint32_t order_line_update_offset = offsetof(OrderLine, ol_delivery_d);

							OrderLine *line_ptr = update_order_line(warehouse_id, d_id, o_id, i, order_line_update_size, order_line_update_offset);
							if (line_ptr == nullptr) [[unlikely]] { return false; }
							OrderLine &line = *line_ptr;

							if (line.ol_delivery_d[0] != '\0') [[unlikely]] { return false; }

							if constexpr (COPY_STRING) {
								memcpy(line.ol_delivery_d, now, DATETIME_SIZE);
							}

							total += line.ol_amount;
						}
					}

					{
						constexpr uint32_t customer_update_offset = std::min(
								offsetof(Customer, c_balance),
								offsetof(Customer, c_delivery_cnt)
						);
						constexpr uint32_t customer_update_size   = std::max(
								offsetof(Customer, c_balance) + sizeof(Customer::c_balance),
								offsetof(Customer, c_delivery_cnt) + sizeof(Customer::c_delivery_cnt)
						) - customer_update_offset;

						Customer *c_ptr = update_customer(warehouse_id, d_id, o.o_c_id, customer_update_size, customer_update_offset);
						if (c_ptr == nullptr) [[unlikely]] { return false; }
						Customer &c = *c_ptr;

						c.c_balance += total;
						c.c_delivery_cnt += 1;
					}
				}

				return true;
			}

		private:
			static const int STOCK_LEVEL_ORDERS = 20;

			// Loads each item from the items table. Returns true if they are all found.
			bool find_and_validate_items(const auto item_iter_begin, const auto item_iter_end,
			                             auto item_tuple_iter) {
				auto output_iter = item_tuple_iter;
				// CHEAT: Validate all items to see if we will need to abort
				for (auto iter = item_iter_begin; iter != item_iter_end; ++iter, ++output_iter) {
					const Item *item_ptr = find_item(iter->i_id);
					if (item_ptr == nullptr) [[unlikely]] { return false; }
					*output_iter = item_ptr;
				}
				return true;
			}

			// Implements order status transaction after the customer tuple has been located.
			bool internal_order_status(const Customer &customer, OrderStatusOutput* output) {
				if constexpr (FORMAL_OUTPUT) {
					output->c_id = customer.c_id;
					// retrieve from customer: balance, first, middle, last
					output->c_balance = customer.c_balance;
					strcpy(output->c_first, customer.c_first);
					strcpy(output->c_middle, customer.c_middle);
					strcpy(output->c_last, customer.c_last);
				}

				// Find the row in the order table with largest o_id
				const Order *order_ptr = find_last_order_by_customer(customer.c_w_id, customer.c_d_id, customer.c_id);
				if (order_ptr == nullptr) [[unlikely]] { return false;}
				const Order &order = *order_ptr;

				if constexpr (FORMAL_OUTPUT) {
					output->o_id         = order.o_id;
					output->o_carrier_id = order.o_carrier_id;
					memcpy(output->o_entry_d, order.o_entry_d, DATETIME_SIZE);
					output->lines.resize(order.o_ol_cnt);
				}

				for (int32_t line_number = 1; line_number <= order.o_ol_cnt; ++line_number) {
					const OrderLine *line_ptr = find_order_line(order.o_w_id, order.o_d_id, order.o_id, line_number);
					if (line_ptr == nullptr) [[unlikely]] { return false; }
					const OrderLine &line = *line_ptr;

					if constexpr (FORMAL_OUTPUT) {
						output->lines[line_number - 1] = {
								.ol_i_id        = line.ol_i_id,
								.ol_supply_w_id = line.ol_supply_w_id,
								.ol_quantity    = line.ol_quantity,
								.ol_amount      = line.ol_amount
						};
						memcpy(output->lines[line_number - 1].ol_delivery_d, line.ol_delivery_d, DATETIME_SIZE);
					}
				}

				return true;
			}

			// Implements payment transaction after the customer tuple has been located.
			void internal_payment_remote(int32_t warehouse_id, int32_t district_id, Customer &c,
			                             float h_amount, PaymentOutput* output) {
				c.c_balance     -= h_amount;
				c.c_ytd_payment += h_amount;
				c.c_payment_cnt += 1;

				if constexpr (COPY_STRING) {
					if (strcmp(c.c_credit, Customer::BAD_CREDIT) == 0) {
						// Bad credit: insert history into c_data
						static constexpr int HISTORY_SIZE = Customer::MAX_DATA + 1;

						char history[HISTORY_SIZE];
						int characters = snprintf(history, HISTORY_SIZE, "(%d, %d, %d, %d, %d, %.2f)\n",
						                          c.c_id, c.c_d_id, c.c_w_id, district_id, warehouse_id, h_amount);
						DEBUG_ASSERT(characters < HISTORY_SIZE);

						// Perform the insert with a move and copy
						int current_keep = static_cast<int>(strlen(c.c_data));
						if (current_keep + characters > Customer::MAX_DATA) {
							current_keep = Customer::MAX_DATA - characters;
						}
						DEBUG_ASSERT(current_keep + characters <= Customer::MAX_DATA);
						memmove(c.c_data+characters, c.c_data, current_keep);
						memcpy(c.c_data, history, characters);
						c.c_data[characters + current_keep] = '\0';
						DEBUG_ASSERT(strlen(c.c_data) == static_cast<size_t>(characters) + current_keep);
					}
				}

				if constexpr (FORMAL_OUTPUT) {
					output->c_credit_lim = c.c_credit_lim;
					output->c_discount   = c.c_discount;
					output->c_balance    = c.c_balance;
					memcpy(output->c_first, c.c_first, sizeof(output->c_first));
					memcpy(output->c_middle, c.c_middle, sizeof(output->c_middle));
					memcpy(output->c_last, c.c_last, sizeof(output->c_last));
					memcpy(output->c_phone, c.c_phone, sizeof(output->c_phone));
					memcpy(output->c_since, c.c_since, sizeof(output->c_since));
					memcpy(output->c_credit, c.c_credit, sizeof(output->c_credit));
					memcpy(output->c_data, c.c_data, sizeof(output->c_data));

					Address::copy(
							output->c_street_1, output->c_street_2, output->c_city,
							output->c_state, output->c_zip,
							c.c_street_1, c.c_street_2, c.c_city,
							c.c_state, c.c_zip);
				}
			}

		private:
			const Item *find_item(int32_t id) {
				TPCCKey key(DurableTable::Item, id);
				return executor_ptr_->template read<Item>(key);
			}

			const Warehouse *find_warehouse(int32_t w_id) {
				TPCCKey key(DurableTable::Warehouse, w_id);
				return executor_ptr_->template read<Warehouse>(key);
			}

			const Stock *find_stock(int64_t num_key) {
				TPCCKey key(DurableTable::Stock, num_key);
				return executor_ptr_->template read<Stock>(key);
			}

			const Stock *find_stock(int32_t w_id, int32_t i_id) {
				return find_stock(TPCCKeyMaker::make_stock_key(w_id, i_id));
			}

			const District *find_district(int64_t num_key) {
				TPCCKey key(DurableTable::District, num_key);
				return executor_ptr_->template read<District>(key);
			}

			const District *find_district(int32_t w_id, int32_t d_id) {
				return find_district(TPCCKeyMaker::make_district_key(w_id, d_id));
			}

			const Customer *find_customer(int64_t num_key) {
				TPCCKey key(DurableTable::Customer, num_key);
				return executor_ptr_->template read<Customer>(key);
			}

			const Customer *find_customer(int32_t w_id, int32_t d_id, int32_t c_id) {
				return find_customer(TPCCKeyMaker::make_customer_key(w_id, d_id, c_id));
			}

			const Order *find_order(int64_t num_key) {
				TPCCKey key(DurableTable::Order, num_key);
				return executor_ptr_->template read<Order>(key);
			}

			const Order *find_order(int32_t w_id, int32_t d_id, int32_t o_id) {
				return find_order(TPCCKeyMaker::make_order_key(w_id, d_id, o_id));
			}

			const OrderLine *find_order_line(int64_t num_key) {
				TPCCKey key(DurableTable::OrderLine, num_key);
				return executor_ptr_->template read<OrderLine>(key);
			}

			const OrderLine *find_order_line(int32_t w_id, int32_t d_id, int32_t o_id, int32_t number) {
				return find_order_line(TPCCKeyMaker::make_order_line_key(w_id, d_id, o_id, number));
			}

			const NewOrder *find_new_order(int64_t num_key) {
				TPCCKey key(DurableTable::NewOrder, num_key);
				return executor_ptr_->template read<NewOrder>(key);
			}

			const NewOrder *find_new_order(int32_t w_id, int32_t d_id, int32_t o_id) {
				return find_new_order(TPCCKeyMaker::make_new_order_key(w_id, d_id, o_id));
			}

			const NewOrderID *find_new_order_id(int64_t num_key) {
				TPCCKey key(DurableTable::NewOrderID, num_key);
				return executor_ptr_->template read<NewOrderID>(key);
			}

			const NewOrderID *find_new_order_id(int32_t w_id, int32_t d_id) {
				// New order id table is corresponded to District.
				// so we just need to reuse the district key.
				return find_new_order_id(TPCCKeyMaker::make_district_key(w_id, d_id));
			}

	        // Finds all customers that match (w_id, d_id, *, c_last), taking the n/2th one (rounded up).
			CustomerNameIdentify find_customer_by_name(int32_t w_id, int32_t d_id, const char* c_last) {
				// select (w_id, d_id, *, c_last) order by c_first
				Customer c {
						.c_d_id = d_id,
						.c_w_id = w_id,
						.c_first = "\0"
				};
				if constexpr (COPY_STRING) {
					strcpy(c.c_last, c_last);
				}


				auto [found, c_iden] = second_index_ptr_->get_customer_indentify_by_name(c);

				if constexpr (COPY_STRING) {
					DEBUG_ASSERT(
							c_iden->c_w_id == w_id && c_iden->c_d_id == d_id &&
							strcmp(c_iden->c_last, c_last) == 0
					);
				}
				return *c_iden;
			}

			const Order *find_last_order_by_customer(int32_t w_id, int32_t d_id, int32_t c_id) {
				// Increment the (w_id, d_id, c_id) tuple
				int64_t order_key = second_index_ptr_->get_order_by_customer(w_id, d_id, c_id);
				return find_order(order_key);
			}

		public:
			// Copies item into the item table.
			bool insert_item(const Item& item) {
				return executor_ptr_->insert(
						TPCCKey(DurableTable::Item, item.i_id),
						&item, sizeof(Item)
				);
			}

			bool insert_stock(const Stock& stock) {
				return executor_ptr_->insert(
						TPCCKey(DurableTable::Stock, TPCCKeyMaker::make_stock_key(stock.s_w_id, stock.s_i_id)),
						&stock, sizeof(Stock)
				);
			}

			bool insert_warehouse(const Warehouse& warehouse) {
				return executor_ptr_->insert(
						TPCCKey(DurableTable::Warehouse, warehouse.w_id),
						&warehouse, sizeof(Warehouse)
				);
			}

			bool insert_district(const District& district) {
				return executor_ptr_->insert(
						TPCCKey(DurableTable::District, TPCCKeyMaker::make_district_key(district.d_w_id, district.d_id)),
						&district, sizeof(District)
				);
			}

			bool insert_customer(const Customer& customer) {
				second_index_ptr_->insert_customer_by_name(customer);

				return executor_ptr_->insert(
						TPCCKey(DurableTable::Customer, TPCCKeyMaker::make_customer_key(customer.c_w_id, customer.c_d_id, customer.c_id)),
						&customer, sizeof(Customer)
				);
			}

			// Stores order in the database. Returns a pointer to the database's tuple.
			bool insert_order(const Order& order) {
				int64_t order_key = TPCCKeyMaker::make_order_key(order.o_w_id, order.o_d_id, order.o_id);

				bool success = executor_ptr_->insert(
						TPCCKey(DurableTable::Order, order_key),
						&order, sizeof(Order)
				);
				if (success) {
					second_index_ptr_->insert_order_by_customer(order.o_w_id, order.o_d_id, order.o_c_id, order_key);
				}
				return success;
			}

			// Creates a new order in the database. Returns a pointer to the database's tuple.
			bool insert_new_order(const NewOrder &new_order) {
				auto key = TPCCKeyMaker::make_new_order_key(new_order.no_w_id, new_order.no_d_id, new_order.no_o_id);

				return executor_ptr_->insert(
						TPCCKey(DurableTable::NewOrder, key),
						&new_order, sizeof(NewOrder)
				);
			}

			// Creates a new order in the database. Returns a pointer to the database's tuple.
			bool insert_new_order_id(const NewOrderID &new_order_id) {
				auto key = TPCCKeyMaker::make_district_key(new_order_id.w_id, new_order_id.d_id);

				return executor_ptr_->insert(
						TPCCKey(DurableTable::NewOrderID, key),
						&new_order_id, sizeof(NewOrderID)
				);
			}

			// Stores orderline in the database. Returns a pointer to the database's tuple.
			bool insert_order_line(const OrderLine& orderline) {
				return executor_ptr_->insert(
						TPCCKey(DurableTable::OrderLine, TPCCKeyMaker::make_order_line_key(orderline.ol_w_id, orderline.ol_d_id, orderline.ol_o_id, orderline.ol_number)),
						&orderline, sizeof(OrderLine)
				);
			}

			bool insert_history(const History& history) {
				static std::atomic<int64_t> history_key{0};
				return executor_ptr_->insert(
						TPCCKey(DurableTable::History, history_key++),
						&history, sizeof(History)
				);
			}

		private:
			Item *update_item(int32_t id) {
				TPCCKey key(DurableTable::Item, id);
				return executor_ptr_->template update<Item>(key);
			}

			Warehouse *update_warehouse(int32_t w_id) {
				TPCCKey key(DurableTable::Warehouse, w_id);
				return executor_ptr_->template update<Warehouse>(key);
			}

			Stock *update_stock(int64_t num_key) {
				TPCCKey key(DurableTable::Stock, num_key);
				return executor_ptr_->template update<Stock>(key);
			}

			Stock *update_stock(int32_t w_id, int32_t i_id) {
				return update_stock(TPCCKeyMaker::make_stock_key(w_id, i_id));
			}

			District *update_district(int64_t num_key) {
				TPCCKey key(DurableTable::District, num_key);
				return executor_ptr_->template update<District>(key);
			}

			District *update_district(int32_t w_id, int32_t d_id) {
				return update_district(TPCCKeyMaker::make_district_key(w_id, d_id));
			}

			Customer *update_customer(int64_t num_key) {
				TPCCKey key(DurableTable::Customer, num_key);
				return executor_ptr_->template update<Customer>(key);
			}

			Customer *update_customer(int32_t w_id, int32_t d_id, int32_t c_id) {
				return update_customer(TPCCKeyMaker::make_customer_key(w_id, d_id, c_id));
			}

			Order *update_order(int64_t num_key) {
				TPCCKey key(DurableTable::Order, num_key);
				return executor_ptr_->template update<Order>(key);
			}

			Order *update_order(int32_t w_id, int32_t d_id, int32_t o_id) {
				return update_order(TPCCKeyMaker::make_order_key(w_id, d_id, o_id));
			}

			OrderLine *update_order_line(int64_t num_key) {
				TPCCKey key(DurableTable::OrderLine, num_key);
				return executor_ptr_->template update<OrderLine>(key);
			}

			OrderLine *update_order_line(int32_t w_id, int32_t d_id, int32_t o_id, int32_t number) {
				return update_order_line(TPCCKeyMaker::make_order_line_key(w_id, d_id, o_id, number));
			}

			NewOrder *update_new_order(int64_t num_key) {
				TPCCKey key(DurableTable::NewOrder, num_key);
				return executor_ptr_->template update<NewOrder>(key);
			}

			NewOrder *update_new_order(int32_t w_id, int32_t d_id, int32_t o_id) {
				return update_new_order(TPCCKeyMaker::make_new_order_key(w_id, d_id, o_id));
			}

			NewOrderID *update_new_order_id(int64_t num_key) {
				TPCCKey key(DurableTable::NewOrderID, num_key);
				return executor_ptr_->template update<NewOrderID>(key);
			}

			NewOrderID *update_new_order_id(int32_t w_id, int32_t d_id) {
				// New order id table is corresponded to District.
				// so we just need to reuse the district key.
				return update_new_order_id(TPCCKeyMaker::make_district_key(w_id, d_id));
			}

			Item *update_item(int32_t id, uint32_t size, uint32_t offset) {
				TPCCKey key(DurableTable::Item, id);
				return executor_ptr_->template update<Item>(key, size, offset);
			}

			Warehouse *update_warehouse(int32_t w_id, uint32_t size, uint32_t offset) {
				TPCCKey key(DurableTable::Warehouse, w_id);
				return executor_ptr_->template update<Warehouse>(key, size, offset);
			}

			Stock *update_stock(int64_t num_key, uint32_t size, uint32_t offset) {
				TPCCKey key(DurableTable::Stock, num_key);
				return executor_ptr_->template update<Stock>(key, size, offset);
			}

			Stock *update_stock(int32_t w_id, int32_t i_id, uint32_t size, uint32_t offset) {
				return update_stock(TPCCKeyMaker::make_stock_key(w_id, i_id), size, offset);
			}

			District *update_district(int64_t num_key, uint32_t size, uint32_t offset) {
				TPCCKey key(DurableTable::District, num_key);
				return executor_ptr_->template update<District>(key, size, offset);
			}

			District *update_district(int32_t w_id, int32_t d_id, uint32_t size, uint32_t offset) {
				return update_district(TPCCKeyMaker::make_district_key(w_id, d_id), size, offset);
			}

			Customer *update_customer(int64_t num_key, uint32_t size, uint32_t offset) {
				TPCCKey key(DurableTable::Customer, num_key);
				return executor_ptr_->template update<Customer>(key, size, offset);
			}

			Customer *update_customer(int32_t w_id, int32_t d_id, int32_t c_id, uint32_t size, uint32_t offset) {
				return update_customer(TPCCKeyMaker::make_customer_key(w_id, d_id, c_id), size, offset);
			}

			Order *update_order(int64_t num_key, uint32_t size, uint32_t offset) {
				TPCCKey key(DurableTable::Order, num_key);
				return executor_ptr_->template update<Order>(key, size, offset);
			}

			Order *update_order(int32_t w_id, int32_t d_id, int32_t o_id, uint32_t size, uint32_t offset) {
				return update_order(TPCCKeyMaker::make_order_key(w_id, d_id, o_id), size, offset);
			}

			OrderLine *update_order_line(int64_t num_key, uint32_t size, uint32_t offset) {
				TPCCKey key(DurableTable::OrderLine, num_key);
				return executor_ptr_->template update<OrderLine>(key, size, offset);
			}

			OrderLine *update_order_line(int32_t w_id, int32_t d_id, int32_t o_id, int32_t number, uint32_t size, uint32_t offset) {
				return update_order_line(TPCCKeyMaker::make_order_line_key(w_id, d_id, o_id, number), size, offset);
			}

			NewOrder *update_new_order(int64_t num_key, uint32_t size, uint32_t offset) {
				TPCCKey key(DurableTable::NewOrder, num_key);
				return executor_ptr_->template update<NewOrder>(key, size, offset);
			}

			NewOrder *update_new_order(int32_t w_id, int32_t d_id, int32_t o_id, uint32_t size, uint32_t offset) {
				return update_new_order(TPCCKeyMaker::make_new_order_key(w_id, d_id, o_id), size, offset);
			}

			NewOrderID *update_new_order_id(int64_t num_key, uint32_t size, uint32_t offset) {
				TPCCKey key(DurableTable::NewOrderID, num_key);
				return executor_ptr_->template update<NewOrderID>(key, size, offset);
			}

			NewOrderID *update_new_order_id(int32_t w_id, int32_t d_id, uint32_t size, uint32_t offset) {
				// New order id table is corresponded to District.
				// so we just need to reuse the district key.
				return update_new_order_id(TPCCKeyMaker::make_district_key(w_id, d_id), size, offset);
			}

		private:
			bool delete_new_order(int64_t num_key) {
				return executor_ptr_->remove(
						TPCCKey(DurableTable::NewOrder, num_key)
				);
			}


		};


		template<class Config, class Executor>
			requires (!FineGranularityExecutorConcept<Executor, AbstractKey<typename TPCCConfigManager<Config>::KeyType>> &&
				ExecutorConcept<Executor, AbstractKey<typename TPCCConfigManager<Config>::KeyType>>)
		class TPCCWorkloadEngine<Config, Executor> {
		public:
			using ConfigManager                 = TPCCConfigManager<Config>;

			using KeyType                       = ConfigManager::KeyType;

			using TPCCKey                       = AbstractKey<KeyType>;

			using SecondaryIndexType            = TPCCSecondaryIndex<Config>;

			static constexpr bool COPY_STRING      = ConfigManager::COPY_STRING;

			static constexpr bool FORMAL_OUTPUT    = ConfigManager::FORMAL_OUTPUT;

			static constexpr bool DO_INSERT_REMOVE = ConfigManager::DO_INSERT_REMOVE;

		private:
			Executor *executor_ptr_;

			SecondaryIndexType *second_index_ptr_;

		public:
			explicit TPCCWorkloadEngine(
					Executor *executor_ptr,
					SecondaryIndexType *second_index_ptr):
					executor_ptr_(executor_ptr),
					second_index_ptr_(second_index_ptr) {}

		public:

			int32_t stock_level(int32_t warehouse_id, int32_t district_id, int32_t threshold) {
				/* EXEC SQL SELECT d_next_o_id INTO :o_id FROM district
				WHERE d_w_id=:w_id AND d_id=:d_id; */
				District d;
				bool district_exist = find_district(warehouse_id, district_id, d);
				if (!district_exist) [[unlikely]] { return false; }

				int32_t o_id = d.d_next_o_id;

				/* EXEC SQL SELECT COUNT(DISTINCT (s_i_id)) INTO :stock_count FROM order_line, stock
					WHERE ol_w_id=:w_id AND ol_d_id=:d_id AND ol_o_id<:o_id AND ol_o_id>=:o_id-20
						AND s_w_id=:w_id AND s_i_id=ol_i_id AND s_quantity < :threshold;*/

				// with linear search, and std::vector with binary search using std::lower_bound. The best
				// seemed to be to simply save all the s_i_ids, then sort and eliminate duplicates at the end.
				std::vector<int32_t> s_i_ids;
				// Average size is more like ~30.
				s_i_ids.reserve(300);

				// Iterate over [o_id-20, o_id)
				DEBUG_ASSERT(o_id >= STOCK_LEVEL_ORDERS);
				for (int order_id = o_id - STOCK_LEVEL_ORDERS; order_id < o_id; ++order_id) {
					// HACK: We shouldn't rely on MAX_OL_CNT. See comment above.
					for (int line_number = 1; line_number <= Order::MAX_OL_CNT; ++line_number) {
						OrderLine line;
						bool line_exist = find_order_line(warehouse_id, district_id, order_id, line_number, line);
						if (!line_exist) [[unlikely]] { break; }

						// Check if s_quantity < threshold
						Stock stock;
						bool stock_exist = find_stock(warehouse_id, line.ol_i_id, stock);
						if (!stock_exist) [[unlikely]] { return false; }

						if (stock.s_quantity < threshold) {
							s_i_ids.push_back(line.ol_i_id);
						}
					}
				}

				// Filter out duplicate s_i_id: multiple order lines can have the same item
				if constexpr (FORMAL_OUTPUT) {
					std::sort(s_i_ids.begin(), s_i_ids.end());
					[[maybe_unused]]int num_distinct = 0;
					int32_t last = -1;  // NOTE: This relies on -1 being an invalid s_i_id
					for (int s_i_id : s_i_ids) {
						if (s_i_id != last) {
							last = s_i_id;
							++num_distinct;
						}
					}
				}

				return true;
			}


			bool order_status(int32_t warehouse_id, int32_t district_id, int32_t customer_id,
			                  OrderStatusOutput* output) {
				Customer c;
				bool customer_exist = find_customer(warehouse_id, district_id, customer_id, c);
				if (!customer_exist) [[unlikely]] { return false; }
				return internal_order_status(c, output);
			}

			bool order_status(int32_t warehouse_id, int32_t district_id, const char* c_last,
			                  OrderStatusOutput* output) {
				const CustomerNameIdentify c_iden = find_customer_by_name(warehouse_id, district_id, c_last);

				Customer c;
				bool customer_exist = find_customer(warehouse_id, district_id, c_iden.c_id, c);
				if (!customer_exist) [[unlikely]] { return false; }

				return internal_order_status(c, output);
			}

			bool new_order(int32_t warehouse_id, int32_t district_id, int32_t customer_id,
			               const auto item_iter_begin, const auto item_iter_end,
			               const char* now,
			               bool do_remote, NewOrderOutput* output) {
				// perform the home part
				bool result = new_order_local(warehouse_id, district_id, customer_id,
				                              item_iter_begin, item_iter_end, now, do_remote, output);
				return result;
			}

			bool new_order_local(int32_t warehouse_id, int32_t district_id, int32_t customer_id,
			                     const auto item_iter_begin, const auto item_iter_end,
			                     const char* now,
			                     bool do_remote, NewOrderOutput* output) {
				{
					Customer c;
					bool customer_exist = find_customer(warehouse_id, district_id, customer_id, c);
					if (!customer_exist) [[unlikely]] { return false; }

					if constexpr (FORMAL_OUTPUT) {
						memcpy(output->c_last, c.c_last, sizeof(output->c_last));
						memcpy(output->c_credit, c.c_credit, sizeof(output->c_credit));
						output->c_discount = c.c_discount;
					}
				}

				// We will not abort: update the status and the database state, allocate an undo buffer
				if constexpr (COPY_STRING) {
					output->status[0] = '\0';
				}

				int32_t order_id;

				{
					District d;
					bool district_exist = find_district(warehouse_id, district_id, d);
					if (!district_exist) [[unlikely]] { return false; }

					if constexpr (FORMAL_OUTPUT) {
						output->d_tax = d.d_tax;
						output->o_id  = d.d_next_o_id;
					}

					order_id = d.d_next_o_id;
					// Modify the order id to assign it
					d.d_next_o_id += 1;

					bool update_success = update_district(warehouse_id, district_id, d);
					if (!update_success) { return false; }
				}

				{
					Warehouse w;
					bool warehouse_exist = find_warehouse(warehouse_id, w);
					if (!warehouse_exist) [[unlikely]] { return false; }

					if constexpr (FORMAL_OUTPUT) {
						output->w_tax = w.w_tax;
					}
				}

				{
					Order order {
							.o_id         = order_id,
							.o_c_id       = customer_id,
							.o_d_id       = district_id,
							.o_w_id       = warehouse_id,
							.o_carrier_id = Order::NULL_CARRIER_ID,
							.o_ol_cnt     = static_cast<int>(item_iter_end - item_iter_begin),
							.o_all_local  = do_remote ? 0 : 1
					};

					if constexpr (FORMAL_OUTPUT) {
						memcpy(order.o_entry_d, now, DATETIME_SIZE);
					}

					if constexpr (DO_INSERT_REMOVE) {
						if (!insert_order(order)) [[unlikely]] { return false; }
					}
				}

				{
					NewOrder new_order = {
							.no_w_id = warehouse_id,
							.no_d_id = district_id,
							.no_o_id = order_id
					};
					if constexpr (DO_INSERT_REMOVE) {
						if (!insert_new_order(new_order)) [[unlikely]] { return false; }
					}
				}

				// CHEAT: Validate all items to see if we will need to abort
				std::array<Item, Order::MAX_OL_CNT> item_tuples;
				if (!find_and_validate_items(item_iter_begin, item_iter_end, item_tuples.begin())) [[unlikely]] {
					if constexpr (FORMAL_OUTPUT) {
						strcpy(output->status, NewOrderOutput::INVALID_ITEM_STATUS);
					}
					return false;
				}

				OrderLine line {
						.ol_o_id = order_id,
						.ol_d_id = district_id,
						.ol_w_id = warehouse_id
				};
				if constexpr (COPY_STRING) {
					memset(line.ol_delivery_d, 0, DATETIME_SIZE);
				}

				if constexpr (FORMAL_OUTPUT) {
					output->items.resize(item_iter_end - item_iter_begin);
					output->total = 0;
				}

				int ol_number_count = 0;
				for (auto item_iter = item_iter_begin; item_iter != item_iter_end; ++item_iter) {
					const auto &item             = *item_iter;
					auto &item_tuple      = item_tuples[ol_number_count];

					// Vertical Partitioning HACK: We read s_dist_xx from our local replica, assuming that
					// these columns are replicated everywhere.
					// replicas. Try the "two round" version in the future.
					Stock stock;
					bool stock_exist = find_stock(item.ol_supply_w_id, item.i_id, stock);
					if (!stock_exist) [[unlikely]] { return false; }

					// update stock

					if (stock.s_quantity >= item.ol_quantity + 10) {
						stock.s_quantity -= item.ol_quantity;
					}
					else {
						stock.s_quantity = stock.s_quantity - item.ol_quantity + 91;
					}

					auto &output_item = output->items[ol_number_count];
					if constexpr (FORMAL_OUTPUT) {
						output_item.s_quantity = stock.s_quantity;
					}

					stock.s_ytd       += item.ol_quantity;
					stock.s_order_cnt += 1;
					// new_order_local calls new_order_remote, so this is needed
					if (item.ol_supply_w_id != warehouse_id) {
						// remote order
						stock.s_remote_cnt += 1;
					}

					bool stock_update_success = update_stock(item.ol_supply_w_id, item.i_id, stock);
					if (!stock_update_success) { return false; }

					// Make order line

					// Since we *need* to replicate s_dist_xx columns, might as well replicate s_data
					// Makes it 290 bytes per tuple, or ~28 MB per warehouse.

					if constexpr (FORMAL_OUTPUT) {
						bool stock_is_original = (strstr(stock.s_data, "ORIGINAL") != nullptr);
						if (stock_is_original && strstr(item_tuples[item_iter - item_iter_begin].i_data, "ORIGINAL") != nullptr) {
							output_item.brand_generic = NewOrderOutput::ItemInfo::BRAND;
						}
						else {
							output_item.brand_generic = NewOrderOutput::ItemInfo::GENERIC;
						}
					}

					if constexpr (FORMAL_OUTPUT) {
						static_assert(sizeof(output_item.i_name) == sizeof(item_tuple.i_name));
						memcpy(output_item.i_name, item_tuple.i_name, sizeof(output_item.i_name));
						output_item.i_price    = item_tuple.i_price;
						output_item.ol_amount  = static_cast<float>(item.ol_quantity) * item_tuple.i_price;
						output->total         += output_item.ol_amount;
					}

					line.ol_number      = ++ol_number_count;
					line.ol_i_id        = item.i_id;
					line.ol_supply_w_id = item.ol_supply_w_id;
					line.ol_quantity    = item.ol_quantity;
					line.ol_amount      = static_cast<float>(item.ol_quantity) * item_tuple.i_price;

					if constexpr (COPY_STRING) {
						memcpy(line.ol_dist_info, stock.s_dist[district_id], sizeof(line.ol_dist_info));
					}

					if constexpr (DO_INSERT_REMOVE) {
						if (!insert_order_line(line)) [[unlikely]] { return false; }
					}
				}

				return true;
			}

			bool payment(int32_t warehouse_id, int32_t district_id, int32_t c_warehouse_id,
			             int32_t c_district_id, int32_t customer_id, float h_amount, const char* now,
			             PaymentOutput* output) {
				Customer customer;
				bool customer_exist = find_customer(c_warehouse_id, c_district_id, customer_id, customer);
				if (!customer_exist) [[unlikely]] { return false; }

				if (!payment_local(
						warehouse_id, district_id, c_warehouse_id, c_district_id, customer_id, h_amount,
						now, output
				)) [[unlikely]] { return false; }
				internal_payment_remote(warehouse_id, district_id, customer, h_amount, output);

				bool customer_update_success = update_customer(c_warehouse_id, c_district_id, customer_id, customer);
				if (!customer_update_success) { return false; }

				return true;
			}

			bool payment(int32_t warehouse_id, int32_t district_id, int32_t c_warehouse_id,
			             int32_t c_district_id, const char* c_last, float h_amount, const char* now,
			             PaymentOutput* output) {
				const CustomerNameIdentify temp_customer = find_customer_by_name(c_warehouse_id, c_district_id, c_last);

				Customer customer;
				bool customer_exist = find_customer(temp_customer.c_w_id, temp_customer.c_d_id, temp_customer.c_id, customer);
				if (!customer_exist) [[unlikely]] { return false; }

				if (!payment_local(
						warehouse_id, district_id, c_warehouse_id, c_district_id, customer.c_id, h_amount,
						now, output
				)) [[unlikely]] { return false; }
				internal_payment_remote(warehouse_id, district_id, customer, h_amount, output);

				bool customer_update_success = update_customer(temp_customer.c_w_id, temp_customer.c_d_id, temp_customer.c_id, customer);
				if (!customer_update_success) { return false; }

				return true;
			}

			bool payment_local(int32_t warehouse_id, int32_t district_id, int32_t c_warehouse_id,
			                   int32_t c_district_id, int32_t customer_id, float h_amount, const char* now,
			                   PaymentOutput* output) {
				Warehouse w;
				bool warehouse_exist = find_warehouse(warehouse_id, w);
				if (!warehouse_exist) [[unlikely]] { return false; }

				w.w_ytd += h_amount;

				bool warehouse_update_success = update_warehouse(warehouse_id, w);
				if (!warehouse_update_success) { return false; }

				if constexpr (FORMAL_OUTPUT) {
					Address::copy(
							output->w_street_1, output->w_street_2, output->w_city,
							output->w_state, output->w_zip,
							w.w_street_1, w.w_street_2, w.w_city,
							w.w_state, w.w_zip);
				}

				District d;
				bool district_exist = find_district(warehouse_id, district_id, d);
				if (!district_exist) [[unlikely]] { return false; }

				d.d_ytd += h_amount;

				bool district_update_success = update_district(warehouse_id, district_id, d);
				if (!district_update_success) { return false; }

				if constexpr (FORMAL_OUTPUT) {
					Address::copy(
							output->d_street_1, output->d_street_2, output->d_city,
							output->d_state, output->d_zip,
							d.d_street_1, d.d_street_2, d.d_city,d.d_state, d.d_zip);
				}

				// Insert the line into the history table
				History h {
						.h_c_id   = customer_id,
						.h_c_d_id = c_district_id,
						.h_c_w_id = c_warehouse_id,
						.h_d_id   = district_id,
						.h_w_id   = warehouse_id,
						.h_amount = h_amount
				};
				if constexpr (COPY_STRING) {
					memcpy(h.h_date, now, DATETIME_SIZE);
					strcpy(h.h_data, w.w_name);
					strcat(h.h_data, "    ");
					strcat(h.h_data, d.d_name);
				}

				if constexpr (DO_INSERT_REMOVE) {
					if (!insert_history(h)) [[unlikely]] { return false; }
				}

				// Zero all the customer fields: avoid uninitialized data for serialization
				if constexpr (FORMAL_OUTPUT) {
					output->c_credit_lim  = 0;
					output->c_discount    = 0;
					output->c_balance     = 0;
					output->c_first[0]    = '\0';
					output->c_middle[0]   = '\0';
					output->c_last[0]     = '\0';
					output->c_phone[0]    = '\0';
					output->c_since[0]    = '\0';
					output->c_credit[0]   = '\0';
					output->c_data[0]     = '\0';
					output->c_street_1[0] = '\0';
					output->c_street_2[0] = '\0';
					output->c_city[0]     = '\0';
					output->c_state[0]    = '\0';
					output->c_zip[0]      = '\0';
				}

				return true;
			}

			bool delivery(int32_t warehouse_id, int32_t carrier_id, const char* now,
			              std::vector<DeliveryOrderInfo>* orders) {
				orders->clear();
				for (int32_t d_id = 1; d_id <= District::NUM_PER_WAREHOUSE; ++d_id) {
					NewOrderID new_order_id_tuple;
					bool new_order_id_exist = find_new_order_id(warehouse_id, d_id, new_order_id_tuple);
					if (!new_order_id_exist) [[unlikely]] { return false; }

					int32_t new_order_key = new_order_id_tuple.next_no_o_id;
					int32_t o_id = new_order_key;

					if constexpr (DO_INSERT_REMOVE) {
						++new_order_id_tuple.next_no_o_id;
					}

					bool new_order_id_update_success = update_new_order_id(warehouse_id, d_id, new_order_id_tuple);
					if (!new_order_id_update_success) { return false; }

					if constexpr (DO_INSERT_REMOVE) {
						if (!delete_new_order(TPCCKeyMaker::make_new_order_key(warehouse_id, d_id, o_id))) [[unlikely]] { return false; }
					}

					orders->emplace_back(
							DeliveryOrderInfo {
									.d_id = d_id,
									.o_id = o_id
							}
					);

					Order o;
					bool order_exist = find_order(warehouse_id, d_id, o_id, o);
					if (!order_exist) { return false; }

					if constexpr (DO_INSERT_REMOVE) {
						if (o.o_carrier_id != Order::NULL_CARRIER_ID) { return false; }
						o.o_carrier_id = carrier_id;
					}

					bool order_update_success = update_order(warehouse_id, d_id, o_id, o);
					if (!order_update_success) { return false; }

					float total = 0;

					if constexpr (DO_INSERT_REMOVE) {
						for (int32_t i = 1; i <= o.o_ol_cnt; ++i) {
							OrderLine line;
							bool order_line_exist = find_order_line(warehouse_id, d_id, o_id, i, line);
							if (!order_line_exist) [[unlikely]] { return false; }

							if (line.ol_delivery_d[0] != '\0') [[unlikely]] { return false; }

							if constexpr (COPY_STRING) {
								memcpy(line.ol_delivery_d, now, DATETIME_SIZE);
							}

							bool order_line_update_success = update_order_line(warehouse_id, d_id, o_id, i, line);
							if (!order_line_update_success) [[unlikely]] { return false; }

							total += line.ol_amount;
						}
					}

					{
						Customer c;
						bool customer_exist = find_customer(warehouse_id, d_id, o.o_c_id, c);
						if (!customer_exist) [[unlikely]] { return false; }

						c.c_balance += total;
						c.c_delivery_cnt += 1;

						bool customer_update_success = update_customer(warehouse_id, d_id, o.o_c_id, c);
						if (!customer_update_success) { return false; }
					}
				}

				return true;
			}

		private:
			static const int STOCK_LEVEL_ORDERS = 20;

			// Loads each item from the items table. Returns true if they are all found.
			bool find_and_validate_items(const auto item_iter_begin, const auto item_iter_end,
			                             auto item_tuple_iter) {
				auto output_iter = item_tuple_iter;

				Item item;
				// CHEAT: Validate all items to see if we will need to abort
				for (auto iter = item_iter_begin; iter != item_iter_end; ++iter, ++output_iter) {
					bool item_exist = find_item(iter->i_id, item);
					if (!item_exist) [[unlikely]] { return false; }
					std::memcpy(std::addressof(*output_iter), &item, sizeof(Item));
				}
				return true;
			}

			// Implements order status transaction after the customer tuple has been located.
			bool internal_order_status(const Customer &customer, OrderStatusOutput* output) {
				if constexpr (FORMAL_OUTPUT) {
					output->c_id = customer.c_id;
					// retrieve from customer: balance, first, middle, last
					output->c_balance = customer.c_balance;
					strcpy(output->c_first, customer.c_first);
					strcpy(output->c_middle, customer.c_middle);
					strcpy(output->c_last, customer.c_last);
				}

				// Find the row in the order table with largest o_id
				Order order;
				bool order_exist = find_last_order_by_customer(customer.c_w_id, customer.c_d_id, customer.c_id, order);
				if (!order_exist) [[unlikely]] { return false;}

				if constexpr (FORMAL_OUTPUT) {
					output->o_id         = order.o_id;
					output->o_carrier_id = order.o_carrier_id;
					memcpy(output->o_entry_d, order.o_entry_d, DATETIME_SIZE);
					output->lines.resize(order.o_ol_cnt);
				}

				for (int32_t line_number = 1; line_number <= order.o_ol_cnt; ++line_number) {
					OrderLine line;
					bool line_update_success = find_order_line(order.o_w_id, order.o_d_id, order.o_id, line_number, line);
					if (!line_update_success) [[unlikely]] { return false; }

					if constexpr (FORMAL_OUTPUT) {
						output->lines[line_number - 1] = {
								.ol_i_id        = line.ol_i_id,
								.ol_supply_w_id = line.ol_supply_w_id,
								.ol_quantity    = line.ol_quantity,
								.ol_amount      = line.ol_amount
						};
						memcpy(output->lines[line_number - 1].ol_delivery_d, line.ol_delivery_d, DATETIME_SIZE);
					}
				}

				return true;
			}

			// Implements payment transaction after the customer tuple has been located.
			void internal_payment_remote(int32_t warehouse_id, int32_t district_id, Customer &c,
			                             float h_amount, PaymentOutput* output) {
				c.c_balance     -= h_amount;
				c.c_ytd_payment += h_amount;
				c.c_payment_cnt += 1;

				if constexpr (COPY_STRING) {
					if (strcmp(c.c_credit, Customer::BAD_CREDIT) == 0) {
						// Bad credit: insert history into c_data
						static constexpr int HISTORY_SIZE = Customer::MAX_DATA + 1;

						char history[HISTORY_SIZE];
						int characters = snprintf(history, HISTORY_SIZE, "(%d, %d, %d, %d, %d, %.2f)\n",
						                          c.c_id, c.c_d_id, c.c_w_id, district_id, warehouse_id, h_amount);
						DEBUG_ASSERT(characters < HISTORY_SIZE);

						// Perform the insert with a move and copy
						int current_keep = static_cast<int>(strlen(c.c_data));
						if (current_keep + characters > Customer::MAX_DATA) {
							current_keep = Customer::MAX_DATA - characters;
						}
						DEBUG_ASSERT(current_keep + characters <= Customer::MAX_DATA);
						memmove(c.c_data+characters, c.c_data, current_keep);
						memcpy(c.c_data, history, characters);
						c.c_data[characters + current_keep] = '\0';
						DEBUG_ASSERT(strlen(c.c_data) == static_cast<size_t>(characters) + current_keep);
					}
				}

				if constexpr (FORMAL_OUTPUT) {
					output->c_credit_lim = c.c_credit_lim;
					output->c_discount   = c.c_discount;
					output->c_balance    = c.c_balance;
					memcpy(output->c_first, c.c_first, sizeof(output->c_first));
					memcpy(output->c_middle, c.c_middle, sizeof(output->c_middle));
					memcpy(output->c_last, c.c_last, sizeof(output->c_last));
					memcpy(output->c_phone, c.c_phone, sizeof(output->c_phone));
					memcpy(output->c_since, c.c_since, sizeof(output->c_since));
					memcpy(output->c_credit, c.c_credit, sizeof(output->c_credit));
					memcpy(output->c_data, c.c_data, sizeof(output->c_data));

					Address::copy(
							output->c_street_1, output->c_street_2, output->c_city,
							output->c_state, output->c_zip,
							c.c_street_1, c.c_street_2, c.c_city,
							c.c_state, c.c_zip);
				}
			}

		private:
			bool find_item(int32_t id, Item &item) {
				TPCCKey key(DurableTable::Item, id);
				return executor_ptr_->read(key, &item, sizeof(Item), 0);
			}

			bool find_warehouse(int32_t w_id, Warehouse &warehouse) {
				TPCCKey key(DurableTable::Warehouse, w_id);
				return executor_ptr_->read(key, &warehouse, sizeof(Warehouse), 0);
			}

			bool find_stock(int64_t num_key, Stock &stock) {
				TPCCKey key(DurableTable::Stock, num_key);
				return executor_ptr_->read(key, &stock, sizeof(Stock), 0);
			}

			bool find_stock(int32_t w_id, int32_t i_id, Stock &stock) {
				return find_stock(TPCCKeyMaker::make_stock_key(w_id, i_id), stock);
			}

			bool find_district(int64_t num_key, District &district) {
				TPCCKey key(DurableTable::District, num_key);
				return executor_ptr_->read(key, &district, sizeof(District), 0);
			}

			bool find_district(int32_t w_id, int32_t d_id, District &district) {
				return find_district(TPCCKeyMaker::make_district_key(w_id, d_id), district);
			}

			bool find_customer(int64_t num_key, Customer &customer) {
				TPCCKey key(DurableTable::Customer, num_key);
				return executor_ptr_->read(key, &customer, sizeof(Customer), 0);
			}

			bool find_customer(int32_t w_id, int32_t d_id, int32_t c_id, Customer &customer) {
				return find_customer(TPCCKeyMaker::make_customer_key(w_id, d_id, c_id), customer);
			}

			bool find_order(int64_t num_key, Order &order) {
				TPCCKey key(DurableTable::Order, num_key);
				return executor_ptr_->read(key, &order, sizeof(Order), 0);
			}

			bool find_order(int32_t w_id, int32_t d_id, int32_t o_id, Order &order) {
				return find_order(TPCCKeyMaker::make_order_key(w_id, d_id, o_id), order);
			}

			bool find_order_line(int64_t num_key, OrderLine &order_line) {
				TPCCKey key(DurableTable::OrderLine, num_key);
				return executor_ptr_->read(key, &order_line, sizeof(OrderLine), 0);
			}

			bool find_order_line(int32_t w_id, int32_t d_id, int32_t o_id, int32_t number, OrderLine &order_line) {
				return find_order_line(TPCCKeyMaker::make_order_line_key(w_id, d_id, o_id, number), order_line);
			}

			bool find_new_order(int64_t num_key, NewOrder &new_order) {
				TPCCKey key(DurableTable::NewOrder, num_key);
				return executor_ptr_->read(key, &new_order, sizeof(NewOrder), 0);
			}

			bool find_new_order(int32_t w_id, int32_t d_id, int32_t o_id, NewOrder &new_order) {
				return find_new_order(TPCCKeyMaker::make_new_order_key(w_id, d_id, o_id), new_order);
			}

			bool find_new_order_id(int64_t num_key, NewOrderID &new_order_id) {
				TPCCKey key(DurableTable::NewOrderID, num_key);
				return executor_ptr_->read(key, &new_order_id, sizeof(NewOrderID), 0);
			}

			bool find_new_order_id(int32_t w_id, int32_t d_id, NewOrderID &new_order_id) {
				// New order id table is corresponded to District.
				// so we just need to reuse the district key.
				return find_new_order_id(TPCCKeyMaker::make_district_key(w_id, d_id), new_order_id);
			}

			// Finds all customers that match (w_id, d_id, *, c_last), taking the n/2th one (rounded up).
			CustomerNameIdentify find_customer_by_name(int32_t w_id, int32_t d_id, const char* c_last) {
				// select (w_id, d_id, *, c_last) order by c_first
				Customer c {
						.c_d_id = d_id,
						.c_w_id = w_id,
						.c_first = "\0"
				};
				if constexpr (COPY_STRING) {
					strcpy(c.c_last, c_last);
				}


				auto [found, c_iden] = second_index_ptr_->get_customer_indentify_by_name(c);

				if constexpr (COPY_STRING) {
					DEBUG_ASSERT(
							c_iden->c_w_id == w_id && c_iden->c_d_id == d_id &&
							strcmp(c_iden->c_last, c_last) == 0
					);
				}
				return *c_iden;
			}

			bool find_last_order_by_customer(int32_t w_id, int32_t d_id, int32_t c_id, Order &order) {
				// Increment the (w_id, d_id, c_id) tuple
				int64_t order_key = second_index_ptr_->get_order_by_customer(w_id, d_id, c_id);
				return find_order(order_key, order);
			}

		public:
			// Copies item into the item table.
			bool insert_item(const Item& item) {
				return executor_ptr_->insert(
						TPCCKey(DurableTable::Item, item.i_id),
						&item, sizeof(Item)
				);
			}

			bool insert_stock(const Stock& stock) {
				return executor_ptr_->insert(
						TPCCKey(DurableTable::Stock, TPCCKeyMaker::make_stock_key(stock.s_w_id, stock.s_i_id)),
						&stock, sizeof(Stock)
				);
			}

			bool insert_warehouse(const Warehouse& warehouse) {
				return executor_ptr_->insert(
						TPCCKey(DurableTable::Warehouse, warehouse.w_id),
						&warehouse, sizeof(Warehouse)
				);
			}

			bool insert_district(const District& district) {
				return executor_ptr_->insert(
						TPCCKey(DurableTable::District, TPCCKeyMaker::make_district_key(district.d_w_id, district.d_id)),
						&district, sizeof(District)
				);
			}

			bool insert_customer(const Customer& customer) {
				second_index_ptr_->insert_customer_by_name(customer);

				return executor_ptr_->insert(
						TPCCKey(DurableTable::Customer, TPCCKeyMaker::make_customer_key(customer.c_w_id, customer.c_d_id, customer.c_id)),
						&customer, sizeof(Customer)
				);
			}

			// Stores order in the database. Returns a pointer to the database's tuple.
			bool insert_order(const Order& order) {
				int64_t order_key = TPCCKeyMaker::make_order_key(order.o_w_id, order.o_d_id, order.o_id);

				bool success = executor_ptr_->insert(
						TPCCKey(DurableTable::Order, order_key),
						&order, sizeof(Order)
				);
				if (success) {
					second_index_ptr_->insert_order_by_customer(order.o_w_id, order.o_d_id, order.o_c_id, order_key);
				}
				return success;
			}

			// Creates a new order in the database. Returns a pointer to the database's tuple.
			bool insert_new_order(const NewOrder &new_order) {
				auto key = TPCCKeyMaker::make_new_order_key(new_order.no_w_id, new_order.no_d_id, new_order.no_o_id);

				return executor_ptr_->insert(
						TPCCKey(DurableTable::NewOrder, key),
						&new_order, sizeof(NewOrder)
				);
			}

			// Creates a new order in the database. Returns a pointer to the database's tuple.
			bool insert_new_order_id(const NewOrderID &new_order_id) {
				auto key = TPCCKeyMaker::make_district_key(new_order_id.w_id, new_order_id.d_id);

				return executor_ptr_->insert(
						TPCCKey(DurableTable::NewOrderID, key),
						&new_order_id, sizeof(NewOrderID)
				);
			}

			// Stores orderline in the database. Returns a pointer to the database's tuple.
			bool insert_order_line(const OrderLine& orderline) {
				return executor_ptr_->insert(
						TPCCKey(DurableTable::OrderLine, TPCCKeyMaker::make_order_line_key(orderline.ol_w_id, orderline.ol_d_id, orderline.ol_o_id, orderline.ol_number)),
						&orderline, sizeof(OrderLine)
				);
			}

			bool insert_history(const History& history) {
				static std::atomic<int64_t> history_key{0};
				return executor_ptr_->insert(
						TPCCKey(DurableTable::History, history_key++),
						&history, sizeof(History)
				);
			}

		private:
			bool update_item(int32_t id, const Item &item) {
				TPCCKey key(DurableTable::Item, id);
				return executor_ptr_->update(key, &item, sizeof(Item), 0);
			}

			bool update_warehouse(int32_t w_id, const Warehouse &warehouse) {
				TPCCKey key(DurableTable::Warehouse, w_id);
				return executor_ptr_->update(key, &warehouse, sizeof(Warehouse), 0);
			}

			bool update_stock(int64_t num_key, const Stock &stock) {
				TPCCKey key(DurableTable::Stock, num_key);
				return executor_ptr_->update(key, &stock, sizeof(Stock), 0);
			}

			bool update_stock(int32_t w_id, int32_t i_id, const Stock &stock) {
				return update_stock(TPCCKeyMaker::make_stock_key(w_id, i_id), stock);
			}

			bool update_district(int64_t num_key, const District &district) {
				TPCCKey key(DurableTable::District, num_key);
				return executor_ptr_->update(key, &district, sizeof(District), 0);
			}

			bool update_district(int32_t w_id, int32_t d_id, const District &district) {
				return update_district(TPCCKeyMaker::make_district_key(w_id, d_id), district);
			}

			bool update_customer(int64_t num_key, const Customer &customer) {
				TPCCKey key(DurableTable::Customer, num_key);
				return executor_ptr_->update(key, &customer, sizeof(Customer), 0);
			}

			bool update_customer(int32_t w_id, int32_t d_id, int32_t c_id, const Customer &customer) {
				return update_customer(TPCCKeyMaker::make_customer_key(w_id, d_id, c_id), customer);
			}

			bool update_order(int64_t num_key, const Order &order) {
				TPCCKey key(DurableTable::Order, num_key);
				return executor_ptr_->update(key, &order, sizeof(Order), 0);
			}

			bool update_order(int32_t w_id, int32_t d_id, int32_t o_id, const Order &order) {
				return update_order(TPCCKeyMaker::make_order_key(w_id, d_id, o_id), order);
			}

			bool update_order_line(int64_t num_key, const OrderLine &order_line) {
				TPCCKey key(DurableTable::OrderLine, num_key);
				return executor_ptr_->update(key, &order_line, sizeof(OrderLine), 0);
			}

			bool update_order_line(int32_t w_id, int32_t d_id, int32_t o_id, int32_t number, const OrderLine &order_line) {
				return update_order_line(TPCCKeyMaker::make_order_line_key(w_id, d_id, o_id, number), order_line);
			}

			bool update_new_order(int64_t num_key, NewOrder &new_order) {
				TPCCKey key(DurableTable::NewOrder, num_key);
				return executor_ptr_->update(key, &new_order, sizeof(NewOrder), 0);
			}

			bool update_new_order(int32_t w_id, int32_t d_id, int32_t o_id, NewOrder &new_order) {
				return update_new_order(TPCCKeyMaker::make_new_order_key(w_id, d_id, o_id), new_order);
			}

			bool update_new_order_id(int64_t num_key, NewOrderID &new_order_id) {
				TPCCKey key(DurableTable::NewOrderID, num_key);
				return executor_ptr_->update(key, &new_order_id, sizeof(NewOrderID), 0);
			}

			bool update_new_order_id(int32_t w_id, int32_t d_id, NewOrderID &new_order_id) {
				// New order id table is corresponded to District.
				// so we just need to reuse the district key.
				return update_new_order_id(TPCCKeyMaker::make_district_key(w_id, d_id), new_order_id);
			}

		private:
			bool delete_new_order(int64_t num_key) {
				return executor_ptr_->remove(
						TPCCKey(DurableTable::NewOrder, num_key)
				);
			}


		};

	}
}
