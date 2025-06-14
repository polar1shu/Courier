/*
 * @author: BL-GS 
 * @date:   2023/2/28
 */

#pragma once

#include <cstdint>
#include <cstring>
#include <vector>

namespace workload {

	struct Address {
		// duplication, but would also change the field names
		static constexpr int MIN_STREET = 10;
		static constexpr int MAX_STREET = 20;
		static constexpr int MIN_CITY   = 10;
		static constexpr int MAX_CITY   = 20;
		static constexpr int STATE      = 2;
		static constexpr int ZIP        = 9;

		static void copy(char* street1, char* street2, char* city, char* state, char* zip,
		                 const char* src_street1, const char* src_street2, const char* src_city,
		                 const char* src_state, const char* src_zip) {
			memcpy(street1, src_street1, MAX_STREET+1);
			memcpy(street2, src_street2, MAX_STREET+1);
			memcpy(city, src_city, MAX_CITY+1);
			memcpy(state, src_state, STATE+1);
			memcpy(zip, src_zip, ZIP+1);
		}
	};

	struct alignas(16) Item {
		static constexpr int MIN_IM      = 1;
		static constexpr int MAX_IM      = 10000;
		static constexpr float MIN_PRICE = 1.00;
		static constexpr float MAX_PRICE = 100.00;
		static constexpr int MIN_NAME    = 14;
		static constexpr int MAX_NAME    = 24;
		static constexpr int MIN_DATA    = 26;
		static constexpr int MAX_DATA    = 50;
		static constexpr int NUM_ITEMS   = 100000;

		int32_t i_id;
		int32_t i_im_id;
		float i_price;
		char i_name[MAX_NAME+1];
		char i_data[MAX_DATA+1];
	};

	struct alignas(16) Warehouse {
		static constexpr float MIN_TAX     = 0;
		static constexpr float MAX_TAX     = 0.2000f;
		static constexpr float INITIAL_YTD = 300000.00f;
		static constexpr int MIN_NAME      = 6;
		static constexpr int MAX_NAME      = 10;
		// TPC-C 1.3.1 (page 11) requires 2*W. This permits testing up to 50 warehouses. This is an
		// arbitrary limit created to pack ids into integers.
		static constexpr int MAX_WAREHOUSE_ID = 512;

		int32_t w_id;
		float w_tax;
		float w_ytd;
		char w_name[MAX_NAME+1];
		char w_street_1[Address::MAX_STREET+1];
		char w_street_2[Address::MAX_STREET+1];
		char w_city[Address::MAX_CITY+1];
		char w_state[Address::STATE+1];
		char w_zip[Address::ZIP+1];
	};

	struct alignas(16) District {
		static constexpr float MIN_TAX         = 0;
		static constexpr float MAX_TAX         = 0.2000f;
		static constexpr float INITIAL_YTD     = 30000.00; // different from Warehouse
		static constexpr int INITIAL_NEXT_O_ID = 3001;
		static constexpr int MIN_NAME          = 6;
		static constexpr int MAX_NAME          = 10;
		static constexpr int NUM_PER_WAREHOUSE = 10;

		int32_t d_id;
		int32_t d_w_id;
		float d_tax;
		float d_ytd;
		int32_t d_next_o_id;
		char d_name[MAX_NAME+1];
		char d_street_1[Address::MAX_STREET+1];
		char d_street_2[Address::MAX_STREET+1];
		char d_city[Address::MAX_CITY+1];
		char d_state[Address::STATE+1];
		char d_zip[Address::ZIP+1];
	};

	// Note: this is not included in TPCC standard, which is set to record the id of next new order in district;
	// Divide it from District to decrease conflicts.
	struct alignas(16) NewOrderID {
		int32_t w_id;
		int32_t d_id;
		int32_t next_no_o_id;
	};

	struct alignas(16) Stock {
		static constexpr int MIN_QUANTITY            = 10;
		static constexpr int MAX_QUANTITY            = 100;
		static constexpr int DIST                    = 24;
		static constexpr int MIN_DATA                = 26;
		static constexpr int MAX_DATA                = 50;
		static constexpr int NUM_STOCK_PER_WAREHOUSE = 100000;

		int32_t s_i_id;
		int32_t s_w_id;
		int32_t s_quantity;
		int32_t s_ytd;
		int32_t s_order_cnt;
		int32_t s_remote_cnt;
		char s_dist[District::NUM_PER_WAREHOUSE][DIST+1];
		char s_data[MAX_DATA+1];
	};

// YYYY-MM-DD HH:MM:SS This is supposed to be a date/time field from Jan 1st 1900 -
// Dec 31st 2100 with a resolution of 1 second. See TPC-C 1.3.1.
	static constexpr int DATETIME_SIZE = 24;

	struct alignas(16) Customer {
		static constexpr float INITIAL_CREDIT_LIM  = 50000.00;
		static constexpr float MIN_DISCOUNT        = 0.0000;
		static constexpr float MAX_DISCOUNT        = 0.5000;
		static constexpr float INITIAL_BALANCE     = -10.00;
		static constexpr float INITIAL_YTD_PAYMENT = 10.00;
		static constexpr int INITIAL_PAYMENT_CNT   = 1;
		static constexpr int INITIAL_DELIVERY_CNT  = 0;
		static constexpr int MIN_FIRST             = 6;
		static constexpr int MAX_FIRST             = 10;
		static constexpr int MIDDLE                = 2;
		static constexpr int MAX_LAST              = 16;
		static constexpr int PHONE                 = 16;
		static constexpr int CREDIT                = 2;
		static constexpr int MIN_DATA              = 300;
		static constexpr int MAX_DATA              = 500;
		static constexpr int NUM_PER_DISTRICT      = 3000;
		static constexpr const char GOOD_CREDIT[] = "0";
		static constexpr const char BAD_CREDIT[]  = "1";

		int32_t c_id;
		int32_t c_d_id;
		int32_t c_w_id;
		float c_credit_lim;
		float c_discount;
		float c_balance;
		float c_ytd_payment;
		int32_t c_payment_cnt;
		int32_t c_delivery_cnt;
		char c_first[MAX_FIRST+1];
		char c_middle[MIDDLE+1];
		char c_last[MAX_LAST+1];
		char c_street_1[Address::MAX_STREET+1];
		char c_street_2[Address::MAX_STREET+1];
		char c_city[Address::MAX_CITY+1];
		char c_state[Address::STATE+1];
		char c_zip[Address::ZIP+1];
		char c_phone[PHONE+1];
		char c_since[DATETIME_SIZE];
		char c_credit[CREDIT+1];
		char c_data[MAX_DATA+1];
	};

	struct CustomerNameIdentify {
		static constexpr int MAX_FIRST = Customer::MAX_FIRST;
		static constexpr int MAX_LAST  = Customer::MAX_LAST;

		int32_t c_id;
		int32_t c_d_id;
		int32_t c_w_id;
		char c_first[MAX_FIRST + 1];
		char c_last[MAX_LAST + 1];

		CustomerNameIdentify(const Customer &c): c_id(c.c_id), c_d_id(c.c_d_id), c_w_id(c.c_w_id) {
			std::memcpy(c_first, c.c_first, CustomerNameIdentify::MAX_FIRST + 1);
			std::memcpy(c_last, c.c_last, CustomerNameIdentify::MAX_LAST + 1);
		}
	};

	struct alignas(16) Order {
		static constexpr int MIN_CARRIER_ID              = 1;
		static constexpr int MAX_CARRIER_ID              = 10;
		// HACK: This is not strictly correct, but it works
		static constexpr int NULL_CARRIER_ID             = 0;
		// Less than this value, carrier != null, >= -> carrier == null
		static constexpr int NULL_CARRIER_LOWER_BOUND    = 2101;
		static constexpr int MIN_OL_CNT                  = 5;
		static constexpr int MAX_OL_CNT                  = 15;
		static constexpr int INITIAL_ALL_LOCAL           = 1;
		static constexpr int INITIAL_ORDERS_PER_DISTRICT = 3000;
		// See TPC-C 1.3.1 (page 15)
		static constexpr int MAX_ORDER_ID                = 10000000;

		int32_t o_id;
		int32_t o_c_id;
		int32_t o_d_id;
		int32_t o_w_id;
		int32_t o_carrier_id;
		int32_t o_ol_cnt;
		int32_t o_all_local;
		char o_entry_d[DATETIME_SIZE];
	};

	struct alignas(16) OrderLine {
		static constexpr int MIN_I_ID                  = 1;
		static constexpr int MAX_I_ID                  = 100000;   // Item::NUM_ITEMS
		static constexpr int INITIAL_QUANTITY          = 5;
		static constexpr float MIN_AMOUNT              = 0.01f;
		static constexpr float MAX_AMOUNT              = 9999.99f;
		// new order has 10/1000 probability of selecting a remote warehouse for ol_supply_w_id
		static constexpr int REMOTE_PROBABILITY_MILLIS = 10;

		int32_t ol_o_id;
		int32_t ol_d_id;
		int32_t ol_w_id;
		int32_t ol_number;
		int32_t ol_i_id;
		int32_t ol_supply_w_id;
		int32_t ol_quantity;
		float ol_amount;
		char ol_delivery_d[DATETIME_SIZE];
		char ol_dist_info[Stock::DIST+1];
	};

	struct alignas(16) NewOrder {
		static constexpr int INITIAL_NUM_PER_DISTRICT = 900;

		int32_t no_w_id;
		int32_t no_d_id;
		int32_t no_o_id;
	};

	struct alignas(16) History {
		static constexpr int MIN_DATA         = 12;
		static constexpr int MAX_DATA         = 24;
		static constexpr float INITIAL_AMOUNT = 10.00f;

		int32_t h_c_id;
		int32_t h_c_d_id;
		int32_t h_c_w_id;
		int32_t h_d_id;
		int32_t h_w_id;
		float h_amount;
		char h_date[DATETIME_SIZE];
		char h_data[MAX_DATA+1];
	};

	// Data returned by the "order status" transaction.
	struct alignas(16) OrderStatusOutput {
		// From customer
		int32_t c_id;  // unclear if this needs to be returned
		float c_balance;

		// From order
		int32_t o_id;
		int32_t o_carrier_id;

		struct OrderLineSubset {
			int32_t ol_i_id;
			int32_t ol_supply_w_id;
			int32_t ol_quantity;
			float ol_amount;
			char ol_delivery_d[DATETIME_SIZE];
		};

		std::vector<OrderLineSubset> lines;

		// From customer
		char c_first[Customer::MAX_FIRST+1];
		char c_middle[Customer::MIDDLE+1];
		char c_last[Customer::MAX_LAST+1];

		// From order
		char o_entry_d[DATETIME_SIZE];
	};

	struct alignas(16) NewOrderItem {
		int32_t i_id;
		int32_t ol_supply_w_id;
		int32_t ol_quantity;
	};

	struct alignas(16) NewOrderOutput {
		// Zero initialize everything. This avoids copying uninitialized data around when
		// serializing/deserializing.
		NewOrderOutput() : w_tax(0), d_tax(0), o_id(0), c_discount(0), total(0) {
			memset(c_last, 0, sizeof(c_last));
			memset(c_credit, 0, sizeof(c_credit));
			memset(status, 0, sizeof(status));
		}

		float w_tax;
		float d_tax;

		// From district d_next_o_id
		int32_t o_id;

		float c_discount;

		// TODO: Client can compute this from other values.
		float total;

		struct ItemInfo {
			static constexpr char BRAND = 'B';
			static constexpr char GENERIC = 'G';

			int32_t s_quantity;
			float i_price;
			// TODO: Client can compute this from other values.
			float ol_amount;
			char brand_generic;
			char i_name[Item::MAX_NAME+1];
		};

		std::vector<ItemInfo> items;
		char c_last[Customer::MAX_LAST+1];
		char c_credit[Customer::CREDIT+1];

		static constexpr int MAX_STATUS = 25;
		static constexpr const char INVALID_ITEM_STATUS[] = "Invalid";
		char status[MAX_STATUS+1];
	};

	struct alignas(16) PaymentOutput {
		// TPC-C 2.5.3.4 specifies these output fields
		char w_street_1[Address::MAX_STREET+1];
		char w_street_2[Address::MAX_STREET+1];
		char w_city[Address::MAX_CITY+1];
		char w_state[Address::STATE+1];
		char w_zip[Address::ZIP+1];

		char d_street_1[Address::MAX_STREET+1];
		char d_street_2[Address::MAX_STREET+1];
		char d_city[Address::MAX_CITY+1];
		char d_state[Address::STATE+1];
		char d_zip[Address::ZIP+1];

		float c_credit_lim;
		float c_discount;
		float c_balance;
		char c_first[Customer::MAX_FIRST+1];
		char c_middle[Customer::MIDDLE+1];
		char c_last[Customer::MAX_LAST+1];
		char c_street_1[Address::MAX_STREET+1];
		char c_street_2[Address::MAX_STREET+1];
		char c_city[Address::MAX_CITY+1];
		char c_state[Address::STATE+1];
		char c_zip[Address::ZIP+1];
		char c_phone[Customer::PHONE+1];
		char c_since[DATETIME_SIZE];
		char c_credit[Customer::CREDIT+1];
		char c_data[Customer::MAX_DATA+1];
	};

	struct alignas(16) DeliveryOrderInfo {
		int32_t d_id;
		int32_t o_id;
	};

	enum class DurableTable: uint32_t {
		Item      = 0,
		Warehouse = 1,
		District  = 2,
		Stock     = 3,
		Customer  = 4,
		Order     = 5,
		OrderLine = 6,
		NewOrder  = 7,
		History   = 8,
		// Not standard
		NewOrderID = 9
	};

}
