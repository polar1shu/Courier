/*
 * @author: BL-GS 
 * @date:   2022/12/1
 */

#pragma once

#include <cstdint>

namespace workload {

	inline namespace ycsb {

		enum class TransactionType {
			Read   = 0,
			Update = 1,
			Scan   = 2,
			Insert = 3,
			ReadModifyWrite = 4
		};

		enum class YCSBConfigType { A, B, C, D, E, F, ReadMostly, WriteIntensive, WriteMostly };

		enum class YCSBConfigGenerator { Zipfian, Uniform, Constant, HotPos, Latest };

		enum class YCSBConfigOrder { Hashed, Ordered };

		template<YCSBConfigType Type>
		struct YCSBConfig {};

		template<>
		struct YCSBConfig<YCSBConfigType::A> {

			static constexpr uint64_t TABLE_SIZE                           = 1024 * 1024 * 10;

			static constexpr uint32_t INITIAL_SIZE                         = TABLE_SIZE;
			/// Number of fields in a record
			static constexpr uint32_t FIELD_COUNT                          = 1;
			/// The field length distribution.
			static constexpr YCSBConfigGenerator FIELD_LENGTH_DISTRIBUTION = YCSBConfigGenerator::Constant;
			/// The maximum length of a field in bytes
			static constexpr uint32_t MAX_FIELD_LENGTH                     = 128;
			/// The minimum length of a field in bytes
			static constexpr uint32_t MIN_FIELD_LENGTH                     = 128;
			/// whether to read one field (false) or all fields (true) of a record
			static constexpr bool READ_ALL_FIELD                           = false;
			/// whether to write one field (false) or all fields (true) of a record
			static constexpr bool WRITE_ALL_FIELD                          = false;
			/// The property of read request.
			static constexpr uint32_t READ_PERCENTAGE                      = 50;
			/// The property of write request.
			static constexpr uint32_t UPDATE_PERCENTAGE                    = 50;
			/// The property of insert request.
			static constexpr uint32_t INSERT_PERCENTAGE                    = 0;
			/// The property of scan request.
			static constexpr uint32_t SCAN_PERCENTAGE                      = 0;
			/// The property of read-modify-write request.
			static constexpr uint32_t READ_MODIFY_WRITE_PERCENTAGE         = 0;
			/// The distribution of requests across the keyspace.
			static constexpr YCSBConfigGenerator REQUEST_DISTRIBUTION      = YCSBConfigGenerator::Zipfian;
			/// The minimum length of scanning in bytes
			static constexpr uint32_t MAX_SCAN_LENGTH                      = 1000;
			/// The maximum length of scanning in bytes
			static constexpr uint32_t MIN_SCAN_LENGTH                      = 1;
			/// The distribution of the scan length
			static constexpr YCSBConfigGenerator SCAN_LENGTH_DISTRIBUTION  = YCSBConfigGenerator::Uniform;
			/// The order of insert records
			static constexpr YCSBConfigOrder INSERT_ORDER                  = YCSBConfigOrder::Ordered;
			/// The size of hot set
			static constexpr uint32_t HOTSPOT_DATA_FRACTION                = 20;
			/// The percentage operations accessing the hot set.
			static constexpr uint32_t HOTSPOT_OPN_FRACTION                 = 80;
			/// How many times to retry when insertion of a single item fails.
			static constexpr uint32_t INSERTION_RETRY_LIMIT                = 0;
			/// On average, how long to wait between the retries, in seconds.
			static constexpr uint32_t INSERTION_RETRY_INTERVAL             = 3;
		};


		template<>
		struct YCSBConfig<YCSBConfigType::B> {

			static constexpr uint64_t TABLE_SIZE                           = 1024 * 1024 * 10;

			static constexpr uint32_t INITIAL_SIZE                         = TABLE_SIZE;
			/// Number of fields in a record
			static constexpr uint32_t FIELD_COUNT                          = 1;
			/// The field length distribution.
			static constexpr YCSBConfigGenerator FIELD_LENGTH_DISTRIBUTION = YCSBConfigGenerator::Constant;
			/// The maximum length of a field in bytes
			static constexpr uint32_t MAX_FIELD_LENGTH                     = 128;
			/// The minimum length of a field in bytes
			static constexpr uint32_t MIN_FIELD_LENGTH                     = 128;
			/// whether to read one field (false) or all fields (true) of a record
			static constexpr bool READ_ALL_FIELD                           = false;
			/// whether to write one field (false) or all fields (true) of a record
			static constexpr bool WRITE_ALL_FIELD                          = false;
			/// The property of read request.
			static constexpr uint32_t READ_PERCENTAGE                      = 95;
			/// The property of write request.
			static constexpr uint32_t UPDATE_PERCENTAGE                    = 5;
			/// The property of insert request.
			static constexpr uint32_t INSERT_PERCENTAGE                    = 0;
			/// The property of scan request.
			static constexpr uint32_t SCAN_PERCENTAGE                      = 0;
			/// The property of read-modify-write request.
			static constexpr uint32_t READ_MODIFY_WRITE_PERCENTAGE         = 0;
			/// The distribution of requests across the keyspace.
			static constexpr YCSBConfigGenerator REQUEST_DISTRIBUTION      = YCSBConfigGenerator::Zipfian;
			/// The minimum length of scanning in bytes
			static constexpr uint32_t MAX_SCAN_LENGTH                      = 1000;
			/// The maximum length of scanning in bytes
			static constexpr uint32_t MIN_SCAN_LENGTH                      = 1;
			/// The distribution of the scan length
			static constexpr YCSBConfigGenerator SCAN_LENGTH_DISTRIBUTION  = YCSBConfigGenerator::Uniform;
			/// The order of insert records
			static constexpr YCSBConfigOrder INSERT_ORDER                  = YCSBConfigOrder::Ordered;
			/// The size of hot set
			static constexpr uint32_t HOTSPOT_DATA_FRACTION                = 20;
			/// The percentage operations accessing the hot set.
			static constexpr uint32_t HOTSPOT_OPN_FRACTION                 = 80;
			/// How many times to retry when insertion of a single item fails.
			static constexpr uint32_t INSERTION_RETRY_LIMIT                = 0;
			/// On average, how long to wait between the retries, in seconds.
			static constexpr uint32_t INSERTION_RETRY_INTERVAL             = 3;
		};


		template<>
		struct YCSBConfig<YCSBConfigType::C> {

			static constexpr uint64_t TABLE_SIZE                           = 1024 * 1024 * 10;

			static constexpr uint32_t INITIAL_SIZE                         = TABLE_SIZE;
			/// Number of fields in a record
			static constexpr uint32_t FIELD_COUNT                          = 1;
			/// The field length distribution.
			static constexpr YCSBConfigGenerator FIELD_LENGTH_DISTRIBUTION = YCSBConfigGenerator::Constant;
			/// The maximum length of a field in bytes
			static constexpr uint32_t MAX_FIELD_LENGTH                     = 128;
			/// The minimum length of a field in bytes
			static constexpr uint32_t MIN_FIELD_LENGTH                     = 128;
			/// whether to read one field (false) or all fields (true) of a record
			static constexpr bool READ_ALL_FIELD                           = false;
			/// whether to write one field (false) or all fields (true) of a record
			static constexpr bool WRITE_ALL_FIELD                          = false;
			/// The property of read request.
			static constexpr uint32_t READ_PERCENTAGE                      = 100;
			/// The property of write request.
			static constexpr uint32_t UPDATE_PERCENTAGE                    = 0;
			/// The property of insert request.
			static constexpr uint32_t INSERT_PERCENTAGE                    = 0;
			/// The property of scan request.
			static constexpr uint32_t SCAN_PERCENTAGE                      = 0;
			/// The property of read-modify-write request.
			static constexpr uint32_t READ_MODIFY_WRITE_PERCENTAGE         = 0;
			/// The distribution of requests across the keyspace.
			static constexpr YCSBConfigGenerator REQUEST_DISTRIBUTION      = YCSBConfigGenerator::Zipfian;
			/// The minimum length of scanning in bytes
			static constexpr uint32_t MAX_SCAN_LENGTH                      = 1000;
			/// The maximum length of scanning in bytes
			static constexpr uint32_t MIN_SCAN_LENGTH                      = 1;
			/// The distribution of the scan length
			static constexpr YCSBConfigGenerator SCAN_LENGTH_DISTRIBUTION  = YCSBConfigGenerator::Uniform;
			/// The order of insert records
			static constexpr YCSBConfigOrder INSERT_ORDER                  = YCSBConfigOrder::Ordered;
			/// The size of hot set
			static constexpr uint32_t HOTSPOT_DATA_FRACTION                = 20;
			/// The percentage operations accessing the hot set.
			static constexpr uint32_t HOTSPOT_OPN_FRACTION                 = 80;
			/// How many times to retry when insertion of a single item fails.
			static constexpr uint32_t INSERTION_RETRY_LIMIT                = 0;
			/// On average, how long to wait between the retries, in seconds.
			static constexpr uint32_t INSERTION_RETRY_INTERVAL             = 3;
		};


		template<>
		struct YCSBConfig<YCSBConfigType::D> {

			static constexpr uint64_t TABLE_SIZE                           = 64 * 1024;

			static constexpr uint32_t INITIAL_SIZE                         = TABLE_SIZE;
			/// Number of fields in a record
			static constexpr uint32_t FIELD_COUNT                          = 10;
			/// The field length distribution.
			static constexpr YCSBConfigGenerator FIELD_LENGTH_DISTRIBUTION = YCSBConfigGenerator::Constant;
			/// The maximum length of a field in bytes
			static constexpr uint32_t MAX_FIELD_LENGTH                     = 128;
			/// The minimum length of a field in bytes
			static constexpr uint32_t MIN_FIELD_LENGTH                     = 1;
			/// whether to read one field (false) or all fields (true) of a record
			static constexpr bool READ_ALL_FIELD                           = true;
			/// whether to write one field (false) or all fields (true) of a record
			static constexpr bool WRITE_ALL_FIELD                          = false;
			/// The property of read request.
			static constexpr uint32_t READ_PERCENTAGE                      = 95;
			/// The property of write request.
			static constexpr uint32_t UPDATE_PERCENTAGE                    = 5;
			/// The property of insert request.
			static constexpr uint32_t INSERT_PERCENTAGE                    = 0;
			/// The property of scan request.
			static constexpr uint32_t SCAN_PERCENTAGE                      = 0;
			/// The property of read-modify-write request.
			static constexpr uint32_t READ_MODIFY_WRITE_PERCENTAGE         = 0;
			/// The distribution of requests across the keyspace.
			static constexpr YCSBConfigGenerator REQUEST_DISTRIBUTION      = YCSBConfigGenerator::Latest;
			/// The minimum length of scanning in bytes
			static constexpr uint32_t MAX_SCAN_LENGTH                      = 1000;
			/// The maximum length of scanning in bytes
			static constexpr uint32_t MIN_SCAN_LENGTH                      = 1;
			/// The distribution of the scan length
			static constexpr YCSBConfigGenerator SCAN_LENGTH_DISTRIBUTION  = YCSBConfigGenerator::Uniform;
			/// The order of insert records
			static constexpr YCSBConfigOrder INSERT_ORDER                  = YCSBConfigOrder::Ordered;
			/// The size of hot set
			static constexpr uint32_t HOTSPOT_DATA_FRACTION                = 20;
			/// The percentage operations accessing the hot set.
			static constexpr uint32_t HOTSPOT_OPN_FRACTION                 = 80;
			/// How many times to retry when insertion of a single item fails.
			static constexpr uint32_t INSERTION_RETRY_LIMIT                = 0;
			/// On average, how long to wait between the retries, in seconds.
			static constexpr uint32_t INSERTION_RETRY_INTERVAL             = 3;
		};


		template<>
		struct YCSBConfig<YCSBConfigType::E> {

			static constexpr uint64_t TABLE_SIZE                           = 64 * 1024;

			static constexpr uint32_t INITIAL_SIZE                         = 10000;
			/// Number of fields in a record
			static constexpr uint32_t FIELD_COUNT                          = 10;
			/// The field length distribution.
			static constexpr YCSBConfigGenerator FIELD_LENGTH_DISTRIBUTION = YCSBConfigGenerator::Constant;
			/// The maximum length of a field in bytes
			static constexpr uint32_t MAX_FIELD_LENGTH                     = 128;
			/// The minimum length of a field in bytes
			static constexpr uint32_t MIN_FIELD_LENGTH                     = 1;
			/// whether to read one field (false) or all fields (true) of a record
			static constexpr bool READ_ALL_FIELD                           = true;
			/// whether to write one field (false) or all fields (true) of a record
			static constexpr bool WRITE_ALL_FIELD                          = false;
			/// The property of read request.
			static constexpr uint32_t READ_PERCENTAGE                      = 0;
			/// The property of write request.
			static constexpr uint32_t UPDATE_PERCENTAGE                    = 0;
			/// The property of insert request.
			static constexpr uint32_t INSERT_PERCENTAGE                    = 5;
			/// The property of scan request.
			static constexpr uint32_t SCAN_PERCENTAGE                      = 95;
			/// The property of read-modify-write request.
			static constexpr uint32_t READ_MODIFY_WRITE_PERCENTAGE         = 0;
			/// The distribution of requests across the keyspace.
			static constexpr YCSBConfigGenerator REQUEST_DISTRIBUTION      = YCSBConfigGenerator::Zipfian;
			/// The minimum length of scanning in bytes
			static constexpr uint32_t MAX_SCAN_LENGTH                      = 1000;
			/// The maximum length of scanning in bytes
			static constexpr uint32_t MIN_SCAN_LENGTH                      = 1;
			/// The distribution of the scan length
			static constexpr YCSBConfigGenerator SCAN_LENGTH_DISTRIBUTION  = YCSBConfigGenerator::Uniform;
			/// The order of insert records
			static constexpr YCSBConfigOrder INSERT_ORDER                  = YCSBConfigOrder::Ordered;
			/// The size of hot set
			static constexpr uint32_t HOTSPOT_DATA_FRACTION                = 20;
			/// The percentage operations accessing the hot set.
			static constexpr uint32_t HOTSPOT_OPN_FRACTION                 = 80;
			/// How many times to retry when insertion of a single item fails.
			static constexpr uint32_t INSERTION_RETRY_LIMIT                = 0;
			/// On average, how long to wait between the retries, in seconds.
			static constexpr uint32_t INSERTION_RETRY_INTERVAL             = 3;
		};

		template<>
		struct YCSBConfig<YCSBConfigType::F> {

			static constexpr uint64_t TABLE_SIZE                           = 64 * 1024;

			static constexpr uint32_t INITIAL_SIZE                         = TABLE_SIZE;
			/// Number of fields in a record
			static constexpr uint32_t FIELD_COUNT                          = 10;
			/// The field length distribution.
			static constexpr YCSBConfigGenerator FIELD_LENGTH_DISTRIBUTION = YCSBConfigGenerator::Constant;
			/// The maximum length of a field in bytes
			static constexpr uint32_t MAX_FIELD_LENGTH                     = 128;
			/// The minimum length of a field in bytes
			static constexpr uint32_t MIN_FIELD_LENGTH                     = 1;
			/// whether to read one field (false) or all fields (true) of a record
			static constexpr bool READ_ALL_FIELD                           = true;
			/// whether to write one field (false) or all fields (true) of a record
			static constexpr bool WRITE_ALL_FIELD                          = false;
			/// The property of read request.
			static constexpr uint32_t READ_PERCENTAGE                      = 50;
			/// The property of write request.
			static constexpr uint32_t UPDATE_PERCENTAGE                    = 0;
			/// The property of insert request.
			static constexpr uint32_t INSERT_PERCENTAGE                    = 0;
			/// The property of scan request.
			static constexpr uint32_t SCAN_PERCENTAGE                      = 0;
			/// The property of read-modify-write request.
			static constexpr uint32_t READ_MODIFY_WRITE_PERCENTAGE         = 50;
			/// The distribution of requests across the keyspace.
			static constexpr YCSBConfigGenerator REQUEST_DISTRIBUTION      = YCSBConfigGenerator::Zipfian;
			/// The minimum length of scanning in bytes
			static constexpr uint32_t MAX_SCAN_LENGTH                      = 1000;
			/// The maximum length of scanning in bytes
			static constexpr uint32_t MIN_SCAN_LENGTH                      = 1;
			/// The distribution of the scan length
			static constexpr YCSBConfigGenerator SCAN_LENGTH_DISTRIBUTION  = YCSBConfigGenerator::Uniform;
			/// The order of insert records
			static constexpr YCSBConfigOrder INSERT_ORDER                  = YCSBConfigOrder::Ordered;
			/// The size of hot set
			static constexpr uint32_t HOTSPOT_DATA_FRACTION                = 20;
			/// The percentage operations accessing the hot set.
			static constexpr uint32_t HOTSPOT_OPN_FRACTION                 = 80;
			/// How many times to retry when insertion of a single item fails.
			static constexpr uint32_t INSERTION_RETRY_LIMIT                = 0;
			/// On average, how long to wait between the retries, in seconds.
			static constexpr uint32_t INSERTION_RETRY_INTERVAL             = 3;
		};

		template<>
		struct YCSBConfig<YCSBConfigType::ReadMostly> {

			static constexpr uint64_t TABLE_SIZE                           = 1024 * 1024 * 10;

			static constexpr uint32_t INITIAL_SIZE                         = TABLE_SIZE;
			/// Number of fields in a record
			static constexpr uint32_t FIELD_COUNT                          = 1;
			/// The field length distribution.
			static constexpr YCSBConfigGenerator FIELD_LENGTH_DISTRIBUTION = YCSBConfigGenerator::Constant;
			/// The maximum length of a field in bytes
			static constexpr uint32_t MAX_FIELD_LENGTH                     = 128;
			/// The minimum length of a field in bytes
			static constexpr uint32_t MIN_FIELD_LENGTH                     = 128;
			/// whether to read one field (false) or all fields (true) of a record
			static constexpr bool READ_ALL_FIELD                           = false;
			/// whether to write one field (false) or all fields (true) of a record
			static constexpr bool WRITE_ALL_FIELD                          = false;
			/// The property of read request.
			static constexpr uint32_t READ_PERCENTAGE                      = 98;
			/// The property of write request.
			static constexpr uint32_t UPDATE_PERCENTAGE                    = 2;
			/// The property of insert request.
			static constexpr uint32_t INSERT_PERCENTAGE                    = 0;
			/// The property of scan request.
			static constexpr uint32_t SCAN_PERCENTAGE                      = 0;
			/// The property of read-modify-write request.
			static constexpr uint32_t READ_MODIFY_WRITE_PERCENTAGE         = 0;
			/// The distribution of requests across the keyspace.
			static constexpr YCSBConfigGenerator REQUEST_DISTRIBUTION      = YCSBConfigGenerator::Zipfian;
			/// The minimum length of scanning in bytes
			static constexpr uint32_t MAX_SCAN_LENGTH                      = 1000;
			/// The maximum length of scanning in bytes
			static constexpr uint32_t MIN_SCAN_LENGTH                      = 1;
			/// The distribution of the scan length
			static constexpr YCSBConfigGenerator SCAN_LENGTH_DISTRIBUTION  = YCSBConfigGenerator::Uniform;
			/// The order of insert records
			static constexpr YCSBConfigOrder INSERT_ORDER                  = YCSBConfigOrder::Ordered;
			/// The size of hot set
			static constexpr uint32_t HOTSPOT_DATA_FRACTION                = 20;
			/// The percentage operations accessing the hot set.
			static constexpr uint32_t HOTSPOT_OPN_FRACTION                 = 80;
			/// How many times to retry when insertion of a single item fails.
			static constexpr uint32_t INSERTION_RETRY_LIMIT                = 0;
			/// On average, how long to wait between the retries, in seconds.
			static constexpr uint32_t INSERTION_RETRY_INTERVAL             = 3;
		};

		template<>
		struct YCSBConfig<YCSBConfigType::WriteIntensive> {

			static constexpr uint64_t TABLE_SIZE                           = 1024 * 1024 * 10;

			static constexpr uint32_t INITIAL_SIZE                         = TABLE_SIZE;
			/// Number of fields in a record
			static constexpr uint32_t FIELD_COUNT                          = 1;
			/// The field length distribution.
			static constexpr YCSBConfigGenerator FIELD_LENGTH_DISTRIBUTION = YCSBConfigGenerator::Constant;
			/// The maximum length of a field in bytes
			static constexpr uint32_t MAX_FIELD_LENGTH                     = 128;
			/// The minimum length of a field in bytes
			static constexpr uint32_t MIN_FIELD_LENGTH                     = 128;
			/// whether to read one field (false) or all fields (true) of a record
			static constexpr bool READ_ALL_FIELD                           = false;
			/// whether to write one field (false) or all fields (true) of a record
			static constexpr bool WRITE_ALL_FIELD                          = false;
			/// The property of read request.
			static constexpr uint32_t READ_PERCENTAGE                      = 80;
			/// The property of write request.
			static constexpr uint32_t UPDATE_PERCENTAGE                    = 20;
			/// The property of insert request.
			static constexpr uint32_t INSERT_PERCENTAGE                    = 0;
			/// The property of scan request.
			static constexpr uint32_t SCAN_PERCENTAGE                      = 0;
			/// The property of read-modify-write request.
			static constexpr uint32_t READ_MODIFY_WRITE_PERCENTAGE         = 0;
			/// The distribution of requests across the keyspace.
			static constexpr YCSBConfigGenerator REQUEST_DISTRIBUTION      = YCSBConfigGenerator::Zipfian;
			/// The minimum length of scanning in bytes
			static constexpr uint32_t MAX_SCAN_LENGTH                      = 1000;
			/// The maximum length of scanning in bytes
			static constexpr uint32_t MIN_SCAN_LENGTH                      = 1;
			/// The distribution of the scan length
			static constexpr YCSBConfigGenerator SCAN_LENGTH_DISTRIBUTION  = YCSBConfigGenerator::Uniform;
			/// The order of insert records
			static constexpr YCSBConfigOrder INSERT_ORDER                  = YCSBConfigOrder::Ordered;
			/// The size of hot set
			static constexpr uint32_t HOTSPOT_DATA_FRACTION                = 20;
			/// The percentage operations accessing the hot set.
			static constexpr uint32_t HOTSPOT_OPN_FRACTION                 = 80;
			/// How many times to retry when insertion of a single item fails.
			static constexpr uint32_t INSERTION_RETRY_LIMIT                = 0;
			/// On average, how long to wait between the retries, in seconds.
			static constexpr uint32_t INSERTION_RETRY_INTERVAL             = 3;
		};

		template<>
		struct YCSBConfig<YCSBConfigType::WriteMostly> {

			static constexpr uint64_t TABLE_SIZE                           = 1024 * 1024 * 10;

			static constexpr uint32_t INITIAL_SIZE                         = TABLE_SIZE;
			/// Number of fields in a record
			static constexpr uint32_t FIELD_COUNT                          = 1;
			/// The field length distribution.
			static constexpr YCSBConfigGenerator FIELD_LENGTH_DISTRIBUTION = YCSBConfigGenerator::Constant;
			/// The maximum length of a field in bytes
			static constexpr uint32_t MAX_FIELD_LENGTH                     = 128;
			/// The minimum length of a field in bytes
			static constexpr uint32_t MIN_FIELD_LENGTH                     = 128;
			/// whether to read one field (false) or all fields (true) of a record
			static constexpr bool READ_ALL_FIELD                           = false;
			/// whether to write one field (false) or all fields (true) of a record
			static constexpr bool WRITE_ALL_FIELD                          = false;
			/// The property of read request.
			static constexpr uint32_t READ_PERCENTAGE                      = 20;
			/// The property of write request.
			static constexpr uint32_t UPDATE_PERCENTAGE                    = 80;
			/// The property of insert request.
			static constexpr uint32_t INSERT_PERCENTAGE                    = 0;
			/// The property of scan request.
			static constexpr uint32_t SCAN_PERCENTAGE                      = 0;
			/// The property of read-modify-write request.
			static constexpr uint32_t READ_MODIFY_WRITE_PERCENTAGE         = 0;
			/// The distribution of requests across the keyspace.
			static constexpr YCSBConfigGenerator REQUEST_DISTRIBUTION      = YCSBConfigGenerator::Zipfian;
			/// The minimum length of scanning in bytes
			static constexpr uint32_t MAX_SCAN_LENGTH                      = 1000;
			/// The maximum length of scanning in bytes
			static constexpr uint32_t MIN_SCAN_LENGTH                      = 1;
			/// The distribution of the scan length
			static constexpr YCSBConfigGenerator SCAN_LENGTH_DISTRIBUTION  = YCSBConfigGenerator::Uniform;
			/// The order of insert records
			static constexpr YCSBConfigOrder INSERT_ORDER                  = YCSBConfigOrder::Ordered;
			/// The size of hot set
			static constexpr uint32_t HOTSPOT_DATA_FRACTION                = 20;
			/// The percentage operations accessing the hot set.
			static constexpr uint32_t HOTSPOT_OPN_FRACTION                 = 80;
			/// How many times to retry when insertion of a single item fails.
			static constexpr uint32_t INSERTION_RETRY_LIMIT                = 0;
			/// On average, how long to wait between the retries, in seconds.
			static constexpr uint32_t INSERTION_RETRY_INTERVAL             = 3;
		};

	}

}
