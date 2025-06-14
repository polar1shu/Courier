/*
 * @author: BL-GS 
 * @date:   2022/12/2
 */

#pragma once

#include <workload/ycsb/ycsb_config.h>

#include <workload/generator/constant_generator.h>
#include <workload/generator/uniform_generator.h>
#include <workload/generator/zipfian_generator.h>
#include <workload/generator/scramble_zipfian_generator.h>
#include <workload/generator/hotpos_generator.h>
#include <workload/generator/skew_latest_generator.h>

#include <workload/generator/operation_generator.h>
#include <workload/generator/random_value_generator.h>
#include <workload/generator/acknowledged_counter_generator.h>


namespace workload {

	inline namespace ycsb {

		template<YCSBConfigGenerator Config,
				class IntType,
				IntType UPPER_BOUND,
				IntType LOWER_BOUND>
		requires std::is_integral_v<IntType>
		struct FieldLengthDistributionGeneratorTrait {
			using Generator = ConstantGenerator<IntType, UPPER_BOUND, LOWER_BOUND>;
		};

		template<class IntType,
				IntType UPPER_BOUND,
				IntType LOWER_BOUND>
		requires std::is_integral_v<IntType>
		struct FieldLengthDistributionGeneratorTrait<YCSBConfigGenerator::Zipfian, IntType, UPPER_BOUND, LOWER_BOUND> {
			using Generator = ZipfianGenerator<IntType, UPPER_BOUND, LOWER_BOUND>;
		};

		template<class IntType,
				IntType UPPER_BOUND,
				IntType LOWER_BOUND>
		requires std::is_integral_v<IntType>
		struct FieldLengthDistributionGeneratorTrait<YCSBConfigGenerator::Uniform, IntType, UPPER_BOUND, LOWER_BOUND> {
			using Generator = UniformGenerator<IntType, UPPER_BOUND, LOWER_BOUND>;
		};

		template<class OpeType>
		struct RequestDistributionGeneratorTrait {
			using Generator = OperationGenerator<OpeType>;
		};

		template<YCSBConfigGenerator Config,
				class IntType,
				IntType UPPER_BOUND,
				IntType LOWER_BOUND>
		requires std::is_integral_v<IntType>
		struct ScanLengthDistributionGeneratorTrait {
			using Generator = UniformGenerator<IntType, UPPER_BOUND, LOWER_BOUND>;
		};

		template<class IntType,
				IntType UPPER_BOUND,
				IntType LOWER_BOUND>
		requires std::is_integral_v<IntType>
		struct ScanLengthDistributionGeneratorTrait<YCSBConfigGenerator::Zipfian, IntType, UPPER_BOUND, LOWER_BOUND> {
			using Generator = ZipfianGenerator<IntType, UPPER_BOUND, LOWER_BOUND>;
		};

		template<class IntType,
				IntType UPPER_BOUND,
				IntType LOWER_BOUND>
		requires std::is_integral_v<IntType>
		struct ScanLengthDistributionGeneratorTrait<YCSBConfigGenerator::Constant, IntType, UPPER_BOUND, LOWER_BOUND> {
			using Generator = ConstantGenerator<IntType, UPPER_BOUND, LOWER_BOUND>;
		};

		template<YCSBConfigGenerator Config,
				class IntType, IntType UPPER_BOUND, IntType LOWER_BOUND,
				uint32_t HOTSPOS_FRACTION, uint32_t HOTSOPN_FRACTION>
		requires std::is_integral_v<IntType>
		struct KeyGeneratorTrait {
			using Generator = UniformGenerator<IntType, UPPER_BOUND, LOWER_BOUND>;
		};

		template<
				class IntType, IntType UPPER_BOUND, IntType LOWER_BOUND,
				uint32_t HOTSPOS_FRACTION, uint32_t HOTSOPN_FRACTION>
		requires std::is_integral_v<IntType>
		struct KeyGeneratorTrait<YCSBConfigGenerator::Zipfian, IntType, UPPER_BOUND, LOWER_BOUND, HOTSPOS_FRACTION, HOTSOPN_FRACTION> {
			// TODO:
			using Generator = ScrambleZipfianGenerator<IntType, UPPER_BOUND, LOWER_BOUND>;
		};

		template<
				class IntType, IntType UPPER_BOUND, IntType LOWER_BOUND,
				uint32_t HOTSPOS_FRACTION, uint32_t HOTSOPN_FRACTION>
		requires std::is_integral_v<IntType>
		struct KeyGeneratorTrait<YCSBConfigGenerator::Latest, IntType, UPPER_BOUND, LOWER_BOUND, HOTSPOS_FRACTION, HOTSOPN_FRACTION> {
			using Generator = SkewLatestGenerator<IntType, UPPER_BOUND, LOWER_BOUND>;
		};

		template<
				class IntType, IntType UPPER_BOUND, IntType LOWER_BOUND,
				uint32_t HOTSPOS_FRACTION, uint32_t HOTSOPN_FRACTION>
		requires std::is_integral_v<IntType>
		struct KeyGeneratorTrait<YCSBConfigGenerator::HotPos, IntType, UPPER_BOUND, LOWER_BOUND, HOTSPOS_FRACTION, HOTSOPN_FRACTION> {
			using Generator = HotposGenerator<IntType, UPPER_BOUND, LOWER_BOUND, HOTSPOS_FRACTION, HOTSOPN_FRACTION>;
		};


		template<class Config>
		struct YCSBConfigManager {
			static constexpr std::initializer_list<uint32_t> Percentages {
					Config::READ_PERCENTAGE,
					Config::UPDATE_PERCENTAGE,
					Config::SCAN_PERCENTAGE,
					Config::INSERT_PERCENTAGE,
					Config::READ_MODIFY_WRITE_PERCENTAGE
			};

			using KeyType         = uint32_t;
			using FieldType       = uint32_t;
			using FieldLengthType = uint32_t;
			using ScanLengthType  = uint32_t;

			using FieldChooser =
					UniformGenerator<FieldType, Config::FIELD_COUNT - 1, 0>;
			using FieldLengthGenerator =
					typename FieldLengthDistributionGeneratorTrait<Config::FIELD_LENGTH_DISTRIBUTION, FieldLengthType, Config::MAX_FIELD_LENGTH, Config::MIN_FIELD_LENGTH>::Generator;
			using RequestGenerator =
					typename RequestDistributionGeneratorTrait<TransactionType>::Generator;
			using ScanLengthGenerator =
					typename ScanLengthDistributionGeneratorTrait<Config::SCAN_LENGTH_DISTRIBUTION, ScanLengthType, Config::MAX_SCAN_LENGTH, Config::MIN_SCAN_LENGTH>::Generator;
			using KeyGenerator =
					typename KeyGeneratorTrait<Config::REQUEST_DISTRIBUTION, KeyType, Config::TABLE_SIZE - 1, 0, Config::HOTSPOT_DATA_FRACTION, Config::HOTSPOT_OPN_FRACTION>::Generator;
			using ValueGenerator =
					RandomValueGenerator<Config::MAX_FIELD_LENGTH>;
			using StepGenerator =
					// AcknowledgedCounterGenerator<KeyType, Config::TABLE_SIZE - 1, 0>;
					UnlimitedAcknowledgedCounterGenerator<KeyType, 0>;
		};

	}
}
