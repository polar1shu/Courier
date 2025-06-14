/*
 * @author: BL-GS 
 * @date:   2022/12/1
 */

#pragma once

#include <atomic>
#include <limits>

#include <util/utility_macro.h>
#include <util/random_generator.h>
#include <util/simple_hash.h>

#include <workload/abstract_workload.h>
#include <workload/ycsb/ycsb_config.h>
#include <workload/ycsb/ycsb_config_manager.h>
#include <workload/ycsb/ycsb_transaction.h>

#include <workload/generator/abstract_generator.h>
#include <workload/generator/zipfian_generator.h>
#include <workload/generator/uniform_generator.h>
#include <workload/generator/hotpos_generator.h>
#include <workload/generator/operation_generator.h>
#include <workload/generator/random_value_generator.h>

#include <workload/ycsb/ycsb_element.h>

namespace workload {

	template<class Config>
	class YCSB {
	public:
		using ConfigManager = YCSBConfigManager<Config>;
		using KeyType       = ConfigManager::KeyType;
		using AbKeyType     = AbstractKey<KeyType>;
		using FieldType     = ConfigManager::FieldType;
		using ValueType     = YCSBField<Config::MAX_FIELD_LENGTH>;
		using RowType       = YCSBRow<Config::MAX_FIELD_LENGTH, Config::FIELD_COUNT>;

		static constexpr uint32_t ALL_FIELD    = std::numeric_limits<uint32_t>::max();
		static constexpr uint32_t INITIAL_SIZE = Config::INITIAL_SIZE;

		static constexpr std::initializer_list<TableScheme> TableSchemeSizeDefinition = {
				TableScheme {  sizeof(RowType), Config::TABLE_SIZE }
		};

		using Transaction           = YCSBTransaction<KeyType, FieldType, RowType, typename ConfigManager::StepGenerator>;

		using FieldChooser         = ConfigManager::FieldChooser;
		static_assert(NumberGeneratorConcept<FieldChooser>);

		using FieldLengthGenerator = ConfigManager::FieldLengthGenerator;
		static_assert(NumberGeneratorConcept<FieldLengthGenerator>);

		using RequestGenerator     = ConfigManager::RequestGenerator;
		static_assert(OperationGeneratorConcept<RequestGenerator>);

		using KeyGenerator         = ConfigManager::KeyGenerator;
		static_assert(KeyGeneratorConcept<KeyGenerator>);

		using ScanLengthGenerator  = ConfigManager::ScanLengthGenerator;
		static_assert(NumberGeneratorConcept<ScanLengthGenerator>);

		using ValueGenerator       = ConfigManager::ValueGenerator;
		static_assert(StringGeneratorConcept<ValueGenerator>);

		using StepGenerator        = ConfigManager::StepGenerator;
		static_assert(StepNumberGeneratorConcept<StepGenerator>);

	private:

		FieldChooser            field_chooser_;

		FieldLengthGenerator    field_length_generator_;

		RequestGenerator        request_generator_;

		KeyGenerator            key_generator_;

		ScanLengthGenerator     scan_length_generator_;

		ValueGenerator          random_value_generator_;

		StepGenerator           step_key_generator_;


	public:
		YCSB(): request_generator_(ConfigManager::Percentages) {}

		//! Generate transaction
		//! @return a new transaction
		Transaction generate_transaction() {
			TransactionType ope = request_generator_.get_next();
			switch (ope) {
				case TransactionType::Read:
					return generate_read_transaction();

				case TransactionType::Update:
					return generate_update_transaction();

				case TransactionType::Insert:
					return generate_insert_transaction();

				case TransactionType::Scan:
					return generate_scan_transaction();

				case TransactionType::ReadModifyWrite: [[fallthrough]];
				default:
					return generate_readmodifywrite_transaction();
			}
		}

		//! @brief Get insert transaction, always for initialize index or something else.
		//! @return Insert transaction
		std::vector<Transaction> initialize_insert() {
			std::vector<Transaction> res_tx;
			for (uint32_t i = 0; i < INITIAL_SIZE; ++i) {
				res_tx.emplace_back(generate_insert_transaction());
			}
			return res_tx;
		}

	private:
		//! Generate transaction of reading
		//! @return transaction of reading
		Transaction generate_read_transaction() {
			KeyType key = get_limited_key(key_generator_, step_key_generator_.get_limit());

			if constexpr (Config::READ_ALL_FIELD || Config::FIELD_COUNT == 1) {
				// Read all fields
				return Transaction::get_read_tx(key, ALL_FIELD);
			}
			else {
				// Read a random field
				FieldType field = field_chooser_.get_next();
				return Transaction::get_read_tx(key, field);
			}
		}

		//! Generate transaction of updating
		//! @return transaction of updating
		Transaction generate_update_transaction() {
			KeyType key = get_limited_key(key_generator_, step_key_generator_.get_limit());

			if constexpr (Config::WRITE_ALL_FIELD || Config::FIELD_COUNT == 1) {
				// Update all fields
				RowType new_row;
				for (uint32_t i = 0; i < Config::FIELD_COUNT; ++i) {
					auto field_length = field_length_generator_.get_next();
					random_value_generator_.get_next(new_row.elements[i].get_data(), field_length);
				}
				return Transaction::get_update_tx(key, ALL_FIELD, new_row);
			}
			else {
				// Update a random field
				RowType new_row;
				uint32_t field = field_chooser_.get_next();
				auto field_length = field_length_generator_.get_next();
				random_value_generator_.get_next(new_row.elements[field].get_data(), field_length);

				return Transaction::get_update_tx(key, field, new_row);
			}
		}

		//! Generate transaction of inserting
		//! @return transaction of inserting
		Transaction generate_insert_transaction() {
			KeyType key = step_key_generator_.get_next();
			// Write all fields
			RowType new_row;
			for (uint32_t i = 0; i < Config::FIELD_COUNT; ++i) {
				auto field_length = field_length_generator_.get_next();
				random_value_generator_.get_next(new_row.elements[i].get_data(), field_length);
			}
			return Transaction::get_insert_tx(get_hashed_key(key), key, new_row, &step_key_generator_);
		}

		//! Generate transaction of scan
		//! @return transaction of scan
		Transaction generate_scan_transaction() {
			uint32_t scan_length = scan_length_generator_.get_next();

			DEBUG_ASSERT(step_key_generator_.get_limit() > scan_length);
			KeyType start_key = get_limited_key(key_generator_,
											step_key_generator_.get_limit() - scan_length);

			if constexpr (Config::READ_ALL_FIELD || Config::FIELD_COUNT == 1) {
				// Read a random field
				FieldType field = field_chooser_.get_next();
				return Transaction::get_scan_tx(start_key, field, scan_length);
			}
			else {
				return Transaction::get_scan_tx(start_key, ALL_FIELD, scan_length);
			}
		}

		//! Generate transaction of read-modify-write
		//! @return transaction of read-modify-write
		Transaction generate_readmodifywrite_transaction() {
			KeyType key = get_limited_key(key_generator_, step_key_generator_.get_limit());

			if constexpr (Config::WRITE_ALL_FIELD || Config::FIELD_COUNT == 1) {
				// Update all fields
				RowType new_row;
				for (uint32_t i = 0; i < Config::FIELD_COUNT; ++i) {
					auto field_length = field_length_generator_.get_next();
					random_value_generator_.get_next(new_row.elements[i].get_data(), field_length);
				}
				return Transaction::get_update_tx(key, ALL_FIELD, new_row);
			}
			else {
				// Update a random field
				RowType new_row;
				FieldType field = field_chooser_.get_next();
				auto field_length = field_length_generator_.get_next();
				random_value_generator_.get_next(new_row.elements[field].get_data(), field_length);

				return Transaction::get_update_tx(key, field, new_row);
			}
		}

	private:
		KeyType get_hashed_key(const KeyType &key) {
			if constexpr (Config::INSERT_ORDER == YCSBConfigOrder::Hashed) {
				return util::hash(key);
			}
			return key;
		}

		template<class Generator>
		KeyType get_limited_key(Generator &generator, const KeyType &limit) {
			KeyType res;
			if constexpr (will_limitation_change()) {
				do {
					res = generator.get_next(limit);
				} while (res >= limit);
			}
			else {
				res = generator.get_next();
			}

			return get_hashed_key(res);
		}

		static constexpr bool will_limitation_change() {
			return Config::INSERT_PERCENTAGE > 0;
		}
	};
}
