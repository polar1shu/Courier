/*
 * @author: BL-GS 
 * @date:   2022/11/30
 */

#pragma once

#include <cstdint>
#include <vector>
#include <variant>
#include <limits>

#include <workload/abstract_transaction.h>
#include <workload/ycsb/ycsb_config.h>

namespace workload {

	template<typename Key, typename Field, typename Row>
	struct YCSBTransactionComponent {
	public:
		static constexpr uint32_t ALL_FIELD = std::numeric_limits<uint32_t>::max();

	public:
		TransactionType     type_;
		Key                 key_;
		Field               field_;
		uint32_t            scan_length_;
		Row                 row_;
		Key                 origin_key_;

	public:
		std::pair<uint32_t, uint32_t> get_offset() {
			return row_.get_size_and_offset(field_);
		}

		static YCSBTransactionComponent get_read_ope(Key key, Field field) {
			return {
				.type_  = TransactionType::Read,
				.key_   = key,
				.field_ = field
			};
		}

		static YCSBTransactionComponent get_scan_ope(Key key, Field field, uint32_t scan_length) {
			return {
				.type_        = TransactionType::Scan,
				.key_         = key,
				.field_       = field,
				.scan_length_ = scan_length
			};
		}

		static YCSBTransactionComponent get_update_ope(Key key, Field field, const Row &row) {
			return {
				.type_  = TransactionType::Update,
				.key_   = key,
				.field_ = field,
				.row_   = row
			};
		}

		static YCSBTransactionComponent get_insert_ope(Key key, const Row &row, Key origin_key) {
			return {
				.type_       = TransactionType::Insert,
				.key_        = key,
				.field_      = ALL_FIELD,
				.row_        = row,
				.origin_key_ = origin_key
			};
		}

		static YCSBTransactionComponent get_read_modify_write_ope(Key key, Field field, const Row &row) {
			return {
					.type_  = TransactionType::ReadModifyWrite,
					.key_   = key,
					.field_ = field,
					.row_   = row
			};
		}
	};

	template<typename Key, typename Field, typename Row, class InsertGenerator>
	class alignas(32) YCSBTransaction: public AbstractTransaction<YCSBTransaction<Key, Field, Row, InsertGenerator>> {
	public:
		using KeyType   = Key;
		using YCSBKey   = AbstractKey<KeyType>;
		using FieldType = Field;
		using RowType   = Row;

		using Self      = YCSBTransaction<KeyType, FieldType, RowType, InsertGenerator>;
		using Component = YCSBTransactionComponent<YCSBKey, FieldType, RowType>;

	private:
		Component ope_;

		InsertGenerator *generator_ptr_;

	public:
		YCSBTransaction(Component &&ope): ope_(std::forward<Component>(ope)) {}

		YCSBTransaction(Component &&ope, InsertGenerator *generator_ptr): ope_(std::forward<Component>(ope)), generator_ptr_(generator_ptr) {}

	public:
		bool is_only_read_impl() const { return ope_.type_ == TransactionType::Read || ope_.type_ == TransactionType::Scan; }

		template<class Executor>
		bool run_impl(Executor &executor) {

			auto [size, offset] = ope_.get_offset();

			switch (ope_.type_) {

				case TransactionType::Read:
					if (!executor.read(
							ope_.key_,
							&ope_.row_,
							size,
							offset
					)) { return false; }
					break;

				case TransactionType::Update:
					if (!executor.update(
							ope_.key_,
							&ope_.row_,
							size,
							offset
					)) { return false; }
					break;

				case TransactionType::Scan:
					for (uint32_t i = 0; i < ope_.scan_length_; ++i) {
						if (!executor.read(
								ope_.key_,
								&ope_.row_,
								size,
								offset
						)) { return false; }
						ope_.key_.logic_key_++;
					}
					break;

				case TransactionType::Insert:
					if (!executor.insert(ope_.key_, &ope_.row_, size)) { return false; }
					generator_ptr_->acknowledge(ope_.origin_key_.logic_key_);
					break;

				case TransactionType::ReadModifyWrite:
				    if (!executor.update(ope_.key_, &ope_.row_, size, offset)) { return false; }
					break;
			}
			return true;
		}

	public:
		//! Add a read operation into transaction.
		//! \param key
		//! \param field
		static Self get_read_tx(const KeyType &key, const FieldType &field) {
			return {
				Component::get_read_ope(YCSBKey{0, key}, field)
			};
		}

		//! Add a update operation into transaction.
		//! \param key
		//! \param field
		//! \param value
		static Self get_update_tx(const KeyType &key, const FieldType &field, const RowType &value) {
			return {
					Component::get_update_ope(YCSBKey{0, key}, field, value)
			};
		}

		//! Add a insert operation into transaction.
		//! \param key
		//! \param field
		//! \param value
		static Self get_insert_tx(const KeyType &key, const Key &origin_key, const RowType &value, InsertGenerator *generator_ptr) {
			return {
					Component::get_insert_ope(YCSBKey{0, key}, value, YCSBKey{0, origin_key}),
					generator_ptr
			};
		}

		//! Add a scan operation into transaction.
		//! \param key
		//! \param field
		static Self get_scan_tx(const KeyType &key, const FieldType &field, uint32_t scan_length) {
			return {
					Component::get_scan_ope(YCSBKey{0, key}, field, scan_length)
			};
		}

		//! Add a scan operation into transaction.
		//! \param key
		//! \param field
		static Self get_read_modify_write_tx(const KeyType &key, const FieldType &field, const RowType &value) {
			return {
					Component::get_read_modify_write_ope(YCSBKey{0, key}, field, value)
			};
		}
	};
}
