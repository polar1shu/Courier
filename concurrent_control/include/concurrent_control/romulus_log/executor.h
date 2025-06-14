/*
 * @author: BL-GS 
 * @date:   2023/2/25
 */

#pragma once

#include <concurrent_control/romulus_log/tx_context.h>

namespace cc::romulus {

	template<class CC, class Key, class Field, class Row>
	class Executor {
	public:
		using Self        = Executor<CC, Key, Field, Row>;

		using CCType      = CC;
		using KeyType     = Key;
		using FieldType   = Field;
		using RowType     = Row;

		using ContextType = TxContext;

	private:
		CCType *cc_ptr_;

		ContextType context_;

	public:
		template<class ...Args>
		explicit Executor(CCType *cc_ptr, Args &&...args): cc_ptr_(cc_ptr), context_(std::forward<Args>(args)...) {
			context_.message_.start_running();
		}

		~Executor() {
			cc_ptr_->clean_up(context_);
		}

		CCType *get_concurrent_control_ptr() { return cc_ptr_; }

		ContextType &get_context() { return context_; };

		bool read(KeyType key, FieldType field, RowType *row_ptr) {
			return cc_ptr_->read(context_, key, field, row_ptr);
		}

		bool update(KeyType key, FieldType field, RowType &&row) {
			return cc_ptr_->update(context_, key, field, std::forward<RowType>(row));
		}

		bool scan(KeyType key, FieldType field, uint32_t scan_length, void *row_ptr) {
			return cc_ptr_->scan(context_, key, field, scan_length, row_ptr);
		}

		bool insert(KeyType key, FieldType field, RowType &&row) {
			return cc_ptr_->insert(context_, key, field, std::forward<RowType>(row));
		}

		bool read_modify_write(KeyType key, FieldType field, RowType &&row) {
			return cc_ptr_->update(context_, key, field, std::forward<RowType>(row));
		}

		bool commit() {
			return cc_ptr_->commit(context_);
		}

		bool abort() {
			return cc_ptr_->abort(context_);
		}

		bool reset() {
			return cc_ptr_->reset(context_);
		}
	};
} // namespace cc::romulus
