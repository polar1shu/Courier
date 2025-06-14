/*
 * @author: BL-GS 
 * @date:   2023/2/25
 */

#pragma once

#include <concurrent_control/abstract_concurrent_control.h>
#include <concurrent_control/courier_save/tx_context.h>

namespace cc {

	namespace courier_save {

		template<class CC, class AbKey>
			requires AbstractKeyConcept<AbKey>
		class Executor {
		public:
			using Self        = Executor<CC, AbKey>;

			using CCType      = CC;
			using AbKeyType   = AbKey;
			using KeyType     = AbKey::MainKeyType;

			using ContextType = TxContext<AbKeyType>;

		private:
			CCType *cc_ptr_;

			ContextType context_;

		public:
			template<class ...Args>
			explicit Executor(CCType *cc_ptr, Args &&...args): cc_ptr_(cc_ptr), context_(std::forward<Args>(args)...) {
				context_.message_.start_running();
			}

			Executor(const Executor &other) = default;

			Executor(Executor &&other) noexcept = default;

			~Executor() {
				cc_ptr_->clean_up(context_);
			}

			CCType *get_concurrent_control_ptr() { return cc_ptr_; }

			ContextType &get_context() { return context_; };

		public: // Interface for workload

			/*!
			 * @brief Read operation
			 * @param key Abstract key of data tuple
			 * @return The pointer to data
			 */
			template<class Data>
			const Data *read(const AbKeyType &key) {
				return (Data *)cc_ptr_->read(context_, key);
			}

			/*!
			 * @brief Read operation
			 * Compromise to those ugly implementations to compare with.
			 * @param key Abstract key of data tuple
			 * @param output_ptr Pointer for output
			 * @param size Size to read
			 * @param offset Offset of start position to data tuple
			 * @return Whether reading is successful
			 */
			bool read(const AbKeyType &key, void *output_ptr, size_t size, size_t offset) {
				const uint8_t *data_ptr = (const uint8_t *)cc_ptr_->read(context_, key);
				if (data_ptr == nullptr) [[unlikely]] { return false; }
				std::memcpy(output_ptr, data_ptr + offset, size);
				return true;
			}

			/*!
			 * @brief Update operation
			 * @param key Abstract key of data tuple
			 * @return The pointer of the temp data for writing
			 */
			template<class Data>
			Data *update(const AbKeyType &key) {
				return (Data *)cc_ptr_->update(context_, key);
			}

			/*!
			 * @brief Update operation
			 * @note [Compromise] Actually we should overwrite a whole row when updating
			 * The size and offset here is guaranteed by workload itself.
			 * @param key Abstract key of data tuple
			 * @param size Size to update
			 * @param offset Offset of start position to data tuple
			 * @return The pointer of the temp data for writing
			 */
			template<class Data>
			Data *update(const AbKeyType &key, uint32_t size, uint32_t offset) {
				return (Data *)cc_ptr_->update(context_, key, size, offset);
			}

			/*!
			 * @brief Update operation
			 * @param key Abstract key of data tuple
			 * @param row Data to overwrite(update), which should have been moved by corresponding offset
			 * @param size Size to update
			 * @param offset Offset of start position to data tuple
			 * @return Whether the tuple exists
			 */
			bool update(const AbKeyType &key, const void *row_ptr, uint32_t size, uint32_t offset) {
				uint8_t *data_ptr = (uint8_t *)cc_ptr_->update(context_, key, size, offset);
				if (data_ptr == nullptr) { return false; }
				std::memcpy(data_ptr + offset, (uint8_t *)row_ptr + offset, size);
				return true;
			}

			/*!
			 * @brief Insert operation
			 * @param key Abstract key of data tuple
			 * @param row Data to overwrite(update), which should have been moved by corresponding offset
			 * @param size Size to update
			 * @return If there has been a tuple with the same key, return false
			 */
			bool insert(const AbKeyType &key, const void *row, uint32_t size) {
				return cc_ptr_->insert(context_, key, row, size);
			}

			/*!
			 * @brief Remove operation
			 * @param key Abstract key of data tuple
			 * @return If there isn't a tuple with the same key, return falseI
			 */
			bool remove(const AbKeyType &key) {
				return cc_ptr_->remove(context_, key);
			}

		public:  // Interface for transaction manager

			/*!
			 * @brief Abortion and clean temp info
			 * @return Always true
			 */
			bool commit() {
				return cc_ptr_->commit(context_);
			}

			/*!
			 * @brief Abortion and clean temp info
			 * @return Always true
			 */
			bool abort() {
				return cc_ptr_->abort(context_);
			}

			/*!
			 * @brief Reset context for another execution
			 * @return Always true
			 */
			bool reset() {
				return cc_ptr_->reset(context_);
			}

		};


	}

}
