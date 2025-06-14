/*
 * @author: BL-GS 
 * @date:   2023/3/9
 */

#pragma once

#include <cstdint>
#include <iostream>

#include <spdlog/spdlog.h>
#include <thread/thread.h>

#include <index/abstract_index.h>
#include <index/index_storage.h>
#include <index/bptree/btree.h>

namespace ix {

	inline namespace bptree {

		template<class Key, class Value>
		class BPTreeHeader {
		public:
			using KeyType           = Key;
			using ValueType         = Value;
			using NodeType          = Value;
		};

		template<class Key, class Value>
		requires KeyConcept<Key>
		         && ValueConcept<Value>
		class BPTree {
		public:
			using Self              = BPTree<Key, Value>;

			using KeyType           = Key;
			using ValueType         = Value;
			using Allocator         = IndexStorage<IndexStorageKind::DRAM>;

		private:
			Allocator value_allocator_;

			btree inner_tree;

		public:
			BPTree(uint32_t tuple_size, uint32_t expected_amount):
					value_allocator_(sizeof(Value), expected_amount),
					inner_tree([this](){
						thread::THREAD_CONTEXT.allocate_tid();
						page *res = (page *) new uint8_t[sizeof(page)];
						thread::THREAD_CONTEXT.deallocate_tid();
						return res;
					}) {
				spdlog::warn(
						"Index BPTree hasn't implemented clearing, which may lead to memory leak."
				);
			}

		public:
			template<class ...Args>
			bool insert(const KeyType &new_key, Args &&...args) {
				auto res = value_allocator_.allocate(sizeof(ValueType));
				new(res) ValueType {std::forward<Args>(args)...};

				inner_tree.btree_insert(new_key, (char *)res, [](){
					return (page *) new uint8_t[sizeof(page)];
				});
				return true;
			}

			bool remove([[maybe_unused]] const KeyType &key) {
				return true;
			}

			bool read(const KeyType &key, Value &data) {
				const void *res = inner_tree.btree_search(key);
				if (res == nullptr) { return false; }
				std::memcpy(&data, res, sizeof(ValueType));
				return true;
			}

			template<class ...Args>
			bool update(const KeyType &key, Args && ...args) {
				ValueType temp_value{std::forward<Args>(args)...};
				void *res = inner_tree.btree_search(key);
				if (res == nullptr) { return false; }
				std::memcpy(res, &temp_value, sizeof(ValueType));
				return true;
			}

			bool contain(const KeyType &key) {
				const void *res = inner_tree.btree_search(key);
				return res != nullptr;
			}

			void clear([[maybe_unused]] std::function<void(const Value &)> &func) {
			}

			uint32_t size() const {
				return 0;
			}
		};

	}

}
