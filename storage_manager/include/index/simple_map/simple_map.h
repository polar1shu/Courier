/*
 * @author: BL-GS 
 * @date:   2023/3/9
 */

#pragma once

#include <cstdint>
#include <cstring>
#include <iostream>
#include <unordered_map>
#include <mutex>

#include <index/abstract_index.h>
#include <index/index_storage.h>

namespace ix {

	inline namespace smap {

		template<class Key, class Value>
		class SimpleMapHeader {
		public:
			using KeyType           = Key;
			using ValueType         = Value;
			using NodeType          = Value;
		};

		template<class Key, class Value>
		requires KeyConcept<Key>
		         && ValueConcept<Value>
		class SimpleMap {
		public:
			using Self              = SimpleMap<Key, Value>;

			using KeyType           = Key;
			using ValueType         = Value;
			using Allocator         = IndexStorage<IndexStorageKind::DRAM>;

		private:
			std::mutex global_mutex_;

			std::unordered_map<KeyType, ValueType> inner_tree;

		public:
			SimpleMap(uint32_t tuple_size, uint32_t expected_amount) {
				spdlog::warn(
						"Index SimpleMap is laid without control of allocator."
				);
			}

		public:
			template<class ...Args>
			bool insert(const KeyType &new_key, Args &&...args) {
				std::lock_guard<std::mutex> lock(global_mutex_);
				return inner_tree.try_emplace(new_key, std::forward<Args>(args)...).second;
			}

			bool remove(const KeyType &key) {
				std::lock_guard<std::mutex> lock(global_mutex_);	
				auto iter = inner_tree.find(key);
				if (iter == inner_tree.end()) { return false; }
				inner_tree.erase(iter);
				return true;
			}

			bool read(const KeyType &key, ValueType &data) {
				auto iter = inner_tree.find(key);
				if (iter == inner_tree.end()) { return false; }
				data = iter->second;
				return true;
			}

			template<class ...Args>
			bool update(const KeyType &key, Args && ...args) {
				std::lock_guard<std::mutex> lock(global_mutex_);
				auto iter = inner_tree.find(key);
				if (iter == inner_tree.end()) { return false; }
				iter->second = ValueType{std::forward<Args>(args)...};
				return true;
			}

			bool contain(const KeyType &key) {
				const void *res = inner_tree.btree_search(key);
				return res != nullptr;
			}

			void clear(std::function<void(const Value &)> &func) {
				for (auto &item: inner_tree) {
					func(item.second);
				}
				inner_tree.clear();
			}

			uint32_t size() const {
				return inner_tree.size();
			}
		};

	}

}
