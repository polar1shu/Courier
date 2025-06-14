/*
 * @author: BL-GS 
 * @date:   2023/1/1
 */

#pragma once

#include <cstdint>
#include <spdlog/spdlog.h>

#include <tbb/concurrent_hash_map.h>


#include <index/index_storage.h>
#include <index/abstract_index.h>

namespace ix {

	inline namespace hashmap {

		template<class Key, class Value>
		class TBBHashMapHeader {
		public:
			using KeyType           = Key;
			using ValueType         = Value;
			using NodeType          = Value;
		};

		template<class Key, class Value>
		requires KeyConcept<Key>
		         && ValueConcept<Value>
		class TBBHashMap {
		public:
			using Self              = TBBHashMap<Key, Value>;

			using KeyType           = Key;
			using ValueType         = Value;

			using MapType           = tbb::concurrent_hash_map<Key, Value>;
			using AccessorType      = MapType::accessor;
			using ConstAccessorType = MapType::const_accessor;
			using MapValue          = MapType::value_type;

		private:
			MapType hashmap_;

		public:
			TBBHashMap(uint32_t tuple_size, uint32_t expected_amount) {
				spdlog::warn(
						"Index TBBHashmap is laid without control of allocator."
				);
			}

		public:
			template<class ...Args>
			bool insert(Key new_key, Args &&...args) {
				AccessorType accessor;
				if (!hashmap_.insert(accessor, new_key)) { return false; }
				accessor->second = Value(std::forward<Args>(args)...);
				return true;
			}

			bool remove(Key key) {
				return hashmap_.erase(key);
			}

			bool read(Key key, Value &data) {
				ConstAccessorType accessor;
				if (!hashmap_.find(accessor, key)) { return false; }
				data = accessor->second;
				return true;
			}

			template<class ...Args>
			bool update(Key key, Args && ...args) {
				AccessorType accessor;
				if (!hashmap_.find(accessor, key)) { return false; }
				accessor->second = Value(std::forward<Args &&>(args)...);
				return true;
			}

			bool contain(Key key) {
				ConstAccessorType accessor;
				return hashmap_.find(accessor, key);
			}

			void clear(std::function<void(const Value &)> &func) {
				std::for_each(hashmap_.begin(), hashmap_.end(), [&](const auto &item) {
					func(item.second);
				});
				hashmap_.clear();
			}

			uint32_t size() const {
				return hashmap_.size();
			}
		};

	}

}
