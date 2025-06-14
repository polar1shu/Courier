/*
 * @author: BL-GS 
 * @date:   2023/1/2
 */

#pragma once

#include <index/abstract_index.h>

#include <index/tbb_hashmap/tbb_hashmap.h>
#include <index/bptree/bptree.h>
#include <index/simple_map/simple_map.h>

namespace ix {

	enum class IndexType {
		HashMap,
		BPTree,
		SimpleMap
	};

	template<IndexType Type, class KeyType, class ValueType>
	struct IndexManager {
		using NodeType = void;
		using Index    = void;
	};

	template<class KeyType, class ValueType>
	struct IndexManager<IndexType::HashMap, KeyType, ValueType> {
		using NodeType = TBBHashMapHeader<KeyType, ValueType>::NodeType;
		using Index    = TBBHashMap<KeyType, ValueType>;

		static_assert(IndexConcept<Index>);
	};

	template<class KeyType, class ValueType>
	struct IndexManager<IndexType::BPTree, KeyType, ValueType> {
		using NodeType = BPTreeHeader<KeyType, ValueType>::NodeType;
		using Index    = BPTree<KeyType, ValueType>;

		static_assert(IndexConcept<Index>);
	};

	template<class KeyType, class ValueType>
	struct IndexManager<IndexType::SimpleMap, KeyType, ValueType> {
		using NodeType = SimpleMapHeader<KeyType, ValueType>::NodeType;
		using Index    = SimpleMap<KeyType, ValueType>;

		static_assert(IndexConcept<Index>);
	};

}
