/*
 * @author: BL-GS 
 * @date:   2023/2/26
 */

#pragma once

#include <mem_allocator/mem_allocator.h>

#include <data_manager/abstract_data_manager.h>
#include <data_manager/simple_data_manager/simple_data_manager.h>

namespace datam {

	template<class Key, class TupleHeader, class IndexTuple, IndexType IndexTp>
	struct PMEMDataManagerBasic {
		using DataAllocator = allocator::SimplePmemAllocator;

		using DataIndex = ix::IndexManager<IndexTp, Key, IndexTuple>::Index;
	};

	template<class Key, class TupleHeader, class IndexTuple, IndexType IndexTp>
	class PMEMDataManager : public SimpleDataManagerTemplate<
			Key, TupleHeader,
			IndexTuple,
			typename PMEMDataManagerBasic<Key, TupleHeader, IndexTuple, IndexTp>::DataIndex,
			typename PMEMDataManagerBasic<Key, TupleHeader, IndexTuple, IndexTp>::DataAllocator
	>   {
	public:
		using BaseType      = SimpleDataManagerTemplate<
				Key, TupleHeader,
				IndexTuple,
				typename PMEMDataManagerBasic<Key, TupleHeader, IndexTuple, IndexTp>::DataIndex,
				typename PMEMDataManagerBasic<Key, TupleHeader, IndexTuple, IndexTp>::DataAllocator
		>;

		using DataKeyType         = Key;
		using DataTupleHeaderType = TupleHeader;
		using IndexTupleType      = IndexTuple;

		static constexpr StorageOrder DATA_STORAGE_ORDER = StorageOrder::Sequential;

		static constexpr StorageOrder get_data_storage_order() { return DATA_STORAGE_ORDER; };

		static constexpr StorageMemType DATA_STORAGE_MEM_TYPE = StorageMemType::PMEM;

		static constexpr StorageMemType get_data_storage_memtype() { return DATA_STORAGE_MEM_TYPE; };

		using DataAllocator = allocator::SimplePmemAllocator;
		static_assert(allocator::MemAllocatorConcept<DataAllocator>);

		static constexpr size_t ALLOC_ALIGN_SIZE = DataAllocator::ALLOC_ALIGN_SIZE;

		static constexpr StorageControlHeader get_data_storage_control_header() { return DataAllocator::get_header(); }

		/*
		 * Index Configuration
		 */

		using DataIndexNodeType = ix::IndexManager<IndexTp, DataKeyType, IndexTupleType>::NodeType;

		using DataIndex         = ix::IndexManager<IndexTp, DataKeyType, IndexTupleType>::Index;

		static_assert(ix::IndexConcept<DataIndex>);

		/*
		 * Align Assumed
		 */
		static constexpr size_t VHEADER_ALLOC_ALIGN_SIZE = BaseType::VHEADER_ALLOC_ALIGN_SIZE;

		static constexpr size_t DATA_ALLOC_ALIGN_SIZE    = BaseType::DATA_ALLOC_ALIGN_SIZE;

		PMEMDataManager(size_t tuple_size, size_t expected_amount): BaseType(tuple_size, expected_amount) {}

	};

}
