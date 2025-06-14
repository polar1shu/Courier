/*
 * @author: BL-GS 
 * @date:   2023/2/26
 */

#pragma once

#include <data_manager/abstract_data_manager.h>

#include <data_manager/simple_data_manager/dram_data_manager.h>
#include <data_manager/simple_data_manager/pmem_data_manager.h>

namespace datam {

	enum class DataManagerKind {
		DRAM,
		PMEM,
		PMEM_NUMA,
	};

	template<DataManagerKind Type, class Key, class TupleHeader, class IndexTuple, IndexType IndexTp>
	struct DataManagerManager {
		using DataManager = void;
	};

	template<class Key, class TupleHeader, class IndexTuple, IndexType IndexTp>
	struct DataManagerManager<DataManagerKind::DRAM, Key, TupleHeader, IndexTuple, IndexTp> {
		using DataManager = DRAMDataManager<Key, TupleHeader, IndexTuple, IndexTp>;

		static_assert(DataManagerConcept<DataManager>);
	};

	template<class Key, class TupleHeader, class IndexTuple, IndexType IndexTp>
	struct DataManagerManager<DataManagerKind::PMEM, Key, TupleHeader, IndexTuple, IndexTp> {
		using DataManager = PMEMDataManager<Key, TupleHeader, IndexTuple, IndexTp>;

		static_assert(DataManagerConcept<DataManager>);
	};

}
