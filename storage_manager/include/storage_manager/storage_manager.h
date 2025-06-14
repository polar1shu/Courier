/*
 * @author: BL-GS 
 * @date:   2023/1/25
 */

#pragma once

#include <index/abstract_index.h>

#include <storage_manager/abstract_storage_manager.h>

#include <storage_manager/simple_storage_manager/random_dram_manager.h>
#include <storage_manager/simple_storage_manager/random_pmemdata_dramlog_manager.h>
#include <storage_manager/simple_storage_manager/simple_pmemdata_dramlog_manager.h>
#include <storage_manager/simple_storage_manager/simple_pmemdata_pmemlog_manager.h>

namespace storage {

	enum class StorageManagerType {
		Random_DRAM,
		Random_PMEMDATA_DRAMLOG,
		Simple_PMEMDATA_DRAMLOG,
		Simple_PMEMDATA_PMEMLOG
	};

	template<StorageManagerType SMT,
			typename DataKey, typename DataTupleHeader,
			typename IndexTuple, IndexType DataIndexTp,
			typename VersionType>
	struct StorageManagerManager {
		using StorageManager = DRAMRandomManager<DataKey, DataTupleHeader, IndexTuple, DataIndexTp, VersionType>;

		static_assert(StorageManagerConcept<StorageManager>);
	};

	template<
			typename DataKey, typename DataTupleHeader,
			typename IndexTuple, IndexType DataIndexTp,
			typename VersionType>
	struct StorageManagerManager<StorageManagerType::Random_DRAM,
			DataKey, DataTupleHeader, IndexTuple, DataIndexTp,
			VersionType> {
		using StorageManager = DRAMRandomManager<DataKey, DataTupleHeader, IndexTuple, DataIndexTp, VersionType>;

		static_assert(StorageManagerConcept<StorageManager>);
	};

	template<
			typename DataKey, typename IndexTuple, typename DataTupleHeader, IndexType DataIndexTp,
			typename VersionType>
	struct StorageManagerManager<StorageManagerType::Random_PMEMDATA_DRAMLOG,
			DataKey, DataTupleHeader, IndexTuple, DataIndexTp,
			VersionType> {
		using StorageManager = PDDLRandomManager<DataKey, DataTupleHeader, IndexTuple, DataIndexTp, VersionType>;

		static_assert(StorageManagerConcept<StorageManager>);
	};

	template<
			typename DataKey, typename IndexTuple, typename DataTupleHeader, IndexType DataIndexTp,
			typename VersionType>
	struct StorageManagerManager<StorageManagerType::Simple_PMEMDATA_DRAMLOG,
			DataKey, DataTupleHeader, IndexTuple, DataIndexTp,
			VersionType> {
		using StorageManager = PDDLSimpleManager<DataKey, DataTupleHeader, IndexTuple, DataIndexTp, VersionType>;

		static_assert(StorageManagerConcept<StorageManager>);
	};

	template<
			typename DataKey, typename IndexTuple, typename DataTupleHeader, IndexType DataIndexTp,
			typename VersionType>
	struct StorageManagerManager<StorageManagerType::Simple_PMEMDATA_PMEMLOG,
			DataKey, DataTupleHeader, IndexTuple, DataIndexTp,
			VersionType> {
		using StorageManager = PDPLSimpleManager<DataKey, DataTupleHeader, IndexTuple, DataIndexTp, VersionType>;

		static_assert(StorageManagerConcept<StorageManager>);
	};

}
