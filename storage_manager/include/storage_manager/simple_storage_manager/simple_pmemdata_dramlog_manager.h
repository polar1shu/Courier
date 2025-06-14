/*
 * @author: BL-GS 
 * @date:   2023/1/24
 */

#pragma once

#include <data_manager/data_manager.h>
#include <log_manager/log_manager.h>
#include <version_manager/version_manager.h>

#include <storage_manager/simple_storage_manager/simple_storage_manager.h>

namespace storage {

	template<typename DataKey,
			typename TupleHeader,
			typename IndexTuple,
			IndexType DataIndexTp,
			typename VersionHeader>
	requires DataKeyTypeConcept<DataKey>
	class PDDLSimpleManager:
			public 	SimpleMVStorageManagerTemplate<
					typename datam::DataManagerManager<datam::DataManagerKind::PMEM, DataKey, TupleHeader, IndexTuple, DataIndexTp>::DataManager,
					typename logm::LogManagerManager<logm::LogManagerKind::DRAM_TL>::LogManager,
					typename versionm::VersionManagerManager<versionm::VersionManagerKind::PMEM, VersionHeader>::VersionManager
			> {

	public:
		using Self             = PDDLSimpleManager<DataKey, TupleHeader, IndexTuple, DataIndexTp, VersionHeader>;
		using DataManager      = datam::DataManagerManager<datam::DataManagerKind::PMEM, DataKey, TupleHeader, IndexTuple, DataIndexTp>::DataManager;
		using LogManager       = logm::LogManagerManager<logm::LogManagerKind::DRAM_TL>::LogManager;
		using VersionManager   = versionm::VersionManagerManager<versionm::VersionManagerKind::PMEM, VersionHeader>::VersionManager;
		using BaseManager      = SimpleMVStorageManagerTemplate<DataManager, LogManager, VersionManager>;

		/*
		 * Data manager parameters
		 */
		using DataKeyType      = DataManager::DataKeyType;
		using DataTupleHeaderType = DataManager::DataTupleHeaderType;
		using IndexTupleType   = DataManager::IndexTupleType;

		/*
		 * Align Assumed
		 */
		static constexpr size_t DATA_ALLOC_ALIGN_SIZE    = DataManager::DATA_ALLOC_ALIGN_SIZE;
		static constexpr size_t VHEADER_ALLOC_ALIGN_SIZE = DataManager::VHEADER_ALLOC_ALIGN_SIZE;
		static constexpr size_t LOG_ALLOC_ALIGN_SIZE     = LogManager::LOG_ALLOC_ALIGN_SIZE;
	};

	template<typename DataKey,
			typename TupleHeader,
			typename IndexTuple,
			IndexType DataIndexTp>
	requires DataKeyTypeConcept<DataKey>
	class PDDLSimpleManager<DataKey, TupleHeader, IndexTuple, DataIndexTp, void>:
			public SimpleStorageManagerTemplate<
					typename datam::DataManagerManager<datam::DataManagerKind::PMEM, DataKey, TupleHeader, IndexTuple, DataIndexTp>::DataManager,
					typename logm::LogManagerManager<logm::LogManagerKind::DRAM_TL>::LogManager
			> {


	public:
	using Self                 = PDDLSimpleManager<DataKey, TupleHeader, IndexTuple, DataIndexTp, void>;
		using DataManager      = datam::DataManagerManager<datam::DataManagerKind::PMEM, DataKey, TupleHeader, IndexTuple, DataIndexTp>::DataManager;
		using LogManager       = logm::LogManagerManager<logm::LogManagerKind::DRAM_TL>::LogManager;
		using BaseManager      = SimpleStorageManagerTemplate<DataManager, LogManager>;

		/*
		 * Data manager parameters
		 */
		using DataKeyType      = DataManager::DataKeyType;
		using DataTupleHeaderType = DataManager::DataTupleHeaderType;
		using IndexTupleType   = DataManager::IndexTupleType;

		/*
		 * Align Assumed
		 */
		static constexpr size_t DATA_ALLOC_ALIGN_SIZE    = DataManager::DATA_ALLOC_ALIGN_SIZE;
		static constexpr size_t VHEADER_ALLOC_ALIGN_SIZE = DataManager::VHEADER_ALLOC_ALIGN_SIZE;
		static constexpr size_t LOG_ALLOC_ALIGN_SIZE     = LogManager::LOG_ALLOC_ALIGN_SIZE;
	};

}
