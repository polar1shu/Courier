/*
 * @author: BL-GS 
 * @date:   2023/1/2
 */

#pragma once

#include <magic_enum.hpp>

#include <thread_allocator/thread_allocator.h>

#include <workload/workload.h>

#include <index/index.h>

#include <log_manager/log_manager.h>

#include <concurrent_control/concurrent_control.h>

#include <transaction_manager/transaction_manager.h>

#include <storage_manager/storage_manager.h>

#include <memory/memory_config.h>

namespace ptm {

	// Retell about configuration enumeration
	using WorkloadType             = workload::WorkloadType;

	using TableScheme              = workload::TableScheme;

	using ConcurrentControlType    = cc::CCKind;

	using TransactionManagerType   = transaction::TransactionManagerType;

	using ThreadBindStrategyType   = transaction::ThreadBindStrategy;

	using IndexType                = storage::IndexType;

	using StorageManagerType       = storage::StorageManagerType;

	// Global Configuration Manager

	template<
			WorkloadType WorkloadTp,
	        IndexType DataIndexTp,
			ConcurrentControlType ConcurrentControlTp,
			TransactionManagerType TransactionManagerTp,
			ThreadBindStrategyType ThreadBindStrategyTp,
			StorageManagerType StorageManagerTp>
	struct GlobalConfigManager {

		using WorkloadType        = workload::WorkloadManager<WorkloadTp>::Workload;

		using KeyType             = WorkloadType::KeyType;

		using DataTupleHeaderType = cc::ConcurrentControlBasicManager<ConcurrentControlTp, WorkloadType>::DataTupleHeaderType;

		using IndexTupleType      = cc::ConcurrentControlBasicManager<ConcurrentControlTp, WorkloadType>::IndexTupleType;

		using VersionHeaderType   = cc::ConcurrentControlBasicManager<ConcurrentControlTp, WorkloadType>::VersionHeaderType;

		using StorageManager      = storage::StorageManagerManager<StorageManagerTp, KeyType, DataTupleHeaderType, IndexTupleType, DataIndexTp, VersionHeaderType>::StorageManager;

		using ConcurrentControl   = cc::ConcurrentControlManager<ConcurrentControlTp, WorkloadType, StorageManager>::ConcurrentControl;

		using TransactionManager  = transaction::TransactionManagerManager<TransactionManagerTp, WorkloadType, ConcurrentControl>::TransactionManager;

		static constexpr auto ThreadBindStrategy = ThreadBindStrategyTp;

		static void print_property() {
			util::print_property("Global Config",
			                      std::make_tuple("KeySize", sizeof(KeyType), ""),
								  std::make_tuple("Workload", magic_enum::enum_name(WorkloadTp), ""),
								  std::make_tuple("PWB", magic_enum::enum_name(get_current_pwb_type()), ""),
								  std::make_tuple("ConcurrentControl", magic_enum::enum_name(ConcurrentControlTp), ""),
								  std::make_tuple("StorageManager", magic_enum::enum_name(StorageManagerTp), ""),
								  std::make_tuple("TransactionManager", magic_enum::enum_name(TransactionManagerTp), ""),
								  std::make_tuple("ThreadBindStrategy", magic_enum::enum_name(ThreadBindStrategyTp), "")
			  );
		}
	};
}
