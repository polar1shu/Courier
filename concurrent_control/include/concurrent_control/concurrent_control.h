/*
 * @author: BL-GS 
 * @date:   2022/12/31
 */

#pragma once

#include <concurrent_control/abstract_concurrent_control.h>
#include <concurrent_control/config.h>

#include <concurrent_control/occ/occ.h>
//#include <concurrent_control/occ_numa/occ.h>
#include <concurrent_control/tictoc/tictoc.h>
#include <concurrent_control/mvcc/mvcc.h>
#include <concurrent_control/romulus_log/romulus_log.h>
#include <concurrent_control/tpl/tpl.h>
#include <concurrent_control/sp3/sp3.h>
#include <concurrent_control/courier/courier.h>
#include <concurrent_control/courier_save/courier.h>

namespace cc {
	enum class CCKind {
		OCC,
//		OCC_NUMA,
		TICTOC,
		MVCC,
		ROMULUSLOG,
		TPL,
		SP,
		COURIER,
		COURIER_SAVE
	};

	template<CCKind Type, class Workload>
	struct ConcurrentControlBasicManager {
		using DataTupleHeaderType = void;
		using IndexTupleType      = void;
		using VersionHeaderType   = void;
	};

	template<CCKind Type, class Workload, class StorageManager>
	struct ConcurrentControlManager {
		using ConcurrentControl = void;
	};

	template<class Workload>
	struct ConcurrentControlBasicManager<CCKind::OCC, Workload> {
		using DataTupleHeaderType = occ::OCCBasic<Workload>::DataTupleHeaderType;
		using IndexTupleType      = occ::OCCBasic<Workload>::IndexTupleType;
		using VersionHeaderType   = void;
	};

	template<class Workload, class StorageManager>
	struct ConcurrentControlManager<CCKind::OCC, Workload, StorageManager> {
		using ConcurrentControl = occ::OCC<Workload, StorageManager>;

		static_assert(CCConcept<ConcurrentControl>);
	};

//	template<class Workload>
//	struct ConcurrentControlBasicManager<CCKind::OCC_NUMA, Workload> {
//		using DataTupleHeaderType = occ_numa::OCCNUMABasic<Workload>::DataTupleHeaderType;
//		using IndexTupleType      = occ_numa::OCCNUMABasic<Workload>::IndexTupleType;
//		using VersionHeaderType   = void;
//	};
//
//	template<class Workload, class StorageManager>
//	struct ConcurrentControlManager<CCKind::OCC_NUMA, Workload, StorageManager> {
//		using ConcurrentControl = occ_numa::OCCNUMA<Workload, StorageManager>;
//
//		static_assert(CCConcept<ConcurrentControl>);
//	};

	template<class Workload>
	struct ConcurrentControlBasicManager<CCKind::TICTOC, Workload> {
		using DataTupleHeaderType = tictoc::TicTocBasic<Workload>::DataTupleHeaderType;
		using IndexTupleType      = tictoc::TicTocBasic<Workload>::IndexTupleType;
		using VersionHeaderType   = void;
	};

	template<class Workload, class StorageManager>
	struct ConcurrentControlManager<CCKind::TICTOC, Workload, StorageManager> {
		using ConcurrentControl = tictoc::TicToc<Workload, StorageManager>;

		static_assert(CCConcept<ConcurrentControl>);
	};

	template<class Workload>
	struct ConcurrentControlBasicManager<CCKind::MVCC, Workload> {
		using DataTupleHeaderType = mvcc::MVCCBasic<Workload>::DataTupleHeaderType;
		using IndexTupleType      = mvcc::MVCCBasic<Workload>::IndexTupleType;
		using VersionHeaderType   = mvcc::MVCCBasic<Workload>::VersionType;
	};

	template<class Workload, class StorageManager>
	struct ConcurrentControlManager<CCKind::MVCC, Workload, StorageManager> {
		using ConcurrentControl = mvcc::MVCC<Workload, StorageManager>;

		static_assert(CCConcept<ConcurrentControl>);
	};

	template<class Workload>
	struct ConcurrentControlBasicManager<CCKind::ROMULUSLOG, Workload> {
		using DataType       = romulus::RomulusLogBasic<Workload>::DataType;
		using IndexTupleType = romulus::RomulusLogBasic<Workload>::IndexTupleType;
	};

	template<class Workload, class StorageManager>
	struct ConcurrentControlManager<CCKind::ROMULUSLOG, Workload, StorageManager> {
		using ConcurrentControl = romulus::RomulusLog<Workload, StorageManager>;

		static_assert(CCConcept<ConcurrentControl>);
	};

	template<class Workload>
	struct ConcurrentControlBasicManager<CCKind::TPL, Workload> {
		using DataTupleHeaderType = tpl::TPLBasic<Workload>::DataTupleHeaderType;
		using IndexTupleType      = tpl::TPLBasic<Workload>::IndexTupleType;
		using VersionHeaderType   = void;
	};

	template<class Workload, class StorageManager>
	struct ConcurrentControlManager<CCKind::TPL, Workload, StorageManager> {
		using ConcurrentControl = tpl::TPL<Workload, StorageManager>;

		static_assert(CCConcept<ConcurrentControl>);
	};

	template<class Workload>
	struct ConcurrentControlBasicManager<CCKind::SP, Workload> {
		using DataTupleHeaderType = sp::SPBasic<Workload>::DataTupleHeaderType;
		using IndexTupleType      = sp::SPBasic<Workload>::IndexTupleType;
		using VersionHeaderType   = void;
	};

	template<class Workload, class StorageManager>
	struct ConcurrentControlManager<CCKind::SP, Workload, StorageManager> {
		using ConcurrentControl = sp::SP<Workload, StorageManager>;

		static_assert(CCConcept<ConcurrentControl>);
	};


	template<class Workload>
	struct ConcurrentControlBasicManager<CCKind::COURIER, Workload> {
		using DataTupleHeaderType = courier::CourierBasic<Workload>::DataTupleHeaderType;
		using IndexTupleType      = courier::CourierBasic<Workload>::IndexTupleType;
		using VersionHeaderType   = void;
	};

	template<class Workload, class StorageManager>
	struct ConcurrentControlManager<CCKind::COURIER, Workload, StorageManager> {
		using ConcurrentControl = courier::Courier<Workload, StorageManager>;

		static_assert(CCConcept<ConcurrentControl>);
	};

	template<class Workload>
	struct ConcurrentControlBasicManager<CCKind::COURIER_SAVE, Workload> {
		using DataTupleHeaderType = courier_save::CourierSaveBasic<Workload>::DataTupleHeaderType;
		using IndexTupleType      = courier_save::CourierSaveBasic<Workload>::IndexTupleType;
		using VersionHeaderType   = void;
	};

	template<class Workload, class StorageManager>
	struct ConcurrentControlManager<CCKind::COURIER_SAVE, Workload, StorageManager> {
		using ConcurrentControl = courier_save::CourierSave<Workload, StorageManager>;

		static_assert(CCConcept<ConcurrentControl>);
	};

}

