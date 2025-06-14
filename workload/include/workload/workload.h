/*
 * @author: BL-GS 
 * @date:   2023/2/23
 */

#pragma once

#include <workload/abstract_workload.h>
#include <workload/ycsb/ycsb.h>
#include <workload/tpcc/tpcc.h>
#include <workload/smallbank/smallbank.h>

namespace workload {

	enum class WorkloadType {
		YCSB_A,
		YCSB_B,
		YCSB_C,
		YCSB_D,
		YCSB_E,
		YCSB_F,
		TPCC,
		TPCC_Light,
		TPCC_Fat,
		ReadMostly,
		WriteIntensive,
		WriteMostly,
		SmallBank
	};


	template<WorkloadType workloadTp>
	struct WorkloadManager {};

	template<>
	struct WorkloadManager<WorkloadType::YCSB_A> {
		using WorkloadConfig = YCSBConfig<YCSBConfigType::A>;

		using Workload       = YCSB<WorkloadConfig>;

		static_assert(WorkloadConcept<Workload>);
	};

	template<>
	struct WorkloadManager<WorkloadType::YCSB_B> {
		using WorkloadConfig = YCSBConfig<YCSBConfigType::B>;

		using Workload       = YCSB<WorkloadConfig>;

		static_assert(WorkloadConcept<Workload>);
	};

	template<>
	struct WorkloadManager<WorkloadType::YCSB_C> {
		using WorkloadConfig = YCSBConfig<YCSBConfigType::C>;

		using Workload       = YCSB<WorkloadConfig>;

		static_assert(WorkloadConcept<Workload>);
	};

	template<>
	struct WorkloadManager<WorkloadType::YCSB_D> {
		using WorkloadConfig = YCSBConfig<YCSBConfigType::D>;

		using Workload       = YCSB<WorkloadConfig>;

		static_assert(WorkloadConcept<Workload>);
	};

	template<>
	struct WorkloadManager<WorkloadType::YCSB_E> {
		using WorkloadConfig = YCSBConfig<YCSBConfigType::E>;

		using Workload       = YCSB<WorkloadConfig>;

		static_assert(WorkloadConcept<Workload>);
	};

	template<>
	struct WorkloadManager<WorkloadType::YCSB_F> {
		using WorkloadConfig = YCSBConfig<YCSBConfigType::F>;

		using Workload       = YCSB<WorkloadConfig>;

		static_assert(WorkloadConcept<Workload>);
	};

	template<>
	struct WorkloadManager<WorkloadType::TPCC> {
		using WorkloadConfig = TPCCConfig<TPCCConfigType::Normal>;

		using Workload       = TPCC<WorkloadConfig>;

		static_assert(WorkloadConcept<Workload>);
	};

	template<>
	struct WorkloadManager<WorkloadType::TPCC_Light> {
		using WorkloadConfig = TPCCConfig<TPCCConfigType::Light>;

		using Workload       = TPCC<WorkloadConfig>;

		static_assert(WorkloadConcept<Workload>);
	};

	template<>
	struct WorkloadManager<WorkloadType::TPCC_Fat> {
		using WorkloadConfig = TPCCConfig<TPCCConfigType::Fat>;

		using Workload       = TPCC<WorkloadConfig>;

		static_assert(WorkloadConcept<Workload>);
	};

	template<>
	struct WorkloadManager<WorkloadType::ReadMostly> {
		using WorkloadConfig = YCSBConfig<YCSBConfigType::ReadMostly>;

		using Workload       = YCSB<WorkloadConfig>;

		static_assert(WorkloadConcept<Workload>);
	};

	template<>
	struct WorkloadManager<WorkloadType::WriteIntensive> {
		using WorkloadConfig = YCSBConfig<YCSBConfigType::WriteIntensive>;

		using Workload       = YCSB<WorkloadConfig>;

		static_assert(WorkloadConcept<Workload>);
	};

	template<>
	struct WorkloadManager<WorkloadType::WriteMostly> {
		using WorkloadConfig = YCSBConfig<YCSBConfigType::WriteMostly>;

		using Workload       = YCSB<WorkloadConfig>;

		static_assert(WorkloadConcept<Workload>);
	};

	template<>
	struct WorkloadManager<WorkloadType::SmallBank> {
		using WorkloadConfig = SmallBankConfig<SmallBankConfigType::Normal>;

		using Workload       = SmallBank<WorkloadConfig>;

		static_assert(WorkloadConcept<Workload>);
	};
}
