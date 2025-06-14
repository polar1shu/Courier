/*
 * @author: BL-GS 
 * @date:   2023/3/22
 */

#pragma once

#include <recovery/noreserve/noreserve_recovery.h>
#include <recovery/reserve/reserve_recovery.h>

namespace recovery {

	enum class RecoveryType {
		NoReserve,
		Reserve
	};

	template<RecoveryType recoveryTp, class StorageManager, class Key>
	struct RecoveryManager {};

	template<class StorageManager, class Key>
	struct RecoveryManager<RecoveryType::NoReserve, StorageManager, Key> {
		using Recovery = NoReserveRecovery<StorageManager, Key>;
	};

	template<class StorageManager, class Key>
	struct RecoveryManager<RecoveryType::Reserve, StorageManager, Key> {
		using Recovery = ReserveRecovery<StorageManager, Key>;
	};
}
