/*
 * @author: BL-GS 
 * @date:   2023/2/22
 */

#pragma once

#include <cstdint>
#include <cstdio>
#include <numa.h>

#include <arch/arch.h>
#include <spdlog/spdlog.h>

namespace thread {

	struct NUMAConfig {
	public:
		static constexpr int NUMA_NODE_NUM = ARCH_NUMA_NODE_NUM;

	public:
		struct CPUBitmaskWrapper {
			bitmask *cpu_mask_ptr_;

			CPUBitmaskWrapper(int node_id): cpu_mask_ptr_(numa_allocate_cpumask()) {
				numa_node_to_cpus(node_id, cpu_mask_ptr_);
			}

			CPUBitmaskWrapper(CPUBitmaskWrapper &&other) {
				cpu_mask_ptr_ = other.cpu_mask_ptr_;
				other.cpu_mask_ptr_ = nullptr;
			}

			~CPUBitmaskWrapper() {
				if (cpu_mask_ptr_ != nullptr) {
					numa_free_cpumask(cpu_mask_ptr_);
				}
			}

			[[nodiscard]] bool is_set(int cpu_id) const {
				return numa_bitmask_isbitset(cpu_mask_ptr_, cpu_id) != 0;
			}
		};

	public:
		NUMAConfig() {
			if (numa_available() < 0) {
				spdlog::error("NUMA isn't available on this architecture.");
				exit(-1);
			}
		}

		~NUMAConfig() = default;

	public:
		/*!
		 * @brief Bind the current thread to specific numa node.
		 * @param node_id The id of numa node
		 */
		static void bind_node(int node_id) {
			numa_available_warn();

			numa_run_on_node(node_id);
			numa_set_preferred(node_id);
		}

		/*!
		 * @brief Unbind the current thread
		 */
		static void unbind_node() {
			numa_available_warn();

			bitmask mask;
			numa_run_on_node_mask_all(&mask);
		}

	public:
		/*!
		 * @brief Acquire the max number of available nodes in the current system
		 * @return The max number of available nodes.
		 */
		[[nodiscard]] static constexpr int get_max_numa_node() {
			return NUMA_NODE_NUM;
		}

		/*!
		 * @brief Acquire bitmask pointed cpus on specific numa node.
		 */
		static CPUBitmaskWrapper get_cpu_per_node(int node_id) {
			return {node_id};
		}

	private:
		static bool numa_available_warn() {
			if (!numa_available()) {
				spdlog::warn(
						"NUMA is not available in this system. Binding node may incur undefined results."
				);
				return false;
			}
			return true;
		}

	};

	inline static NUMAConfig NUMA_CONFIG;

}
