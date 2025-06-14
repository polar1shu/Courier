/*
 * @author: BL-GS 
 * @date:   2023/2/28
 */

#pragma once

#include <cstdint>

namespace workload {

	inline namespace ycsb {

		template<uint32_t MAX_LENGTH>
		struct YCSBField {
		public:
			char value_[MAX_LENGTH];

		public:
			YCSBField() = default;

			YCSBField(const YCSBField &other) = default;

			YCSBField &operator= (const YCSBField &other) = default;

			char *get_data() { return value_; }
		};

		template<uint32_t MAX_LENGTH, uint32_t NUM_ELEMENTS>
		struct YCSBRow {
			static constexpr uint32_t ALL_FIELD = std::numeric_limits<uint32_t>::max();

			using ElementType = YCSBField<MAX_LENGTH>;

			using Self        = YCSBRow<MAX_LENGTH, NUM_ELEMENTS>;

		public:
			ElementType elements[NUM_ELEMENTS];

		public:
			YCSBRow() = default;

			YCSBRow(const YCSBRow &other) = default;

			YCSBRow &operator= (const YCSBRow &other) = default;

		public:
			std::pair<uint32_t, uint32_t> get_size_and_offset(uint32_t field) {
				if (field == ALL_FIELD) {
					return {
							sizeof(Self),
							0
					};
				}

				return {
						sizeof(ElementType),
						static_cast<uint32_t>(reinterpret_cast<uint64_t>(elements + field) - reinterpret_cast<uint64_t>(elements))
				};
			}
		};

	}
}
