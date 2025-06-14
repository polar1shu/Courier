/*
 * @author: BL-GS 
 * @date:   2023/1/2
 */

#pragma once

namespace cc::romulus {

	template<class DataType>
	struct alignas(64) DataTuple {
	private:
		using Self = DataTuple<DataType>;

	public:
		alignas(128) DataType data_;

		alignas(128) DataType back_data_;

	public:
		DataTuple() = default;

		explicit DataTuple(const DataType &data) : data_(data), back_data_(data) {}

		DataTuple(const Self &other) = default;

		Self &operator=(const Self &other) = default;

		Self &operator=(const DataType &other_data) { data_ = other_data; }

		DataType &get_data() { return data_; }

		void set_data(DataType data) { data_ = data; }

		DataType *get_main_data_ptr() { return &data_; }

		DataType *get_back_data_ptr() { return &back_data_; }

		void back_up() { memcpy(&back_data_, &data_, sizeof(DataType)); }

	public:
		auto operator<=>(const Self &other) const {
			return data_ <=> other.data_;
		}
	};

	template<class DataType>
	struct alignas(64) IndexTuple {
	private:
		using Self = IndexTuple<DataType>;

		using DataTupleType = DataTuple<DataType>;

	public:
		DataTupleType *data_ptr_;

	public:
		IndexTuple(): data_ptr_(nullptr) {}

		explicit IndexTuple(DataTupleType *data_ptr) : data_ptr_(data_ptr) {}

		IndexTuple(const Self &other) = default;

		IndexTuple(Self &&other) noexcept = default;

		Self &operator=(const Self &other) = default;

		Self &operator=(Self &&other) noexcept = default;

		DataTupleType *get_data_ptr() const { return data_ptr_; }

		DataType *get_main_data_ptr() const { return data_ptr_->get_main_data_ptr(); }

		DataType *get_back_data_ptr() const { return data_ptr_->get_back_data_ptr(); }

		void set_data_ptr(DataTupleType *new_data_ptr) { data_ptr_ = new_data_ptr; }

		DataType &get_data() { return data_ptr_->get_data(); }

		void set_data(const DataType &data) { data_ptr_->set_data(data); }

		void back_up() { data_ptr_->back_up(); }
	};
} // namespace cc::romulus
