// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.

#include "formats/csv/numeric_converter.h"

#include "column/fixed_length_column.h"
#include "common/logging.h"
#include "util/string_parser.hpp"

namespace starrocks::vectorized::csv {

template <typename T>
Status NumericConverter<T>::write_string(OutputStream* os, const Column& column, size_t row_num,
                                         const Options& options) const {
    auto numeric_column = down_cast<const FixedLengthColumn<DataType>*>(&column);
    if constexpr (std::is_same_v<int8_t, DataType>) {
        return os->write<int16_t>(numeric_column->get_data()[row_num]);
    } else {
        return os->write(numeric_column->get_data()[row_num]);
    }
}

template <typename T>
Status NumericConverter<T>::write_quoted_string(OutputStream* os, const Column& column, size_t row_num,
                                                const Options& options) const {
    return write_string(os, column, row_num, options);
}

template <typename T>
bool NumericConverter<T>::read_string(Column* column, Slice s, const Options& options) const {
    StringParser::ParseResult r;
    DataType v = StringParser::string_to_int<DataType>(s.data, s.size, &r);
    if (r == StringParser::PARSE_SUCCESS) {
        down_cast<FixedLengthColumn<DataType>*>(column)->append(v);
        return true;
    } else if (r != StringParser::PARSE_OVERFLOW && r != StringParser::PARSE_UNDERFLOW) {
        if constexpr (sizeof(DataType) <= sizeof(int32_t)) {
            double d = StringParser::string_to_float<double>(s.data, s.size, &r);
            if (r == StringParser::PARSE_SUCCESS) {
                d = std::trunc(d);
                // Implicit cast.
                // NOTE: this behavior is consistent with the cast expression of StarRocks but different
                // from MySQL.
                DataType n = implicit_cast<DataType>(d);
                // Check overflow/underflow.
                if (implicit_cast<double>(n) != d) {
                    return false;
                } else {
                    down_cast<FixedLengthColumn<DataType>*>(column)->append(n);
                    return true;
                }
            } else {
                return false;
            }
        } else {
            DecimalV2Value decimal;
            if (decimal.parse_from_str(s.data, s.size) != 0) {
                return false;
            } else {
                int64_t n = decimal.int_value();
                down_cast<FixedLengthColumn<DataType>*>(column)->append(implicit_cast<DataType>(n));
                return true;
            }
        }
    } else {
        return false;
    }
}

template <typename T>
bool NumericConverter<T>::read_quoted_string(Column* column, Slice s, const Options& options) const {
    return read_string(column, s, options);
}

/// Explicit template instantiations
template class NumericConverter<int8_t>;
template class NumericConverter<int16_t>;
template class NumericConverter<int32_t>;
template class NumericConverter<int64_t>;
template class NumericConverter<int128_t>;

} // namespace starrocks::vectorized::csv
