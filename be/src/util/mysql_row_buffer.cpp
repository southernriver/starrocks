// This file is made available under Elastic License 2.0.
// This file is based on code available under the Apache license here:
//   https://github.com/apache/incubator-doris/blob/master/be/src/util/mysql_row_buffer.cpp

// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

#include "util/mysql_row_buffer.h"

#include <fmt/compile.h>
#include <fmt/format.h>
#include <ryu/ryu.h>

#include <cstdio>
#include <type_traits>

#include "common/logging.h"
#include "gutil/strings/fastmem.h"
#include "runtime/large_int_value.h"
#include "util/mysql_global.h"

namespace starrocks {

// the first byte:
// <= 250: length
// = 251: NULL
// = 252: the next two byte is length
// = 253: the next three byte is length
// = 254: the next eighth byte is length
static uint8_t* pack_vlen(uint8_t* packet, uint64_t length) {
    if (length < 251ULL) {
        int1store(packet, length);
        return packet + 1;
    }

    /* 251 is reserved for NULL */
    if (length < 65536ULL) {
        *packet++ = 252;
        int2store(packet, length);
        return packet + 2;
    }

    if (length < 16777216ULL) {
        *packet++ = 253;
        int3store(packet, length);
        return packet + 3;
    }

    *packet++ = 254;
    int8store(packet, length);
    return packet + 8;
}


template <typename DateType>
void MysqlRowBuffer::push_datetime(const DateType& date_value) {
    if (_is_binary_format) {
        char buff[12], *pos;
        size_t length;
        _field_pos++;
        pos = buff + 1;

        int year = 0;
        int month = 0;
        int day = 0;
        int hour = 0;
        int minute = 0;
        int second = 0;
        int microsecond = 0;
        if constexpr (std::is_same_v<DateType, vectorized::DateValue>) {
            date_value.to_date(&year, &month, &day);
        } else {
            date_value.to_timestamp(&year, &month, &day, &hour, &minute, &second, &microsecond);
            int4store(pos + 7, microsecond);
        }

        int2store(pos, year);
        pos[2] = (uchar)month;
        pos[3] = (uchar)day;
        pos[4] = (uchar)hour;
        pos[5] = (uchar)minute;
        pos[6] = (uchar)second;

        if (microsecond > 0) {
            length = 11;
        } else if (hour > 0 || minute > 0 || second > 0) {
            length = 7;
        } else if (year > 0 || month > 0 || day > 0) {
            length = 4;
        } else {
            length = 0;
        }
        buff[0] = (char)length; // Length is stored first
        return _append(buff, length + 1);
    } else {
        std::string s = date_value.to_string();
        push_string(s.data(), s.size());
    }
}

void MysqlRowBuffer::_append(const char* data, int64_t len) {
    char* pos = _resize_extra(len);
    memcpy(pos, data, len);
}

void MysqlRowBuffer::_append_var_string(const char* data, int64_t len) {
    /*
     The +9 comes from that strings of length longer than 16M require
     9 bytes to be stored (see net_store_length).
    */
    char* pos = _resize_extra(len + 9);
    pos = reinterpret_cast<char*>(pack_vlen(reinterpret_cast<uint8_t*>(pos), len));
    memcpy(pos, data, len);
}

void MysqlRowBuffer::start_binary_row(uint32_t num_cols) {
    assert(_is_binary_format);
    int bit_fields = (num_cols + 9) / 8;
    char* pos = _resize_extra(bit_fields + 1);
//    reserve(bit_fields + 1);
    memset(pos, 0, 1 + bit_fields);
//    _pos += bit_fields + 1;
    _field_pos = 0;
}

void MysqlRowBuffer::push_null() {
    if (_is_binary_format) {
        uint offset = (_field_pos + 2) / 8 + 1;
        uint bit = (1 << ((_field_pos + 2) & 7));
        /* Room for this as it's allocated start_binary_row */
        char* pos = _data.data();
        char* to = pos + offset;
        *to = (char)((uchar)*to | (uchar)bit);
        _field_pos++;
    } else {
        if (_array_level == 0) {
            _data.push_back(0xfb);
        } else {
            // lowercase 'null' is more convenient for JSON parsing
            _data.append("null");
        }
    }
}

template <typename T>
void MysqlRowBuffer::push_number(T data) {
    static_assert(std::is_arithmetic_v<T> || std::is_same_v<T, __int128>);
    int length = 0;
    char* end = nullptr;
    char* pos = nullptr;
    const int length_prefix_bytes = _array_level == 0 ? 1 : 0;
    if constexpr (std::is_same_v<T, float>) {
        if (_is_binary_format) {
            char buff[4];
            _field_pos++;
            float4store(buff, data);
            _append(buff, 4);
            return;
        }
        // 1 for length, 1 for sign, other for digits.
        pos = _resize_extra(2 + MAX_FLOAT_STR_LENGTH);
        length = f2s_buffered_n(data, pos + length_prefix_bytes);
    } else if constexpr (std::is_same_v<T, double>) {
        if (_is_binary_format) {
            char buff[8];
            _field_pos++;
            float8store(buff, data);
            _append(buff, 8);
            return;
        }
        // 1 for string trail, 1 for length, 1 for sign, other for digits
        pos = _resize_extra(2 + MAX_DOUBLE_STR_LENGTH);
        length = d2s_buffered_n(data, pos + length_prefix_bytes);
    } else if constexpr (std::is_same_v<std::make_signed_t<T>, int8_t>) {
        if (_is_binary_format) {
            char buff[1];
            _field_pos++;
            int1store(buff, data);
            _append(buff, 1);
            return;
        }
        pos = _resize_extra(2 + MAX_TINYINT_WIDTH);
        end = fmt::format_to(pos + length_prefix_bytes, FMT_COMPILE("{}"), data);
        length = end - pos - length_prefix_bytes;
    } else if constexpr (std::is_same_v<std::make_signed_t<T>, int16_t>) {
        if (_is_binary_format) {
            char buff[2];
            _field_pos++;
            int2store(buff, data);
            _append(buff, 2);
            return;
        }
        pos = _resize_extra(2 + MAX_SMALLINT_WIDTH);
        end = fmt::format_to(pos + length_prefix_bytes, FMT_COMPILE("{}"), data);
        length = end - pos - length_prefix_bytes;
    } else if constexpr (std::is_same_v<std::make_signed_t<T>, int32_t>) {
        if (_is_binary_format) {
            char buff[4];
            _field_pos++;
            int4store(buff, data);
            _append(buff, 4);
            return;
        }
        pos = _resize_extra(2 + MAX_INT_WIDTH);
        end = fmt::format_to(pos + length_prefix_bytes, FMT_COMPILE("{}"), data);
        length = end - pos - length_prefix_bytes;
    } else if constexpr (std::is_same_v<std::make_signed_t<T>, int64_t>) {
        if (_is_binary_format) {
            char buff[8];
            _field_pos++;
            int8store(buff, data);
            _append(buff, 8);
            return;
        }
        pos = _resize_extra(2 + MAX_BIGINT_WIDTH);
        end = fmt::format_to(pos + length_prefix_bytes, FMT_COMPILE("{}"), data);
        length = end - pos - length_prefix_bytes;
    } else if constexpr (std::is_same_v<std::make_signed_t<T>, __int128>) {
        if (_is_binary_format) {
            // large int as type string
            std::string value = LargeIntValue::to_string(data);
            _field_pos++;
            _append_var_string(value.data(), value.size());
            return;
        }
        pos = _resize_extra(2 + 40);
        end = fmt::format_to(pos + length_prefix_bytes, FMT_COMPILE("{}"), data);
        length = end - pos - length_prefix_bytes;
    } else {
        CHECK(false) << "unhandled data type";
    }
    if (length_prefix_bytes > 0) {
        int1store(pos, length);
    }
    pos += length + length_prefix_bytes;
    DCHECK(pos >= _data.data() && pos <= _data.data() + _data.size());
    _data.resize(pos - _data.data());
}

void MysqlRowBuffer::push_string(const char* str, size_t length, char escape_char) {
    if (_array_level == 0) {
        _push_string_normal(str, length);
    } else {
        // Surround the string with two double-quotas.
        const size_t escaped_len = 2 + _length_after_escape(str, length, escape_char);
        char* pos = _resize_extra(escaped_len);
        *pos++ = escape_char;
        if (escaped_len == length + 2) {
            // No '\' or '"' exists in |str|, copy directly.
            strings::memcpy_inlined(pos, str, length);
            pos += length;
        } else {
            // Escape '\' and '"'.
            pos = _escape(pos, str, length, escape_char);
        }
        *pos++ = escape_char;
        DCHECK_EQ(_data.data() + _data.size(), pos);
        _data.resize(pos - _data.data());
    }
}

void MysqlRowBuffer::push_decimal(const Slice& s) {
    if (_array_level == 0) {
        if (_is_binary_format) {
            ++_field_pos;
        }
        _push_string_normal(s.data, s.size);
    } else {
        char* pos = _resize_extra(s.size);
        strings::memcpy_inlined(pos, s.data, s.size);
        pos += s.size;
        DCHECK_EQ(_data.data() + _data.size(), pos);
        _data.resize(pos - _data.data());
    }
}

void MysqlRowBuffer::_enter_scope(char c) {
    if (++_array_level == 1) {
        // Leave one space for storing the string length.
        _data.push_back(0x00);
        _array_offset = _data.size();
    }
    _data.push_back(c);
}

void MysqlRowBuffer::_leave_scope(char c) {
    DCHECK_GT(_array_level, 0);
    _data.push_back(c);
    if (--_array_level == 0) {
        uint64_t curr_scope_len = _data.size() - _array_offset;
        if (curr_scope_len < 251) {
            int1store(&_data[_array_offset - 1], curr_scope_len);
        } else if (curr_scope_len < 65536ULL) {
            _data.resize(_data.size() + 2);
            memmove(_data.data() + _array_offset + 2, _data.data() + _array_offset, curr_scope_len);
            int1store(&_data[_array_offset - 1], 252);
            int2store(&_data[_array_offset], curr_scope_len);
        } else if (curr_scope_len < 16777216ULL) {
            _data.resize(_data.size() + 3);
            memmove(_data.data() + _array_offset + 3, _data.data() + _array_offset, curr_scope_len);
            int1store(&_data[_array_offset - 1], 253);
            int3store(&_data[_array_offset], curr_scope_len);
        } else {
            _data.resize(_data.size() + 8);
            memmove(_data.data() + _array_offset + 8, _data.data() + _array_offset, curr_scope_len);
            int1store(&_data[_array_offset - 1], 254);
            int8store(&_data[_array_offset], curr_scope_len);
        }
    }
}

void MysqlRowBuffer::separator(char c) {
    DCHECK_GT(_array_level, 0);
    _data.push_back(c);
}

size_t MysqlRowBuffer::_length_after_escape(const char* str, size_t length, char escape_char) {
    size_t new_len = length;
    for (size_t i = 0; i < length; i++) {
        new_len += ((str[i] == escape_char) | (str[i] == '\\'));
        //                         ^^ use '|' or instead of '||' intentionally.
    }
    return new_len;
}

char* MysqlRowBuffer::_escape(char* dst, const char* src, size_t length, char escape_char) {
    for (size_t i = 0; i < length; i++) {
        char c = src[i];
        if (c == escape_char) {
            *dst++ = '\\';
            *dst++ = escape_char;
        } else if (c == '\\') {
            *dst++ = '\\';
            *dst++ = '\\';
        } else {
            *dst++ = c;
        }
    }
    return dst;
}

void MysqlRowBuffer::_push_string_normal(const char* str, size_t length) {
    if (_is_binary_format) {
        ++_field_pos;
    }
    char* pos = _resize_extra(9 + length);
    pos = reinterpret_cast<char*>(pack_vlen(reinterpret_cast<uint8_t*>(pos), length));
    strings::memcpy_inlined(pos, str, length);
    pos += length;
    DCHECK(pos >= _data.data() && pos <= _data.data() + _data.size());
    _data.resize(pos - _data.data());
}

template void MysqlRowBuffer::push_number<int8_t>(int8_t);
template void MysqlRowBuffer::push_number<int16_t>(int16_t);
template void MysqlRowBuffer::push_number<int32_t>(int32_t);
template void MysqlRowBuffer::push_number<int64_t>(int64_t);
template void MysqlRowBuffer::push_number<uint8_t>(uint8_t);
template void MysqlRowBuffer::push_number<uint16_t>(uint16_t);
template void MysqlRowBuffer::push_number<uint32_t>(uint32_t);
template void MysqlRowBuffer::push_number<uint64_t>(uint64_t);
template void MysqlRowBuffer::push_number<__int128>(__int128);
template void MysqlRowBuffer::push_number<float>(float);
template void MysqlRowBuffer::push_number<double>(double);
template void MysqlRowBuffer::push_datetime<vectorized::DateValue>(const vectorized::DateValue&);
template void MysqlRowBuffer::push_datetime<vectorized::TimestampValue>(const vectorized::TimestampValue&);

} // namespace starrocks

/* vim: set ts=4 sw=4 sts=4 tw=100 */
