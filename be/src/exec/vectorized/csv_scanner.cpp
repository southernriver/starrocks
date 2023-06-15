// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.

#include "exec/vectorized/csv_scanner.h"

#include "column/adaptive_nullable_column.h"
#include "column/chunk.h"
#include "column/column_helper.h"
#include "column/hash_set.h"
#include "fs/fs.h"
#include "gutil/strings/substitute.h"
#include "runtime/runtime_state.h"
#include "util/utf8_check.h"

namespace starrocks::vectorized {

Status CSVScanner::ScannerCSVReader::_fill_buffer() {
    SCOPED_RAW_TIMER(&_counter->file_read_ns);

    DCHECK(_buff.free_space() > 0);
    Slice s(_buff.limit(), _buff.free_space());
    auto res = _file->read(s.data, s.size);
    // According to the specification of `FileSystem::read`, when reached the end of
    // a file, the returned status will be OK instead of EOF, but here we check
    // EOF also for safety.
    if (res.status().is_end_of_file()) {
        s.size = 0;
    } else if (!res.ok()) {
        return res.status();
    } else {
        s.size = *res;
    }
    _buff.add_limit(s.size);
    auto n = _buff.available();
    if (s.size == 0) {
        if (n == 0) {
            // Has reached the end of file and the buffer is empty.
            return Status::EndOfFile(_file->filename());
        } else if (n < _row_delimiter_length || _buff.find(_row_delimiter, n - _row_delimiter_length) == nullptr) {
            // Has reached the end of file but still no record delimiter found, which
            // is valid, according the RFC, add the record delimiter ourself.
            for (char ch : _row_delimiter) {
                _buff.append(ch);
            }
        }
    }
    return Status::OK();
}

CSVScanner::CSVScanner(RuntimeState* state, RuntimeProfile* profile, const TBrokerScanRange& scan_range,
                       ScannerCounter* counter)
        : FileScanner(state, profile, scan_range.params, counter), _scan_range(scan_range) {
    if (scan_range.params.__isset.multi_column_separator) {
        _field_delimiter = scan_range.params.multi_column_separator;
    } else {
        _field_delimiter = scan_range.params.column_separator;
    }
    if (scan_range.params.__isset.multi_row_delimiter) {
        _record_delimiter = scan_range.params.multi_row_delimiter;
    } else {
        _record_delimiter = scan_range.params.row_delimiter;
    }
}

void CSVScanner::close() {
    FileScanner::close();
};

Status CSVScanner::open() {
    RETURN_IF_ERROR(FileScanner::open());

    if (_scan_range.ranges.empty()) {
        return Status::OK();
    }

    const auto& first_range = _scan_range.ranges[0];
    if (!first_range.__isset.num_of_columns_from_file) {
        return Status::InternalError("'num_of_columns_from_file' not set");
    }

    for (const auto& rng : _scan_range.ranges) {
        if (rng.columns_from_path.size() != first_range.columns_from_path.size()) {
            return Status::InvalidArgument("path column count of range mismatch");
        }
        if (rng.num_of_columns_from_file != first_range.num_of_columns_from_file) {
            return Status::InvalidArgument("CSV column count of range mismatch");
        }
        if (rng.num_of_columns_from_file + rng.columns_from_path.size() != _src_slot_descriptors.size()) {
            return Status::InvalidArgument("slot descriptor and column count mismatch");
        }
    }

    _num_fields_in_csv = first_range.num_of_columns_from_file;

    for (int i = _num_fields_in_csv; i < _src_slot_descriptors.size(); i++) {
        if (_src_slot_descriptors[i] != nullptr && _src_slot_descriptors[i]->type().type != TYPE_VARCHAR) {
            auto t = _src_slot_descriptors[i]->type();
            return Status::InvalidArgument("Incorrect path column type '" + t.debug_string() + "'");
        }
    }

    for (int i = 0; i < _num_fields_in_csv; i++) {
        auto slot = _src_slot_descriptors[i];
        if (slot == nullptr) {
            // This means the i-th field in CSV file should be ignored.
            continue;
        }
        // NOTE: Here we always create a nullable converter, even if |slot->is_nullable()| is false,
        // since |slot->is_nullable()| is false does not guarantee that no NULL in the CSV files.
        // This implies that the input column of |conv| must be a `NullableColumn`.
        //
        // For those columns defined as non-nullable, NULL records will be filtered out by the
        // `TabletSink`.
        ConverterPtr conv = csv::get_converter(slot->type(), true);
        if (conv == nullptr) {
            auto msg = strings::Substitute("Unsupported CSV type $0", slot->type().debug_string());
            return Status::InternalError(msg);
        }
        _converters.emplace_back(std::move(conv));
    }

    return Status::OK();
}

void CSVScanner::_materialize_src_chunk_adaptive_nullable_column(ChunkPtr& chunk) {
    chunk->materialized_nullable();
    for (int i = 0; i < chunk->num_columns(); i++) {
        AdaptiveNullableColumn* adaptive_column =
                down_cast<AdaptiveNullableColumn*>(chunk->get_column_by_index(i).get());
        chunk->update_column_by_index(NullableColumn::create(adaptive_column->materialized_raw_data_column(),
                                                             adaptive_column->materialized_raw_null_column()),
                                      i);
    }
}

StatusOr<ChunkPtr> CSVScanner::get_next() {
    SCOPED_RAW_TIMER(&_counter->total_ns);

    ChunkPtr chunk;
    auto src_chunk = _create_chunk(_src_slot_descriptors);

    do {
        if (_curr_reader == nullptr && ++_curr_file_index < _scan_range.ranges.size()) {
            std::shared_ptr<SequentialFile> file;
            const TBrokerRangeDesc& range_desc = _scan_range.ranges[_curr_file_index];
            Status st = create_sequential_file(range_desc, _scan_range.broker_addresses[0], _scan_range.params, &file);
            if (!st.ok()) {
                LOG(WARNING) << "Failed to create sequential files. status: " << st.to_string();
                return st;
            }

            // If file format is KV, use origin ScannerKVReader
            // Use _src_slot_descriptors to extract corresponding columns
            // For columns that are not shown in dest table, the slot is assigned NULL, so need to deal with it
            if(_scan_range.ranges[_curr_file_index].format_type == TFileFormatType::FORMAT_TDMSG_KV){
                std::vector<string> columns;
                int null_idx = 0;
                for(auto slot: _src_slot_descriptors){
                    if(slot == nullptr){
                        columns.emplace_back("null" + std::to_string(null_idx++));
                    }else{
                        columns.emplace_back(slot->col_name());
                    }
                }
                _curr_reader = std::make_unique<ScannerKVReader>(file, _record_delimiter, _field_delimiter, columns);
            }else {
                _curr_reader = std::make_unique<ScannerCSVReader>(file, _record_delimiter, _field_delimiter);
            }
            _curr_reader->set_counter(_counter);
            if (_scan_range.ranges[_curr_file_index].size > 0 &&
                (_scan_range.ranges[_curr_file_index].format_type == TFileFormatType::FORMAT_CSV_PLAIN ||
                 _scan_range.ranges[_curr_file_index].format_type == TFileFormatType::FORMAT_TDMSG_CSV ||
                 _scan_range.ranges[_curr_file_index].format_type == TFileFormatType::FORMAT_TDMSG_KV )) {
                // Does not set limit for compressed file.
                _curr_reader->set_limit(_scan_range.ranges[_curr_file_index].size);
            }
            if (_scan_range.ranges[_curr_file_index].start_offset > 0) {
                // Skip the first record started from |start_offset|.
                auto status = file->skip(_scan_range.ranges[_curr_file_index].start_offset);
                if (status.is_time_out()) {
                    // open this file next time
                    --_curr_file_index;
                    _curr_reader.reset();
                    return status;
                }
                CSVReader::Record dummy;
                RETURN_IF_ERROR(_curr_reader->next_record(&dummy));
            }
        } else if (_curr_reader == nullptr) {
            return Status::EndOfFile("CSVScanner");
        }

        src_chunk->set_num_rows(0);
        Status status = _parse_csv(src_chunk.get());
        if (!status.ok()) {
            if (status.is_end_of_file()) {
                _curr_reader = nullptr;
                DCHECK_EQ(0, src_chunk->num_rows());
            } else if (status.is_time_out()) {
                // if timeout happens at the beginning of reading src_chunk, we return the error state
                // else we will _materialize the lines read before timeout
                if (src_chunk->num_rows() == 0) {
                    return status;
                }
            } else {
                return status;
            }
        }

        if (src_chunk->num_rows() > 0) {
            _materialize_src_chunk_adaptive_nullable_column(src_chunk);
        }
    } while ((src_chunk)->num_rows() == 0);

    fill_columns_from_path(src_chunk, _num_fields_in_csv, _scan_range.ranges[_curr_file_index].columns_from_path,
                           src_chunk->num_rows());
    ASSIGN_OR_RETURN(chunk, materialize(nullptr, src_chunk));

    return std::move(chunk);
}

Status CSVScanner::_parse_csv(Chunk* chunk) {
    const int capacity = _state->chunk_size();
    DCHECK_EQ(0, chunk->num_rows());
    Status status;
    CSVReader::Record record;

    int num_columns = chunk->num_columns();
    _column_raw_ptrs.resize(num_columns);
    for (int i = 0; i < num_columns; i++) {
        _column_raw_ptrs[i] = chunk->get_column_by_index(i).get();
    }

    csv::Converter::Options options{.invalid_field_as_null = !_strict_mode};

    for (size_t num_rows = chunk->num_rows(); num_rows < capacity; /**/) {
        status = _curr_reader->next_record(&record);
        if (status.is_end_of_file()) {
            break;
        } else if (!status.ok()) {
            return status;
        } else if (record.empty()) {
            // always skip blank lines.
            continue;
        }

        fields.clear();
        try{
            _curr_reader->split_record(record, &fields);
        }catch (const char* error_msg){
            _report_error(record.to_string(), error_msg);
            continue;
        }

        if (fields.size() != _num_fields_in_csv) {
            if (_ignore_tail_columns) {
                fields.resize(_num_fields_in_csv);
            } else {
                if (_counter->num_rows_filtered++ < 50) {
                    std::stringstream error_msg;
                    error_msg << "Value count does not match column count. "
                              << "Expect " << _num_fields_in_csv << ", but got " << fields.size();
                    _report_error(record.to_string(), error_msg.str());
                }
                continue;
            } 
        }

        if (!_skip_utf8_check && !validate_utf8(record.data, record.size)) {
            if (_counter->num_rows_filtered++ < 50) {
                _report_error(record.to_string(), "Invalid UTF-8 row");
            }
            continue;
        }

        SCOPED_RAW_TIMER(&_counter->fill_ns);
        bool has_error = false;
        for (int j = 0, k = 0; j < _num_fields_in_csv; j++) {
            auto slot = _src_slot_descriptors[j];
            if (slot == nullptr) {
                continue;
            }
            const Slice& field = fields[j];
            options.type_desc = &(slot->type());
            if (!_converters[k]->read_string_for_adaptive_null_column(_column_raw_ptrs[k], field, options)) {
                chunk->set_num_rows(num_rows);
                if (_counter->num_rows_filtered++ < 50) {
                    std::stringstream error_msg;
                    error_msg << "Value '" << field.to_string() << "' is out of range. "
                              << "The type of '" << slot->col_name() << "' is " << slot->type().debug_string();
                    _report_error(record.to_string(), error_msg.str());
                }
                has_error = true;
                break;
            }
            k++;
        }
        num_rows += !has_error;
    }
    fields.clear();
    return chunk->num_rows() > 0 ? Status::OK() : Status::EndOfFile("");
}

ChunkPtr CSVScanner::_create_chunk(const std::vector<SlotDescriptor*>& slots) {
    SCOPED_RAW_TIMER(&_counter->init_chunk_ns);

    auto chunk = std::make_shared<Chunk>();
    for (int i = 0; i < _num_fields_in_csv; ++i) {
        if (slots[i] == nullptr) {
            continue;
        }
        // NOTE: Always create a nullable column, even if |slot->is_nullable()| is false.
        // See the comment in `CSVScanner::Open` for reference.
        // here we optimize it through adaptive nullable column
        auto column = ColumnHelper::create_column(slots[i]->type(), true, false, 0, true);

        chunk->append_column(std::move(column), slots[i]->id());
    }
    return chunk;
}

void CSVScanner::_report_error(const std::string& line, const std::string& err_msg) {
    _state->append_error_msg_to_file(line, err_msg);
}


void CSVScanner::ScannerKVReader::split_record(const Record& record, Fields* kv_fields) const {
    const char* value = record.data;
    const char* ptr = record.data;
    const size_t size = record.size;

    // It means it is an empty msg
    if(size == 0){
        return;
    }

    string _equal_separator = "=";
    std::unordered_map<string, int> key_field_idx_dict;
    std::unordered_map<int, string> field_idx_key_dict;

    string key = "";
    const string null_flag = "null";
    int null_idx = 0; // process useless key
    if (_column_separator_length == 1) {
        for (size_t i = 0; i < size; ++i, ++ptr) {
            if (*ptr == _equal_separator[0]) {
                key = string(value, ptr - value);
                if (std::find(_columns.begin(), _columns.end(), key) == _columns.end()) {
                    key = null_flag + std::to_string(null_idx);
                    ++null_idx;
                }
                value = ptr + 1;
            } else if (*ptr == _column_separator[0]) {
                // for some scenario such as a=1,,b=2,c=3
                if (ptr != value) {
                    if (key != "") {
                        key_field_idx_dict[key] = kv_fields->size();
                        field_idx_key_dict[kv_fields->size()] = key;
                        key = ""; // ensure a value corresponds to a key
                        kv_fields->emplace_back(value, ptr - value);
                    }
                }
                value = ptr + 1;
            }
        }
    } else {
        const auto* const base = ptr;
        const char* equal_ptr = ptr; //address for _equal_separator
        const char* col_ptr = ptr;   //address for _col_separator
        do {
            // firstly, find _col_separator address, to check if it is an empty kv message
            // for example, for _col_separator = "&&"
            // there are kv_row like  "a1=1&&&&a3=3"
            col_ptr = static_cast<char*>(
                    memmem(value, size - (value - base), _column_separator.data(), _column_separator_length));
            if(col_ptr != nullptr && col_ptr == value){
                value = col_ptr + _column_separator_length;
            }else{
                equal_ptr = static_cast<char*>(
                        memmem(value, size - (value - base), _equal_separator.data(), _equal_separator.size()));
                if(equal_ptr != nullptr){
                    key = string(value, equal_ptr - value);
                    value = equal_ptr + _equal_separator.size();
                    if(std::find(_columns.begin(), _columns.end(), key) == _columns.end()){
                        key = null_flag + std::to_string(null_idx);
                        ++null_idx;
                    }
                }
                if(col_ptr != nullptr){
                    if(key == ""){
                        throw "Unsupported Format: unable to parse using KV format";
                    }
                    key_field_idx_dict[key] = kv_fields->size();
                    field_idx_key_dict[kv_fields->size()] = key;
                    key = "";
                    kv_fields->emplace_back(value, col_ptr - value);
                    value = col_ptr + _column_separator_length;
                }
            }
        } while (col_ptr != nullptr);

        // use ptr to have the same format with the case _column_separator_length == 1
        ptr = record.data + size;
    }
    key_field_idx_dict[key] = kv_fields->size();
    field_idx_key_dict[kv_fields->size()] = key;
    kv_fields->emplace_back(value, ptr - value);

    for(int i=0; i < _columns.size(); i++) {
        string name = _columns[i];
        if (key_field_idx_dict.count(name) == 0) {
            key_field_idx_dict[name] = kv_fields->size();
            field_idx_key_dict[kv_fields->size()] = key;
            kv_fields->emplace_back(_null_char, 2); // "\\N"
        }
        int idx = key_field_idx_dict[name];
        if (idx != i) {
            field_idx_key_dict[idx] = field_idx_key_dict[i];
            key_field_idx_dict[field_idx_key_dict[i]] = idx;
            field_idx_key_dict[i] = name;
            key_field_idx_dict[name] = i;
            std::swap(kv_fields->at(idx), kv_fields->at(i));
        }
    }
}
} // namespace starrocks::vectorized
