// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.

#include "exec/pipeline/sink/export_sink_operator.h"

#include "exec/data_sink.h"
#include "exec/file_builder.h"
#include "exec/orc_builder.h"
#include "exec/parquet_builder.h"
#include "exec/pipeline/fragment_context.h"
#include "exec/pipeline/sink/sink_io_buffer.h"
#include "exec/plain_text_builder.h"
#include "formats/csv/converter.h"
#include "formats/csv/output_stream.h"
#include "fs/fs_broker.h"
#include "runtime/runtime_state.h"
#include "util/starrocks_metrics.h"
#include "util/time.h"

namespace starrocks::pipeline {

class ExportSinkIOBuffer final : public SinkIOBuffer {
public:
    ExportSinkIOBuffer(const TExportSink& t_export_sink, std::vector<ExprContext*>& output_expr_ctxs,
                       int32_t num_sinkers, FragmentContext* fragment_ctx)
            : SinkIOBuffer(num_sinkers),
              _t_export_sink(t_export_sink),
              _output_expr_ctxs(output_expr_ctxs),
              _fragment_ctx(fragment_ctx),
              _num_rows(0) {
        if (_t_export_sink.__isset.file_options && _t_export_sink.file_options.__isset.parquet_max_group_bytes) {
            _parquet_options.row_group_max_size = _t_export_sink.file_options.parquet_max_group_bytes;
        }
        if (_t_export_sink.__isset.file_options && _t_export_sink.file_options.__isset.use_dict) {
            _parquet_options.use_dict = _t_export_sink.file_options.use_dict;
        }
        if (_t_export_sink.__isset.file_options && _t_export_sink.file_options.__isset.compression_type) {
            _parquet_options.compression_type = _t_export_sink.file_options.compression_type;
        }
        if (_t_export_sink.__isset.file_options && _t_export_sink.file_options.__isset.max_file_size_bytes) {
            max_file_size_bytes = _t_export_sink.file_options.max_file_size_bytes;
        }
        if (_t_export_sink.__isset.file_options && _t_export_sink.file_options.__isset.max_file_size_rows) {
            max_file_size_rows = _t_export_sink.file_options.max_file_size_rows;
        }
        
    }

    ~ExportSinkIOBuffer() override = default;

    Status prepare(RuntimeState* state, RuntimeProfile* parent_profile) override;

    void close(RuntimeState* state) override;

private:
    void _process_chunk(bthread::TaskIterator<ChunkPtr>& iter) override;

    Status _open_file_writer();

    Status _gen_file_name(std::string* file_name);

    TExportSink _t_export_sink;
    const std::vector<ExprContext*> _output_expr_ctxs;
    std::unique_ptr<FileBuilder> _file_builder;
    FragmentContext* _fragment_ctx;
    parquet::ParquetBuilderOptions _parquet_options;
    ORCBuilderOptions _orc_options;
    size_t _num_rows;
    int64_t max_file_size_rows = -1;
    int64_t max_file_size_bytes = -1;
    int64_t _number_written_rows = 0;
    int64_t _number_written_bytes = 0;
};

Status ExportSinkIOBuffer::prepare(RuntimeState* state, RuntimeProfile* parent_profile) {
    bool expected = false;
    if (!_is_prepared.compare_exchange_strong(expected, true)) {
        return Status::OK();
    }
    _state = state;

    bthread::ExecutionQueueOptions options;
    options.executor = SinkIOExecutor::instance();
    _exec_queue_id = std::make_unique<bthread::ExecutionQueueId<ChunkPtr>>();
    int ret = bthread::execution_queue_start<ChunkPtr>(_exec_queue_id.get(), &options,
                                                       &ExportSinkIOBuffer::execute_io_task, this);
    if (ret != 0) {
        _exec_queue_id.reset();
        return Status::InternalError("start execution queue error");
    }

    return Status::OK();
}

void ExportSinkIOBuffer::close(RuntimeState* state) {
    if (_file_builder != nullptr) {
        Status status = _file_builder->finish();
        set_io_status(status);
        if (status.ok()) {
            int64_t size = _file_builder->file_size();
            _number_written_bytes += size;
            StarRocksMetrics::instance()->exported_bytes_total.increment(size);
        }
        _file_builder.reset();
    }
    state->update_num_rows_load_from_sink(_number_written_rows);
    state->update_num_bytes_load_from_sink(_number_written_bytes);
    SinkIOBuffer::close(state);
}

void ExportSinkIOBuffer::_process_chunk(bthread::TaskIterator<ChunkPtr>& iter) {
    --_num_pending_chunks;
    if (_is_finished) {
        return;
    }

    if (_is_cancelled && !_is_finished) {
        if (_num_pending_chunks == 0) {
            close(_state);
        }
        return;
    }

    if (_file_builder == nullptr) {
        if (Status status = _open_file_writer(); !status.ok()) {
            LOG(WARNING) << "open file write failed, error: " << status.to_string();
            _fragment_ctx->cancel(status);
            return;
        }
    }
    const auto& chunk = *iter;
    if (chunk == nullptr) {
        // this is the last chunk
        DCHECK_EQ(_num_pending_chunks, 0);
        close(_state);
        return;
    }
    size_t chunkNumRows = chunk->num_rows();
    if (Status status = _file_builder->add_chunk(chunk.get()); !status.ok()) {
        LOG(WARNING) << "add chunk to file builder failed, error: " << status.to_string();
        _fragment_ctx->cancel(status);
        return;
    }

    _number_written_rows += chunkNumRows;
    StarRocksMetrics::instance()->exported_rows_total.increment(chunkNumRows);
    if ((max_file_size_rows > 0 && _num_rows >= max_file_size_rows) ||
        (max_file_size_bytes > 0 && _file_builder->file_size() >= max_file_size_bytes)) {
        if (Status status = _file_builder->finish(); !status.ok()) {
            LOG(WARNING) << "finish file build failed, error: " << status.to_string();
            return;
        }
        int64_t size = _file_builder->file_size();
        _number_written_bytes += size;
        StarRocksMetrics::instance()->exported_bytes_total.increment(size);
        _file_builder.reset();
        _num_rows = 0;
    }
    _num_rows += chunkNumRows;
}

Status ExportSinkIOBuffer::_open_file_writer() {
    std::unique_ptr<WritableFile> output_file;
    std::string file_name;
    RETURN_IF_ERROR(_gen_file_name(&file_name));
    std::string file_path = _t_export_sink.export_path + "/" + file_name;
    WritableFileOptions options{.sync_on_close = false, .mode = FileSystem::MUST_CREATE};
    const auto& file_type = _t_export_sink.file_type;
    switch (file_type) {
    case TFileType::FILE_LOCAL: {
        ASSIGN_OR_RETURN(output_file, FileSystem::Default()->new_writable_file(options, file_path));
        break;
    }
    case TFileType::FILE_BROKER: {
        if (_t_export_sink.__isset.use_broker && !_t_export_sink.use_broker) {
            ASSIGN_OR_RETURN(auto fs, FileSystem::CreateUniqueFromString(file_path, FSOptions(&_t_export_sink)));
            ASSIGN_OR_RETURN(output_file, fs->new_writable_file(options, file_path));
        } else {
            if (_t_export_sink.broker_addresses.empty()) {
                LOG(WARNING) << "ExportSink broker_addresses empty";
                return Status::InternalError("ExportSink broker_addresses empty");
            }
            const TNetworkAddress& broker_addr = _t_export_sink.broker_addresses[0];
            BrokerFileSystem fs_broker(broker_addr, _t_export_sink.properties);
            ASSIGN_OR_RETURN(output_file, fs_broker.new_writable_file(options, file_path));
        }
        break;
    }
    case TFileType::FILE_STREAM:
        return Status::NotSupported(strings::Substitute("Unsupported file type $0", file_type));
    }
    const auto& file_format = string(_t_export_sink.file_format);
    if (file_format == "csv") {
        _file_builder = std::make_unique<PlainTextBuilder>(
                PlainTextBuilderOptions{.column_terminated_by = _t_export_sink.column_separator,
                                        .line_terminated_by = _t_export_sink.row_delimiter},
                std::move(output_file), _output_expr_ctxs);
    } else if (file_format == "parquet") {
        std::vector<TypeDescriptor> output_types;
        for (const auto& type : _t_export_sink.file_output_types) {
            output_types.push_back(TypeDescriptor::from_thrift(type));
        }
        auto properties = parquet::ParquetBuildHelper::make_properties(_parquet_options);
        auto result = parquet::ParquetBuildHelper::make_schema(_t_export_sink.file_column_names, output_types,
                                                           std::vector<parquet::FileColumnId>(_output_expr_ctxs.size()));
        if (!result.ok()) {
            return Status::NotSupported(result.status().message());
        }
        auto schema = result.ValueOrDie();
        auto parquet_builder = std::make_unique<ParquetBuilder>(
                std::move(output_file), std::move(properties), std::move(schema), _output_expr_ctxs,
                _parquet_options.row_group_max_size, max_file_size_bytes);
        RETURN_IF_ERROR(parquet_builder->init());
        _file_builder = std::move(parquet_builder);
    } else if (file_format == "orc") {
        std::vector<TypeDescriptor> output_types;
        for (const auto& type : _t_export_sink.file_output_types) {
            output_types.push_back(TypeDescriptor::from_thrift(type));
        }
        _file_builder = std::make_unique<ORCBuilder>(_orc_options, std::move(output_file), _output_expr_ctxs, nullptr,
                                                     _t_export_sink.file_column_names, output_types);
    } else {
        return Status::NotSupported("unsupported file format " + file_format);
    }

    _state->add_export_output_file(file_path);

    LOG(INFO) << "create file for exporting query result. file name: " << file_name
              << ". query id: " << print_id(_state->query_id());
    return Status::OK();
}

Status ExportSinkIOBuffer::_gen_file_name(std::string* file_name) {
    if (!_t_export_sink.__isset.file_name_prefix) {
        return Status::InternalError("file name prefix is not set");
    }
    std::stringstream file_name_ss;
    // now file-number is 0.
    // <file-name-prefix>_<file-number>.csv.<timestamp>
    file_name_ss << _t_export_sink.file_name_prefix << "0" << "." << _t_export_sink.file_format << "." << UnixMillis();
    *file_name = file_name_ss.str();
    return Status::OK();
}


Status ExportSinkOperator::prepare(RuntimeState* state) {
    RETURN_IF_ERROR(Operator::prepare(state));
    return _export_sink_buffer->prepare(state, _unique_metrics.get());
}

void ExportSinkOperator::close(RuntimeState* state) {
    Operator::close(state);
}

bool ExportSinkOperator::need_input() const {
    return _export_sink_buffer->need_input();
}

bool ExportSinkOperator::is_finished() const {
    return _export_sink_buffer->is_finished();
}

Status ExportSinkOperator::set_finishing(RuntimeState* state) {
    return _export_sink_buffer->set_finishing();
}

bool ExportSinkOperator::pending_finish() const {
    return !_export_sink_buffer->is_finished();
}

Status ExportSinkOperator::set_cancelled(RuntimeState* state) {
    _export_sink_buffer->cancel_one_sinker();
    return Status::OK();
}

StatusOr<vectorized::ChunkPtr> ExportSinkOperator::pull_chunk(RuntimeState* state) {
    return Status::InternalError("Shouldn't pull chunk from export sink operator");
}

Status ExportSinkOperator::push_chunk(RuntimeState* state, const vectorized::ChunkPtr& chunk) {
    return _export_sink_buffer->append_chunk(state, chunk);
}

Status ExportSinkOperatorFactory::prepare(RuntimeState* state) {
    RETURN_IF_ERROR(OperatorFactory::prepare(state));
    RETURN_IF_ERROR(Expr::create_expr_trees(state->obj_pool(), _t_output_expr, &_output_expr_ctxs));
    RETURN_IF_ERROR(Expr::prepare(_output_expr_ctxs, state));
    RETURN_IF_ERROR(Expr::open(_output_expr_ctxs, state));

    _export_sink_buffer =
            std::make_shared<ExportSinkIOBuffer>(_t_export_sink, _output_expr_ctxs, _num_sinkers, _fragment_ctx);
    return Status::OK();
}

void ExportSinkOperatorFactory::close(RuntimeState* state) {
    Expr::close(_output_expr_ctxs, state);
    OperatorFactory::close(state);
}

} // namespace starrocks::pipeline
