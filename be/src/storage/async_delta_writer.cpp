// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.

#include "storage/async_delta_writer.h"

#include <fmt/format.h>

#include "runtime/current_thread.h"
#include "storage/segment_flush_executor.h"
#include "storage/storage_engine.h"

namespace starrocks::vectorized {

AsyncDeltaWriter::~AsyncDeltaWriter() {
    _close();
    _writer.reset();
}

int AsyncDeltaWriter::_execute(void* meta, bthread::TaskIterator<AsyncDeltaWriter::Task>& iter) {
    if (iter.is_queue_stopped()) {
        return 0;
    }
    auto writer = static_cast<DeltaWriter*>(meta);
    for (; iter; ++iter) {
        Status st;
        if (iter->abort) {
            writer->abort(iter->abort_with_log);
            continue;
        }
        if (iter->chunk != nullptr && iter->indexes_size > 0) {
            st = writer->write(*iter->chunk, iter->indexes, 0, iter->indexes_size);
        }
        if (st.ok() && iter->commit_after_write) {
            if (st = writer->close(); !st.ok()) {
                iter->write_cb->run(st, nullptr);
                continue;
            }
            if (st = writer->commit(); !st.ok()) {
                iter->write_cb->run(st, nullptr);
                continue;
            }
            CommittedRowsetInfo info{.tablet = writer->tablet(),
                                     .rowset = writer->committed_rowset(),
                                     .rowset_writer = writer->committed_rowset_writer(),
                                     .replicate_token = writer->replicate_token()};
            iter->write_cb->run(st, &info);
        } else {
            iter->write_cb->run(st, nullptr);
        }
        // Do NOT touch |iter->commit_cb| since here, it may have been deleted.
        LOG_IF(ERROR, !st.ok()) << "Fail to write or commit. txn_id=" << writer->txn_id()
                                << " tablet_id=" << writer->tablet()->tablet_id() << ": " << st;
    }
    return 0;
}

StatusOr<std::unique_ptr<AsyncDeltaWriter>> AsyncDeltaWriter::open(const DeltaWriterOptions& opt,
                                                                   MemTracker* mem_tracker) {
    auto res = DeltaWriter::open(opt, mem_tracker);
    if (!res.ok()) {
        return res.status();
    }
    auto w = std::make_unique<AsyncDeltaWriter>(private_type(0), std::move(res).value());
    RETURN_IF_ERROR(w->_init());
    return std::move(w);
}

Status AsyncDeltaWriter::_init() {
    if (UNLIKELY(StorageEngine::instance() == nullptr)) {
        return Status::InternalError("StorageEngine::instance() is NULL");
    }
    bthread::ExecutionQueueOptions opts;
    opts.executor = StorageEngine::instance()->async_delta_writer_executor();
    if (UNLIKELY(opts.executor == nullptr)) {
        return Status::InternalError("AsyncDeltaWriterExecutor init failed");
    }
    if (int r = bthread::execution_queue_start(&_queue_id, &opts, _execute, _writer.get()); r != 0) {
        return Status::InternalError(fmt::format("fail to create bthread execution queue: {}", r));
    }
    _segment_flush_executor =
            std::move(StorageEngine::instance()->segment_flush_executor()->create_flush_token(_writer));
    if (_segment_flush_executor == nullptr) {
        return Status::InternalError("SegmentFlushExecutor init failed");
    }
    return Status::OK();
}

void AsyncDeltaWriter::write(const AsyncDeltaWriterRequest& req, AsyncDeltaWriterCallback* cb) {
    DCHECK(cb != nullptr);
    Task task;
    task.chunk = req.chunk;
    task.indexes = req.indexes;
    task.indexes_size = req.indexes_size;
    task.write_cb = cb;
    task.commit_after_write = req.commit_after_write;
    int r = bthread::execution_queue_execute(_queue_id, task);
    if (r != 0) {
        LOG(WARNING) << "Fail to execution_queue_execute: " << r;
        task.write_cb->run(Status::InternalError("fail to call execution_queue_execute"), nullptr);
    }
}

void AsyncDeltaWriter::write_segment(const AsyncDeltaWriterSegmentRequest& req) {
    auto st = _segment_flush_executor->submit(req.cntl, req.request, req.response, req.done);
    if (!st.ok()) {
        LOG(WARNING) << "Failed to submit write segment, err=" << st;
    }
}

void AsyncDeltaWriter::commit(AsyncDeltaWriterCallback* cb) {
    DCHECK(cb != nullptr);
    Task task;
    task.chunk = nullptr;
    task.indexes = nullptr;
    task.indexes_size = 0;
    task.write_cb = cb;
    task.commit_after_write = true;
    int r = bthread::execution_queue_execute(_queue_id, task);
    if (r != 0) {
        LOG(WARNING) << "Fail to execution_queue_execute: " << r;
        task.write_cb->run(Status::InternalError("fail to call execution_queue_execute"), nullptr);
    }
}

void AsyncDeltaWriter::abort(bool with_log) {
    Task task;
    task.abort = true;
    task.abort_with_log = with_log;

    bthread::TaskOptions options;
    options.high_priority = true;
    int r = bthread::execution_queue_execute(_queue_id, task, &options);
    LOG_IF(WARNING, r != 0) << "Fail to execution_queue_execute: " << r;

    if (_segment_flush_executor != nullptr) {
        _segment_flush_executor->cancel();
    }

    // Wait until all background tasks finished
    // https://github.com/StarRocks/starrocks/issues/8906
    _close();
}

void AsyncDeltaWriter::_close() {
    bool value = _closed.load(std::memory_order_acquire);
    if (value) {
        return;
    }
    if (_closed.compare_exchange_strong(value, true, std::memory_order_acq_rel) && _queue_id.value != kInvalidQueueId) {
        int r = bthread::execution_queue_stop(_queue_id);
        LOG_IF(WARNING, r != 0) << "Fail to stop execution queue: " << r;
        r = bthread::execution_queue_join(_queue_id);
        LOG_IF(WARNING, r != 0) << "Fail to join execution queue: " << r;
    }
    // wait is thread-safe
    if (_segment_flush_executor != nullptr) {
        _segment_flush_executor->wait();
    }
}

} // namespace starrocks::vectorized
