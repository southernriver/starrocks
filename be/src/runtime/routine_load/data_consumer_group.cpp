// This file is made available under Elastic License 2.0.
// This file is based on code available under the Apache license here:
//   https://github.com/apache/incubator-doris/blob/master/be/src/runtime/routine_load/data_consumer_group.cpp

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
#include "runtime/routine_load/data_consumer_group.h"

#include "librdkafka/rdkafka.h"
#include "librdkafka/rdkafkacpp.h"
#include "runtime/routine_load/data_consumer.h"
#include "runtime/routine_load/kafka_consumer_pipe.h"
#include "runtime/stream_load/stream_load_context.h"

namespace starrocks {

Status KafkaDataConsumerGroup::assign_topic_partitions(StreamLoadContext* ctx) {
    DCHECK(ctx->kafka_info);
    DCHECK(_consumers.size() >= 1);

    // divide partitions
    int consumer_size = _consumers.size();
    std::vector<std::map<int32_t, int64_t>> divide_parts(consumer_size);
    int i = 0;
    for (auto& kv : ctx->kafka_info->begin_offset) {
        int idx = i % consumer_size;
        divide_parts[idx].emplace(kv.first, kv.second);
        i++;
    }

    // assign partitions to consumers equally
    for (int i = 0; i < consumer_size; ++i) {
        RETURN_IF_ERROR(std::static_pointer_cast<KafkaDataConsumer>(_consumers[i])
                                ->assign_topic_partitions(divide_parts[i], ctx->kafka_info->topic, ctx));
    }

    return Status::OK();
}

KafkaDataConsumerGroup::~KafkaDataConsumerGroup() {
    // clean the msgs left in queue
    _queue.shutdown();
    while (true) {
        RdKafka::Message* msg;
        if (_queue.blocking_get(&msg)) {
            delete msg;
            msg = nullptr;
        } else {
            break;
        }
    }
    DCHECK(_queue.get_size() == 0);
}

Status KafkaDataConsumerGroup::start_all(StreamLoadContext* ctx) {
    Status result_st = Status::OK();
    // start all consumers
    for (auto& consumer : _consumers) {
        if (!_thread_pool.offer(
                    [this, consumer, capture0 = ctx, capture1 = &_queue, capture2 = ctx->max_interval_s * 1000,
                     capture3 = [this, &result_st](const Status& st) {
                         std::unique_lock<std::mutex> lock(_mutex);
                         _counter--;
                         VLOG(1) << "group counter is: " << _counter << ", grp: " << _grp_id;
                         if (_counter == 0) {
                             _queue.shutdown();
                             LOG(INFO) << "all consumers are finished. shutdown queue. group id: " << _grp_id;
                         }
                         if (result_st.ok() && !st.ok()) {
                             result_st = st;
                         }
                     }] { actual_consume(consumer, capture0, capture1, capture2, capture3); })) {
            LOG(WARNING) << "failed to submit data consumer: " << consumer->id() << ", group id: " << _grp_id;
            return Status::InternalError("failed to submit data consumer");
        } else {
            VLOG(1) << "submit a data consumer: " << consumer->id() << ", group id: " << _grp_id;
        }
    }

    // consuming from queue and put data to stream load pipe
    int64_t left_time = ctx->max_interval_s * 1000;
    int64_t received_rows = 0;
    int64_t left_bytes = ctx->max_batch_size;

    std::shared_ptr<KafkaConsumerPipe> kafka_pipe = std::static_pointer_cast<KafkaConsumerPipe>(ctx->body_sink);

    LOG(INFO) << "start consumer group: " << _grp_id << ". max time(ms): " << left_time
              << ", batch size: " << left_bytes << ". " << ctx->brief();

    // copy one
    std::map<int32_t, int64_t> cmt_offset = ctx->kafka_info->cmt_offset;

    //improve performance
    Status (KafkaConsumerPipe::*append_data)(const char* data, size_t size, char row_delimiter);
    char row_delimiter = '\n';
    if (ctx->format == TFileFormatType::FORMAT_JSON) {
        append_data = &KafkaConsumerPipe::append_json;
    } else {
        append_data = &KafkaConsumerPipe::append_with_row_delimiter;

        auto& per_node_scan_ranges = ctx->put_result.params.params.per_node_scan_ranges;

        if (!per_node_scan_ranges.empty()) {
            DCHECK_GE(per_node_scan_ranges.begin()->second.size(), 1);

            auto& scan_range = per_node_scan_ranges.begin()->second[0].scan_range;
            auto& params = scan_range.broker_scan_range.params;
            row_delimiter = static_cast<char>(params.row_delimiter);
        }
    }

    MonotonicStopWatch watch;
    watch.start();
    bool eos = false;
    while (true) {
        if (eos || left_time <= 0 || left_bytes <= 0) {
            LOG(INFO) << "consumer group done: " << _grp_id
                      << ". consume time(ms)=" << ctx->max_interval_s * 1000 - left_time
                      << ", received rows=" << received_rows << ", received bytes=" << ctx->max_batch_size - left_bytes
                      << ", eos: " << eos << ", left_time: " << left_time << ", left_bytes: " << left_bytes
                      << ", blocking get time(us): " << _queue.total_get_wait_time() / 1000
                      << ", blocking put time(us): " << _queue.total_put_wait_time() / 1000;

            // shutdown queue
            _queue.shutdown();
            // cancel all consumers
            for (auto& consumer : _consumers) {
                consumer->cancel(ctx);
            }

            // waiting all threads finished
            _thread_pool.shutdown();
            _thread_pool.join();

            if (!result_st.ok()) {
                // some consumers encounter errors, cancel this task
                return result_st;
            }

            if (left_bytes == ctx->max_batch_size) {
                // nothing to be consumed, we have to cancel it, because
                // we do not allow finishing stream load pipe without data.
                //
                // But if the offset have already moved, such as the control msg,
                // we need to commit and tell fe to move offset to the newest offset, otherwise, fe will retry consume.
                for (auto& item : cmt_offset) {
                    if (item.second > ctx->kafka_info->cmt_offset[item.first]) {
                        kafka_pipe->finish();
                        ctx->kafka_info->cmt_offset = std::move(cmt_offset);
                        ctx->receive_bytes = 0;
                        return Status::OK();
                    }
                }
                kafka_pipe->cancel(Status::Cancelled("Cancelled"));
                return Status::Cancelled("Cancelled");
            } else {
                DCHECK(left_bytes < ctx->max_batch_size);
                kafka_pipe->finish();
                ctx->kafka_info->cmt_offset = std::move(cmt_offset);
                ctx->receive_bytes = ctx->max_batch_size - left_bytes;
                std::map<std::string, int64_t> res_lag;
                for (auto& kv : _consume_lags) {
                    res_lag[std::to_string(kv.first)] = kv.second;
                }
                ctx->rltask_statistics = RoutineLoadTaskStatistics(
                        ctx->max_interval_s * 1000 - left_time, _queue.total_get_wait_time() / 1000,
                        _queue.total_put_wait_time() / 1000, received_rows, ctx->receive_bytes, res_lag);
                return Status::OK();
            }
        }

        RdKafka::Message* msg;
        bool res = _queue.blocking_get(&msg);
        if (res) {
            VLOG(3) << "get kafka message"
                    << ", partition: " << msg->partition() << ", offset: " << msg->offset() << ", len: " << msg->len();

            if (msg->err() == RdKafka::ERR__PARTITION_EOF) {
                // For transaction producer, producer will append one control msg to the group of msgs,
                // but the control msg will not return to consumer,
                // so we use the offset of eof to compute the last offset.
                // The last offset of partition = `offset of eof` - 1
                //
                // if msg->offset == 0, don't record into cmt_offset,
                // because the fe will +1 and then consume the next msg.
                //
                // Our offset recorded in the kafka is the offset of last consumed msg,
                // but the standard usage is to record the last offset + 1.
                if (msg->offset() > 0) {
                    cmt_offset[msg->partition()] = msg->offset() - 1;
                }
            } else {
                Status st;
                if (ctx->format == TFileFormatType::FORMAT_TDMSG_KV ||
                    ctx->format == TFileFormatType::FORMAT_TDMSG_CSV) {
                    std::list<tubemq::DataItem> data_items;
                    st = get_data_items(msg, &data_items);
                    if (st.ok()) {
                        for (auto& data_item : data_items) {
                            std::size_t len = data_item.GetLength();
                            st = (kafka_pipe.get()->*append_data)(static_cast<const char*>(data_item.GetData()),
                                                                  static_cast<size_t>(len), row_delimiter);
                            if (st.ok()) {
                                received_rows++;
                                left_bytes -= len;
                            } else {
                                // failed to append this msg, we must stop
                                LOG(WARNING) << "failed to append msg to pipe. grp: " << _grp_id
                                             << ", errmsg=" << st.get_error_msg();
                                break;
                            }
                        }
                        cmt_offset[msg->partition()] = msg->offset();
                        if (_consume_lags.find(msg->partition()) == _consume_lags.end()) {
                            auto msg_timestamp = std::chrono::time_point<std::chrono::system_clock>(
                                    std::chrono::milliseconds(msg->timestamp().timestamp));
                            _consume_lags[msg->partition()] = std::chrono::duration_cast<std::chrono::seconds>(
                                                                      std::chrono::system_clock::now() - msg_timestamp)
                                                                      .count();
                        }
                        VLOG(3) << "consume partition[" << msg->partition() << " - " << msg->offset() << "]";
                    } else {
                        LOG(WARNING) << "failed to append msg to pipe. grp: " << _grp_id
                                     << ", errmsg=" << st.get_error_msg();
                    }
                } else {
                    st = (kafka_pipe.get()->*append_data)(static_cast<const char*>(msg->payload()),
                                                          static_cast<size_t>(msg->len()), row_delimiter);
                    if (st.ok()) {
                        received_rows++;
                        left_bytes -= msg->len();
                        cmt_offset[msg->partition()] = msg->offset();
                        if (_consume_lags.find(msg->partition()) == _consume_lags.end()) {
                            auto msg_timestamp = std::chrono::time_point<std::chrono::system_clock>(
                                    std::chrono::milliseconds(msg->timestamp().timestamp));
                            _consume_lags[msg->partition()] = std::chrono::duration_cast<std::chrono::seconds>(
                                                                      std::chrono::system_clock::now() - msg_timestamp)
                                                                      .count();
                        }
                        VLOG(3) << "consume partition[" << msg->partition() << " - " << msg->offset() << "]";
                    } else {
                        // failed to append this msg, we must stop
                        LOG(WARNING) << "failed to append msg to pipe. grp: " << _grp_id
                                     << ", errmsg=" << st.get_error_msg();
                    }
                }
                if (!st.ok()) {
                    eos = true;
                    {
                        std::unique_lock<std::mutex> lock(_mutex);
                        if (result_st.ok()) {
                            result_st = st;
                        }
                    }
                }
            }
            delete msg;
        } else {
            // queue is empty and shutdown
            eos = true;
        }

        left_time = ctx->max_interval_s * 1000 - watch.elapsed_time() / 1000 / 1000;
    }

    return Status::OK();
}

void KafkaDataConsumerGroup::actual_consume(const std::shared_ptr<DataConsumer>& consumer, StreamLoadContext* ctx,
                                            TimedBlockingQueue<RdKafka::Message*>* queue, int64_t max_running_time_ms,
                                            const ConsumeFinishCallback& cb) {
    Status st = std::static_pointer_cast<KafkaDataConsumer>(consumer)->group_consume(ctx, queue, max_running_time_ms);
    cb(st);
}

Status KafkaDataConsumerGroup::get_data_items(const RdKafka::Message* msg, std::list<tubemq::DataItem>* data_items) {
    std::string err_info;
    tubemq::TubeMQTDMsg tdmsg;
    if (tdmsg.ParseTDMsg(static_cast<const char*>(msg->payload()), msg->len(), err_info)) {
        for (auto attr_data : tdmsg.GetAttr2DataMap()) {
            data_items->splice(data_items->end(), attr_data.second);
        }
    } else {
        LOG(WARNING) << "failed to parse tube data: " << err_info;
        return Status::InternalError("PAUSE: failed to parse kafka data: " + err_info);
    }

    return Status::OK();
}

Status PulsarDataConsumerGroup::assign_topic_partitions(StreamLoadContext* ctx) {
    DCHECK(ctx->pulsar_info);
    DCHECK(_consumers.size() >= 1);
    DCHECK(_consumers.size() == ctx->pulsar_info->initial_positions.size());

    // assign partition to consumers
    int i = 0;
    for (auto& initial_position : ctx->pulsar_info->initial_positions) {
        RETURN_IF_ERROR(
                std::static_pointer_cast<PulsarDataConsumer>(_consumers[i])->assign_partition(ctx, initial_position));
        i++;
    }

    return Status::OK();
}

PulsarDataConsumerGroup::~PulsarDataConsumerGroup() {
    // clean the msgs left in queue
    _queue.shutdown();
    while (true) {
        pulsar::Message* msg;
        if (_queue.blocking_get(&msg)) {
            delete msg;
            msg = nullptr;
        } else {
            break;
        }
    }
    DCHECK(_queue.get_size() == 0);
}

Status PulsarDataConsumerGroup::start_all(StreamLoadContext* ctx) {
    Status result_st = Status::OK();
    // start all consumers
    for (auto& consumer : _consumers) {
        if (!_thread_pool.offer([this, consumer, capture0 = &_queue, capture1 = ctx->max_interval_s * 1000,
                                 capture2 = [this, &result_st](const Status& st) {
                                     std::unique_lock<std::mutex> lock(_mutex);
                                     _counter--;
                                     VLOG(1) << "group counter is: " << _counter << ", grp: " << _grp_id;
                                     if (_counter == 0) {
                                         _queue.shutdown();
                                         LOG(INFO)
                                                 << "all consumers are finished. shutdown queue. group id: " << _grp_id;
                                     }
                                     if (result_st.ok() && !st.ok()) {
                                         result_st = st;
                                     }
                                 }] { actual_consume(consumer, capture0, capture1, capture2); })) {
            LOG(WARNING) << "failed to submit data consumer: " << consumer->id() << ", group id: " << _grp_id;
            return Status::InternalError("failed to submit data consumer");
        } else {
            VLOG(1) << "submit a data consumer: " << consumer->id() << ", group id: " << _grp_id;
        }
    }

    // consuming from queue and put data to stream load pipe
    int64_t left_time = ctx->max_interval_s * 1000;
    int64_t received_rows = 0;
    int64_t left_bytes = ctx->max_batch_size;

    std::shared_ptr<PulsarConsumerPipe> pulsar_pipe = std::static_pointer_cast<PulsarConsumerPipe>(ctx->body_sink);

    LOG(INFO) << "start consumer group: " << _grp_id << ". max time(ms): " << left_time
              << ", batch size: " << left_bytes << ". " << ctx->brief();

    // copy one
    std::map<std::string, pulsar::MessageId> ack_offset = ctx->pulsar_info->ack_offset;

    //improve performance
    Status (PulsarConsumerPipe::*append_data)(const char* data, size_t size, char row_delimiter);
    char row_delimiter = '\n';
    if (ctx->format == TFileFormatType::FORMAT_JSON) {
        append_data = &PulsarConsumerPipe::append_json;
    } else {
        append_data = &PulsarConsumerPipe::append_with_row_delimiter;
        auto& per_node_scan_ranges = ctx->put_result.params.params.per_node_scan_ranges;

        if (!per_node_scan_ranges.empty()) {
            DCHECK_GE(per_node_scan_ranges.begin()->second.size(), 1);

            auto& scan_range = per_node_scan_ranges.begin()->second[0].scan_range;
            auto& params = scan_range.broker_scan_range.params;
            row_delimiter = static_cast<char>(params.row_delimiter);
        }
    }

    MonotonicStopWatch watch;
    watch.start();
    bool eos = false;
    while (true) {
        if (eos || left_time <= 0 || left_bytes <= 0) {
            LOG(INFO) << "consumer group done: " << _grp_id
                      << ". consume time(ms)=" << ctx->max_interval_s * 1000 - left_time
                      << ", received rows=" << received_rows << ", received bytes=" << ctx->max_batch_size - left_bytes
                      << ", eos: " << eos << ", left_time: " << left_time << ", left_bytes: " << left_bytes
                      << ", blocking get time(us): " << _queue.total_get_wait_time() / 1000
                      << ", blocking put time(us): " << _queue.total_put_wait_time() / 1000;

            // shutdown queue
            _queue.shutdown();
            // cancel all consumers
            for (auto& consumer : _consumers) {
                consumer->cancel(ctx);
            }

            // waiting all threads finished
            _thread_pool.shutdown();
            _thread_pool.join();

            if (!result_st.ok()) {
                // some consumers encounter errors, cancel this task
                return result_st;
            }

            if (left_bytes == ctx->max_batch_size) {
                // nothing to be consumed, we have to cancel it, because
                // we do not allow finishing stream load pipe without data
                pulsar_pipe->cancel(Status::Cancelled("Cancelled"));
                return Status::Cancelled("Cancelled");
            } else {
                DCHECK(left_bytes < ctx->max_batch_size);
                pulsar_pipe->finish();
                ctx->pulsar_info->ack_offset = std::move(ack_offset);
                ctx->receive_bytes = ctx->max_batch_size - left_bytes;
                ctx->rltask_statistics = RoutineLoadTaskStatistics(
                        ctx->max_interval_s * 1000 - left_time, _queue.total_get_wait_time() / 1000,
                        _queue.total_put_wait_time() / 1000, received_rows, ctx->receive_bytes, _consume_lags);
                update_ctx_info(ctx);
                return Status::OK();
            }
        }

        pulsar::Message* msg;
        bool res = _queue.blocking_get(&msg);
        if (res) {
            std::string partition = msg->getTopicName();
            pulsar::MessageId msg_id = msg->getMessageId();
            std::size_t len = msg->getLength();

            VLOG(3) << "get pulsar message"
                    << ", partition: " << partition << ", message id: " << msg_id << ", len: " << len;

            // Pulsar server might redeliver same messages when it's restart
            if (msg_id > ack_offset[partition]) {
                Status st;
                if (ctx->format == TFileFormatType::FORMAT_TDMSG_KV ||
                    ctx->format == TFileFormatType::FORMAT_TDMSG_CSV) {
                    std::list<tubemq::DataItem> data_items;
                    st = get_data_items(msg, &data_items);
                    if (st.ok()) {
                        for (auto& data_item : data_items) {
                            std::size_t tdmsg_len = data_item.GetLength();
                            st = (pulsar_pipe.get()->*append_data)(static_cast<const char*>(data_item.GetData()),
                                                                   static_cast<size_t>(tdmsg_len), row_delimiter);
                            if (st.ok()) {
                                received_rows++;
                                left_bytes -= tdmsg_len;
                            } else {
                                // failed to append this msg, we must stop
                                LOG(WARNING) << "failed to append msg to pipe. grp: " << _grp_id
                                             << ", errmsg=" << st.get_error_msg();
                                break;
                            }
                        }
                        ack_offset[partition] = msg_id;
                        if (_consume_lags.find(partition) == _consume_lags.end()) {
                            auto msg_timestamp = std::chrono::time_point<std::chrono::system_clock>(
                                    std::chrono::milliseconds(msg->getPublishTimestamp()));
                            _consume_lags[partition] = std::chrono::duration_cast<std::chrono::seconds>(
                                                               std::chrono::system_clock::now() - msg_timestamp)
                                                               .count();
                        }
                        VLOG(3) << "consume partition" << partition << " - " << msg_id;
                    } else {
                        LOG(WARNING) << "failed to append msg to pipe. grp: " << _grp_id
                                     << ", errmsg=" << st.get_error_msg();
                    }
                } else {
                    st = (pulsar_pipe.get()->*append_data)(static_cast<const char*>(msg->getData()),
                                                           static_cast<size_t>(len), row_delimiter);
                    if (st.ok()) {
                        received_rows++;
                        left_bytes -= len;
                        ack_offset[partition] = msg_id;
                         if (_consume_lags.find(partition) == _consume_lags.end()) {
                            auto msg_timestamp = std::chrono::time_point<std::chrono::system_clock>(
                                    std::chrono::milliseconds(msg->getPublishTimestamp()));
                            _consume_lags[partition] = std::chrono::duration_cast<std::chrono::seconds>(
                                                               std::chrono::system_clock::now() - msg_timestamp)
                                                               .count();
                        }
                        VLOG(3) << "consume partition" << partition << " - " << msg_id;
                    } else {
                        // failed to append this msg, we must stop
                        LOG(WARNING) << "failed to append msg to pipe. grp: " << _grp_id
                                     << ", errmsg=" << st.get_error_msg();
                    }
                }

                if (!st.ok()) {
                    eos = true;
                    {
                        std::unique_lock<std::mutex> lock(_mutex);
                        if (result_st.ok()) {
                            result_st = st;
                        }
                    }
                }
            } else {
                LOG(WARNING) << "Redelivering pulsar message, partition: " << partition
                             << ", message id[received]: " << msg_id
                             << ", message id[already_consumed]: " << ack_offset[partition];
            }
            delete msg;
        } else {
            // queue is empty and shutdown
            eos = true;
        }

        left_time = ctx->max_interval_s * 1000 - watch.elapsed_time() / 1000 / 1000;
    }

    return Status::OK();
}

void PulsarDataConsumerGroup::actual_consume(const std::shared_ptr<DataConsumer>& consumer,
                                             TimedBlockingQueue<pulsar::Message*>* queue, int64_t max_running_time_ms,
                                             const ConsumeFinishCallback& cb) {
    Status st = std::static_pointer_cast<PulsarDataConsumer>(consumer)->group_consume(queue, max_running_time_ms);
    cb(st);
}

Status PulsarDataConsumerGroup::get_data_items(const pulsar::Message* msg, std::list<tubemq::DataItem>* data_items) {
    std::string err_info;
    tubemq::TubeMQTDMsg tdmsg;
    if (tdmsg.ParseTDMsg(static_cast<const char*>(msg->getData()), msg->getLength(), err_info)) {
        for (auto attr_data : tdmsg.GetAttr2DataMap()) {
            data_items->splice(data_items->end(), attr_data.second);
        }
    } else {
        LOG(WARNING) << "failed to parse tube data: " << err_info;
        return Status::InternalError("PAUSE: failed to parse pulsar data: " + err_info);
    }

    return Status::OK();
}

void PulsarDataConsumerGroup::update_ctx_info(StreamLoadContext* ctx) {
    for (auto& cur_msg_id : ctx->pulsar_info->ack_offset) {
        std::string current_position;
        cur_msg_id.second.serialize(current_position);
        ctx->pulsar_info->current_positions[cur_msg_id.first] = current_position;
    }
}

TubeDataConsumerGroup::~TubeDataConsumerGroup() {
    // clean the msgs left in queue
    _queue.shutdown();
    while (true) {
        tubemq::ConsumerResult* consumer_result;
        if (_queue.blocking_get(&consumer_result)) {
            delete consumer_result;
            consumer_result = nullptr;
        } else {
            break;
        }
    }
    DCHECK(_queue.get_size() == 0);
}

Status TubeDataConsumerGroup::start_all(StreamLoadContext* ctx) {
    Status result_st = Status::OK();
    // start all consumers
    for (auto& consumer : _consumers) {
        if (!_thread_pool.offer([this, consumer, capture0 = &_queue, capture1 = ctx->max_interval_s * 1000,
                                 capture2 = [this, &result_st](const Status& st) {
                                     std::unique_lock<std::mutex> lock(_mutex);
                                     _counter--;
                                     VLOG(1) << "group counter is: " << _counter << ", grp: " << _grp_id;
                                     if (_counter == 0) {
                                         _queue.shutdown();
                                         LOG(INFO)
                                                 << "all consumers are finished. shutdown queue. group id: " << _grp_id;
                                     }
                                     if (result_st.ok() && !st.ok()) {
                                         result_st = st;
                                     }
                                 }] { actual_consume(consumer, capture0, capture1, capture2); })) {
            LOG(WARNING) << "failed to submit data consumer: " << consumer->id() << ", group id: " << _grp_id;
            return Status::InternalError("failed to submit data consumer");
        } else {
            VLOG(1) << "submit a data consumer: " << consumer->id() << ", group id: " << _grp_id;
        }
    }

    // consuming from queue and put data to stream load pipe
    int64_t left_time = ctx->max_interval_s * 1000;
    int64_t received_rows = 0;
    int64_t left_bytes = ctx->max_batch_size;

    std::shared_ptr<TubeConsumerPipe> tube_pipe = std::static_pointer_cast<TubeConsumerPipe>(ctx->body_sink);

    LOG(INFO) << "start consumer group: " << _grp_id << ". max time(ms): " << left_time
              << ", batch size: " << left_bytes << ". " << ctx->brief();

    // copy one
    std::map<std::string, int64_t> cmt_offset = ctx->tube_info->cmt_offset;

    //improve performance
    Status (TubeConsumerPipe::*append_data)(const char* data, size_t size, char row_delimiter);
    char row_delimiter = '\n';
    if (ctx->format == TFileFormatType::FORMAT_JSON) {
        append_data = &TubeConsumerPipe::append_json;
    } else {
        append_data = &TubeConsumerPipe::append_with_row_delimiter;
        auto& per_node_scan_ranges = ctx->put_result.params.params.per_node_scan_ranges;

        if (!per_node_scan_ranges.empty()) {
            DCHECK_GE(per_node_scan_ranges.begin()->second.size(), 1);

            auto& scan_range = per_node_scan_ranges.begin()->second[0].scan_range;
            auto& params = scan_range.broker_scan_range.params;
            row_delimiter = static_cast<char>(params.row_delimiter);
        }
    }

    MonotonicStopWatch watch;
    watch.start();
    bool eos = false;
    while (true) {
        if (eos || left_time <= 0 || left_bytes <= 0) {
            LOG(INFO) << "consumer group done: " << _grp_id
                      << ". consume time(ms)=" << ctx->max_interval_s * 1000 - left_time
                      << ", received rows=" << received_rows << ", received bytes=" << ctx->max_batch_size - left_bytes
                      << ", eos: " << eos << ", left_time: " << left_time << ", left_bytes: " << left_bytes
                      << ", blocking get time(us): " << _queue.total_get_wait_time() / 1000
                      << ", blocking put time(us): " << _queue.total_put_wait_time() / 1000;

            // shutdown queue
            _queue.shutdown();
            // cancel all consumers
            for (auto& consumer : _consumers) {
                consumer->cancel(ctx);
            }

            // waiting all threads finished
            _thread_pool.shutdown();
            _thread_pool.join();

            if (!result_st.ok()) {
                // some consumers encounter errors, cancel this task
                return result_st;
            }

            if (left_bytes == ctx->max_batch_size) {
                // nothing to be consumed, we have to cancel it, because
                // we do not allow finishing stream load pipe without data
                tube_pipe->cancel(Status::Cancelled("Cancelled"));
                return Status::Cancelled("Cancelled");
            } else {
                DCHECK(left_bytes < ctx->max_batch_size);
                tube_pipe->finish();
                ctx->receive_bytes = ctx->max_batch_size - left_bytes;
                ctx->tube_info->cmt_offset = std::move(cmt_offset);
                ctx->rltask_statistics = RoutineLoadTaskStatistics(
                        ctx->max_interval_s * 1000 - left_time, _queue.total_get_wait_time() / 1000,
                        _queue.total_put_wait_time() / 1000, received_rows, ctx->receive_bytes);
                return Status::OK();
            }
        }

        tubemq::ConsumerResult* consumer_result;
        bool res = _queue.blocking_get(&consumer_result);
        if (res) {
            for (auto& msg : consumer_result->GetMessageList()) {
                std::string topic = msg.GetTopic();
                VLOG(3) << "get tube message: " << topic << " - " << msg.GetDataLength();

                std::list<tubemq::DataItem> data_items;
                Status st = get_data_items(msg, &data_items);
                if (st.ok()) {
                    for (auto& data_item : data_items) {
                        std::size_t len = data_item.GetLength();
                        st = (tube_pipe.get()->*append_data)(static_cast<const char*>(data_item.GetData()),
                                                             static_cast<size_t>(len), row_delimiter);
                        if (st.ok()) {
                            received_rows++;
                            left_bytes -= len;
                            VLOG(3) << "consume parsed tube message: " << topic << " - " << len;
                        } else {
                            // failed to append this msg, we must stop
                            LOG(WARNING) << "failed to append msg to pipe. grp: " << _grp_id
                                         << ", errmsg=" << st.get_error_msg();
                            break;
                        }
                    }
                }

                // There could be tubemq parse error or inside pipe append error here
                if (!st.ok()) {
                    eos = true;
                    {
                        std::unique_lock<std::mutex> lock(_mutex);
                        if (result_st.ok()) {
                            result_st = st;
                        }
                    }
                    break;
                }
            }
            cmt_offset[consumer_result->GetPartitionKey()] = consumer_result->GetCurrOffset();
            delete consumer_result;
        } else {
            // queue is empty and shutdown
            eos = true;
        }

        left_time = ctx->max_interval_s * 1000 - watch.elapsed_time() / 1000 / 1000;
    }

    return Status::OK();
}

Status TubeDataConsumerGroup::get_data_items(const tubemq::Message& msg, std::list<tubemq::DataItem>* data_items) {
    std::string err_info;
    tubemq::TubeMQTDMsg tdmsg;
    if (tdmsg.ParseTDMsg(msg.GetData(), msg.GetDataLength(), err_info)) {
        for (auto attr_data : tdmsg.GetAttr2DataMap()) {
            data_items->splice(data_items->end(), attr_data.second);
        }
    } else {
        LOG(WARNING) << "failed to parse tube data: " << err_info;
        return Status::InternalError("PAUSE: failed to parse tube data: " + err_info);
    }

    return Status::OK();
}

void TubeDataConsumerGroup::actual_consume(const std::shared_ptr<DataConsumer>& consumer,
                                           TimedBlockingQueue<tubemq::ConsumerResult*>* queue,
                                           int64_t max_running_time_ms, const ConsumeFinishCallback& cb) {
    Status st = std::static_pointer_cast<TubeDataConsumer>(consumer)->group_consume(queue, max_running_time_ms);
    cb(st);
}

} // namespace starrocks
