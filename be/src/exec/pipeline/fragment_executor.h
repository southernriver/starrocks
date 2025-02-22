// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.

#pragma once

#include "common/status.h"
#include "exec/pipeline/pipeline_fwd.h"
#include "exec/workgroup/work_group_fwd.h"
#include "gen_cpp/InternalService_types.h"
#include "gutil/macros.h"

namespace starrocks {
class DataSink;
class ExecEnv;
class RuntimeProfile;
class TPlanFragmentExecParams;
class RuntimeState;

namespace pipeline {
class FragmentContext;
class PipelineBuilderContext;
class QueryContext;

// For the exec_batch_plan_fragments RPC request, common_request and unique_request are different.
// - common_request contains the common fields of all the fragment instances.
// - unique_request contains the unique fields for a specific fragment instance, including:
//    - backend_num
//    - pipeline_dop
//    - params.fragment_instance_id
//    - params.sender_id
//    - params.per_node_scan_ranges
//    - fragment.output_sink (only for MultiCastDataStreamSink and ExportSink)
// For the exec_plan_fragments request, common_request and unique_request are identical.

using PerDriverScanRangesMap = std::map<int32_t, std::vector<TScanRangeParams>>;
class UnifiedExecPlanFragmentParams {
public:
    UnifiedExecPlanFragmentParams(const TExecPlanFragmentParams& common_request,
                                  const TExecPlanFragmentParams& unique_request)
            : _common_request(common_request), _unique_request(unique_request) {
        DCHECK(unique_request.__isset.backend_num);
        DCHECK(unique_request.__isset.pipeline_dop);
        DCHECK(unique_request.__isset.params);
        DCHECK(unique_request.params.__isset.sender_id);
    }

    DISALLOW_COPY_AND_MOVE(UnifiedExecPlanFragmentParams);

    // Access the common fields by this method.
    const TExecPlanFragmentParams& common() const { return _common_request; }

    // Access the unique fields by the following methods.
    int32_t backend_num() const { return _unique_request.backend_num; }
    int32_t pipeline_dop() const { return _unique_request.__isset.pipeline_dop ? _unique_request.pipeline_dop : 0; }
    const TUniqueId& fragment_instance_id() const { return _unique_request.params.fragment_instance_id; }
    int32_t sender_id() const { return _unique_request.params.sender_id; }

    const std::vector<TScanRangeParams>& scan_ranges_of_node(TPlanNodeId node_id) const;
    const PerDriverScanRangesMap& per_driver_seq_scan_ranges_of_node(TPlanNodeId node_id) const;

    bool isset_output_sink() const {
        return _common_request.fragment.__isset.output_sink || _unique_request.fragment.__isset.output_sink;
    }
    const TDataSink& output_sink() const;

private:
    static const std::vector<TScanRangeParams> _no_scan_ranges;
    static const PerDriverScanRangesMap _no_scan_ranges_per_driver_seq;
    const TExecPlanFragmentParams& _common_request;
    const TExecPlanFragmentParams& _unique_request;
};

class FragmentExecutor {
public:
    FragmentExecutor();
    Status prepare(ExecEnv* exec_env, const TExecPlanFragmentParams& common_request,
                   const TExecPlanFragmentParams& unique_request);
    Status execute(ExecEnv* exec_env);

private:
    void _fail_cleanup();
    int32_t _calc_dop(ExecEnv* exec_env, const UnifiedExecPlanFragmentParams& request) const;
    int _calc_delivery_expired_seconds(const UnifiedExecPlanFragmentParams& request) const;
    int _calc_query_expired_seconds(const UnifiedExecPlanFragmentParams& request) const;

    // Several steps of prepare a fragment
    // 1. query context
    // 2. fragment context
    // 3. workgroup
    // 4. runtime state
    // 5. exec plan
    // 6. pipeline driver
    Status _prepare_query_ctx(ExecEnv* exec_env, const UnifiedExecPlanFragmentParams& request);
    Status _prepare_fragment_ctx(const UnifiedExecPlanFragmentParams& request);
    Status _prepare_workgroup(const UnifiedExecPlanFragmentParams& request);
    Status _prepare_runtime_state(ExecEnv* exec_env, const UnifiedExecPlanFragmentParams& request);
    Status _prepare_exec_plan(ExecEnv* exec_env, const UnifiedExecPlanFragmentParams& request);
    Status _prepare_global_dict(const UnifiedExecPlanFragmentParams& request);
    Status _prepare_pipeline_driver(ExecEnv* exec_env, const UnifiedExecPlanFragmentParams& request);

    Status _decompose_data_sink_to_operator(RuntimeState* runtime_state, PipelineBuilderContext* context,
                                            const UnifiedExecPlanFragmentParams& request,
                                            std::unique_ptr<starrocks::DataSink>& datasink);

    int64_t _fragment_start_time = 0;
    QueryContext* _query_ctx = nullptr;
    FragmentContextPtr _fragment_ctx = nullptr;
    workgroup::WorkGroupPtr _wg = nullptr;
};
} // namespace pipeline
} // namespace starrocks
