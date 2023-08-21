// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.

#pragma once

#include <unordered_map>
#include <gen_cpp/Descriptors_types.h>
#include <gen_cpp/tablet_schema.pb.h>

#include "common/status.h"
#include "gen_cpp/Types_types.h"
#include "olap_common.h"

namespace starrocks {

class TTabletSchema;
enum RowsetTypePB : int;

enum class FieldTypeVersion {
    kV1,
    kV2,
};

Status convert_t_schema_to_pb_schema(const TTabletSchema& tablet_schema, uint32_t next_unique_id,
                                     const std::unordered_map<uint32_t, uint32_t>& col_ordinal_to_unique_id,
                                     TabletSchemaPB* schema, TCompressionType::type compression_type);

void convert_to_new_version(TColumn* tcolumn);

Status t_column_to_pb_column(int32_t unique_id, const TColumn& t_column, FieldTypeVersion v, ColumnPB* column_pb,
                                    size_t depth = 0);

} // namespace starrocks