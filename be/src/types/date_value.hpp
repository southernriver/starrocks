// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.

#pragma once

#include "types/date_value.h"
#include "types/timestamp_value.h"

namespace starrocks::vectorized {
inline DateValue::operator TimestampValue() const {
    return TimestampValue{date::to_timestamp(_julian)};
}

inline TimestampValue::operator DateValue() const {
    return DateValue{timestamp::to_julian(_timestamp)};
}

} // namespace starrocks::vectorized
