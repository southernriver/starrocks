//
// Created by root on 5/19/23.
//


#include "exec/vectorized/csv_scanner.h"

#include <gtest/gtest.h>

#include <iostream>

#include "column/chunk.h"
#include "column/datum_tuple.h"
#include "fs/fs_memory.h"
#include "gen_cpp/Descriptors_types.h"
#include "runtime/descriptor_helper.h"
#include "runtime/descriptors.h"
#include "runtime/mem_tracker.h"
#include "runtime/runtime_state.h"

namespace starrocks::vectorized {

class CSVScannerKvReaderTest : public ::testing::Test {
protected:
    void SetUp() override {}
    void TearDown() override {}

    std::unique_ptr<CSVScanner> create_csv_scanner(const std::vector<TypeDescriptor>& types,
                                                   const std::vector<TBrokerRangeDesc>& ranges,
                                                   const string& multi_row_delimiter = "\n",
                                                   const string& multi_column_separator = "|") {
        /// Init DescriptorTable
        TDescriptorTableBuilder desc_tbl_builder;
        TTupleDescriptorBuilder tuple_desc_builder;
        for (auto& t : types) {
            TSlotDescriptorBuilder slot_desc_builder;
            if (t.field_names.size() > 0) {
                slot_desc_builder.type(t)
                        .length(t.len)
                        .precision(t.precision)
                        .scale(t.scale)
                        .nullable(true)
                        .column_name(t.field_names[0]);
            } else {
                slot_desc_builder.type(t).length(t.len).precision(t.precision).scale(t.scale).nullable(true);
            }
            tuple_desc_builder.add_slot(slot_desc_builder.build());
        }
        tuple_desc_builder.build(&desc_tbl_builder);

        DescriptorTbl* desc_tbl = nullptr;
        Status st = DescriptorTbl::create(&_runtime_state, &_obj_pool, desc_tbl_builder.desc_tbl(), &desc_tbl,
                                          config::vector_chunk_size);
        CHECK(st.ok()) << st.to_string();

        /// Init RuntimeState
        RuntimeState* state = _obj_pool.add(new RuntimeState(TUniqueId(), TQueryOptions(), TQueryGlobals(), nullptr));
        state->set_desc_tbl(desc_tbl);
        state->init_instance_mem_tracker();

        /// TBrokerScanRangeParams
        TBrokerScanRangeParams* params = _obj_pool.add(new TBrokerScanRangeParams());
        params->__set_multi_row_delimiter(multi_row_delimiter);
        params->__set_multi_column_separator(multi_column_separator);
        params->strict_mode = true;
        params->dest_tuple_id = 0;
        params->src_tuple_id = 0;
        params->__set_ignore_tail_columns(true);
        for (int i = 0; i < types.size(); i++) {
            params->expr_of_dest_slot[i] = TExpr();
            params->expr_of_dest_slot[i].nodes.emplace_back(TExprNode());
            params->expr_of_dest_slot[i].nodes[0].__set_type(types[i].to_thrift());
            params->expr_of_dest_slot[i].nodes[0].__set_node_type(TExprNodeType::SLOT_REF);
            params->expr_of_dest_slot[i].nodes[0].__set_is_nullable(true);
            params->expr_of_dest_slot[i].nodes[0].__set_slot_ref(TSlotRef());
            params->expr_of_dest_slot[i].nodes[0].slot_ref.__set_slot_id(i);
            params->expr_of_dest_slot[i].nodes[0].__set_type(types[i].to_thrift());
        }

        for (int i = 0; i < types.size(); i++) {
            params->src_slot_ids.emplace_back(i);
        }

        RuntimeProfile* profile = _obj_pool.add(new RuntimeProfile("test_prof", true));

        ScannerCounter* counter = _obj_pool.add(new ScannerCounter());

        TBrokerScanRange* broker_scan_range = _obj_pool.add(new TBrokerScanRange());
        broker_scan_range->params = *params;
        broker_scan_range->ranges = ranges;
        return std::make_unique<CSVScanner>(state, profile, *broker_scan_range, counter);
    }

private:
    RuntimeState _runtime_state;
    ObjectPool _obj_pool;
};

TEST_F(CSVScannerKvReaderTest, test_normal_read) {
    std::vector<TypeDescriptor> types;
    types.emplace_back(TYPE_INT);
    types.emplace_back(TYPE_DOUBLE);
    types.emplace_back(TYPE_VARCHAR);
    types.emplace_back(TYPE_DATE);
    types.emplace_back(TYPE_INT);

    types[0].field_names.emplace_back("a1");
    types[1].field_names.emplace_back("a2");
    types[2].field_names.emplace_back("a3");
    types[3].field_names.emplace_back("a4");
    types[4].field_names.emplace_back("a5");

    types[2].len = 10;

    std::vector<TBrokerRangeDesc> ranges;
    TBrokerRangeDesc range_one;
    range_one.__set_path("./be/test/exec/test_data/csv_scanner/kv_reader/kv_file1");
    range_one.__set_start_offset(0);
    range_one.__set_num_of_columns_from_file(types.size());
    range_one.__set_format_type(TFileFormatType::FORMAT_TDMSG_KV);
    ranges.push_back(range_one);

    auto scanner = create_csv_scanner(types, ranges, "\n", ",");
    EXPECT_NE(scanner, nullptr);

    auto st = scanner->open();
    ASSERT_TRUE(st.ok()) << st.to_string();

    auto res = scanner->get_next();
    ASSERT_TRUE(res.ok()) << res.status().to_string();

    auto chunk = res.value();

    EXPECT_EQ(5, chunk->num_columns());
    EXPECT_EQ(3, chunk->num_rows());

    EXPECT_EQ(1, chunk->get(0)[0].get_int32());
    EXPECT_EQ(178, chunk->get(1)[0].get_int32());
    EXPECT_EQ(138, chunk->get(2)[0].get_int32());

    // double column
    EXPECT_FLOAT_EQ(2.5, chunk->get(0)[1].get_double());
    EXPECT_FLOAT_EQ(444, chunk->get(1)[1].get_double());
    EXPECT_TRUE(chunk->get(2)[1].is_null());

    //string column
    EXPECT_EQ("hello", chunk->get(0)[2].get_slice());
    EXPECT_EQ("ok", chunk->get(1)[2].get_slice());
    EXPECT_EQ("7k7k", chunk->get(2)[2].get_slice());

    // date column
    EXPECT_EQ("2020-01-01", chunk->get(0)[3].get_date().to_string());
    EXPECT_EQ("1998-09-01", chunk->get(1)[3].get_date().to_string());
    EXPECT_EQ("1997-09-01", chunk->get(2)[3].get_date().to_string());
}

TEST_F(CSVScannerKvReaderTest, test_adaptive_nullable_column1) {
    std::vector<TypeDescriptor> types{TypeDescriptor(TYPE_INT), TypeDescriptor(TYPE_VARCHAR), TypeDescriptor(TYPE_INT)};
    types[0].field_names.emplace_back("a1");
    types[1].field_names.emplace_back("a2");
    types[2].field_names.emplace_back("a3");

    std::vector<TBrokerRangeDesc> ranges;
    TBrokerRangeDesc range;
    range.__set_num_of_columns_from_file(3);
    range.__set_path("./be/test/exec/test_data/csv_scanner/kv_reader/kv_file20");
    range.__set_format_type(TFileFormatType::FORMAT_TDMSG_KV);
    ranges.push_back(range);

    auto scanner = create_csv_scanner(types, ranges, "\n", ",");
    Status st = scanner->open();
    ASSERT_TRUE(st.ok()) << st.to_string();

    ChunkPtr chunk = scanner->get_next().value();
    EXPECT_EQ(4, chunk->num_rows());

    EXPECT_EQ(true, chunk->get(0)[0].is_null());
    EXPECT_EQ(2, chunk->get(1)[0].get_int32());
    EXPECT_EQ(true, chunk->get(2)[0].is_null());
    EXPECT_EQ(4, chunk->get(3)[0].get_int32());

    EXPECT_EQ(true, chunk->get(0)[1].is_null());
    EXPECT_EQ(true, chunk->get(1)[1].is_null());
    EXPECT_EQ(true, chunk->get(2)[1].is_null());
    EXPECT_EQ("Julia", chunk->get(3)[1].get_slice());

    EXPECT_EQ(true, chunk->get(0)[2].is_null());
    EXPECT_EQ(true, chunk->get(1)[2].is_null());
    EXPECT_EQ(25, chunk->get(2)[2].get_int32());
    EXPECT_EQ(25, chunk->get(3)[2].get_int32());
}

TEST_F(CSVScannerKvReaderTest, test_adaptive_nullable_column2) {
    std::vector<TypeDescriptor> types{TypeDescriptor(TYPE_INT), TypeDescriptor(TYPE_VARCHAR), TypeDescriptor(TYPE_INT)};
    types[0].field_names.emplace_back("a1");
    types[1].field_names.emplace_back("a2");
    types[2].field_names.emplace_back("a3");

    std::vector<TBrokerRangeDesc> ranges;
    TBrokerRangeDesc range;
    range.__set_num_of_columns_from_file(3);
    range.__set_path("./be/test/exec/test_data/csv_scanner/kv_reader/kv_file21");
    range.__set_format_type(TFileFormatType::FORMAT_TDMSG_KV);
    ranges.push_back(range);

    auto scanner = create_csv_scanner(types, ranges, "\n", ",");
    Status st = scanner->open();
    ASSERT_TRUE(st.ok()) << st.to_string();

    ChunkPtr chunk = scanner->get_next().value();
    EXPECT_EQ(4, chunk->num_rows());

    EXPECT_EQ(1, chunk->get(0)[0].get_int32());
    EXPECT_EQ(2, chunk->get(1)[0].get_int32());
    EXPECT_EQ(3, chunk->get(2)[0].get_int32());
    EXPECT_EQ(true, chunk->get(3)[0].is_null());

    EXPECT_EQ("Julia", chunk->get(0)[1].get_slice());
    EXPECT_EQ("Andy", chunk->get(1)[1].get_slice());
    EXPECT_EQ("Joke", chunk->get(2)[1].get_slice());
    EXPECT_EQ(true, chunk->get(3)[1].is_null());

    EXPECT_EQ(20, chunk->get(0)[2].get_int32());
    EXPECT_EQ(21, chunk->get(1)[2].get_int32());
    EXPECT_EQ(22, chunk->get(2)[2].get_int32());
    EXPECT_EQ(25, chunk->get(3)[2].get_int32());
}

TEST_F(CSVScannerKvReaderTest, test_adaptive_nullable_column3) {
    std::vector<TypeDescriptor> types{TypeDescriptor(TYPE_INT), TypeDescriptor(TYPE_VARCHAR), TypeDescriptor(TYPE_INT)};
    types[0].field_names.emplace_back("a1");
    types[1].field_names.emplace_back("a2");
    types[2].field_names.emplace_back("a3");

    std::vector<TBrokerRangeDesc> ranges;
    TBrokerRangeDesc range;
    range.__set_num_of_columns_from_file(3);
    range.__set_path("./be/test/exec/test_data/csv_scanner/kv_reader/kv_file20");
    range.__set_format_type(TFileFormatType::FORMAT_TDMSG_KV);
    ranges.push_back(range);

    TBrokerRangeDesc range2;
    range2.__set_num_of_columns_from_file(3);
    range2.__set_path("./be/test/exec/test_data/csv_scanner/kv_reader/kv_file21");
    range2.__set_format_type(TFileFormatType::FORMAT_TDMSG_KV);
    ranges.push_back(range2);

    auto scanner = create_csv_scanner(types, ranges, "\n", ",");
    Status st = scanner->open();
    ASSERT_TRUE(st.ok()) << st.to_string();

    ChunkPtr chunk = scanner->get_next().value();
    EXPECT_EQ(4, chunk->num_rows());

    EXPECT_EQ(true, chunk->get(0)[0].is_null());
    EXPECT_EQ(2, chunk->get(1)[0].get_int32());
    EXPECT_EQ(true, chunk->get(2)[0].is_null());
    EXPECT_EQ(4, chunk->get(3)[0].get_int32());

    EXPECT_EQ(true, chunk->get(0)[1].is_null());
    EXPECT_EQ(true, chunk->get(1)[1].is_null());
    EXPECT_EQ(true, chunk->get(2)[1].is_null());
    EXPECT_EQ("Julia", chunk->get(3)[1].get_slice());

    EXPECT_EQ(true, chunk->get(0)[2].is_null());
    EXPECT_EQ(true, chunk->get(1)[2].is_null());
    EXPECT_EQ(25, chunk->get(2)[2].get_int32());
    EXPECT_EQ(25, chunk->get(3)[2].get_int32());

    chunk = scanner->get_next().value();
    EXPECT_EQ(4, chunk->num_rows());

    EXPECT_EQ(1, chunk->get(0)[0].get_int32());
    EXPECT_EQ(2, chunk->get(1)[0].get_int32());
    EXPECT_EQ(3, chunk->get(2)[0].get_int32());
    EXPECT_EQ(true, chunk->get(3)[0].is_null());

    EXPECT_EQ("Julia", chunk->get(0)[1].get_slice());
    EXPECT_EQ("Andy", chunk->get(1)[1].get_slice());
    EXPECT_EQ("Joke", chunk->get(2)[1].get_slice());
    EXPECT_EQ(true, chunk->get(3)[1].is_null());

    EXPECT_EQ(20, chunk->get(0)[2].get_int32());
    EXPECT_EQ(21, chunk->get(1)[2].get_int32());
    EXPECT_EQ(22, chunk->get(2)[2].get_int32());
    EXPECT_EQ(25, chunk->get(3)[2].get_int32());
}

TEST_F(CSVScannerKvReaderTest, test_multi_seprator) {
    std::vector<TypeDescriptor> types;
    types.emplace_back(TYPE_INT);
    types.emplace_back(TYPE_DOUBLE);
    types.emplace_back(TYPE_VARCHAR);
    types.emplace_back(TYPE_DATE);
    types.emplace_back(TYPE_VARCHAR);

    types[0].field_names.emplace_back("a1");
    types[1].field_names.emplace_back("a2");
    types[2].field_names.emplace_back("a3");
    types[3].field_names.emplace_back("a4");
    types[4].field_names.emplace_back("a5");

    types[2].len = 10;
    types[4].len = 6;

    std::vector<TBrokerRangeDesc> ranges;
    TBrokerRangeDesc range_one;
    range_one.__set_path("./be/test/exec/test_data/csv_scanner/kv_reader/kv_file14");
    range_one.__set_start_offset(0);
    range_one.__set_num_of_columns_from_file(types.size());
    range_one.__set_format_type(TFileFormatType::FORMAT_TDMSG_KV);
    ranges.push_back(range_one);

    auto scanner = create_csv_scanner(types, ranges, "<br>", "^^");
    EXPECT_NE(scanner, nullptr);

    auto st = scanner->open();
    ASSERT_TRUE(st.ok()) << st.to_string();

    auto res = scanner->get_next();
    ASSERT_TRUE(res.ok()) << res.status().to_string();
    auto chunk = res.value();

    EXPECT_EQ(5, chunk->num_columns());
    EXPECT_EQ(2, chunk->num_rows());

    // int column
    EXPECT_EQ(1, chunk->get(0)[0].get_int32());
    EXPECT_EQ(-1, chunk->get(1)[0].get_int32());

    // double column
    EXPECT_FLOAT_EQ(1.1, chunk->get(0)[1].get_double());
    EXPECT_FLOAT_EQ(-0.1, chunk->get(1)[1].get_double());

    // string column
    EXPECT_EQ("ap", chunk->get(0)[2].get_slice());
    EXPECT_EQ("br", chunk->get(1)[2].get_slice());

    // date column
    EXPECT_EQ("2020-01-01", chunk->get(0)[3].get_date().to_string());
    EXPECT_EQ("1998-09-01", chunk->get(1)[3].get_date().to_string());
}



TEST_F(CSVScannerKvReaderTest, test_array_of_int) {
    TypeDescriptor t;
    t.type = TYPE_ARRAY;
    t.field_names.emplace_back("a1");
    t.children.emplace_back(TYPE_INT);

    std::vector<TBrokerRangeDesc> ranges;
    TBrokerRangeDesc range;
    range.__set_path("./be/test/exec/test_data/csv_scanner/kv_reader/kv_file3");
    range.__set_start_offset(0);
    range.__set_num_of_columns_from_file(1);
    range.__set_format_type(TFileFormatType::FORMAT_TDMSG_KV);
    ranges.push_back(range);

    auto scanner = create_csv_scanner({t}, ranges);
    EXPECT_NE(scanner, nullptr);

    auto st = scanner->open();
    ASSERT_TRUE(st.ok()) << st.to_string();

    auto chunk = scanner->get_next().value();
    EXPECT_EQ(1, chunk->num_columns());
    EXPECT_EQ(5, chunk->num_rows());

    // 1st row
    EXPECT_EQ(0, chunk->get(0)[0].get_array().size());

    // 2nd row
    EXPECT_EQ(1, chunk->get(1)[0].get_array().size());
    EXPECT_TRUE(chunk->get(1)[0].get_array()[0].is_null());

    // 3rd row
    EXPECT_TRUE(chunk->get(2)[0].is_null());

    // 4th row
    EXPECT_EQ(2, chunk->get(3)[0].get_array().size());
    EXPECT_EQ(1, chunk->get(3)[0].get_array()[0].get_int32());
    EXPECT_EQ(2, chunk->get(3)[0].get_array()[1].get_int32());

    // 5th row
    EXPECT_EQ(2, chunk->get(4)[0].get_array().size());
    EXPECT_EQ(1, chunk->get(4)[0].get_array()[0].get_int32());
    EXPECT_TRUE(chunk->get(4)[0].get_array()[1].is_null());
}

TEST_F(CSVScannerKvReaderTest, test_array_of_string) {
    std::vector<TypeDescriptor> types;
    // ARRAY<VARCHAR(10)>
    TypeDescriptor t;
    t.type = TYPE_ARRAY;
    t.field_names.emplace_back("a1");
    t.children.emplace_back(TYPE_VARCHAR);
    t.children.back().len = 10;

    types.emplace_back(t);

    std::vector<TBrokerRangeDesc> ranges;
    TBrokerRangeDesc range;
    range.__set_path("./be/test/exec/test_data/csv_scanner/kv_reader/kv_file4");
    range.__set_start_offset(0);
    range.__set_num_of_columns_from_file(types.size());
    range.__set_format_type(TFileFormatType::FORMAT_TDMSG_KV);
    ranges.push_back(range);

    auto scanner = create_csv_scanner(types, ranges);
    EXPECT_NE(scanner, nullptr);

    Status st = scanner->open();
    ASSERT_TRUE(st.ok()) << st.to_string();
    auto res = scanner->get_next();
    ASSERT_TRUE(res.ok()) << res.status().to_string();
    auto chunk = res.value();
    EXPECT_EQ(1, chunk->num_columns());
    EXPECT_EQ(6, chunk->num_rows());

    // []
    EXPECT_EQ(0, chunk->get(0)[0].get_array().size());

    // [null]
    EXPECT_EQ(1, chunk->get(1)[0].get_array().size());
    EXPECT_TRUE(chunk->get(1)[0].get_array()[0].is_null());

    // \N
    EXPECT_TRUE(chunk->get(2)[0].is_null());

    // ["apple",null,"pear"]
    EXPECT_EQ(3, chunk->get(3)[0].get_array().size());
    EXPECT_EQ("apple", chunk->get(3)[0].get_array()[0].get_slice());
    EXPECT_TRUE(chunk->get(3)[0].get_array()[1].is_null());
    EXPECT_EQ("pear", chunk->get(3)[0].get_array()[2].get_slice());

    // ["str with left bracket([)","str with dot(,)"]
    EXPECT_EQ(2, chunk->get(4)[0].get_array().size());
    EXPECT_EQ("str with left bracket([)", chunk->get(4)[0].get_array()[0].get_slice());
    EXPECT_EQ("str with dot(,)", chunk->get(4)[0].get_array()[1].get_slice());

    // ["I""m hungry!",""]
    EXPECT_EQ(2, chunk->get(5)[0].get_array().size());
    EXPECT_EQ("I\"m hungry!", chunk->get(5)[0].get_array()[0].get_slice());
    EXPECT_EQ("", chunk->get(5)[0].get_array()[1].get_slice());
}

TEST_F(CSVScannerKvReaderTest, test_array_of_date) {
    TypeDescriptor t;
    t.type = TYPE_ARRAY;
    t.field_names.emplace_back("a1");
    t.children.emplace_back(TYPE_DATE);

    std::vector<TBrokerRangeDesc> ranges;
    TBrokerRangeDesc range;
    range.__set_path("./be/test/exec/test_data/csv_scanner/kv_reader/kv_file5");
    range.__set_start_offset(0);
    range.__set_num_of_columns_from_file(1);
    range.__set_format_type(TFileFormatType::FORMAT_TDMSG_KV);
    ranges.push_back(range);

    auto scanner = create_csv_scanner({t}, ranges);
    EXPECT_NE(scanner, nullptr);

    Status st;

    st = scanner->open();
    ASSERT_TRUE(st.ok()) << st.to_string();
    auto res = scanner->get_next();
    ASSERT_TRUE(res.ok()) << res.status().to_string();
    auto chunk = res.value();
    EXPECT_EQ(1, chunk->num_columns());
    EXPECT_EQ(5, chunk->num_rows());

    // []
    EXPECT_EQ(0, chunk->get(0)[0].get_array().size());

    // [null]
    EXPECT_EQ(1, chunk->get(1)[0].get_array().size());
    EXPECT_TRUE(chunk->get(1)[0].get_array()[0].is_null());

    // \N
    EXPECT_TRUE(chunk->get(2)[0].is_null());

    // ["2020-01-01","2021-01-01"]
    EXPECT_EQ(2, chunk->get(3)[0].get_array().size());
    EXPECT_EQ("2020-01-01", chunk->get(3)[0].get_array()[0].get_date().to_string());
    EXPECT_EQ("2021-01-01", chunk->get(3)[0].get_array()[1].get_date().to_string());

    // ["2022-01-01",null]
    EXPECT_EQ(2, chunk->get(4)[0].get_array().size());
    EXPECT_EQ("2022-01-01", chunk->get(4)[0].get_array()[0].get_date().to_string());
    EXPECT_TRUE(chunk->get(4)[0].get_array()[1].is_null());
}

TEST_F(CSVScannerKvReaderTest, test_nested_array_of_int) {
    TypeDescriptor t(TYPE_ARRAY);
    t.children.emplace_back(TYPE_ARRAY);
    t.field_names.emplace_back("a1");
    t.children.back().children.emplace_back(TYPE_INT);

    std::vector<TBrokerRangeDesc> ranges;
    TBrokerRangeDesc range;
    range.__set_path("./be/test/exec/test_data/csv_scanner/kv_reader/kv_file6");
    range.__set_start_offset(0);
    range.__set_num_of_columns_from_file(1);
    range.__set_format_type(TFileFormatType::FORMAT_TDMSG_KV);
    ranges.push_back(range);

    auto scanner = create_csv_scanner({t}, ranges);
    EXPECT_NE(scanner, nullptr);

    Status st = scanner->open();
    ASSERT_TRUE(st.ok()) << st.to_string();
    ChunkPtr chunk = scanner->get_next().value();

    EXPECT_EQ(8, chunk->num_rows());

    // []
    EXPECT_EQ(0, chunk->get(0)[0].get_array().size());

    // [[]]
    EXPECT_EQ(1, chunk->get(1)[0].get_array().size());
    EXPECT_EQ(0, chunk->get(1)[0].get_array()[0].get_array().size());

    // \N
    EXPECT_TRUE(chunk->get(2)[0].is_null());

    // [[1,2,3]]
    EXPECT_EQ(1, chunk->get(3)[0].get_array().size());
    //  -> [1,2,3]
    EXPECT_EQ(3, chunk->get(3)[0].get_array()[0].get_array().size());
    EXPECT_EQ(1, chunk->get(3)[0].get_array()[0].get_array()[0].get_int32());
    EXPECT_EQ(2, chunk->get(3)[0].get_array()[0].get_array()[1].get_int32());
    EXPECT_EQ(3, chunk->get(3)[0].get_array()[0].get_array()[2].get_int32());

    // [[1],[2],[3]]
    EXPECT_EQ(3, chunk->get(4)[0].get_array().size());
    //  -> [1]
    EXPECT_EQ(1, chunk->get(4)[0].get_array()[0].get_array().size());
    EXPECT_EQ(1, chunk->get(4)[0].get_array()[0].get_array()[0].get_int32());
    //  -> [2]
    EXPECT_EQ(1, chunk->get(4)[0].get_array()[1].get_array().size());
    EXPECT_EQ(2, chunk->get(4)[0].get_array()[1].get_array()[0].get_int32());
    //  -> [3]
    EXPECT_EQ(1, chunk->get(4)[0].get_array()[2].get_array().size());
    EXPECT_EQ(3, chunk->get(4)[0].get_array()[2].get_array()[0].get_int32());

    // [[1,2],[3]]
    EXPECT_EQ(2, chunk->get(5)[0].get_array().size());
    //  -> [1,2]
    EXPECT_EQ(2, chunk->get(5)[0].get_array()[0].get_array().size());
    EXPECT_EQ(1, chunk->get(5)[0].get_array()[0].get_array()[0].get_int32());
    EXPECT_EQ(2, chunk->get(5)[0].get_array()[0].get_array()[1].get_int32());
    //  -> [3]
    EXPECT_EQ(1, chunk->get(5)[0].get_array()[1].get_array().size());
    EXPECT_EQ(3, chunk->get(5)[0].get_array()[1].get_array()[0].get_int32());

    // [null]
    EXPECT_EQ(1, chunk->get(6)[0].get_array().size());
    EXPECT_TRUE(chunk->get(6)[0].get_array()[0].is_null());

    // [[null]]
    EXPECT_EQ(1, chunk->get(7)[0].get_array().size());
    EXPECT_EQ(1, chunk->get(7)[0].get_array()[0].get_array().size());
    EXPECT_TRUE(chunk->get(7)[0].get_array()[0].get_array()[0].is_null());
}

TEST_F(CSVScannerKvReaderTest, test_invalid_field_as_null) {
    std::vector<TypeDescriptor> types{TypeDescriptor(TYPE_INT)};
    types[0].field_names.emplace_back("a1");

    std::vector<TBrokerRangeDesc> ranges;
    TBrokerRangeDesc range;
    range.__set_path("./be/test/exec/test_data/csv_scanner/kv_reader/kv_file7");
    range.__set_start_offset(0);
    range.__set_num_of_columns_from_file(types.size());
    range.__set_format_type(TFileFormatType::FORMAT_TDMSG_KV);
    ranges.push_back(range);

    auto scanner = create_csv_scanner({types}, ranges);
    EXPECT_NE(scanner, nullptr);

    Status st;

    st = scanner->open();
    ASSERT_TRUE(st.ok()) << st.to_string();

    ChunkPtr chunk = scanner->get_next().value();
    ASSERT_TRUE(st.ok()) << st.to_string();
    EXPECT_EQ(3, chunk->num_rows());

    EXPECT_TRUE(chunk->get(0)[0].is_null());
    EXPECT_TRUE(chunk->get(1)[0].is_null());
    EXPECT_TRUE(chunk->get(2)[0].is_null());
}

TEST_F(CSVScannerKvReaderTest, test_invalid_field_of_array_as_null) {
    std::vector<TypeDescriptor> types{TypeDescriptor(TYPE_ARRAY)};
    types[0].children.emplace_back(TYPE_INT);
    types[0].field_names.emplace_back("a1");

    std::vector<TBrokerRangeDesc> ranges;
    TBrokerRangeDesc range;
    range.__set_path("./be/test/exec/test_data/csv_scanner/kv_reader/kv_file8");
    range.__set_start_offset(0);
    range.__set_num_of_columns_from_file(types.size());
    range.__set_format_type(TFileFormatType::FORMAT_TDMSG_KV);
    ranges.push_back(range);

    auto scanner = create_csv_scanner(types, ranges);
    EXPECT_NE(scanner, nullptr);

    Status st;

    st = scanner->open();
    ASSERT_TRUE(st.ok()) << st.to_string();

    ChunkPtr chunk = scanner->get_next().value();
    EXPECT_EQ(3, chunk->num_rows());

    EXPECT_EQ(1, chunk->get(0)[0].is_null());
    EXPECT_EQ(1, chunk->get(1)[0].is_null());
    EXPECT_EQ(1, chunk->get(2)[0].is_null());
}

TEST_F(CSVScannerKvReaderTest, test_start_offset) {
    std::vector<TypeDescriptor> types{TypeDescriptor(TYPE_INT), TypeDescriptor(TYPE_INT)};
    types[0].field_names.emplace_back("a1");
    types[1].field_names.emplace_back("a2");

    std::vector<TBrokerRangeDesc> ranges;
    TBrokerRangeDesc range;
    range.__set_num_of_columns_from_file(2);
    range.__set_start_offset(11);
    range.__set_size(20);
    range.__set_path("./be/test/exec/test_data/csv_scanner/kv_reader/kv_file9");
    range.__set_format_type(TFileFormatType::FORMAT_TDMSG_KV);
    ranges.push_back(range);

    auto scanner = create_csv_scanner(types, ranges);
    Status st = scanner->open();
    ASSERT_TRUE(st.ok()) << st.to_string();

    ChunkPtr chunk = scanner->get_next().value();
    ASSERT_TRUE(st.ok()) << st.to_string();
    EXPECT_EQ(2, chunk->num_rows());

    EXPECT_EQ(5, chunk->get(0)[0].get_int32());
    EXPECT_EQ(7, chunk->get(1)[0].get_int32());

    EXPECT_EQ(6, chunk->get(0)[1].get_int32());
    EXPECT_EQ(8, chunk->get(1)[1].get_int32());
}

TEST_F(CSVScannerKvReaderTest, test_file_not_ended_with_record_delimiter) {
    std::vector<TypeDescriptor> types{TypeDescriptor(TYPE_INT), TypeDescriptor(TYPE_INT)};
    types[0].field_names.emplace_back("a1");
    types[1].field_names.emplace_back("a2");

    std::vector<TBrokerRangeDesc> ranges;
    TBrokerRangeDesc range;
    range.__set_start_offset(0);
    range.__set_num_of_columns_from_file(types.size());
    range.__set_path("./be/test/exec/test_data/csv_scanner/kv_reader/kv_file10");
    range.__set_format_type(TFileFormatType::FORMAT_TDMSG_KV);
    ranges.push_back(range);

    auto scanner = create_csv_scanner(types, ranges);
    Status st = scanner->open();
    ASSERT_TRUE(st.ok()) << st.to_string();

    ChunkPtr chunk = scanner->get_next().value();
    EXPECT_EQ(5, chunk->num_rows());

    EXPECT_EQ(1, chunk->get(0)[0].get_int32());
    EXPECT_EQ(3, chunk->get(1)[0].get_int32());
    EXPECT_EQ(5, chunk->get(2)[0].get_int32());
    EXPECT_EQ(7, chunk->get(3)[0].get_int32());
    EXPECT_EQ(9, chunk->get(4)[0].get_int32());

    EXPECT_EQ(2, chunk->get(0)[1].get_int32());
    EXPECT_EQ(4, chunk->get(1)[1].get_int32());
    EXPECT_EQ(6, chunk->get(2)[1].get_int32());
    EXPECT_EQ(8, chunk->get(3)[1].get_int32());
    EXPECT_EQ(0, chunk->get(4)[1].get_int32());
}

TEST_F(CSVScannerKvReaderTest, test_empty) {
    auto run_test = [this](LogicalType pt) {
        std::vector<TypeDescriptor> types{TypeDescriptor(pt)};
        if (pt == TYPE_VARCHAR || pt == TYPE_CHAR) {
            types[0].len = 10;
        }
        types[0].field_names.emplace_back("a1");
        std::vector<TBrokerRangeDesc> ranges;
        TBrokerRangeDesc range;
        range.__set_start_offset(0);
        range.__set_path("./be/test/exec/test_data/csv_scanner/kv_reader/kv_file13");
        range.__set_num_of_columns_from_file(types.size());
        range.__set_format_type(TFileFormatType::FORMAT_TDMSG_KV);
        ranges.push_back(range);

        auto scanner = create_csv_scanner(types, ranges);
        ASSERT_TRUE(scanner->open().ok());
        auto res = scanner->get_next();
        ASSERT_TRUE(res.status().is_end_of_file());
    };
    run_test(TYPE_VARCHAR);
    run_test(TYPE_CHAR);
    run_test(TYPE_INT);
    run_test(TYPE_DATE);
    run_test(TYPE_DATETIME);
}

TEST_F(CSVScannerKvReaderTest, test_dynamic_column){
    std::vector<TypeDescriptor> types{TypeDescriptor(TYPE_INT), TypeDescriptor(TYPE_VARCHAR), TypeDescriptor(TYPE_INT)};
    types[0].field_names.emplace_back("a1");
    types[1].field_names.emplace_back("a2");
    types[2].field_names.emplace_back("a3");

    std::vector<TBrokerRangeDesc> ranges;
    TBrokerRangeDesc range;
    range.__set_num_of_columns_from_file(3);
    range.__set_path("./be/test/exec/test_data/csv_scanner/kv_reader/kv_file22");
    range.__set_format_type(TFileFormatType::FORMAT_TDMSG_KV);
    ranges.push_back(range);

    auto scanner = create_csv_scanner(types, ranges, "\n", ",");

    Status st = scanner->open();
    ASSERT_TRUE(st.ok()) << st.to_string();

    ChunkPtr chunk = scanner->get_next().value();
    EXPECT_EQ(4, chunk->num_rows());

    EXPECT_EQ(1, chunk->get(0)[0].get_int32());
    EXPECT_EQ(2, chunk->get(1)[0].get_int32());
    EXPECT_EQ(22, chunk->get(2)[0].get_int32());
    EXPECT_EQ(true, chunk->get(3)[0].is_null());

    EXPECT_EQ("Julia", chunk->get(0)[1].get_slice());
    EXPECT_EQ("Andy", chunk->get(1)[1].get_slice());
    EXPECT_EQ("Joke", chunk->get(2)[1].get_slice());
    EXPECT_EQ(true, chunk->get(3)[1].is_null());

    EXPECT_EQ(20, chunk->get(0)[2].get_int32());
    EXPECT_EQ(21, chunk->get(1)[2].get_int32());
    EXPECT_EQ(3, chunk->get(2)[2].get_int32());
    EXPECT_EQ(25, chunk->get(3)[2].get_int32());
}

TEST_F(CSVScannerKvReaderTest, test_empty_kv){
    std::vector<TypeDescriptor> types{TypeDescriptor(TYPE_INT), TypeDescriptor(TYPE_VARCHAR), TypeDescriptor(TYPE_INT)};
    types[0].field_names.emplace_back("a1");
    types[1].field_names.emplace_back("a2");
    types[2].field_names.emplace_back("a3");

    std::vector<TBrokerRangeDesc> ranges;
    TBrokerRangeDesc range;
    range.__set_num_of_columns_from_file(3);
    range.__set_path("./be/test/exec/test_data/csv_scanner/kv_reader/kv_file23");
    range.__set_format_type(TFileFormatType::FORMAT_TDMSG_KV);
    ranges.push_back(range);

    auto scanner = create_csv_scanner(types, ranges, "\n", ",");

    Status st = scanner->open();
    ASSERT_TRUE(st.ok()) << st.to_string();

    ChunkPtr chunk = scanner->get_next().value();
    EXPECT_EQ(4, chunk->num_rows());

    EXPECT_EQ(1, chunk->get(0)[0].get_int32());
    EXPECT_EQ(2, chunk->get(1)[0].get_int32());
    EXPECT_EQ(22, chunk->get(2)[0].get_int32());
    EXPECT_EQ(true, chunk->get(3)[0].is_null());

    EXPECT_EQ("Julia", chunk->get(0)[1].get_slice());
    EXPECT_EQ("Andy", chunk->get(1)[1].get_slice());
    EXPECT_EQ("Joke", chunk->get(2)[1].get_slice());
    EXPECT_EQ(true, chunk->get(3)[1].is_null());

    EXPECT_EQ(20, chunk->get(0)[2].get_int32());
    EXPECT_EQ(21, chunk->get(1)[2].get_int32());
    EXPECT_EQ(3, chunk->get(2)[2].get_int32());
    EXPECT_EQ(25, chunk->get(3)[2].get_int32());
}

TEST_F(CSVScannerKvReaderTest, test_multi_col_empty_kv){
    std::vector<TypeDescriptor> types{TypeDescriptor(TYPE_INT), TypeDescriptor(TYPE_VARCHAR), TypeDescriptor(TYPE_INT)};
    types[0].field_names.emplace_back("a1");
    types[1].field_names.emplace_back("a2");
    types[2].field_names.emplace_back("a3");

    std::vector<TBrokerRangeDesc> ranges;
    TBrokerRangeDesc range;
    range.__set_num_of_columns_from_file(3);
    range.__set_path("./be/test/exec/test_data/csv_scanner/kv_reader/kv_file24");
    range.__set_format_type(TFileFormatType::FORMAT_TDMSG_KV);
    ranges.push_back(range);

    auto scanner = create_csv_scanner(types, ranges, "\n", "&&");

    Status st = scanner->open();
    ASSERT_TRUE(st.ok()) << st.to_string();

    ChunkPtr chunk = scanner->get_next().value();
    EXPECT_EQ(4, chunk->num_rows());

    EXPECT_EQ(1, chunk->get(0)[0].get_int32());
    EXPECT_EQ(2, chunk->get(1)[0].get_int32());
    EXPECT_EQ(22, chunk->get(2)[0].get_int32());
    EXPECT_EQ(true, chunk->get(3)[0].is_null());

    EXPECT_EQ("Julia", chunk->get(0)[1].get_slice());
    EXPECT_EQ("Andy", chunk->get(1)[1].get_slice());
    EXPECT_EQ("Joke", chunk->get(2)[1].get_slice());
    EXPECT_EQ(true, chunk->get(3)[1].is_null());

    EXPECT_EQ(20, chunk->get(0)[2].get_int32());
    EXPECT_EQ(21, chunk->get(1)[2].get_int32());
    EXPECT_EQ(3, chunk->get(2)[2].get_int32());
    EXPECT_EQ(25, chunk->get(3)[2].get_int32());
}

}