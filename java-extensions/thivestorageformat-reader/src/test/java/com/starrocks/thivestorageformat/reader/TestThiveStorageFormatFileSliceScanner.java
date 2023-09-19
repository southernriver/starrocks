package com.starrocks.thivestorageformat.reader;

import com.starrocks.jni.connector.OffHeapTable;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.net.URL;
import java.util.HashMap;
import java.util.Map;

public class TestThiveStorageFormatFileSliceScanner {

    @Before
    public void setUp() {
        System.setProperty("starrocks.fe.test", "1");
    }

    @After
    public void tearDown() {
        System.setProperty("starrocks.fe.test", "0");
    }

    /*
       CREATE TABLE `test_rcfile_1` (
        id INT,
        dt STRING,
        name STRING)
       PARTITION BY LIST( id )
       (
            PARTITION p_2000 VALUES IN ( 2000, 2001, 2002 ),
            PARTITION p_2010 VALUES IN ( 2010, 2011, 2012),
            PARTITION p_2020 VALUES IN ( 2020, 2021, 2022),
            PARTITION default
       )
       STORED AS RCFILE;
    */

    Map<String, String> createScanTestParams() {
        Map<String, String> params = new HashMap<>();
        URL resource = TestThiveStorageFormatFileSliceScanner.class.getResource("/test_thive_storage_format");
        String basePath = resource.getPath();
        params.put("base_path", basePath);
        params.put("data_file_path", basePath + "/attempt_1693236853081_313524_m_000000_0.1693279009477");
        params.put("hive_column_names", "a,b");
        params.put("hive_column_types", "int#int");
        params.put("data_file_length", "203");
        params.put("required_fields", "a,b");
        params.put("offset", "0");
        params.put("length", "203");
        return params;
    }

    String runScanOnParams(Map<String, String> params) throws IOException {
        ThiveStorageFormatFileSliceScanner scanner = new ThiveStorageFormatFileSliceScanner(4096, params);
        System.out.println(scanner.toString());
        scanner.open();
        StringBuilder sb = new StringBuilder();
        while (true) {
            scanner.getNextOffHeapChunk();
            OffHeapTable table = scanner.getOffHeapTable();
            if (table.getNumRows() == 0) {
                break;
            }
            table.show(10);
            sb.append(table.dump(10));
            table.checkTableMeta(true);
            table.close();
        }
        scanner.close();
        return sb.toString();
    }

    @Test
    public void c1DoScanTestOnPrimitiveType() throws IOException {
        Map<String, String> params = createScanTestParams();
        System.out.println(runScanOnParams(params));
    }
}
