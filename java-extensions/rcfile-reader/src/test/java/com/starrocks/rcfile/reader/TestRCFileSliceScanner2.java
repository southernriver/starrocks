package com.starrocks.rcfile.reader;

import com.starrocks.jni.connector.OffHeapTable;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.net.URL;
import java.util.HashMap;
import java.util.Map;

public class TestRCFileSliceScanner2 {

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
        URL resource = TestRCFileSliceScanner2.class.getResource("/test_thive_rcfile");
        String basePath = resource.getPath();
        params.put("base_path", basePath);
        params.put("data_file_path", basePath + "/attempt_1691983972110_18317082_m_000000_0.1692161597418_2.rcf");
        params.put("hive_column_names", "id,dt,name,id_hive_part");
        params.put("hive_column_types", "int#string#string#string");
        params.put("data_file_length", "235");
        params.put("required_fields", "id,dt,name,id_hive_part");
        params.put("offset", "0");
        params.put("length", "235");
        return params;
    }

    String runScanOnParams(Map<String, String> params) throws IOException {
        RCFileSliceScanner scanner = new RCFileSliceScanner(4096, params);
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
