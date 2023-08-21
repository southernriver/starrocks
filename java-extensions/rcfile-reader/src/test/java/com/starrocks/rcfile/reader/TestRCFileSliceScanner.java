package com.starrocks.rcfile.reader;

import com.starrocks.jni.connector.OffHeapTable;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.net.URL;
import java.util.HashMap;
import java.util.Map;

public class TestRCFileSliceScanner {

    @Before
    public void setUp() {
        System.setProperty("starrocks.fe.test", "1");
    }

    @After
    public void tearDown() {
        System.setProperty("starrocks.fe.test", "0");
    }

    /*
       CREATE TABLE `test_rcfile_array_map` (
         tdbank_imp_date STRING COMMENT 'partition fields',
         `uuid` STRING,
         `ts` int,
         `a` int,
         `b` string,
         `c` array<int>,
         `d` map<string, array<int>>)
       PARTITION BY LIST( tdbank_imp_date )
       (
           PARTITION p_20230724 VALUES IN ( '20230724' )
       )
       STORED AS RCFILE;
    */

    Map<String, String> createScanTestParams() {
        Map<String, String> params = new HashMap<>();
        URL resource = TestRCFileSliceScanner.class.getResource("/test_thive_rcfile");
        String basePath = resource.getPath();
        params.put("base_path", basePath);
        params.put("data_file_path", basePath + "/attempt_1684304420547_289756026_m_000000_0.1690622035043_3.rcf");
        params.put("hive_column_names", "tdbank_imp_date,uuid,ts,a,b,c,d");
        params.put("hive_column_types", "string#string#int#int#string#array<array<int>>#map<string,int>");
        params.put("data_file_length", "436081");
        params.put("required_fields", "tdbank_imp_date,uuid,ts,a,b,c,d");
        params.put("offset", "0");
        params.put("length", "297");
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
