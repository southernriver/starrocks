package com.starrocks.analysis;

import com.starrocks.common.AnalysisException;
import com.starrocks.common.FeConstants;
import com.starrocks.qe.ConnectContext;
import com.starrocks.utframe.StarRocksAssert;
import com.starrocks.utframe.UtFrameUtils;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import java.util.Arrays;
import java.util.List;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import static com.starrocks.common.ErrorCode.ERROR_NO_WG_ERROR;

public class NodeResourceGroupStmtTest {
    private static final Pattern idPattern = Pattern.compile("\\bid=(\\b\\d+\\b)");
    private static StarRocksAssert starRocksAssert;
    private String createRg1Sql = "create resource group rg1\n" +
            "with (\n" +
            "    'type' = 'node_level',\n" +
            "    'be.number' = '3',\n" +
            "    'be.number.max' = '3'\n" +
            ");";

    private String createRg2Sql = "create resource group rg2\n" +
            "with (\n" +
            "    'type' = 'node_level',\n" +
            "    'be.number' = '2',\n" +
            "    'be.number.max' = '2'\n" +
            ");";

    @BeforeClass
    public static void setUp() throws Exception {
        UtFrameUtils.createMinStarRocksCluster();
        for (int i = 10002; i < 10005; i++) {
            UtFrameUtils.addMockBackend(i);
        }
        ConnectContext ctx = UtFrameUtils.createDefaultCtx();
        FeConstants.default_scheduler_interval_millisecond = 1;
        starRocksAssert = new StarRocksAssert(ctx);
        starRocksAssert.withRole("rg1_role1");
        starRocksAssert.withUser("rg1_user1", "rg1_role1");
        List<String> databases = Arrays.asList("db1", "db2");
        for (String db : databases) {
            starRocksAssert.withDatabase(db);
        }
    }


    @Test
    public void testCreateResourceGroup() throws Exception {
        List<List<String>> rows1 = starRocksAssert.executeResourceGroupShowSql("show resource groups all");
        String result1 = rowsToString(rows1);
        String expect1 = "" +
                "default_wg|0|0|0|0|0|0|NODE_LEVEL|(weight=0.0)|0|0|4|4|[]|[10001, 10002, 10003, 10004]";
        Assert.assertEquals(result1, expect1);

        createResourceGroups(new String[]{createRg1Sql});
        List<List<String>> rows2 = starRocksAssert.executeResourceGroupShowSql("show resource groups all");
        String result2 = rowsToString(rows2);
        String expect2 = "" +
                "default_wg|0|0|0|0|0|0|NODE_LEVEL|(weight=0.0)|0|0|1|1|[]|[10004]\n" +
                "rg1|0|0|0|0|0|0|NODE_LEVEL|(weight=0.0)|0|0|3|3|[]|[10001, 10002, 10003]";
        Assert.assertEquals(result2, expect2);
        dropResourceGroups(new String[]{"rg1"});
    }

    @Test
    public void testDropResourceGroup() throws Exception {
        createResourceGroups(new String[]{createRg1Sql});
        List<List<String>> rows1 = starRocksAssert.executeResourceGroupShowSql("show resource groups all");
        String result1 = rowsToString(rows1);
        String expect1 = "" +
                "default_wg|0|0|0|0|0|0|NODE_LEVEL|(weight=0.0)|0|0|1|1|[]|[10004]\n" +
                "rg1|0|0|0|0|0|0|NODE_LEVEL|(weight=0.0)|0|0|3|3|[]|[10001, 10002, 10003]";
        Assert.assertEquals(result1, expect1);
        dropResourceGroups(new String[]{"rg1"});

        List<List<String>> rows2 = starRocksAssert.executeResourceGroupShowSql("show resource groups all");
        String result2 = rowsToString(rows2);
        String expect2 = "" +
                "default_wg|0|0|0|0|0|0|NODE_LEVEL|(weight=0.0)|0|0|4|4|[]|[10001, 10002, 10003, 10004]";
        Assert.assertEquals(result2, expect2);
    }

    @Test
    public void testAlterResourceGroup() throws Exception {
        createResourceGroups(new String[]{createRg2Sql});
        List<List<String>> rows1 = starRocksAssert.executeResourceGroupShowSql("show resource groups all");
        String result1 = rowsToString(rows1);
        String expect1 = "" +
                "default_wg|0|0|0|0|0|0|NODE_LEVEL|(weight=0.0)|0|0|2|2|[]|[10003, 10004]\n" +
                "rg2|0|0|0|0|0|0|NODE_LEVEL|(weight=0.0)|0|0|2|2|[]|[10001, 10002]";
        Assert.assertEquals(result1, expect1);

        // Set be.number.max = 3
        String setMaxCmd = String.format("ALTER RESOURCE GROUP %s with ('be.number.max' = '%s' )", "rg2", "3");
        starRocksAssert.executeResourceGroupDdlSql(setMaxCmd);
        List<List<String>> afterSetMaxCmd = starRocksAssert.executeResourceGroupShowSql("show resource groups all");
        String expectMax = "" +
                "default_wg|0|0|0|0|0|0|NODE_LEVEL|(weight=0.0)|0|0|2|2|[]|[10003, 10004]\n" +
                "rg2|0|0|0|0|0|0|NODE_LEVEL|(weight=0.0)|0|0|2|3|[]|[10001, 10002]";
        Assert.assertEquals(rowsToString(afterSetMaxCmd), expectMax);

        // ADD BACKEND
        String alterSql2 = String.format("ALTER RESOURCE GROUP %s ADD BACKEND '%s'", "rg2", "10004");
        starRocksAssert.executeResourceGroupDdlSql(alterSql2);
        List<List<String>> rows3 = starRocksAssert.executeResourceGroupShowSql("show resource groups all");
        String result3 = rowsToString(rows3);
        String expect3 = "" +
                "default_wg|0|0|0|0|0|0|NODE_LEVEL|(weight=0.0)|0|0|1|1|[]|[10003]\n" +
                "rg2|0|0|0|0|0|0|NODE_LEVEL|(weight=0.0)|0|0|2|3|[]|[10001, 10002, 10004]";
        Assert.assertEquals(result3, expect3);

        // DROP BACKEND
        String alterSql = String.format("ALTER RESOURCE GROUP %s DROP BACKEND '%s'", "rg2", "10001");
        starRocksAssert.executeResourceGroupDdlSql(alterSql);
        List<List<String>> rows2 = starRocksAssert.executeResourceGroupShowSql("show resource groups all");
        String result2 = rowsToString(rows2);
        String expect2 = "" +
                "default_wg|0|0|0|0|0|0|NODE_LEVEL|(weight=0.0)|0|0|2|2|[]|[10001, 10003]\n" +
                "rg2|0|0|0|0|0|0|NODE_LEVEL|(weight=0.0)|0|0|2|3|[]|[10002, 10004]";
        Assert.assertEquals(result2, expect2);
        dropResourceGroups(new String[]{"rg2"});
    }

    private void createResourceGroups(String[] sqls) throws Exception {
        for (String sql : sqls) {
            starRocksAssert.executeResourceGroupDdlSql(sql);
        }
    }

    private void dropResourceGroups(String[] rgNames) throws Exception {
        for (String name : rgNames) {
            starRocksAssert.executeResourceGroupDdlSql("DROP RESOURCE GROUP " + name);
        }
        List<List<String>> rows = starRocksAssert.executeResourceGroupShowSql("show resource groups all");
        Assert.assertTrue(rows.size() == 1);
    }

    private void assertResourceGroupNotExist(String wg) {
        Assert.assertThrows(ERROR_NO_WG_ERROR.formatErrorMsg(wg), AnalysisException.class,
                () -> starRocksAssert.executeResourceGroupShowSql("show resource group " + wg));
    }

    private static String rowsToString(List<List<String>> rows) {

        List<String> lines = rows.stream().map(
                row -> {
                    row.remove(1);
                    return String.join("|", row.toArray(new String[0])).replaceAll("id=\\d+(,\\s+)?", "");
                }
        ).collect(Collectors.toList());
        return String.join("\n", lines.toArray(new String[0]));
    }

}
