/*
 * Copyright 2021 DataCanvas
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.dingodb.test;

import io.dingodb.common.utils.JDBCUtils;
import io.dingodb.dailytest.PartitionParam;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;
import utils.FileReaderUtil;
import utils.YamlDataHelper;

import java.sql.SQLException;
import java.sql.Statement;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

public class TestPartitionParam extends YamlDataHelper {
    public static PartitionParam partitionObj = new PartitionParam();

    public void initTable(String tableName, String tableMetaPath) throws SQLException {
        String tableMeta = FileReaderUtil.readFile(tableMetaPath);
        partitionObj.tableCreate(tableName, tableMeta);
    }

    public void insertTableValue(String tableName, String insertFields, String tableValuePath) throws SQLException {
        String tableValues = FileReaderUtil.readFile(tableValuePath);
        partitionObj.insertTableValues(tableName, insertFields, tableValues);
    }

    @BeforeClass(alwaysRun = true, description = "测试开始前，连接数据库")
    public static void setupAll() {
        Assert.assertNotNull(partitionObj.connection);
    }

    @Test(priority = 0, enabled = true, dataProvider = "partitionParamMethod", description = "验证分区表创建成功")
    public void test01PartitionTableCreatePos(Map<String, String> param) throws SQLException {
        String tableMetaPath = param.get("metaPath");
        initTable(param.get("tableName"), tableMetaPath);
    }

    @Test(priority = 1, enabled = true, dataProvider = "partitionParamMethod", expectedExceptions = SQLException.class, description = "验证分区参数错误，创建表失败")
    public void test02PartitionTableCreateNeg(Map<String, String> param) throws SQLException {
        String tableMetaPath = param.get("metaPath");
        initTable(param.get("tableName"), tableMetaPath);
    }

    @AfterClass(alwaysRun = true, description = "测试完成后删除数据和表格并关闭连接")
    public void tearDownAll() throws SQLException, ClassNotFoundException {
        Statement tearDownStatement = null;
        List<String> tableList = Arrays.asList(
                "parttest2315", "parttest2317", "parttest2322_1", "parttest2322_2", "parttest2324",
                "parttest2326", "parttest2327", "parttest2328", "parttest2329", "parttest2330",
                "parttest2331", "parttest2332", "parttest2333", "parttest2334", "parttest2335",
                "parttest2336", "parttest2337", "parttest2338", "parttest2339", "parttest2340",
                "parttest2341", "parttest2346_1", "parttest2346_2", "parttest2347", "parttest2348",
                "parttest2349", "parttest2350", "parttest2351", "parttest2352", "parttest2353",
                "parttest2354", "parttest2674", "parttest2675", "parttest2676", "parttest2677",
                "parttest2678", "parttest2679", "parttest2680", "parttest2681", "parttest2682",
                "parttest2683", "parttest2684", "parttest2685", "parttest2686", "parttest2687",
                "parttest2688", "parttest2690", "parttest2694", "ttl_part_test2695"
        );
        try{
            tearDownStatement = partitionObj.connection.createStatement();
            if (tableList.size() > 0) {
                for(int i = 0; i < tableList.size(); i++) {
                    try {
                        tearDownStatement.execute("drop table " + tableList.get(i));
                    }catch (SQLException e) {
                        e.printStackTrace();
                    }
                }
            }
        } catch (SQLException e) {
            e.printStackTrace();
        }finally {
            JDBCUtils.closeResource(partitionObj.connection, tearDownStatement);
        }
    }

}
