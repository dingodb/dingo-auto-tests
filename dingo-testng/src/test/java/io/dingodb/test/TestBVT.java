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
import io.dingodb.dailytest.DailyBVT;
import listener.EmailableReporterListener;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Listeners;
import org.testng.annotations.Test;
import utils.FileReaderUtil;
import utils.YamlDataHelper;

import java.sql.SQLException;
import java.sql.Statement;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

@Listeners(EmailableReporterListener.class)
public class TestBVT extends YamlDataHelper {
    public static DailyBVT bvtObj = new DailyBVT();

    public void initTable(String tableName, String tableMetaPath) throws SQLException {
        String tableMeta = FileReaderUtil.readFile(tableMetaPath);
        bvtObj.tableCreate(tableName, tableMeta);
    }

    public void insertTableValue(String tableName, String insertFields, String tableValuePath) throws SQLException {
        String tableValues = FileReaderUtil.readFile(tableValuePath);
        bvtObj.insertTableValues(tableName, insertFields, tableValues);
    }

    @BeforeClass(alwaysRun = true, groups = {"BVT"}, description = "测试开始前，连接数据库")
    public static void setUpAll() throws ClassNotFoundException, SQLException {
        Assert.assertNotNull(DailyBVT.connection);
    }

    @Test(priority = 0, groups = {"BVT"}, description = "验证创建表成功后，获取表名成功")
    public void test01TableCreate1() throws Exception {
        bvtObj.createTable();
        String expectedTableName = bvtObj.getTableName().toUpperCase();
        List<String> afterCreateTableList = JDBCUtils.getTableList();
        System.out.println(afterCreateTableList);
        Assert.assertTrue(afterCreateTableList.contains(expectedTableName));
    }

    @Test(priority = 1, groups = {"BVT"},dependsOnMethods = {"test01TableCreate1"}, description = "验证插入数据成功")
    public void test02TableInsert() throws Exception {
        int expectedInsertCount = 1;
        int actualInsertCount = bvtObj.insertTableValues();
        Assert.assertEquals(actualInsertCount, expectedInsertCount);
    }

    @Test(priority = 2, groups = {"BVT"}, dependsOnMethods = {"test02TableInsert"},description = "验证更新字符串型数据成功")
    public void test03StringUpdate() throws Exception {
        int expectedUpdateCount = 1;
        int actualUpdateCount = bvtObj.updateStringValues();
        Assert.assertEquals(actualUpdateCount, expectedUpdateCount);
    }

    @Test(priority = 3, groups = {"BVT"}, dependsOnMethods = {"test02TableInsert"},description = "验证更新整型数据成功")
    public void test04IntUpdate() throws Exception {
        int expectedUpdateCount = 1;
        int actualUpdateCount = bvtObj.updateIntValues();
        Assert.assertEquals(actualUpdateCount, expectedUpdateCount);
    }

    @Test(priority = 4, groups = {"BVT"}, dependsOnMethods = {"test03StringUpdate"},description = "验证查询数据成功")
    public void test05TableQuery() throws Exception {
        String expectedQueryName = bvtObj.newName;
        System.out.println("Expected: " + expectedQueryName);
        String actualQueryName = bvtObj.queryTable();
        System.out.println("Actual: " + actualQueryName);
        Assert.assertEquals(actualQueryName, expectedQueryName);
    }

    @Test(priority = 5, groups = {"BVT"}, dependsOnMethods = {"test05TableQuery"}, description = "验证清空表数据成功")
    public void test06TableDelete() throws Exception {
        int expectedDeleteCount = 1;
        int actualDeleteCount = bvtObj.deleteTableValues();
        Assert.assertEquals(actualDeleteCount, expectedDeleteCount);
    }

    @Test(priority = 6, groups = {"BVT"}, dependsOnMethods = {"test06TableDelete"}, description = "验证删除表成功")
    public void test07TableDrop() throws Exception {
        bvtObj.dropTable();
        String expectedTableName = bvtObj.getTableName();
        List<String> afterDropTableList = JDBCUtils.getTableList();
        Assert.assertFalse(afterDropTableList.contains(expectedTableName));
    }

    @Test(priority = 7, enabled = true, groups = {"BVT"}, dataProvider = "yamlBVTMethod", description = "创建测试表，并插入多条数据")
    public void test08TableCreate2(Map<String, String> param) throws SQLException, InterruptedException {
        String tableMetaPath = param.get("metaPath");
        String tableValuePath = param.get("valuePath");
        initTable(param.get("tableName"), tableMetaPath);
        Thread.sleep(5000);
        insertTableValue(param.get("tableName"), param.get("insertFields"), tableValuePath);
        int tableRowCount = bvtObj.queryTableRowCount(param.get("tableName"), param.get("queryFields"), param.get("queryState"));
        System.out.println("表数据行数：" + tableRowCount);
        Assert.assertEquals(tableRowCount, Integer.parseInt(param.get("outCount")));
    }

    @Test(priority = 8, enabled = true, groups = {"BVT"}, dependsOnMethods = {"test08TableCreate2"}, dataProvider = "yamlBVTMethod", description = "truncate清空表")
    public void test09TableTruncate(Map<String, String> param) throws SQLException, InterruptedException {
        bvtObj.truncateTable(param.get("truncateSql"));
        Thread.sleep(10000);
        int tableRowCount = bvtObj.queryTableRowCount(param.get("tableName"), param.get("queryFields"), param.get("queryState"));
        System.out.println("表数据行数：" + tableRowCount);
        Assert.assertEquals(tableRowCount, 0);
    }

    @AfterClass(alwaysRun = true, description = "测试类完成后，关闭数据库连接")
    public void tearDownAll() throws SQLException, ClassNotFoundException {
        Statement tearDownStatement = null;
//        List<String> tableList = JDBCUtils.getTableList();
        List<String> tableList = Arrays.asList("bvttest2","bvttest3");
        System.out.println(tableList);
        try{
            tearDownStatement = DailyBVT.connection.createStatement();
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
            JDBCUtils.closeResource(DailyBVT.connection, tearDownStatement);
        }
    }

}
