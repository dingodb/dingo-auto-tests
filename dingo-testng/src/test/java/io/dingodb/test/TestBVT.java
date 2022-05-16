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

import io.dingodb.dailytest.DailyBVT;
import listener.EmailableReporterListener;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Listeners;
import org.testng.annotations.Test;

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

@Listeners(EmailableReporterListener.class)
public class TestBVT {
    private static Connection connection;
    public static DailyBVT bvtObj = new DailyBVT();

    @BeforeClass(alwaysRun = true, groups = {"BVT"}, description = "连接数据库")
    public static void setUpAll() throws ClassNotFoundException, SQLException {
        connection = DailyBVT.connectDingo();
    }

    public List<String> getTableList() throws SQLException, ClassNotFoundException {
        List<String> tableList = new ArrayList<String>();
        DatabaseMetaData dmd = connection.getMetaData();
        ResultSet resultSetSchema = dmd.getSchemas();
        List<String> schemaList = new ArrayList<>();
        while (resultSetSchema.next()) {
            schemaList.add(resultSetSchema.getString(1));
        }
        System.out.println(schemaList.get(0));
        ResultSet rst = dmd.getTables(null, schemaList.get(0), "%", null);
//        ResultSet rst = dmd.getTables(null, "DINGO", "%", null);
        while (rst.next()) {
            tableList.add(rst.getString("TABLE_NAME").toUpperCase());
        }
        return tableList;
    }

    @Test(priority = 0, groups = {"BVT"}, description = "验证创建表成功后，获取表名成功")
    public void test01TableCreate() throws Exception {
        bvtObj.createTable();
        String expectedTableName = bvtObj.getTableName().toUpperCase();
        List<String> afterCreateTableList = getTableList();
        Assert.assertTrue(afterCreateTableList.contains(expectedTableName));
    }

    @Test(priority = 1, groups = {"BVT"},dependsOnMethods = {"test01TableCreate"}, description = "验证插入数据成功")
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
        System.out.println("----" + expectedQueryName + "-----");
        String actualQueryName = bvtObj.queryTable();
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
        List<String> afterDropTableList = getTableList();
        Assert.assertFalse(afterDropTableList.contains(expectedTableName));
    }

    @AfterClass(description = "测试类完成后，关闭数据库连接")
    public void tearDownAll() throws SQLException, ClassNotFoundException {
        connection.close();
    }

}
