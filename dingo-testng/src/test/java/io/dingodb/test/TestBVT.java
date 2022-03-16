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

import io.dingodb.driver.client.DingoDriverClient;
import io.dingodb.dailytest.DailyBVT;
import listener.EmailableReporterListener;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Listeners;
import org.testng.annotations.Test;

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

@Listeners(EmailableReporterListener.class)
public class TestBVT {
    private static final String defaultConnectIP = "172.20.3.26";
    private static final String JDBC_DRIVER = "io.dingodb.driver.client.DingoDriverClient";
    private static final String connectUrl = "jdbc:dingo:thin:url=http://" + defaultConnectIP + ":8765";
    private static Connection connection;

    @BeforeClass(alwaysRun = true, groups = {"BVT"}, description = "连接数据库")
    public void setUpAll() throws ClassNotFoundException, SQLException {
        Class.forName(JDBC_DRIVER);
        connection = DriverManager.getConnection(connectUrl);
    }

    public List<String> getTableList() throws SQLException {
        List<String> tableList = new ArrayList<String>();
        DatabaseMetaData dmd = connection.getMetaData();
        ResultSet rst = dmd.getTables(null, null, "%", null);
        while (rst.next()) {
            tableList.add(rst.getString("TABLE_NAME").toLowerCase());
        }
        return tableList;
    }

    @Test(priority = 0, groups = {"BVT"}, description = "验证创建表成功后，获取表名成功")
    public void test01TableCreate() throws Exception {
        DailyBVT testCreate = new DailyBVT();
        testCreate.createTable();
        String expectedTableName = testCreate.getTableName().toLowerCase();
        List<String> afterCreateTableList = getTableList();
        Assert.assertTrue(afterCreateTableList.contains(expectedTableName));
    }

    @Test(priority = 1, groups = {"BVT"}, dependsOnMethods = {"test01TableCreate"},description = "验证插入数据成功")
    public void test02TableInsert() throws Exception {
        int expectedInsertCount = 1;
        DailyBVT testInsert = new DailyBVT();
        int actualInsertCount = testInsert.insertTableValues();
        Assert.assertEquals(actualInsertCount, expectedInsertCount);
    }

    @Test(priority = 2, groups = {"BVT"}, dependsOnMethods = {"test02TableInsert"},description = "验证更新数据成功")
    public void test03TableUpdate() throws Exception {
        int expectedUpdateCount = 1;
        DailyBVT testUpdate = new DailyBVT();
        int actualUpdateCount = testUpdate.updateTableValues();
        Assert.assertEquals(actualUpdateCount, expectedUpdateCount);
    }

    @Test(priority = 3, groups = {"BVT"}, dependsOnMethods = {"test03TableUpdate"},description = "验证查询数据成功")
    public void test04TableQuery() throws Exception {
        DailyBVT testQuery = new DailyBVT();
        String expectedQueryName = testQuery.newName;
        System.out.println("----" + expectedQueryName + "-----");
        String actualQueryName = testQuery.queryTable();
        Assert.assertEquals(actualQueryName, expectedQueryName);
    }

    @Test(priority = 4, groups = {"BVT"}, dependsOnMethods = {"test04TableQuery"}, description = "验证清空表数据成功")
    public void test05TableDelete() throws Exception {
        int expectedDeleteCount = 1;
        DailyBVT testDelete = new DailyBVT();
        int actualDeleteCount = testDelete.deleteTableValues();
        Assert.assertEquals(actualDeleteCount, expectedDeleteCount);
    }

    @Test(priority = 5, groups = {"BVT"}, dependsOnMethods = {"test05TableDelete"}, description = "验证删除表成功")
    public void test06TableDrop() throws Exception {
        DailyBVT testDrop = new DailyBVT();
        testDrop.dropTable();
        String expectedTableName = testDrop.getTableName();
        List<String> afterDropTableList = getTableList();
        Assert.assertFalse(afterDropTableList.contains(expectedTableName));
    }

    @AfterClass(description = "测试类完成后，关闭数据库连接")
    public void tearDownAll() throws SQLException {
        connection.close();
    }

}
