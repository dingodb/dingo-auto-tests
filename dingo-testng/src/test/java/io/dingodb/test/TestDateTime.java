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
import utils.UTCTimeFormat;
import utils.UTCDateFormat;

import io.dingodb.dailytest.DateTimeFuncs;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import java.sql.DatabaseMetaData;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;

public class TestDateTime extends DateTimeFuncs {
    public static DateTimeFuncs dateTimeObj = new DateTimeFuncs();

    public static List<String> getTableList() throws SQLException, ClassNotFoundException {
        List<String> tableList = new ArrayList<String>();
        DatabaseMetaData dmd = connection.getMetaData();
        ResultSet resultSetSchema = dmd.getSchemas();
        List<String> schemaList = new ArrayList<>();
        while (resultSetSchema.next()) {
            schemaList.add(resultSetSchema.getString(1));
        }
        System.out.println(schemaList.get(0));
        ResultSet rst = dmd.getTables(null, schemaList.get(0), "%", null);
        while (rst.next()) {
            tableList.add(rst.getString("TABLE_NAME").toUpperCase());
        }
        return tableList;
    }

    @BeforeClass(alwaysRun = true, description = "测试数据库连接")
    public static void setUpAll() throws ClassNotFoundException, SQLException {
        Assert.assertNotNull(connection);
    }

    @Test(priority = 0, description = "验证创建表成功后，获取表名成功")
    public void test01TableWithDateTimeFieldsCreate() throws Exception {
        dateTimeObj.createTable();
        String expectedTableName = dateTimeObj.getDateTimeTableName().toUpperCase();
        List<String> afterCreateTableList = getTableList();
        Assert.assertTrue(afterCreateTableList.contains(expectedTableName));
    }

    @Test(priority = 1, dependsOnMethods = {"test01TableWithDateTimeFieldsCreate"}, description = "验证插入数据成功")
    public void test02DateTimeInsert() throws Exception {
        int expectedInsertCount = 7;
        int actualInsertCount = dateTimeObj.insertValues();
        Assert.assertEquals(actualInsertCount, expectedInsertCount);
    }

    @Test(priority = 2, dependsOnMethods = {"test02DateTimeInsert"}, description = "验证函数Now()返回结果正常")
    public void test03NowFunc() throws SQLException {
        String currentUTCTime = UTCTimeFormat.getUTCTimeStr();
        System.out.println(currentUTCTime);
        String expectedUTCTimeStr = UTCTimeFormat.formatUTCTime(currentUTCTime);
        System.out.println(expectedUTCTimeStr);
        String nowTime = dateTimeObj.nowFunc();
        System.out.println(nowTime);
        String actualNowTimeStr = UTCTimeFormat.formatUTCTime(nowTime);
        System.out.println(actualNowTimeStr);
        Assert.assertEquals(actualNowTimeStr, expectedUTCTimeStr);
    }

    @Test(priority = 3, dependsOnMethods = {"test02DateTimeInsert"}, description = "验证函数CurDate()返回结果正常")
    public void test04CurDateFunc() throws SQLException {
        String currentUTCDate = UTCDateFormat.getUTCDateStr();
        System.out.println(currentUTCDate);
        String expectedUTCDate = UTCDateFormat.formatUTCDate(currentUTCDate);
        System.out.println(expectedUTCDate);
        String actualCurDateStr = dateTimeObj.curDateFunc();
        System.out.println(actualCurDateStr);
        Assert.assertEquals(actualCurDateStr, expectedUTCDate);
    }

    @AfterClass(alwaysRun = true, description = "测试完成后删除数据和表格并关闭连接")
    public void tearDownAll() throws SQLException {
        String tableName = DateTimeFuncs.getDateTimeTableName();
        Statement tearDownStatement = connection.createStatement();
        tearDownStatement.execute("delete from " + tableName);
        tearDownStatement.execute("drop table " + tableName);
        tearDownStatement.close();
        connection.close();
    }

}
