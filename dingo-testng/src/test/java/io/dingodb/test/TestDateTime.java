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
import utils.UTCTimestampFormat;
import utils.UTCDateFormat;
import utils.YamlDataHelper;

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
import java.util.Map;


public class TestDateTime extends YamlDataHelper{
    public static DateTimeFuncs dateTimeObj = new DateTimeFuncs();

    public static List<String> getTableList() throws SQLException, ClassNotFoundException {
        List<String> tableList = new ArrayList<String>();
        DatabaseMetaData dmd = DateTimeFuncs.connection.getMetaData();
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
        Assert.assertNotNull(DateTimeFuncs.connection);
    }

    @Test(priority = 0, description = "验证创建表成功后，获取表名成功")
    public void test01TableWithDateTimeFieldsCreate() throws Exception {
        dateTimeObj.createTable();
        String expectedTableName = dateTimeObj.getDateTimeTableName().toUpperCase();
        List<String> afterCreateTableList = getTableList();
        Assert.assertTrue(afterCreateTableList.contains(expectedTableName));
    }

    @Test(priority = 1, enabled = true, dependsOnMethods = {"test01TableWithDateTimeFieldsCreate"}, description = "验证插入数据成功")
    public void test02DateTimeInsert() throws Exception {
        int expectedInsertCount = 7;
        int actualInsertCount = dateTimeObj.insertValues();
        Assert.assertEquals(actualInsertCount, expectedInsertCount);
    }

    @Test(priority = 2, enabled = true, description = "验证函数Now()返回结果正常")
    public void test03NowFunc() throws SQLException {
        String currentUTCTimestamp = UTCTimestampFormat.getUTCTimestampStr();
        System.out.println(currentUTCTimestamp);
        String expectedUTCTimestampStr = UTCTimestampFormat.formatUTCTimestamp(currentUTCTimestamp);
        System.out.println(expectedUTCTimestampStr);
        String nowTimestampStr = dateTimeObj.nowFunc();
        System.out.println(nowTimestampStr);
        String actualNowTimestampStr = nowTimestampStr.substring(0,16);
        System.out.println(actualNowTimestampStr);
        Assert.assertEquals(nowTimestampStr.length(), 19);
        Assert.assertEquals(actualNowTimestampStr, expectedUTCTimestampStr);
    }

    @Test(priority = 3, enabled = true, description = "验证函数CurDate()返回结果正常")
    public void test04CurDateFunc() throws SQLException {
        String currentUTCDate = UTCDateFormat.getUTCDateStr();
        System.out.println(currentUTCDate);
        String expectedUTCDate = UTCDateFormat.formatUTCDate(currentUTCDate);
        System.out.println(expectedUTCDate);
        String actualCurDateStr = dateTimeObj.curDateFunc();
        System.out.println(actualCurDateStr);
        Assert.assertEquals(actualCurDateStr, expectedUTCDate);
    }

    @Test(priority = 4, enabled = true, description = "验证函数Current_Date返回结果正常")
    public void test05Current_DateFunc() throws SQLException {
        String currentUTCDate = UTCDateFormat.getUTCDateStr();
        System.out.println(currentUTCDate);
        String expectedUTCDate = UTCDateFormat.formatUTCDate(currentUTCDate);
        System.out.println(expectedUTCDate);
        String actualCurrent_DateStr = dateTimeObj.current_DateFunc();
        System.out.println(actualCurrent_DateStr);
        Assert.assertEquals(actualCurrent_DateStr, expectedUTCDate);
    }

    @Test(priority = 5, enabled = false, description = "验证函数CurTime()返回结果正常")
    public void test06CurTimeFunc() throws SQLException {
        String currentUTCTime = UTCTimeFormat.getUTCTimeStr();
        System.out.println(currentUTCTime);
        String expectedUTCTimeStr = UTCTimeFormat.formatUTCTime(currentUTCTime);
        System.out.println(expectedUTCTimeStr);
        String getCurTimeStr = dateTimeObj.curTimeFunc();
        System.out.println(getCurTimeStr);
        String actualCurTimeStr = getCurTimeStr.substring(0, 5);
        Assert.assertEquals(getCurTimeStr.length(), 8);
        Assert.assertEquals(actualCurTimeStr, expectedUTCTimeStr);
    }

    @Test(priority = 6, enabled = false, description = "验证函数Current_Time返回结果正常")
    public void test07Current_TimeFunc() throws SQLException {
        String currentUTCTime = UTCTimeFormat.getUTCTimeStr();
        System.out.println(currentUTCTime);
        String expectedUTCTimeStr = UTCTimeFormat.formatUTCTime(currentUTCTime);
        System.out.println(expectedUTCTimeStr);
        String getCurrent_TimeStr = dateTimeObj.current_TimeFunc();
        System.out.println(getCurrent_TimeStr);
        String actualCurrent_TimeStr = getCurrent_TimeStr.substring(0, 5);
        System.out.println(actualCurrent_TimeStr);
        Assert.assertEquals(getCurrent_TimeStr.length(),8);
        Assert.assertEquals(actualCurrent_TimeStr, expectedUTCTimeStr);
    }

    @Test(priority = 7, enabled = false, description = "验证函数Current_TimeStamp返回结果正常")
    public void test08Current_TimeStampFunc() throws SQLException {
        String currentUTCTimeStamp = UTCTimestampFormat.getUTCTimestampStr();
        System.out.println(currentUTCTimeStamp);
        String expectedUTCTimeStampStr = UTCTimestampFormat.formatUTCTimestamp(currentUTCTimeStamp);
        System.out.println(expectedUTCTimeStampStr);
        String getCurrent_TimeStampStr = dateTimeObj.current_TimeStampFunc();
        System.out.println(getCurrent_TimeStampStr);
        String actualCurrent_TimeStampStr = getCurrent_TimeStampStr.substring(0, 16);
        System.out.println(actualCurrent_TimeStampStr);
        Assert.assertEquals(getCurrent_TimeStampStr.length(),19);
        Assert.assertEquals(actualCurrent_TimeStampStr, expectedUTCTimeStampStr);
    }

    @Test(priority = 8, enabled = true, description = "验证函数from_UnixTime的返回结果正常")
    public void test09From_UnixTimeFunc() throws SQLException {
        String expectedFrom_UnitTimeStr = "2022-04-20 14:24:26";
        System.out.println(expectedFrom_UnitTimeStr);
        String actualFrom_UnixTimeReturn = dateTimeObj.from_UnixTimeFunc();
        System.out.println(actualFrom_UnixTimeReturn);
        Assert.assertEquals(actualFrom_UnixTimeReturn, expectedFrom_UnitTimeStr);
    }

    @Test(priority = 9, enabled = true, dataProvider = "yamlDataMethod", description = "验证函数unix_TimeStamp的返回结果正常")
    public void test10Unix_TimeStampFunc(Map<String, String> param) throws SQLException {
//        System.out.println(param.get("inputStr"));
//        System.out.println(param.get("outputStr"));
        String actualUnix_TimeStamp = dateTimeObj.unix_TimeStampFunc(param.get("inputStr"));
        System.out.println(actualUnix_TimeStamp);
        String expectedUnix_TimeStamp = param.get("outputStr");
        System.out.println(expectedUnix_TimeStamp);
        Assert.assertEquals(actualUnix_TimeStamp, expectedUnix_TimeStamp);
    }

    @Test(priority = 10, enabled = true, dataProvider = "yamlDataMethod", description = "验证函数date_format的返回结果正常")
    public void test11Date_FormatFunc(Map<String, String> param) throws SQLException {
//        System.out.println(param.get("inputDate"));
//        System.out.println(param.get("outputDate"));
        String actualDate_FormatStr = dateTimeObj.date_FormatFunc(param.get("inputDate"), param.get("inputFormat"));
        System.out.println(actualDate_FormatStr);
        String expectedDate_FormatStr = param.get("outputDate");
        System.out.println(expectedDate_FormatStr);
        Assert.assertEquals(actualDate_FormatStr, expectedDate_FormatStr);
    }

    @Test(priority = 11, enabled = true, dataProvider = "yamlDataMethod", description = "验证函数datediff的返回结果正常")
    public void test12DateDiffFunc(Map<String, String> param) throws SQLException {
        String actualDateDiff = dateTimeObj.dateDiffFunc(param.get("inputDate1"), param.get("inputDate2"));
        System.out.println(actualDateDiff);
        String expectedDateDiff = param.get("outputDiff");
        System.out.println(expectedDateDiff);
        Assert.assertEquals(actualDateDiff, expectedDateDiff);
    }


    @AfterClass(alwaysRun = true, description = "测试完成后删除数据和表格并关闭连接")
    public void tearDownAll() throws SQLException {
        String tableName = DateTimeFuncs.getDateTimeTableName();
        Statement tearDownStatement = DateTimeFuncs.connection.createStatement();
        tearDownStatement.execute("delete from " + tableName);
        tearDownStatement.execute("drop table " + tableName);
        tearDownStatement.close();
        DateTimeFuncs.connection.close();
    }

}
