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
import listener.EmailableReporterListener;
import org.testng.annotations.Listeners;
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

@Listeners(EmailableReporterListener.class)
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

    @Test(priority = 0, description = "验证创建DateTime表成功后，获取表名成功")
    public void test01TableWithDateTimeFieldsCreate() throws Exception {
        dateTimeObj.createTable();
        String expectedTableName = dateTimeObj.getDateTimeTableName().toUpperCase();
        List<String> afterCreateTableList = getTableList();
        Assert.assertTrue(afterCreateTableList.contains(expectedTableName));
    }

    @Test(priority = 0, description = "验证创建date表成功后，获取表名成功")
    public void test01TableWithDateFieldCreate() throws Exception {
        dateTimeObj.createDateTable();
        String expectedDateTableName = dateTimeObj.getDateTableName().toUpperCase();
        List<String> afterCreateTableList = getTableList();
        Assert.assertTrue(afterCreateTableList.contains(expectedDateTableName));
    }

    @Test(priority = 0, description = "验证创建time表成功后，获取表名成功")
    public void test01TableWithTimeFieldCreate() throws Exception {
        dateTimeObj.createTimeTable();
        String expectedTimeTableName = dateTimeObj.getTimeTableName().toUpperCase();
        List<String> afterCreateTableList = getTableList();
        Assert.assertTrue(afterCreateTableList.contains(expectedTimeTableName));
    }

    @Test(priority = 0, description = "验证创建timestamp表成功后，获取表名成功")
    public void test01TableWithTimestampFieldCreate() throws Exception {
        dateTimeObj.createTimestampTable();
        String expectedTimestampTableName = dateTimeObj.getTimestampTableName().toUpperCase();
        List<String> afterCreateTableList = getTableList();
        Assert.assertTrue(afterCreateTableList.contains(expectedTimestampTableName));
    }

    @Test(priority = 1, enabled = true, dependsOnMethods = {"test01TableWithDateTimeFieldsCreate"}, description = "验证插入DateTime各字段数据成功")
    public void test02DateTimeInsert() throws Exception {
        int expectedInsertCount = 7;
        int actualInsertCount = dateTimeObj.insertDateTimeValues();
        Assert.assertEquals(actualInsertCount, expectedInsertCount);
    }

    @Test(priority = 1, enabled = true, dependsOnMethods = {"test01TableWithDateFieldCreate"}, description = "验证插入Date字段数据成功")
    public void test02DateInsert() throws Exception {
        int expectedInsertDateCount = 7;
        int actualInsertDateCount = dateTimeObj.insertDateValues();
        Assert.assertEquals(actualInsertDateCount, expectedInsertDateCount);
    }

    @Test(priority = 1, enabled = true, dependsOnMethods = {"test01TableWithTimeFieldCreate"}, description = "验证插入Time字段数据成功")
    public void test02TimeInsert() throws Exception {
        int expectedInsertTimeCount = 10;
        int actualInsertTimeCount = dateTimeObj.insertTimeValues();
        Assert.assertEquals(actualInsertTimeCount, expectedInsertTimeCount);
    }

    @Test(priority = 1, enabled = true, dependsOnMethods = {"test01TableWithTimestampFieldCreate"}, description = "验证插入Timestamp字段数据成功")
    public void test02TimestampInsert() throws Exception {
        int expectedInsertTimestampCount = 1;
        int actualInsertTimestampCount = dateTimeObj.insertTimeStampValues();
        Assert.assertEquals(actualInsertTimestampCount, expectedInsertTimestampCount);
    }

    @Test(priority = 2, enabled = true, description = "验证函数Now()返回结果正常")
    public void test03NowFunc() throws SQLException {
        String currentUTCTimestamp = UTCTimestampFormat.getUTCTimestampStr();
//        System.out.println(currentUTCTimestamp);
        String expectedUTCTimestampStr = UTCTimestampFormat.formatUTCTimestamp(currentUTCTimestamp);
        System.out.println("Expected: " + expectedUTCTimestampStr);
        String nowTimestampStr = dateTimeObj.nowFunc();
        System.out.println("Return: " + nowTimestampStr);
        String actualNowTimestampStr = nowTimestampStr.substring(0,16);
        System.out.println("Actual:" + actualNowTimestampStr);
        Assert.assertEquals(nowTimestampStr.length(), 19);
        Assert.assertEquals(actualNowTimestampStr, expectedUTCTimestampStr);
    }

    @Test(priority = 3, enabled = true, description = "验证函数CurDate()返回结果正常")
    public void test04CurDateFunc() throws SQLException {
        String currentUTCDate = UTCDateFormat.getUTCDateStr();
//        System.out.println(currentUTCDate);
        String expectedUTCDate = UTCDateFormat.formatUTCDate(currentUTCDate);
        System.out.println("Expected: " + expectedUTCDate);
        String actualCurDateStr = dateTimeObj.curDateFunc();
        System.out.println("Actual:" + actualCurDateStr);
        Assert.assertEquals(actualCurDateStr, expectedUTCDate);
    }

    @Test(priority = 4, enabled = true, description = "验证函数Current_Date返回结果正常")
    public void test05Current_DateFunc() throws SQLException {
        String currentUTCDate = UTCDateFormat.getUTCDateStr();
//        System.out.println(currentUTCDate);
        String expectedUTCDate = UTCDateFormat.formatUTCDate(currentUTCDate);
        System.out.println("Expected: " + expectedUTCDate);
        String actualCurrent_DateStr = dateTimeObj.current_DateFunc();
        System.out.println("Actual:" + actualCurrent_DateStr);
        Assert.assertEquals(actualCurrent_DateStr, expectedUTCDate);
    }

    @Test(priority = 5, enabled = true, description = "验证函数Current_Date()返回结果正常")
    public void test06Current_DateWithBracketsFunc() throws SQLException {
        String currentUTCDate = UTCDateFormat.getUTCDateStr();
//        System.out.println(currentUTCDate);
        String expectedUTCDate = UTCDateFormat.formatUTCDate(currentUTCDate);
        System.out.println("Expected: " + expectedUTCDate);
        String actualCurrent_DateWithBracketsStr = dateTimeObj.current_DateWithBracketsFunc();
        System.out.println("Actual:" + actualCurrent_DateWithBracketsStr);
        Assert.assertEquals(actualCurrent_DateWithBracketsStr, expectedUTCDate);
    }

    @Test(priority = 6, enabled = true, description = "验证函数CurTime()返回结果正常")
    public void test07CurTimeFunc() throws SQLException {
        String currentUTCTime = UTCTimeFormat.getUTCTimeStr();
//        System.out.println(currentUTCTime);
        String expectedUTCTimeStr = UTCTimeFormat.formatUTCTime(currentUTCTime);
        System.out.println("Expected: " + expectedUTCTimeStr);
        String getCurTimeStr = dateTimeObj.curTimeFunc();
        System.out.println("Actual:" + getCurTimeStr);
        String actualCurTimeStr = getCurTimeStr.substring(0, 5);
        Assert.assertEquals(getCurTimeStr.length(), 8);
        Assert.assertEquals(actualCurTimeStr, expectedUTCTimeStr);
    }

    @Test(priority = 7, enabled = true, description = "验证函数Current_Time返回结果正常")
    public void test08Current_TimeFunc() throws SQLException {
        String currentUTCTime = UTCTimeFormat.getUTCTimeStr();
//        System.out.println(currentUTCTime);
        String expectedUTCTimeStr = UTCTimeFormat.formatUTCTime(currentUTCTime);
        System.out.println("Expected: " + expectedUTCTimeStr);
        String getCurrent_TimeStr = dateTimeObj.current_TimeFunc();
        System.out.println("Return: " + getCurrent_TimeStr);
        String actualCurrent_TimeStr = getCurrent_TimeStr.substring(0, 5);
        System.out.println("Actual:" + actualCurrent_TimeStr);
        Assert.assertEquals(getCurrent_TimeStr.length(),8);
        Assert.assertEquals(actualCurrent_TimeStr, expectedUTCTimeStr);
    }

    @Test(priority = 8, enabled = true, description = "验证函数Current_Time()返回结果正常")
    public void test09Current_TimeWithBracketsFunc() throws SQLException {
        String currentUTCTime = UTCTimeFormat.getUTCTimeStr();
//        System.out.println(currentUTCTime);
        String expectedUTCTimeStr = UTCTimeFormat.formatUTCTime(currentUTCTime);
        System.out.println("Expected: " + expectedUTCTimeStr);
        String getCurrent_TimeWithBracketsStr = dateTimeObj.current_TimeWithBracketsFunc();
        System.out.println("Return: " + getCurrent_TimeWithBracketsStr);
        String actualCurrent_TimeWithBracketsStr = getCurrent_TimeWithBracketsStr.substring(0, 5);
        System.out.println("Actual:" + actualCurrent_TimeWithBracketsStr);
        Assert.assertEquals(getCurrent_TimeWithBracketsStr.length(),8);
        Assert.assertEquals(actualCurrent_TimeWithBracketsStr, expectedUTCTimeStr);
    }

    @Test(priority = 9, enabled = true, description = "验证函数Current_TimeStamp返回结果正常")
    public void test10Current_TimeStampFunc() throws SQLException {
        String currentUTCTimeStamp = UTCTimestampFormat.getUTCTimestampStr();
//        System.out.println(currentUTCTimeStamp);
        String expectedUTCTimeStampStr = UTCTimestampFormat.formatUTCTimestamp(currentUTCTimeStamp);
        System.out.println("Expected: " + expectedUTCTimeStampStr);
        String getCurrent_TimeStampStr = dateTimeObj.current_TimeStampFunc();
        System.out.println("Return: " + getCurrent_TimeStampStr);
        String actualCurrent_TimeStampStr = getCurrent_TimeStampStr.substring(0, 16);
        System.out.println("Actual:" + actualCurrent_TimeStampStr);
        Assert.assertEquals(getCurrent_TimeStampStr.length(),19);
        Assert.assertEquals(actualCurrent_TimeStampStr, expectedUTCTimeStampStr);
    }

    @Test(priority = 10, enabled = true, description = "验证函数Current_TimeStamp()返回结果正常")
    public void test11Current_TimeStampWithBracketsFunc() throws SQLException {
        String currentUTCTimeStamp = UTCTimestampFormat.getUTCTimestampStr();
//        System.out.println(currentUTCTimeStamp);
        String expectedUTCTimeStampStr = UTCTimestampFormat.formatUTCTimestamp(currentUTCTimeStamp);
        System.out.println("Expected: " + expectedUTCTimeStampStr);
        String getCurrent_TimeStampWithBracketsStr = dateTimeObj.current_TimeStampWithBracketsFunc();
        System.out.println("Return: " + getCurrent_TimeStampWithBracketsStr);
        String actualCurrent_TimeStampWithBracketsStr = getCurrent_TimeStampWithBracketsStr.substring(0, 16);
        System.out.println("Actual:" + actualCurrent_TimeStampWithBracketsStr);
        Assert.assertEquals(getCurrent_TimeStampWithBracketsStr.length(),19);
        Assert.assertEquals(actualCurrent_TimeStampWithBracketsStr, expectedUTCTimeStampStr);
    }

    @Test(priority = 11, enabled = true, dataProvider = "yamlDataMethod", description = "验证函数from_UnixTime的返回结果正常")
    public void test12From_UnixTimeFunc_TimestampInput(Map<String, String> param) throws SQLException {
        String expectedFrom_UnitTimeStr = param.get("outputDate");
        System.out.println("Expected: " + expectedFrom_UnitTimeStr);
        String actualFrom_UnixTimeReturn = dateTimeObj.from_UnixTimeWithTimestampFunc(param.get("inputTimestamp"));
        System.out.println("Actual: " + actualFrom_UnixTimeReturn);
        Assert.assertEquals(actualFrom_UnixTimeReturn, expectedFrom_UnitTimeStr);
    }

    @Test(priority = 11, enabled = true, description = "验证函数from_UnixTime传入字符串数值的返回结果正常")
    public void test12From_UnixTimeFunc_StringInput() throws SQLException {
        String expectedFrom_UnitTimeWithStringParamStr = "2022-04-12 21:28:30";
        System.out.println("Expected: " + expectedFrom_UnitTimeWithStringParamStr);
        String actualFrom_UnixTimeWithStringParamReturn = dateTimeObj.from_UnixTimeWithStringFunc();
        System.out.println("Actual: " + actualFrom_UnixTimeWithStringParamReturn);
        Assert.assertEquals(actualFrom_UnixTimeWithStringParamReturn, expectedFrom_UnitTimeWithStringParamStr);
    }

    @Test(priority = 12, enabled = true, dataProvider = "yamlDataMethod", description = "验证函数unix_TimeStamp有参时的返回结果正常")
    public void test13Unix_TimeStampFunc(Map<String, String> param) throws SQLException {
//        System.out.println(param.get("inputStr"));
//        System.out.println(param.get("outputStr"));
        String expectedUnix_TimeStamp = param.get("outputTimestamp");
        System.out.println("Expected: " + expectedUnix_TimeStamp);
        String actualUnix_TimeStamp = dateTimeObj.unix_TimeStampFunc(param.get("inputDate"));
        System.out.println("Actual: " + actualUnix_TimeStamp);
        Assert.assertEquals(actualUnix_TimeStamp, expectedUnix_TimeStamp);
    }

    @Test(priority = 12, enabled = true, description = "验证函数unix_TimeStamp空参时的返回结果正常")
    public void test13Unix_TimeStampNoArgFunc() throws SQLException {
        String currentTimeStamp = String.valueOf(System.currentTimeMillis()/1000);
        System.out.println("Current Timestamp: " + currentTimeStamp);
        String returnUnix_TimeStampWithoutArg = dateTimeObj.unix_TimeStampNoArgFunc();
        System.out.println("Return Timestamp: " + returnUnix_TimeStampWithoutArg);
        Assert.assertEquals(returnUnix_TimeStampWithoutArg.length(), 10);

        int timeStampDiff = Integer.parseInt(returnUnix_TimeStampWithoutArg) - Integer.parseInt(currentTimeStamp);
        Assert.assertTrue(timeStampDiff < 60);
    }

    @Test(priority = 13, enabled = true, dataProvider = "yamlDataMethod", description = "验证函数date_format字符串参数的返回结果正常")
    public void test14Date_FormatStrArgFunc(Map<String, String> param) throws SQLException {
        String expectedDate_FormatSargStr = param.get("outputDate");
        System.out.println("Expected: " + expectedDate_FormatSargStr);
        String actualDate_FormatSargStr = dateTimeObj.date_FormatStrArgFunc(param.get("inputDate"), param.get("inputFormat"));
        System.out.println("Actual: " + actualDate_FormatSargStr);

        Assert.assertEquals(actualDate_FormatSargStr, expectedDate_FormatSargStr);
    }

    @Test(priority = 13, enabled = true, dataProvider = "yamlDataMethod", description = "验证函数date_format数字参数的返回结果正常")
    public void test14Date_FormatNumArgFunc(Map<String, String> param) throws SQLException {
        String expectedDate_FormatNargStr = param.get("outputDate");
        System.out.println("Expected: " + expectedDate_FormatNargStr);
        String actualDate_FormatNargStr = dateTimeObj.date_FormatNumArgFunc(param.get("inputDate"), param.get("inputFormat"));
        System.out.println("Actual: " + actualDate_FormatNargStr);
        Assert.assertEquals(actualDate_FormatNargStr, expectedDate_FormatNargStr);
    }

    @Test(priority = 14, enabled = true, dataProvider = "yamlDataMethod", description = "验证函数datediff字符串参数的返回结果正常")
    public void test15DateDiffStrArgFunc(Map<String, String> param) throws SQLException {
        String expectedDateStrArgDiff = param.get("outputDiff");
        System.out.println("Expected: " + expectedDateStrArgDiff);
        String actualDateStrArgDiff = dateTimeObj.dateDiffStrArgFunc(param.get("inputDate1"), param.get("inputDate2"));
        System.out.println("Actual: " + actualDateStrArgDiff);
        Assert.assertEquals(actualDateStrArgDiff, expectedDateStrArgDiff);
    }

    @Test(priority = 14, enabled = true, dataProvider = "yamlDataMethod", description = "验证函数datediff数字参数的返回结果正常")
    public void test15DateDiffNumArgFunc(Map<String, String> param) throws SQLException {
        String expectedDateNumArgDiff = param.get("outputDiff");
        System.out.println("Expected: " + expectedDateNumArgDiff);
        String actualDateNumArgDiff = dateTimeObj.dateDiffNumArgFunc(param.get("inputDate1"), param.get("inputDate2"));
        System.out.println("Actual: " + actualDateNumArgDiff);
        Assert.assertEquals(actualDateNumArgDiff, expectedDateNumArgDiff);
    }

    @Test(priority = 15, enabled = true, dataProvider = "yamlDataMethod", description = "验证插入不同格式的日期成功")
    public void test16VariousFormatDateInsert(Map<String, String> param) throws SQLException, ClassNotFoundException {
        String expectedDateQuery = param.get("expectedDate");
        System.out.println("Expected: " + expectedDateQuery);
        String actualDateQuery = dateTimeObj.insertVariousFormatDateValues(param.get("insertID"), param.get("insertDate"));
        System.out.println("Actual: " + actualDateQuery);
        Assert.assertEquals(actualDateQuery, expectedDateQuery);
    }

    @Test(priority = 16, enabled = true, dataProvider = "yamlDataMethod", description = "验证函数和字符串上下文返回")
    public void test17FuncConcatStr(Map<String, String> param) throws SQLException {
        String actualFuncConcatStr = dateTimeObj.funcConcatStr(param.get("funcName"));
        System.out.println("Actual: " + actualFuncConcatStr);
        boolean matchResult = actualFuncConcatStr.matches(param.get("matchReg"));
        Assert.assertTrue(matchResult);
    }


    @AfterClass(alwaysRun = true, description = "测试完成后删除数据和表格并关闭连接")
    public void tearDownAll() throws SQLException {
        String dateTimeTableName = DateTimeFuncs.getDateTimeTableName();
        String dateTableName = DateTimeFuncs.getDateTableName();
        String timeTableName = DateTimeFuncs.getTimeTableName();
        String timestampTableName = DateTimeFuncs.getTimestampTableName();
        Statement tearDownStatement = DateTimeFuncs.connection.createStatement();
        tearDownStatement.execute("delete from " + dateTimeTableName);
        tearDownStatement.execute("delete from " + dateTableName);
        tearDownStatement.execute("delete from " + timeTableName);
        tearDownStatement.execute("delete from " + timestampTableName);
        tearDownStatement.execute("drop table " + dateTimeTableName);
        tearDownStatement.execute("drop table " + dateTableName);
        tearDownStatement.execute("drop table " + timeTableName);
        tearDownStatement.execute("drop table " + timestampTableName);
        tearDownStatement.close();
        DateTimeFuncs.connection.close();
    }

}
