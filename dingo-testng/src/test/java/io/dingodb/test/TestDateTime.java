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

import io.dingodb.dailytest.DateTimeFuncs;
import listener.EmailableReporterListener;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Listeners;
import org.testng.annotations.Test;
import utils.GetDateDiff;
import utils.JDK8DateTime;
import utils.YamlDataHelper;

import java.sql.DatabaseMetaData;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.text.ParseException;
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

    @Test(priority = 0, enabled = true, description = "验证创建DateTime表成功后，获取表名成功")
    public void test01TableWithDateTimeFieldsCreate() throws Exception {
        dateTimeObj.createTable();
        String expectedTableName = dateTimeObj.getDateTimeTableName().toUpperCase();
        List<String> afterCreateTableList = getTableList();
        Assert.assertTrue(afterCreateTableList.contains(expectedTableName));
    }

    @Test(priority = 0, enabled = true, description = "验证创建date表成功后，获取表名成功")
    public void test01TableWithDateFieldCreate() throws Exception {
        dateTimeObj.createDateTable();
        String expectedDateTableName = dateTimeObj.getDateTableName().toUpperCase();
        List<String> afterCreateTableList = getTableList();
        Assert.assertTrue(afterCreateTableList.contains(expectedDateTableName));
    }

    @Test(priority = 0, enabled = true, description = "验证创建time表成功后，获取表名成功")
    public void test01TableWithTimeFieldCreate() throws Exception {
        dateTimeObj.createTimeTable();
        String expectedTimeTableName = dateTimeObj.getTimeTableName().toUpperCase();
        List<String> afterCreateTableList = getTableList();
        Assert.assertTrue(afterCreateTableList.contains(expectedTimeTableName));
    }

    @Test(priority = 0, enabled = true, description = "验证创建timestamp表成功后，获取表名成功")
    public void test01TableWithTimestampFieldCreate() throws Exception {
        dateTimeObj.createTimestampTable();
        String expectedTimestampTableName = dateTimeObj.getTimestampTableName().toUpperCase();
        List<String> afterCreateTableList = getTableList();
        Assert.assertTrue(afterCreateTableList.contains(expectedTimestampTableName));
    }

    @Test(priority = 1, enabled = true, dependsOnMethods = {"test01TableWithDateTimeFieldsCreate"},
            description = "验证插入DateTime各字段数据成功")
    public void test02DateTimeInsert() throws Exception {
        int expectedInsertCount = 7;
        int actualInsertCount = dateTimeObj.insertDateTimeValues();
        Assert.assertEquals(actualInsertCount, expectedInsertCount);
    }

    @Test(priority = 1, enabled = true, dependsOnMethods = {"test01TableWithDateFieldCreate"},
            description = "验证插入Date字段数据成功")
    public void test02DateInsert() throws Exception {
        int expectedInsertDateCount = 7;
        int actualInsertDateCount = dateTimeObj.insertDateValues();
        Assert.assertEquals(actualInsertDateCount, expectedInsertDateCount);
    }

    @Test(priority = 1, enabled = true, dependsOnMethods = {"test01TableWithTimeFieldCreate"},
            description = "验证插入Time字段数据成功")
    public void test02TimeInsert() throws Exception {
        int expectedInsertTimeCount = 10;
        int actualInsertTimeCount = dateTimeObj.insertTimeValues();
        Assert.assertEquals(actualInsertTimeCount, expectedInsertTimeCount);
    }

    @Test(priority = 1, enabled = true, dependsOnMethods = {"test01TableWithTimestampFieldCreate"},
            description = "验证插入Timestamp字段数据成功")
    public void test02TimestampInsert() throws Exception {
        int expectedInsertTimestampCount = 1;
        int actualInsertTimestampCount = dateTimeObj.insertTimeStampValues();
        Assert.assertEquals(actualInsertTimestampCount, expectedInsertTimestampCount);
    }

    @Test(priority = 2, enabled = true, description = "验证函数Now()返回结果正常")
    public void test03NowFunc() throws SQLException {
//        UTCTimestampFormat utcNow = new UTCTimestampFormat();
//        String currentUTCTS = utcNow.getUTCTimestampStr();
//        String expectedNowStr = utcNow.formatUTCTimestamp(currentUTCTS);
        String expectedNowStr = JDK8DateTime.formatNow();
        System.out.println("Expected: " + expectedNowStr);
        String nowTimestampStr = dateTimeObj.nowFunc();
        System.out.println("Return: " + nowTimestampStr);
        String actualNowTimestampStr = nowTimestampStr.substring(0,16);
        System.out.println("Actual:" + actualNowTimestampStr);
        Assert.assertEquals(nowTimestampStr.length(), 19);
        Assert.assertEquals(actualNowTimestampStr, expectedNowStr);
    }

    @Test(priority = 3, enabled = true, description = "验证函数CurDate()返回结果正常")
    public void test04CurDateFunc() throws SQLException {
//        UTCDateFormat utcCurDate = new UTCDateFormat();
//        String curDateStr = utcCurDate.getUTCDateStr();
//        String expectedDateStr = utcCurDate.formatUTCDate(curDateStr);
        String expectedDateStr = JDK8DateTime.formatCurDate();
        System.out.println("Expected: " + expectedDateStr);
        String actualCurDateStr = dateTimeObj.curDateFunc();
        System.out.println("Actual:" + actualCurDateStr);
        Assert.assertEquals(actualCurDateStr, expectedDateStr);
    }

    @Test(priority = 4, enabled = true, description = "验证函数Current_Date返回结果正常")
    public void test05Current_DateFunc() throws SQLException {
//        UTCDateFormat utcCurrentDate = new UTCDateFormat();
//        String currentDateStr = utcCurrentDate.getUTCDateStr();
//        String expectedCurrentDateStr = utcCurrentDate.formatUTCDate(currentDateStr);
        String expectedCurrentDateStr = JDK8DateTime.formatCurDate();
        System.out.println("Expected: " + expectedCurrentDateStr);
        String actualCurrent_DateStr = dateTimeObj.current_DateFunc();
        System.out.println("Actual:" + actualCurrent_DateStr);
        Assert.assertEquals(actualCurrent_DateStr, expectedCurrentDateStr);
    }

    @Test(priority = 5, enabled = true, description = "验证函数Current_Date()返回结果正常")
    public void test06Current_DateWithBracketsFunc() throws SQLException {
//        UTCDateFormat utcCurrentDateWB = new UTCDateFormat();
//        String currentDateWBStr = utcCurrentDateWB.getUTCDateStr();
//        String expectedCurrentDateWBStr = utcCurrentDateWB.formatUTCDate(currentDateWBStr);
        String expectedCurrentDateWBStr = JDK8DateTime.formatCurDate();
        System.out.println("Expected: " + expectedCurrentDateWBStr);
        String actualCurrent_DateWithBracketsStr = dateTimeObj.current_DateWithBracketsFunc();
        System.out.println("Actual:" + actualCurrent_DateWithBracketsStr);
        Assert.assertEquals(actualCurrent_DateWithBracketsStr, expectedCurrentDateWBStr);
    }

    @Test(priority = 6, enabled = true, description = "验证函数CurTime()返回结果正常")
    public void test07CurTimeFunc() throws SQLException {
//        UTCTimeFormat utcCurTime = new UTCTimeFormat();
//        String curTimeStr = utcCurTime.getUTCTimeStr();
//        String expectedCurTimeStr = utcCurTime.formatUTCTime(curTimeStr);
        String expectedCurTimeStr = JDK8DateTime.formatCurTime();
        System.out.println("Expected: " + expectedCurTimeStr);
        String getCurTimeStr = dateTimeObj.curTimeFunc();
        System.out.println("Return:" + getCurTimeStr);
        String actualCurTimeStr = getCurTimeStr.substring(0, 5);
        System.out.println("Actual: " + actualCurTimeStr);
        Assert.assertEquals(getCurTimeStr.length(), 8);
        Assert.assertEquals(actualCurTimeStr, expectedCurTimeStr);
    }

    @Test(priority = 7, enabled = true, description = "验证函数Current_Time返回结果正常")
    public void test08Current_TimeFunc() throws SQLException {
//        UTCTimeFormat utcCurrentTime = new UTCTimeFormat();
//        String currentTimeStr = utcCurrentTime.getUTCTimeStr();
//        String expectedCurrentTimeStr = utcCurrentTime.formatUTCTime(currentTimeStr);
        String expectedCurrentTimeStr = JDK8DateTime.formatCurTime();
        System.out.println("Expected: " + expectedCurrentTimeStr);
        String getCurrent_TimeStr = dateTimeObj.current_TimeFunc();
        System.out.println("Return: " + getCurrent_TimeStr);
        String actualCurrent_TimeStr = getCurrent_TimeStr.substring(0, 5);
        System.out.println("Actual:" + actualCurrent_TimeStr);
        Assert.assertEquals(getCurrent_TimeStr.length(),8);
        Assert.assertEquals(actualCurrent_TimeStr, expectedCurrentTimeStr);
    }

    @Test(priority = 8, enabled = true, description = "验证函数Current_Time()返回结果正常")
    public void test09Current_TimeWithBracketsFunc() throws SQLException {
//        UTCTimeFormat utcCurrentTimeWB =new UTCTimeFormat();
//        String currentTimeWB = utcCurrentTimeWB.getUTCTimeStr();
//        String expectedCurrentTimeWBStr = utcCurrentTimeWB.formatUTCTime(currentTimeWB);
        String expectedCurrentTimeWBStr = JDK8DateTime.formatCurTime();
        System.out.println("Expected: " + expectedCurrentTimeWBStr);
        String getCurrent_TimeWithBracketsStr = dateTimeObj.current_TimeWithBracketsFunc();
        System.out.println("Return: " + getCurrent_TimeWithBracketsStr);
        String actualCurrent_TimeWithBracketsStr = getCurrent_TimeWithBracketsStr.substring(0, 5);
        System.out.println("Actual:" + actualCurrent_TimeWithBracketsStr);
        Assert.assertEquals(getCurrent_TimeWithBracketsStr.length(),8);
        Assert.assertEquals(actualCurrent_TimeWithBracketsStr, expectedCurrentTimeWBStr);
    }

    @Test(priority = 9, enabled = true, description = "验证函数Current_TimeStamp返回结果正常")
    public void test10Current_TimeStampFunc() throws SQLException {
//        UTCTimestampFormat utcTimestamp = new UTCTimestampFormat();
//        String currentUTCTimeStamp = utcTimestamp.getUTCTimestampStr();
//        String expectedUTCTimeStampStr = utcTimestamp.formatUTCTimestamp(currentUTCTimeStamp);
        String expectedUTCTimeStampStr = JDK8DateTime.formatNow();
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
//        UTCTimestampFormat utcTimestampWithBrackets = new UTCTimestampFormat();
//        String currentUTCTimeStamp = utcTimestampWithBrackets.getUTCTimestampStr();
//        String expectedUTCTimeStampStr = utcTimestampWithBrackets.formatUTCTimestamp(currentUTCTimeStamp);
        String expectedUTCTimeStampStr = JDK8DateTime.formatNow();
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

    @Test(priority = 11, enabled = true, expectedExceptions = SQLException.class,
            description = "验证函数from_UnixTime传入字符串为非数值型，预期失败")
    public void test12From_UnixTimeFunc_NotNumArg() throws SQLException {
        dateTimeObj.from_UnixTimeNotNumStrFunc();
    }

    @Test(priority = 11, enabled = true, expectedExceptions = SQLException.class,
            description = "验证函数from_UnixTime参数为空，预期失败")
    public void test12From_UnixTimeFuncNoArg() throws SQLException {
        dateTimeObj.from_UnixTimeNoArg();
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
    public void test13Unix_TimeStampNoArg() throws SQLException {
        String currentTimeStamp = String.valueOf(System.currentTimeMillis()/1000);
        System.out.println("Current Timestamp: " + currentTimeStamp);
        String returnUnix_TimeStampWithoutArg = dateTimeObj.unix_TimeStampNoArgFunc();
        System.out.println("Return Timestamp: " + returnUnix_TimeStampWithoutArg);
        Assert.assertEquals(returnUnix_TimeStampWithoutArg.length(), 10);

        int timeStampDiff = Integer.parseInt(returnUnix_TimeStampWithoutArg) - Integer.parseInt(currentTimeStamp);
        Assert.assertTrue(Math.abs(timeStampDiff) < 60);
    }

    @Test(priority = 12, enabled = true, dataProvider = "yamlDataMethod",
            description = "验证函数unix_TimeStamp参数为数字，按原参数返回")
    public void test13Unix_TimeStampNumArg(Map<String, String> param) throws SQLException {
        String expectedUnix_TimeStamp = param.get("outputTimestamp");
        System.out.println("Expected: " + expectedUnix_TimeStamp);
        String actualUnix_TimeStamp = dateTimeObj.unix_TimeStampNumArg(param.get("inputDate"));
        System.out.println("Actual: " + actualUnix_TimeStamp);
        Assert.assertEquals(actualUnix_TimeStamp, expectedUnix_TimeStamp);
    }

    @Test(priority = 12, enabled = true, dataProvider = "yamlDataMethod",
            description = "验证函数unix_TimeStamp参数为日期函数时的返回结果正常")
    public void test13Unix_TimeStampFuncArg(Map<String, String> param) throws SQLException {
        Long currentTimeStamp = System.currentTimeMillis()/1000;
        System.out.println("Current Timestamp: " + currentTimeStamp);
        String returnUnix_TimeStampWithFuncArg = dateTimeObj.unix_TimeStampFuncArg(param.get("argFunc"));
        System.out.println("Return Timestamp: " + returnUnix_TimeStampWithFuncArg);
        Assert.assertEquals(returnUnix_TimeStampWithFuncArg.length(), 10);
        int timeStampDiff = (int) (Integer.parseInt(returnUnix_TimeStampWithFuncArg) - currentTimeStamp);
        Assert.assertTrue(Math.abs(timeStampDiff) < 60);


        /**
         *2022-07-22：需求变更后，不再支持日期格式
         */

//        if (param.get("argFunc").equals("Now()") || param.get("argFunc").equals("Current_TimeStamp()") ||
//                param.get("argFunc").equals("Current_TimeStamp")) {
//            int timeStampDiff = (int) (Integer.parseInt(returnUnix_TimeStampWithFuncArg) - currentTimeStamp);
//            Assert.assertTrue(Math.abs(timeStampDiff) < 60);
//        } else if (param.get("argFunc").equals("CurDate()") || param.get("argFunc").equals("Current_Date()") ||
//                param.get("argFunc").equals("Current_Date")) {
//            Long todayStartTime = GetZeroTimestampOfCurDate.getTodayStartTime();
//            System.out.println("CurDateStartTimeStamp: " + todayStartTime);
//            Assert.assertTrue(returnUnix_TimeStampWithFuncArg.equals(String.valueOf(todayStartTime)));
//        }
    }

    @Test(priority = 12, enabled = true, dataProvider = "yamlNegativeDateTimeMethod", expectedExceptions = SQLException.class,
            description = "验证函数unix_TimeStamp输入日期格式非法，预期异常")
    public void test13Unix_TimeStampNegativeDate(Map<String, String> param) throws SQLException {
        dateTimeObj.unix_TimeStampFunc(param.get("inputDate"));

    }

    @Test(priority = 12, enabled = true, dataProvider = "yamlNegativeDateTimeMethod", expectedExceptions = SQLException.class,
            description = "验证函数unix_TimeStamp输入带有小数的数字或curdate函数，预期异常")
    public void test13Unix_TimeStampNegativeNum(Map<String, String> param) throws SQLException {
        dateTimeObj.unix_TimeStampNumArg(param.get("inputDate"));

    }

    /**
     * 2022-07-22：需求变更后，unix_timestamp不再支持date类型的格式，用例预期异常
     */
    @Test(priority = 12, enabled = true, expectedExceptions = SQLException.class, dependsOnMethods = {"test02DateTimeInsert"},
            description = "验证函数unix_TimeStamp在表格中Date字段使用，预期异常")
    public void test13Unix_TimeStampInTableDate() throws SQLException {
        List<String> expectedQueryUTSBList = new ArrayList<>();
        // unix_timestamp接收日期不考虑夏令时，1987-07-16日按标准时间返回时间戳
        //String[] ustbArray = new String[]{"891792000","570988800","1646323200","1605024000","1285862400","553363200","-662716800"};

        // unix_timestamp接收日期考虑夏令时，1987-07-16日为中国历史上的DST规则的DST offset=1的区间内，因此时间戳会减少一小时
        String[] ustbArray = new String[]{"891792000","570988800","1646323200","1605024000","1285862400","553359600","-662716800"};
        for (int i=0; i < ustbArray.length; i++){
            expectedQueryUTSBList.add(ustbArray[i]);
        }
        System.out.println("期望查询到的列表为：" + expectedQueryUTSBList);

        List<String> actualQueryUTSBList = dateTimeObj.queryUnix_timestampDateInTable();
        System.out.println("实际查询到的列表为：" + actualQueryUTSBList);
        Assert.assertTrue(actualQueryUTSBList.equals(expectedQueryUTSBList));
    }

    @Test(priority = 12, enabled = true, dependsOnMethods = {"test02DateTimeInsert"},
            description = "验证函数unix_TimeStamp在表格中使用时的返回结果正常")
    public void test13Unix_TimeStampInTableTimestamp() throws SQLException {
        List<String> expectedQueryUTSCList = new ArrayList<>();
        String[] ustcArray = new String[]{"1649412307","951753600","920217599","1620100800","1285869722","-536528868","1669827723"};
        for (int i=0; i < ustcArray.length; i++){
            expectedQueryUTSCList.add(ustcArray[i]);
        }
        System.out.println("期望查询到的列表为：" + expectedQueryUTSCList);

        List<String> actualQueryUTSCList = dateTimeObj.queryUnix_timestampTimeStampInTable();
        System.out.println("实际查询到的列表为：" + actualQueryUTSCList);
        Assert.assertTrue(actualQueryUTSCList.equals(expectedQueryUTSCList));
    }

    @Test(priority = 13, enabled = true, dataProvider = "yamlDataMethod", description = "验证函数date_format字符串参数的返回结果正常")
    public void test14Date_FormatStrArgFunc(Map<String, String> param) throws SQLException {
        String expectedDate_FormatSargStr = param.get("outputDate");
        System.out.println("Expected: " + expectedDate_FormatSargStr);
        String actualDate_FormatSargStr = dateTimeObj.date_FormatStrArgFunc(param.get("inputDate"), param.get("inputFormat"));
        System.out.println("Actual: " + actualDate_FormatSargStr);

        Assert.assertEquals(actualDate_FormatSargStr, expectedDate_FormatSargStr);
    }

    /**
     * 2022-07-22：需求变更后，date_format函数的第一个参数不再支持数字，传入数字，预期异常。
     */
    @Test(priority = 13, enabled = true, dataProvider = "yamlDataMethod", expectedExceptions = SQLException.class,
            description = "验证函数date_format日期参数为数字，预期异常")
    public void test14Date_FormatNumArgFunc(Map<String, String> param) throws SQLException {
        String expectedDate_FormatNargStr = param.get("outputDate");
        System.out.println("Expected: " + expectedDate_FormatNargStr);
        String actualDate_FormatNargStr = dateTimeObj.date_FormatNumArgFunc(param.get("inputDate"), param.get("inputFormat"));
        System.out.println("Actual: " + actualDate_FormatNargStr);
        Assert.assertEquals(actualDate_FormatNargStr, expectedDate_FormatNargStr);
    }

    @Test(priority = 13, enabled = true, dataProvider = "yamlDataMethod", description = "验证函数date_format参数为函数的返回结果正常")
    public void test14Date_FormatFuncArgFunc(Map<String, String> param) throws SQLException {
        String actualDate_FormatFuncArgStr = dateTimeObj.date_FormatFuncArg(param.get("argFunc"), param.get("inputFormat"));
        System.out.println("Actual: " + actualDate_FormatFuncArgStr);
        boolean matchRst = actualDate_FormatFuncArgStr.matches(param.get("matchReg"));
        Assert.assertTrue(matchRst);
    }


    /**
     * 2022-07-22: 最新实现，date_format缺少格式参数，默认按标准日期格式输出
     * @param param
     * @throws SQLException
     */
    @Test(priority = 13, enabled = true, dataProvider = "yamlDataMethod", description = "验证函数date_format缺少格式参数，默认按标准日期格式输出")
    public void test14Date_FormatMissingFormatArg(Map<String, String> param) throws SQLException {
        String expectedOutDate = param.get("outDate");
        System.out.println("Expected: " + expectedOutDate);
        String actualOutDate = dateTimeObj.date_FormatMissingFormatArg(param.get("inputDate"));
        System.out.println("Actual: " + actualOutDate);
        Assert.assertEquals(actualOutDate, expectedOutDate);
    }

    @Test(priority = 13, enabled = true, dependsOnMethods = {"test02DateTimeInsert"}, description = "验证函数date_format在表格中使用时的返回结果正常")
    public void test14Date_FormatTableDate() throws SQLException {
        List<String> expectedQueryDFBList = new ArrayList<>();
        String[] dfbArray = new String[]{"1998 year 04 month 06 day","1988 year 02 month 05 day","2022 year 03 month 04 day",
                "2020 year 11 month 11 day","2010 year 10 month 01 day","1987 year 07 month 16 day","1949 year 01 month 01 day"};
        for (int i=0; i < dfbArray.length; i++){
            expectedQueryDFBList.add(dfbArray[i]);
        }
        System.out.println("期望查询到的列表为：" + expectedQueryDFBList);

        List<String> actualQueryDFBList = dateTimeObj.queryDate_FormatDateInTable();
        System.out.println("实际查询到的列表为：" + actualQueryDFBList);
        Assert.assertTrue(actualQueryDFBList.equals(expectedQueryDFBList));
    }

    @Test(priority = 13, enabled = true, description = "验证函数date_format参数为Null返回Null")
    public void test14Date_FormatNullArg() throws SQLException {
        String actualDate_FormatNullArgStr = dateTimeObj.date_FormatNullArg();
        System.out.println("Actual: " + actualDate_FormatNullArgStr);
        Assert.assertNull(actualDate_FormatNullArgStr);
    }

    @Test(priority = 13, enabled = true, description = "验证函数date_format参数为空字符串返回Null")
    public void test14Date_FormatEmptyDateArg() throws SQLException {
        String actualDate_FormatEmptyDateArgStr = dateTimeObj.date_FormatEmptyArg();
        System.out.println("Actual: " + actualDate_FormatEmptyDateArgStr);
        Assert.assertNull(actualDate_FormatEmptyDateArgStr);
    }

    @Test(priority = 13, enabled = true, dataProvider = "yamlNegativeDateTimeMethod", expectedExceptions = SQLException.class,
            description = "验证函数date_format日期格式非法，预期异常")
    public void test14Date_FormatNegativeDate(Map<String, String> param) throws SQLException {
        dateTimeObj.date_FormatStrArgFunc(param.get("inputDate"), param.get("inputFormat"));
    }

    @Test(priority = 13, enabled = true, dataProvider = "yamlNegativeDateTimeMethod", expectedExceptions = SQLException.class,
            description = "验证函数date_format缺少参数，预期异常")
    public void test14Date_FormatMissingDateArg(Map<String, String> param) throws SQLException {
        dateTimeObj.date_FormatMissingDateArg(param.get("inputParam"));
    }

    @Test(priority = 14, enabled = true, dataProvider = "yamlDataMethod", description = "验证函数datediff字符串参数的返回结果正常")
    public void test15DateDiffStrArgFunc(Map<String, String> param) throws SQLException {
        String expectedDateStrArgDiff = param.get("outputDiff");
        System.out.println("Expected: " + expectedDateStrArgDiff);
        String actualDateStrArgDiff = dateTimeObj.dateDiffStrArgFunc(param.get("inputDate1"), param.get("inputDate2"));
        System.out.println("Actual: " + actualDateStrArgDiff);
        Assert.assertEquals(actualDateStrArgDiff, expectedDateStrArgDiff);
    }


    /**
     * 2022-07-22: 需求变更后，datediff函数不再支持数字类型的日期参数，预期异常
     * @param param
     * @throws SQLException
     */
    @Test(priority = 14, enabled = true, dataProvider = "yamlDataMethod", expectedExceptions = SQLException.class,
            description = "验证函数datediff数字参数的返回结果正常")
    public void test15DateDiffNumArgFunc(Map<String, String> param) throws SQLException {
        String expectedDateNumArgDiff = param.get("outputDiff");
        System.out.println("Expected: " + expectedDateNumArgDiff);
        String actualDateNumArgDiff = dateTimeObj.dateDiffNumArgFunc(param.get("inputDate1"), param.get("inputDate2"));
        System.out.println("Actual: " + actualDateNumArgDiff);
        Assert.assertEquals(actualDateNumArgDiff, expectedDateNumArgDiff);
    }

    @Test(priority = 14, enabled = true, dataProvider = "yamlDataMethod", description = "验证函数datediff参数1为函数的返回结果正常")
    public void test15DateDiffFuncArg1Func(Map<String, String> param) throws SQLException, ParseException {
        Long expectedDateFuncArg1Diff = GetDateDiff.getDiffDate(param.get("inputDate2"));
        System.out.println("Expected: " + expectedDateFuncArg1Diff);
        String actualDateFuncArg1Diff = dateTimeObj.dateDiffFuncArg1Func(param.get("argFunc"), param.get("inputDate2"));
        System.out.println("Actual: " + actualDateFuncArg1Diff);
        Assert.assertTrue(actualDateFuncArg1Diff.equals(String.valueOf(expectedDateFuncArg1Diff)));
    }

    @Test(priority = 14, enabled = true, dataProvider = "yamlDataMethod", description = "验证函数datediff参数2为函数的返回结果正常")
    public void test15DateDiffFuncArg2Func(Map<String, String> param) throws SQLException, ParseException {
        Long expectedDateFuncArg2Diff = -GetDateDiff.getDiffDate(param.get("inputDate2"));
        System.out.println("Expected: " + expectedDateFuncArg2Diff);
        String actualDateFuncArg2Diff = dateTimeObj.dateDiffFuncArg2Func(param.get("argFunc"), param.get("inputDate2"));
        System.out.println("Actual: " + actualDateFuncArg2Diff);
        Assert.assertTrue(actualDateFuncArg2Diff.equals(String.valueOf(expectedDateFuncArg2Diff)));
    }

    @Test(priority = 14, enabled = true, dataProvider = "yamlNegativeDateTimeMethod", expectedExceptions = SQLException.class,
            description = "验证函数datediff日期格式非法，预期异常")
    public void test15DateDiffNegativeDate(Map<String, String> param) throws SQLException {
        dateTimeObj.dateDiffStrArgFunc(param.get("inputDate1"), param.get("inputDate2"));
    }

    @Test(priority = 14, enabled = true, dataProvider = "yamlNegativeDateTimeMethod", expectedExceptions = SQLException.class,
            description = "验证函数datediff参数数量不正确，预期异常")
    public void test15DateDiffWrongArg(Map<String, String> param) throws SQLException {
        dateTimeObj.dateDiffStateArg(param.get("datediffState"));
    }

    @Test(priority = 14, enabled = true, dataProvider = "yamlDataMethod",
            description = "验证函数datediff参数有一个或两个为null或空字符串，预期输出null")
    public void test15DateDiffEmptyNullState(Map<String, String> param) throws SQLException {
        String expectedDateDiff = param.get("outputDiff");
        System.out.println("Expected: " + expectedDateDiff);
        String actualDateDiff = dateTimeObj.dateDiffStateArg(param.get("datediffState"));
        System.out.println("Actual: " + actualDateDiff);
        Assert.assertEquals(actualDateDiff, expectedDateDiff);
    }

    @Test(priority = 15, enabled = true, dependsOnMethods = {"test02DateInsert"}, dataProvider = "yamlDataMethod", description = "验证插入不同格式的日期成功")
    public void test16VariousFormatDateInsert(Map<String, String> param) throws SQLException, ClassNotFoundException {
        String expectedDateQuery = param.get("expectedDate");
        System.out.println("Expected: " + expectedDateQuery);
        String actualDateQuery = dateTimeObj.insertVariousFormatDateValues(param.get("insertID"), param.get("insertDate"));
        System.out.println("Actual: " + actualDateQuery);
        Assert.assertEquals(actualDateQuery, expectedDateQuery);
    }

    @Test(priority = 15, enabled = true, dependsOnMethods = {"test02TimeInsert"}, dataProvider = "yamlDataMethod",
            description = "验证插入不同格式的时间成功")
    public void test16VariousFormatTimeInsert(Map<String, String> param) throws SQLException, ClassNotFoundException {
        String expectedTimeQuery = param.get("expectedTime");
        System.out.println("Expected: " + expectedTimeQuery);
        String actualTimeQuery = dateTimeObj.insertVariousFormatTimeValues(param.get("insertID"), param.get("insertTime"));
        System.out.println("Actual: " + actualTimeQuery);
        Assert.assertEquals(actualTimeQuery, expectedTimeQuery);
    }


    @Test(priority = 16, enabled = true, dataProvider = "yamlDataMethod", description = "验证函数和字符串上下文返回")
    public void test17FuncConcatStr(Map<String, String> param) throws SQLException {
        String actualFuncConcatStr = dateTimeObj.funcConcatStr(param.get("funcName"));
        System.out.println("Actual: " + actualFuncConcatStr);
        boolean matchResult = actualFuncConcatStr.matches(param.get("matchReg"));
        Assert.assertTrue(matchResult);
    }

    @Test(priority = 17, enabled = true, dependsOnMethods = {"test01TableWithDateFieldCreate"}, dataProvider = "yamlNegativeDateTimeMethod",
            expectedExceptions = SQLException.class, description = "验证插入非法日期，预期异常")
    public void test18InsertNegativeDate(Map<String, String> param) throws SQLException, ClassNotFoundException {
        dateTimeObj.insertNegativeDate(param.get("insertDate"));
    }

    @Test(priority = 18, enabled = true, dependsOnMethods = {"test01TableWithTimeFieldCreate"}, dataProvider = "yamlNegativeDateTimeMethod",
            expectedExceptions = SQLException.class, description = "验证插入非法时间，预期异常")
    public void test19InsertNegativeTime(Map<String, String> param) throws SQLException, ClassNotFoundException {
        dateTimeObj.insertNegativeTime(param.get("insertTime"));
    }

    @Test(priority = 19, enabled = true, dependsOnMethods = {"test01TableWithTimestampFieldCreate"}, dataProvider = "yamlNegativeDateTimeMethod",
            expectedExceptions = SQLException.class, description = "验证插入非法timestamp，预期异常")
    public void test20InsertNegativeTimeStamp(Map<String, String> param) throws SQLException, ClassNotFoundException {
        dateTimeObj.insertNegativeTimeStamp(param.get("insertTimeStamp"));
    }

    @Test(priority = 20, enabled = true, dataProvider = "yamlDataMethod", description = "验证时间日期函数作为字段值插入表格")
    public void test21InsertWithFunc(Map<String, String> param) throws SQLException {
        if (param.get("insertFunc").equals("Now()") || param.get("insertFunc").equals("Current_TimeStamp()") || param.get("insertFunc").equals("Current_TimeStamp")) {
            String expectedNowStr = JDK8DateTime.formatNow();
            System.out.println("Expected: " + expectedNowStr);
            String actualQueryNowStr = dateTimeObj.createTableUsingFunc(param.get("insertType"), param.get("insertFunc"), param.get("insertID"));
            System.out.println("Actual: " + actualQueryNowStr);
            Assert.assertEquals(actualQueryNowStr.length(), 19);
            Assert.assertEquals(actualQueryNowStr.substring(0,16), expectedNowStr);
        }else if(param.get("insertFunc").equals("CurDate()") || param.get("insertFunc").equals("Current_Date()") || param.get("insertFunc").equals("Current_Date")) {
            String expectedDateStr = JDK8DateTime.formatCurDate();
            System.out.println("Expected: " + expectedDateStr);
            String actualQueryCurDateStr = dateTimeObj.createTableUsingFunc(param.get("insertType"), param.get("insertFunc"), param.get("insertID"));
            System.out.println("Actual: " + actualQueryCurDateStr);
            Assert.assertEquals(actualQueryCurDateStr, expectedDateStr);
        } else if(param.get("insertFunc").equals("CurTime()") || param.get("insertFunc").equals("Current_Time()") || param.get("insertFunc").equals("Current_Time")) {
            String expectedTimeStr = JDK8DateTime.formatCurTime();
            System.out.println("Expected: " + expectedTimeStr);
            String actualQueryCurTimeStr = dateTimeObj.createTableUsingFunc(param.get("insertType"), param.get("insertFunc"), param.get("insertID"));
            System.out.println("Actual: " + actualQueryCurTimeStr);
            Assert.assertEquals(actualQueryCurTimeStr.length(),8);
            Assert.assertEquals(actualQueryCurTimeStr.substring(0,5), expectedTimeStr);
        } else if(param.get("insertFunc").equals("Unix_TimeStamp()")) {
            String actualQueryTimestamp = dateTimeObj.createTableUsingFunc(param.get("insertType"), param.get("insertFunc"), param.get("insertID"));
            System.out.println("Actual: " + actualQueryTimestamp);
            Assert.assertEquals(actualQueryTimestamp.length(), 10);
            Long currentTimeStamp = System.currentTimeMillis()/1000;
            System.out.println("Current: " + currentTimeStamp);
            int timeStampDiff = (int) (Integer.parseInt(actualQueryTimestamp) - currentTimeStamp);
            Assert.assertTrue(Math.abs(timeStampDiff) < 60);
        }
    }

    @Test(priority = 21, enabled = true, dataProvider = "yamlDataMethod", description = "验证函数time_format字符串参数的返回结果正常")
    public void test22Time_FormatStrArgFunc(Map<String, String> param) throws SQLException {
        String expectedTime_FormatSargStr = param.get("outputTime");
        System.out.println("Expected: " + expectedTime_FormatSargStr);
        String actualTime_FormatSargStr = dateTimeObj.time_FormatStrArgFunc(param.get("inputTime"), param.get("inputFormat"));
        System.out.println("Actual: " + actualTime_FormatSargStr);

        Assert.assertEquals(actualTime_FormatSargStr, expectedTime_FormatSargStr);
    }


    /**
     * 2022-07-22: 需求变更后，time_format函数不再支持数字类型的时间参数，预期失败
     * @param param
     * @throws SQLException
     */
    @Test(priority = 21, enabled = true, dataProvider = "yamlDataMethod", expectedExceptions = SQLException.class,
            description = "验证函数time_format数字参数的返回结果正常")
    public void test22Time_FormatNumArgFunc(Map<String, String> param) throws SQLException {
        String expectedTime_FormatNargStr = param.get("outputTime");
        System.out.println("Expected: " + expectedTime_FormatNargStr);
        String actualTime_FormatNargStr = dateTimeObj.time_FormatNumArgFunc(param.get("inputTime"), param.get("inputFormat"));
        System.out.println("Actual: " + actualTime_FormatNargStr);
        Assert.assertEquals(actualTime_FormatNargStr, expectedTime_FormatNargStr);
    }

    @Test(priority = 21, enabled = true, dataProvider = "yamlDataMethod", description = "验证函数time_format参数为函数的返回结果正常")
    public void test22Time_FormatArgFunc(Map<String, String> param) throws SQLException {
        String actualTime_FormatFuncArgStr = dateTimeObj.time_FormatFuncArg(param.get("argFunc"), param.get("inputFormat"));
        System.out.println("Actual: " + actualTime_FormatFuncArgStr);
        boolean matchRst = actualTime_FormatFuncArgStr.matches(param.get("matchReg"));
        Assert.assertTrue(matchRst);
    }

    @Test(priority = 21, enabled = true, dataProvider = "yamlNegativeDateTimeMethod", expectedExceptions = SQLException.class,
            description = "验证函数time_format参数为非时间函数，预期失败")
    public void test22Time_FormatNegativeArgFunc(Map<String, String> param) throws SQLException {
        dateTimeObj.time_FormatFuncArg(param.get("argFunc"), param.get("inputFormat"));
    }

    @Test(priority = 21, enabled = true, dataProvider = "yamlNegativeDateTimeMethod", expectedExceptions = SQLException.class,
            description = "验证函数time_format时间格式非法，预期异常")
    public void test22Time_FormatNegativeTimeStr(Map<String, String> param) throws SQLException {
        dateTimeObj.time_FormatStrArgFunc(param.get("inputTime"), param.get("inputFormat"));
    }

    @Test(priority = 21, enabled = true, dataProvider = "yamlNegativeDateTimeMethod", expectedExceptions = SQLException.class,
            description = "验证函数time_format时间数字非法，预期异常")
    public void test22Time_FormatNegativeTimeNum(Map<String, String> param) throws SQLException {
        dateTimeObj.time_FormatNumArgFunc(param.get("inputTime"), param.get("inputFormat"));
    }

    /**
     * 2022-07-22: 最新实现，time_format缺少格式参数，默认按标准时间格式输出
     * @param param
     * @throws SQLException
     */
    @Test(priority = 21, enabled = true, dataProvider = "yamlDataMethod",
            description = "验证函数time_format缺少格式参数，默认按标准时间格式输出")
    public void test22Time_FormatMissingFormatArg(Map<String, String> param) throws SQLException {
        String expectedOutTime = param.get("outTime");
        System.out.println("Expected: " + expectedOutTime);
        String actualOutTime = dateTimeObj.time_FormatMissingFormatArg(param.get("inputTime"));
        System.out.println("Actual: " + actualOutTime);
        Assert.assertEquals(actualOutTime, expectedOutTime);
    }

    @Test(priority = 21, enabled = true, dataProvider = "yamlNegativeDateTimeMethod", expectedExceptions = SQLException.class,
            description = "验证函数time_format缺少参数，预期异常")
    public void test22Time_FormatMissingArg(Map<String, String> param) throws SQLException {
        dateTimeObj.time_FormatMissingArg(param.get("inputParam"));
    }

    @Test(priority = 21, enabled = true, description = "验证函数time_format参数1为Null返回Null")
    public void test22Time_FormatNullArg() throws SQLException {
        String actualTime_FormatNullArgStr = dateTimeObj.time_FormatNullArg();
        System.out.println("Actual: " + actualTime_FormatNullArgStr);
        Assert.assertNull(actualTime_FormatNullArgStr);
    }

    @Test(priority = 21, enabled = true, description = "验证函数time_format参数1为空字符串返回Null")
    public void test22Time_FormatEmptyTimeArg() throws SQLException {
        String actualTime_FormatEmptyTimeArgStr = dateTimeObj.time_FormatEmptyArg();
        System.out.println("Actual: " + actualTime_FormatEmptyTimeArgStr);
        Assert.assertNull(actualTime_FormatEmptyTimeArgStr);
    }

    @Test(priority = 21, enabled = true, dependsOnMethods = {"test02TimeInsert"},
            description = "验证函数time_format在表格中使用时的返回结果正常")
    public void test22Time_FormatTableTime() throws SQLException {
        List<String> expectedQueryList = new ArrayList<>();
        String[] formatOutArray = new String[]{"08:10:10","06:15:08","07:03:15","05:59:59","19:00:00","23:59:59",
                "01:02:03","00:30:08","02:02:00","00:00:00"};
        for (int i=0; i < formatOutArray.length; i++){
            expectedQueryList.add(formatOutArray[i]);
        }
        System.out.println("期望查询到的列表为：" + expectedQueryList);

        List<String> actualQueryList = dateTimeObj.queryTime_FormatTimeInTable();
        System.out.println("实际查询到的列表为：" + actualQueryList);
        Assert.assertTrue(actualQueryList.equals(expectedQueryList));
    }

    public List<List> expectedUpdateSingleRow() {
        String[][] dataArray = {{"7","new","33","1111.111","new Addr","2022-05-19","18:33:00","1949-07-01 23:59:59","true"}};
        List<List> expectedList = new ArrayList<List>();
        for(int i=0; i<dataArray.length; i++) {
            List columnList = new ArrayList();
            for (int j=0; j<dataArray[i].length; j++) {
                columnList.add(dataArray[i][j]);
            }
            expectedList.add(columnList);
        }
        return expectedList;
    }

    public List<List> expectedUpdateAllRow() {
        String[][] dataArray = {{"1","zhangsan","100","22.22","BJ","1998-04-06","08:10:10","2022-10-01 10:08:06","false"},
                {"2","lisi","100","22.22","BJ","1988-02-05","06:15:08","2022-10-01 10:08:06","false"},
                {"3","l3","100","22.22","BJ","2022-03-04","07:03:15","2022-10-01 10:08:06","false"},
                {"4","HAHA","100","22.22","BJ","2020-11-11","05:59:59","2022-10-01 10:08:06","false"},
                {"5","awJDs","100","22.22","BJ","2010-10-01","19:00:00","2022-10-01 10:08:06","false"},
                {"6","123","100","22.22","BJ","1987-07-16","01:02:03","2022-10-01 10:08:06","false"},
                {"7","new","100","22.22","BJ","2022-05-19","18:33:00","2022-10-01 10:08:06","false"}};
        List<List> expectedList = new ArrayList<List>();
        for(int i=0; i<dataArray.length; i++) {
            List columnList = new ArrayList();
            for (int j=0; j<dataArray[i].length; j++) {
                columnList.add(dataArray[i][j]);
            }
            expectedList.add(columnList);
        }
        return expectedList;
    }

    @Test(priority = 22, enabled = true, description = "验证更新单行多字段")
    public void test23UpdateSingleRowMultiCol() throws Exception {
        List<List> expectedList = expectedUpdateSingleRow();
        System.out.println("Expected: " + expectedList);
        List<List> actualUpdateList = dateTimeObj.updateSingleRow();
        System.out.println("Actual: " + actualUpdateList);

        Assert.assertTrue(actualUpdateList.containsAll(expectedList));
        Assert.assertTrue(expectedList.containsAll(actualUpdateList));
    }

    @Test(priority = 22, enabled = true, dependsOnMethods = {"test23UpdateSingleRowMultiCol"},
            description = "验证更新全部行多字段")
    public void test23UpdateAllRows() throws Exception {
        List<List> expectedList = expectedUpdateAllRow();
        System.out.println("Expected: " + expectedList);
        List<List> actualUpdateList = dateTimeObj.updateAllRow();
        System.out.println("Actual: " + actualUpdateList);

        Assert.assertTrue(actualUpdateList.containsAll(expectedList));
        Assert.assertTrue(expectedList.containsAll(actualUpdateList));
    }

    @Test(priority = 23, enabled = true, expectedExceptions = SQLException.class, description = "验证date类型字段，插入空值预期异常")
    public void test24InsertBlankDate() throws SQLException {
        dateTimeObj.insertBlankDate();
    }

    @Test(priority = 24, enabled = true, expectedExceptions = SQLException.class, description = "验证time类型字段，插入空值预期异常")
    public void test25InsertBlankTime() throws SQLException {
        dateTimeObj.insertBlankTime();
    }

    @Test(priority = 25, enabled = true, expectedExceptions = SQLException.class, description = "验证timestamp类型字段，插入空值预期异常")
    public void test26InsertBlankTimestamp() throws SQLException {
        dateTimeObj.insertBlankTimestamp();
    }

    @Test(priority = 26, enabled = true, dataProvider = "yamlDataMethod", description = "验证函数timestamp_format参数为函数的返回结果正常")
    public void test27Timestamp_FormatFuncArgFunc(Map<String, String> param) throws SQLException {
        String actualTimestamp_FormatFuncArgStr = dateTimeObj.timestamp_FormatFuncArg(param.get("argFunc"), param.get("inputFormat"));
        System.out.println("Actual: " + actualTimestamp_FormatFuncArgStr);
        boolean matchRst = actualTimestamp_FormatFuncArgStr.matches(param.get("matchReg"));
        Assert.assertTrue(matchRst);
    }

    @Test(priority = 26, enabled = true, dataProvider = "yamlDataMethod", description = "验证函数timestamp_format字符串参数的返回结果正常")
    public void test28Timestamp_FormatStrArgFunc(Map<String, String> param) throws SQLException {
        String expectedTimestamp_FormatSargStr = param.get("outputDate");
        System.out.println("Expected: " + expectedTimestamp_FormatSargStr);
        String actualTimestamp_FormatSargStr = dateTimeObj.timestamp_FormatStrArgFunc(param.get("inputDate"), param.get("inputFormat"));
        System.out.println("Actual: " + actualTimestamp_FormatSargStr);

        Assert.assertEquals(actualTimestamp_FormatSargStr, expectedTimestamp_FormatSargStr);
    }

    @Test(priority = 26, enabled = true, dependsOnMethods = {"test02DateTimeInsert"},
            description = "验证函数timestamp_format在表格中对timestamp类型字段使用时的返回结果正常")
    public void test29Timestamp_FormatTableTimestamp() throws SQLException {
        List<String> expectedQueryList = new ArrayList<>();
        String[] tsfArray = new String[]{"2022/04/08 18.05.07","2000/02/29 00.00.00","1999/02/28 23.59.59",
                "2021/05/04 12.00.00","2010/10/01 02.02.02","1952/12/31 12.12.12","2022/12/01 01.02.03"};
        for (int i=0; i < tsfArray.length; i++){
            expectedQueryList.add(tsfArray[i]);
        }
        System.out.println("期望查询到的列表为：" + expectedQueryList);

        List<String> actualQueryList = dateTimeObj.queryTimestamp_FormatTimestampInTable();
        System.out.println("实际查询到的列表为：" + actualQueryList);
        Assert.assertTrue(actualQueryList.equals(expectedQueryList));
    }


    @AfterClass(alwaysRun = true, description = "测试完成后删除数据和表格并关闭连接")
    public void tearDownAll() throws SQLException {
        String dateTimeTableName = DateTimeFuncs.getDateTimeTableName();
        String dateTableName = DateTimeFuncs.getDateTableName();
        String timeTableName = DateTimeFuncs.getTimeTableName();
        String timestampTableName = DateTimeFuncs.getTimestampTableName();
        Statement tearDownStatement = null;
        try {
            tearDownStatement = DateTimeFuncs.connection.createStatement();
            tearDownStatement.execute("delete from " + dateTimeTableName);
            tearDownStatement.execute("delete from " + dateTableName);
            tearDownStatement.execute("delete from " + timeTableName);
            tearDownStatement.execute("delete from " + timestampTableName);
            tearDownStatement.execute("drop table " + dateTimeTableName);
            tearDownStatement.execute("drop table " + dateTableName);
            tearDownStatement.execute("drop table " + timeTableName);
            tearDownStatement.execute("drop table " + timestampTableName);
            tearDownStatement.execute("drop table Orders705");
            tearDownStatement.execute("drop table Orders673");
            tearDownStatement.execute("drop table Orders6841");
            tearDownStatement.execute("drop table Orders6842");
            tearDownStatement.execute("drop table Orders690");
            tearDownStatement.execute("drop table Orders6961");
            tearDownStatement.execute("drop table Orders6962");
            tearDownStatement.execute("drop table Orders7041");
            tearDownStatement.execute("drop table Orders7042");
            tearDownStatement.execute("drop table Orders752");
//        tearDownStatement.execute("drop table Orders7522");
            tearDownStatement.execute("delete from uptesttable");
            tearDownStatement.execute("drop table uptesttable");
            tearDownStatement.execute("drop table case1434");
            tearDownStatement.execute("drop table case1435");
            tearDownStatement.execute("drop table case1436");
        } catch (SQLException e) {
            e.printStackTrace();
        } finally {
            try {
                if(tearDownStatement != null){
                    tearDownStatement.close();
                }
            } catch (SQLException e) {
                e.printStackTrace();
            }

            try {
                if(DateTimeFuncs.connection != null){
                    DateTimeFuncs.connection.close();
                }
            } catch (SQLException e) {
                e.printStackTrace();
            }
        }
    }
}
