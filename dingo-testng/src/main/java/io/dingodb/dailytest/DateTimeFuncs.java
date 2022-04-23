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

package io.dingodb.dailytest;

import java.sql.*;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;

public class DateTimeFuncs {
//    private static final String defaultConnectIP = "172.20.3.26";
    private static String defaultConnectIP = CommonArgs.getDefaultDingoClusterIP();
    private static final String JDBC_DRIVER = "io.dingodb.driver.client.DingoDriverClient";
    private static final String connectUrl = "jdbc:dingo:thin:url=" + defaultConnectIP + ":8765";
    public static Connection connection = null;

    static{
        try {
            Class.forName(JDBC_DRIVER);
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
        }
        try {
            connection = DriverManager.getConnection(connectUrl);
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    //生成datetime的测试表格名称并返回
    public static String getDateTimeTableName() {
        final String dateTimeTablePrefix = "dateTimeFieldTest";
        String dateTimeTableName = dateTimeTablePrefix + CommonArgs.getCurDateStr();
        return dateTimeTableName;
    }

    //生成date的测试表格名称并返回
    public static String getDateTableName() {
        final String datetablePrefix = "dateFieldTest";
        String dateTableName = datetablePrefix + CommonArgs.getCurDateStr();
        return dateTableName;
    }

    //生成time的测试表格名称并返回
    public static String getTimeTableName() {
        final String timeTablePrefix = "timeFieldTest";
        String timeTableName = timeTablePrefix + CommonArgs.getCurDateStr();
        return timeTableName;
    }

    //生成timestamp的测试表格名称并返回
    public static String getTimestampTableName() {
        final String timestampTablePrefix = "timestampFieldTest";
        String timestampTableName = timestampTablePrefix + CommonArgs.getCurDateStr();
        return timestampTableName;
    }

    //创建含有date,time和timestamp字段类型的表格
    public void createTable() throws Exception {
        String tableName = getDateTimeTableName();
        Statement statement = connection.createStatement();
        String sql = "create table " + tableName + "("
                + "id int,"
                + "name varchar(32) not null,"
                + "age int,"
                + "amount double,"
                + "address varchar(255),"
                + "birthday date,"
                + "createTime time,"
                + "updateTime timestamp,"
                + "primary key(id)"
                + ")";
        statement.execute(sql);
        statement.close();
    }

    // 插入DateTime数据
    public int insertDateTimeValues() throws ClassNotFoundException, SQLException {
        String dateTimeTableName = getDateTimeTableName();
        Statement statement = connection.createStatement();

        String batInsertSql = "insert into " + dateTimeTableName +
                " values (1,'zhangsan',18,23.50,'beijing','1998-4-6', '08:10:10', '2022-4-8 18:05:07'),\n" +
                "(2,'lisi',25,895,' beijing haidian ', '1988-2-05', '06:15:8', '2000-02-29 00:00:00'),\n" +
                "(3,'l3',55,123.123,'wuhan NO.1 Street', '2022-03-4', '07:3:15', '1999-2-28 23:59:59'),\n" +
                "(4,'HAHA',57,9.0762556,'CHANGping', '2020-11-11', '5:59:59', '2021-05-04 12:00:00'),\n" +
                "(5,'awJDs',1,1453.9999,'pingYang1', '2010-10-1', '19:0:0', '2010-10-1 02:02:02'),\n" +
                "(6,'123',544,0,'543', '1987-7-16', '1:2:3', '1952-12-31 12:12:12'),\n" +
                "(7,'yamaha',76,2.30,'beijing changyang', '1949-01-01', '0:30:8', '2022-12-01 1:2:3')";
        int insertRows = statement.executeUpdate(batInsertSql);
        statement.close();
        return insertRows;
    }

    //创建含有date字段类型的表格
    public void createDateTable() throws Exception {
        String dateTableName = getDateTableName();
        Statement statement = connection.createStatement();
        String sql = "create table " + dateTableName + "("
                + "id int,"
                + "name varchar(32) not null,"
                + "age int,"
                + "amount double,"
                + "address varchar(255),"
                + "birthday date,"
                + "primary key(id)"
                + ")";
        statement.execute(sql);
        statement.close();
    }

    // 插入date数据
    public int insertDateValues() throws ClassNotFoundException, SQLException {
        String dateTableName = getDateTableName();
        Statement statement = connection.createStatement();

        String batDateInsertSql = "insert into " + dateTableName +
                " values (1,'zhangsan',18,23.50,'beijing','1998-4-6'),\n" +
                "(2,'lisi',25,895,' beijing haidian ', '1988-2-05'),\n" +
                "(3,'l3',55,123.123,'wuhan NO.1 Street', '2022-03-4'),\n" +
                "(4,'HAHA',57,9.0762556,'CHANGping', '2020-11-11'),\n" +
                "(5,'awJDs',1,1453.9999,'pingYang1', '2010-10-1'),\n" +
                "(6,'123',544,0,'543', '1987-7-16'),\n" +
                "(7,'yamaha',76,2.30,'beijing changyang', '1949-01-01')";
        int insertRows = statement.executeUpdate(batDateInsertSql);
        statement.close();
        return insertRows;
    }

    // 插入其他格式date类型数据并返回查询出的日期
    public String insertVariousFormatDateValues(String inputID, String inputDate) throws ClassNotFoundException, SQLException {
        String dateTableName = getDateTableName();
        Statement statement = connection.createStatement();

        String batDateInsertSql = "insert into " + dateTableName +
                " values (" + inputID + ",'zhangsan',18,23.50,'beijing','" + inputDate +  "')";
        int insertRows = statement.executeUpdate(batDateInsertSql);
        String selectInsertDateSql = "select birthday from " + dateTableName + " where id = " + inputID;
        ResultSet findDateRst = statement.executeQuery(selectInsertDateSql);
        String findDateStr = null;
        while (findDateRst.next()) {
            findDateStr = findDateRst.getString("birthday");
        }
        statement.close();
        return findDateStr;
    }

    //创建含有time字段类型的表格
    public void createTimeTable() throws Exception {
        String timeTableName = getTimeTableName();
        Statement statement = connection.createStatement();
        String sql = "create table " + timeTableName + "("
                + "id int,"
                + "name varchar(32) not null,"
                + "age int,"
                + "amount double,"
                + "address varchar(255),"
                + "create_time time,"
                + "primary key(id)"
                + ")";
        statement.execute(sql);
        statement.close();
    }

    // 插入time数据
    public int insertTimeValues() throws ClassNotFoundException, SQLException {
        String timeTableName = getTimeTableName();
        Statement statement = connection.createStatement();

        String batTimeInsertSql = "insert into " + timeTableName +
                " values (1, 'zhang san', 18, 1342.09,'beijing', '08:10:10'),\n" +
                "(2, 'Hello', 7, 10.50,' beijing haidian ', '06:15:8'),\n" +
                "(3,'l3',55,123.123,'wuhan NO.1 Street', '07:3:15'),\n" +
                "(4,'HAHA',57,9.0762556,'CHANGping', '5:59:59'),\n" +
                "(5,'awJDs',1,1453.9999,'pingYang1', '19:0:0'),\n" +
                "(6,'123',544,0,'543', '23:59:59'),\n" +
                "(7,'yamaha',76,2.30,'beijing changyang', '1:2:3'),\n" +
                "(8,'bilibili', 93, 2345.2, 'Shahe Gaojiao1', '0:30:8'),\n" +
                "(9,'guji', 3, 4, ' Heze No.20 ', '2:2:00'),\n" +
                "(10,'XJ', 9, 0.1, '#87-2-31', '00:00:00')";
        int insertRows = statement.executeUpdate(batTimeInsertSql);
        statement.close();
        return insertRows;
    }

    //创建含有timestamp字段类型的表格
    public void createTimestampTable() throws Exception {
        String timestampTableName = getTimestampTableName();
        Statement statement = connection.createStatement();
        String sql = "create table " + timestampTableName + "("
                + "id int,"
                + "name varchar(32) not null,"
                + "age int,"
                + "amount double,"
                + "address varchar(255),"
                + "upload_time timestamp,"
                + "primary key(id)"
                + ")";
        statement.execute(sql);
        statement.close();
    }

    // 插入timestamp数据
    public int insertTimeStampValues() throws ClassNotFoundException, SQLException {
        String timestampTableName = getTimestampTableName();
        Statement statement = connection.createStatement();

        String batTimeStampInsertSql = "insert into " + timestampTableName +
                " values (1, 'zhang san', 18, 1342.09,'BeiJing', '2022-4-8 18:05:07')";
        int insertRows = statement.executeUpdate(batTimeStampInsertSql);
        statement.close();
        return insertRows;
    }


    // 获取函数Now()返回值
    public String nowFunc() throws SQLException {
        Statement statement = connection.createStatement();
        String nowSql = "select Now()";
        ResultSet nowRst = statement.executeQuery(nowSql);
        String nowString = null;
        while (nowRst.next()) {
            nowString = nowRst.getString(1);
        }
        return nowString;
    }

    // 获取函数CurDate()返回值
    public String curDateFunc() throws SQLException {
        Statement statement = connection.createStatement();
        String curDateSql = "select CurDate()";
        ResultSet curDateRst = statement.executeQuery(curDateSql);
        String curDateString = null;
        while (curDateRst.next()) {
            curDateString = curDateRst.getString(1);
        }
        return curDateString;
    }

    // 获取函数Current_Date返回值
    public String current_DateFunc() throws SQLException {
        Statement statement = connection.createStatement();
        String current_DateSql = "select Current_Date";
        ResultSet current_DateRst = statement.executeQuery(current_DateSql);
        String current_DateString = null;
        while (current_DateRst.next()) {
            current_DateString = current_DateRst.getString(1);
        }
        return current_DateString;
    }

    // 获取函数Current_Date()返回值
    public String current_DateWithBracketsFunc() throws SQLException {
        Statement statement = connection.createStatement();
        String current_DateWithBracketsSql = "select Current_Date()";
        ResultSet current_DateWithBracketsRst = statement.executeQuery(current_DateWithBracketsSql);
        String current_DateWithBracketString = null;
        while (current_DateWithBracketsRst.next()) {
            current_DateWithBracketString = current_DateWithBracketsRst.getString(1);
        }
        return current_DateWithBracketString;
    }

    // 获取函数CurTime()返回值
    public String curTimeFunc() throws SQLException {
        Statement statement = connection.createStatement();
        String curTimeSql = "select CurTime()";
        ResultSet curTimeRst = statement.executeQuery(curTimeSql);
        String curTimeString = null;
        while (curTimeRst.next()) {
            curTimeString = curTimeRst.getString(1);
        }
        return curTimeString;
    }

    // 获取函数current_Time返回值
    public String current_TimeFunc() throws SQLException {
        Statement statement = connection.createStatement();
        String current_TimeSql = "select Current_Time";
        ResultSet current_TimeRst = statement.executeQuery(current_TimeSql);
        String current_TimeString = null;
        while (current_TimeRst.next()) {
            current_TimeString = current_TimeRst.getString(1);
        }
        return current_TimeString;
    }

    // 获取函数current_Time()返回值
    public String current_TimeWithBracketsFunc() throws SQLException {
        Statement statement = connection.createStatement();
        String current_TimeWithBracketsSql = "select Current_Time()";
        ResultSet current_TimeWithBracketsRst = statement.executeQuery(current_TimeWithBracketsSql);
        String current_TimeWithBracketsString = null;
        while (current_TimeWithBracketsRst.next()) {
            current_TimeWithBracketsString = current_TimeWithBracketsRst.getString(1);
        }
        return current_TimeWithBracketsString;
    }

    // 获取函数Current_Timestamp返回值
    public String current_TimeStampFunc() throws SQLException {
        Statement statement = connection.createStatement();
        String current_TimeStampSql = "select Current_TimeStamp";
        ResultSet current_TimeStampRst = statement.executeQuery(current_TimeStampSql);
        String current_TimeStampString = null;
        while (current_TimeStampRst.next()) {
            current_TimeStampString = current_TimeStampRst.getString(1);
        }
        return current_TimeStampString;
    }

    // 获取函数Current_Timestamp()返回值
    public String current_TimeStampWithBracketsFunc() throws SQLException {
        Statement statement = connection.createStatement();
        String current_TimeStampWithBracketsSql = "select Current_TimeStamp()";
        ResultSet current_TimeStampWithBracketsRst = statement.executeQuery(current_TimeStampWithBracketsSql);
        String current_TimeStampWithBracketsString = null;
        while (current_TimeStampWithBracketsRst.next()) {
            current_TimeStampWithBracketsString = current_TimeStampWithBracketsRst.getString(1);
        }
        return current_TimeStampWithBracketsString;
    }

    // 获取函数From_UnixTime参数为timestamp返回值
    public String from_UnixTimeWithTimestampFunc(String inputTimestamp) throws SQLException {
        Statement statement = connection.createStatement();
        String from_UnixTimeSql = "select from_unixtime(" + inputTimestamp + ")";
        ResultSet from_UnixTimeRst = statement.executeQuery(from_UnixTimeSql);
        String from_UnixTimeString = null;
        while (from_UnixTimeRst.next()) {
            from_UnixTimeString = from_UnixTimeRst.getString(1);
        }
        return from_UnixTimeString;
    }

    // 获取函数From_UnixTime参数为数值字符串返回值
    public String from_UnixTimeWithStringFunc() throws SQLException {
        Statement statement = connection.createStatement();
        String from_UnixTimeWithStringSql = "select from_unixtime('1649770110')";
        ResultSet from_UnixTimeWithStringRst = statement.executeQuery(from_UnixTimeWithStringSql);
        String from_UnixTimeWithStringStr = null;
        while (from_UnixTimeWithStringRst.next()) {
            from_UnixTimeWithStringStr = from_UnixTimeWithStringRst.getString(1);
        }
        return from_UnixTimeWithStringStr;
    }

    // 获取函数Unix_TimeStamp返回值
    public String unix_TimeStampFunc(String inputStr) throws SQLException {
        Statement statement = connection.createStatement();
        String unix_TimeStampSql = "select unix_TimeStamp('" + inputStr + "')";
        ResultSet unixTimeStampRst = statement.executeQuery(unix_TimeStampSql);
        String unix_TimeStampStr  = null;
        while (unixTimeStampRst.next()) {
            unix_TimeStampStr = unixTimeStampRst.getString(1);
        }
        return unix_TimeStampStr;
    }

    // 获取函数Unix_TimeStamp参数为空时的返回值
    public String unix_TimeStampNoArgFunc() throws SQLException {
        Statement statement = connection.createStatement();
        String unix_TimeStampNoAgrSql = "select unix_TimeStamp()";
        ResultSet unixTimeStampNoArgRst = statement.executeQuery(unix_TimeStampNoAgrSql);
        String unix_TimeStampNoArgStr  = null;
        while (unixTimeStampNoArgRst.next()) {
            unix_TimeStampNoArgStr = unixTimeStampNoArgRst.getString(1);
        }
        return unix_TimeStampNoArgStr;
    }

    // 获取函数Date_Format参数为字符串返回值
    public String date_FormatStrArgFunc(String inputDate, String inputFormat) throws SQLException {
        Statement statement = connection.createStatement();
        String date_formatSargSql = "select date_format('" + inputDate + "','" + inputFormat + "')";
        ResultSet date_formatSargRst = statement.executeQuery(date_formatSargSql);
        String date_formatSargStr  = null;
        while (date_formatSargRst.next()) {
            date_formatSargStr = date_formatSargRst.getString(1);
        }
        return date_formatSargStr;
    }

    // 获取函数Date_Format参数为数字返回值
    public String date_FormatNumArgFunc(String inputDate, String inputFormat) throws SQLException {
        Statement statement = connection.createStatement();
        String date_formatNargSql = "select date_format(" + inputDate + ",'" + inputFormat + "')";
        ResultSet date_formatNargRst = statement.executeQuery(date_formatNargSql);
        String date_formatNargStr  = null;
        while (date_formatNargRst.next()) {
            date_formatNargStr = date_formatNargRst.getString(1);
        }
        return date_formatNargStr;
    }

    // 获取函数DateDiff参数为字符串返回值
    public String dateDiffStrArgFunc(String Date1, String Date2) throws SQLException {
        Statement statement = connection.createStatement();
        String dateDiffSargSql = "select datediff('" + Date1 + "','" + Date2 + "') as diffDate";
        ResultSet dateDiffSargRst = statement.executeQuery(dateDiffSargSql);
        String dateDiffSargStr  = null;
        while (dateDiffSargRst.next()) {
            dateDiffSargStr = dateDiffSargRst.getString(1);
        }
        return dateDiffSargStr;
    }

    // 获取函数DateDiff参数为数字返回值
    public String dateDiffNumArgFunc(String Date1, String Date2) throws SQLException {
        Statement statement = connection.createStatement();
        String dateDiffNargSql = "select datediff(" + Date1 + "," + Date2 + ") as diffDate";
        ResultSet dateDiffNargRst = statement.executeQuery(dateDiffNargSql);
        String dateDiffNargStr  = null;
        while (dateDiffNargRst.next()) {
            dateDiffNargStr = dateDiffNargRst.getString(1);
        }
        return dateDiffNargStr;
    }

    // 获取字符串上下文返回格式
    public String funcConcatStr(String inputFunc) throws SQLException {
        Statement statement = connection.createStatement();
        String funcConcatSql = "select 'test_'||" + inputFunc;
        ResultSet funcConcatRst = statement.executeQuery(funcConcatSql);
        String funcConcatStr  = null;
        while (funcConcatRst.next()) {
            funcConcatStr = funcConcatRst.getString(1);
        }
        return funcConcatStr;
    }

}
