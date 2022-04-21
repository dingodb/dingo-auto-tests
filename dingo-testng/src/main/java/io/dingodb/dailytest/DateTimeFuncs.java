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

    //生成测试表格名称并返回
    public static String getDateTimeTableName() {
        final String tablePrefix = "dateTimeFieldTest";
        Date date = new Date();
        SimpleDateFormat simpleDateFormat = new SimpleDateFormat("yyyyMMdd");
        String dateNowStr = simpleDateFormat.format(date);
        String tableName = tablePrefix + dateNowStr;
        return tableName;
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

    // 插入数据
    public int insertValues() throws ClassNotFoundException, SQLException {
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

    // 获取函数CurTime()返回值
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

    // 获取函数From_UnixTime返回值
    public String from_UnixTimeFunc() throws SQLException {
        Statement statement = connection.createStatement();
        String from_UnixTimeSql = "select from_unixtime(1650435866)";
        ResultSet from_UnixTimeRst = statement.executeQuery(from_UnixTimeSql);
        String from_UnixTimeString = null;
        while (from_UnixTimeRst.next()) {
            from_UnixTimeString = from_UnixTimeRst.getString(1);
        }
        return from_UnixTimeString;
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

    // 获取函数Date_Format返回值
    public String date_FormatFunc(String inputDate, String inputFormat) throws SQLException {
        Statement statement = connection.createStatement();
        String date_formatSql = "select date_format('" + inputDate + "','" + inputFormat + "')";
        ResultSet date_formatRst = statement.executeQuery(date_formatSql);
        String date_formatStr  = null;
        while (date_formatRst.next()) {
            date_formatStr = date_formatRst.getString(1);
        }
        return date_formatStr;
    }

    // 获取函数DateDiff返回值
    public String dateDiffFunc(String Date1, String Date2) throws SQLException {
        Statement statement = connection.createStatement();
        String dateDiffSql = "select datediff('" + Date1 + "','" + Date2 + "')";
        ResultSet dateDiffRst = statement.executeQuery(dateDiffSql);
        String dateDiffStr  = null;
        while (dateDiffRst.next()) {
            dateDiffStr = dateDiffRst.getString(1);
        }
        return dateDiffStr;
    }

}
