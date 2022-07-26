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

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;

public class DateTimeFuncs {
//    private static final String defaultConnectIP = "172.20.3.27";
//    private static final String defaultConnectIP = "172.20.61.1";
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
        try(Statement statement = connection.createStatement()) {
            String sql = "create table " + tableName + "("
                    + "id int,"
                    + "name varchar(32) not null,"
                    + "age int,"
                    + "amount double,"
                    + "address varchar(255),"
                    + "birthday date,"
                    + "createTime time,"
                    + "update_Time timestamp,"
                    + "primary key(id)"
                    + ")";
            statement.execute(sql);
        }
    }

    // 插入DateTime数据
    public int insertDateTimeValues() throws ClassNotFoundException, SQLException {
        String dateTimeTableName = getDateTimeTableName();
        try(Statement statement = connection.createStatement()) {
            String batInsertSql = "insert into " + dateTimeTableName +
                    " values \n" +
                    "(1,'zhangsan',18,23.50,'beijing','1998-4-6', '08:10:10', '2022-4-8 18:05:07'),\n" +
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
    }

    //创建表格使用函数为字段值
    public String createTableUsingFunc(String insertType, String insertFunc, String insertID) throws SQLException {
        try(Statement statement = connection.createStatement()) {
            String funcCreateSql = "CREATE TABLE Orders" + insertID +
                    "(\n" +
                    "OrderId int NOT NULL,\n" +
                    "ProductName varchar(50) NOT NULL,\n" +
                    "OrderTime " + insertType + " NOT NULL DEFAULT " + insertFunc +
                    ",PRIMARY KEY (OrderId)\n" +
                    ")";
            statement.execute(funcCreateSql);

            String insertFuncSql = "insert into Orders" + insertID + "(OrderID,ProductName) values(" + insertID + ", 'Dingo DB')";
            statement.executeUpdate(insertFuncSql);

            String queryFuncSql = "select * from Orders" + insertID + " where OrderId = " + insertID;
            ResultSet resultSet = statement.executeQuery(queryFuncSql);
            String queryStr = null;
            if (insertFunc.equals("CurTime()") || insertFunc.equals("Current_Time()") || insertFunc.equals("Current_Time")) {
                while(resultSet.next()) {
                    queryStr = resultSet.getTime("OrderTime").toString();
                }
            }else{
                while(resultSet.next()) {
                    queryStr = resultSet.getString("OrderTime");
                }
            }
            statement.close();
            return queryStr;
        }
    }

    //查询unix_timestamp在表格date字段使用的结果
    public List<String> queryUnix_timestampDateInTable() throws SQLException {
        String queryUTSBTable = getDateTimeTableName();
        try(Statement statement = connection.createStatement()) {
            String queryUTSBSql = "select name,Unix_TimeStamp(birthday) UTSB from " + queryUTSBTable + " where id<8";

            ResultSet queryUTSBRst = statement.executeQuery(queryUTSBSql);
            List<String> queryUTSBList = new ArrayList<String>();
            while (queryUTSBRst.next()) {
                queryUTSBList.add(queryUTSBRst.getString("UTSB"));
            }
            statement.close();
            return queryUTSBList;
        }
    }

    //查询unix_timestamp在表格timestamp字段使用的结果
    public List<String> queryUnix_timestampTimeStampInTable() throws SQLException {
        String queryUTSCTable = getDateTimeTableName();
        try(Statement statement = connection.createStatement()) {
            String queryUTSCSql = "select name,Unix_TimeStamp(update_Time) UTSC from " + queryUTSCTable + " where id<8";

            ResultSet queryUTSCRst = statement.executeQuery(queryUTSCSql);
            List<String> queryUTSCList = new ArrayList<String>();
            while (queryUTSCRst.next()) {
                queryUTSCList.add(queryUTSCRst.getString("UTSC"));
            }
            statement.close();
            return queryUTSCList;
        }
    }

    //查询date_format在表格date字段使用的结果
    public List<String> queryDate_FormatDateInTable() throws SQLException {
        String queryDFBTable = getDateTimeTableName();
        try(Statement statement = connection.createStatement()) {
            String queryDFBSql = "select name,date_format(birthday, '%Y year %m month %d day') birth_out from " + queryDFBTable + " where id<8";

            ResultSet queryDFBRst = statement.executeQuery(queryDFBSql);
            List<String> queryDFBList = new ArrayList<String>();
            while (queryDFBRst.next()) {
                queryDFBList.add(queryDFBRst.getString("birth_out"));
            }
            statement.close();
            return queryDFBList;
        }
    }

    //创建含有date字段类型的表格
    public void createDateTable() throws Exception {
        String dateTableName = getDateTableName();
        try(Statement statement = connection.createStatement()) {
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
        }
    }

    // 插入date数据
    public int insertDateValues() throws ClassNotFoundException, SQLException {
        String dateTableName = getDateTableName();
        try(Statement statement = connection.createStatement()) {
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
    }

    // 插入其他格式date类型数据并返回查询出的日期
    public String insertVariousFormatDateValues(String inputID, String inputDate) throws ClassNotFoundException, SQLException {
        String dateTableName = getDateTableName();
        try(Statement statement = connection.createStatement()) {
            String dateFormatInsertSql = "insert into " + dateTableName +
                    " values (" + inputID + ",'zhangsan',18,23.50,'beijing','" + inputDate +  "')";
            int insertRows = statement.executeUpdate(dateFormatInsertSql);
            String queryInsertDateSql = "select birthday from " + dateTableName + " where id = " + inputID;
            ResultSet findDateRst = statement.executeQuery(queryInsertDateSql);
            String findDateStr = null;
            while (findDateRst.next()) {
                findDateStr = findDateRst.getString("birthday");
            }
            statement.close();
            return findDateStr;
        }
    }

    //创建含有time字段类型的表格
    public void createTimeTable() throws Exception {
        String timeTableName = getTimeTableName();
        try(Statement statement = connection.createStatement()) {
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
        }
    }

    // 插入time数据
    public int insertTimeValues() throws ClassNotFoundException, SQLException {
        String timeTableName = getTimeTableName();
        try(Statement statement = connection.createStatement()) {
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
    }

    //查询time_format在表格time字段使用的结果
    public List<String> queryTime_FormatTimeInTable() throws SQLException {
        String queryTFTTable = getTimeTableName();
        try(Statement statement = connection.createStatement()) {
            String queryTFTSql = "select name,time_format(create_time, '%H:%i:%S') time_out from " + queryTFTTable + " where id<11";

            ResultSet queryTFTRst = statement.executeQuery(queryTFTSql);
            List<String> queryTFList = new ArrayList<String>();
            while (queryTFTRst.next()) {
                queryTFList.add(queryTFTRst.getString("time_out"));
            }
            statement.close();
            return queryTFList;
        }
    }

    // 插入其他格式time类型数据并返回查询出的时间
    public String insertVariousFormatTimeValues(String inputID, String inputTime) throws ClassNotFoundException, SQLException {
        String timeTableName = getTimeTableName();
        try(Statement statement = connection.createStatement()) {
            String timeFormatInsertSql = "insert into " + timeTableName +
                    " values (" + inputID + ",'zhangsan',35,23.50,'Bei Jing No.#12-03-01','" + inputTime +  "')";
            int insertRows = statement.executeUpdate(timeFormatInsertSql);
            String queryInsertTimeSql = "select create_time from " + timeTableName + " where id = " + inputID;
            ResultSet findTimeRst = statement.executeQuery(queryInsertTimeSql);
            String findTimeStr = null;
            while (findTimeRst.next()) {
                findTimeStr = findTimeRst.getTime("create_time").toString();
            }
            statement.close();
            return findTimeStr;
        }
    }

    //创建含有timestamp字段类型的表格
    public void createTimestampTable() throws Exception {
        String timestampTableName = getTimestampTableName();
        try(Statement statement = connection.createStatement()) {
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
        }
    }

    // 插入timestamp数据
    public int insertTimeStampValues() throws ClassNotFoundException, SQLException {
        String timestampTableName = getTimestampTableName();
        try(Statement statement = connection.createStatement()) {
            String batTimeStampInsertSql = "insert into " + timestampTableName +
                    " values (1, 'zhang san', 18, 1342.09,'BeiJing', '2022-4-8 18:05:07')";
            int insertRows = statement.executeUpdate(batTimeStampInsertSql);
            statement.close();
            return insertRows;
        }
    }


    // 获取函数Now()返回值
    public String nowFunc() throws SQLException {
        try(Statement statement = connection.createStatement()) {
            String nowSql = "select Now()";
            ResultSet nowRst = statement.executeQuery(nowSql);
            String nowString = null;
            while (nowRst.next()) {
                nowString = nowRst.getString(1);
            }

            statement.close();
            return nowString;
        }
    }

    // 获取函数CurDate()返回值
    public String curDateFunc() throws SQLException {
        try(Statement statement = connection.createStatement()) {
            String curDateSql = "select CurDate()";
            ResultSet curDateRst = statement.executeQuery(curDateSql);
            String curDateString = null;
            while (curDateRst.next()) {
                curDateString = curDateRst.getString(1);
            }
            statement.close();
            return curDateString;
        }
    }

    // 获取函数Current_Date返回值
    public String current_DateFunc() throws SQLException {
        try(Statement statement = connection.createStatement()) {
            String current_DateSql = "select Current_Date";
            ResultSet current_DateRst = statement.executeQuery(current_DateSql);
            String current_DateString = null;
            while (current_DateRst.next()) {
                current_DateString = current_DateRst.getString(1);
            }
            statement.close();
            return current_DateString;
        }
    }

    // 获取函数Current_Date()返回值
    public String current_DateWithBracketsFunc() throws SQLException {
        try(Statement statement = connection.createStatement()) {
            String current_DateWithBracketsSql = "select Current_Date()";
            ResultSet current_DateWithBracketsRst = statement.executeQuery(current_DateWithBracketsSql);
            String current_DateWithBracketString = null;
            while (current_DateWithBracketsRst.next()) {
                current_DateWithBracketString = current_DateWithBracketsRst.getString(1);
            }

            statement.close();
            return current_DateWithBracketString;
        }
    }

    // 获取函数CurTime()返回值
    public String curTimeFunc() throws SQLException {
        try(Statement statement = connection.createStatement()) {
            String curTimeSql = "select CurTime()";
            ResultSet curTimeRst = statement.executeQuery(curTimeSql);
            String curTimeString = null;
            while (curTimeRst.next()) {
                curTimeString = curTimeRst.getString(1);
            }

            statement.close();
            return curTimeString;
        }
    }

    // 获取函数current_Time返回值
    public String current_TimeFunc() throws SQLException {
        try(Statement statement = connection.createStatement()) {
            String current_TimeSql = "select Current_Time";
            ResultSet current_TimeRst = statement.executeQuery(current_TimeSql);
            String current_TimeString = null;
            while (current_TimeRst.next()) {
                current_TimeString = current_TimeRst.getString(1);
            }

            statement.close();
            return current_TimeString;
        }
    }

    // 获取函数current_Time()返回值
    public String current_TimeWithBracketsFunc() throws SQLException {
        try(Statement statement = connection.createStatement()) {
            String current_TimeWithBracketsSql = "select Current_Time()";
            ResultSet current_TimeWithBracketsRst = statement.executeQuery(current_TimeWithBracketsSql);
            String current_TimeWithBracketsString = null;
            while (current_TimeWithBracketsRst.next()) {
                current_TimeWithBracketsString = current_TimeWithBracketsRst.getString(1);
            }

            statement.close();
            return current_TimeWithBracketsString;
        }
    }

    // 获取函数Current_Timestamp返回值
    public String current_TimeStampFunc() throws SQLException {
        try(Statement statement = connection.createStatement()) {
            String current_TimeStampSql = "select Current_TimeStamp";
            ResultSet current_TimeStampRst = statement.executeQuery(current_TimeStampSql);
            String current_TimeStampString = null;
            while (current_TimeStampRst.next()) {
                current_TimeStampString = current_TimeStampRst.getString(1);
            }

            statement.close();
            return current_TimeStampString;
        }
    }

    // 获取函数Current_Timestamp()返回值
    public String current_TimeStampWithBracketsFunc() throws SQLException {
        try(Statement statement = connection.createStatement()) {
            String current_TimeStampWithBracketsSql = "select Current_TimeStamp()";
            ResultSet current_TimeStampWithBracketsRst = statement.executeQuery(current_TimeStampWithBracketsSql);
            String current_TimeStampWithBracketsString = null;
            while (current_TimeStampWithBracketsRst.next()) {
                current_TimeStampWithBracketsString = current_TimeStampWithBracketsRst.getString(1);
            }

            statement.close();
            return current_TimeStampWithBracketsString;
        }
    }

    // 获取函数From_UnixTime参数为timestamp返回值
    public String from_UnixTimeWithTimestampFunc(String inputTimestamp) throws SQLException {
        try(Statement statement = connection.createStatement()) {
            String from_UnixTimeSql = "select from_unixtime(" + inputTimestamp + ")";
            ResultSet from_UnixTimeRst = statement.executeQuery(from_UnixTimeSql);
            String from_UnixTimeString = null;
            while (from_UnixTimeRst.next()) {
                from_UnixTimeString = from_UnixTimeRst.getString(1);
            }

            statement.close();
            return from_UnixTimeString;
        }
    }

    // 获取函数From_UnixTime参数为数值字符串返回值
    public String from_UnixTimeWithStringFunc() throws SQLException {
        try(Statement statement = connection.createStatement()) {
            String from_UnixTimeWithStringSql = "select from_unixtime('1649770110')";
            ResultSet from_UnixTimeWithStringRst = statement.executeQuery(from_UnixTimeWithStringSql);
            String from_UnixTimeWithStringStr = null;
            while (from_UnixTimeWithStringRst.next()) {
                from_UnixTimeWithStringStr = from_UnixTimeWithStringRst.getString(1);
            }

            statement.close();
            return from_UnixTimeWithStringStr;
        }
    }

    // From_UnixTime参数为普通字符串返回错误
    public void from_UnixTimeNotNumStrFunc() throws SQLException {
        try(Statement statement = connection.createStatement()) {
            String from_UnixTimeNotNumStrSql = "select from_unixtime('1649abc')";
            statement.executeQuery(from_UnixTimeNotNumStrSql);
        }
    }

    // From_UnixTime参数为空返回错误
    public void from_UnixTimeNoArg() throws SQLException {
        try(Statement statement = connection.createStatement()) {
            String from_UnixTimeNoArgSql = "select from_unixtime()";
            statement.executeQuery(from_UnixTimeNoArgSql);
        }
    }

    // 获取函数Unix_TimeStamp返回值
    public String unix_TimeStampFunc(String inputStr) throws SQLException {
        try(Statement statement = connection.createStatement()) {
            String unix_TimeStampSql = "select unix_TimeStamp('" + inputStr + "')";
            ResultSet unixTimeStampRst = statement.executeQuery(unix_TimeStampSql);
            String unix_TimeStampStr  = null;
            while (unixTimeStampRst.next()) {
                unix_TimeStampStr = unixTimeStampRst.getString(1);
            }

            statement.close();
            return unix_TimeStampStr;
        }
    }

    // 获取函数Unix_TimeStamp参数为空时的返回值
    public String unix_TimeStampNoArgFunc() throws SQLException {
        try(Statement statement = connection.createStatement()) {
            String unix_TimeStampNoAgrSql = "select unix_TimeStamp()";
            ResultSet unixTimeStampNoArgRst = statement.executeQuery(unix_TimeStampNoAgrSql);
            String unix_TimeStampNoArgStr  = null;
            while (unixTimeStampNoArgRst.next()) {
                unix_TimeStampNoArgStr = unixTimeStampNoArgRst.getString(1);
            }

            statement.close();
            return unix_TimeStampNoArgStr;
        }
    }

    // 获取函数Unix_TimeStamp参数为数字时的返回值
    public String unix_TimeStampNumArg(String inputNum) throws SQLException {
        try(Statement statement = connection.createStatement()) {
            String unix_TimeStampNumArgSql = "select unix_TimeStamp(" + inputNum + ")";
            ResultSet unixTimeStampNumArgRst = statement.executeQuery(unix_TimeStampNumArgSql);
            String unix_TimeStampNumArgStr  = null;
            while (unixTimeStampNumArgRst.next()) {
                unix_TimeStampNumArgStr = unixTimeStampNumArgRst.getString(1);
            }

            statement.close();
            return unix_TimeStampNumArgStr;
        }
    }

    // 获取函数Unix_TimeStamp参数为日期函数时的返回值
    public String unix_TimeStampFuncArg(String argFunc) throws SQLException {
        try(Statement statement = connection.createStatement()) {
            String unix_TimeStampFuncAgrSql = "select unix_TimeStamp(" + argFunc + ")";
            ResultSet unixTimeStampFuncArgRst = statement.executeQuery(unix_TimeStampFuncAgrSql);
            String unix_TimeStampFuncArgStr  = null;
            while (unixTimeStampFuncArgRst.next()) {
                unix_TimeStampFuncArgStr = unixTimeStampFuncArgRst.getString(1);
            }

            statement.close();
            return unix_TimeStampFuncArgStr;
        }
    }

    // 获取函数Date_Format参数为字符串返回值
    public String date_FormatStrArgFunc(String inputDate, String inputFormat) throws SQLException {
        try(Statement statement = connection.createStatement()) {
            String date_formatSargSql = "select date_format('" + inputDate + "','" + inputFormat + "')";
            ResultSet date_formatSargRst = statement.executeQuery(date_formatSargSql);
            String date_formatSargStr  = null;
            while (date_formatSargRst.next()) {
                date_formatSargStr = date_formatSargRst.getString(1);
            }

            statement.close();
            return date_formatSargStr;
        }
    }

    // 获取函数Date_Format参数为数字返回值
    public String date_FormatNumArgFunc(String inputDate, String inputFormat) throws SQLException {
        try(Statement statement = connection.createStatement()) {
            String date_formatNargSql = "select date_format(" + inputDate + ",'" + inputFormat + "')";
            ResultSet date_formatNargRst = statement.executeQuery(date_formatNargSql);
            String date_formatNargStr  = null;
            while (date_formatNargRst.next()) {
                date_formatNargStr = date_formatNargRst.getString(1);
            }

            statement.close();
            return date_formatNargStr;
        }
    }

    // 获取函数Date_Format参数为时间日期函数的返回值
    public String date_FormatFuncArg(String inputFunc, String inputFormat) throws SQLException {
        try(Statement statement = connection.createStatement()) {
            String date_formatFuncArgSql = "select date_format(" + inputFunc + ",'" + inputFormat + "')";
            ResultSet date_formatFuncArgRst = statement.executeQuery(date_formatFuncArgSql);
            String date_formatFuncArgStr  = null;
            while (date_formatFuncArgRst.next()) {
                date_formatFuncArgStr = date_formatFuncArgRst.getString(1);
            }

            statement.close();
            return date_formatFuncArgStr;
        }
    }

    // 获取函数Date_Format参数为Null的返回值
    public String date_FormatNullArg() throws SQLException {
        try(Statement statement = connection.createStatement()) {
            String date_formatNullArgSql = "select date_format(Null,'%Y%m%d')";
            ResultSet date_formatNullArgRst = statement.executeQuery(date_formatNullArgSql);
            String date_formatNullArgStr  = null;
            while (date_formatNullArgRst.next()) {
                date_formatNullArgStr = date_formatNullArgRst.getString(1);
            }

            statement.close();
            return date_formatNullArgStr;
        }
    }

    // 获取函数Date_Format参数为空字符串的返回值
    public String date_FormatEmptyArg() throws SQLException {
        try(Statement statement = connection.createStatement()) {
            String date_formatEmptyArgSql = "select date_format('','%Y%m%d')";
            ResultSet date_formatEmptyArgRst = statement.executeQuery(date_formatEmptyArgSql);
            String date_formatEmptyArgStr  = null;
            while (date_formatEmptyArgRst.next()) {
                date_formatEmptyArgStr = date_formatEmptyArgRst.getString(1);
            }

            statement.close();
            return date_formatEmptyArgStr;
        }
    }

    // 函数Date_Format缺少格式参数，按标准日期格式输出
    public String date_FormatMissingFormatArg(String inputDate) throws SQLException {
        try(Statement statement = connection.createStatement()) {
            String date_formatSql = "select date_format('" + inputDate + "')";
            ResultSet date_formatRst = statement.executeQuery(date_formatSql);
            String date_formatStr  = null;
            while (date_formatRst.next()) {
                date_formatStr = date_formatRst.getString(1);
            }

            statement.close();
            return date_formatStr;
        }
    }

    // 获取函数Date_Format缺少日期参数，返回错误
    public void date_FormatMissingDateArg(String inputParam) throws SQLException {
        try(Statement statement = connection.createStatement()) {
            String date_formatMissingArgSql = "select date_format('" + inputParam + "')";
            statement.executeQuery(date_formatMissingArgSql);
        }
    }


    // 获取函数Time_Format参数为字符串返回值
    public String time_FormatStrArgFunc(String inputTime, String inputFormat) throws SQLException {
        try(Statement statement = connection.createStatement()) {
            String time_formatSargSql = "select time_format('" + inputTime + "','" + inputFormat + "')";
            ResultSet time_formatSargRst = statement.executeQuery(time_formatSargSql);
            String time_formatSargStr  = null;
            while (time_formatSargRst.next()) {
                time_formatSargStr = time_formatSargRst.getString(1);
            }

            statement.close();
            return time_formatSargStr;
        }
    }

    // 获取函数Time_Format参数为数字返回值
    public String time_FormatNumArgFunc(String inputTime, String inputFormat) throws SQLException {
        try(Statement statement = connection.createStatement()) {
            String time_formatNargSql = "select time_format(" + inputTime + ",'" + inputFormat + "')";
            ResultSet time_formatNargRst = statement.executeQuery(time_formatNargSql);
            String time_formatNargStr  = null;
            while (time_formatNargRst.next()) {
                time_formatNargStr = time_formatNargRst.getString(1);
            }

            statement.close();
            return time_formatNargStr;
        }
    }

    // 获取函数Time_Format参数为时间日期函数的返回值
    public String time_FormatFuncArg(String inputFunc, String inputFormat) throws SQLException {
        try(Statement statement = connection.createStatement()) {
            String time_formatFuncArgSql = "select time_format(" + inputFunc + ",'" + inputFormat + "')";
            ResultSet time_formatFuncArgRst = statement.executeQuery(time_formatFuncArgSql);
            String time_formatFuncArgStr  = null;
            while (time_formatFuncArgRst.next()) {
                time_formatFuncArgStr = time_formatFuncArgRst.getString(1);
            }

            statement.close();
            return time_formatFuncArgStr;
        }
    }

    // 2022-07-22: 需求变更后，函数Time_Format缺少格式参数，按标准时间格式输出
    public String time_FormatMissingFormatArg(String inputTime) throws SQLException {
        try(Statement statement = connection.createStatement()) {
            String time_formatSql = "select time_format('" + inputTime + "')";
            ResultSet time_formatRst = statement.executeQuery(time_formatSql);
            String time_formatStr  = null;
            while (time_formatRst.next()) {
                time_formatStr = time_formatRst.getString(1);
            }

            statement.close();
            return time_formatStr;
        }
    }

    // 获取函数Time_Format缺少参数，返回错误
    public void time_FormatMissingArg(String inputParam) throws SQLException {
        try(Statement statement = connection.createStatement()) {
            String time_formatMissingArgSql = "select time_format('" + inputParam + "')";
            statement.executeQuery(time_formatMissingArgSql);
        }
    }

    // 获取函数Time_Format参数为Null的返回值
    public String time_FormatNullArg() throws SQLException {
        try(Statement statement = connection.createStatement()) {
            String time_formatNullArgSql = "select time_format(Null,'%H:%i:%S')";
            ResultSet time_formatNullArgRst = statement.executeQuery(time_formatNullArgSql);
            String time_formatNullArgStr  = null;
            while (time_formatNullArgRst.next()) {
                time_formatNullArgStr = time_formatNullArgRst.getString(1);
            }

            statement.close();
            return time_formatNullArgStr;
        }
    }

    // 获取函数Time_Format参数为空字符串的返回值
    public String time_FormatEmptyArg() throws SQLException {
        try(Statement statement = connection.createStatement()) {
            String time_formatEmptyArgSql = "select time_format('','%H:%i:%S')";
            ResultSet time_formatEmptyArgRst = statement.executeQuery(time_formatEmptyArgSql);
            String time_formatEmptyArgStr  = null;
            while (time_formatEmptyArgRst.next()) {
                time_formatEmptyArgStr = time_formatEmptyArgRst.getString(1);
            }

            statement.close();
            return time_formatEmptyArgStr;
        }
    }

    // 获取函数DateDiff参数为字符串返回值
    public String dateDiffStrArgFunc(String Date1, String Date2) throws SQLException {
        try(Statement statement = connection.createStatement()) {
            String dateDiffSargSql = "select datediff('" + Date1 + "','" + Date2 + "') as diffDate";
            ResultSet dateDiffSargRst = statement.executeQuery(dateDiffSargSql);
            String dateDiffSargStr  = null;
            while (dateDiffSargRst.next()) {
                dateDiffSargStr = dateDiffSargRst.getString(1);
            }

            statement.close();
            return dateDiffSargStr;
        }
    }

    // 获取函数DateDiff参数为数字返回值
    public String dateDiffNumArgFunc(String Date1, String Date2) throws SQLException {
        try(Statement statement = connection.createStatement()) {
            String dateDiffNargSql = "select datediff(" + Date1 + "," + Date2 + ") as diffDate";
            ResultSet dateDiffNargRst = statement.executeQuery(dateDiffNargSql);
            String dateDiffNargStr  = null;
            while (dateDiffNargRst.next()) {
                dateDiffNargStr = dateDiffNargRst.getString(1);
            }

            statement.close();
            return dateDiffNargStr;
        }
    }

    // 获取函数DateDiff第一个参数为时间日期函数返回值
    public String dateDiffFuncArg1Func(String funcArg, String Date2) throws SQLException {
        try(Statement statement = connection.createStatement()) {
            String dateDiffFarg1Sql = "select datediff(" + funcArg + ",'" + Date2 + "') as diffDate";
            ResultSet dateDiffFarg1Rst = statement.executeQuery(dateDiffFarg1Sql);
            String dateDiffFarg1Str  = null;
            while (dateDiffFarg1Rst.next()) {
                dateDiffFarg1Str = dateDiffFarg1Rst.getString(1);
            }

            statement.close();
            return dateDiffFarg1Str;
        }
    }
    // 获取函数DateDiff第二个参数为时间日期函数返回值
    public String dateDiffFuncArg2Func(String funcArg, String Date2) throws SQLException {
        try(Statement statement = connection.createStatement()) {
            String dateDiffFarg2Sql = "select datediff('" + Date2 + "'," + funcArg + ") as diffDate";
            ResultSet dateDiffFarg2Rst = statement.executeQuery(dateDiffFarg2Sql);
            String dateDiffFarg2Str  = null;
            while(dateDiffFarg2Rst.next()) {
                dateDiffFarg2Str = dateDiffFarg2Rst.getString(1);
            }

            statement.close();
            return dateDiffFarg2Str;
        }
    }

    // 函数DateDiff传入完整查询语句
    public String dateDiffStateArg(String datediffState) throws SQLException {
        try(Statement statement = connection.createStatement()) {
            String dateDiffWrongArgSql = datediffState;
            ResultSet dateDiffStateRst = statement.executeQuery(dateDiffWrongArgSql);
            String dateDiffStateStr = null;
            while(dateDiffStateRst.next()) {
                dateDiffStateStr = dateDiffStateRst.getString(1);
            }
            statement.close();
            return dateDiffStateStr;
        }
    }

    // 获取字符串上下文返回格式
    public String funcConcatStr(String inputFunc) throws SQLException {
        try(Statement statement = connection.createStatement()) {
            String funcConcatSql = "select 'test_'||" + inputFunc;
            ResultSet funcConcatRst = statement.executeQuery(funcConcatSql);
            String funcConcatStr  = null;
            while (funcConcatRst.next()) {
                funcConcatStr = funcConcatRst.getString(1);
            }

            statement.close();
            return funcConcatStr;
        }
    }

    // 插入非法date类型数据，不允许插入，返回错误信息
    public void insertNegativeDate(String inputDate) throws ClassNotFoundException, SQLException {
        String dateTableName = getDateTableName();
        try(Statement statement = connection.createStatement()) {
            String dateInsertSql = "insert into " + dateTableName +
                    " values " + inputDate;
            statement.executeUpdate(dateInsertSql);
        }
    }

    // 插入非法time类型数据，不允许插入，返回错误信息
    public void insertNegativeTime(String inputTime) throws ClassNotFoundException, SQLException {
        String timeTableName = getTimeTableName();
        try(Statement statement = connection.createStatement()) {
            String timeInsertSql = "insert into " + timeTableName +
                    " values " + inputTime;
            statement.executeUpdate(timeInsertSql);
        }
    }

    // 插入非法timestamp类型数据，不允许插入，返回错误信息
    public void insertNegativeTimeStamp(String inputTimeStamp) throws ClassNotFoundException, SQLException {
        String timestampTableName = getTimestampTableName();
        try(Statement statement = connection.createStatement()) {
            String timestampInsertSql = "insert into " + timestampTableName +
                    " values " + inputTimeStamp;
            statement.executeUpdate(timestampInsertSql);
        }
    }

    //创建含有date,time和timestamp字段类型的表格,用于测试update语句
    public void createUpdateTable() throws Exception {
        try(Statement statement = connection.createStatement()) {
            String sql = "create table uptesttable" + "("
                    + "id int,"
                    + "name varchar(32) not null,"
                    + "age int,"
                    + "amount double,"
                    + "address varchar(255),"
                    + "birthday date,"
                    + "createTime time,"
                    + "update_Time timestamp,"
                    + "is_delete boolean,"
                    + "primary key(id)"
                    + ")";
            statement.execute(sql);
        }
    }

    // 向Update表插入DateTime数据
    public void insertValueToUpdateTestTable() throws ClassNotFoundException, SQLException {
        try(Statement statement = connection.createStatement()) {
            String insertSql = "insert into uptesttable " +
                    "values \n" +
                    "(1,'zhangsan',18,23.50,'beijing','1998-4-6', '08:10:10', '2022-4-8 18:05:07', true),\n" +
                    "(2,'lisi',25,895,' beijing haidian ', '1988-2-05', '06:15:8', '2000-02-29 00:00:00', true),\n" +
                    "(3,'l3',55,123.123,'wuhan NO.1 Street', '2022-03-4', '07:3:15', '1999-2-28 23:59:59', false),\n" +
                    "(4,'HAHA',57,9.0762556,'CHANGping', '2020-11-11', '5:59:59', '2021-05-04 12:00:00', false),\n" +
                    "(5,'awJDs',1,1453.9999,'pingYang1', '2010-10-1', '19:0:0', '2010-10-1 02:02:02', true),\n" +
                    "(6,'123',544,0,'543', '1987-7-16', '1:2:3', '1952-12-31 12:12:12', false),\n" +
                    "(7,'yamaha',76,2.30,'beijing changyang', '1949-01-01', '0:30:8', '2022-12-01 1:2:3', false)";
            statement.executeUpdate(insertSql);
        }
    }

    // 更新单行多字段
    public List<List> updateSingleRow() throws Exception {
        createUpdateTable();
        insertValueToUpdateTestTable();
        try(Statement statement = connection.createStatement()) {
            String updateSql1 = "update uptesttable set name='new',age=33,amount=1111.111,address='new Addr'," +
                    "birthday='2022-05-19',createTime='18:33:00',update_time='1949-07-01 23:59:59',is_delete=true where id=7";
            statement.executeUpdate(updateSql1);
            String queryUpdateSql1 = "select * from uptesttable where id=7";
            ResultSet queryUpdateRst = statement.executeQuery(queryUpdateSql1);
            List<List> queryUpdateList1 = new ArrayList<List>();

            while(queryUpdateRst.next()) {
                List rowList = new ArrayList ();
                rowList.add(queryUpdateRst.getString(1));
                rowList.add(queryUpdateRst.getString(2));
                rowList.add(queryUpdateRst.getString(3));
                rowList.add(queryUpdateRst.getString(4));
                rowList.add(queryUpdateRst.getString(5));
                rowList.add(queryUpdateRst.getString(6));
                rowList.add(queryUpdateRst.getTime(7).toString());
                rowList.add(queryUpdateRst.getString(8));
                rowList.add(queryUpdateRst.getString(9));

                queryUpdateList1.add(rowList);
            }
            statement.close();
            return queryUpdateList1;
        }
    }

    // 更新全部多字段
    public List<List> updateAllRow() throws Exception {
        try(Statement statement = connection.createStatement()) {
            String updateSql2 = "update uptesttable set age=100,amount=22.22,address='BJ'," +
                    "update_time='2022-10-01 10:08:06',is_delete=false";
            statement.executeUpdate(updateSql2);
            String queryUpdateSql2 = "select * from uptesttable";
            ResultSet queryUpdateRst = statement.executeQuery(queryUpdateSql2);
            List<List> queryUpdateList2 = new ArrayList<List>();

            while(queryUpdateRst.next()) {
                List rowList = new ArrayList ();
                rowList.add(queryUpdateRst.getString(1));
                rowList.add(queryUpdateRst.getString(2));
                rowList.add(queryUpdateRst.getString(3));
                rowList.add(queryUpdateRst.getString(4));
                rowList.add(queryUpdateRst.getString(5));
                rowList.add(queryUpdateRst.getString(6));
                rowList.add(queryUpdateRst.getTime(7).toString());
                rowList.add(queryUpdateRst.getString(8));
                rowList.add(queryUpdateRst.getString(9));

                queryUpdateList2.add(rowList);
            }
            statement.close();
            return queryUpdateList2;
        }
    }

    //创建表并插入空date
    public List insertBlankDate() throws SQLException {
        try(Statement statement = connection.createStatement()) {
            String createSQL = "create table case1434 (id int, birthday date, primary key(id))";
            statement.execute(createSQL);
            String insertSQL = "insert into case1434 values(1,'')";
            statement.execute(insertSQL);
            String querySQl = "select * from case1434 where id=1";
            ResultSet resultSet = statement.executeQuery(querySQl);
            List queryList = new ArrayList();
            while(resultSet.next()) {
                queryList.add(resultSet.getInt(1));
                queryList.add(resultSet.getDate(2));
            }
            statement.close();
            return queryList;
        }
    }

    //创建表并插入空time
    public List insertBlankTime() throws SQLException {
        try(Statement statement = connection.createStatement()) {
            String createSQL = "create table case1435 (id int, create_time time, primary key(id))";
            statement.execute(createSQL);
            String insertSQL = "insert into case1435 values(1,'')";
            statement.execute(insertSQL);
            String querySQl = "select * from case1435 where id=1";
            ResultSet resultSet = statement.executeQuery(querySQl);
            List queryList = new ArrayList();
            while(resultSet.next()) {
                queryList.add(resultSet.getInt(1));
                queryList.add(resultSet.getTime(2));
            }
            statement.close();
            return queryList;
        }
    }

    //创建表并插入空timestamp
    public List insertBlankTimestamp() throws SQLException {
        try(Statement statement = connection.createStatement()) {
            String createSQL = "create table case1436 (id int, create_time timestamp, primary key(id))";
            statement.execute(createSQL);
            String insertSQL = "insert into case1436 values(1,'')";
            statement.execute(insertSQL);

            String querySQl = "select * from case1434 where id=1";
            ResultSet resultSet = statement.executeQuery(querySQl);
            List queryList = new ArrayList();
            while(resultSet.next()) {
                queryList.add(resultSet.getInt(1));
                queryList.add(resultSet.getTimestamp(2));
            }
            statement.close();
            return queryList;
        }
    }

    // 获取函数timestamp_Format参数为时间日期函数的返回值
    public String timestamp_FormatFuncArg(String inputFunc, String inputFormat) throws SQLException {
        try(Statement statement = connection.createStatement()) {
            String timestamp_formatFuncArgSql = "select timestamp_format(" + inputFunc + ",'" + inputFormat + "')";
            ResultSet timestamp_formatFuncArgRst = statement.executeQuery(timestamp_formatFuncArgSql);
            String timestamp_formatFuncArgStr  = null;
            while (timestamp_formatFuncArgRst.next()) {
                timestamp_formatFuncArgStr = timestamp_formatFuncArgRst.getString(1);
            }

            statement.close();
            return timestamp_formatFuncArgStr;
        }
    }

    // 获取函数Timestamp_Format参数为字符串返回值
    public String timestamp_FormatStrArgFunc(String inputDate, String inputFormat) throws SQLException {
        try(Statement statement = connection.createStatement()) {
            String timestamp_formatSargSql = "select timestamp_format('" + inputDate + "','" + inputFormat + "')";
            ResultSet timestamp_formatSargRst = statement.executeQuery(timestamp_formatSargSql);
            String timestamp_formatSargStr  = null;
            while (timestamp_formatSargRst.next()) {
                timestamp_formatSargStr = timestamp_formatSargRst.getString(1);
            }

            statement.close();
            return timestamp_formatSargStr;
        }
    }

    // 获取函数Timestamp_Format参数为数字返回值
    public String timestamp_FormatNumArgFunc(String inputTimestamp, String inputFormat) throws SQLException {
        try(Statement statement = connection.createStatement()) {
            String timestamp_formatNargSql = "select timestamp_format(" + inputTimestamp + ",'" + inputFormat + "')";
            ResultSet timestamp_formatNargRst = statement.executeQuery(timestamp_formatNargSql);
            String timestamp_formatNargStr  = null;
            while (timestamp_formatNargRst.next()) {
                timestamp_formatNargStr = timestamp_formatNargRst.getString(1);
            }

            statement.close();
            return timestamp_formatNargStr;
        }
    }

    //查询timestamp_format在表格timestamp字段使用的结果
    public List<String> queryTimestamp_FormatTimestampInTable() throws SQLException {
        String queryTable = getDateTimeTableName();
        try(Statement statement = connection.createStatement()) {
            String querySql = "select name,timestamp_format(update_Time, '%Y/%m/%d %H.%i.%s') ts_out from " + queryTable + " where id<8";

            ResultSet resultSet = statement.executeQuery(querySql);
            List<String> queryList = new ArrayList<String>();
            while (resultSet.next()) {
                queryList.add(resultSet.getString("ts_out"));
            }
            statement.close();
            return queryList;
        }
    }

    // timestamp_format的第一个参数为空字符串时，预期返回null
    public String timestamp_FormatEmptyArg() throws SQLException {
        try(Statement statement = connection.createStatement()) {
            String timestamp_formatEmptyArgSql = "select timestamp_format('','%Y%m%d')";
            ResultSet timestamp_formatEmptyArgRst = statement.executeQuery(timestamp_formatEmptyArgSql);
            String timestamp_formatEmptyArgStr  = null;
            while (timestamp_formatEmptyArgRst.next()) {
                timestamp_formatEmptyArgStr = timestamp_formatEmptyArgRst.getString(1);
            }

            statement.close();
            return timestamp_formatEmptyArgStr;
        }
    }

    // 函数timestamp_Format缺少格式参数，按标准timestamp格式输出
    public String timestamp_FormatMissingFormatArg(String inputTimestamp) throws SQLException {
        try(Statement statement = connection.createStatement()) {
            String timestamp_formatSql = "select timestamp_format('" + inputTimestamp + "')";
            ResultSet timestamp_formatRst = statement.executeQuery(timestamp_formatSql);
            String timestamp_formatStr  = null;
            while (timestamp_formatRst.next()) {
                timestamp_formatStr = timestamp_formatRst.getString(1);
            }

            statement.close();
            return timestamp_formatStr;
        }
    }

    // 获取函数timestamp_Format参数为Null的返回值
    public String timestamp_FormatNullArg() throws SQLException {
        try(Statement statement = connection.createStatement()) {
            String timestamp_formatNullArgSql = "select timestamp_format(Null,'%Y-%m-%d %H:%i:%S')";
            ResultSet timestamp_formatNullArgRst = statement.executeQuery(timestamp_formatNullArgSql);
            String timestamp_formatNullArgStr  = null;
            while (timestamp_formatNullArgRst.next()) {
                timestamp_formatNullArgStr = timestamp_formatNullArgRst.getString(1);
            }

            statement.close();
            return timestamp_formatNullArgStr;
        }
    }
}
