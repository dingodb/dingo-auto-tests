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
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;
import utils.FileReaderUtil;

import java.sql.Connection;
import java.sql.Date;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Time;
import java.sql.Timestamp;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.util.ArrayList;
import java.util.List;

public class TestPreparedStatement {
    private static String tableName = "pstest_tbl";
    private static Connection connection = null;

    static {
        try {
            connection = JDBCUtils.getConnection();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public void createPSTable() throws SQLException {
        String ps_meta1_path = "src/test/resources/testdata/tablemeta/between_and_meta1.txt";
        String ps_value1_path = "src/test/resources/testdata/tableInsertValues/between_and_value1.txt";
        String ps_meta1 = FileReaderUtil.readFile(ps_meta1_path);
        String ps_value1 = FileReaderUtil.readFile(ps_value1_path);

        try(Statement statement = connection.createStatement()) {
            String createSql = "create table " + tableName + ps_meta1;
            statement.execute(createSql);
            String insertSql = "insert into " + tableName + " values " + ps_value1;
            statement.execute(insertSql);
        }
    }

    public static List<List> expectedTest01List() {
        String[][] dataArray = {{"1","zhangsan","18","23.5","beijing","1998-04-06", "08:10:10", "2022-04-08 18:05:07", "true"}};
        List<List> psList = new ArrayList<List>();
        for(int i=0; i<dataArray.length; i++) {
            List columnList = new ArrayList();
            for (int j=0; j<dataArray[i].length; j++) {
                columnList.add(dataArray[i][j]);
            }
            psList.add(columnList);
        }
        return psList;
    }

    public static List<List> expectedTest02List() {
        String[][] dataArray = {{"1","zhangsan"},{"2","lisi"},{"3","li3"},{"4","HAHA"},{"5","awJDs"}};
        List<List> psList = new ArrayList<List>();
        for(int i=0; i<dataArray.length; i++) {
            List columnList = new ArrayList();
            for (int j=0; j<dataArray[i].length; j++) {
                columnList.add(dataArray[i][j]);
            }
            psList.add(columnList);
        }
        return psList;
    }

    public static List<List> expectedTest03List() {
        String[][] dataArray = {
                {"23.5", "1", "zhangsan", "18", "1998-04-06", "true"},
                {"20010.0", "10", "3M", "31", "2021-03-04", "false"},
                {"4201.98", "11", "GiGi", "98", "1976-07-07", "false"},
                {"87231.0", "12", "Kelay", "10", "2018-05-31", "true"},
                {"2000.01", "21", "Zala", "76", "2022-07-07", "false"}
        };
        List<List> psList = new ArrayList<List>();
        for(int i=0; i<dataArray.length; i++) {
            List columnList = new ArrayList();
            for (int j=0; j<dataArray[i].length; j++) {
                columnList.add(dataArray[i][j]);
            }
            psList.add(columnList);
        }
        return psList;
    }

    public static List<List> expectedTest04List() {
        String[][] dataArray = {
                {"1", "beijing"},
                {"7", "beijing changyang"},
                {"8", "beijing"},
                {"9", " beijing haidian "},
                {"14", "beijing changyang"},
                {"15", "beijing"}
        };
        List<List> psList = new ArrayList<List>();
        for(int i=0; i<dataArray.length; i++) {
            List columnList = new ArrayList();
            for (int j=0; j<dataArray[i].length; j++) {
                columnList.add(dataArray[i][j]);
            }
            psList.add(columnList);
        }
        return psList;
    }

    public static List<List> expectedTest05List() {
        String[][] dataArray = {
                {"true", "2010-10-01", "19:00:00", "2010-10-01 02:02:02"},
                {"true", "2018-05-31", "21:00:00", "2000-01-01 00:00:00"},
                {"false", "2014-10-13", "01:00:00", "1999-12-31 23:59:59"},
                {"false", "2010-10-01", "19:00:00", "2010-10-01 02:02:02"}
        };
        List<List> psList = new ArrayList<List>();
        for(int i=0; i<dataArray.length; i++) {
            List columnList = new ArrayList();
            for (int j=0; j<dataArray[i].length; j++) {
                columnList.add(dataArray[i][j]);
            }
            psList.add(columnList);
        }
        return psList;
    }

    public static List<List> expectedTest06List() {
        String[][] dataArray = {
                {"1"},{"4"},{"5"},{"6"},{"8"},{"9"},
                {"12"},{"14"},{"15"},{"17"},{"20"}
        };
        List<List> psList = new ArrayList<List>();
        for(int i=0; i<dataArray.length; i++) {
            List columnList = new ArrayList();
            for (int j=0; j<dataArray[i].length; j++) {
                columnList.add(dataArray[i][j]);
            }
            psList.add(columnList);
        }
        return psList;
    }

    @BeforeClass(alwaysRun = true, description = "测试开始前，连接数据库")
    public static void setupAll() {
        Assert.assertNotNull(connection);
    }

    @Test(priority = 0, enabled = true, description = "创建测试表")
    public void test00CreatePSTable() throws SQLException {
        createPSTable();
    }

    @Test(priority = 1, enabled = true, description = "使用preparedStatement等值查询")
    public void test01PSEqualQuery() throws SQLException {
        String querysql = "select * from " + tableName + " where id = ?";
        List<List> queryList = new ArrayList<List>();
        try (PreparedStatement ps = connection.prepareStatement(querysql)) {
            ps.setInt(1, 1);
            ResultSet resultSet = ps.executeQuery();
            while (resultSet.next()) {
                List rowList = new ArrayList();
                rowList.add(resultSet.getString(1));
                rowList.add(resultSet.getString(2));
                rowList.add(resultSet.getString(3));
                rowList.add(resultSet.getString(4));
                rowList.add(resultSet.getString(5));
                rowList.add(resultSet.getString(6));
                rowList.add(resultSet.getString(7));
                rowList.add(resultSet.getString(8));
                rowList.add(resultSet.getString(9));
                queryList.add(rowList);
            }
            resultSet.close();
        }

        List<List> expectedList = expectedTest01List();
        System.out.println("Expected: " + expectedList);
        System.out.println("Actual：" + queryList);
        Assert.assertEquals(queryList, expectedList);
    }

    @Test(priority = 2, enabled = true, description = "使用preparedStatement范围查询")
    public void test02PSRangeQuery() throws SQLException {
        String querysql = "select id,name from " + tableName + " where id between ? and ?";
        List<List> queryList = new ArrayList<List>();
        try (PreparedStatement ps = connection.prepareStatement(querysql)) {
            ps.setInt(1,1);
            ps.setInt(2,5);
            ResultSet resultSet = ps.executeQuery();
            while (resultSet.next()) {
                List rowList = new ArrayList();
                rowList.add(resultSet.getString(1));
                rowList.add(resultSet.getString(2));
                queryList.add(rowList);
            }
            resultSet.close();
        }

        List<List> expectedList = expectedTest02List();
        System.out.println("Expected: " + expectedList);
        System.out.println("Actual：" + queryList);
        Assert.assertEquals(queryList, expectedList);
    }

    @Test(priority = 3, enabled = true, description = "使用preparedStatement逻辑运算符查询")
    public void test03PSLogicQuery() throws SQLException {
        String querysql = "select amount,id,name,age,birthday,is_delete from " + tableName +
                " where name = ? or amount > ?";
        List<List> queryList = new ArrayList<List>();
        try (PreparedStatement ps = connection.prepareStatement(querysql)) {
            ps.setString(1, "zhangsan");
            ps.setDouble(2, 2000.0);
            ResultSet resultSet = ps.executeQuery();
            while (resultSet.next()) {
                List rowList = new ArrayList();
                rowList.add(resultSet.getString(1));
                rowList.add(resultSet.getString(2));
                rowList.add(resultSet.getString(3));
                rowList.add(resultSet.getString(4));
                rowList.add(resultSet.getString(5));
                rowList.add(resultSet.getString(6));
                queryList.add(rowList);
            }
            resultSet.close();
        }

        List<List> expectedList = expectedTest03List();
        System.out.println("Expected: " + expectedList);
        System.out.println("Actual：" + queryList);
        Assert.assertTrue(queryList.containsAll(expectedList));
        Assert.assertTrue(expectedList.containsAll(queryList));
    }

    @Test(priority = 4, enabled = true, description = "使用preparedStatement在函数中使用变量")
    public void test04PSQueryInFunc() throws SQLException {
        String querysql = "select id,address from " + tableName +
                " where locate(?, address)<>0";
        List<List> queryList = new ArrayList<List>();
        try (PreparedStatement ps = connection.prepareStatement(querysql)) {
            ps.setString(1, "beijing");
            ResultSet resultSet = ps.executeQuery();
            while (resultSet.next()) {
                List rowList = new ArrayList();
                rowList.add(resultSet.getString(1));
                rowList.add(resultSet.getString(2));
                queryList.add(rowList);
            }
            resultSet.close();
        }

        List<List> expectedList = expectedTest04List();
        System.out.println("Expected: " + expectedList);
        System.out.println("Actual：" + queryList);
        Assert.assertTrue(queryList.containsAll(expectedList));
        Assert.assertTrue(expectedList.containsAll(queryList));
    }

    @Test(priority = 5, enabled = true, description = "使用preparedStatement对时间日期值使用变量")
    public void test05PSQueryForDateTime() throws SQLException {
        String querysql = "select is_delete,birthday,create_time,update_time from " + tableName +
                " where birthday>? and create_time in(?, ?, ?) and update_time < ?";
        List<List> queryList = new ArrayList<List>();
        try (PreparedStatement ps = connection.prepareStatement(querysql)) {
            ps.setString(1, "2005-01-01");
            ps.setString(2, "01:00:00");
            ps.setString(3, "19:00:00");
            ps.setString(4, "21:00:00");
            ps.setString(5,"2010-10-01 00:00:00");
            ResultSet resultSet = ps.executeQuery();
            while (resultSet.next()) {
                List rowList = new ArrayList();
                rowList.add(resultSet.getString(1));
                rowList.add(resultSet.getString(2));
                rowList.add(resultSet.getString(3));
                rowList.add(resultSet.getString(4));
                queryList.add(rowList);
            }
            resultSet.close();
        }

        List<List> expectedList = expectedTest05List();
        System.out.println("Expected: " + expectedList);
        System.out.println("Actual：" + queryList);
        Assert.assertTrue(queryList.containsAll(expectedList));
        Assert.assertTrue(expectedList.containsAll(queryList));
    }

    @Test(priority = 6, enabled = true, description = "使用preparedStatement对布尔字段值使用变量")
    public void test06PSQueryForBool() throws SQLException {
        String querysql = "select id from " + tableName + " where is_delete = ?";
        List<List> queryList = new ArrayList<List>();
        try (PreparedStatement ps = connection.prepareStatement(querysql)) {
            ps.setBoolean(1, true);
            ResultSet resultSet = ps.executeQuery();
            while (resultSet.next()) {
                List rowList = new ArrayList();
                rowList.add(resultSet.getString(1));
                queryList.add(rowList);
            }
            resultSet.close();
        }

        List<List> expectedList = expectedTest06List();
        System.out.println("Expected: " + expectedList);
        System.out.println("Actual：" + queryList);
        Assert.assertTrue(queryList.containsAll(expectedList));
        Assert.assertTrue(expectedList.containsAll(queryList));
    }

    @Test(priority = 7, enabled = true, description = "更新语句中使用preparedStatement")
    public void test07PSInUpdate() throws SQLException {
        String updatesql = "update " + tableName + " set address = ? where address = ?";
        int affectRows = 0;
        try (PreparedStatement ps = connection.prepareStatement(updatesql)) {
            ps.setString(1, "Dingo");
            ps.setString(2, "beijing");
            affectRows = ps.executeUpdate();
        }
        Assert.assertEquals(affectRows, 3);
    }

    @Test(priority = 8, enabled = true, description = "删除语句中使用preparedStatement")
    public void test08PSInDelete() throws SQLException {
        String deletesql = "delete from " + tableName + " where name<>? and age<? and is_delete=?";
        int affectRows = 0;
        try (PreparedStatement ps = connection.prepareStatement(deletesql)) {
            ps.setString(1, "zhangsan");
            ps.setInt(2, 20);
            ps.setBoolean(3, true);
            affectRows = ps.executeUpdate();
        }
        Assert.assertEquals(affectRows, 4);
    }

    @Test(priority = 9, enabled = true, description = "插入语句中使用preparedStatement")
    public void test09PSInInsert() throws SQLException {
        String insertsql = "insert into " + tableName + " values(?,?,?,?,?,?,?,?,?)";
        int affectRows = 0;
        try (PreparedStatement ps = connection.prepareStatement(insertsql)) {
            for(int i = 100; i <= 110; i++) {
                ps.setInt(1, i);
                ps.setString(2, "Java");
                ps.setInt(3, 33);
                ps.setDouble(4, 2345.6789);
                ps.setString(5, "Beijing Zhongguancun");

                LocalDate localDate = LocalDate.of(2022, 5, 1);
                ps.setDate(6, Date.valueOf(localDate));

                LocalTime localTime = LocalTime.of(11, 46, 25);
                ps.setTime(7, Time.valueOf(localTime));

                LocalDateTime localDateTime = LocalDateTime.of(2022, 10, 1, 14, 15, 10);
                ps.setTimestamp(8, Timestamp.valueOf(localDateTime));
                ps.setBoolean(9, true);
                ps.execute();
                System.out.println("插入第" + i + "条完成。");
            }
        }
    }

    @AfterClass(alwaysRun = true, description = "测试完成后删除数据和表格并关闭连接")
    public void tearDownAll() throws SQLException, ClassNotFoundException {
        Statement tearDownStatement = null;
        List<String> tableList = JDBCUtils.getTableList();
        try {
            tearDownStatement = connection.createStatement();
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
        } finally {
            JDBCUtils.closeResource(connection, tearDownStatement);
        }
    }

}
