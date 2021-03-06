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

import io.dingodb.dailytest.CommonArgs;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;
import utils.GetRandomValue;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

public class TestSplit {
//    private static final String defaultConnectIP = "172.20.3.27";
//    private static final String defaultConnectIP = "172.20.61.1";
    private static String defaultConnectIP = CommonArgs.getDefaultDingoClusterIP();
    private static final String JDBC_DRIVER = "io.dingodb.driver.client.DingoDriverClient";
    private static final String connectUrl = "jdbc:dingo:thin:url=" + defaultConnectIP + ":8765";
    public static Connection connection = null;
    private static String tableName = "jdbctest20";

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

    public void createSplitTable() throws SQLException {
        try(Statement statement = connection.createStatement()) {
            String sql = "create table " + tableName + "("
                    + "id int,"
                    + "name varchar(32) not null,"
                    + "age int,"
                    + "amount double,"
                    + "primary key(id)"
                    + ")";
            statement.execute(sql);
        }
    }

    @BeforeClass(alwaysRun = true, description = "?????????????????????????????????")
    public static void setupAll() {
        Assert.assertNotNull(connection);
    }

    @Test(priority = 0, enabled = true, description = "????????????")
    public void test00CreateSplitTable() throws SQLException {
        TestSplit testSplit = new TestSplit();
        testSplit.createSplitTable();
    }

    @Test(priority = 1, enabled = true, dependsOnMethods = {"test00CreateSplitTable"}, description = "??????????????????????????????")
    public void test01InsertValues() throws SQLException {
        int insertNum = 200000;
        try(Statement statement = connection.createStatement()) {
            for(int i=1; i<=insertNum; i++) {
                String nameStr = GetRandomValue.getRandStr(6);
                int ageNum = GetRandomValue.getRandInt(100);
//                double amountNum = Double.parseDouble(GetRandomValue.formateRate(String.valueOf(GetRandomValue.getRandDouble(0, 10000)),2));
                double amountNum = GetRandomValue.getRandDouble(0, 10000);
                String insertSql = "insert into " + tableName + " values (" + i + ",'" +
                        nameStr + "'," + ageNum + "," + amountNum + ")";
                statement.executeUpdate(insertSql);
            }
        }
    }

    @Test(priority = 2, enabled = true, dependsOnMethods = {"test01InsertValues"}, description = "?????????????????????????????????")
    public void test02CountAll() throws SQLException, InterruptedException {
        Thread.sleep(1200000);
        try(Statement statement = connection.createStatement()) {
            String querySql = "select count(*) from " + tableName;
            ResultSet resultSet = statement.executeQuery(querySql);
            int countNum = 0;
            while (resultSet.next()) {
                countNum = resultSet.getInt(1);
            }
            statement.close();
            System.out.println("count????????????????????? " + countNum);
            Assert.assertEquals(countNum, 200000);
        }
    }

    @Test(priority = 3, enabled = true, dependsOnMethods = {"test02CountAll"}, description = "????????????????????????????????????")
    public void test03CountRange() throws SQLException, InterruptedException {
//        Thread.sleep(1200000);
        try(Statement statement = connection.createStatement()) {
            String querySql = "select count(*) from " + tableName + " where id>40000 and id<=200000";
            ResultSet resultSet = statement.executeQuery(querySql);
            int countNum = 0;
            while (resultSet.next()) {
                countNum = resultSet.getInt(1);
            }
            statement.close();
            System.out.println("count???????????????????????? " + countNum);
            Assert.assertEquals(countNum, 160000);
        }
    }

    @Test(priority = 4, enabled = false, dependsOnMethods = {"test03CountRange"}, description = "??????????????????")
    public void test04UpdateRange() throws SQLException, InterruptedException {
//        Thread.sleep(1200000);
        try(Statement statement = connection.createStatement()) {
            String updateSql = "update " + tableName + " set name='BJ' where id>40000 and id<=200000";
            int updateNum = statement.executeUpdate(updateSql);
            Thread.sleep(60000);
            System.out.println("????????????????????? " + updateNum);
            Assert.assertEquals(updateNum, 160000);
            String querySql = "select count(*) from " + tableName + " where name='BJ'";
            ResultSet resultSet = statement.executeQuery(querySql);
            int countNum = 0;
            while (resultSet.next()) {
                countNum = resultSet.getInt(1);
            }
            statement.close();
            Assert.assertEquals(countNum, 160000);
        }
    }

    @Test(priority = 5, enabled = true, dependsOnMethods = {"test03CountRange"}, description = "??????????????????")
    public void test05DeleteAll() throws SQLException, InterruptedException {
//        Thread.sleep(1200000);
        try(Statement statement = connection.createStatement()) {
            String deleteSql = "delete from " + tableName;
            int deleteNum = statement.executeUpdate(deleteSql);
            Thread.sleep(60000);
            System.out.println("????????????????????? " + deleteNum);
            Assert.assertEquals(deleteNum, 200000);
            String querySql = "select count(*) from " + tableName;
            ResultSet resultSet = statement.executeQuery(querySql);
            int countNum = 0;
            while (resultSet.next()) {
                countNum = resultSet.getInt(1);
            }
            statement.close();
            System.out.println("???????????????count??????????????? " + countNum);
            Assert.assertEquals(countNum, 0);
        }
    }

    @AfterClass(alwaysRun = true, description = "???????????????????????????????????????????????????")
    public void tearDownAll() throws SQLException {
        Statement tearDownStatement = null;
        try{
            tearDownStatement = connection.createStatement();
            tearDownStatement.execute("drop table " + tableName);
        } catch (SQLException e) {
            e.printStackTrace();
        }finally {
            try{
                if(tearDownStatement != null) {
                    tearDownStatement.close();
                }
            } catch (SQLException e) {
                e.printStackTrace();
            }

            try{
                if(connection != null) {
                    connection.close();
                }
            } catch (SQLException e) {
                e.printStackTrace();
            }
        }
    }

}
