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
import utils.GetRandomValue;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

public class TestSplit {

    private static String tableName = "jdbctest20";
    private static Connection connection = null;

    static {
        try {
            connection = JDBCUtils.getConnection();
        } catch (Exception e) {
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

    @BeforeClass(alwaysRun = true, description = "测试开始前，连接数据库")
    public static void setupAll() {
        Assert.assertNotNull(connection);
    }

    @Test(priority = 0, enabled = true, description = "创建表格")
    public void test00CreateSplitTable() throws SQLException {
        TestSplit testSplit = new TestSplit();
        testSplit.createSplitTable();
    }

    @Test(priority = 1, enabled = true, dependsOnMethods = {"test00CreateSplitTable"},
            description = "使用statement插入指定条数的数据量")
    public void test01InsertValues1() throws SQLException {
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

    @Test(priority = 1, enabled = false, dependsOnMethods = {"test00CreateSplitTable"},
            description = "使用preparedStatement批量插入数据")
    public void test01InsertValues2() throws SQLException {
        int insertNum = 200000;
        String insertsql = "insert into " + tableName + " values (?, ?, ?, ?)";
        try(PreparedStatement ps = connection.prepareStatement(insertsql)) {
            for(int i=1; i<=insertNum; i++) {
                String nameStr = GetRandomValue.getRandStr(6);
                int ageNum = GetRandomValue.getRandInt(100);
//                double amountNum = Double.parseDouble(GetRandomValue.formateRate(String.valueOf(GetRandomValue.getRandDouble(0, 10000)),2));
                double amountNum = GetRandomValue.getRandDouble(0, 10000);
                ps.setInt(1, i);
                ps.setString(2, nameStr);
                ps.setInt(3, ageNum);
                ps.setDouble(4, amountNum);

                //添加批量sql
                ps.addBatch();

                if(i % 500 == 0){
                    //执行batch
                    ps.executeBatch();

                    //清空batch
                    ps.clearBatch();
                }
            }
        }
    }

    @Test(priority = 2, enabled = true, dependsOnMethods = {"test01InsertValues1"}, description = "统计插入总条数是否正确")
    public void test02CountAll() throws SQLException, InterruptedException {
        Thread.sleep(1200000);
        try(Statement statement = connection.createStatement()) {
            String querySql = "select count(*) from " + tableName;
            ResultSet resultSet = statement.executeQuery(querySql);
            int countNum = 0;
            while (resultSet.next()) {
                countNum = resultSet.getInt(1);
            }

            resultSet.close();
            statement.close();
            System.out.println("count全表统计条数： " + countNum);
            Assert.assertEquals(countNum, 200000);
        }
    }

    @Test(priority = 3, enabled = true, dependsOnMethods = {"test02CountAll"}, description = "统计条件区间条数是否正确")
    public void test03CountRange() throws SQLException, InterruptedException {
//        Thread.sleep(1200000);
        try(Statement statement = connection.createStatement()) {
            String querySql = "select count(*) from " + tableName + " where id>40000 and id<=200000";
            ResultSet resultSet = statement.executeQuery(querySql);
            int countNum = 0;
            while (resultSet.next()) {
                countNum = resultSet.getInt(1);
            }

            resultSet.close();
            statement.close();
            System.out.println("count统计区间总条数： " + countNum);
            Assert.assertEquals(countNum, 160000);
        }
    }

    @Test(priority = 4, enabled = true, dependsOnMethods = {"test03CountRange"}, description = "验证区间更新")
    public void test04UpdateRange() throws SQLException, InterruptedException {
//        Thread.sleep(1200000);
        try(Statement statement = connection.createStatement()) {
            String updateSql = "update " + tableName + " set name='BJ' where id>40000 and id<=200000";
            int updateNum = statement.executeUpdate(updateSql);
            Thread.sleep(60000);
            System.out.println("实际更新条数： " + updateNum);
            Assert.assertEquals(updateNum, 160000);
            String querySql = "select count(*) from " + tableName + " where name='BJ'";
            ResultSet resultSet = statement.executeQuery(querySql);
            int countNum = 0;
            while (resultSet.next()) {
                countNum = resultSet.getInt(1);
            }

            resultSet.close();
            statement.close();
            Assert.assertEquals(countNum, 160000);
        }
    }

    @Test(priority = 5, enabled = true, dependsOnMethods = {"test04UpdateRange"}, description = "验证全表删除")
    public void test05DeleteAll() throws SQLException, InterruptedException {
//        Thread.sleep(1200000);
        try(Statement statement = connection.createStatement()) {
            String deleteSql = "delete from " + tableName;
            int deleteNum = statement.executeUpdate(deleteSql);
            Thread.sleep(60000);
            System.out.println("实际删除条数： " + deleteNum);
            Assert.assertEquals(deleteNum, 200000);
            String querySql = "select count(*) from " + tableName;
            ResultSet resultSet = statement.executeQuery(querySql);
            int countNum = 0;
            while (resultSet.next()) {
                countNum = resultSet.getInt(1);
            }

            resultSet.close();
            statement.close();
            System.out.println("清空表后，count统计条数： " + countNum);
            Assert.assertEquals(countNum, 0);
        }
    }

    @AfterClass(alwaysRun = true, description = "测试完成后删除数据和表格并关闭连接")
    public void tearDownAll() throws SQLException {
        Statement tearDownStatement = null;
        try{
            tearDownStatement = connection.createStatement();
            tearDownStatement.execute("drop table " + tableName);
        } catch (SQLException e) {
            e.printStackTrace();
        }finally {
            JDBCUtils.closeResource(connection, tearDownStatement);
        }
    }

}