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
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class TestSplit {

    private static String tableName1 = "splittest20";
    private static String tableName2 = "statetest2";
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
            String sql = "create table " + tableName1 + "("
                    + "id int,"
                    + "name varchar(32) not null,"
                    + "age int,"
                    + "amount double,"
                    + "primary key(id)"
                    + ") partition by range values(50001),(100001),(150001) with (propKey = propValue)";
            statement.execute(sql);
        }
    }

    public void createStateTable() throws SQLException {
        try(Statement statement = connection.createStatement()) {
            String sql = "create table " + tableName2 + "("
                    + "id int,"
                    + "name varchar(32) not null,"
                    + "age int,"
                    + "amount double,"
                    + "primary key(id)"
                    + ") partition by range values(10000) with (propKey = propValue)";
            statement.execute(sql);
        }
    }

    @BeforeClass(alwaysRun = true, description = "测试开始前，连接数据库")
    public static void setupAll() {
        Assert.assertNotNull(connection);
    }

    @Test(priority = 0, enabled = true, description = "创建表1")
    public void test00CreateSplitTable() throws SQLException {
        TestSplit testSplit = new TestSplit();
        testSplit.createSplitTable();
    }

    @Test(priority = 0, enabled = true, description = "创建表2")
    public void test00CreateStateTable() throws SQLException {
        TestSplit testSplit = new TestSplit();
        testSplit.createStateTable();
    }

    @Test(priority = 1, enabled = true, dependsOnMethods = {"test00CreateStateTable"},
            description = "使用statement插入指定条数的数据量")
    public void test01StateSingleInsert() throws SQLException {
        int insertNum = 20000;
        try(Statement statement = connection.createStatement()) {
            GetRandomValue getRandomValue = new GetRandomValue();
            Instant start = Instant.now();
            for(int i = 1; i <= insertNum; i++) {
                String nameStr = getRandomValue.getRandStr(6);
                int ageNum = getRandomValue.getRandInt(100);
//                double amountNum = GetRandomValue.formatDoubleDecimal1(GetRandomValue.getRandDouble(0, 10000),2);
                double amountNum = getRandomValue.getRandDouble(0, 10000);
                String insertSql = "insert into " + tableName2 + " values (" + i + ",'" +
                        nameStr + "'," + ageNum + "," + amountNum + ")";
                statement.executeUpdate(insertSql);
            }
            statement.close();
            Instant finish = Instant.now();
            long timeElapsed = Duration.between(start, finish).toMillis();
            System.out.println("use time: " + timeElapsed);
        }
    }

    @Test(priority = 1, enabled = true, dependsOnMethods = {"test00CreateStateTable"},
            description = "使用preparedStatement单行插入数据")
    public void test01PSSingleInsert() throws SQLException {
        int baseNum = 20000;
        int insertNum = 10000;
        String insertsql = "insert into " + tableName2 + " values (?, ?, ?, ?)";
        try(PreparedStatement ps = connection.prepareStatement(insertsql)) {
            GetRandomValue getRandomValue = new GetRandomValue();
            Instant start = Instant.now();
            for(int i = baseNum + 1; i <= baseNum + insertNum; i++) {
                String nameStr = getRandomValue.getRandStr(6);
                int ageNum = getRandomValue.getRandInt(100);
                double amountNum = getRandomValue.getRandDouble(0, 10000);
                ps.setInt(1, i);
                ps.setString(2, nameStr);
                ps.setInt(3, ageNum);
                ps.setDouble(4, amountNum);
                ps.executeUpdate();
            }
            ps.close();
            Instant finish = Instant.now();
            long timeElapsed = Duration.between(start, finish).toMillis();
            System.out.println("use time: " + timeElapsed);
        }
    }

    @Test(priority = 1, enabled = true, dependsOnMethods = {"test00CreateSplitTable"},
            description = "使用preparedStatement批量插入数据")
    public void test01PSBatchInsert() throws SQLException {
        int insertNum = 200000;
        String insertsql = "insert into " + tableName1 + " values (?, ?, ?, ?)";
        try(PreparedStatement ps = connection.prepareStatement(insertsql)) {
            GetRandomValue getRandomValue = new GetRandomValue();
            Instant start = Instant.now();
            for(int i=1; i<=insertNum; i++) {
                String nameStr = getRandomValue.getRandStr(6);
                int ageNum = getRandomValue.getRandInt(100);
                double amountNum = getRandomValue.getRandDouble(0, 10000);
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
            Instant finish = Instant.now();
            long timeElapsed = Duration.between(start, finish).toMillis();
            System.out.println("use time: " + timeElapsed);
        }
    }

    @Test(priority = 2, enabled = false, dependsOnMethods = {"test01PSBatchInsert"},
            description = "使用preparedStatement查询数据")
    public void test02QueryUsingPreState() throws SQLException {
        String querysql = "select * from " + tableName1 + " where id=?";
        try(PreparedStatement ps = connection.prepareStatement(querysql)) {
            ps.setInt(1,1);
            ResultSet resultSet = ps.executeQuery();
            List<List> queryList = new ArrayList<List>();
            while(resultSet.next()) {
                List rowList = new ArrayList();
                rowList.add(resultSet.getString(1));
                rowList.add(resultSet.getString(2));
                rowList.add(resultSet.getString(3));
                rowList.add(resultSet.getString(4));

                queryList.add(rowList);
            }
            System.out.println("查询到的数据：" + queryList);
            resultSet.close();
        }
    }

    @Test(priority = 3, enabled = true, dependsOnMethods = {"test01PSBatchInsert"}, description = "统计插入总条数是否正确")
    public void test03CountAll() throws SQLException, InterruptedException {
        Thread.sleep(30000);
        try(Statement statement = connection.createStatement()) {
            String querySql = "select count(*) from " + tableName1;
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

    @Test(priority = 4, enabled = true, dependsOnMethods = {"test03CountAll"}, description = "统计条件区间条数是否正确")
    public void test04CountRange() throws SQLException, InterruptedException {
        Thread.sleep(300000);
        try(Statement statement = connection.createStatement()) {
            String querySql = "select count(*) from " + tableName1 + " where id>40000 and id<=200000";
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

    @Test(priority = 5, enabled = true, dependsOnMethods = {"test04CountRange"}, description = "验证区间更新")
    public void test05UpdateRange() throws SQLException, InterruptedException {
//        Thread.sleep(1200000);
        try(Statement statement = connection.createStatement()) {
            String updateSql = "update " + tableName1 + " set name='BJ' where id>40000 and id<=200000";
            int updateNum = statement.executeUpdate(updateSql);
            Thread.sleep(60000);
            System.out.println("实际更新条数： " + updateNum);
            Assert.assertEquals(updateNum, 160000);
            String querySql = "select count(*) from " + tableName1 + " where name='BJ'";
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

    @Test(priority = 6, enabled = true, dependsOnMethods = {"test05UpdateRange"}, description = "验证全表删除")
    public void test06DeleteAll() throws SQLException, InterruptedException {
//        Thread.sleep(1200000);
        try(Statement statement = connection.createStatement()) {
            String deleteSql = "delete from " + tableName1;
            int deleteNum = statement.executeUpdate(deleteSql);
            Thread.sleep(60000);
            System.out.println("实际删除条数： " + deleteNum);
            Assert.assertEquals(deleteNum, 200000);
            String querySql = "select count(*) from " + tableName1;
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
    public void tearDownAll() throws SQLException, ClassNotFoundException {
        Statement tearDownStatement = null;
        List<String> tableList = Arrays.asList(tableName1, tableName2);
        try{
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
//            tearDownStatement.execute("drop table " + tableName1);
//            tearDownStatement.execute("drop table " + tableName2);
        } catch (SQLException e) {
            e.printStackTrace();
        }finally {
            JDBCUtils.closeResource(connection, tearDownStatement);
        }
    }

}
