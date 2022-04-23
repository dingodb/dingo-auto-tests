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

import org.testng.annotations.AfterClass;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;

public class SQLFuncs {
//    private static final String defaultConnectIP = "172.20.3.26";
    private static String defaultConnectIP = CommonArgs.getDefaultDingoClusterIP();
    private static final String JDBC_DRIVER = "io.dingodb.driver.client.DingoDriverClient";
    private static String connectUrl = "jdbc:dingo:thin:url=" + defaultConnectIP + ":8765";

    public static Connection connection;

    public static Connection connectDB() throws ClassNotFoundException, SQLException {
        Class.forName(JDBC_DRIVER);
        connection = DriverManager.getConnection(connectUrl);
        return connection;
    }

    public static String getFuncTableName() {
        final String funcTablePrefix = "funcTest";
        String funcTableName = funcTablePrefix + CommonArgs.getCurDateStr();
        return funcTableName;
    }

    public void createFuncTable() throws SQLException, ClassNotFoundException {
        String funcTableName = getFuncTableName();
        connection = connectDB();
        Statement statement = connection.createStatement();

        String createFuncSQL = "create table " + funcTableName + "("
                + "id int,"
                + "name varchar(32) not null,"
                + "age int,"
                + "amount double,"
                + "primary key(id)"
                + ")";
        statement.execute(createFuncSQL);
        statement.close();
//        connection.close();
    }

    public int insertMultiValues() throws ClassNotFoundException, SQLException {
        String funcTableName = getFuncTableName();
        connection = connectDB();
        Statement statement = connection.createStatement();

        String batInsertSql = "insert into " + funcTableName +
                " values (1, 'Alice', 18, 3.5),\n" +
                "(2, 'Betty', 22, 4.1),\n" +
                "(3, 'Cindy', 39, 4.6),\n" +
                "(4, 'Doris', 25, 5.2),\n" +
                "(5, 'Emily', 24, 5.8),\n" +
                "(6, 'Alice', 32, 6.1),\n" +
                "(7, 'Betty', 18, 6.9),\n" +
                "(8, 'Alice', 22, 7.3),\n" +
                "(9, 'Cindy', 25, 3.5)";
        int insertMultiRowCount = statement.executeUpdate(batInsertSql);
        statement.close();
//        connection.close();
        return insertMultiRowCount;
    }

    //按姓名去重查询
    public List<String> distinctNameFunc() throws SQLException, ClassNotFoundException {
        String funcTableName = getFuncTableName();
        connection = connectDB();
        Statement statement = connection.createStatement();

        String distinctNameSql = "select distinct(name) from " + funcTableName;
        ResultSet distinctNameRst = statement.executeQuery(distinctNameSql);
        List<String> distinctNameList = new ArrayList<String>();
        while (distinctNameRst.next()) {
            distinctNameList.add(distinctNameRst.getString("name"));
        }
        statement.close();
//        connection.close();
        return distinctNameList;
    }

    //按年龄去重查询
    public List<Integer> distinctAgeFunc() throws SQLException, ClassNotFoundException {
        String funcTableName = getFuncTableName();
        connection = connectDB();
        Statement statement = connection.createStatement();

        String distinctAgeSql = "select distinct(age) from " + funcTableName;
        ResultSet distinctAgeRst = statement.executeQuery(distinctAgeSql);
        List<Integer> distinctAgeList = new ArrayList<Integer>();
        while (distinctAgeRst.next()) {
            distinctAgeList.add(distinctAgeRst.getInt("age"));
        }
        statement.close();
//        connection.close();
        return distinctAgeList;
    }

    //求平均
    public int avgAgeFunc() throws SQLException, ClassNotFoundException {
        String funcTableName = getFuncTableName();
        connection = connectDB();
        Statement statement = connection.createStatement();

        String avgSql = "select avg(age) from " + funcTableName;
        ResultSet avgRst = statement.executeQuery(avgSql);
        int avgAge = 0;
        while (avgRst.next()) {
            avgAge = avgRst.getInt(1);
        }
        statement.close();
//        connection.close();
        return avgAge;
    }

    //求和
    public int sumAgeFunc() throws SQLException, ClassNotFoundException {
        String funcTableName = getFuncTableName();
        connection = connectDB();
        Statement statement = connection.createStatement();

        String sumSql = "select sum(age) from " + funcTableName;
        ResultSet sumRst = statement.executeQuery(sumSql);
        int sumAge = 0;
        while (sumRst.next()) {
            sumAge = sumRst.getInt(1);
        }
        statement.close();
//        connection.close();
        return sumAge;
    }

    //求最大值
    public int maxAgeFunc() throws SQLException, ClassNotFoundException {
        String funcTableName = getFuncTableName();
        connection = connectDB();
        Statement statement = connection.createStatement();

        String maxSql = "select max(age) from " + funcTableName;
        ResultSet maxRst = statement.executeQuery(maxSql);
        int maxAge = 0;
        while (maxRst.next()) {
            maxAge = maxRst.getInt(1);
        }
        statement.close();
//        connection.close();
        return maxAge;
    }

    //求最小值
    public int minAgeFunc() throws SQLException, ClassNotFoundException {
        String funcTableName = getFuncTableName();
        connection = connectDB();
        Statement statement = connection.createStatement();

        String minSql = "select min(age) from " + funcTableName;
        ResultSet minRst = statement.executeQuery(minSql);
        int minAge = 0;
        while (minRst.next()) {
            minAge = minRst.getInt(1);
        }
        statement.close();
//        connection.close();
        return minAge;
    }

    //统计总数
    public int countFunc() throws SQLException, ClassNotFoundException {
        String funcTableName = getFuncTableName();
        connection = connectDB();
        Statement statement = connection.createStatement();

        String countSql = "select count(*) cnt from " + funcTableName;
        ResultSet countRst = statement.executeQuery(countSql);
        int countRows = 0;
        while (countRst.next()) {
            countRows = countRst.getInt("cnt");
        }
        statement.close();
//        connection.close();
        return countRows;
    }

    //升序
    public List<Integer> orderAscFunc() throws SQLException, ClassNotFoundException {
        String funcTableName = getFuncTableName();
        connection = connectDB();
        Statement statement = connection.createStatement();

        String orderAscSql = "select * from " + funcTableName + " order by age";
        ResultSet orderAscRst = statement.executeQuery(orderAscSql);
        List<Integer> orderAscAgeList = new ArrayList<Integer>();
        while (orderAscRst.next()) {
            orderAscAgeList.add(orderAscRst.getInt("age"));
        }
        statement.close();
//        connection.close();
        return orderAscAgeList;
    }

    //降序
    public List<Integer> orderDescFunc() throws SQLException, ClassNotFoundException {
        String funcTableName = getFuncTableName();
        connection = connectDB();
        Statement statement = connection.createStatement();

        String orderDescSql = "select * from " + funcTableName + " order by age desc";
        ResultSet orderDescRst = statement.executeQuery(orderDescSql);
        List<Integer> orderDescAgeList = new ArrayList<Integer>();
        while (orderDescRst.next()) {
            orderDescAgeList.add(orderDescRst.getInt("age"));
        }
        statement.close();
//        connection.close();
        return orderDescAgeList;
    }

    //不排序取limit
    public List<String> limitWithoutOrderAndOffsetFunc() throws SQLException, ClassNotFoundException {
        String funcTableName = getFuncTableName();
        connection = connectDB();
        Statement statement = connection.createStatement();

        String limitSql = "select * from " + funcTableName + " limit 5";
        ResultSet limitRst = statement.executeQuery(limitSql);
        List<String> limitList = new ArrayList<String>();
        while (limitRst.next()) {
            limitList.add(limitRst.getString("name"));
        }
        statement.close();
//        connection.close();
        return limitList;
    }

    //排序取top1
    public List<Integer> orderLimitWithoutOffsetFunc() throws SQLException, ClassNotFoundException {
        String funcTableName = getFuncTableName();
        connection = connectDB();
        Statement statement = connection.createStatement();

        String orderLimitSql = "select * from " + funcTableName + " order by age desc limit 1";
        ResultSet orderLimitRst = statement.executeQuery(orderLimitSql);
        List<Integer> orderLimitList = new ArrayList<Integer>();
        while (orderLimitRst.next()) {
            orderLimitList.add(orderLimitRst.getInt("age"));
        }
        statement.close();
//        connection.close();
        return orderLimitList;
    }

    //排序指定偏移量取指定条数
    public List<Integer> orderLimitWithOffsetFunc() throws SQLException, ClassNotFoundException {
        String funcTableName = getFuncTableName();
        connection = connectDB();
        Statement statement = connection.createStatement();

        String orderLimitOffsetSql = "select * from " + funcTableName + " order by age desc limit 2 offset 3";
        ResultSet orderLimitOffsetRst = statement.executeQuery(orderLimitOffsetSql);
        List<Integer> orderLimitOffsetList = new ArrayList<Integer>();
        while (orderLimitOffsetRst.next()) {
            orderLimitOffsetList.add(orderLimitOffsetRst.getInt("age"));
        }
        statement.close();
//        connection.close();
        return orderLimitOffsetList;
    }

    //分组查询,获取sum值
    public List<Double> groupOrderAmountFunc() throws SQLException, ClassNotFoundException {
        String funcTableName = getFuncTableName();
        connection = connectDB();
        Statement statement = connection.createStatement();

        String groupOrderSql = "select name,sum(amount) sa from " + funcTableName + " group by name order by sa";
        ResultSet groupOrderRst = statement.executeQuery(groupOrderSql);
        List<Double> groupOrderAmountList = new ArrayList<Double>();
        while (groupOrderRst.next()) {
            groupOrderAmountList.add(groupOrderRst.getDouble("sa"));
        }
        statement.close();
//        connection.close();
        return groupOrderAmountList;
    }

    //分组查询，获取姓名列表
    public List<String> groupOrderNameFunc() throws SQLException, ClassNotFoundException {
        String funcTableName = getFuncTableName();
        connection = connectDB();
        Statement statement = connection.createStatement();

        String groupOrderSql = "select name,sum(amount) sa from " + funcTableName + " group by name order by sa";
        ResultSet groupOrderRst = statement.executeQuery(groupOrderSql);
        List<String> groupOrderNameList = new ArrayList<String>();
        while (groupOrderRst.next()) {
            groupOrderNameList.add(groupOrderRst.getString("name"));
        }
        statement.close();
//        connection.close();
        return groupOrderNameList;
    }

    //按姓名删除
    public int deleteWithNameCondition() throws SQLException, ClassNotFoundException {
        String funcTableName = getFuncTableName();
        connection = connectDB();
        Statement statement = connection.createStatement();

        String deleteNameSql = "delete from " + funcTableName + " where name=" + "'Alice" + "'";
        int deleteCount = statement.executeUpdate(deleteNameSql);
        statement.close();
//        connection.close();
        return deleteCount;
    }

    //按姓名条件删除后，获取姓名列表
    public List<String> getNameListAfterDeleteName() throws SQLException, ClassNotFoundException {
        String funcTableName = getFuncTableName();
        connection = connectDB();
        Statement statement = connection.createStatement();

        String allSql = "select * from " + funcTableName;
        ResultSet allRst = statement.executeQuery(allSql);
        List<String> afterDeleteNameList = new ArrayList<String>();
        while (allRst.next()) {
            afterDeleteNameList.add(allRst.getString("name"));
        }
        statement.close();
//        connection.close();
        return afterDeleteNameList;
    }

    //验证cast函数，将字符串转化为整形
    public int castFunc() throws SQLException, ClassNotFoundException {
        String funcTableName = getFuncTableName();
        connection = connectDB();
        Statement statement = connection.createStatement();

        String insertSql = "insert into " + funcTableName + " values (10,'10',15,24.2)";
        statement.execute(insertSql);

        String castSql = "select cast(name as int) + 123 as castnum from " + funcTableName + " where name='10'";
        ResultSet castNameRst = statement.executeQuery(castSql);
        int afterCast = 0;
        while (castNameRst.next()) {
            afterCast += castNameRst.getInt("castnum");
        }
        statement.close();
//        connection.close();
        return afterCast;
    }

}
